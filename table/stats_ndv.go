// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package table

import (
	"context"
	"encoding/binary"
	"fmt"
	"math"
	"slices"
	"unsafe"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/decimal128"
	"github.com/apache/arrow-go/v18/arrow/extensions"
	"github.com/apache/iceberg-go"
	"github.com/apache/iceberg-go/table/stats"
)

// NDVStats is the result of ComputeNDVs.
type NDVStats struct {
	// Snapshot is the snapshot the sketches were computed over. Its ID and
	// sequence number identify the sketches when they are written to a Puffin
	// statistics file. It is nil when the table has no snapshot.
	Snapshot *Snapshot
	// Columns holds one sketch per analyzed column, keyed by field ID.
	Columns map[int]NDVResult
}

// NDVResult is the number-of-distinct-values sketch computed for one column.
type NDVResult struct {
	// NDV is the estimated number of distinct non-null values.
	NDV int64
	// Sketch is the serialized DataSketches Theta sketch, i.e. the body of an
	// apache-datasketches-theta-v1 statistics blob.
	Sketch []byte
}

// ComputeNDVs scans a snapshot and computes a DataSketches Theta sketch per
// top-level primitive column, returning the estimated number of distinct values
// keyed by field ID along with the snapshot that was analyzed.
//
// When snapshotID is 0 the current snapshot is used; a table without a
// snapshot yields an empty result, as in Iceberg-Java's ComputeTableStats.
// Columns are resolved against the analyzed snapshot's schema. When columns
// is empty every top-level primitive column is analyzed except geometry and
// geography columns, which have no single-value serialization to hash, and
// columns of type unknown, which hold only nulls. Naming a missing column, a
// non-primitive column, a geospatial or unknown-typed column, or a column
// nested inside a struct is an error.
//
// Values are fed to the sketch using Iceberg's single-value serialization (the
// same encoding used for manifest bounds), so estimates are comparable with
// sketches produced by other Iceberg implementations. Null values are not
// counted, and neither are empty string or binary values.
func (t Table) ComputeNDVs(ctx context.Context, snapshotID int64, columns ...string) (NDVStats, error) {
	base := t.Scan(WithSnapshotID(snapshotID))

	snap, err := base.ResolveSnapshot()
	if err != nil {
		return NDVStats{}, fmt.Errorf("compute NDVs: %w", err)
	}
	if snap == nil {
		return NDVStats{Columns: map[int]NDVResult{}}, nil
	}

	// Pin the resolved snapshot so names are resolved against its schema, as
	// Iceberg-Java does. With snapshotID 0 the scan would use the current
	// schema, which may have columns the snapshot doesn't.
	pinned := WithSnapshotID(snap.SnapshotID)
	schema, err := t.Scan(pinned).Projection()
	if err != nil {
		return NDVStats{}, fmt.Errorf("compute NDVs: %w", err)
	}

	fields, err := ndvFields(schema, columns)
	if err != nil {
		return NDVStats{}, fmt.Errorf("compute NDVs: %w", err)
	}
	if len(fields) == 0 {
		return NDVStats{Snapshot: snap, Columns: map[int]NDVResult{}}, nil
	}

	// Select only the sketched columns so the default path does not read and
	// decode nested or geospatial columns just to drop them.
	byName := make(map[string]iceberg.NestedField, len(fields))
	names := make([]string, 0, len(fields))
	for _, f := range fields {
		byName[f.Name] = f
		names = append(names, f.Name)
	}

	arrowSchema, records, err := t.Scan(pinned, WithSelectedFields(names...)).
		ToArrowRecords(ctx)
	if err != nil {
		return NDVStats{}, fmt.Errorf("compute NDVs: %w", err)
	}

	// Map each arrow column index to the target field and its sketch, matching
	// by name (the returned arrow schema carries no field-id metadata).
	targets := make(map[int]*ndvTarget, len(fields))
	for i, f := range arrowSchema.Fields() {
		nf, ok := byName[f.Name]
		if !ok {
			continue
		}
		targets[i] = &ndvTarget{field: nf, sketch: stats.NewSketch()}
	}

	for rec, err := range records {
		if err != nil {
			return NDVStats{}, fmt.Errorf("compute NDVs: %w", err)
		}
		if err := addRecordToSketches(rec, targets); err != nil {
			rec.Release()

			return NDVStats{}, fmt.Errorf("compute NDVs: %w", err)
		}
		rec.Release()
	}

	results := make(map[int]NDVResult, len(targets))
	for _, tgt := range targets {
		results[tgt.field.ID] = NDVResult{
			NDV:    tgt.sketch.NDV(),
			Sketch: tgt.sketch.Serialize(),
		}
	}

	return NDVStats{Snapshot: snap, Columns: results}, nil
}

// ndvFields returns the columns to sketch. With no columns named it returns
// every top-level primitive column that can be sketched. Named columns are
// validated as Iceberg-Java's ComputeTableStats does, so a column that can't
// be sketched is an error rather than a silently missing result.
func ndvFields(schema *iceberg.Schema, columns []string) ([]iceberg.NestedField, error) {
	if len(columns) == 0 {
		var fields []iceberg.NestedField
		for _, f := range schema.Fields() {
			if ndvSketchable(f.Type) {
				fields = append(fields, f)
			}
		}

		return fields, nil
	}

	fields := make([]iceberg.NestedField, 0, len(columns))
	for _, name := range columns {
		f, ok := schema.FindFieldByName(name)
		if !ok {
			return nil, fmt.Errorf("%w: can't find column %q", iceberg.ErrInvalidArgument, name)
		}
		if _, ok := f.Type.(iceberg.PrimitiveType); !ok {
			return nil, fmt.Errorf("%w: can't compute stats on non-primitive type column: %s (%s)",
				iceberg.ErrInvalidArgument, name, f.Type)
		}
		if !ndvSketchable(f.Type) {
			return nil, fmt.Errorf("%w: can't compute stats on %s column: %s",
				iceberg.ErrInvalidArgument, f.Type, name)
		}
		if !slices.ContainsFunc(schema.Fields(), func(top iceberg.NestedField) bool { return top.ID == f.ID }) {
			return nil, fmt.Errorf("%w: can't compute stats on nested column: %s",
				iceberg.ErrInvalidArgument, name)
		}
		if !slices.ContainsFunc(fields, func(seen iceberg.NestedField) bool { return seen.ID == f.ID }) {
			fields = append(fields, f)
		}
	}

	return fields, nil
}

// ndvSketchable reports whether values of typ can be hashed into a sketch.
// Geometry and geography are primitive, but Iceberg defines no single-value
// serialization for their values (only for their bounds), so there is no
// encoding shared with other implementations to hash. Unknown columns hold
// only nulls and are read as an Arrow Null array, so there is nothing to hash.
func ndvSketchable(typ iceberg.Type) bool {
	switch typ.(type) {
	case iceberg.GeometryType, iceberg.GeographyType, iceberg.UnknownType:
		return false
	case iceberg.PrimitiveType:
		return true
	default:
		return false
	}
}

// ndvTarget pairs a primitive column with the sketch accumulating its values.
type ndvTarget struct {
	field  iceberg.NestedField
	sketch *stats.Sketch
}

// addRecordToSketches feeds every non-null value of each targeted column into
// its sketch, using Iceberg single-value serialization for the hashed bytes.
func addRecordToSketches(rec arrow.RecordBatch, targets map[int]*ndvTarget) error {
	numRows := int(rec.NumRows())
	var scratch [16]byte
	for col := range int(rec.NumCols()) {
		tgt, ok := targets[col]
		if !ok {
			continue
		}
		arr := rec.Column(col)
		for row := range numRows {
			if arr.IsNull(row) {
				continue // null value: not counted
			}
			if b, ok := ndvValueBytes(scratch[:0], arr, row, tgt.field.Type); ok {
				tgt.sketch.Add(b)

				continue
			}
			lit, err := getArrowValueAsIcebergLiteral(arr, row, tgt.field.Type)
			if err != nil {
				return fmt.Errorf("field %q: row %d: %w", tgt.field.Name, row, err)
			}
			if lit == nil {
				continue // null value: not counted
			}
			b, err := lit.MarshalBinary()
			if err != nil {
				return fmt.Errorf("field %q: serialize value: %w", tgt.field.Name, err)
			}
			tgt.sketch.Add(b)
		}
	}

	return nil
}

// ndvValueBytes returns the Iceberg single-value serialization of the non-null
// value at row, read straight from the Arrow buffers. It produces the same
// bytes as getArrowValueAsIcebergLiteral followed by MarshalBinary, without the
// per-value literal and allocation. The result may alias scratch or the
// array's buffers, so it is only valid until the next call. ok is false for
// arrays without a fast path or values that fail to convert; the caller then
// falls back to the literal path, which also reports the error.
func ndvValueBytes(scratch []byte, arr arrow.Array, row int, typ iceberg.Type) (b []byte, ok bool) {
	switch arr := arr.(type) {
	case *array.Boolean:
		if arr.Value(row) {
			return append(scratch, 1), true
		}

		return append(scratch, 0), true
	case *array.Int32:
		return binary.LittleEndian.AppendUint32(scratch, uint32(arr.Value(row))), true
	case *array.Int64:
		return binary.LittleEndian.AppendUint64(scratch, uint64(arr.Value(row))), true
	case *array.Float32:
		return binary.LittleEndian.AppendUint32(scratch, math.Float32bits(arr.Value(row))), true
	case *array.Float64:
		return binary.LittleEndian.AppendUint64(scratch, math.Float64bits(arr.Value(row))), true
	case *array.Date32:
		return binary.LittleEndian.AppendUint32(scratch, uint32(arr.Value(row))), true
	case *array.Time64:
		if arr.DataType().(*arrow.Time64Type).Unit != arrow.Microsecond {
			return nil, false
		}

		return binary.LittleEndian.AppendUint64(scratch, uint64(arr.Value(row))), true
	case *array.Timestamp:
		unit := arr.DataType().(*arrow.TimestampType).Unit
		var v int64
		var err error
		switch typ.(type) {
		case iceberg.TimestampType, iceberg.TimestampTzType:
			v, err = arrowTimestampToMicros(int64(arr.Value(row)), unit)
		case iceberg.TimestampNsType, iceberg.TimestampTzNsType:
			v, err = arrowTimestampToNanos(int64(arr.Value(row)), unit)
		default:
			return nil, false
		}
		if err != nil {
			return nil, false
		}

		return binary.LittleEndian.AppendUint64(scratch, uint64(v)), true
	case *array.Decimal128:
		return appendDecimalBytes(scratch, arr.Value(row)), true
	case *array.String:
		// The literal path converts strings for binary, fixed and uuid columns.
		if _, ok := typ.(iceberg.StringType); !ok {
			return nil, false
		}
		s := arr.Value(row)

		return unsafe.Slice(unsafe.StringData(s), len(s)), true
	case *array.LargeString:
		if _, ok := typ.(iceberg.StringType); !ok {
			return nil, false
		}
		s := arr.Value(row)

		return unsafe.Slice(unsafe.StringData(s), len(s)), true
	case *array.Binary:
		if _, ok := typ.(iceberg.BinaryType); !ok {
			return nil, false
		}

		return arr.Value(row), true
	case *array.LargeBinary:
		if _, ok := typ.(iceberg.BinaryType); !ok {
			return nil, false
		}

		return arr.Value(row), true
	case *array.FixedSizeBinary:
		ft, ok := typ.(iceberg.FixedType)
		if !ok || ft.Len() != arr.DataType().(*arrow.FixedSizeBinaryType).ByteWidth {
			return nil, false
		}

		return arr.Value(row), true
	case *extensions.UUIDArray:
		if _, ok := typ.(iceberg.UUIDType); !ok {
			return nil, false
		}

		return arr.Storage().(*array.FixedSizeBinary).Value(row), true
	default:
		return nil, false
	}
}

// appendDecimalBytes appends n as minimal-length big-endian two's complement,
// as DecimalLiteral.MarshalBinary does, without going through big.Int.
func appendDecimalBytes(dst []byte, n decimal128.Num) []byte {
	var b [16]byte
	binary.BigEndian.PutUint64(b[:8], uint64(n.HighBits()))
	binary.BigEndian.PutUint64(b[8:], n.LowBits())
	// Drop leading bytes that only repeat the sign bit of the byte after them.
	i := 0
	for i < len(b)-1 && ((b[i] == 0x00 && b[i+1] < 0x80) || (b[i] == 0xff && b[i+1] >= 0x80)) {
		i++
	}

	return append(dst, b[i:]...)
}
