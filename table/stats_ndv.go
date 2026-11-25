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
	"fmt"
	"slices"

	"github.com/apache/arrow-go/v18/arrow"
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
	for col := range int(rec.NumCols()) {
		tgt, ok := targets[col]
		if !ok {
			continue
		}
		arr := rec.Column(col)
		for row := range numRows {
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
