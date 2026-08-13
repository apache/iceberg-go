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
	"bytes"
	"context"
	"errors"
	"fmt"
	"math"
	"unsafe"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/bitutil"
	"github.com/apache/arrow-go/v18/arrow/compute"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/DataDog/iceberg-go"
	iceinternal "github.com/DataDog/iceberg-go/internal"
	iceio "github.com/DataDog/iceberg-go/io"
	"github.com/DataDog/iceberg-go/table/internal"
	"golang.org/x/sync/errgroup"
)

var ErrAmbiguousEqualityColumn = errors.New("equality delete column is ambiguous")

// equalityDeleteSet holds the set of delete keys and the column names
// used to look them up in data records. Each set corresponds to one
// group of equality field IDs — delete files with different field IDs
// produce separate sets.
type equalityDeleteSet struct {
	keys     set[string]
	fieldIDs []int
	colNames []string
}

type arrowFieldRef struct {
	path []int
}

type arrowFieldRefsByID map[int][]arrowFieldRef

func equalityFieldLocation(filePath string) string {
	location := filePath
	if location == "" {
		location = "data record"
	}

	return location
}

// indexArrowFields derives Arrow child paths from the structurally aligned,
// ID-resolved Iceberg file schema. It deliberately ignores names: dots in an
// Iceberg name are literal and must not be interpreted as a path.
func indexArrowFields(schema *iceberg.Schema) arrowFieldRefsByID {
	refs := make(arrowFieldRefsByID)
	if schema == nil {
		return refs
	}

	var visit func([]iceberg.NestedField, []int)
	visit = func(fields []iceberg.NestedField, parentPath []int) {
		for i, field := range fields {
			path := append(append([]int(nil), parentPath...), i)
			refs[field.ID] = append(refs[field.ID], arrowFieldRef{path: path})

			if nested, ok := field.Type.(*iceberg.StructType); ok {
				visit(nested.Fields(), path)
			}
		}
	}
	visit(schema.Fields(), nil)

	return refs
}

// indexArrowFieldsByMetadata is used for delete files whose Arrow fields carry
// IDs directly, before any name mapping is needed.
func indexArrowFieldsByMetadata(schema *arrow.Schema) arrowFieldRefsByID {
	refs := make(arrowFieldRefsByID)
	var visit func([]arrow.Field, []int)
	visit = func(fields []arrow.Field, parentPath []int) {
		for i, field := range fields {
			path := append(append([]int(nil), parentPath...), i)
			if id := getFieldID(field); id != nil {
				refs[*id] = append(refs[*id], arrowFieldRef{path: path})
			}

			if nested, ok := field.Type.(*arrow.StructType); ok {
				visit(nested.Fields(), path)
			}
		}
	}
	visit(schema.Fields(), nil)

	return refs
}

func resolveArrowField(refs arrowFieldRefsByID, fieldID int, fieldName, filePath string) (arrowFieldRef, error) {
	matches := refs[fieldID]
	location := equalityFieldLocation(filePath)
	if len(matches) == 1 {
		return matches[0], nil
	}
	if len(matches) > 1 {
		return arrowFieldRef{}, fmt.Errorf("%w: equality field ID %d (%s) in %s: found %d fields",
			ErrAmbiguousEqualityColumn, fieldID, fieldName, location, len(matches))
	}

	return arrowFieldRef{}, fmt.Errorf("equality field ID %d (%s) not found in %s", fieldID, fieldName, location)
}

func arrowArrayAtField(record arrow.RecordBatch, ref arrowFieldRef, fieldID int, fieldName, filePath string) (arrow.Array, error) {
	result, _, err := arrowArraysAtField(record, ref, fieldID, fieldName, filePath)

	return result, err
}

func arrowArraysAtField(record arrow.RecordBatch, ref arrowFieldRef, fieldID int, fieldName, filePath string) (arrow.Array, []arrow.Array, error) {
	location := filePath
	if location == "" {
		location = "data record"
	}
	if len(ref.path) == 0 || ref.path[0] >= int(record.NumCols()) {
		return nil, nil, fmt.Errorf("equality field ID %d (%s) not found in %s", fieldID, fieldName, location)
	}

	result := record.Column(ref.path[0])
	parents := make([]arrow.Array, 0, len(ref.path)-1)
	for _, index := range ref.path[1:] {
		structArray, ok := result.(*array.Struct)
		if !ok || index >= structArray.NumField() {
			return nil, nil, fmt.Errorf("equality field ID %d (%s) has unsupported nested path in %s", fieldID, fieldName, location)
		}
		parents = append(parents, result)
		result = structArray.Field(index)
	}

	return result, parents, nil
}

func makeArrowFieldEncoder(record arrow.RecordBatch, ref arrowFieldRef, fieldID int, fieldName, filePath string) (colEncoder, error) {
	column, parents, err := arrowArraysAtField(record, ref, fieldID, fieldName, filePath)
	if err != nil {
		return nil, err
	}

	encoder := makeColEncoder(column)
	if len(parents) == 0 {
		return encoder, nil
	}

	return func(buf *bytes.Buffer, row int) {
		for _, parent := range parents {
			if parent.IsNull(row) {
				buf.WriteByte(0)

				return
			}
		}

		encoder(buf, row)
	}, nil
}

// readAllEqualityDeleteFiles reads all unique equality delete files from
// the tasks and builds per-task delete key sets. Returns nil if there are
// no equality deletes. Delete files with different equality field IDs are
// kept as separate sets (not merged).
func readAllEqualityDeleteFiles(ctx context.Context, fs iceio.IO, schema *iceberg.Schema, nameMapping iceberg.NameMapping, tasks []FileScanTask, concurrency int) (map[int][]*equalityDeleteSet, error) {
	type deleteFileInfo struct {
		file     iceberg.DataFile
		fieldIDs []int
	}

	uniqueDeletes := make(map[string]deleteFileInfo)
	hasAny := false

	for _, t := range tasks {
		for _, d := range t.EqualityDeleteFiles {
			if d.ContentType() != iceberg.EntryContentEqDeletes {
				continue
			}

			if len(d.EqualityFieldIDs()) == 0 {
				return nil, fmt.Errorf("%w: equality delete file %s", ErrEmptyEqualityFieldIDs, d.FilePath())
			}

			hasAny = true
			if _, ok := uniqueDeletes[d.FilePath()]; !ok {
				uniqueDeletes[d.FilePath()] = deleteFileInfo{
					file:     d,
					fieldIDs: d.EqualityFieldIDs(),
				}
			}
		}
	}

	if !hasAny {
		return nil, nil
	}

	type deleteFileResult struct {
		path     string
		fieldIDs []int
		colNames []string
		keys     set[string]
	}

	g, ctx := errgroup.WithContext(ctx)
	g.SetLimit(concurrency)

	resultCh := make(chan deleteFileResult, len(uniqueDeletes))

	go func() {
		defer close(resultCh)

		for _, info := range uniqueDeletes {
			g.Go(func() error {
				keys, colNames, err := readEqualityDeleteFile(ctx, fs, schema, nameMapping, info.file, info.fieldIDs)
				if err != nil {
					return err
				}

				resultCh <- deleteFileResult{
					path:     info.file.FilePath(),
					fieldIDs: info.fieldIDs,
					colNames: colNames,
					keys:     keys,
				}

				return nil
			})
		}

		_ = g.Wait()
	}()

	type perFileDeleteKeys struct {
		fieldIDs []int
		colNames []string
		keys     set[string]
	}

	perFile := make(map[string]*perFileDeleteKeys)
	for result := range resultCh {
		perFile[result.path] = &perFileDeleteKeys{
			fieldIDs: result.fieldIDs,
			colNames: result.colNames,
			keys:     result.keys,
		}
	}

	if err := g.Wait(); err != nil {
		return nil, err
	}

	// Build per-task delete sets. Group by field IDs so delete files with
	// different equality field sets are applied independently.
	perTask := make(map[int][]*equalityDeleteSet)
	for i, t := range tasks {
		if len(t.EqualityDeleteFiles) == 0 {
			continue
		}

		// Group delete files by their field IDs key.
		groups := make(map[string]*equalityDeleteSet)
		for _, d := range t.EqualityDeleteFiles {
			dk, ok := perFile[d.FilePath()]
			if !ok {
				continue
			}

			groupKey := fmt.Sprint(dk.fieldIDs)
			g, exists := groups[groupKey]
			if !exists {
				g = &equalityDeleteSet{
					keys:     make(set[string]),
					fieldIDs: dk.fieldIDs,
					colNames: dk.colNames,
				}
				groups[groupKey] = g
			}

			for k := range dk.keys {
				g.keys[k] = struct{}{}
			}
		}

		sets := make([]*equalityDeleteSet, 0, len(groups))
		for _, g := range groups {
			if len(g.keys) > 0 {
				sets = append(sets, g)
			}
		}

		if len(sets) > 0 {
			perTask[i] = sets
		}
	}

	return perTask, nil
}

// readEqualityDeleteFile reads a single equality delete file and returns
// the set of encoded delete keys and the column names used.
func readEqualityDeleteFile(ctx context.Context, fs iceio.IO, tableSchema *iceberg.Schema, nameMapping iceberg.NameMapping, dataFile iceberg.DataFile, fieldIDs []int) (set[string], []string, error) {
	src, err := internal.GetFile(ctx, fs, dataFile, true)
	if err != nil {
		return nil, nil, err
	}

	rdr, err := src.GetReader(ctx)
	if err != nil {
		return nil, nil, err
	}
	defer iceinternal.CheckedClose(rdr, &err)

	tbl, err := rdr.ReadTable(ctx)
	if err != nil {
		return nil, nil, err
	}
	defer tbl.Release()

	hasFieldIDs, err := VisitArrowSchema(tbl.Schema(), hasIDs{})
	if err != nil {
		return nil, nil, err
	}

	var fileSchema *iceberg.Schema
	if !hasFieldIDs {
		if nameMapping == nil {
			nameMapping = tableSchema.NameMapping()
		}

		fileSchema, err = ArrowSchemaToIcebergWithOptions(tbl.Schema(), ArrowToIcebergOptions{
			NameMapping: nameMapping,
			TableSchema: tableSchema,
		})
		if err != nil {
			return nil, nil, err
		}
	}

	var fieldRefsByID arrowFieldRefsByID
	if hasFieldIDs {
		fieldRefsByID = indexArrowFieldsByMetadata(tbl.Schema())
	} else {
		fieldRefsByID = indexArrowFields(fileSchema)
	}

	// Resolve column names from field IDs.
	colNames := make([]string, len(fieldIDs))
	fieldRefs := make([]arrowFieldRef, len(fieldIDs))

	for i, fid := range fieldIDs {
		name, ok := tableSchema.FindColumnName(fid)
		if !ok {
			return nil, nil, fmt.Errorf("equality delete field ID %d not found in table schema for %s", fid, dataFile.FilePath())
		}

		ref, err := resolveArrowField(fieldRefsByID, fid, name, dataFile.FilePath())
		if err != nil {
			return nil, nil, err
		}

		colNames[i] = name
		fieldRefs[i] = ref
	}

	// Build the set of encoded delete keys by iterating aligned batches.
	keys := make(set[string])

	var keyBuf bytes.Buffer

	tr := array.NewTableReader(tbl, tbl.NumRows())
	defer tr.Release()

	for tr.Next() {
		rec := tr.RecordBatch()
		encoders := make([]colEncoder, len(fieldRefs))
		for i, ref := range fieldRefs {
			encoders[i], err = makeArrowFieldEncoder(rec, ref, fieldIDs[i], colNames[i], dataFile.FilePath())
			if err != nil {
				return nil, nil, err
			}
		}

		numRows := int(rec.NumRows())
		for row := 0; row < numRows; row++ {
			keyBuf.Reset()
			for _, enc := range encoders {
				enc(&keyBuf, row)
			}

			keys[keyBuf.String()] = struct{}{}
		}
	}

	return keys, colNames, nil
}

// bufPutUint16 writes a uint16 in big-endian without allocating.
func bufPutUint16(buf *bytes.Buffer, v uint16) {
	buf.Write([]byte{byte(v >> 8), byte(v)})
}

// bufPutUint32 writes a uint32 in big-endian without allocating.
func bufPutUint32(buf *bytes.Buffer, v uint32) {
	buf.Write([]byte{byte(v >> 24), byte(v >> 16), byte(v >> 8), byte(v)})
}

// bufPutUint64 writes a uint64 in big-endian without allocating.
func bufPutUint64(buf *bytes.Buffer, v uint64) {
	buf.Write([]byte{
		byte(v >> 56), byte(v >> 48), byte(v >> 40), byte(v >> 32),
		byte(v >> 24), byte(v >> 16), byte(v >> 8), byte(v),
	})
}

// bufString returns the buffer contents as a string without copying.
// The returned string is only valid until the next buffer modification.
func bufString(buf *bytes.Buffer) string {
	b := buf.Bytes()

	return unsafe.String(unsafe.SliceData(b), len(b))
}

// encodeArrowValue writes a single Arrow value to the buffer for key
// encoding. Values are type-tagged and length-prefixed for variable-length
// types to avoid hash collisions.
func encodeArrowValue(buf *bytes.Buffer, arr arrow.Array, idx int) {
	if arr.IsNull(idx) {
		buf.WriteByte(0) // null tag

		return
	}

	buf.WriteByte(1) // non-null tag

	switch a := arr.(type) {
	case *array.Int8:
		buf.WriteByte(byte(a.Value(idx)))
	case *array.Int16:
		bufPutUint16(buf, uint16(a.Value(idx)))
	case *array.Int32:
		bufPutUint32(buf, uint32(a.Value(idx)))
	case *array.Int64:
		bufPutUint64(buf, uint64(a.Value(idx)))
	case *array.Float32:
		bufPutUint32(buf, math.Float32bits(a.Value(idx)))
	case *array.Float64:
		bufPutUint64(buf, math.Float64bits(a.Value(idx)))
	case *array.String:
		s := a.Value(idx)
		bufPutUint32(buf, uint32(len(s)))
		buf.WriteString(s)
	case *array.LargeString:
		s := a.Value(idx)
		bufPutUint32(buf, uint32(len(s)))
		buf.WriteString(s)
	case *array.Binary:
		b := a.Value(idx)
		bufPutUint32(buf, uint32(len(b)))
		buf.Write(b)
	case *array.LargeBinary:
		b := a.Value(idx)
		bufPutUint32(buf, uint32(len(b)))
		buf.Write(b)
	case *array.FixedSizeBinary:
		buf.Write(a.Value(idx))
	case *array.Boolean:
		if a.Value(idx) {
			buf.WriteByte(1)
		} else {
			buf.WriteByte(0)
		}
	case *array.Date32:
		bufPutUint32(buf, uint32(a.Value(idx)))
	case *array.Date64:
		bufPutUint64(buf, uint64(a.Value(idx)))
	case *array.Time32:
		bufPutUint32(buf, uint32(a.Value(idx)))
	case *array.Time64:
		bufPutUint64(buf, uint64(a.Value(idx)))
	case *array.Timestamp:
		bufPutUint64(buf, uint64(a.Value(idx)))
	default:
		// Fallback: length-prefixed string representation.
		s := a.ValueStr(idx)
		bufPutUint32(buf, uint32(len(s)))
		buf.WriteString(s)
	}
}

// colEncoder writes the value at row idx to buf. Resolved once per column
// to avoid per-row type switches.
type colEncoder func(buf *bytes.Buffer, row int)

func writeNullTagIfNull(buf *bytes.Buffer, arr arrow.Array, row int) bool {
	if arr.IsNull(row) {
		buf.WriteByte(0)

		return true
	}

	return false
}

func withNullGuard(arr arrow.Array, enc colEncoder) colEncoder {
	if arr.NullN() == 0 {
		return enc
	}

	return func(buf *bytes.Buffer, row int) {
		if writeNullTagIfNull(buf, arr, row) {
			return
		}

		enc(buf, row)
	}
}

// makeColEncoder returns a colEncoder for the given Arrow array that writes
// values directly from the raw typed backing slice when possible.
func makeColEncoder(arr arrow.Array) colEncoder {
	switch a := arr.(type) {
	case *array.Int8:
		vals := a.Int8Values()

		return withNullGuard(a, func(buf *bytes.Buffer, row int) {
			buf.WriteByte(1)
			buf.WriteByte(byte(vals[row]))
		})
	case *array.Int16:
		vals := a.Int16Values()

		return withNullGuard(a, func(buf *bytes.Buffer, row int) {
			buf.WriteByte(1)
			bufPutUint16(buf, uint16(vals[row]))
		})
	case *array.Int32:
		vals := a.Int32Values()

		return withNullGuard(a, func(buf *bytes.Buffer, row int) {
			buf.WriteByte(1)
			bufPutUint32(buf, uint32(vals[row]))
		})
	case *array.Int64:
		vals := a.Int64Values()

		return withNullGuard(a, func(buf *bytes.Buffer, row int) {
			buf.WriteByte(1)
			bufPutUint64(buf, uint64(vals[row]))
		})
	case *array.Float32:
		vals := a.Float32Values()

		return withNullGuard(a, func(buf *bytes.Buffer, row int) {
			buf.WriteByte(1)
			bufPutUint32(buf, math.Float32bits(vals[row]))
		})
	case *array.Float64:
		vals := a.Float64Values()

		return withNullGuard(a, func(buf *bytes.Buffer, row int) {
			buf.WriteByte(1)
			bufPutUint64(buf, math.Float64bits(vals[row]))
		})
	case *array.Date32:
		vals := a.Date32Values()

		return withNullGuard(a, func(buf *bytes.Buffer, row int) {
			buf.WriteByte(1)
			bufPutUint32(buf, uint32(vals[row]))
		})
	case *array.Date64:
		vals := a.Date64Values()

		return withNullGuard(a, func(buf *bytes.Buffer, row int) {
			buf.WriteByte(1)
			bufPutUint64(buf, uint64(vals[row]))
		})
	case *array.Time32:
		vals := a.Time32Values()

		return withNullGuard(a, func(buf *bytes.Buffer, row int) {
			buf.WriteByte(1)
			bufPutUint32(buf, uint32(vals[row]))
		})
	case *array.Time64:
		vals := a.Time64Values()

		return withNullGuard(a, func(buf *bytes.Buffer, row int) {
			buf.WriteByte(1)
			bufPutUint64(buf, uint64(vals[row]))
		})
	case *array.Timestamp:
		vals := a.TimestampValues()

		return withNullGuard(a, func(buf *bytes.Buffer, row int) {
			buf.WriteByte(1)
			bufPutUint64(buf, uint64(vals[row]))
		})
	case *array.String:
		offsets := a.ValueOffsets()

		rawBytes := a.ValueBytes()

		return withNullGuard(a, func(buf *bytes.Buffer, row int) {
			buf.WriteByte(1)
			start, end := offsets[row], offsets[row+1]
			bufPutUint32(buf, uint32(end-start))
			buf.Write(rawBytes[start:end])
		})
	case *array.LargeString:
		offsets := a.ValueOffsets()
		rawBytes := a.ValueBytes()

		return withNullGuard(a, func(buf *bytes.Buffer, row int) {
			buf.WriteByte(1)
			start, end := offsets[row], offsets[row+1]
			bufPutUint32(buf, uint32(end-start))
			buf.Write(rawBytes[start:end])
		})
	case *array.Binary:
		offsets := a.ValueOffsets()
		rawBytes := a.ValueBytes()

		return withNullGuard(a, func(buf *bytes.Buffer, row int) {
			buf.WriteByte(1)
			start, end := offsets[row], offsets[row+1]
			bufPutUint32(buf, uint32(end-start))
			buf.Write(rawBytes[start:end])
		})
	case *array.LargeBinary:
		offsets := a.ValueOffsets()
		rawBytes := a.ValueBytes()

		return withNullGuard(a, func(buf *bytes.Buffer, row int) {
			buf.WriteByte(1)
			start, end := offsets[row], offsets[row+1]
			bufPutUint32(buf, uint32(end-start))
			buf.Write(rawBytes[start:end])
		})
	case *array.Boolean:
		return withNullGuard(a, func(buf *bytes.Buffer, row int) {
			buf.WriteByte(1)
			if a.Value(row) {
				buf.WriteByte(1)
			} else {
				buf.WriteByte(0)
			}
		})
	case *array.FixedSizeBinary:
		return withNullGuard(a, func(buf *bytes.Buffer, row int) {
			buf.WriteByte(1)
			buf.Write(a.Value(row))
		})
	default:
		return withNullGuard(arr, func(buf *bytes.Buffer, row int) {
			encodeArrowValue(buf, arr, row)
		})
	}
}

// processEqualityDeletesColumnarForFile resolves field paths once per file and
// typed column encoders once per batch, then iterates rows without per-row type
// switches. Each delete set is applied independently because sets may have
// different field IDs.
func processEqualityDeletesColumnarForFile(ctx context.Context, eqDeleteSets []*equalityDeleteSet, fileSchema *iceberg.Schema, dataFilePath string) (recProcessFn, error) {
	fieldRefsByID := indexArrowFields(fileSchema)
	fieldRefs := make([][]arrowFieldRef, len(eqDeleteSets))
	for i, eqDel := range eqDeleteSets {
		if len(eqDel.fieldIDs) != len(eqDel.colNames) {
			return nil, fmt.Errorf("%w: equality delete set has %d field IDs and %d column names",
				iceberg.ErrInvalidArgument, len(eqDel.fieldIDs), len(eqDel.colNames))
		}

		fieldRefs[i] = make([]arrowFieldRef, len(eqDel.fieldIDs))
		for fieldIdx, fieldID := range eqDel.fieldIDs {
			ref, err := resolveArrowField(fieldRefsByID, fieldID, eqDel.colNames[fieldIdx], dataFilePath)
			if err != nil {
				return nil, err
			}

			fieldRefs[i][fieldIdx] = ref
		}
	}

	return func(r arrow.RecordBatch) (arrow.RecordBatch, error) {
		defer r.Release()

		mem := compute.GetAllocator(ctx)
		numRows := int(r.NumRows())

		maskBuf := memory.NewResizableBuffer(mem)
		defer maskBuf.Release()
		maskBuf.Resize(int(bitutil.BytesForBits(int64(numRows))))
		maskBytes := maskBuf.Bytes()

		for i := range maskBytes {
			maskBytes[i] = 0xFF
		}

		var keyBuf bytes.Buffer

		for setIdx, eqDel := range eqDeleteSets {
			encoders := make([]colEncoder, len(eqDel.colNames))
			for i, name := range eqDel.colNames {
				var err error
				encoders[i], err = makeArrowFieldEncoder(r, fieldRefs[setIdx][i], eqDel.fieldIDs[i], name, dataFilePath)
				if err != nil {
					return nil, err
				}
			}

			for row := 0; row < numRows; row++ {
				if !bitutil.BitIsSet(maskBytes, row) {
					continue
				}

				keyBuf.Reset()

				for _, enc := range encoders {
					enc(&keyBuf, row)
				}

				if _, deleted := eqDel.keys[bufString(&keyBuf)]; deleted {
					bitutil.ClearBit(maskBytes, row)
				}
			}
		}

		mask := array.NewBooleanData(array.NewData(
			arrow.FixedWidthTypes.Boolean, numRows,
			[]*memory.Buffer{nil, maskBuf}, nil, 0, 0))
		defer mask.Release()

		filtered, err := compute.Filter(ctx,
			compute.NewDatumWithoutOwning(r),
			compute.NewDatumWithoutOwning(mask),
			*compute.DefaultFilterOptions())
		if err != nil {
			return nil, err
		}

		return filtered.(*compute.RecordDatum).Value, nil
	}, nil
}
