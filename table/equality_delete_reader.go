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
	"fmt"
	"math"
	"unsafe"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/bitutil"
	"github.com/apache/arrow-go/v18/arrow/compute"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/iceberg-go"
	iceinternal "github.com/apache/iceberg-go/internal"
	iceio "github.com/apache/iceberg-go/io"
	"github.com/apache/iceberg-go/table/internal"
	"golang.org/x/sync/errgroup"
)

// equalityDeleteSet holds the set of delete keys and the column names
// used to look them up in data records. Each set corresponds to one
// group of equality field IDs — delete files with different field IDs
// produce separate sets.
type equalityDeleteSet struct {
	keys     set[string]
	fieldIDs []int
	colNames []string
}

// readAllEqualityDeleteFiles reads all unique equality delete files from
// the tasks and builds per-task delete key sets. Returns nil if there are
// no equality deletes. Delete files with different equality field IDs are
// kept as separate sets (not merged).
func readAllEqualityDeleteFiles(ctx context.Context, fs iceio.IO, schema *iceberg.Schema, tasks []FileScanTask, concurrency int) (map[int][]*equalityDeleteSet, error) {
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
				keys, colNames, err := readEqualityDeleteFile(ctx, fs, schema, info.file, info.fieldIDs)
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
func readEqualityDeleteFile(ctx context.Context, fs iceio.IO, tableSchema *iceberg.Schema, dataFile iceberg.DataFile, fieldIDs []int) (set[string], []string, error) {
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

	// Resolve column names from field IDs.
	colNames := make([]string, len(fieldIDs))
	colIndices := make([]int, len(fieldIDs))

	for i, fid := range fieldIDs {
		name, ok := tableSchema.FindColumnName(fid)
		if !ok {
			return nil, nil, fmt.Errorf("equality delete field ID %d not found in table schema", fid)
		}

		colNames[i] = name
		indices := tbl.Schema().FieldIndices(name)
		if len(indices) == 0 {
			return nil, nil, fmt.Errorf("equality delete column %q not found in delete file %s", name, dataFile.FilePath())
		}

		colIndices[i] = indices[0]
	}

	// Build the set of encoded delete keys.
	keys := make(set[string])
	numRows := int(tbl.NumRows())

	var keyBuf bytes.Buffer

	for row := 0; row < numRows; row++ {
		keyBuf.Reset()
		encodeDeleteKeyTo(&keyBuf, tbl, colIndices, row)
		keys[keyBuf.String()] = struct{}{}
	}

	return keys, colNames, nil
}

// encodeDeleteKeyTo encodes the values at the given row from the specified
// columns into the provided buffer for hash-based lookup.
func encodeDeleteKeyTo(buf *bytes.Buffer, tbl arrow.Table, colIndices []int, row int) {
	for _, colIdx := range colIndices {
		col := tbl.Column(colIdx).Data()
		chunkRow := row

		for _, chunk := range col.Chunks() {
			if chunkRow < chunk.Len() {
				encodeArrowValue(buf, chunk, chunkRow)

				break
			}

			chunkRow -= chunk.Len()
		}
	}
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

// processEqualityDeletes returns a pipeline function that filters out
// rows whose equality key columns match any entry in the delete sets.
// Each set is applied independently (they may have different field IDs).
func processEqualityDeletes(ctx context.Context, eqDeleteSets []*equalityDeleteSet) (recProcessFn, error) {
	return processEqualityDeletesColumnar(ctx, eqDeleteSets)
}

// colEncoder writes the value at row idx to buf. Resolved once per column
// to avoid per-row type switches.
type colEncoder func(buf *bytes.Buffer, row int)

// makeColEncoder returns a colEncoder for the given Arrow array that writes
// values directly from the raw typed backing slice when possible.
func makeColEncoder(arr arrow.Array) colEncoder {
	switch a := arr.(type) {
	case *array.Int8:
		vals := a.Int8Values()

		return func(buf *bytes.Buffer, row int) {
			buf.WriteByte(1)
			buf.WriteByte(byte(vals[row]))
		}
	case *array.Int16:
		vals := a.Int16Values()

		return func(buf *bytes.Buffer, row int) {
			buf.WriteByte(1)
			bufPutUint16(buf, uint16(vals[row]))
		}
	case *array.Int32:
		vals := a.Int32Values()

		return func(buf *bytes.Buffer, row int) {
			buf.WriteByte(1)
			bufPutUint32(buf, uint32(vals[row]))
		}
	case *array.Int64:
		vals := a.Int64Values()

		return func(buf *bytes.Buffer, row int) {
			buf.WriteByte(1)
			bufPutUint64(buf, uint64(vals[row]))
		}
	case *array.Float32:
		vals := a.Float32Values()

		return func(buf *bytes.Buffer, row int) {
			buf.WriteByte(1)
			bufPutUint32(buf, math.Float32bits(vals[row]))
		}
	case *array.Float64:
		vals := a.Float64Values()

		return func(buf *bytes.Buffer, row int) {
			buf.WriteByte(1)
			bufPutUint64(buf, math.Float64bits(vals[row]))
		}
	case *array.Date32:
		vals := a.Date32Values()

		return func(buf *bytes.Buffer, row int) {
			buf.WriteByte(1)
			bufPutUint32(buf, uint32(vals[row]))
		}
	case *array.Date64:
		vals := a.Date64Values()

		return func(buf *bytes.Buffer, row int) {
			buf.WriteByte(1)
			bufPutUint64(buf, uint64(vals[row]))
		}
	case *array.Time32:
		vals := a.Time32Values()

		return func(buf *bytes.Buffer, row int) {
			buf.WriteByte(1)
			bufPutUint32(buf, uint32(vals[row]))
		}
	case *array.Time64:
		vals := a.Time64Values()

		return func(buf *bytes.Buffer, row int) {
			buf.WriteByte(1)
			bufPutUint64(buf, uint64(vals[row]))
		}
	case *array.Timestamp:
		vals := a.TimestampValues()

		return func(buf *bytes.Buffer, row int) {
			buf.WriteByte(1)
			bufPutUint64(buf, uint64(vals[row]))
		}
	case *array.String:
		offsets := a.ValueOffsets()

		rawBytes := a.ValueBytes()

		return func(buf *bytes.Buffer, row int) {
			if a.IsNull(row) {
				buf.WriteByte(0)

				return
			}

			buf.WriteByte(1)
			start, end := offsets[row], offsets[row+1]
			bufPutUint32(buf, uint32(end-start))
			buf.Write(rawBytes[start:end])
		}
	case *array.LargeString:
		offsets := a.ValueOffsets()
		rawBytes := a.ValueBytes()

		return func(buf *bytes.Buffer, row int) {
			if a.IsNull(row) {
				buf.WriteByte(0)

				return
			}

			buf.WriteByte(1)
			start, end := offsets[row], offsets[row+1]
			bufPutUint32(buf, uint32(end-start))
			buf.Write(rawBytes[start:end])
		}
	case *array.Binary:
		offsets := a.ValueOffsets()
		rawBytes := a.ValueBytes()

		return func(buf *bytes.Buffer, row int) {
			if a.IsNull(row) {
				buf.WriteByte(0)

				return
			}

			buf.WriteByte(1)
			start, end := offsets[row], offsets[row+1]
			bufPutUint32(buf, uint32(end-start))
			buf.Write(rawBytes[start:end])
		}
	case *array.LargeBinary:
		offsets := a.ValueOffsets()
		rawBytes := a.ValueBytes()

		return func(buf *bytes.Buffer, row int) {
			if a.IsNull(row) {
				buf.WriteByte(0)

				return
			}

			buf.WriteByte(1)
			start, end := offsets[row], offsets[row+1]
			bufPutUint32(buf, uint32(end-start))
			buf.Write(rawBytes[start:end])
		}
	case *array.Boolean:
		return func(buf *bytes.Buffer, row int) {
			if a.IsNull(row) {
				buf.WriteByte(0)

				return
			}

			buf.WriteByte(1)
			if a.Value(row) {
				buf.WriteByte(1)
			} else {
				buf.WriteByte(0)
			}
		}
	case *array.FixedSizeBinary:
		return func(buf *bytes.Buffer, row int) {
			if a.IsNull(row) {
				buf.WriteByte(0)

				return
			}

			buf.WriteByte(1)
			buf.Write(a.Value(row))
		}
	default:
		return func(buf *bytes.Buffer, row int) {
			encodeArrowValue(buf, arr, row)
		}
	}
}

// processEqualityDeletesColumnar resolves typed column encoders once per
// batch, then iterates rows without per-row type switches.
func processEqualityDeletesColumnar(ctx context.Context, eqDeleteSets []*equalityDeleteSet) (recProcessFn, error) {
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

		for _, eqDel := range eqDeleteSets {
			encoders := make([]colEncoder, len(eqDel.colNames))
			for i, name := range eqDel.colNames {
				indices := r.Schema().FieldIndices(name)
				if len(indices) == 0 {
					return nil, fmt.Errorf("equality delete column %q not found in data record", name)
				}

				encoders[i] = makeColEncoder(r.Column(indices[0]))
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
