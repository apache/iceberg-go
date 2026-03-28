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
	"encoding/binary"
	"fmt"
	"math"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/compute"
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

	for row := 0; row < numRows; row++ {
		key := encodeDeleteKey(tbl, colIndices, row)
		keys[key] = struct{}{}
	}

	return keys, colNames, nil
}

// encodeDeleteKey encodes the values at the given row from the specified
// columns into a single string key for hash-based lookup.
func encodeDeleteKey(tbl arrow.Table, colIndices []int, row int) string {
	var buf bytes.Buffer

	for _, colIdx := range colIndices {
		col := tbl.Column(colIdx).Data()
		chunkRow := row

		for _, chunk := range col.Chunks() {
			if chunkRow < chunk.Len() {
				encodeArrowValue(&buf, chunk, chunkRow)

				break
			}

			chunkRow -= chunk.Len()
		}
	}

	return buf.String()
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
		_ = binary.Write(buf, binary.BigEndian, a.Value(idx))
	case *array.Int32:
		_ = binary.Write(buf, binary.BigEndian, a.Value(idx))
	case *array.Int64:
		_ = binary.Write(buf, binary.BigEndian, a.Value(idx))
	case *array.Float32:
		_ = binary.Write(buf, binary.BigEndian, math.Float32bits(a.Value(idx)))
	case *array.Float64:
		_ = binary.Write(buf, binary.BigEndian, math.Float64bits(a.Value(idx)))
	case *array.String:
		s := a.Value(idx)
		_ = binary.Write(buf, binary.BigEndian, int32(len(s)))
		buf.WriteString(s)
	case *array.LargeString:
		s := a.Value(idx)
		_ = binary.Write(buf, binary.BigEndian, int32(len(s)))
		buf.WriteString(s)
	case *array.Binary:
		b := a.Value(idx)
		_ = binary.Write(buf, binary.BigEndian, int32(len(b)))
		buf.Write(b)
	case *array.LargeBinary:
		b := a.Value(idx)
		_ = binary.Write(buf, binary.BigEndian, int32(len(b)))
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
		_ = binary.Write(buf, binary.BigEndian, a.Value(idx))
	case *array.Date64:
		_ = binary.Write(buf, binary.BigEndian, a.Value(idx))
	case *array.Time32:
		_ = binary.Write(buf, binary.BigEndian, a.Value(idx))
	case *array.Time64:
		_ = binary.Write(buf, binary.BigEndian, a.Value(idx))
	case *array.Timestamp:
		_ = binary.Write(buf, binary.BigEndian, a.Value(idx))
	default:
		// Fallback: length-prefixed string representation.
		s := a.ValueStr(idx)
		_ = binary.Write(buf, binary.BigEndian, int32(len(s)))
		buf.WriteString(s)
	}
}

// processEqualityDeletes returns a pipeline function that filters out
// rows whose equality key columns match any entry in the delete sets.
// Each set is applied independently (they may have different field IDs).
func processEqualityDeletes(ctx context.Context, eqDeleteSets []*equalityDeleteSet) (recProcessFn, error) {
	// Pre-resolve column names for each set — these will be looked up
	// once per record batch below.
	return func(r arrow.RecordBatch) (arrow.RecordBatch, error) {
		defer r.Release()

		mem := compute.GetAllocator(ctx)
		numRows := int(r.NumRows())

		// Start with all rows kept.
		keep := make([]bool, numRows)
		for i := range keep {
			keep[i] = true
		}

		for _, eqDel := range eqDeleteSets {
			colIndices := make([]int, len(eqDel.colNames))
			for i, name := range eqDel.colNames {
				indices := r.Schema().FieldIndices(name)
				if len(indices) == 0 {
					return nil, fmt.Errorf("equality delete column %q not found in data record", name)
				}

				colIndices[i] = indices[0]
			}

			var keyBuf bytes.Buffer

			for row := 0; row < numRows; row++ {
				if !keep[row] {
					continue // already deleted by a previous set
				}

				keyBuf.Reset()

				for _, colIdx := range colIndices {
					encodeArrowValue(&keyBuf, r.Column(colIdx), row)
				}

				if _, deleted := eqDel.keys[keyBuf.String()]; deleted {
					keep[row] = false
				}
			}
		}

		bldr := array.NewBooleanBuilder(mem)
		defer bldr.Release()

		for _, k := range keep {
			bldr.Append(k)
		}

		mask := bldr.NewArray()
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
