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
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/iceberg-go"
	"github.com/stretchr/testify/require"
)

// TestPositionDeltaWriter_BuildUnifiedIterator_AbortNoDoubleRelease verifies
// that buildUnifiedIterator does not release batches it has already yielded
// when the consumer aborts mid-stream.
//
// The releasing wrapper inside [WriteRecords] (write_records.go) owns the
// release on both the success and the early-stop paths. If the producer also
// releases on !yield, refcount would go negative — undefined behavior in
// Arrow that happy-path tests miss because no IO error ever fires the abort
// branch. This test simulates that wrapper contract directly on both the
// reinsert path and the insert (enriched) path.
func TestPositionDeltaWriter_BuildUnifiedIterator_AbortNoDoubleRelease(t *testing.T) {
	cases := []struct {
		name         string
		numReinserts int
		numInserts   int
		// abortAfter yields are consumed and released by the test, then the
		// loop breaks — triggering the producer's !yield branch on whichever
		// phase the next yield would have come from.
		abortAfter int
		abortPhase string
	}{
		{name: "abort_on_reinsert", numReinserts: 2, numInserts: 1, abortAfter: 1, abortPhase: "reinsert"},
		{name: "abort_on_insert", numReinserts: 0, numInserts: 2, abortAfter: 1, abortPhase: "insert"},
		{name: "abort_at_phase_boundary", numReinserts: 1, numInserts: 1, abortAfter: 1, abortPhase: "reinsert"},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			mem := memory.NewCheckedAllocator(memory.NewGoAllocator())
			defer mem.AssertSize(t, 0)

			w := &PositionDeltaWriter{}

			reinserts := make([]arrow.RecordBatch, 0, tc.numReinserts)
			for i := 0; i < tc.numReinserts; i++ {
				b := buildIntPlusRowIDBatch(mem, []int64{int64(i)}, []int64{int64(1<<40 + i)})
				reinserts = append(reinserts, b)
				require.NoError(t, w.Reinsert(b))
			}
			inserts := make([]arrow.RecordBatch, 0, tc.numInserts)
			for i := 0; i < tc.numInserts; i++ {
				b := buildIntBatch(mem, []int64{int64(100 + i)})
				inserts = append(inserts, b)
				require.NoError(t, w.Insert(b))
			}
			defer func() {
				for _, b := range reinserts {
					b.Release()
				}
				for _, b := range inserts {
					b.Release()
				}
			}()

			records := w.buildUnifiedIterator()
			count := 0
			for rec, err := range records {
				require.NoError(t, err)
				rec.Release()
				count++
				if count >= tc.abortAfter {
					break
				}
			}
			require.Equal(t, tc.abortAfter, count, "unexpected yield count for %s", tc.abortPhase)

			// Run the slice-release deferred work that Close() would do, so
			// the CheckedAllocator's AssertSize(0) check is meaningful.
			for _, b := range w.reinsertBatches {
				b.Release()
			}
			for _, b := range w.insertBatches {
				b.Release()
			}
		})
	}
}

// TestPositionDeltaWriter_AppendNullRowIDColumn_NoSourceAliasing verifies that
// appendNullRowIDColumn does not mutate the source batch's internal field /
// column slices when those slices have spare capacity. A naive append into a
// shared backing array would silently corrupt the caller's batch.
func TestPositionDeltaWriter_AppendNullRowIDColumn_NoSourceAliasing(t *testing.T) {
	mem := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer mem.AssertSize(t, 0)

	// Build a batch whose schema fields and column slice both have capacity
	// > length, so a plain append would write into the shared backing array.
	idBldr := array.NewInt64Builder(mem)
	defer idBldr.Release()
	idBldr.AppendValues([]int64{1, 2}, nil)
	idArr := idBldr.NewArray()
	defer idArr.Release()

	srcCols := append(make([]arrow.Array, 0, 4), idArr)
	srcFields := append(make([]arrow.Field, 0, 4), arrow.Field{
		Name: "id", Type: arrow.PrimitiveTypes.Int64, Nullable: false,
	})
	srcSchema := arrow.NewSchema(srcFields, nil)
	src := array.NewRecordBatch(srcSchema, srcCols, 2)
	defer src.Release()

	enriched, err := appendNullRowIDColumn(mem, src)
	require.NoError(t, err)
	defer enriched.Release()

	require.Equal(t, 1, src.Schema().NumFields(),
		"appendNullRowIDColumn must not mutate source schema field count")
	require.Equal(t, 1, len(src.Columns()),
		"appendNullRowIDColumn must not mutate source columns")
	require.Equal(t, 2, enriched.Schema().NumFields())
	require.Equal(t, iceberg.RowIDColumnName, enriched.Schema().Field(1).Name)

	fieldIDStr, ok := enriched.Schema().Field(1).Metadata.GetValue(ArrowParquetFieldIDKey)
	require.True(t, ok, "appended _row_id field must carry %s metadata", ArrowParquetFieldIDKey)
	require.Equal(t, "2147483540", fieldIDStr)
}

func buildIntPlusRowIDBatch(mem memory.Allocator, ids, rowIDs []int64) arrow.RecordBatch {
	schema := arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int64, Nullable: false},
		{Name: iceberg.RowIDColumnName, Type: arrow.PrimitiveTypes.Int64, Nullable: true},
	}, nil)
	idBldr := array.NewInt64Builder(mem)
	defer idBldr.Release()
	idBldr.AppendValues(ids, nil)
	rowIDBldr := array.NewInt64Builder(mem)
	defer rowIDBldr.Release()
	rowIDBldr.AppendValues(rowIDs, nil)
	idArr := idBldr.NewArray()
	defer idArr.Release()
	rowIDArr := rowIDBldr.NewArray()
	defer rowIDArr.Release()

	return array.NewRecordBatch(schema, []arrow.Array{idArr, rowIDArr}, int64(len(ids)))
}

func buildIntBatch(mem memory.Allocator, ids []int64) arrow.RecordBatch {
	schema := arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int64, Nullable: false},
	}, nil)
	idBldr := array.NewInt64Builder(mem)
	defer idBldr.Release()
	idBldr.AppendValues(ids, nil)
	idArr := idBldr.NewArray()
	defer idArr.Release()

	return array.NewRecordBatch(schema, []arrow.Array{idArr}, int64(len(ids)))
}
