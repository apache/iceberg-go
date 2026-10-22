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
	"fmt"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/compute"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/iceberg-go"
	tblutils "github.com/apache/iceberg-go/table/internal"
)

// makeMaterializedLineageBatch builds two reserved columns with optional nulls.
// A null index of -1 means the column is already fully populated.
func makeMaterializedLineageBatch(alloc memory.Allocator, rows, rowIDNullAt, seqNullAt int) arrow.RecordBatch {
	values := make([]int64, rows)
	sequences := make([]int64, rows)
	for i := range values {
		values[i] = 1_000 + int64(i)
		sequences[i] = 10 + int64(i)
	}

	build := func(values []int64, nullAt int) arrow.Array {
		var valid []bool
		if nullAt >= 0 {
			valid = make([]bool, len(values))
			for i := range valid {
				valid[i] = true
			}
			valid[nullAt] = false
		}
		builder := array.NewInt64Builder(alloc)
		defer builder.Release()
		builder.AppendValues(values, valid)

		return builder.NewArray()
	}

	rowID := build(values, rowIDNullAt)
	sequence := build(sequences, seqNullAt)
	schema := arrow.NewSchema([]arrow.Field{
		lineageArrowField(iceberg.RowIDColumnName, iceberg.RowIDFieldID),
		lineageArrowField(iceberg.LastUpdatedSequenceNumberColumnName, iceberg.LastUpdatedSequenceNumberFieldID),
	}, nil)
	batch := array.NewRecordBatch(schema, []arrow.Array{rowID, sequence}, int64(rows))
	rowID.Release()
	sequence.Release()

	return batch
}

func BenchmarkSynthesizeRowLineageColumns(b *testing.B) {
	for _, rows := range []int{1_024, 65_536} {
		for _, scenario := range []struct {
			name                 string
			rowNullAt, seqNullAt int
		}{
			{name: "complete", rowNullAt: -1, seqNullAt: -1},
			{name: "row_id_sparse", rowNullAt: rows / 2, seqNullAt: -1},
			{name: "both_sparse", rowNullAt: rows / 2, seqNullAt: rows / 4},
		} {
			b.Run(fmt.Sprintf("rows=%d/%s", rows, scenario.name), func(b *testing.B) {
				batch := makeMaterializedLineageBatch(memory.DefaultAllocator, rows, scenario.rowNullAt, scenario.seqNullAt)
				defer batch.Release()
				firstRowID, sequence := int64(1_000), int64(10)
				task := FileScanTask{FirstRowID: &firstRowID, DataSequenceNumber: &sequence}
				source := &rowPositionSource{spans: []tblutils.RowGroupSpan{
					{FirstRowPos: 0, NumRows: int64(rows / 2)},
					{FirstRowPos: int64(rows * 2), NumRows: int64(rows / 2)},
				}}
				ctx := compute.WithAllocator(b.Context(), memory.DefaultAllocator)
				b.ReportAllocs()
				b.ResetTimer()
				for b.Loop() {
					cursor := source.cursor()
					output, err := synthesizeRowLineageColumns(ctx, cursor, task, batch, true, true)
					if err != nil {
						b.Fatal(err)
					}
					output.Release()
				}
			})
		}
	}
}
