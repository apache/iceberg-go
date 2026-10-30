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

	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/compute"
	"github.com/apache/arrow-go/v18/arrow/memory"
	tblutils "github.com/apache/iceberg-go/table/internal"
	"github.com/stretchr/testify/require"
)

func TestSynthesizeRowLineageFullyMaterialized(t *testing.T) {
	alloc := memory.NewCheckedAllocator(memory.DefaultAllocator)
	defer alloc.AssertSize(t, 0)
	batch := makeMaterializedLineageBatch(alloc, 4, -1, -1)
	defer batch.Release()

	firstRowID, sequence := int64(1_000), int64(10)
	task := FileScanTask{FirstRowID: &firstRowID, DataSequenceNumber: &sequence}
	source := &rowPositionSource{spans: []tblutils.RowGroupSpan{
		{FirstRowPos: 5, NumRows: 2},
		{FirstRowPos: 20, NumRows: 2},
	}}
	cursor := source.cursor()
	ctx := compute.WithAllocator(t.Context(), alloc)

	output, err := synthesizeRowLineageColumns(ctx, cursor, task, batch, true, true)
	require.NoError(t, err)
	defer output.Release()

	// The caller owns an extra reference to the input batch, not a rebuild.
	require.Same(t, batch, output)
	require.Equal(t, int64(1_002), output.Column(0).(*array.Int64).Value(2))
	require.Equal(t, int64(12), output.Column(1).(*array.Int64).Value(2))
	require.Equal(t, int64(22), cursor.next()) // preserved position after a pruned group
}

func TestSynthesizeRowLineageOnlyRebuildsNullColumn(t *testing.T) {
	alloc := memory.NewCheckedAllocator(memory.DefaultAllocator)
	defer alloc.AssertSize(t, 0)
	batch := makeMaterializedLineageBatch(alloc, 4, 2, -1)
	defer batch.Release()

	firstRowID, sequence := int64(1_000), int64(10)
	task := FileScanTask{FirstRowID: &firstRowID, DataSequenceNumber: &sequence}
	source := &rowPositionSource{spans: []tblutils.RowGroupSpan{
		{FirstRowPos: 5, NumRows: 2},
		{FirstRowPos: 20, NumRows: 2},
	}}
	cursor := source.cursor()
	output, err := synthesizeRowLineageColumns(compute.WithAllocator(t.Context(), alloc), cursor, task, batch, true, true)
	require.NoError(t, err)
	defer output.Release()

	require.NotSame(t, batch, output)
	require.NotSame(t, batch.Column(0), output.Column(0))
	require.Same(t, batch.Column(1), output.Column(1))
	require.Equal(t, int64(1_020), output.Column(0).(*array.Int64).Value(2))
	require.Equal(t, int64(12), output.Column(1).(*array.Int64).Value(2))
	require.Equal(t, int64(22), cursor.next())
}

func TestRowPositionCursorAdvanceEquivalent(t *testing.T) {
	spans := [][]tblutils.RowGroupSpan{
		nil,
		{{FirstRowPos: 5, NumRows: 2}},
		{{FirstRowPos: 5, NumRows: 2}, {FirstRowPos: 20, NumRows: 3}},
		{{FirstRowPos: 0, NumRows: 0}, {FirstRowPos: 10, NumRows: 3}},
		{{FirstRowPos: 0, NumRows: 1}, {FirstRowPos: 10, NumRows: 0}, {FirstRowPos: 20, NumRows: 2}},
	}
	for _, groups := range spans {
		for _, counts := range [][]int64{{0, 1, 0, 2, 3}, {2, 2, 1, 12}, {100}} {
			src := &rowPositionSource{spans: groups}
			expected, actual := src.cursor(), src.cursor()
			for _, n := range counts {
				for range n {
					expected.next()
				}
				actual.advance(n)
				require.Equal(t, expected.spanIdx, actual.spanIdx, "spans=%v n=%d", groups, n)
				require.Equal(t, expected.consumed, actual.consumed, "spans=%v n=%d", groups, n)
			}
			require.Equal(t, expected.next(), actual.next(), "spans=%v counts=%v", groups, counts)
		}
	}
}
