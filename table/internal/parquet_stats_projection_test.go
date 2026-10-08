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

package internal_test

import (
	"bytes"
	"context"
	"testing"

	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/parquet"
	"github.com/apache/arrow-go/v18/parquet/file"
	"github.com/apache/arrow-go/v18/parquet/metadata"
	"github.com/apache/arrow-go/v18/parquet/schema"
	"github.com/apache/iceberg-go/table/internal"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestParquetRowGroupStatsUseRequestedFieldIDs(t *testing.T) {
	data := buildMultiColumnDictionaryTestParquet(t, multiColumnDictionaryTestRowGroup{
		ids:        []int32{1},
		categories: []string{"a"},
	})

	for _, test := range []struct {
		name     string
		fieldIDs []int
		wantCols []int
	}{
		{name: "nil uses read columns", wantCols: []int{0, 1}},
		{name: "filter column only", fieldIDs: []int{2}, wantCols: []int{1}},
		{name: "no filter columns", fieldIDs: []int{}, wantCols: []int{}},
		{name: "missing field", fieldIDs: []int{999}, wantCols: []int{}},
	} {
		t.Run(test.name, func(t *testing.T) {
			rdr := openBloomTestReader(t, data)
			defer rdr.Close()

			var calls [][]int
			tester := &internal.ParquetRowGroupTester{
				StatsFn: func(_ *metadata.RowGroupMetaData, cols []int) (bool, error) {
					calls = append(calls, append([]int{}, cols...))

					return true, nil
				},
				StatsFieldIDs: test.fieldIDs,
			}
			rr, err := rdr.GetRecords(context.Background(), []int{0, 1}, tester)
			require.NoError(t, err)
			assert.Equal(t, int64(1), countRecords(t, rr))
			require.Len(t, calls, 1)
			assert.Equal(t, test.wantCols, calls[0])
		})
	}
}

func TestParquetRowGroupStatsPruneUsingUnprojectedColumn(t *testing.T) {
	data := buildMultiColumnDictionaryTestParquet(t,
		multiColumnDictionaryTestRowGroup{ids: []int32{9}, categories: []string{"drop"}},
		multiColumnDictionaryTestRowGroup{ids: []int32{42}, categories: []string{"keep"}},
	)
	rdr := openBloomTestReader(t, data)
	defer rdr.Close()

	var survivors []internal.RowGroupSpan
	var calls int
	tester := &internal.ParquetRowGroupTester{
		StatsFieldIDs: []int{2},
		Survivors:     &survivors,
		StatsFn: func(rg *metadata.RowGroupMetaData, cols []int) (bool, error) {
			calls++
			require.Equal(t, []int{1}, cols, "stats must use the filter column, independent of projection")
			column, err := rg.ColumnChunk(cols[0])
			require.NoError(t, err)
			stats, err := column.Statistics()
			require.NoError(t, err)
			require.NotNil(t, stats)

			return bytes.Equal(stats.EncodeMin(), []byte("keep")), nil
		},
	}
	rr, err := rdr.GetRecords(t.Context(), []int{0}, tester)
	require.NoError(t, err)
	defer rr.Release()

	require.True(t, rr.Next())
	record := rr.RecordBatch()
	require.Equal(t, int64(1), record.NumCols(), "pruning must not add the filter column to the output")
	assert.Equal(t, "id", record.Schema().Field(0).Name)
	assert.Equal(t, []int32{42}, record.Column(0).(*array.Int32).Int32Values())
	assert.False(t, rr.Next())
	require.NoError(t, rr.Err())
	assert.Equal(t, 2, calls)
	assert.Equal(t, []internal.RowGroupSpan{{FirstRowPos: 1, NumRows: 1}}, survivors)
}

func TestParquetRowGroupStatsUseNestedFieldIDs(t *testing.T) {
	before := schema.NewInt32Node("before", parquet.Repetitions.Required, 1)
	target := schema.NewInt32Node("target", parquet.Repetitions.Required, 3)
	other := schema.NewInt32Node("other", parquet.Repetitions.Required, 4)
	nested, err := schema.NewGroupNode("nested", parquet.Repetitions.Required,
		schema.FieldList{target, other}, 2)
	require.NoError(t, err)
	after := schema.NewInt32Node("after", parquet.Repetitions.Required, 5)
	root, err := schema.NewGroupNode("schema", parquet.Repetitions.Required,
		schema.FieldList{before, nested, after}, -1)
	require.NoError(t, err)

	var buf bytes.Buffer
	writer := file.NewParquetWriter(&buf, root,
		file.WithWriterProps(parquet.NewWriterProperties(parquet.WithStats(true))))
	rg, err := writer.AppendRowGroupChecked()
	require.NoError(t, err)
	for _, value := range []int32{10, 20, 30, 40} {
		column, err := rg.NextColumn()
		require.NoError(t, err)
		_, err = column.(*file.Int32ColumnChunkWriter).WriteBatch([]int32{value}, nil, nil)
		require.NoError(t, err)
		require.NoError(t, column.Close())
	}
	require.NoError(t, rg.Close())
	require.NoError(t, writer.Close())

	rdr := openBloomTestReader(t, buf.Bytes())
	defer rdr.Close()

	var calls [][]int
	tester := &internal.ParquetRowGroupTester{
		StatsFn: func(_ *metadata.RowGroupMetaData, cols []int) (bool, error) {
			calls = append(calls, append([]int{}, cols...))

			return true, nil
		},
		StatsFieldIDs: []int{4},
	}
	rr, err := rdr.GetRecords(context.Background(), []int{0, 1, 2, 3}, tester)
	require.NoError(t, err)
	assert.Equal(t, int64(1), countRecords(t, rr))
	require.Len(t, calls, 1)
	assert.Equal(t, []int{2}, calls[0])
}

func TestParquetRowGroupStatsShareMappingWithBloomAndDictionaryPruning(t *testing.T) {
	tests := []struct {
		name       string
		bloom      bool
		dictionary bool
	}{
		{name: "stats only"},
		{name: "stats and bloom", bloom: true},
		{name: "stats and dictionary", dictionary: true},
		{name: "stats and both", bloom: true, dictionary: true},
	}
	data := buildBloomTestParquet(t, 1)
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			rdr := openBloomTestReader(t, data)
			defer rdr.Close()

			var calls [][]int
			tester := &internal.ParquetRowGroupTester{
				StatsFn: func(_ *metadata.RowGroupMetaData, cols []int) (bool, error) {
					calls = append(calls, append([]int{}, cols...))

					return true, nil
				},
				StatsFieldIDs: []int{1},
			}
			if test.bloom {
				tester.BloomPreds = []internal.RowGroupBloomPred{{
					FieldID: 1,
					PhysBytes: [][]byte{
						int32PhysBytes(1),
						int32PhysBytes(2),
					},
				}}
			}
			if test.dictionary {
				tester.DictionaryPreds = []internal.RowGroupDictionaryPred{{
					FieldID: 1,
					PhysBytes: [][]byte{
						int32PhysBytes(1),
						int32PhysBytes(2),
					},
				}}
			}

			rr, err := rdr.GetRecords(context.Background(), []int{0}, tester)
			require.NoError(t, err)
			assert.Equal(t, int64(2), countRecords(t, rr))
			require.Len(t, calls, 2)
			assert.Equal(t, []int{0}, calls[0])
			assert.Equal(t, []int{0}, calls[1])
		})
	}
}
