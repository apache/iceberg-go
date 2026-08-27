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

	"github.com/apache/arrow-go/v18/parquet"
	"github.com/apache/arrow-go/v18/parquet/metadata"
	parquetschema "github.com/apache/arrow-go/v18/parquet/schema"
	"github.com/apache/iceberg-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func buildRowGroupMetricsMetadata(t testing.TB, rowGroups, columns int, withStats bool) *metadata.FileMetaData {
	t.Helper()
	fields := make(parquetschema.FieldList, columns)
	for i := range fields {
		fields[i] = parquetschema.NewByteArrayNode(fmt.Sprintf("field_%d", i), parquet.Repetitions.Required, int32(i+1))
	}
	root, err := parquetschema.NewGroupNode("schema", parquet.Repetitions.Required, fields, -1)
	if err != nil {
		t.Fatal(err)
	}

	builder := metadata.NewFileMetadataBuilder(parquetschema.NewSchema(root), parquet.NewWriterProperties(), nil)
	for rowGroup := range rowGroups {
		rg := builder.AppendRowGroup()
		rg.SetNumRows(1)
		for column := range columns {
			chunk := rg.NextColumnChunk()
			if withStats {
				var stats metadata.EncodedStatistics
				stats.SetMin([]byte("a"))
				stats.SetMax([]byte("z"))
				stats.SetNullCount(0)
				chunk.SetStats(stats)
			}
			if err := chunk.Finish(metadata.ChunkMetaInfo{
				NumValues:        1,
				DataPageOffset:   int64(100 + rowGroup*columns + column),
				IndexPageOffset:  -1,
				CompressedSize:   8,
				UncompressedSize: 8,
			}, false, false, metadata.EncodingStats{}); err != nil {
				t.Fatal(err)
			}
		}
		if err := rg.Finish(int64(columns*8), int16(rowGroup)); err != nil {
			t.Fatal(err)
		}
	}

	meta, err := builder.Finish()
	if err != nil {
		t.Fatal(err)
	}

	return meta
}

func TestInclusiveMetricsEvalRowGroupMetricsLifecycle(t *testing.T) {
	withStats := buildRowGroupMetricsMetadata(t, 1, 2, true)
	withoutStats := buildRowGroupMetricsMetadata(t, 1, 2, false)
	eval := &inclusiveMetricsEval{expr: iceberg.AlwaysTrue{}}

	keep, err := eval.TestRowGroup(withoutStats.RowGroup(0), []int{0, 1})
	require.NoError(t, err)
	assert.True(t, keep)
	assert.NotNil(t, eval.valueCounts)
	assert.Nil(t, eval.nullCounts)
	assert.Nil(t, eval.lowerBounds)
	assert.Nil(t, eval.upperBounds)

	keep, err = eval.TestRowGroup(withStats.RowGroup(0), []int{0, 1})
	require.NoError(t, err)
	assert.True(t, keep)
	assert.Len(t, eval.valueCounts, 2)
	assert.Len(t, eval.nullCounts, 2)
	assert.Len(t, eval.lowerBounds, 2)
	assert.Len(t, eval.upperBounds, 2)

	keep, err = eval.TestRowGroup(withoutStats.RowGroup(0), []int{0, 1})
	require.NoError(t, err)
	assert.True(t, keep)
	assert.Empty(t, eval.valueCounts)
	assert.Empty(t, eval.nullCounts)
	assert.Empty(t, eval.lowerBounds)
	assert.Empty(t, eval.upperBounds)
}
