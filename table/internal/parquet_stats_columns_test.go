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

package internal

import (
	"testing"

	"github.com/apache/arrow-go/v18/parquet"
	"github.com/apache/arrow-go/v18/parquet/metadata"
	"github.com/apache/arrow-go/v18/parquet/schema"
	"github.com/apache/iceberg-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func buildStatsColumnsMetadata(t testing.TB) *metadata.FileMetaData {
	t.Helper()

	details, err := schema.NewGroupNode("details", parquet.Repetitions.Optional, schema.FieldList{
		schema.NewFloat64Node("score", parquet.Repetitions.Optional, -1),
		schema.NewByteArrayNode("name", parquet.Repetitions.Optional, -1),
	}, -1)
	require.NoError(t, err)

	variantScore, err := schema.NewGroupNode("score", parquet.Repetitions.Optional, schema.FieldList{
		schema.NewByteArrayNode("value", parquet.Repetitions.Optional, -1),
		schema.NewFloat64Node("typed_value", parquet.Repetitions.Optional, -1),
	}, -1)
	require.NoError(t, err)

	variantCity, err := schema.NewGroupNode("city", parquet.Repetitions.Optional, schema.FieldList{
		schema.NewByteArrayNode("value", parquet.Repetitions.Optional, -1),
		schema.NewByteArrayNode("typed_value", parquet.Repetitions.Optional, -1),
	}, -1)
	require.NoError(t, err)

	variantTypedValue, err := schema.NewGroupNode("typed_value", parquet.Repetitions.Optional, schema.FieldList{
		variantScore,
		variantCity,
	}, -1)
	require.NoError(t, err)

	payload, err := schema.NewGroupNode("payload", parquet.Repetitions.Optional, schema.FieldList{
		schema.NewByteArrayNode("metadata", parquet.Repetitions.Optional, -1),
		schema.NewByteArrayNode("value", parquet.Repetitions.Optional, -1),
		variantTypedValue,
	}, -1)
	require.NoError(t, err)

	root, err := schema.NewGroupNode("schema", parquet.Repetitions.Required, schema.FieldList{
		schema.NewInt32Node("id", parquet.Repetitions.Required, -1),
		details,
		payload,
	}, -1)
	require.NoError(t, err)

	builder := metadata.NewFileMetadataBuilder(schema.NewSchema(root), parquet.NewWriterProperties(), nil)
	meta, err := builder.Finish()
	require.NoError(t, err)

	return meta
}

func TestResolveParquetStatsColumnsPreservesPathResolution(t *testing.T) {
	meta := buildStatsColumnsMetadata(t)
	statsCols := map[int]StatisticsCollector{
		1: {FieldID: 1, Mode: MetricsMode{Typ: MetricModeFull}, IcebergTyp: iceberg.PrimitiveTypes.Int32},
		2: {FieldID: 2, Mode: MetricsMode{Typ: MetricModeCounts}, IcebergTyp: iceberg.PrimitiveTypes.Float64},
		3: {FieldID: 3, Mode: MetricsMode{Typ: MetricModeTruncate, Len: 8}, IcebergTyp: iceberg.PrimitiveTypes.String},
		4: {FieldID: 4, Mode: MetricsMode{Typ: MetricModeFull}},
	}
	colMapping := map[string]int{
		"id":            1,
		"details.score": 2,
		"details.name":  3,
		"payload":       4,
	}
	variantFieldIDs := map[int]struct{}{4: {}}

	columns := resolveParquetStatsColumns(meta, statsCols, colMapping, variantFieldIDs)
	require.Len(t, columns, 9)

	wantPaths := []string{
		"id",
		"details.score",
		"details.name",
		"payload.metadata",
		"payload.value",
		"payload.typed_value.score.value",
		"payload.typed_value.score.typed_value",
		"payload.typed_value.city.value",
		"payload.typed_value.city.typed_value",
	}
	for pos, wantPath := range wantPaths {
		assert.Equal(t, wantPath, columns[pos].path, "physical column %d path", pos)
		assert.Nil(t, columns[pos].resolveErr, "physical column %d should resolve", pos)
	}

	assert.Equal(t, 1, columns[0].fieldID)
	assert.Equal(t, 2, columns[1].fieldID)
	assert.Equal(t, 3, columns[2].fieldID)
	assert.Equal(t, MetricModeFull, columns[0].statsCol.Mode.Typ)
	assert.Equal(t, MetricModeCounts, columns[1].statsCol.Mode.Typ)
	assert.Equal(t, MetricModeTruncate, columns[2].statsCol.Mode.Typ)
	for _, column := range columns[3:] {
		assert.True(t, column.variantChild, "variant child %q should not require an Iceberg field ID", column.path)
	}
}

func TestResolveParquetStatsColumnsRetainsMissingPathError(t *testing.T) {
	meta := buildStatsColumnsMetadata(t)
	columns := resolveParquetStatsColumns(meta, nil, map[string]int{
		"id":      1,
		"payload": 4,
	}, map[int]struct{}{4: {}})

	require.Len(t, columns, 9)
	assert.ErrorContains(t, columns[1].resolveErr, `column chunk "details.score" not found in column mapping`)
	assert.ErrorContains(t, columns[2].resolveErr, `column chunk "details.name" not found in column mapping`)
	for _, column := range columns[3:] {
		assert.True(t, column.variantChild)
		assert.NoError(t, column.resolveErr)
	}
}

func TestResolveParquetStatsColumnsHandlesMissingMetricsPlan(t *testing.T) {
	t.Run("reserved row-lineage column is skipped", func(t *testing.T) {
		meta := buildStatsColumnsMetadata(t)
		columns := resolveParquetStatsColumns(meta, nil, map[string]int{
			"id": iceberg.RowIDFieldID,
		}, nil)

		require.Len(t, columns, 9)
		assert.Equal(t, iceberg.RowIDFieldID, columns[0].fieldID)
		assert.True(t, columns[0].skipStats)
		assert.NoError(t, columns[0].resolveErr)
	})

	t.Run("non-reserved column retains metrics plan error", func(t *testing.T) {
		meta := buildStatsColumnsMetadata(t)
		columns := resolveParquetStatsColumns(meta, nil, map[string]int{
			"id": 1,
		}, nil)

		require.Len(t, columns, 9)
		assert.False(t, columns[0].skipStats)
		assert.ErrorContains(t, columns[0].resolveErr, `field id 1 (column "id") not found in the metrics plan`)
	})
}
