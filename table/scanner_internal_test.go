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
	"runtime"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/compute"
	"github.com/apache/iceberg-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func newDataManifest(minSeqNum int64) iceberg.ManifestFile {
	return iceberg.NewManifestFile(2, "/path/to/manifest.avro", 1000, 0, 1).
		Content(iceberg.ManifestContentData).
		SequenceNum(minSeqNum, minSeqNum).
		Build()
}

func newDeleteManifest(minSeqNum int64) iceberg.ManifestFile {
	return iceberg.NewManifestFile(2, "/path/to/manifest.avro", 1000, 0, 1).
		Content(iceberg.ManifestContentDeletes).
		SequenceNum(minSeqNum, minSeqNum).
		Build()
}

func TestMinSequenceNum(t *testing.T) {
	tests := []struct {
		name      string
		manifests []iceberg.ManifestFile
		expected  int64
	}{
		{
			name:      "empty list returns 0",
			manifests: []iceberg.ManifestFile{},
			expected:  0,
		},
		{
			name: "single data manifest returns its sequence number",
			manifests: []iceberg.ManifestFile{
				newDataManifest(5),
			},
			expected: 5,
		},
		{
			name: "multiple data manifests returns minimum",
			manifests: []iceberg.ManifestFile{
				newDataManifest(10),
				newDataManifest(3),
				newDataManifest(7),
			},
			expected: 3,
		},
		{
			name: "only delete manifests returns 0",
			manifests: []iceberg.ManifestFile{
				newDeleteManifest(5),
				newDeleteManifest(10),
			},
			expected: 0,
		},
		{
			name: "mixed manifests only considers data manifests",
			manifests: []iceberg.ManifestFile{
				newDeleteManifest(1), // should be ignored
				newDataManifest(8),
				newDeleteManifest(2), // should be ignored
				newDataManifest(5),
			},
			expected: 5,
		},
		{
			name: "data manifest with sequence 0 is handled correctly",
			manifests: []iceberg.ManifestFile{
				newDataManifest(0),
				newDataManifest(5),
			},
			expected: 0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := minSequenceNum(tt.manifests)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestKeyDefaultMapRaceCondition(t *testing.T) {
	var factoryCallCount atomic.Int64
	factory := func(key string) int {
		factoryCallCount.Add(1)
		runtime.Gosched() // to widen the race window

		return 42
	}

	kdm := newKeyDefaultMap(factory)

	var wg sync.WaitGroup
	start := make(chan struct{})

	numGoroutines := 1000
	for range numGoroutines {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			_ = kdm.Get("same-key")
		}()
	}

	close(start)
	wg.Wait()

	callCount := factoryCallCount.Load()
	assert.Equal(t, int64(1), callCount,
		"factory should be called exactly once per key, but was called %d times", callCount)
}

func TestBuildPartitionProjectionWithInvalidSpecID(t *testing.T) {
	schema := iceberg.NewSchema(
		1,
		iceberg.NestedField{
			ID: 1, Name: "id",
			Type: iceberg.PrimitiveTypes.Int64, Required: true,
		},
	)

	metadata, err := NewMetadata(
		schema,
		iceberg.UnpartitionedSpec,
		UnsortedSortOrder,
		"s3://test-bucket/test_table",
		iceberg.Properties{},
	)
	require.NoError(t, err)

	scan := &Scan{
		metadata:      metadata,
		rowFilter:     iceberg.AlwaysTrue{},
		caseSensitive: true,
	}

	expr, err := scan.buildPartitionProjection(999)
	require.Error(t, err)
	assert.Nil(t, expr)
	assert.ErrorIs(t, err, ErrPartitionSpecNotFound)
	assert.ErrorContains(t, err, "id 999")
}

func TestBuildManifestEvaluatorWithInvalidSpecID(t *testing.T) {
	schema := iceberg.NewSchema(
		1,
		iceberg.NestedField{
			ID: 1, Name: "id",
			Type: iceberg.PrimitiveTypes.Int64, Required: true,
		},
	)

	metadata, err := NewMetadata(
		schema,
		iceberg.UnpartitionedSpec,
		UnsortedSortOrder,
		"s3://test-bucket/test_table",
		iceberg.Properties{},
	)
	require.NoError(t, err)

	scan := &Scan{
		metadata:      metadata,
		rowFilter:     iceberg.AlwaysTrue{},
		caseSensitive: true,
	}

	scan.partitionFilters = newKeyDefaultMapWrapErr(scan.buildPartitionProjection)

	evaluator, err := scan.buildManifestEvaluator(999)
	require.Error(t, err)
	assert.Nil(t, evaluator)
	assert.ErrorIs(t, err, ErrPartitionSpecNotFound)
	assert.ErrorContains(t, err, "id 999")
}

func TestBuildPartitionEvaluatorWithInvalidSpecID(t *testing.T) {
	schema := iceberg.NewSchema(
		1,
		iceberg.NestedField{
			ID: 1, Name: "id",
			Type: iceberg.PrimitiveTypes.Int64, Required: true,
		},
	)

	metadata, err := NewMetadata(
		schema,
		iceberg.UnpartitionedSpec,
		UnsortedSortOrder,
		"s3://test-bucket/test_table",
		iceberg.Properties{},
	)
	require.NoError(t, err)

	scan := &Scan{
		metadata:      metadata,
		rowFilter:     iceberg.AlwaysTrue{},
		caseSensitive: true,
	}

	evaluator, err := scan.buildPartitionEvaluator(999)
	require.Error(t, err)
	assert.Nil(t, evaluator)
	assert.ErrorIs(t, err, ErrPartitionSpecNotFound)
	assert.ErrorContains(t, err, "id 999")
}

// TestSynthesizeRowLineageColumns verifies that _row_id and _last_updated_sequence_number
// are filled from task constants when those columns are present and null.
func TestSynthesizeRowLineageColumns(t *testing.T) {
	ctx := context.Background()
	firstRowID := int64(1000)
	dataSeqNum := int64(5)
	task := FileScanTask{FirstRowID: &firstRowID, DataSequenceNumber: &dataSeqNum}
	rowOffset := int64(0)

	// Build a batch with a data column plus _row_id and _last_updated_sequence_number (all nulls).
	schema := arrow.NewSchema(
		[]arrow.Field{
			{Name: "x", Type: arrow.PrimitiveTypes.Int64, Nullable: true},
			{Name: iceberg.RowIDColumnName, Type: arrow.PrimitiveTypes.Int64, Nullable: true},
			{Name: iceberg.LastUpdatedSequenceNumberColumnName, Type: arrow.PrimitiveTypes.Int64, Nullable: true},
		},
		nil,
	)
	const nrows = 3
	xBldr := array.NewInt64Builder(compute.GetAllocator(ctx))
	defer xBldr.Release()
	xBldr.AppendValues([]int64{1, 2, 3}, nil)
	rowIDBldr := array.NewInt64Builder(compute.GetAllocator(ctx))
	defer rowIDBldr.Release()
	rowIDBldr.AppendNulls(nrows)
	seqBldr := array.NewInt64Builder(compute.GetAllocator(ctx))
	defer seqBldr.Release()
	seqBldr.AppendNulls(nrows)

	batch := array.NewRecordBatch(schema, []arrow.Array{
		xBldr.NewArray(),
		rowIDBldr.NewArray(),
		seqBldr.NewArray(),
	}, nrows)
	defer batch.Release()

	out, err := synthesizeRowLineageColumns(ctx, &rowOffset, task, batch, nil, false)
	require.NoError(t, err)
	defer out.Release()

	// _row_id should be 1000, 1001, 1002
	rowIDCol := out.Column(1).(*array.Int64)
	require.Equal(t, nrows, rowIDCol.Len())
	for i := 0; i < nrows; i++ {
		assert.False(t, rowIDCol.IsNull(i), "row %d", i)
		assert.EqualValues(t, 1000+int64(i), rowIDCol.Value(i), "row %d", i)
	}
	// _last_updated_sequence_number should be 5 for all
	seqCol := out.Column(2).(*array.Int64)
	for i := 0; i < nrows; i++ {
		assert.False(t, seqCol.IsNull(i), "row %d", i)
		assert.EqualValues(t, 5, seqCol.Value(i), "row %d", i)
	}
	assert.EqualValues(t, 3, rowOffset)
}
