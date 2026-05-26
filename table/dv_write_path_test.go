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
	"errors"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/iceberg-go"
	iceio "github.com/apache/iceberg-go/io"
	"github.com/apache/iceberg-go/table/dv"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestDVWritePathProducesReadableOutput(t *testing.T) {
	mb := newPositionDeleteUnpartitionedMetadata(t, 3)
	fs := iceio.NewMemFS()

	batch := mustLoadRecordBatchFromJSON(PositionalDeleteArrowSchema, `[
		{"file_path": "s3://bucket/data/file-001.parquet", "pos": 1},
		{"file_path": "s3://bucket/data/file-001.parquet", "pos": 3},
		{"file_path": "s3://bucket/data/file-001.parquet", "pos": 5},
		{"file_path": "s3://bucket/data/file-002.parquet", "pos": 0},
		{"file_path": "s3://bucket/data/file-002.parquet", "pos": 10}
	]`)
	itr := func(yield func(arrow.RecordBatch, error) bool) {
		batch.Retain()
		yield(batch, nil)
	}

	seq := positionDeleteRecordsToDataFiles(context.Background(), "mem://test", mb, nil,
		recordWritingArgs{
			sc:  PositionalDeleteArrowSchema,
			itr: itr,
			fs:  fs,
		})

	var dataFiles []iceberg.DataFile
	for df, err := range seq {
		require.NoError(t, err)
		dataFiles = append(dataFiles, df)
	}

	require.Len(t, dataFiles, 2)

	df1 := dataFiles[0]
	assert.Equal(t, iceberg.PuffinFile, df1.FileFormat())
	assert.Equal(t, int64(3), df1.Count())
	assert.Equal(t, "s3://bucket/data/file-001.parquet", *df1.ReferencedDataFile())
	assert.NotNil(t, df1.ContentOffset())
	assert.NotNil(t, df1.ContentSizeInBytes())

	df2 := dataFiles[1]
	assert.Equal(t, iceberg.PuffinFile, df2.FileFormat())
	assert.Equal(t, int64(2), df2.Count())
	assert.Equal(t, "s3://bucket/data/file-002.parquet", *df2.ReferencedDataFile())

	bm1, err := dv.ReadDV(fs, df1)
	require.NoError(t, err)
	assert.Equal(t, int64(3), bm1.Cardinality())
	assert.True(t, bm1.Contains(1))
	assert.True(t, bm1.Contains(3))
	assert.True(t, bm1.Contains(5))
	assert.False(t, bm1.Contains(2))

	bm2, err := dv.ReadDV(fs, df2)
	require.NoError(t, err)
	assert.Equal(t, int64(2), bm2.Cardinality())
	assert.True(t, bm2.Contains(0))
	assert.True(t, bm2.Contains(10))
}

func TestDVWritePathV2UsesParquetPositionDeletes(t *testing.T) {
	mb := newPositionDeleteUnpartitionedMetadata(t, 2)

	batch := mustLoadRecordBatchFromJSON(PositionalDeleteArrowSchema, `[
		{"file_path": "s3://bucket/data/file-001.parquet", "pos": 1}
	]`)
	itr := func(yield func(arrow.RecordBatch, error) bool) {
		batch.Retain()
		yield(batch, nil)
	}

	seq := positionDeleteRecordsToDataFiles(context.Background(), t.TempDir(), mb, nil,
		recordWritingArgs{
			sc:  PositionalDeleteArrowSchema,
			itr: itr,
			fs:  iceio.LocalFS{},
		})

	var dataFiles []iceberg.DataFile
	for df, err := range seq {
		require.NoError(t, err)
		dataFiles = append(dataFiles, df)
	}

	require.NotEmpty(t, dataFiles, "v2 position-delete write should produce at least one DataFile")
	assert.Equal(t, iceberg.ParquetFile, dataFiles[0].FileFormat(),
		"v2 must use Parquet position-delete files, not Puffin DVs")
}

func TestDVWritePathV3PartitionedUsesParquetPositionDeletes(t *testing.T) {
	// DV+partitioned support is deferred; v3 partitioned writes must still
	// route through the Parquet position-delete writer.
	mb := newPositionDeletePartitionedMetadata(t, 3)

	const path = "file://namespace/age_bucket=1/test.parquet"
	partitions := map[string]partitionContext{
		path: {partitionData: map[int]any{iceberg.PartitionDataIDStart: 1}, specID: 0},
	}
	batch := mustLoadRecordBatchFromJSON(PositionalDeleteArrowSchema,
		`[{"file_path": "`+path+`", "pos": 0}]`)
	itr := func(yield func(arrow.RecordBatch, error) bool) {
		batch.Retain()
		yield(batch, nil)
	}

	seq := positionDeleteRecordsToDataFiles(context.Background(), t.TempDir(), mb, partitions,
		recordWritingArgs{
			sc:  PositionalDeleteArrowSchema,
			itr: itr,
			fs:  iceio.LocalFS{},
		})

	var dataFiles []iceberg.DataFile
	for df, err := range seq {
		require.NoError(t, err)
		dataFiles = append(dataFiles, df)
	}

	require.NotEmpty(t, dataFiles, "v3 partitioned write should still produce Parquet position deletes")
	assert.Equal(t, iceberg.ParquetFile, dataFiles[0].FileFormat(),
		"v3 partitioned write must fall back to Parquet until DV+partitioned support lands")
}

// TestDVWritePathCancelledContext verifies that when the iterator surfaces a
// context cancellation error (e.g., the upstream scan honored a cancelled
// ctx), the DV producer propagates it instead of silently producing partial
// output.
func TestDVWritePathCancelledContext(t *testing.T) {
	mb := newPositionDeleteUnpartitionedMetadata(t, 3)
	fs := iceio.NewMemFS()

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	itr := func(yield func(arrow.RecordBatch, error) bool) {
		yield(nil, ctx.Err())
	}

	seq := positionDeleteRecordsToDataFiles(ctx, "mem://test", mb, nil,
		recordWritingArgs{
			sc:  PositionalDeleteArrowSchema,
			itr: itr,
			fs:  fs,
		})

	var sawErr bool
	var dataFiles []iceberg.DataFile
	for df, err := range seq {
		if err != nil {
			sawErr = true
			assert.ErrorIs(t, err, context.Canceled)

			continue
		}
		dataFiles = append(dataFiles, df)
	}
	assert.True(t, sawErr, "expected cancellation error from iterator to propagate")
	assert.Empty(t, dataFiles, "no DataFiles should be produced when the iterator errors")
}

// TestDVWritePathPropagatesMidStreamError verifies that an error surfaced
// after at least one good batch is propagated to the consumer rather than
// being swallowed and producing a partial Puffin file.
func TestDVWritePathPropagatesMidStreamError(t *testing.T) {
	mb := newPositionDeleteUnpartitionedMetadata(t, 3)
	fs := iceio.NewMemFS()

	good := mustLoadRecordBatchFromJSON(PositionalDeleteArrowSchema,
		`[{"file_path": "s3://bucket/data/file-001.parquet", "pos": 1}]`)

	wantErr := errors.New("simulated upstream failure")
	itr := func(yield func(arrow.RecordBatch, error) bool) {
		good.Retain()
		if !yield(good, nil) {
			return
		}
		yield(nil, wantErr)
	}

	seq := positionDeleteRecordsToDataFiles(context.Background(), "mem://test", mb, nil,
		recordWritingArgs{
			sc:  PositionalDeleteArrowSchema,
			itr: itr,
			fs:  fs,
		})

	var sawErr bool
	var dataFiles []iceberg.DataFile
	for df, err := range seq {
		if err != nil {
			sawErr = true
			assert.ErrorIs(t, err, wantErr)

			continue
		}
		dataFiles = append(dataFiles, df)
	}
	assert.True(t, sawErr, "expected mid-stream error to propagate to the consumer")
	assert.Empty(t, dataFiles,
		"DV producer must not flush a partial Puffin when the iterator errors mid-stream")
}

// TestDVWritePathMergesMultiBatchSameFile verifies that positions for the
// same data file arriving across multiple batches are merged into a single
// DV blob (one DataFile, one bitmap with the union of positions).
func TestDVWritePathMergesMultiBatchSameFile(t *testing.T) {
	mb := newPositionDeleteUnpartitionedMetadata(t, 3)
	fs := iceio.NewMemFS()

	const path = "s3://bucket/data/file-001.parquet"
	batch1 := mustLoadRecordBatchFromJSON(PositionalDeleteArrowSchema, `[
		{"file_path": "`+path+`", "pos": 1},
		{"file_path": "`+path+`", "pos": 3}
	]`)
	batch2 := mustLoadRecordBatchFromJSON(PositionalDeleteArrowSchema, `[
		{"file_path": "`+path+`", "pos": 5},
		{"file_path": "`+path+`", "pos": 7}
	]`)

	itr := func(yield func(arrow.RecordBatch, error) bool) {
		batch1.Retain()
		if !yield(batch1, nil) {
			return
		}
		batch2.Retain()
		yield(batch2, nil)
	}

	seq := positionDeleteRecordsToDataFiles(context.Background(), "mem://test", mb, nil,
		recordWritingArgs{
			sc:  PositionalDeleteArrowSchema,
			itr: itr,
			fs:  fs,
		})

	var dataFiles []iceberg.DataFile
	for df, err := range seq {
		require.NoError(t, err)
		dataFiles = append(dataFiles, df)
	}

	require.Len(t, dataFiles, 1,
		"two batches targeting the same file should merge into one DV blob")
	df := dataFiles[0]
	assert.Equal(t, iceberg.PuffinFile, df.FileFormat())
	assert.Equal(t, path, *df.ReferencedDataFile())
	assert.Equal(t, int64(4), df.Count(),
		"merged DV cardinality should be the union across batches")

	bm, err := dv.ReadDV(fs, df)
	require.NoError(t, err)
	assert.Equal(t, int64(4), bm.Cardinality())
	for _, pos := range []uint64{1, 3, 5, 7} {
		assert.True(t, bm.Contains(pos), "merged bitmap should contain pos %d", pos)
	}
}
