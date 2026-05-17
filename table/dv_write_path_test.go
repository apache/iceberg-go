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

func TestDVWritePathV2FallsBackToPosition(t *testing.T) {
	mb := newPositionDeleteUnpartitionedMetadata(t, 2)

	emptyItr := func(yield func(arrow.RecordBatch, error) bool) {}

	seq := positionDeleteRecordsToDataFiles(context.Background(), t.TempDir(), mb, nil,
		recordWritingArgs{
			sc:  PositionalDeleteArrowSchema,
			itr: emptyItr,
			fs:  iceio.LocalFS{},
		})

	for _, err := range seq {
		require.NoError(t, err)
	}
}

func TestDVWritePathExplicitPropertyOverride(t *testing.T) {
	mb := newPositionDeleteUnpartitionedMetadata(t, 3)
	mb.props = iceberg.Properties{WriteDeleteFormatKey: WriteDeleteFormatPosition}

	emptyItr := func(yield func(arrow.RecordBatch, error) bool) {}

	seq := positionDeleteRecordsToDataFiles(context.Background(), t.TempDir(), mb, nil,
		recordWritingArgs{
			sc:  PositionalDeleteArrowSchema,
			itr: emptyItr,
			fs:  iceio.LocalFS{},
		})

	for _, err := range seq {
		require.NoError(t, err)
	}
}
