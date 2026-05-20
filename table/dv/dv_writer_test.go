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

package dv

import (
	"context"
	"testing"

	"github.com/apache/iceberg-go"
	iceio "github.com/apache/iceberg-go/io"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestSerializeDVRoundTrip(t *testing.T) {
	bm := NewRoaringPositionBitmap()
	bm.Set(1)
	bm.Set(3)
	bm.Set(5)
	bm.Set(7)
	bm.Set(9)

	data, err := SerializeDV(bm)
	require.NoError(t, err)
	require.NotEmpty(t, data)

	decoded, err := DeserializeDV(data, 5)
	require.NoError(t, err)

	assert.Equal(t, int64(5), decoded.Cardinality())
	assert.True(t, decoded.Contains(1))
	assert.True(t, decoded.Contains(3))
	assert.True(t, decoded.Contains(5))
	assert.True(t, decoded.Contains(7))
	assert.True(t, decoded.Contains(9))
	assert.False(t, decoded.Contains(2))
	assert.False(t, decoded.Contains(4))
}

func TestSerializeDVEmpty(t *testing.T) {
	bm := NewRoaringPositionBitmap()

	data, err := SerializeDV(bm)
	require.NoError(t, err)

	decoded, err := DeserializeDV(data, 0)
	require.NoError(t, err)
	assert.True(t, decoded.IsEmpty())
}

func TestSerializeDVLargePositions(t *testing.T) {
	bm := NewRoaringPositionBitmap()
	bm.Set(100)
	bm.Set(101)
	bm.Set(2147483747)
	bm.Set(2147483748)
	bm.Set((uint64(1) << 32) | 42)

	data, err := SerializeDV(bm)
	require.NoError(t, err)

	decoded, err := DeserializeDV(data, 5)
	require.NoError(t, err)

	assert.Equal(t, int64(5), decoded.Cardinality())
	assert.True(t, decoded.Contains(100))
	assert.True(t, decoded.Contains(101))
	assert.True(t, decoded.Contains(2147483747))
	assert.True(t, decoded.Contains(2147483748))
	assert.True(t, decoded.Contains((uint64(1)<<32)|42))
}

func newTestFS() *iceio.MemFS {
	return iceio.NewMemFS()
}

func TestDVWriterFlushEmpty(t *testing.T) {
	fs := newTestFS()
	w := NewDVWriter(fs)

	dataFiles, err := w.Flush(context.Background(), "mem://test/dv.puffin")
	require.NoError(t, err)
	assert.Nil(t, dataFiles)
}

func TestDVWriterSingleDataFile(t *testing.T) {
	fs := newTestFS()
	w := NewDVWriter(fs)

	dataPath := "s3://bucket/data/file-001.parquet"
	w.Add(dataPath, []int64{1, 3, 5, 7, 9})

	dataFiles, err := w.Flush(context.Background(), "mem://test/dv.puffin")
	require.NoError(t, err)
	require.Len(t, dataFiles, 1)

	df := dataFiles[0]
	assert.Equal(t, iceberg.EntryContentPosDeletes, df.ContentType())
	assert.Equal(t, "mem://test/dv.puffin", df.FilePath())
	assert.Equal(t, iceberg.PuffinFile, df.FileFormat())
	assert.Equal(t, int64(5), df.Count())
	assert.NotNil(t, df.ReferencedDataFile())
	assert.Equal(t, dataPath, *df.ReferencedDataFile())
	assert.NotNil(t, df.ContentOffset())
	assert.NotNil(t, df.ContentSizeInBytes())

	verifyDVReadBack(t, fs, df)
}

func TestDVWriterMultipleDataFiles(t *testing.T) {
	fs := newTestFS()
	w := NewDVWriter(fs)

	path1 := "s3://bucket/data/file-001.parquet"
	path2 := "s3://bucket/data/file-002.parquet"
	w.Add(path1, []int64{0, 10, 20})
	w.Add(path2, []int64{5, 15, 25, 35})

	dataFiles, err := w.Flush(context.Background(), "mem://test/multi-dv.puffin")
	require.NoError(t, err)
	require.Len(t, dataFiles, 2)

	assert.Equal(t, int64(3), dataFiles[0].Count())
	assert.Equal(t, path1, *dataFiles[0].ReferencedDataFile())

	assert.Equal(t, int64(4), dataFiles[1].Count())
	assert.Equal(t, path2, *dataFiles[1].ReferencedDataFile())

	for _, df := range dataFiles {
		assert.Equal(t, "mem://test/multi-dv.puffin", df.FilePath())
		verifyDVReadBack(t, fs, df)
	}
}

func TestDVWriterDeduplicatesPositions(t *testing.T) {
	fs := newTestFS()
	w := NewDVWriter(fs)

	dataPath := "s3://bucket/data/file-001.parquet"
	w.Add(dataPath, []int64{1, 3, 5})
	w.Add(dataPath, []int64{3, 5, 7})

	dataFiles, err := w.Flush(context.Background(), "mem://test/dedup.puffin")
	require.NoError(t, err)
	require.Len(t, dataFiles, 1)

	assert.Equal(t, int64(4), dataFiles[0].Count())

	verifyDVReadBack(t, fs, dataFiles[0])
}

func TestDVWriterResetsAfterFlush(t *testing.T) {
	fs := newTestFS()
	w := NewDVWriter(fs)

	w.Add("s3://bucket/data/file-001.parquet", []int64{1, 2, 3})
	_, err := w.Flush(context.Background(), "mem://test/first.puffin")
	require.NoError(t, err)

	dataFiles, err := w.Flush(context.Background(), "mem://test/second.puffin")
	require.NoError(t, err)
	assert.Nil(t, dataFiles)
}

func verifyDVReadBack(t *testing.T, fs iceio.IO, df iceberg.DataFile) {
	t.Helper()

	bm, err := ReadDV(fs, df)
	require.NoError(t, err)
	assert.Equal(t, df.Count(), bm.Cardinality())
}
