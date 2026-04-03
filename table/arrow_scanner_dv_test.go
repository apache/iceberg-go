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
	"bytes"
	"context"
	"os"
	"testing"

	"github.com/apache/iceberg-go"
	iceio "github.com/apache/iceberg-go/io"
	"github.com/apache/iceberg-go/puffin"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestReadAllDeleteFilesDV(t *testing.T) {
	// 1. Build a puffin file containing a DV blob
	bm := NewRoaringPositionBitmap()
	bm.Set(3)
	bm.Set(7)
	bm.Set(42)

	dvBytes := serializeDVForTest(t, bm)

	var puffinBuf bytes.Buffer
	pw, err := puffin.NewWriter(&puffinBuf)
	require.NoError(t, err)

	blobMeta, err := pw.AddBlob(puffin.BlobMetadataInput{
		Type:           puffin.BlobTypeDeletionVector,
		SnapshotID:     -1,
		SequenceNumber: -1,
		Fields:         []int32{int32(iceberg.PositionalDeleteSchema.HighestFieldID())},
		Properties: map[string]string{
			"referenced-data-file": "s3://bucket/data/file1.parquet",
			"cardinality":          "3",
		},
	}, dvBytes)
	require.NoError(t, err)
	require.NoError(t, pw.Finish())

	// 2. Write puffin to a temp file
	tmpDir := t.TempDir()
	puffinPath := tmpDir + "/dv.puffin"
	require.NoError(t, os.WriteFile(puffinPath, puffinBuf.Bytes(), 0644))

	// 3. Create a local FS that can open the temp file.
	// ioFS.Open strips leading "/" so os.DirFS("/") + absolute path works.
	fs := iceio.FS(os.DirFS("/"))

	// 4. Build FileScanTask with a DV delete file
	dataFilePath := "s3://bucket/data/file1.parquet"
	offset := blobMeta.Offset
	length := blobMeta.Length

	dvFile := &mockDVDataFile{
		mockDataFile: mockDataFile{
			contentType: iceberg.EntryContentPosDeletes,
			path:        puffinPath,
			format:      "PUFFIN",
			count:       3,
			filesize:    int64(puffinBuf.Len()),
		},
		referencedDataFile: &dataFilePath,
		contentOffset:      &offset,
		contentSizeInBytes: &length,
	}

	tasks := []FileScanTask{
		{
			File: &mockDataFile{
				contentType: iceberg.EntryContentData,
				path:        dataFilePath,
				format:      iceberg.ParquetFile,
				count:       100,
				filesize:    1024,
			},
			DeleteFiles: []iceberg.DataFile{dvFile},
			Start:       0,
			Length:      1024,
		},
	}

	// 5. Call readAllDeleteFiles
	ctx := context.Background()
	posDeletes, dvDeletes, err := readAllDeleteFiles(ctx, fs, tasks, 1)
	require.NoError(t, err)

	// 6. Verify
	assert.Empty(t, posDeletes, "should have no parquet positional deletes")
	require.Contains(t, dvDeletes, dataFilePath)

	bitmap := dvDeletes[dataFilePath]
	assert.Equal(t, int64(3), bitmap.Cardinality())
	assert.True(t, bitmap.Contains(3))
	assert.True(t, bitmap.Contains(7))
	assert.True(t, bitmap.Contains(42))
	assert.False(t, bitmap.Contains(0))
}
