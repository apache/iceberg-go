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
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package table

import (
	"testing"

	"github.com/apache/iceberg-go"
	"github.com/stretchr/testify/require"
)

func TestDecideDanglingDeleteFilesAllTypes(t *testing.T) {
	makeDelete := func(path string, content iceberg.ManifestEntryContent) iceberg.DataFile {
		t.Helper()
		builder, err := iceberg.NewDataFileBuilder(
			*iceberg.UnpartitionedSpec, content, path,
			iceberg.ParquetFile, nil, nil, nil, 1, 128)
		require.NoError(t, err)
		if content == iceberg.EntryContentEqDeletes {
			builder.EqualityFieldIDs([]int{1})
		}

		return builder.Build()
	}

	livePosition := makeDelete("live-pos", iceberg.EntryContentPosDeletes)
	danglingPosition := makeDelete("dangling-pos", iceberg.EntryContentPosDeletes)
	liveEquality := makeDelete("live-eq", iceberg.EntryContentEqDeletes)
	danglingEquality := makeDelete("dangling-eq", iceberg.EntryContentEqDeletes)

	got := decideDanglingDeleteFiles(
		[]iceberg.DataFile{livePosition, danglingPosition, liveEquality, danglingEquality},
		[]FileScanTask{
			{DeleteFiles: []iceberg.DataFile{livePosition}, EqualityDeleteFiles: []iceberg.DataFile{liveEquality}},
			{DeleteFiles: []iceberg.DataFile{livePosition}},
		})
	require.Len(t, got, 2)
	require.Equal(t, danglingPosition.FilePath(), got[0].FilePath())
	require.Equal(t, danglingEquality.FilePath(), got[1].FilePath())
}

func TestDecideDanglingDeleteFilesSharedPuffin(t *testing.T) {
	// A Puffin container can hold multiple DV blobs. Seeing one live vector
	// must not preserve a different, dangling vector in the same container.
	makeVector := func(reference string, offset, length int64) iceberg.DataFile {
		return &dvMockDataFile{
			mockDataFile: mockDataFile{
				path:        "shared.puffin",
				contentType: iceberg.EntryContentPosDeletes,
				format:      iceberg.PuffinFile,
			},
			referencedDataFile: &reference,
			contentOffset:      &offset,
			contentSizeInBytes: &length,
		}
	}

	live := makeVector("live.parquet", 0, 100)
	dangling := makeVector("missing.parquet", 100, 100)
	got := decideDanglingDeleteFiles(
		[]iceberg.DataFile{live, dangling},
		[]FileScanTask{{DeletionVectorFiles: []iceberg.DataFile{live}}},
	)
	require.Len(t, got, 1)
	require.Equal(t, dangling.ReferencedDataFile(), got[0].ReferencedDataFile())
	require.Equal(t, dangling.ContentOffset(), got[0].ContentOffset())
}

func TestRemoveDanglingDeleteFilesUnpartitionedNoop(t *testing.T) {
	schema := iceberg.NewSchema(0,
		iceberg.NestedField{ID: 1, Name: "id", Type: iceberg.PrimitiveTypes.Int64, Required: true},
	)
	meta, err := NewMetadata(schema, iceberg.UnpartitionedSpec, UnsortedSortOrder, t.TempDir(), nil)
	require.NoError(t, err)
	tbl := New([]string{"db", "example"}, meta, "", nil, nil)
	updated, count, err := tbl.RemoveDanglingDeleteFiles(t.Context())
	require.NoError(t, err)
	require.Zero(t, count)
	require.Equal(t, tbl.Metadata(), updated.Metadata())
}
