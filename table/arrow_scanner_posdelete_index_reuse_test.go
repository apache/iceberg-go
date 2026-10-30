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

	"github.com/apache/iceberg-go"
	"github.com/stretchr/testify/require"
)

func TestPositionDeleteIndexReuseAcrossSplits(t *testing.T) {
	data := &mockDataFile{path: "s3://bucket/data/a.parquet"}
	otherData := &mockDataFile{path: "s3://bucket/data/b.parquet"}
	deleteA := &mockDataFile{path: "s3://bucket/delete/a.parquet", contentType: iceberg.EntryContentPosDeletes}
	deleteB := &mockDataFile{path: "s3://bucket/delete/b.parquet", contentType: iceberg.EntryContentPosDeletes}
	deleteC := &mockDataFile{path: "s3://bucket/delete/c.parquet", contentType: iceberg.EntryContentPosDeletes}
	otherDelete := &mockDataFile{path: "s3://bucket/delete/not-pos.parquet", contentType: iceberg.EntryContentData}

	tasks := []FileScanTask{
		{File: data, DeleteFiles: []iceberg.DataFile{deleteA, deleteB}},
		{File: data, DeleteFiles: []iceberg.DataFile{deleteA, deleteB}},          // Adjacent identical set.
		{File: data, DeleteFiles: []iceberg.DataFile{deleteB, deleteA}},          // Reordered set.
		{File: data, DeleteFiles: []iceberg.DataFile{deleteA, deleteC}},          // Changed set.
		{File: data, DeleteFiles: []iceberg.DataFile{deleteA, deleteB, deleteB}}, // Duplicate path.
		{File: otherData, DeleteFiles: []iceberg.DataFile{deleteA, deleteB}},     // Different target.
		{
			File: data, DeleteFiles: []iceberg.DataFile{deleteA, deleteB},
			DeletionVectorFiles: []iceberg.DataFile{deleteC}, // Superseding DV.
		},
		{File: data, DeleteFiles: []iceberg.DataFile{deleteA, deleteB}},     // After DV.
		{File: data, DeleteFiles: []iceberg.DataFile{deleteA, otherDelete}}, // Non-positional delete.
		{File: data, DeleteFiles: []iceberg.DataFile{deleteA}},
		{File: data, DeleteFiles: []iceberg.DataFile{deleteA, deleteB}},
		{DeleteFiles: []iceberg.DataFile{deleteA}}, // Missing data file path.
		{File: data}, // No delete files.
	}

	loader := newLazyPositionDeleteLoader(nil, tasks)
	require.NotNil(t, loader)
	defer loader.release()

	for i, task := range tasks {
		wantPath := ""
		if task.File != nil {
			wantPath = task.File.FilePath()
		}
		require.Equal(t, wantPath, loader.indexes[i].targetPath, "task %d", i)
	}

	indexes := loader.indexes
	require.NotNil(t, indexes[0].index)
	for _, i := range []int{1, 2, 4, 7, 10} {
		require.Same(t, indexes[0].index, indexes[i].index, "task %d should share the canonical index", i)
	}
	require.Equal(t, []string{deleteA.FilePath(), deleteB.FilePath()}, indexes[0].index.deletePaths)
	require.NotSame(t, indexes[0].index, indexes[3].index) // Changed delete set.
	require.NotSame(t, indexes[0].index, indexes[5].index) // Changed target path.
	require.Same(t, indexes[8].index, indexes[9].index)    // Non-positional entries are ignored.
	require.NotSame(t, indexes[0].index, indexes[9].index)
	for _, i := range []int{6, 11, 12} {
		require.Nil(t, indexes[i].index, "task %d must not have an index", i)
	}
}

func TestSameOrderedPositionDeleteFiles(t *testing.T) {
	a := &mockDataFile{path: "delete-a.parquet", contentType: iceberg.EntryContentPosDeletes}
	b := &mockDataFile{path: "delete-b.parquet", contentType: iceberg.EntryContentPosDeletes}
	nonPosA := &mockDataFile{path: "other-a.parquet", contentType: iceberg.EntryContentData}
	nonPosB := &mockDataFile{path: "other-b.parquet", contentType: iceberg.EntryContentData}

	require.True(t, sameOrderedPositionDeleteFiles(
		[]iceberg.DataFile{a, nonPosA}, []iceberg.DataFile{a, nonPosB}))
	require.False(t, sameOrderedPositionDeleteFiles(
		[]iceberg.DataFile{a, b}, []iceberg.DataFile{b, a}))
	require.False(t, sameOrderedPositionDeleteFiles(
		[]iceberg.DataFile{a}, []iceberg.DataFile{b}))
	require.False(t, sameOrderedPositionDeleteFiles(
		[]iceberg.DataFile{a}, []iceberg.DataFile{nonPosA}))
	require.False(t, sameOrderedPositionDeleteFiles(
		[]iceberg.DataFile{a}, nil))
}
