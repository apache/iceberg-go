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
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type positionalDeleteIndexTestFile struct {
	*mockDataFile
	referencedDataFile *string
}

func (f *positionalDeleteIndexTestFile) ReferencedDataFile() *string {
	return f.referencedDataFile
}

func newPositionalDeleteIndexTestEntry(
	path string,
	specID int32,
	partition map[int]any,
	sequenceNumber int64,
	referencedDataFile *string,
	boundPath string,
) iceberg.ManifestEntry {
	lowerBounds := map[int][]byte(nil)
	upperBounds := map[int][]byte(nil)
	if boundPath != "" {
		lowerBounds = map[int][]byte{filePathFieldID: []byte(boundPath)}
		upperBounds = map[int][]byte{filePathFieldID: []byte(boundPath)}
	}

	file := &positionalDeleteIndexTestFile{
		mockDataFile: &mockDataFile{
			path: path, specid: specID, partition: partition,
			contentType: iceberg.EntryContentPosDeletes,
			lowerBounds: lowerBounds, upperBounds: upperBounds,
			count: 1,
		},
		referencedDataFile: referencedDataFile,
	}

	return iceberg.NewManifestEntry(
		iceberg.EntryStatusADDED, nil, &sequenceNumber, nil, file)
}

func newPositionalDeleteIndexDataEntry(
	path string,
	specID int32,
	partition map[int]any,
	sequenceNumber int64,
) iceberg.ManifestEntry {
	file := &mockDataFile{
		path: path, specid: specID, partition: partition,
		contentType: iceberg.EntryContentData,
	}

	return iceberg.NewManifestEntry(
		iceberg.EntryStatusADDED, nil, &sequenceNumber, nil, file)
}

func TestPositionalDeleteIndexMatchesPathPartitionAndSequence(t *testing.T) {
	partition := map[int]any{1000: []byte{0xde, 0xad}}
	dataPath := "data.parquet"
	otherPath := "other.parquet"
	deleteEntries := []iceberg.ManifestEntry{
		newPositionalDeleteIndexTestEntry(
			"path-same-sequence.parquet", 9, map[int]any{1000: "other"}, 5, &dataPath, ""),
		newPositionalDeleteIndexTestEntry(
			"path-newer-from-bounds.parquet", 9, nil, 7, nil, dataPath),
		newPositionalDeleteIndexTestEntry(
			"other-path.parquet", 1, partition, 8, &otherPath, ""),
		newPositionalDeleteIndexTestEntry(
			"partition-same-sequence.parquet", 1, partition, 5, nil, ""),
		newPositionalDeleteIndexTestEntry(
			"partition-newer.parquet", 1,
			map[int]any{1000: append([]byte(nil), 0xde, 0xad)}, 6, nil, ""),
		newPositionalDeleteIndexTestEntry(
			"partition-older.parquet", 1, partition, 4, nil, ""),
		newPositionalDeleteIndexTestEntry(
			"different-partition.parquet", 1, map[int]any{1000: []byte{0xbe, 0xef}}, 8, nil, ""),
		newPositionalDeleteIndexTestEntry(
			"different-spec.parquet", 2, partition, 8, nil, ""),
	}

	idx, err := buildPositionalDeleteIndex(deleteEntries)
	require.NoError(t, err)

	dataEntry := newPositionalDeleteIndexDataEntry(dataPath, 1, partition, 5)
	matched, err := idx.forDataFile(dataEntry)
	require.NoError(t, err)
	require.Len(t, matched, 4)
	assert.Equal(t, []string{
		"partition-same-sequence.parquet",
		"partition-newer.parquet",
		"path-same-sequence.parquet",
		"path-newer-from-bounds.parquet",
	}, positionalDeletePaths(matched))
}

func TestPositionalDeleteIndexHandlesUnknownSequenceNumbers(t *testing.T) {
	dataPath := "data.parquet"
	deleteEntries := []iceberg.ManifestEntry{
		newPositionalDeleteIndexTestEntry("unknown.parquet", 0, nil, -1, &dataPath, ""),
		newPositionalDeleteIndexTestEntry("sequence-1.parquet", 0, nil, 1, &dataPath, ""),
		newPositionalDeleteIndexTestEntry("sequence-3.parquet", 0, nil, 3, &dataPath, ""),
	}

	idx, err := buildPositionalDeleteIndex(deleteEntries)
	require.NoError(t, err)

	knownData := newPositionalDeleteIndexDataEntry(dataPath, 0, nil, 2)
	matched, err := idx.forDataFile(knownData)
	require.NoError(t, err)
	assert.Equal(t, []string{"sequence-3.parquet"}, positionalDeletePaths(matched))

	unknownData := newPositionalDeleteIndexDataEntry(dataPath, 0, nil, -1)
	matched, err = idx.forDataFile(unknownData)
	require.NoError(t, err)
	assert.Equal(t, []string{"unknown.parquet", "sequence-1.parquet", "sequence-3.parquet"},
		positionalDeletePaths(matched))
}

func TestPositionalDeleteIndexSkipsPartitionKeyForPathOnlyDeletes(t *testing.T) {
	dataPath := "data.parquet"
	deleteEntry := newPositionalDeleteIndexTestEntry(
		"delete.parquet", 1, nil, 2, &dataPath, "")

	idx, err := buildPositionalDeleteIndex([]iceberg.ManifestEntry{deleteEntry})
	require.NoError(t, err)

	dataEntry := newPositionalDeleteIndexDataEntry(
		dataPath, 1, map[int]any{1000: struct{}{}}, 1)
	matched, err := idx.forDataFile(dataEntry)
	require.NoError(t, err)
	assert.Equal(t, []string{"delete.parquet"}, positionalDeletePaths(matched))
}

func TestPositionalDeleteIndexRejectsUnsupportedPartitionValue(t *testing.T) {
	deleteEntry := newPositionalDeleteIndexTestEntry(
		"delete.parquet", 1, map[int]any{1000: struct{}{}}, 2, nil, "")

	_, err := buildPositionalDeleteIndex([]iceberg.ManifestEntry{deleteEntry})
	require.Error(t, err)
	assert.ErrorContains(t, err, "indexing positional delete file delete.parquet")
	assert.ErrorContains(t, err, "partition field 1000")
}

func positionalDeletePaths(files []iceberg.DataFile) []string {
	paths := make([]string, len(files))
	for i, file := range files {
		paths[i] = file.FilePath()
	}

	return paths
}
