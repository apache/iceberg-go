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
	"fmt"
	"reflect"
	"sort"
	"testing"

	"github.com/apache/iceberg-go"
	"github.com/apache/iceberg-go/internal"
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
	return newPositionalDeleteIndexTestEntryWithBounds(
		path, specID, partition, sequenceNumber, referencedDataFile, boundPath, boundPath)
}

func newPositionalDeleteIndexTestEntryWithBounds(
	path string,
	specID int32,
	partition map[int]any,
	sequenceNumber int64,
	referencedDataFile *string,
	lowerPath string,
	upperPath string,
) iceberg.ManifestEntry {
	var lowerBounds, upperBounds map[int][]byte
	if lowerPath != "" {
		lowerBounds = map[int][]byte{filePathFieldID: []byte(lowerPath)}
	}
	if upperPath != "" {
		upperBounds = map[int][]byte{filePathFieldID: []byte(upperPath)}
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

type borrowedPartitionDataFile struct {
	iceberg.DataFile
	publicPartitionCalls   int
	borrowedPartitionCalls int
}

func (f *borrowedPartitionDataFile) Partition() map[int]any {
	f.publicPartitionCalls++

	return f.DataFile.Partition()
}

func (f *borrowedPartitionDataFile) DataFilePartitionRef(_ internal.DataFileRef) map[int]any {
	f.borrowedPartitionCalls++

	return internal.BorrowedDataFilePartition(f.DataFile)
}

func newBorrowedPartitionDataFile(
	t *testing.T,
	contentType iceberg.ManifestEntryContent,
	path string,
	partition map[int]any,
) *borrowedPartitionDataFile {
	t.Helper()

	spec := iceberg.NewPartitionSpecID(1, iceberg.PartitionField{
		SourceIDs: []int{1},
		FieldID:   1000,
		Name:      "part",
		Transform: iceberg.IdentityTransform{},
	})
	builder, err := iceberg.NewDataFileBuilder(
		spec, contentType, path, iceberg.ParquetFile, partition, nil, nil, 1, 1)
	require.NoError(t, err)

	return &borrowedPartitionDataFile{DataFile: builder.Build()}
}

func TestPositionalDeleteIndexUsesBorrowedPartitions(t *testing.T) {
	partition := map[int]any{1000: int32(7)}
	deleteFile := newBorrowedPartitionDataFile(
		t, iceberg.EntryContentPosDeletes, "delete.parquet", partition)
	dataFile := newBorrowedPartitionDataFile(
		t, iceberg.EntryContentData, "data.parquet", partition)
	deleteSequence, dataSequence := int64(2), int64(1)
	deleteEntry := iceberg.NewManifestEntry(
		iceberg.EntryStatusADDED, nil, &deleteSequence, nil, deleteFile)
	dataEntry := iceberg.NewManifestEntry(
		iceberg.EntryStatusADDED, nil, &dataSequence, nil, dataFile)

	idx, err := buildPositionalDeleteIndex([]iceberg.ManifestEntry{deleteEntry})
	require.NoError(t, err)

	matched, err := idx.forDataFile(dataEntry)
	require.NoError(t, err)
	require.Len(t, matched, 1)
	assert.Equal(t, "delete.parquet", matched[0].FilePath())
	assert.Zero(t, deleteFile.publicPartitionCalls)
	assert.Equal(t, 1, deleteFile.borrowedPartitionCalls)
	assert.Zero(t, dataFile.publicPartitionCalls)
	assert.Equal(t, 1, dataFile.borrowedPartitionCalls)
}

func TestPositionalDeleteIndexMatchesPathPartitionAndSequence(t *testing.T) {
	partition := map[int]any{1000: []byte{0xde, 0xad}}
	dataPath := "data.parquet"
	otherPath := "other.parquet"
	deleteEntries := []iceberg.ManifestEntry{
		newPositionalDeleteIndexTestEntry(
			"path-same-sequence.parquet", 1, partition, 5, &dataPath, ""),
		newPositionalDeleteIndexTestEntry(
			"path-newer-from-bounds.parquet", 1, partition, 7, nil, dataPath),
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

func TestPositionalDeleteIndexMatchesReferenceApplicability(t *testing.T) {
	dataA := "data-a.parquet"
	dataB := "data-b.parquet"
	otherData := "other-data.parquet"
	usPartition := map[int]any{1000: "us", 1001: int32(7)}
	euPartition := map[int]any{1000: "eu", 1001: int32(7)}

	// Keep the input deliberately out of sequence and bucket order. The
	// reference below applies the Iceberg rules directly rather than calling
	// the previous scanner matcher or any index classification helper.
	deleteEntries := []iceberg.ManifestEntry{
		newPositionalDeleteIndexTestEntryWithBounds(
			"partition-us-after.parquet", 1, usPartition, 8, nil,
			"data-x.parquet", "data-z.parquet"),
		newPositionalDeleteIndexTestEntry(
			"path-explicit-a-same-sequence.parquet", 99, euPartition, 5, &dataA, ""),
		newPositionalDeleteIndexTestEntry(
			"partition-eu-newer.parquet", 1, euPartition, 6, nil, ""),
		newPositionalDeleteIndexTestEntry(
			"path-inferred-b-newer.parquet", 2, euPartition, 7, nil, dataB),
		newPositionalDeleteIndexTestEntry(
			"partition-us-older.parquet", 1, usPartition, 4, nil, ""),
		newPositionalDeleteIndexTestEntryWithBounds(
			"partition-us-range.parquet", 1, usPartition, 7, nil, dataA, dataB),
		newPositionalDeleteIndexTestEntry(
			"path-explicit-other.parquet", 1, usPartition, 9, &otherData, ""),
		newPositionalDeleteIndexTestEntry(
			"partition-other-spec.parquet", 2, usPartition, 6, nil, ""),
		newPositionalDeleteIndexTestEntry(
			"path-explicit-a-older.parquet", 1, usPartition, 4, &dataA, ""),
		newPositionalDeleteIndexTestEntryWithBounds(
			"partition-us-before.parquet", 1, usPartition, 8, nil,
			"data-0.parquet", "data-9.parquet"),
		newPositionalDeleteIndexTestEntry(
			"path-inferred-a-newer.parquet", 2, euPartition, 7, nil, dataA),
		newPositionalDeleteIndexTestEntry(
			"partition-us-same-sequence.parquet", 1, usPartition, 5, nil, ""),
		newPositionalDeleteIndexTestEntry(
			"path-explicit-a-unknown-sequence.parquet", 1, usPartition, -1, &dataA, ""),
	}

	dataEntries := []iceberg.ManifestEntry{
		newPositionalDeleteIndexDataEntry(dataA, 1, usPartition, 5),
		newPositionalDeleteIndexDataEntry(dataB, 1, usPartition, 6),
		newPositionalDeleteIndexDataEntry("data-c.parquet", 1, euPartition, 5),
		newPositionalDeleteIndexDataEntry("data-d.parquet", 2, usPartition, 5),
		newPositionalDeleteIndexDataEntry(dataA, 1, usPartition, -1),
		newPositionalDeleteIndexDataEntry(dataA, 1, usPartition, 8),
	}

	pathField, ok := iceberg.PositionalDeleteSchema.FindFieldByName("file_path")
	require.True(t, ok)
	idx, err := buildPositionalDeleteIndex(deleteEntries)
	require.NoError(t, err)

	for _, dataEntry := range dataEntries {
		t.Run(fmt.Sprintf("%s/spec=%d/sequence=%d",
			dataEntry.DataFile().FilePath(), dataEntry.DataFile().SpecID(), dataEntry.SequenceNum()), func(t *testing.T) {
			matched, err := idx.forDataFile(dataEntry)
			require.NoError(t, err)

			got := positionalDeletePaths(matched)
			want := make([]string, 0)
			for _, deleteEntry := range deleteEntries {
				if referencePositionalDeleteApplies(dataEntry, deleteEntry, pathField.ID) {
					want = append(want, deleteEntry.DataFile().FilePath())
				}
			}
			sort.Strings(got)
			sort.Strings(want)
			assert.Equal(t, want, got)
		})
	}
}

func referencePositionalDeleteApplies(
	dataEntry iceberg.ManifestEntry,
	deleteEntry iceberg.ManifestEntry,
	pathFieldID int,
) bool {
	if deleteEntry.SequenceNum() < dataEntry.SequenceNum() {
		return false
	}

	dataFile := dataEntry.DataFile()
	deleteFile := deleteEntry.DataFile()
	if referencedPath := deleteFile.ReferencedDataFile(); referencedPath != nil {
		return *referencedPath == dataFile.FilePath()
	}

	lower, hasLower := deleteFile.LowerBoundValues()[pathFieldID]
	upper, hasUpper := deleteFile.UpperBoundValues()[pathFieldID]
	if hasLower && len(lower) > 0 && bytes.Equal(lower, upper) {
		return string(lower) == dataFile.FilePath()
	}

	if deleteFile.SpecID() != dataFile.SpecID() ||
		!referencePartitionsEqual(deleteFile.Partition(), dataFile.Partition()) {
		return false
	}

	path := []byte(dataFile.FilePath())
	if hasLower && bytes.Compare(path, lower) < 0 {
		return false
	}
	if hasUpper && bytes.Compare(path, upper) > 0 {
		return false
	}

	return true
}

func referencePartitionsEqual(left, right map[int]any) bool {
	if len(left) != len(right) {
		return false
	}
	for fieldID, leftValue := range left {
		rightValue, ok := right[fieldID]
		if !ok || !reflect.DeepEqual(leftValue, rightValue) {
			return false
		}
	}

	return true
}

func TestPositionalDeleteIndexPrunesPartitionEntriesUsingPathMetrics(t *testing.T) {
	partition := map[int]any{1000: "partition"}
	deleteEntries := []iceberg.ManifestEntry{
		newPositionalDeleteIndexTestEntryWithBounds(
			"matching-range.parquet", 1, partition, 5, nil, "data-a.parquet", "data-z.parquet"),
		newPositionalDeleteIndexTestEntryWithBounds(
			"range-before.parquet", 1, partition, 5, nil, "data-a.parquet", "data-b.parquet"),
		newPositionalDeleteIndexTestEntryWithBounds(
			"range-after.parquet", 1, partition, 5, nil, "data-x.parquet", "data-z.parquet"),
		newPositionalDeleteIndexTestEntry(
			"no-metrics.parquet", 1, partition, 5, nil, ""),
	}

	idx, err := buildPositionalDeleteIndex(deleteEntries)
	require.NoError(t, err)

	dataEntry := newPositionalDeleteIndexDataEntry("data-m.parquet", 1, partition, 5)
	matched, err := idx.forDataFile(dataEntry)
	require.NoError(t, err)
	assert.Equal(t, []string{"matching-range.parquet", "no-metrics.parquet"},
		positionalDeletePaths(matched))
}

func TestFilePathMayMatch(t *testing.T) {
	const dataFilePath = "data-m.parquet"

	tests := []struct {
		name        string
		count       int64
		lower       []byte
		upper       []byte
		hasLower    bool
		hasUpper    bool
		valueCounts map[int]int64
		nullCounts  map[int]int64
		nanCounts   map[int]int64
		want        bool
	}{
		{
			name:     "empty file",
			count:    0,
			lower:    []byte(dataFilePath),
			upper:    []byte(dataFilePath),
			hasLower: true,
			hasUpper: true,
			want:     false,
		},
		{
			name:     "exact bounds",
			count:    1,
			lower:    []byte(dataFilePath),
			upper:    []byte(dataFilePath),
			hasLower: true,
			hasUpper: true,
			want:     true,
		},
		{
			name:     "inside bounds",
			count:    1,
			lower:    []byte("data-a.parquet"),
			upper:    []byte("data-z.parquet"),
			hasLower: true,
			hasUpper: true,
			want:     true,
		},
		{
			name:     "below lower bound",
			count:    1,
			lower:    []byte("data-n.parquet"),
			upper:    []byte("data-z.parquet"),
			hasLower: true,
			hasUpper: true,
			want:     false,
		},
		{
			name:     "above upper bound",
			count:    1,
			lower:    []byte("data-a.parquet"),
			upper:    []byte("data-l.parquet"),
			hasLower: true,
			hasUpper: true,
			want:     false,
		},
		{
			name:     "inverted bounds are conservative",
			count:    1,
			lower:    []byte("data-z.parquet"),
			upper:    []byte("data-a.parquet"),
			hasLower: true,
			hasUpper: true,
			want:     true,
		},
		{
			name:     "missing lower bound",
			count:    1,
			upper:    []byte("data-z.parquet"),
			hasUpper: true,
			want:     true,
		},
		{
			name:     "missing upper bound",
			count:    1,
			lower:    []byte("data-a.parquet"),
			hasLower: true,
			want:     true,
		},
		{
			name:  "missing both bounds",
			count: 1,
			want:  true,
		},
		{
			name:     "nil bounds are missing",
			count:    1,
			hasLower: true,
			hasUpper: true,
			want:     true,
		},
		{
			name:        "null-only metadata",
			count:       1,
			lower:       []byte("data-a.parquet"),
			upper:       []byte("data-z.parquet"),
			hasLower:    true,
			hasUpper:    true,
			valueCounts: map[int]int64{filePathFieldID: 1},
			nullCounts:  map[int]int64{filePathFieldID: 1},
			want:        false,
		},
		{
			name:        "nan-only metadata",
			count:       1,
			lower:       []byte("data-a.parquet"),
			upper:       []byte("data-z.parquet"),
			hasLower:    true,
			hasUpper:    true,
			valueCounts: map[int]int64{filePathFieldID: 1},
			nanCounts:   map[int]int64{filePathFieldID: 1},
			want:        false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var lowerBounds, upperBounds map[int][]byte
			if tt.hasLower {
				lowerBounds = map[int][]byte{filePathFieldID: tt.lower}
			}
			if tt.hasUpper {
				upperBounds = map[int][]byte{filePathFieldID: tt.upper}
			}

			file := &mockDataFile{
				count:       tt.count,
				valueCounts: tt.valueCounts,
				nullCounts:  tt.nullCounts,
				nanCounts:   tt.nanCounts,
				lowerBounds: lowerBounds,
				upperBounds: upperBounds,
			}
			assert.Equal(t, tt.want, filePathMayMatch(file, dataFilePath))
		})
	}
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
