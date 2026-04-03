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
	"runtime"
	"sync"
	"sync/atomic"
	"testing"

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

func TestMatchDeletesToDataPuffinNilRef(t *testing.T) {
	snapshotID := int64(1)
	seqNum := int64(10)

	dataEntry := iceberg.NewManifestEntry(
		iceberg.EntryStatusADDED, &snapshotID, &seqNum, nil,
		&mockDataFile{
			contentType: iceberg.EntryContentData,
			path:        "s3://bucket/data/file1.parquet",
			format:      iceberg.ParquetFile,
			count:       100,
			filesize:    1024,
		},
	)

	deleteSeqNum := int64(11)
	puffinDelete := iceberg.NewManifestEntry(
		iceberg.EntryStatusADDED, &snapshotID, &deleteSeqNum, nil,
		&mockDataFile{
			contentType: iceberg.EntryContentPosDeletes,
			path:        "s3://bucket/data/dv.puffin",
			format:      "PUFFIN",
			count:       5,
			filesize:    200,
		},
	)

	_, err := matchDeletesToData(dataEntry, []iceberg.ManifestEntry{puffinDelete})
	require.Error(t, err)
	assert.ErrorContains(t, err, "referenced_data_file")
}

type mockDVDataFile struct {
	mockDataFile
	referencedDataFile *string
	contentOffset      *int64
	contentSizeInBytes *int64
}

func (m *mockDVDataFile) ReferencedDataFile() *string { return m.referencedDataFile }
func (m *mockDVDataFile) ContentOffset() *int64       { return m.contentOffset }
func (m *mockDVDataFile) ContentSizeInBytes() *int64  { return m.contentSizeInBytes }

func TestMatchDeletesToDataPuffinMatch(t *testing.T) {
	snapshotID := int64(1)
	seqNum := int64(10)
	dataPath := "s3://bucket/data/file1.parquet"

	dataEntry := iceberg.NewManifestEntry(
		iceberg.EntryStatusADDED, &snapshotID, &seqNum, nil,
		&mockDataFile{
			contentType: iceberg.EntryContentData,
			path:        dataPath,
			format:      iceberg.ParquetFile,
			count:       100,
			filesize:    1024,
		},
	)

	deleteSeqNum := int64(11)
	puffinDelete := iceberg.NewManifestEntry(
		iceberg.EntryStatusADDED, &snapshotID, &deleteSeqNum, nil,
		&mockDVDataFile{
			mockDataFile: mockDataFile{
				contentType: iceberg.EntryContentPosDeletes,
				path:        "s3://bucket/data/dv.puffin",
				format:      "PUFFIN",
				count:       5,
				filesize:    200,
			},
			referencedDataFile: &dataPath,
		},
	)

	otherPath := "s3://bucket/data/other.parquet"
	nonMatchingDelete := iceberg.NewManifestEntry(
		iceberg.EntryStatusADDED, &snapshotID, &deleteSeqNum, nil,
		&mockDVDataFile{
			mockDataFile: mockDataFile{
				contentType: iceberg.EntryContentPosDeletes,
				path:        "s3://bucket/data/dv2.puffin",
				format:      "PUFFIN",
				count:       3,
				filesize:    150,
			},
			referencedDataFile: &otherPath,
		},
	)

	deletes, err := matchDeletesToData(dataEntry, []iceberg.ManifestEntry{puffinDelete, nonMatchingDelete})
	require.NoError(t, err)
	require.Len(t, deletes, 1)
	assert.Equal(t, "s3://bucket/data/dv.puffin", deletes[0].FilePath())
}

func TestMatchDeletesToDataMixedFormats(t *testing.T) {
	snapshotID := int64(1)
	seqNum := int64(10)
	dataPath := "s3://bucket/data/file1.parquet"

	dataEntry := iceberg.NewManifestEntry(
		iceberg.EntryStatusADDED, &snapshotID, &seqNum, nil,
		&mockDataFile{
			contentType: iceberg.EntryContentData,
			path:        dataPath,
			format:      iceberg.ParquetFile,
			count:       100,
			filesize:    1024,
		},
	)

	deleteSeqNum := int64(11)

	// Parquet positional delete — must have file_path bounds that include dataPath
	parquetDelete := iceberg.NewManifestEntry(
		iceberg.EntryStatusADDED, &snapshotID, &deleteSeqNum, nil,
		&mockDataFile{
			contentType: iceberg.EntryContentPosDeletes,
			path:        "s3://bucket/data/pos-delete.parquet",
			format:      iceberg.ParquetFile,
			count:       10,
			filesize:    500,
			lowerBounds: map[int][]byte{2147483546: []byte(dataPath)},
			upperBounds: map[int][]byte{2147483546: []byte(dataPath)},
		},
	)

	// PUFFIN DV — matches via referenced_data_file
	puffinDelete := iceberg.NewManifestEntry(
		iceberg.EntryStatusADDED, &snapshotID, &deleteSeqNum, nil,
		&mockDVDataFile{
			mockDataFile: mockDataFile{
				contentType: iceberg.EntryContentPosDeletes,
				path:        "s3://bucket/data/dv.puffin",
				format:      "PUFFIN",
				count:       5,
				filesize:    200,
			},
			referencedDataFile: &dataPath,
		},
	)

	deletes, err := matchDeletesToData(dataEntry, []iceberg.ManifestEntry{parquetDelete, puffinDelete})
	require.NoError(t, err)
	require.Len(t, deletes, 2)

	formats := make(map[iceberg.FileFormat]bool)
	for _, d := range deletes {
		formats[d.FileFormat()] = true
	}
	assert.True(t, formats[iceberg.ParquetFile], "should include parquet delete")
	assert.True(t, formats["PUFFIN"], "should include puffin DV")
}
