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
)

// dvMockDataFile extends mockDataFile with the DV-specific fields
// (referenced data file, content offset/size) needed by scan planning tests.
type dvMockDataFile struct {
	mockDataFile
	referencedDataFile *string
	contentOffset      *int64
	contentSizeInBytes *int64
}

func (d *dvMockDataFile) ReferencedDataFile() *string { return d.referencedDataFile }
func (d *dvMockDataFile) ContentOffset() *int64       { return d.contentOffset }
func (d *dvMockDataFile) ContentSizeInBytes() *int64  { return d.contentSizeInBytes }

func strPtr(s string) *string { return &s }
func int64Ptr(v int64) *int64 { return &v }

func TestIsDeletionVector(t *testing.T) {
	tests := []struct {
		name     string
		df       iceberg.DataFile
		expected bool
	}{
		{
			name: "regular data file",
			df: &mockDataFile{
				path:        "s3://bucket/data/file.parquet",
				contentType: iceberg.EntryContentData,
			},
			expected: false,
		},
		{
			name: "regular position delete file",
			df: &mockDataFile{
				path:        "s3://bucket/data/pos-del.parquet",
				contentType: iceberg.EntryContentPosDeletes,
			},
			expected: false,
		},
		{
			name: "deletion vector",
			df: &dvMockDataFile{
				mockDataFile: mockDataFile{
					path:        "s3://bucket/data/dv.puffin",
					contentType: iceberg.EntryContentPosDeletes,
				},
				referencedDataFile: strPtr("s3://bucket/data/file.parquet"),
				contentOffset:      int64Ptr(100),
				contentSizeInBytes: int64Ptr(256),
			},
			expected: true,
		},
		{
			name: "equality delete file (never a DV)",
			df: &mockDataFile{
				path:        "s3://bucket/data/eq-del.parquet",
				contentType: iceberg.EntryContentEqDeletes,
			},
			expected: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.expected, isDeletionVector(tt.df))
		})
	}
}

func TestManifestEntries_DVClassification(t *testing.T) {
	entries := newManifestEntries()
	snapshotID := int64(1)

	// Data entry
	dataFile := &mockDataFile{
		path:        "s3://bucket/data/data-001.parquet",
		contentType: iceberg.EntryContentData,
	}
	entries.addDataEntry(iceberg.NewManifestEntry(
		iceberg.EntryStatusADDED, &snapshotID, nil, nil, dataFile))

	// Regular position delete entry
	posDelFile := &mockDataFile{
		path:        "s3://bucket/data/pos-del-001.parquet",
		contentType: iceberg.EntryContentPosDeletes,
	}
	entries.addPositionalDeleteEntry(iceberg.NewManifestEntry(
		iceberg.EntryStatusADDED, &snapshotID, nil, nil, posDelFile))

	// DV entry
	dvFile := &dvMockDataFile{
		mockDataFile: mockDataFile{
			path:        "s3://bucket/data/dv-001.puffin",
			contentType: iceberg.EntryContentPosDeletes,
		},
		referencedDataFile: strPtr("s3://bucket/data/data-001.parquet"),
		contentOffset:      int64Ptr(0),
		contentSizeInBytes: int64Ptr(128),
	}
	entries.addDVEntry(iceberg.NewManifestEntry(
		iceberg.EntryStatusADDED, &snapshotID, nil, nil, dvFile))

	// Equality delete entry
	eqDelFile := &mockDataFile{
		path:        "s3://bucket/data/eq-del-001.parquet",
		contentType: iceberg.EntryContentEqDeletes,
	}
	entries.addEqualityDeleteEntry(iceberg.NewManifestEntry(
		iceberg.EntryStatusADDED, &snapshotID, nil, nil, eqDelFile))

	assert.Len(t, entries.dataEntries, 1)
	assert.Len(t, entries.positionalDeleteEntries, 1)
	assert.Len(t, entries.dvEntries, 1)
	assert.Len(t, entries.equalityDeleteEntries, 1)
}

func TestDVMatchingToDataFiles(t *testing.T) {
	dataFilePath := "s3://bucket/data/data-001.parquet"
	otherDataFilePath := "s3://bucket/data/data-002.parquet"

	dvForData1 := &dvMockDataFile{
		mockDataFile: mockDataFile{
			path:        "s3://bucket/data/dv-001.puffin",
			contentType: iceberg.EntryContentPosDeletes,
		},
		referencedDataFile: strPtr(dataFilePath),
		contentOffset:      int64Ptr(0),
		contentSizeInBytes: int64Ptr(128),
	}

	dvForData2 := &dvMockDataFile{
		mockDataFile: mockDataFile{
			path:        "s3://bucket/data/dv-002.puffin",
			contentType: iceberg.EntryContentPosDeletes,
		},
		referencedDataFile: strPtr(otherDataFilePath),
		contentOffset:      int64Ptr(0),
		contentSizeInBytes: int64Ptr(64),
	}

	snapshotID := int64(1)
	seqNum := int64(1)
	dvEntries := []iceberg.ManifestEntry{
		iceberg.NewManifestEntry(iceberg.EntryStatusADDED, &snapshotID, &seqNum, nil, dvForData1),
		iceberg.NewManifestEntry(iceberg.EntryStatusADDED, &snapshotID, &seqNum, nil, dvForData2),
	}

	dvIndex, err := buildDVIndex(dvEntries)
	assert.NoError(t, err)
	assert.Len(t, dvIndex, 2)

	// Match DVs against data-001 — should only get dv-001
	dataEntry1 := iceberg.NewManifestEntry(iceberg.EntryStatusADDED, &snapshotID, &seqNum, nil,
		&mockDataFile{path: dataFilePath, contentType: iceberg.EntryContentData})
	matched := matchDVToData(dataEntry1, dvIndex)
	assert.Len(t, matched, 1)
	assert.Equal(t, dvForData1.path, matched[0].FilePath())

	// Match DVs against data-002 — should only get dv-002
	dataEntry2 := iceberg.NewManifestEntry(iceberg.EntryStatusADDED, &snapshotID, &seqNum, nil,
		&mockDataFile{path: otherDataFilePath, contentType: iceberg.EntryContentData})
	matched2 := matchDVToData(dataEntry2, dvIndex)
	assert.Len(t, matched2, 1)
	assert.Equal(t, dvForData2.path, matched2[0].FilePath())
}

func TestDVMatchingNoMatch(t *testing.T) {
	snapshotID := int64(1)
	seqNum := int64(1)

	dvFile := &dvMockDataFile{
		mockDataFile: mockDataFile{
			path:        "s3://bucket/data/dv-001.puffin",
			contentType: iceberg.EntryContentPosDeletes,
		},
		referencedDataFile: strPtr("s3://bucket/data/data-999.parquet"),
		contentOffset:      int64Ptr(0),
		contentSizeInBytes: int64Ptr(128),
	}

	dvEntries := []iceberg.ManifestEntry{
		iceberg.NewManifestEntry(iceberg.EntryStatusADDED, &snapshotID, &seqNum, nil, dvFile),
	}

	dvIndex, err := buildDVIndex(dvEntries)
	assert.NoError(t, err)

	dataEntry := iceberg.NewManifestEntry(iceberg.EntryStatusADDED, &snapshotID, &seqNum, nil,
		&mockDataFile{path: "s3://bucket/data/data-001.parquet", contentType: iceberg.EntryContentData})
	matched := matchDVToData(dataEntry, dvIndex)
	assert.Empty(t, matched)
}

func TestBuildDVIndex_RejectsMultipleDVsPerDataFile(t *testing.T) {
	dataFilePath := "s3://bucket/data/data-001.parquet"
	snapshotID := int64(1)
	seqNum := int64(1)

	dv1 := &dvMockDataFile{
		mockDataFile: mockDataFile{
			path:        "s3://bucket/data/dv-001.puffin",
			contentType: iceberg.EntryContentPosDeletes,
		},
		referencedDataFile: strPtr(dataFilePath),
		contentOffset:      int64Ptr(0),
		contentSizeInBytes: int64Ptr(128),
	}

	dv2 := &dvMockDataFile{
		mockDataFile: mockDataFile{
			path:        "s3://bucket/data/dv-002.puffin",
			contentType: iceberg.EntryContentPosDeletes,
		},
		referencedDataFile: strPtr(dataFilePath),
		contentOffset:      int64Ptr(256),
		contentSizeInBytes: int64Ptr(64),
	}

	dvEntries := []iceberg.ManifestEntry{
		iceberg.NewManifestEntry(iceberg.EntryStatusADDED, &snapshotID, &seqNum, nil, dv1),
		iceberg.NewManifestEntry(iceberg.EntryStatusADDED, &snapshotID, &seqNum, nil, dv2),
	}

	_, err := buildDVIndex(dvEntries)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "can't index multiple deletion vectors")
	assert.Contains(t, err.Error(), dataFilePath)
}

func TestMatchDVToData_SequenceNumberGuard(t *testing.T) {
	dataFilePath := "s3://bucket/data/data-001.parquet"
	snapshotID := int64(1)

	dvFile := &dvMockDataFile{
		mockDataFile: mockDataFile{
			path:        "s3://bucket/data/dv-001.puffin",
			contentType: iceberg.EntryContentPosDeletes,
		},
		referencedDataFile: strPtr(dataFilePath),
		contentOffset:      int64Ptr(0),
		contentSizeInBytes: int64Ptr(128),
	}

	dvSeqNum := int64(5)
	dvEntries := []iceberg.ManifestEntry{
		iceberg.NewManifestEntry(iceberg.EntryStatusADDED, &snapshotID, &dvSeqNum, nil, dvFile),
	}

	dvIndex, err := buildDVIndex(dvEntries)
	assert.NoError(t, err)

	tests := []struct {
		name       string
		dataSeqNum int64
		expectDV   bool
	}{
		{"data seq < DV seq — DV applies", 3, true},
		{"data seq == DV seq — DV applies", 5, true},
		{"data seq > DV seq — DV skipped", 7, false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			seqNum := tt.dataSeqNum
			dataEntry := iceberg.NewManifestEntry(iceberg.EntryStatusADDED, &snapshotID, &seqNum, nil,
				&mockDataFile{path: dataFilePath, contentType: iceberg.EntryContentData})
			matched := matchDVToData(dataEntry, dvIndex)
			if tt.expectDV {
				assert.Len(t, matched, 1)
				assert.Equal(t, dvFile.path, matched[0].FilePath())
			} else {
				assert.Empty(t, matched)
			}
		})
	}
}

func TestFileScanTask_DeletionVectorFilesField(t *testing.T) {
	dataFile := &mockDataFile{
		path:        "s3://bucket/data/data-001.parquet",
		contentType: iceberg.EntryContentData,
		filesize:    1024,
	}

	dvFile := &dvMockDataFile{
		mockDataFile: mockDataFile{
			path:        "s3://bucket/data/dv-001.puffin",
			contentType: iceberg.EntryContentPosDeletes,
		},
		referencedDataFile: strPtr("s3://bucket/data/data-001.parquet"),
		contentOffset:      int64Ptr(0),
		contentSizeInBytes: int64Ptr(128),
	}

	task := FileScanTask{
		File:                dataFile,
		DeletionVectorFiles: []iceberg.DataFile{dvFile},
		Start:               0,
		Length:              1024,
	}

	assert.Equal(t, dataFile, task.File)
	assert.Len(t, task.DeletionVectorFiles, 1)
	assert.Equal(t, dvFile, task.DeletionVectorFiles[0])
	assert.NotNil(t, task.DeletionVectorFiles[0].ReferencedDataFile())
	assert.Equal(t, "s3://bucket/data/data-001.parquet", *task.DeletionVectorFiles[0].ReferencedDataFile())
	assert.Empty(t, task.DeleteFiles)
	assert.Empty(t, task.EqualityDeleteFiles)
}
