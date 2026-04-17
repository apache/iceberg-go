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
	dvEntries := []iceberg.ManifestEntry{
		iceberg.NewManifestEntry(iceberg.EntryStatusADDED, &snapshotID, nil, nil, dvForData1),
		iceberg.NewManifestEntry(iceberg.EntryStatusADDED, &snapshotID, nil, nil, dvForData2),
	}

	// Match DVs against data-001 — should only get dv-001
	var matched []iceberg.DataFile
	for _, del := range dvEntries {
		if del.DataFile().ReferencedDataFile() == nil {
			continue
		}

		if *del.DataFile().ReferencedDataFile() == dataFilePath {
			matched = append(matched, del.DataFile())
		}
	}

	assert.Len(t, matched, 1)
	assert.Equal(t, dvForData1.path, matched[0].FilePath())

	// Match DVs against data-002 — should only get dv-002
	var matched2 []iceberg.DataFile
	for _, del := range dvEntries {
		if del.DataFile().ReferencedDataFile() == nil {
			continue
		}

		if *del.DataFile().ReferencedDataFile() == otherDataFilePath {
			matched2 = append(matched2, del.DataFile())
		}
	}

	assert.Len(t, matched2, 1)
	assert.Equal(t, dvForData2.path, matched2[0].FilePath())
}

func TestDVMatchingNoMatch(t *testing.T) {
	dvFile := &dvMockDataFile{
		mockDataFile: mockDataFile{
			path:        "s3://bucket/data/dv-001.puffin",
			contentType: iceberg.EntryContentPosDeletes,
		},
		referencedDataFile: strPtr("s3://bucket/data/data-999.parquet"),
		contentOffset:      int64Ptr(0),
		contentSizeInBytes: int64Ptr(128),
	}

	snapshotID := int64(1)
	dvEntries := []iceberg.ManifestEntry{
		iceberg.NewManifestEntry(iceberg.EntryStatusADDED, &snapshotID, nil, nil, dvFile),
	}

	var matched []iceberg.DataFile
	for _, del := range dvEntries {
		if del.DataFile().ReferencedDataFile() == nil {
			continue
		}

		if *del.DataFile().ReferencedDataFile() == "s3://bucket/data/data-001.parquet" {
			matched = append(matched, del.DataFile())
		}
	}

	assert.Empty(t, matched)
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
