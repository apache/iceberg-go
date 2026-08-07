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
	"time"

	"github.com/DataDog/iceberg-go"
	"github.com/DataDog/iceberg-go/metrics"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestBuildCommitReport(t *testing.T) {
	snap := &Snapshot{
		SnapshotID:     42,
		SequenceNumber: 3,
		Summary: &Summary{
			Operation: OpAppend,
			Properties: iceberg.Properties{
				addedDataFilesKey:      "4",
				deletedDataFilesKey:    "1",
				totalDataFilesKey:      "10",
				addedRecordsKey:        "12345",
				deletedRecordsKey:      "5",
				totalRecordsKey:        "20000",
				addedFileSizeKey:       "4096000",
				removedFileSizeKey:     "100",
				totalFileSizeKey:       "5000000",
				addedPosDeletesKey:     "2",
				addedPosDeleteFilesKey: "1",
				addedEqDeletesKey:      "3",
				addedEqDeleteFilesKey:  "1",
				manifestsCreatedKey:    "6",
				manifestsReplacedKey:   "2",
				manifestsKeptKey:       "4",
				entriesProcessedKey:    "9",
			},
		},
	}

	cr := buildCommitReport("db.tbl", snap, 2, 7*time.Millisecond)

	assert.Equal(t, "db.tbl", cr.TableName)
	assert.Equal(t, int64(42), cr.SnapshotID)
	assert.Equal(t, int64(3), cr.SequenceNumber)
	assert.Equal(t, "append", cr.Operation)

	m := cr.Metrics
	require.NotNil(t, m.TotalDuration)
	assert.Equal(t, (7 * time.Millisecond).Nanoseconds(), m.TotalDuration.TotalDuration)
	require.NotNil(t, m.Attempts)
	assert.Equal(t, int64(2), m.Attempts.Value)

	// Direct (same-name) mappings.
	require.NotNil(t, m.AddedDataFiles)
	assert.Equal(t, int64(4), m.AddedDataFiles.Value)
	require.NotNil(t, m.TotalDataFiles)
	assert.Equal(t, int64(10), m.TotalDataFiles.Value)
	require.NotNil(t, m.AddedRecords)
	assert.Equal(t, int64(12345), m.AddedRecords.Value)
	require.NotNil(t, m.TotalRecords)
	assert.Equal(t, int64(20000), m.TotalRecords.Value)

	// Name-translated mappings (iceberg-go summary key -> Java report name).
	require.NotNil(t, m.RemovedDataFiles, "deleted-data-files -> removed-data-files")
	assert.Equal(t, int64(1), m.RemovedDataFiles.Value)
	require.NotNil(t, m.RemovedRecords, "deleted-records -> removed-records")
	assert.Equal(t, int64(5), m.RemovedRecords.Value)
	require.NotNil(t, m.AddedFilesSizeBytes, "added-files-size -> added-files-size-bytes")
	assert.Equal(t, metrics.UnitBytes, m.AddedFilesSizeBytes.Unit)
	assert.Equal(t, int64(4096000), m.AddedFilesSizeBytes.Value)
	require.NotNil(t, m.RemovedFilesSizeBytes, "removed-files-size -> removed-files-size-bytes")
	assert.Equal(t, int64(100), m.RemovedFilesSizeBytes.Value)
	require.NotNil(t, m.TotalFilesSizeBytes)
	assert.Equal(t, int64(5000000), m.TotalFilesSizeBytes.Value)
	require.NotNil(t, m.AddedPositionalDeletes, "added-position-deletes -> added-positional-deletes")
	assert.Equal(t, int64(2), m.AddedPositionalDeletes.Value)
	require.NotNil(t, m.AddedPositionalDeleteFiles, "added-position-delete-files -> added-positional-delete-files")
	assert.Equal(t, int64(1), m.AddedPositionalDeleteFiles.Value)
	require.NotNil(t, m.AddedEqualityDeletes)
	assert.Equal(t, int64(3), m.AddedEqualityDeletes.Value)
	require.NotNil(t, m.AddedEqualityDeleteFiles)
	assert.Equal(t, int64(1), m.AddedEqualityDeleteFiles.Value)

	// Manifest metrics: entries-processed maps to manifest-entries-processed.
	require.NotNil(t, m.ManifestsCreated)
	assert.Equal(t, int64(6), m.ManifestsCreated.Value)
	require.NotNil(t, m.ManifestsReplaced)
	assert.Equal(t, int64(2), m.ManifestsReplaced.Value)
	require.NotNil(t, m.ManifestsKept)
	assert.Equal(t, int64(4), m.ManifestsKept.Value)
	require.NotNil(t, m.ManifestEntriesProcessed, "entries-processed -> manifest-entries-processed")
	assert.Equal(t, int64(9), m.ManifestEntriesProcessed.Value)

	// Keys absent from the summary are omitted, not zeroed; metrics with no
	// iceberg-go summary key (DVs) stay unset.
	assert.Nil(t, m.RemovedDeleteFiles)
	assert.Nil(t, m.AddedDVs)
}

func TestBuildCommitReportNilSnapshot(t *testing.T) {
	cr := buildCommitReport("db.tbl", nil, 1, time.Millisecond)

	assert.Equal(t, "db.tbl", cr.TableName)
	assert.Equal(t, int64(0), cr.SnapshotID)
	assert.Empty(t, cr.Operation)
	require.NotNil(t, cr.Metrics.Attempts)
	assert.Equal(t, int64(1), cr.Metrics.Attempts.Value)
	assert.Nil(t, cr.Metrics.AddedDataFiles)
}

func TestBuildCommitReportUnparseableValueOmitted(t *testing.T) {
	snap := &Snapshot{
		Summary: &Summary{
			Operation:  OpAppend,
			Properties: iceberg.Properties{addedDataFilesKey: "not-a-number"},
		},
	}
	cr := buildCommitReport("t", snap, 1, time.Millisecond)
	assert.Nil(t, cr.Metrics.AddedDataFiles, "unparseable summary value is omitted")
}
