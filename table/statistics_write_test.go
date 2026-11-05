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
	"context"
	"encoding/json"
	"slices"
	"testing"

	"github.com/apache/iceberg-go"
	iceio "github.com/apache/iceberg-go/io"
	"github.com/apache/iceberg-go/puffin"
	"github.com/stretchr/testify/require"
)

func testStatisticsRegistrationFile() StatisticsFile {
	return StatisticsFile{
		SnapshotID:            103,
		StatisticsPath:        "s3://warehouse/db/stats/metadata/sketch.puffin",
		FileSizeInBytes:       1024,
		FileFooterSizeInBytes: 128,
		BlobMetadata: []BlobMetadata{{
			Type:           puffin.BlobTypeDataSketchesTheta,
			SnapshotID:     103,
			SequenceNumber: 3,
			Fields:         []int32{1},
			Properties:     map[string]string{"creator": "go"},
		}},
	}
}

func TestTransactionStatisticsRegistration(t *testing.T) {
	tbl := inspectTestTable(Identifier{"db", "stats"}, map[string]SnapshotRef{
		MainBranch: {SnapshotID: 103, Type: BranchRef},
	})
	tx := tbl.NewTransaction()
	stats := testStatisticsRegistrationFile()

	require.NoError(t, tx.SetStatistics(stats))
	meta, err := tx.meta.Build()
	require.NoError(t, err)
	require.Equal(t, []StatisticsFile{stats}, slices.Collect(meta.Statistics()))

	// Removing another snapshot must not stage a metadata update.
	stagedUpdateCount := len(tx.meta.updates)
	require.NoError(t, tx.RemoveStatistics(999))
	require.Len(t, tx.meta.updates, stagedUpdateCount)
	meta, err = tx.meta.Build()
	require.NoError(t, err)
	require.Equal(t, []StatisticsFile{stats}, slices.Collect(meta.Statistics()))

	replacement := stats
	replacement.StatisticsPath = "s3://warehouse/db/stats/metadata/replaced.puffin"
	replacement.FileSizeInBytes = 2048
	require.NoError(t, tx.SetStatistics(replacement))
	meta, err = tx.meta.Build()
	require.NoError(t, err)
	require.Equal(t, []StatisticsFile{replacement}, slices.Collect(meta.Statistics()))

	require.NoError(t, tx.RemoveStatistics(stats.SnapshotID))
	meta, err = tx.meta.Build()
	require.NoError(t, err)
	require.Empty(t, slices.Collect(meta.Statistics()))

	// Java's removeStatistics is idempotent if no reference exists.
	updateCount := len(tx.meta.updates)
	require.NoError(t, tx.RemoveStatistics(stats.SnapshotID))
	require.NoError(t, tx.RemoveStatistics(999))
	require.Len(t, tx.meta.updates, updateCount)
}

func TestTransactionStatisticsMetadataRoundTrip(t *testing.T) {
	base, err := getTestTableMetadata("TableMetadataV2Valid.json")
	require.NoError(t, err)
	require.NotEmpty(t, base.Snapshots())
	tbl := New(Identifier{"db", "stats"}, base, "", nil, nil)
	tx := tbl.NewTransaction()
	stats := testStatisticsRegistrationFile()
	stats.SnapshotID = base.Snapshots()[0].SnapshotID
	stats.BlobMetadata[0].SnapshotID = stats.SnapshotID
	stats.BlobMetadata[0].SequenceNumber = base.Snapshots()[0].SequenceNumber
	require.NoError(t, tx.SetStatistics(stats))

	meta, err := tx.meta.Build()
	require.NoError(t, err)
	encoded, err := json.Marshal(meta)
	require.NoError(t, err)
	parsed, err := ParseMetadataBytes(encoded)
	require.NoError(t, err)
	require.Equal(t, []StatisticsFile{stats}, slices.Collect(parsed.Statistics()))
}

func TestTransactionStatisticsCatalogCommit(t *testing.T) {
	base, err := getTestTableMetadata("TableMetadataV2Valid.json")
	require.NoError(t, err)
	require.NotEmpty(t, base.Snapshots())

	cat := &sequentialCatalog{metadata: base}
	ident := Identifier{"db", "stats"}
	fs := iceio.NewMemFS()
	tbl := New(ident, base, "mem://stats/metadata/v1.metadata.json",
		func(context.Context) (iceio.IO, error) { return fs, nil }, cat)
	tx := tbl.NewTransaction()

	stats := testStatisticsRegistrationFile()
	stats.SnapshotID = base.Snapshots()[0].SnapshotID
	stats.BlobMetadata[0].SnapshotID = stats.SnapshotID
	stats.BlobMetadata[0].SequenceNumber = base.Snapshots()[0].SequenceNumber
	require.NoError(t, tx.SetStatistics(stats))
	committed, err := tx.Commit(t.Context())
	require.NoError(t, err)
	require.NotNil(t, committed)

	reloaded, err := cat.LoadTable(t.Context(), ident)
	require.NoError(t, err)
	require.Equal(t, []StatisticsFile{stats}, slices.Collect(reloaded.Metadata().Statistics()))
	require.Equal(t, base.Snapshots(), reloaded.Metadata().Snapshots())
	require.Equal(t, base.CurrentSnapshot(), reloaded.Metadata().CurrentSnapshot())

	removeTx := reloaded.NewTransaction()
	require.NoError(t, removeTx.RemoveStatistics(stats.SnapshotID))
	_, err = removeTx.Commit(t.Context())
	require.NoError(t, err)

	reloaded, err = cat.LoadTable(t.Context(), ident)
	require.NoError(t, err)
	require.Empty(t, slices.Collect(reloaded.Metadata().Statistics()))
	require.Equal(t, base.Snapshots(), reloaded.Metadata().Snapshots())
	require.Equal(t, base.CurrentSnapshot(), reloaded.Metadata().CurrentSnapshot())
}

func TestTransactionSetStatisticsClonesCallerData(t *testing.T) {
	tbl := inspectTestTable(Identifier{"db", "stats"}, map[string]SnapshotRef{
		MainBranch: {SnapshotID: 103, Type: BranchRef},
	})
	tx := tbl.NewTransaction()
	stats := testStatisticsRegistrationFile()
	keyMetadata := "first"
	stats.KeyMetadata = &keyMetadata
	require.NoError(t, tx.SetStatistics(stats))

	keyMetadata = "changed"
	stats.BlobMetadata[0].Fields[0] = 99
	stats.BlobMetadata[0].Properties["creator"] = "changed"
	meta, err := tx.meta.Build()
	require.NoError(t, err)
	registered := slices.Collect(meta.Statistics())
	require.Len(t, registered, 1)
	require.Equal(t, "first", *registered[0].KeyMetadata)
	require.Equal(t, []int32{1}, registered[0].BlobMetadata[0].Fields)
	require.Equal(t, "go", registered[0].BlobMetadata[0].Properties["creator"])
}

func TestTransactionSetStatisticsRejectsInvalidFiles(t *testing.T) {
	tbl := inspectTestTable(Identifier{"db", "stats"}, map[string]SnapshotRef{
		MainBranch: {SnapshotID: 103, Type: BranchRef},
	})
	cases := []struct {
		name   string
		change func(*StatisticsFile)
	}{
		{"missing snapshot ID", func(s *StatisticsFile) { s.SnapshotID = 0 }},
		{"negative snapshot ID", func(s *StatisticsFile) { s.SnapshotID = -1 }},
		{"missing path", func(s *StatisticsFile) { s.StatisticsPath = "" }},
		{"nonpositive file size", func(s *StatisticsFile) { s.FileSizeInBytes = 0 }},
		{"negative file size", func(s *StatisticsFile) { s.FileSizeInBytes = -1 }},
		{"nonpositive footer size", func(s *StatisticsFile) { s.FileFooterSizeInBytes = 0 }},
		{"negative footer size", func(s *StatisticsFile) { s.FileFooterSizeInBytes = -1 }},
		{"footer exceeds file", func(s *StatisticsFile) { s.FileFooterSizeInBytes = 1025 }},
		{"empty blob type", func(s *StatisticsFile) { s.BlobMetadata[0].Type = "" }},
		{"empty second blob type", func(s *StatisticsFile) {
			s.BlobMetadata = append(s.BlobMetadata, BlobMetadata{})
		}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			tx := tbl.NewTransaction()
			stats := testStatisticsRegistrationFile()
			tc.change(&stats)
			err := tx.SetStatistics(stats)
			require.ErrorIs(t, err, iceberg.ErrInvalidArgument)
			if stats.SnapshotID <= 0 {
				require.ErrorContains(t, err, "snapshot ID")
			}
			require.Empty(t, tx.meta.updates)
		})
	}

	var nilTx *Transaction
	require.Error(t, nilTx.SetStatistics(testStatisticsRegistrationFile()))
	require.Error(t, nilTx.RemoveStatistics(103))
}

func TestTransactionRemoveStatisticsNoOp(t *testing.T) {
	tbl := inspectTestTable(Identifier{"db", "stats"}, map[string]SnapshotRef{
		MainBranch: {SnapshotID: 103, Type: BranchRef},
	})
	tx := tbl.NewTransaction()

	require.NoError(t, tx.RemoveStatistics(103))
	require.NoError(t, tx.RemoveStatistics(999))
	require.Empty(t, tx.meta.updates)

	commit, err := tx.TableCommit()
	require.NoError(t, err)
	require.Empty(t, commit.Updates)

	// A no-op must not try to write a new metadata file through a catalog.
	committed, err := tx.Commit(t.Context())
	require.NoError(t, err)
	require.Same(t, tbl.metadata, committed.metadata)
	require.ErrorContains(t, tx.RemoveStatistics(103), "already been committed")
}

func TestTransactionSetStatisticsAllowsUnretainedSnapshot(t *testing.T) {
	tbl := inspectTestTable(Identifier{"db", "stats"}, map[string]SnapshotRef{
		MainBranch: {SnapshotID: 103, Type: BranchRef},
	})
	tx := tbl.NewTransaction()
	stats := testStatisticsRegistrationFile()
	stats.SnapshotID = 999

	// Java's TableMetadata.Builder.setStatistics does not require the
	// statistics snapshot to be present in the current metadata.
	require.NoError(t, tx.SetStatistics(stats))
	meta, err := tx.meta.Build()
	require.NoError(t, err)
	require.Equal(t, []StatisticsFile{stats}, slices.Collect(meta.Statistics()))
}

func TestTransactionSetStatisticsAcceptsNilCollections(t *testing.T) {
	tbl := inspectTestTable(Identifier{"db", "stats"}, map[string]SnapshotRef{
		MainBranch: {SnapshotID: 103, Type: BranchRef},
	})
	cases := []struct {
		name      string
		change    func(*StatisticsFile)
		wantBlobs int
	}{
		{"nil blob metadata", func(s *StatisticsFile) { s.BlobMetadata = nil }, 0},
		{"nil blob fields", func(s *StatisticsFile) { s.BlobMetadata[0].Fields = nil }, 1},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			tx := tbl.NewTransaction()
			stats := testStatisticsRegistrationFile()
			tc.change(&stats)
			require.NoError(t, tx.SetStatistics(stats))

			meta, err := tx.meta.Build()
			require.NoError(t, err)
			encoded, err := json.Marshal(meta)
			require.NoError(t, err)

			var payload struct {
				Statistics []struct {
					Blobs json.RawMessage `json:"blob-metadata"`
				} `json:"statistics"`
			}
			require.NoError(t, json.Unmarshal(encoded, &payload))
			require.Len(t, payload.Statistics, 1)
			if tc.wantBlobs == 0 {
				require.JSONEq(t, "[]", string(payload.Statistics[0].Blobs))
			} else {
				var blobs []struct {
					Fields json.RawMessage `json:"fields"`
				}
				require.NoError(t, json.Unmarshal(payload.Statistics[0].Blobs, &blobs))
				require.Len(t, blobs, 1)
				require.JSONEq(t, "[]", string(blobs[0].Fields))
			}

			parsed, err := ParseMetadataBytes(encoded)
			require.NoError(t, err)
			registered := slices.Collect(parsed.Statistics())
			require.Len(t, registered, 1)
			require.Len(t, registered[0].BlobMetadata, tc.wantBlobs)
			if tc.wantBlobs == 1 {
				require.Empty(t, registered[0].BlobMetadata[0].Fields)
			}
		})
	}
}

func TestTransactionSetStatisticsOmitsUnsetBlobProperties(t *testing.T) {
	tbl := inspectTestTable(Identifier{"db", "stats"}, map[string]SnapshotRef{
		MainBranch: {SnapshotID: 103, Type: BranchRef},
	})
	tx := tbl.NewTransaction()
	stats := testStatisticsRegistrationFile()
	stats.BlobMetadata[0].Type = "vendor-stats-v1"
	stats.BlobMetadata[0].Fields = []int32{}
	stats.BlobMetadata[0].Properties = nil

	require.NoError(t, tx.SetStatistics(stats))
	meta, err := tx.meta.Build()
	require.NoError(t, err)
	encoded, err := json.Marshal(meta)
	require.NoError(t, err)

	var payload struct {
		Statistics []struct {
			Blobs []map[string]json.RawMessage `json:"blob-metadata"`
		} `json:"statistics"`
	}
	require.NoError(t, json.Unmarshal(encoded, &payload))
	require.Len(t, payload.Statistics, 1)
	require.Len(t, payload.Statistics[0].Blobs, 1)
	require.NotContains(t, payload.Statistics[0].Blobs[0], "properties",
		"optional properties must be omitted, not encoded as null")

	parsed, err := ParseMetadataBytes(encoded)
	require.NoError(t, err)
	require.Equal(t, []StatisticsFile{stats}, slices.Collect(parsed.Statistics()))
}
