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
	"bytes"
	"context"
	"fmt"
	"strconv"
	"testing"
	"time"

	"github.com/apache/iceberg-go"
	iceio "github.com/apache/iceberg-go/io"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const snapshotValidationTestID int64 = 41

func v1ValidationEntry(t *testing.T, status iceberg.ManifestEntryStatus, path string) iceberg.ManifestEntry {
	t.Helper()

	data := newTestDataFile(t, iceberg.NewPartitionSpec(), path, nil)
	snapshotID := snapshotValidationTestID

	return iceberg.NewManifestEntry(status, &snapshotID, nil, nil, data)
}

// v1SnapshotValidationTable builds actual v1 Avro manifests and a v1 manifest
// list. No data files are written because the audit only reads metadata.
func v1SnapshotValidationTable(
	t *testing.T,
	embedded bool,
	modify func([]iceberg.ManifestFile) []iceberg.ManifestFile,
	groups ...[]iceberg.ManifestEntry,
) *Table {
	t.Helper()

	ctx := context.Background()
	spec := iceberg.NewPartitionSpec()
	txn, fs := createTestTransactionWithMemIO(t, spec)
	txn.meta.formatVersion = 1

	manifests := make([]iceberg.ManifestFile, 0, len(groups))
	for i, entries := range groups {
		path := fmt.Sprintf("mem://default/table-location/metadata/validate-%d.avro", i)
		var buf bytes.Buffer
		manifest, err := iceberg.WriteManifest(path, &buf, 1, spec, simpleSchema(), snapshotValidationTestID, entries)
		require.NoError(t, err)
		require.NoError(t, fs.WriteFile(ctx, path, buf.Bytes()))
		manifests = append(manifests, manifest)
	}
	if modify != nil {
		manifests = modify(manifests)
	}

	snapshot := Snapshot{
		SnapshotID:  snapshotValidationTestID,
		TimestampMs: time.Now().UnixMilli(),
		Summary:     &Summary{Operation: OpAppend},
	}
	if embedded {
		for _, mf := range manifests {
			snapshot.ManifestLocations = append(snapshot.ManifestLocations, mf.FilePath())
		}
	} else {
		const listPath = "mem://default/table-location/metadata/validation-list.avro"
		var listBuf bytes.Buffer
		require.NoError(t, iceberg.WriteManifestList(1, &listBuf, snapshotValidationTestID,
			nil, nil, 0, manifests))
		require.NoError(t, fs.WriteFile(ctx, listPath, listBuf.Bytes()))
		snapshot.ManifestList = listPath
	}

	require.NoError(t, txn.meta.AddSnapshot(&snapshot))
	require.NoError(t, txn.meta.SetSnapshotRef(MainBranch, snapshotValidationTestID, BranchRef))
	meta, err := txn.meta.Build()
	require.NoError(t, err)

	return New(Identifier{"db", "audit"}, meta, "metadata.json",
		func(context.Context) (iceio.IO, error) { return fs, nil }, nil)
}

func TestValidateSnapshotV1UniqueLiveFiles(t *testing.T) {
	tbl := v1SnapshotValidationTable(t, false, nil,
		[]iceberg.ManifestEntry{
			v1ValidationEntry(t, iceberg.EntryStatusADDED, "file://first.parquet"),
		},
		[]iceberg.ManifestEntry{
			v1ValidationEntry(t, iceberg.EntryStatusEXISTING, "file://second.parquet"),
		},
	)

	report, err := tbl.ValidateSnapshot(context.Background(), snapshotValidationTestID)
	require.NoError(t, err)
	require.NotNil(t, report)
	assert.True(t, report.Valid())
	assert.True(t, report.Complete)
	assert.Equal(t, snapshotValidationTestID, report.SnapshotID)
	assert.Equal(t, int64(2), report.ManifestsChecked)
	assert.Equal(t, int64(2), report.LiveFilesChecked)
	assert.Empty(t, report.Issues)
}

func TestValidateSnapshotV1DuplicateLiveFileAcrossManifests(t *testing.T) {
	tbl := v1SnapshotValidationTable(t, false, nil,
		[]iceberg.ManifestEntry{
			v1ValidationEntry(t, iceberg.EntryStatusADDED, "file://same.parquet"),
		},
		[]iceberg.ManifestEntry{
			v1ValidationEntry(t, iceberg.EntryStatusEXISTING, "file://same.parquet"),
		},
	)

	report, err := tbl.ValidateSnapshot(context.Background(), snapshotValidationTestID)
	require.NoError(t, err)
	assert.False(t, report.Valid())
	assert.True(t, report.Complete)
	assert.Equal(t, int64(2), report.LiveFilesChecked)
	require.Len(t, report.Issues, 1)
	assert.Equal(t, SnapshotIssueDuplicateLiveFile, report.Issues[0].Code)
	assert.Equal(t, "file://same.parquet", report.Issues[0].FilePath)
	assert.Contains(t, report.Issues[0].FirstManifestPath, "validate-0.avro")
	assert.Contains(t, report.Issues[0].ManifestPath, "validate-1.avro")
}

func TestValidateSnapshotV1DuplicateLiveFileWithinManifest(t *testing.T) {
	tbl := v1SnapshotValidationTable(t, false, nil,
		[]iceberg.ManifestEntry{
			v1ValidationEntry(t, iceberg.EntryStatusADDED, "file://same.parquet"),
			v1ValidationEntry(t, iceberg.EntryStatusEXISTING, "file://same.parquet"),
		},
	)

	report, err := tbl.ValidateSnapshot(context.Background(), snapshotValidationTestID)
	require.NoError(t, err)
	assert.False(t, report.Valid())
	require.Len(t, report.Issues, 1)
	assert.Equal(t, SnapshotIssueDuplicateLiveFile, report.Issues[0].Code)
	assert.Equal(t, report.Issues[0].FirstManifestPath, report.Issues[0].ManifestPath)
}

func TestValidateSnapshotV1IgnoresDeletedEntriesForUniqueness(t *testing.T) {
	tbl := v1SnapshotValidationTable(t, false, nil,
		[]iceberg.ManifestEntry{
			v1ValidationEntry(t, iceberg.EntryStatusADDED, "file://same.parquet"),
		},
		[]iceberg.ManifestEntry{
			v1ValidationEntry(t, iceberg.EntryStatusDELETED, "file://same.parquet"),
		},
	)

	report, err := tbl.ValidateSnapshot(context.Background(), snapshotValidationTestID)
	require.NoError(t, err)
	assert.True(t, report.Valid())
	assert.Equal(t, int64(1), report.LiveFilesChecked)
}

func TestValidateSnapshotV1EmbeddedManifests(t *testing.T) {
	tbl := v1SnapshotValidationTable(t, true, nil,
		[]iceberg.ManifestEntry{
			v1ValidationEntry(t, iceberg.EntryStatusADDED, "file://embedded.parquet"),
		},
	)

	report, err := tbl.ValidateSnapshot(context.Background(), snapshotValidationTestID)
	require.NoError(t, err)
	assert.True(t, report.Valid())
	assert.Equal(t, int64(1), report.ManifestsChecked)
	assert.Equal(t, int64(1), report.LiveFilesChecked)
}

func TestValidateSnapshotV1RepeatedManifest(t *testing.T) {
	tbl := v1SnapshotValidationTable(t, false,
		func(manifests []iceberg.ManifestFile) []iceberg.ManifestFile {
			return append(manifests, manifests[0])
		},
		[]iceberg.ManifestEntry{
			v1ValidationEntry(t, iceberg.EntryStatusADDED, "file://same.parquet"),
		},
	)

	report, err := tbl.ValidateSnapshot(context.Background(), snapshotValidationTestID)
	require.NoError(t, err)
	assert.True(t, report.Complete)
	assert.False(t, report.Valid())
	assert.Equal(t, int64(2), report.ManifestsChecked)
	require.Len(t, report.Issues, 2)
	assert.Equal(t, SnapshotIssueDuplicateManifest, report.Issues[0].Code)
	assert.Equal(t, SnapshotIssueDuplicateLiveFile, report.Issues[1].Code)
}

func TestValidateSnapshotV1EmptyManifestList(t *testing.T) {
	tbl := v1SnapshotValidationTable(t, false, nil)

	report, err := tbl.ValidateSnapshot(context.Background(), snapshotValidationTestID)
	require.NoError(t, err)
	assert.True(t, report.Valid())
	assert.Zero(t, report.ManifestsChecked)
	assert.Zero(t, report.LiveFilesChecked)
}

func TestValidateSnapshotV1MissingManifestReturnsIncompleteReport(t *testing.T) {
	tbl := v1SnapshotValidationTable(t, false,
		func(_ []iceberg.ManifestFile) []iceberg.ManifestFile {
			return []iceberg.ManifestFile{
				iceberg.NewManifestBuilder(1,
					"mem://default/table-location/metadata/missing.avro", 123, 0,
					snapshotValidationTestID).AddedFiles(1).Build(),
			}
		},
	)

	report, err := tbl.ValidateSnapshot(context.Background(), snapshotValidationTestID)
	require.Error(t, err)
	require.NotNil(t, report)
	assert.ErrorContains(t, err, "missing.avro")
	assert.False(t, report.Complete)
	assert.False(t, report.Valid())
	assert.Equal(t, int64(1), report.ManifestsChecked)
}

func TestValidateSnapshotCanceledContextReturnsIncompleteReport(t *testing.T) {
	tbl := v1SnapshotValidationTable(t, false, nil)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	report, err := tbl.ValidateSnapshot(ctx, snapshotValidationTestID)
	require.ErrorIs(t, err, context.Canceled)
	require.NotNil(t, report)
	assert.False(t, report.Complete)
	assert.False(t, report.Valid())
	assert.Zero(t, report.ManifestsChecked)
}

func TestValidateSnapshotRejectsUnknownSnapshot(t *testing.T) {
	tbl := v1SnapshotValidationTable(t, false, nil)

	report, err := tbl.ValidateSnapshot(context.Background(), 999)
	require.Error(t, err)
	assert.Nil(t, report)
	assert.ErrorIs(t, err, iceberg.ErrInvalidArgument)
}

func TestValidateSnapshotV1RejectsIncompatibleManifestDescriptor(t *testing.T) {
	manifest := iceberg.NewManifestBuilder(2, "file://manifest.avro", 123, 99,
		snapshotValidationTestID).
		Content(iceberg.ManifestContentDeletes).
		Build()
	report := &SnapshotValidationReport{}
	validateSnapshotSpec(report, manifest.FilePath(), manifest.PartitionSpecID(), map[int]struct{}{0: {}})
	validateSnapshotManifestDescriptor(report, manifest, 1)

	require.Len(t, report.Issues, 3)
	assert.Equal(t, SnapshotIssueUnknownPartitionSpec, report.Issues[0].Code)
	assert.Equal(t, int32(99), report.Issues[0].PartitionSpecID)
	assert.Equal(t, SnapshotIssueIncompatibleManifestVersion, report.Issues[1].Code)
	assert.Equal(t, SnapshotIssueInvalidManifestContent, report.Issues[2].Code)
}

func TestValidateManifestEntryCountOptionalV1(t *testing.T) {
	report := &SnapshotValidationReport{}
	validateManifestEntryCount(report, "manifest", "added", -1, 2, 1)
	validateManifestEntryCount(report, "manifest", "added", 2, 2, 1)
	assert.Empty(t, report.Issues)

	validateManifestEntryCount(report, "manifest", "added", 3, 2, 1)
	require.Len(t, report.Issues, 1)
	assert.Equal(t, SnapshotIssueManifestCountMismatch, report.Issues[0].Code)
	assert.Contains(t, report.Issues[0].Detail, "reports 3, found 2")
}

func TestValidateManifestEntryCountRequiredInV2AndV3(t *testing.T) {
	for _, version := range []int{2, 3} {
		report := &SnapshotValidationReport{}
		validateManifestEntryCount(report, "manifest", "added", -1, 2, version)
		require.Len(t, report.Issues, 1)
		assert.Equal(t, SnapshotIssueManifestCountMismatch, report.Issues[0].Code)
		assert.Contains(t, report.Issues[0].Detail, "invalid count -1")

		report = &SnapshotValidationReport{}
		validateManifestEntryCount(report, "manifest", "deleted", 0, 0, version)
		assert.Empty(t, report.Issues)
	}

	report := &SnapshotValidationReport{}
	validateManifestEntryCount(report, "manifest", "existing", -2, 0, 1)
	require.Len(t, report.Issues, 1)
	assert.Equal(t, SnapshotIssueManifestCountMismatch, report.Issues[0].Code)
}

func writeValidationManifest(
	t *testing.T, fs iceio.WriteFileIO, version int, path string,
	content iceberg.ManifestContent, snapshotID int64,
	added, existing, deleted []iceberg.DataFile,
) iceberg.ManifestFile {
	t.Helper()
	var buf bytes.Buffer
	wr, err := iceberg.NewManifestWriter(
		version, &buf, *iceberg.UnpartitionedSpec, simpleSchema(), snapshotID,
		iceberg.WithManifestWriterContent(content))
	require.NoError(t, err)

	seq := int64(1)
	for _, file := range added {
		require.NoError(t, wr.Add(iceberg.NewManifestEntry(
			iceberg.EntryStatusADDED, &snapshotID, nil, nil, file)))
	}
	for _, file := range existing {
		var dataSeq, fileSeq *int64
		if version > 1 {
			dataSeq, fileSeq = &seq, &seq
		}
		require.NoError(t, wr.Existing(iceberg.NewManifestEntry(
			iceberg.EntryStatusEXISTING, &snapshotID, dataSeq, fileSeq, file)))
	}
	for _, file := range deleted {
		var dataSeq, fileSeq *int64
		if version > 1 {
			dataSeq, fileSeq = &seq, &seq
		}
		require.NoError(t, wr.Delete(iceberg.NewManifestEntry(
			iceberg.EntryStatusDELETED, &snapshotID, dataSeq, fileSeq, file)))
	}
	require.NoError(t, wr.Close())
	require.NoError(t, fs.WriteFile(context.Background(), path, buf.Bytes()))
	manifest, err := wr.ToManifestFile(
		path, int64(buf.Len()), iceberg.WithManifestFileContent(content))
	require.NoError(t, err)

	return manifest
}

func validationTable(
	t *testing.T, version int, fs iceio.IO, snapshots ...Snapshot,
) *Table {
	t.Helper()
	spec := *iceberg.UnpartitionedSpec
	meta, err := NewMetadata(simpleSchema(), &spec, UnsortedSortOrder,
		"mem://validation/table", iceberg.Properties{PropertyFormatVersion: strconv.Itoa(version)})
	require.NoError(t, err)
	builder, err := MetadataBuilderFromBase(meta, "")
	require.NoError(t, err)

	for i := range snapshots {
		require.NoError(t, builder.AddSnapshot(&snapshots[i]))
	}
	require.NotEmpty(t, snapshots)
	require.NoError(t, builder.SetSnapshotRef(
		MainBranch, snapshots[len(snapshots)-1].SnapshotID, BranchRef))
	meta, err = builder.Build()
	require.NoError(t, err)

	return New(Identifier{"db", "diff"}, meta, "mem://validation/table/metadata.json",
		func(context.Context) (iceio.IO, error) { return fs, nil }, nil)
}

func writeValidationManifestListV3(
	t *testing.T, fs iceio.WriteFileIO, path string,
	snapshotID, sequenceNumber int64, manifests []iceberg.ManifestFile,
) string {
	t.Helper()
	out, err := fs.Create(context.Background(), path)
	require.NoError(t, err)
	require.NoError(t, iceberg.WriteManifestList(3, out, snapshotID, nil,
		&sequenceNumber, 0, manifests))
	require.NoError(t, out.Close())

	return path
}

func validationDeletionVector(
	t *testing.T, puffinPath, dataPath string, offset int64,
) iceberg.DataFile {
	t.Helper()
	builder, err := iceberg.NewDataFileBuilder(
		*iceberg.UnpartitionedSpec, iceberg.EntryContentPosDeletes,
		puffinPath, iceberg.PuffinFile, nil, nil, nil, 10, 1024)
	require.NoError(t, err)

	return builder.
		ReferencedDataFile(dataPath).
		ContentOffset(offset).
		ContentSizeInBytes(32).
		Build()
}

func TestValidateSnapshotV3DeletionVectors(t *testing.T) {
	fs := iceio.NewMemFS()
	prefix := "mem://audit/v3/"
	vector1 := validationDeletionVector(t, prefix+"deletes.puffin", prefix+"a.parquet", 100)
	vector2 := validationDeletionVector(t, prefix+"deletes.puffin", prefix+"b.parquet", 200)
	manifest := writeValidationManifest(t, fs, 3, prefix+"deletes.avro", iceberg.ManifestContentDeletes, 1, []iceberg.DataFile{vector1, vector2}, nil, nil)
	list := writeValidationManifestListV3(t, fs, prefix+"list.avro", 1, 1, []iceberg.ManifestFile{manifest})
	zero := int64(0)
	tbl := validationTable(t, 3, fs, Snapshot{SnapshotID: 1, SequenceNumber: 1, TimestampMs: time.Now().UnixMilli(), ManifestList: list, FirstRowID: &zero, AddedRows: &zero})
	report, err := tbl.ValidateSnapshot(context.Background(), 1)
	require.NoError(t, err)
	require.True(t, report.Valid(), "%+v", report.Issues)

	// The same blob referenced twice remains a duplicate.
	duplicate := writeValidationManifest(t, fs, 3, prefix+"duplicate.avro", iceberg.ManifestContentDeletes, 2, []iceberg.DataFile{vector1, vector1}, nil, nil)
	list2 := writeValidationManifestListV3(t, fs, prefix+"list2.avro", 2, 2, []iceberg.ManifestFile{duplicate})
	tbl2 := validationTable(t, 3, fs, Snapshot{SnapshotID: 2, SequenceNumber: 2, TimestampMs: time.Now().UnixMilli(), ManifestList: list2, FirstRowID: &zero, AddedRows: &zero})
	report, err = tbl2.ValidateSnapshot(context.Background(), 2)
	require.NoError(t, err)
	require.Len(t, report.Issues, 1)
	require.Equal(t, SnapshotIssueDuplicateLiveFile, report.Issues[0].Code)
}

func TestValidateSnapshotEmbeddedUsesHeaderSpecID(t *testing.T) {
	entry := v1ValidationEntry(t, iceberg.EntryStatusADDED, "file://embedded.parquet")
	tbl := v1SnapshotValidationTable(t, true, nil, []iceberg.ManifestEntry{entry})
	fs, err := tbl.FS(context.Background())
	require.NoError(t, err)
	writable, ok := fs.(iceio.WriteFileIO)
	require.True(t, ok)
	path := "mem://default/table-location/metadata/validate-0.avro"
	var buf bytes.Buffer
	spec := iceberg.NewPartitionSpecID(42)
	_, err = iceberg.WriteManifest(path, &buf, 1, spec, simpleSchema(), snapshotValidationTestID, []iceberg.ManifestEntry{entry})
	require.NoError(t, err)
	require.NoError(t, writable.WriteFile(context.Background(), path, buf.Bytes()))
	report, err := tbl.ValidateSnapshot(context.Background(), snapshotValidationTestID)
	require.NoError(t, err)
	require.Len(t, report.Issues, 1)
	require.Equal(t, SnapshotIssueUnknownPartitionSpec, report.Issues[0].Code)
	require.Equal(t, int32(42), report.Issues[0].PartitionSpecID)
}

func TestValidateSnapshotChecksDeletedEntries(t *testing.T) {
	fs := iceio.NewMemFS()
	const prefix = "mem://audit/deleted-content/"
	dataFile, err := iceberg.NewDataFileBuilder(*iceberg.UnpartitionedSpec,
		iceberg.EntryContentData, prefix+"old.parquet",
		iceberg.ParquetFile, nil, nil, nil, 1, 1024)
	require.NoError(t, err)
	manifest := writeValidationManifest(t, fs, 3, prefix+"data-manifest.avro",
		iceberg.ManifestContentData, 1, nil, nil, []iceberg.DataFile{dataFile.Build()})
	list := writeValidationManifestListV3(t, fs, prefix+"list.avro", 1, 1,
		[]iceberg.ManifestFile{manifest})
	zero := int64(0)
	tbl := validationTable(t, 3, fs, Snapshot{
		SnapshotID: 1, SequenceNumber: 1, TimestampMs: time.Now().UnixMilli(),
		ManifestList: list, FirstRowID: &zero, AddedRows: &zero,
	})

	report, err := tbl.ValidateSnapshot(t.Context(), 1)
	require.NoError(t, err)
	require.True(t, report.Valid(), "%+v", report.Issues)
	require.Zero(t, report.LiveFilesChecked)
}

func TestValidateSnapshotFileRejectsMixedContent(t *testing.T) {
	// ManifestWriter rejects mixed-content entries before encoding them.
	// Exercise the audit's content rule directly for malformed metadata.
	builder, err := iceberg.NewDataFileBuilder(*iceberg.UnpartitionedSpec,
		iceberg.EntryContentPosDeletes, "mem://audit/deleted-content/pos-deletes.parquet",
		iceberg.ParquetFile, nil, nil, nil, 1, 1024)
	require.NoError(t, err)
	manifest := iceberg.NewManifestBuilder(3,
		"mem://audit/deleted-content/data-manifest.avro", 100, 0, 1).Build()
	report := &SnapshotValidationReport{}
	validateSnapshotFile(report, 3, manifest, builder.Build())
	require.Len(t, report.Issues, 1)
	require.Equal(t, SnapshotIssueInvalidFileContent, report.Issues[0].Code)
}
