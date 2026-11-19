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

// Table fixtures shared by this package's tests. The inspect package keeps its
// own copies built on the public API.

import (
	"bytes"
	"context"
	"strconv"
	"testing"

	"github.com/apache/iceberg-go"
	iceio "github.com/apache/iceberg-go/io"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
)

// inspectTestTable builds the shared metadata fixture with the supplied
// identifier and snapshot refs.
func inspectTestTable(identifier Identifier, snapshotRefs map[string]SnapshotRef) *Table {
	const (
		s1 = int64(101)
		s2 = int64(102)
		s3 = int64(103)
		// expired is present in the snapshot log but not the snapshot list.
		expired = int64(999)
	)
	current := s3
	lastPartitionID := 999

	meta := &metadataV2{commonMetadata: commonMetadata{
		FormatVersion:   2,
		UUID:            uuid.New(),
		Loc:             "s3://test/history",
		LastUpdatedMS:   1400,
		LastColumnId:    1,
		SchemaList:      []*iceberg.Schema{iceberg.NewSchema(0)},
		CurrentSchemaID: 0,
		Specs:           []iceberg.PartitionSpec{*iceberg.UnpartitionedSpec},
		DefaultSpecID:   0,
		LastPartitionID: &lastPartitionID,
		Props:           iceberg.Properties{},
		SnapshotList: []Snapshot{
			{SnapshotID: s1, TimestampMs: 1100, ManifestList: "/snap-101.avro"},
			{SnapshotID: s2, ParentSnapshotID: new(s1), TimestampMs: 1200, ManifestList: "/snap-102.avro"},
			{SnapshotID: s3, ParentSnapshotID: new(s1), TimestampMs: 1300, ManifestList: "/snap-103.avro"},
		},
		CurrentSnapshotID: &current,
		SnapshotLog: []SnapshotLogEntry{
			{SnapshotID: s1, TimestampMs: 1100},
			{SnapshotID: s2, TimestampMs: 1200},
			{SnapshotID: s3, TimestampMs: 1300},
			{SnapshotID: expired, TimestampMs: 1400},
		},
		SortOrderList:      []SortOrder{UnsortedSortOrder},
		DefaultSortOrderID: 0,
		SnapshotRefs:       snapshotRefs,
	}}

	return New(identifier, meta, "", nil, nil)
}

// snapshotsTestTable builds a table with two snapshots: a root carrying a
// summary (operation + properties) and a child with no summary at all, to
// exercise both the populated and null operation/summary paths.
func snapshotsTestTable() *Table {
	const (
		s1 = int64(101)
		s2 = int64(102)
	)
	current := s2
	lastPartitionID := 999

	meta := &metadataV2{commonMetadata: commonMetadata{
		FormatVersion:   2,
		UUID:            uuid.New(),
		Loc:             "s3://test/snapshots",
		LastUpdatedMS:   1200,
		LastColumnId:    1,
		SchemaList:      []*iceberg.Schema{iceberg.NewSchema(0)},
		CurrentSchemaID: 0,
		Specs:           []iceberg.PartitionSpec{*iceberg.UnpartitionedSpec},
		DefaultSpecID:   0,
		LastPartitionID: &lastPartitionID,
		Props:           iceberg.Properties{},
		SnapshotList: []Snapshot{
			{
				SnapshotID:   s1,
				TimestampMs:  1100,
				ManifestList: "/snap-101.avro",
				Summary: &Summary{
					Operation:  OpAppend,
					Properties: iceberg.Properties{"added-records": "10", "total-records": "10"},
				},
			},
			// s2 intentionally carries no summary and no manifest-list path.
			{SnapshotID: s2, ParentSnapshotID: new(s1), TimestampMs: 1200},
		},
		CurrentSnapshotID:  &current,
		SortOrderList:      []SortOrder{UnsortedSortOrder},
		DefaultSortOrderID: 0,
		SnapshotRefs:       map[string]SnapshotRef{MainBranch: {SnapshotID: current, Type: BranchRef}},
	}}

	return New(Identifier{"snapshots"}, meta, "", nil, nil)
}

type inspectManifestSpec struct {
	content iceberg.ManifestContent
	entries []iceberg.ManifestEntry
}

func inspectFilesTableWithManifests(t *testing.T, spec iceberg.PartitionSpec, manifests ...inspectManifestSpec) *Table {
	t.Helper()
	const snapshotID = int64(1)

	txn, memIO := createTestTransactionWithMemIO(t, spec)
	manifestFiles := make([]iceberg.ManifestFile, 0, len(manifests))
	for index, manifestSpec := range manifests {
		manifestPath := "mem://default/table-location/metadata/inspect-" + strconv.Itoa(index) + ".avro"
		var manifestBuf bytes.Buffer
		writer, err := iceberg.NewManifestWriter(3, &manifestBuf, spec, simpleSchema(), snapshotID,
			iceberg.WithManifestWriterContent(manifestSpec.content))
		require.NoError(t, err)
		for _, entry := range manifestSpec.entries {
			switch entry.Status() {
			case iceberg.EntryStatusADDED:
				require.NoError(t, writer.Add(entry))
			case iceberg.EntryStatusEXISTING:
				require.NoError(t, writer.Existing(entry))
			case iceberg.EntryStatusDELETED:
				require.NoError(t, writer.Delete(entry))
			default:
				t.Fatalf("unexpected manifest entry status %v", entry.Status())
			}
		}
		require.NoError(t, writer.Close())
		manifest, err := writer.ToManifestFile(manifestPath, int64(manifestBuf.Len()),
			iceberg.WithManifestFileContent(manifestSpec.content))
		require.NoError(t, err)
		require.NoError(t, memIO.WriteFile(context.Background(), manifestPath, manifestBuf.Bytes()))
		manifestFiles = append(manifestFiles, manifest)
	}

	manifestListPath := "mem://default/table-location/metadata/inspect-manifest-list.avro"
	var listBuf bytes.Buffer
	sequenceNumber := int64(1)
	require.NoError(t, iceberg.WriteManifestList(3, &listBuf, snapshotID, nil, &sequenceNumber, 0, manifestFiles))
	require.NoError(t, memIO.WriteFile(context.Background(), manifestListPath, listBuf.Bytes()))

	snapshotIDValue := snapshotID
	txn.meta.snapshotList = []Snapshot{{
		SnapshotID:     snapshotID,
		ManifestList:   manifestListPath,
		SequenceNumber: sequenceNumber,
	}}
	txn.meta.currentSnapshotID = &snapshotIDValue
	built, err := txn.meta.Build()
	require.NoError(t, err)

	return New(Identifier{"db", "inspect-files"}, built, "metadata.json",
		func(context.Context) (iceio.IO, error) { return memIO, nil }, nil)
}

func writeInspectManifest(
	t *testing.T,
	fs iceio.WriteFileIO,
	path string,
	spec iceberg.PartitionSpec,
	schema *iceberg.Schema,
	snapshotID int64,
	content iceberg.ManifestContent,
	entries []iceberg.ManifestEntry,
) iceberg.ManifestFile {
	t.Helper()

	var buf bytes.Buffer
	writer, err := iceberg.NewManifestWriter(2, &buf, spec, schema, snapshotID,
		iceberg.WithManifestWriterContent(content))
	require.NoError(t, err)
	for _, entry := range entries {
		switch entry.Status() {
		case iceberg.EntryStatusDELETED:
			require.NoError(t, writer.Delete(entry))
		case iceberg.EntryStatusEXISTING:
			require.NoError(t, writer.Existing(entry))
		default:
			require.NoError(t, writer.Add(entry))
		}
	}
	require.NoError(t, writer.Close())
	manifest, err := writer.ToManifestFile(path, int64(buf.Len()),
		iceberg.WithManifestFileContent(content))
	require.NoError(t, err)
	require.NoError(t, fs.WriteFile(context.Background(), path, buf.Bytes()))

	return manifest
}

func inspectAllFilesTable(t *testing.T) *Table {
	t.Helper()

	spec := *iceberg.UnpartitionedSpec
	txn, memIO := createTestTransactionWithMemIO(t, spec)
	schema := simpleSchema()
	sequenceOne, sequenceTwo := int64(1), int64(2)
	snapshotOne, snapshotTwo := int64(1), int64(2)

	entry := func(snapshotID, sequenceNumber int64, file iceberg.DataFile) iceberg.ManifestEntry {
		return iceberg.NewManifestEntry(iceberg.EntryStatusADDED, &snapshotID,
			&sequenceNumber, &sequenceNumber, file)
	}
	sharedData := newTestDataFile(t, spec, "mem://default/table-location/data/shared.parquet", nil)
	deletedData := newTestDataFile(t, spec, "mem://default/table-location/data/deleted.parquet", nil)
	newData := newTestDataFile(t, spec, "mem://default/table-location/data/new.parquet", nil)
	deleteFile := newTestPosDeleteFileForSpec(t, spec,
		"mem://default/table-location/data/delete.parquet", nil, sharedData.FilePath())
	deletedSequence := sequenceOne
	deletedEntry := iceberg.NewManifestEntry(iceberg.EntryStatusDELETED, &snapshotOne,
		&deletedSequence, &deletedSequence, deletedData)

	sharedManifest := writeInspectManifest(t, memIO,
		"mem://default/table-location/metadata/shared.avro", spec, schema, snapshotOne,
		iceberg.ManifestContentData, []iceberg.ManifestEntry{
			entry(snapshotOne, sequenceOne, sharedData), deletedEntry,
		})
	// Keep a second manifest with the same file row to pin that all_* tables
	// deduplicate shared manifests, but do not deduplicate rows across distinct
	// manifests.
	duplicateManifest := writeInspectManifest(t, memIO,
		"mem://default/table-location/metadata/duplicate.avro", spec, schema, snapshotOne,
		iceberg.ManifestContentData, []iceberg.ManifestEntry{entry(snapshotOne, sequenceOne, sharedData)})
	newManifest := writeInspectManifest(t, memIO,
		"mem://default/table-location/metadata/new.avro", spec, schema, snapshotTwo,
		iceberg.ManifestContentData, []iceberg.ManifestEntry{entry(snapshotTwo, sequenceTwo, newData)})
	deleteManifest := writeInspectManifest(t, memIO,
		"mem://default/table-location/metadata/delete.avro", spec, schema, snapshotTwo,
		iceberg.ManifestContentDeletes, []iceberg.ManifestEntry{entry(snapshotTwo, sequenceTwo, deleteFile)})

	writeList := func(path string, snapshotID int64, parent *int64, sequenceNumber int64,
		manifests []iceberg.ManifestFile,
	) []iceberg.ManifestFile {
		var buf bytes.Buffer
		require.NoError(t, iceberg.WriteManifestList(2, &buf, snapshotID, parent,
			&sequenceNumber, 0, manifests))
		require.NoError(t, memIO.WriteFile(context.Background(), path, buf.Bytes()))
		written, err := iceberg.ReadManifestList(bytes.NewReader(buf.Bytes()))
		require.NoError(t, err)

		return written
	}
	listOne := "mem://default/table-location/metadata/snap-1-manifest-list.avro"
	listTwo := "mem://default/table-location/metadata/snap-2-manifest-list.avro"
	writtenOne := writeList(listOne, snapshotOne, nil, sequenceOne,
		[]iceberg.ManifestFile{sharedManifest, duplicateManifest})
	writeList(listTwo, snapshotTwo, &snapshotOne, sequenceTwo,
		[]iceberg.ManifestFile{writtenOne[0], newManifest, deleteManifest})

	txn.meta.snapshotList = []Snapshot{
		{SnapshotID: snapshotOne, ManifestList: listOne, SequenceNumber: sequenceOne},
		{SnapshotID: snapshotTwo, ParentSnapshotID: &snapshotOne, ManifestList: listTwo, SequenceNumber: sequenceTwo},
	}
	txn.meta.currentSnapshotID = &snapshotTwo
	built, err := txn.meta.Build()
	require.NoError(t, err)

	return New(Identifier{"db", "tbl"}, built, "metadata.json",
		func(context.Context) (iceio.IO, error) { return memIO, nil }, nil)
}

// testFieldNames returns the top-level field names of an Iceberg schema in order.
func testFieldNames(sc *iceberg.Schema) []string {
	fields := sc.Fields()
	names := make([]string, len(fields))
	for i, f := range fields {
		names[i] = f.Name
	}

	return names
}
