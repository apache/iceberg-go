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
	"context"
	"errors"
	"fmt"
	"strconv"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/decimal128"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/arrow-go/v18/arrow/scalar"
	"github.com/apache/iceberg-go"
	iceio "github.com/apache/iceberg-go/io"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
)

// historyTestTable builds a table with a rollback in its snapshot log.
//
// Lineage:
//
//	S1 (root) ──▶ S2 (rolled back)
//	  └────────▶ S3 (current)
//
// The snapshot log records the chronological order in which each snapshot
// became current: S1, then S2, then (after rollback and re-append) S3. S3's
// parent is S1, so S2 is NOT an ancestor of the current snapshot. A trailing
// log entry references an expired snapshot (id 999, absent from the snapshot
// list) to exercise the null-parent path.
func historyTestTable() *Table {
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
			{SnapshotID: s2, ParentSnapshotID: int64Ptr(s1), TimestampMs: 1200, ManifestList: "/snap-102.avro"},
			{SnapshotID: s3, ParentSnapshotID: int64Ptr(s1), TimestampMs: 1300, ManifestList: "/snap-103.avro"},
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
		SnapshotRefs:       map[string]SnapshotRef{MainBranch: {SnapshotID: current, SnapshotRefType: BranchRef}},
	}}

	return New(Identifier{"history"}, meta, "", nil, nil)
}

// collectRecord drains a RecordReader into a single record for assertions and
// asserts the reader holds exactly one batch, matching History's contract.
func collectRecord(t *testing.T, rr array.RecordReader) arrow.RecordBatch {
	t.Helper()
	require.True(t, rr.Next(), "expected at least one record batch")
	rec := rr.RecordBatch()
	rec.Retain()
	require.False(t, rr.Next(), "expected exactly one record batch")

	return rec
}

func inspectTableWithManifestList(t *testing.T, spec iceberg.PartitionSpec, version int, manifests []iceberg.ManifestFile) *Table {
	return inspectTableWithManifestListAndSchemas(t, simpleSchema(), nil, spec, version, manifests)
}

func inspectTableWithManifestListAndSchemas(t *testing.T, initialSchema, currentSchema *iceberg.Schema,
	spec iceberg.PartitionSpec, version int, manifests []iceberg.ManifestFile,
) *Table {
	t.Helper()

	const snapshotID = int64(1)
	ctx := context.Background()
	fs, err := iceio.LoadFS(ctx, nil, "mem://default/table-location")
	require.NoError(t, err)
	memIO := fs.(iceio.WriteFileIO)
	meta, err := NewMetadata(initialSchema, &spec, UnsortedSortOrder, "mem://default/table-location", nil)
	require.NoError(t, err)
	tbl := New(Identifier{"db", "tbl"}, meta, "metadata.json",
		func(context.Context) (iceio.IO, error) { return memIO, nil }, nil)
	txn := tbl.NewTransaction()
	if currentSchema != nil {
		require.NoError(t, txn.meta.AddSchema(currentSchema))
		require.NoError(t, txn.meta.SetCurrentSchemaID(currentSchema.ID))
	}

	manifestListPath := "mem://default/table-location/metadata/snap-1-manifest-list.avro"
	sequenceNumber := int64(1)
	var listSequenceNumber *int64
	if version > 1 {
		listSequenceNumber = &sequenceNumber
	}

	var listBuf bytes.Buffer
	require.NoError(t, iceberg.WriteManifestList(version, &listBuf, snapshotID, nil,
		listSequenceNumber, 0, manifests))
	require.NoError(t, memIO.WriteFile(manifestListPath, listBuf.Bytes()))

	snapID := snapshotID
	txn.meta.snapshotList = []Snapshot{{
		SnapshotID:     snapshotID,
		ManifestList:   manifestListPath,
		SequenceNumber: sequenceNumber,
	}}
	txn.meta.currentSnapshotID = &snapID
	built, err := txn.meta.Build()
	require.NoError(t, err)

	return New(Identifier{"db", "tbl"}, built, "metadata.json",
		func(context.Context) (iceio.IO, error) { return memIO, nil }, nil)
}

func TestInspectHistorySchema(t *testing.T) {
	sc := HistorySchema()

	require.Equal(t, []string{"made_current_at", "snapshot_id", "parent_id", "is_current_ancestor"},
		testFieldNames(sc))

	fields := sc.Fields()
	require.Equal(t, 1, fields[0].ID)
	require.Equal(t, 2, fields[1].ID)
	require.Equal(t, 3, fields[2].ID)
	require.Equal(t, 4, fields[3].ID)

	require.True(t, fields[0].Required, "made_current_at is required")
	require.True(t, fields[1].Required, "snapshot_id is required")
	require.False(t, fields[2].Required, "parent_id is optional")
	require.True(t, fields[3].Required, "is_current_ancestor is required")

	require.Equal(t, iceberg.PrimitiveTypes.TimestampTz, fields[0].Type)
	require.Equal(t, iceberg.PrimitiveTypes.Int64, fields[1].Type)
	require.Equal(t, iceberg.PrimitiveTypes.Int64, fields[2].Type)
	require.Equal(t, iceberg.PrimitiveTypes.Bool, fields[3].Type)
}

func TestInspectHistory(t *testing.T) {
	tbl := historyTestTable()

	rr, err := tbl.Inspect().History(context.Background())
	require.NoError(t, err)
	defer rr.Release()

	rec := collectRecord(t, rr)
	defer rec.Release()

	require.EqualValues(t, 4, rec.NumRows())
	require.EqualValues(t, 4, rec.NumCols())

	// made_current_at must be timestamptz: microsecond precision, UTC.
	tsType, ok := rec.Schema().Field(0).Type.(*arrow.TimestampType)
	require.True(t, ok, "made_current_at must be an Arrow timestamp")
	require.Equal(t, arrow.Microsecond, tsType.Unit)
	require.Equal(t, "UTC", tsType.TimeZone)

	madeCurrentAt := rec.Column(0).(*array.Timestamp)
	snapshotID := rec.Column(1).(*array.Int64)
	parentID := rec.Column(2).(*array.Int64)
	isCurrentAncestor := rec.Column(3).(*array.Boolean)

	// made_current_at is milliseconds promoted to microseconds.
	require.EqualValues(t, 1100*1000, madeCurrentAt.Value(0))
	require.EqualValues(t, 1400*1000, madeCurrentAt.Value(3))

	require.EqualValues(t, 101, snapshotID.Value(0))
	require.EqualValues(t, 102, snapshotID.Value(1))
	require.EqualValues(t, 103, snapshotID.Value(2))
	require.EqualValues(t, 999, snapshotID.Value(3))

	// S1 is a root: no parent.
	require.True(t, parentID.IsNull(0), "S1 has no parent")
	// S2 and S3 both descend from S1.
	require.False(t, parentID.IsNull(1))
	require.EqualValues(t, 101, parentID.Value(1))
	require.False(t, parentID.IsNull(2))
	require.EqualValues(t, 101, parentID.Value(2))
	// The expired snapshot cannot be resolved, so its parent is null.
	require.True(t, parentID.IsNull(3), "expired snapshot resolves to a null parent")

	// The current snapshot is S3, whose ancestry is {S1, S3}. S2 was rolled
	// back and the expired entry is off-lineage: both are non-ancestors.
	require.True(t, isCurrentAncestor.Value(0), "S1 is an ancestor of the current snapshot")
	require.False(t, isCurrentAncestor.Value(1), "rolled-back S2 is not an ancestor")
	require.True(t, isCurrentAncestor.Value(2), "current snapshot S3 is its own ancestor")
	require.False(t, isCurrentAncestor.Value(3), "expired snapshot is not an ancestor")
}

// TestInspectHistoryEmpty covers a table with no snapshot log (e.g. freshly
// created): History must yield an empty, well-formed record.
func TestInspectHistoryEmpty(t *testing.T) {
	lastPartitionID := 999
	meta := &metadataV2{commonMetadata: commonMetadata{
		FormatVersion:      2,
		UUID:               uuid.New(),
		Loc:                "s3://test/empty",
		LastUpdatedMS:      1000,
		LastColumnId:       1,
		SchemaList:         []*iceberg.Schema{iceberg.NewSchema(0)},
		CurrentSchemaID:    0,
		Specs:              []iceberg.PartitionSpec{*iceberg.UnpartitionedSpec},
		DefaultSpecID:      0,
		LastPartitionID:    &lastPartitionID,
		Props:              iceberg.Properties{},
		SortOrderList:      []SortOrder{UnsortedSortOrder},
		DefaultSortOrderID: 0,
		SnapshotRefs:       map[string]SnapshotRef{},
	}}
	tbl := New(Identifier{"empty"}, meta, "", nil, nil)

	rr, err := tbl.Inspect().History(context.Background())
	require.NoError(t, err)
	defer rr.Release()

	rec := collectRecord(t, rr)
	defer rec.Release()

	require.EqualValues(t, 0, rec.NumRows())
	require.EqualValues(t, 4, rec.NumCols())
}

// TestInspectHistoryNoCurrentSnapshot covers a table that has a populated
// snapshot list and log but no current snapshot (e.g. after the current ref
// was cleared). With no lineage to anchor against, every log entry is a
// non-ancestor, but the rows are still emitted.
func TestInspectHistoryNoCurrentSnapshot(t *testing.T) {
	const s1 = int64(101)
	lastPartitionID := 999
	meta := &metadataV2{commonMetadata: commonMetadata{
		FormatVersion:   2,
		UUID:            uuid.New(),
		Loc:             "s3://test/no-current",
		LastUpdatedMS:   1100,
		LastColumnId:    1,
		SchemaList:      []*iceberg.Schema{iceberg.NewSchema(0)},
		CurrentSchemaID: 0,
		Specs:           []iceberg.PartitionSpec{*iceberg.UnpartitionedSpec},
		DefaultSpecID:   0,
		LastPartitionID: &lastPartitionID,
		Props:           iceberg.Properties{},
		SnapshotList: []Snapshot{
			{SnapshotID: s1, TimestampMs: 1100, ManifestList: "/snap-101.avro"},
		},
		// CurrentSnapshotID intentionally nil.
		SnapshotLog:        []SnapshotLogEntry{{SnapshotID: s1, TimestampMs: 1100}},
		SortOrderList:      []SortOrder{UnsortedSortOrder},
		DefaultSortOrderID: 0,
		SnapshotRefs:       map[string]SnapshotRef{},
	}}
	tbl := New(Identifier{"no-current"}, meta, "", nil, nil)

	rr, err := tbl.Inspect().History(context.Background())
	require.NoError(t, err)
	defer rr.Release()

	rec := collectRecord(t, rr)
	defer rec.Release()

	require.EqualValues(t, 1, rec.NumRows())
	isCurrentAncestor := rec.Column(3).(*array.Boolean)
	require.False(t, isCurrentAncestor.Value(0), "no current snapshot means no ancestors")
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
			{SnapshotID: s2, ParentSnapshotID: int64Ptr(s1), TimestampMs: 1200},
		},
		CurrentSnapshotID:  &current,
		SortOrderList:      []SortOrder{UnsortedSortOrder},
		DefaultSortOrderID: 0,
		SnapshotRefs:       map[string]SnapshotRef{MainBranch: {SnapshotID: current, SnapshotRefType: BranchRef}},
	}}

	return New(Identifier{"snapshots"}, meta, "", nil, nil)
}

func TestInspectSnapshotsSchema(t *testing.T) {
	sc := SnapshotsSchema()

	require.Equal(t,
		[]string{"committed_at", "snapshot_id", "parent_id", "operation", "manifest_list", "summary"},
		testFieldNames(sc))

	fields := sc.Fields()
	for i := range fields {
		require.Equal(t, i+1, fields[i].ID)
	}

	require.True(t, fields[0].Required, "committed_at is required")
	require.True(t, fields[1].Required, "snapshot_id is required")
	require.False(t, fields[2].Required, "parent_id is optional")
	require.False(t, fields[3].Required, "operation is optional")
	require.False(t, fields[4].Required, "manifest_list is optional")
	require.False(t, fields[5].Required, "summary is optional")

	m, ok := fields[5].Type.(*iceberg.MapType)
	require.True(t, ok, "summary must be a map")
	require.Equal(t, 7, m.KeyID)
	require.Equal(t, 8, m.ValueID)
	require.Equal(t, iceberg.PrimitiveTypes.String, m.KeyType)
	require.Equal(t, iceberg.PrimitiveTypes.String, m.ValueType)
}

func TestInspectSnapshots(t *testing.T) {
	tbl := snapshotsTestTable()

	rr, err := tbl.Inspect().Snapshots(context.Background())
	require.NoError(t, err)
	defer rr.Release()

	rec := collectRecord(t, rr)
	defer rec.Release()

	require.EqualValues(t, 2, rec.NumRows())
	require.EqualValues(t, 6, rec.NumCols())

	// committed_at must be timestamptz: microsecond precision, UTC.
	tsType, ok := rec.Schema().Field(0).Type.(*arrow.TimestampType)
	require.True(t, ok, "committed_at must be an Arrow timestamp")
	require.Equal(t, arrow.Microsecond, tsType.Unit)
	require.Equal(t, "UTC", tsType.TimeZone)

	committedAt := rec.Column(0).(*array.Timestamp)
	snapshotID := rec.Column(1).(*array.Int64)
	parentID := rec.Column(2).(*array.Int64)
	operation := rec.Column(3).(*array.String)
	manifestList := rec.Column(4).(*array.String)
	summary := rec.Column(5).(*array.Map)

	require.EqualValues(t, 1100*1000, committedAt.Value(0))
	require.EqualValues(t, 1200*1000, committedAt.Value(1))

	require.EqualValues(t, 101, snapshotID.Value(0))
	require.EqualValues(t, 102, snapshotID.Value(1))

	require.True(t, parentID.IsNull(0), "root snapshot has no parent")
	require.False(t, parentID.IsNull(1))
	require.EqualValues(t, 101, parentID.Value(1))

	// s1 has a manifest-list path; s2 has none and must render null, not "".
	require.False(t, manifestList.IsNull(0))
	require.Equal(t, "/snap-101.avro", manifestList.Value(0))
	require.True(t, manifestList.IsNull(1), "snapshot without a manifest list has null manifest_list")

	// s1 has a summary; s2 does not.
	require.False(t, operation.IsNull(0))
	require.Equal(t, "append", operation.Value(0))
	require.True(t, operation.IsNull(1), "snapshot without a summary has null operation")

	// The summary map mirrors the stored summary: operation folded in with the
	// extra properties.
	require.False(t, summary.IsNull(0))
	require.Equal(t,
		map[string]string{"operation": "append", "added-records": "10", "total-records": "10"},
		mapRow(t, summary, 0))
	require.True(t, summary.IsNull(1), "snapshot without a summary has null summary")
}

// TestInspectSnapshotsEmpty covers a table with no snapshots.
func TestInspectSnapshotsEmpty(t *testing.T) {
	lastPartitionID := 999
	meta := &metadataV2{commonMetadata: commonMetadata{
		FormatVersion:      2,
		UUID:               uuid.New(),
		Loc:                "s3://test/empty",
		LastUpdatedMS:      1000,
		LastColumnId:       1,
		SchemaList:         []*iceberg.Schema{iceberg.NewSchema(0)},
		CurrentSchemaID:    0,
		Specs:              []iceberg.PartitionSpec{*iceberg.UnpartitionedSpec},
		DefaultSpecID:      0,
		LastPartitionID:    &lastPartitionID,
		Props:              iceberg.Properties{},
		SortOrderList:      []SortOrder{UnsortedSortOrder},
		DefaultSortOrderID: 0,
		SnapshotRefs:       map[string]SnapshotRef{},
	}}
	tbl := New(Identifier{"empty"}, meta, "", nil, nil)

	rr, err := tbl.Inspect().Snapshots(context.Background())
	require.NoError(t, err)
	defer rr.Release()

	rec := collectRecord(t, rr)
	defer rec.Release()

	require.EqualValues(t, 0, rec.NumRows())
	require.EqualValues(t, 6, rec.NumCols())
}

func TestInspectMetadataLogEntriesSchema(t *testing.T) {
	sc := MetadataLogEntriesSchema()

	require.Equal(t,
		[]string{"timestamp", "file", "latest_snapshot_id", "latest_schema_id", "latest_sequence_number"},
		testFieldNames(sc))

	fields := sc.Fields()
	for i := range fields {
		require.Equal(t, i+1, fields[i].ID)
	}

	require.True(t, fields[0].Required, "timestamp is required")
	require.True(t, fields[1].Required, "file is required")
	require.False(t, fields[2].Required, "latest_snapshot_id is optional")
	require.False(t, fields[3].Required, "latest_schema_id is optional")
	require.False(t, fields[4].Required, "latest_sequence_number is optional")

	require.Equal(t, iceberg.PrimitiveTypes.TimestampTz, fields[0].Type)
	require.Equal(t, iceberg.PrimitiveTypes.String, fields[1].Type)
	require.Equal(t, iceberg.PrimitiveTypes.Int64, fields[2].Type)
	require.Equal(t, iceberg.PrimitiveTypes.Int32, fields[3].Type)
	require.Equal(t, iceberg.PrimitiveTypes.Int64, fields[4].Type)
}

func TestInspectMetadataLogEntries(t *testing.T) {
	const (
		s1      = int64(101)
		s2      = int64(102)
		expired = int64(999)
	)
	schema1 := 1
	schema2 := 2
	current := s2
	lastPartitionID := 999
	meta := &metadataV2{commonMetadata: commonMetadata{
		FormatVersion: 2,
		UUID:          uuid.New(),
		Loc:           "s3://test/metadata-log-entries",
		LastUpdatedMS: 4000,
		LastColumnId:  1,
		SchemaList: []*iceberg.Schema{
			iceberg.NewSchema(0),
			iceberg.NewSchema(schema1),
			iceberg.NewSchema(schema2),
		},
		CurrentSchemaID: schema2,
		Specs:           []iceberg.PartitionSpec{*iceberg.UnpartitionedSpec},
		DefaultSpecID:   0,
		LastPartitionID: &lastPartitionID,
		Props:           iceberg.Properties{},
		MetadataLog: []MetadataLogEntry{
			{MetadataFile: "/metadata/v1.json", TimestampMs: 1000},
			{MetadataFile: "/metadata/v2.json", TimestampMs: 2000},
		},
		SnapshotList: []Snapshot{
			{SnapshotID: s1, SequenceNumber: 7, TimestampMs: 1500, SchemaID: &schema1},
			{SnapshotID: s2, SequenceNumber: 8, TimestampMs: 3000, SchemaID: &schema2},
		},
		SnapshotLog: []SnapshotLogEntry{
			{SnapshotID: s1, TimestampMs: 1500},
			{SnapshotID: s2, TimestampMs: 3000},
			{SnapshotID: expired, TimestampMs: 3500},
		},
		CurrentSnapshotID:  &current,
		SortOrderList:      []SortOrder{UnsortedSortOrder},
		DefaultSortOrderID: 0,
		SnapshotRefs:       map[string]SnapshotRef{MainBranch: {SnapshotID: s2, SnapshotRefType: BranchRef}},
	}}
	tbl := New(Identifier{"metadata-log-entries"}, meta, "/metadata/v3.json", nil, nil)

	rr, err := tbl.Inspect().MetadataLogEntries(context.Background())
	require.NoError(t, err)
	defer rr.Release()

	rec := collectRecord(t, rr)
	defer rec.Release()

	require.EqualValues(t, 3, rec.NumRows())
	require.EqualValues(t, 5, rec.NumCols())

	tsType, ok := rec.Schema().Field(0).Type.(*arrow.TimestampType)
	require.True(t, ok, "timestamp must be an Arrow timestamp")
	require.Equal(t, arrow.Microsecond, tsType.Unit)
	require.Equal(t, "UTC", tsType.TimeZone)

	timestamp := rec.Column(0).(*array.Timestamp)
	file := rec.Column(1).(*array.String)
	latestSnapshotID := rec.Column(2).(*array.Int64)
	latestSchemaID := rec.Column(3).(*array.Int32)
	latestSequenceNumber := rec.Column(4).(*array.Int64)

	require.EqualValues(t, 1000*1000, timestamp.Value(0))
	require.EqualValues(t, 2000*1000, timestamp.Value(1))
	require.EqualValues(t, 4000*1000, timestamp.Value(2))
	require.Equal(t, "/metadata/v1.json", file.Value(0))
	require.Equal(t, "/metadata/v2.json", file.Value(1))
	require.Equal(t, "/metadata/v3.json", file.Value(2))

	// No snapshot had been committed at the first metadata-file timestamp.
	require.True(t, latestSnapshotID.IsNull(0))
	require.True(t, latestSchemaID.IsNull(0))
	require.True(t, latestSequenceNumber.IsNull(0))

	require.EqualValues(t, s1, latestSnapshotID.Value(1))
	require.EqualValues(t, schema1, latestSchemaID.Value(1))
	require.EqualValues(t, 7, latestSequenceNumber.Value(1))
	// The snapshot log can retain an entry after the snapshot itself expires.
	// Keep its ID while leaving details that require the missing snapshot null.
	require.EqualValues(t, expired, latestSnapshotID.Value(2))
	require.True(t, latestSchemaID.IsNull(2))
	require.True(t, latestSequenceNumber.IsNull(2))
}

func TestLatestSnapshotAtKeepsExpiredSnapshotID(t *testing.T) {
	const expiredSnapshot = int64(101)
	meta := &metadataV2{commonMetadata: commonMetadata{
		SnapshotLog: []SnapshotLogEntry{{SnapshotID: expiredSnapshot, TimestampMs: 2000}},
	}}

	snapshotID, snapshot, found := latestSnapshotAt(meta, 2500)
	require.True(t, found)
	require.Equal(t, expiredSnapshot, snapshotID)
	require.Nil(t, snapshot)
}

func TestLatestSnapshotAtScansAllSnapshotLogEntries(t *testing.T) {
	const (
		firstSnapshot  = int64(101)
		secondSnapshot = int64(102)
		lateSnapshot   = int64(103)
	)
	meta := &metadataV2{commonMetadata: commonMetadata{
		SnapshotList: []Snapshot{
			{SnapshotID: firstSnapshot},
			{SnapshotID: secondSnapshot},
			{SnapshotID: lateSnapshot},
		},
		// The 500ms inversion is intentional and models clock skew within the
		// metadata validator's one-minute tolerance.
		SnapshotLog: []SnapshotLogEntry{
			{SnapshotID: firstSnapshot, TimestampMs: 2000},
			{SnapshotID: secondSnapshot, TimestampMs: 3000},
			{SnapshotID: lateSnapshot, TimestampMs: 2500},
		},
	}}

	snapshotID, snapshot, found := latestSnapshotAt(meta, 2600)
	require.True(t, found)
	require.NotNil(t, snapshot)
	require.Equal(t, lateSnapshot, snapshotID)
	require.Equal(t, lateSnapshot, snapshot.SnapshotID)
}

func TestLatestSnapshotAtUsesFirstEntryForEqualTimestamps(t *testing.T) {
	const (
		firstSnapshot  = int64(101)
		secondSnapshot = int64(102)
		timestamp      = int64(2000)
	)
	meta := &metadataV2{commonMetadata: commonMetadata{
		SnapshotList: []Snapshot{
			{SnapshotID: firstSnapshot},
			{SnapshotID: secondSnapshot},
		},
		SnapshotLog: []SnapshotLogEntry{
			{SnapshotID: firstSnapshot, TimestampMs: timestamp},
			{SnapshotID: secondSnapshot, TimestampMs: timestamp},
		},
	}}

	snapshotID, snapshot, found := latestSnapshotAt(meta, timestamp)
	require.True(t, found)
	require.NotNil(t, snapshot)
	require.Equal(t, firstSnapshot, snapshotID)
	require.Equal(t, firstSnapshot, snapshot.SnapshotID)
}

func TestInspectMetadataLogEntriesAllowsLiveSnapshotWithoutSchemaID(t *testing.T) {
	const snapshotID = int64(101)
	meta := &metadataV2{commonMetadata: commonMetadata{
		LastUpdatedMS: 2000,
		SnapshotList:  []Snapshot{{SnapshotID: snapshotID, SequenceNumber: 7}},
		SnapshotLog:   []SnapshotLogEntry{{SnapshotID: snapshotID, TimestampMs: 1500}},
		SnapshotRefs:  map[string]SnapshotRef{},
	}}
	tbl := New(Identifier{"metadata-log-entries-nil-schema"}, meta, "/metadata/v2.json", nil, nil)

	rr, err := tbl.Inspect().MetadataLogEntries(context.Background())
	require.NoError(t, err)
	defer rr.Release()

	rec := collectRecord(t, rr)
	defer rec.Release()

	latestSnapshotID := rec.Column(2).(*array.Int64)
	latestSchemaID := rec.Column(3).(*array.Int32)
	latestSequenceNumber := rec.Column(4).(*array.Int64)
	require.EqualValues(t, snapshotID, latestSnapshotID.Value(0))
	require.True(t, latestSchemaID.IsNull(0))
	require.EqualValues(t, 7, latestSequenceNumber.Value(0))
}

func TestInspectMetadataLogEntriesEmpty(t *testing.T) {
	lastPartitionID := 999
	meta := &metadataV2{commonMetadata: commonMetadata{
		FormatVersion:      2,
		UUID:               uuid.New(),
		Loc:                "s3://test/empty-metadata-log",
		LastUpdatedMS:      1000,
		LastColumnId:       1,
		SchemaList:         []*iceberg.Schema{iceberg.NewSchema(0)},
		CurrentSchemaID:    0,
		Specs:              []iceberg.PartitionSpec{*iceberg.UnpartitionedSpec},
		DefaultSpecID:      0,
		LastPartitionID:    &lastPartitionID,
		Props:              iceberg.Properties{},
		SnapshotRefs:       map[string]SnapshotRef{},
		SortOrderList:      []SortOrder{UnsortedSortOrder},
		DefaultSortOrderID: 0,
	}}
	tbl := New(Identifier{"empty-metadata-log"}, meta, "/metadata/v1.json", nil, nil)

	rr, err := tbl.Inspect().MetadataLogEntries(context.Background())
	require.NoError(t, err)
	defer rr.Release()

	rec := collectRecord(t, rr)
	defer rec.Release()

	require.EqualValues(t, 1, rec.NumRows())
	require.Equal(t, "/metadata/v1.json", rec.Column(1).(*array.String).Value(0))
	for column := 2; column < int(rec.NumCols()); column++ {
		require.True(t, rec.Column(column).IsNull(0), "column %d should be null", column)
	}
}

func TestInspectMetadataLogEntriesSkipsEmptyCurrentMetadataLocation(t *testing.T) {
	meta := &metadataV2{commonMetadata: commonMetadata{LastUpdatedMS: 1000}}
	tbl := New(Identifier{"empty-metadata-location"}, meta, "", nil, nil)

	rr, err := tbl.Inspect().MetadataLogEntries(context.Background())
	require.NoError(t, err)
	defer rr.Release()

	rec := collectRecord(t, rr)
	defer rec.Release()

	require.EqualValues(t, 0, rec.NumRows())
	require.EqualValues(t, 5, rec.NumCols())
}

func TestInspectManifestsSchema(t *testing.T) {
	sc := ManifestsSchema()

	require.Equal(t,
		[]string{
			"content", "path", "length", "partition_spec_id", "added_snapshot_id",
			"added_data_files_count", "existing_data_files_count", "deleted_data_files_count",
			"added_delete_files_count", "existing_delete_files_count", "deleted_delete_files_count",
			"partition_summaries",
		},
		testFieldNames(sc))

	fields := sc.Fields()
	require.Equal(t, 14, fields[0].ID)
	require.Equal(t, 1, fields[1].ID)
	require.Equal(t, 17, fields[10].ID)
	require.Equal(t, 8, fields[11].ID)
	require.True(t, fields[4].Required)
	for _, idx := range []int{5, 6, 7, 8, 9, 10} {
		require.False(t, fields[idx].Required)
	}
	require.True(t, fields[11].Required)
	require.True(t, fields[11].Type.(*iceberg.ListType).ElementRequired)
	require.Equal(t, 9, fields[11].Type.(*iceberg.ListType).ElementID)

	arrowSchema, err := SchemaToArrowSchema(sc, nil, true, false)
	require.NoError(t, err)
	require.False(t, arrowSchema.Field(4).Nullable)
	for _, idx := range []int{5, 6, 7, 8, 9, 10} {
		require.True(t, arrowSchema.Field(idx).Nullable)
	}
	require.False(t, arrowSchema.Field(11).Nullable)

	partitionSummary := fields[11].Type.(*iceberg.ListType).Element.(*iceberg.StructType)
	require.Equal(t,
		[]string{"contains_null", "contains_nan", "lower_bound", "upper_bound"},
		testFieldNames(iceberg.NewSchema(0, partitionSummary.FieldList...)))
	require.Equal(t, 10, partitionSummary.FieldList[0].ID)
	require.Equal(t, 11, partitionSummary.FieldList[1].ID)
	require.Equal(t, 12, partitionSummary.FieldList[2].ID)
	require.Equal(t, 13, partitionSummary.FieldList[3].ID)
}

func TestInspectManifests(t *testing.T) {
	const snapshotID = int64(1)
	spec := partitionedSpec()
	txn, memIO := createTestTransactionWithMemIO(t, spec)
	schema := simpleSchema()
	file := newTestDataFileWithCount(t, spec,
		"mem://default/table-location/data/data.parquet", map[int]any{1000: int32(7)}, 3)
	sequenceNumber := int64(1)
	entry := iceberg.NewManifestEntry(
		iceberg.EntryStatusADDED, int64Ptr(snapshotID), &sequenceNumber, &sequenceNumber, file)

	manifestPath := "mem://default/table-location/metadata/data-manifest.avro"
	manifestListPath := "mem://default/table-location/metadata/snap-1-manifest-list.avro"
	var manifestBuf bytes.Buffer
	manifest, err := iceberg.WriteManifest(manifestPath, &manifestBuf, 2, spec, schema, snapshotID,
		[]iceberg.ManifestEntry{entry})
	require.NoError(t, err)
	require.NoError(t, memIO.WriteFile(manifestPath, manifestBuf.Bytes()))

	var listBuf bytes.Buffer
	require.NoError(t, iceberg.WriteManifestList(2, &listBuf, snapshotID, nil, &sequenceNumber, 0,
		[]iceberg.ManifestFile{manifest}))
	require.NoError(t, memIO.WriteFile(manifestListPath, listBuf.Bytes()))

	snapID := snapshotID
	txn.meta.snapshotList = []Snapshot{{
		SnapshotID:     snapshotID,
		ManifestList:   manifestListPath,
		SequenceNumber: sequenceNumber,
	}}
	txn.meta.currentSnapshotID = &snapID
	built, err := txn.meta.Build()
	require.NoError(t, err)

	tbl := New(Identifier{"db", "tbl"}, built, "metadata.json",
		func(context.Context) (iceio.IO, error) { return memIO, nil }, nil)
	rr, err := tbl.Inspect().Manifests(context.Background())
	require.NoError(t, err)
	defer rr.Release()

	record := collectRecord(t, rr)
	defer record.Release()
	require.EqualValues(t, 1, record.NumRows())
	require.EqualValues(t, 1, record.Column(5).(*array.Int32).Value(0))
	require.EqualValues(t, 0, record.Column(8).(*array.Int32).Value(0))
	require.Equal(t, manifestPath, record.Column(1).(*array.String).Value(0))

	summaries := record.Column(11).(*array.List)
	require.False(t, summaries.IsNull(0))
	start, end := summaries.ValueOffsets(0)
	require.EqualValues(t, 1, end-start)
	summary := summaries.ListValues().(*array.Struct)
	require.False(t, summary.Field(0).(*array.Boolean).Value(0))
	require.False(t, summary.Field(1).(*array.Boolean).IsNull(0))
	require.False(t, summary.Field(1).(*array.Boolean).Value(0))
	require.Equal(t, "7", summary.Field(2).(*array.String).Value(0))
	require.Equal(t, "7", summary.Field(3).(*array.String).Value(0))
}

func TestInspectManifestsContainsNaN(t *testing.T) {
	spec := partitionedSpec()
	containsNaN := true
	bound, err := iceberg.Int32Literal(7).MarshalBinary()
	require.NoError(t, err)
	manifest := iceberg.NewManifestFile(2, "mem://default/table-location/metadata/contains-nan.avro",
		100, int32(spec.ID()), 1).
		SequenceNum(1, 1).
		Partitions([]iceberg.FieldSummary{{
			ContainsNaN: &containsNaN,
			LowerBound:  &bound,
			UpperBound:  &bound,
		}}).
		Build()
	tbl := inspectTableWithManifestList(t, spec, 2, []iceberg.ManifestFile{manifest})

	rr, err := tbl.Inspect().Manifests(context.Background())
	require.NoError(t, err)
	defer rr.Release()
	record := collectRecord(t, rr)
	defer record.Release()

	summaries := record.Column(11).(*array.List)
	summary := summaries.ListValues().(*array.Struct)
	containsNaNValues := summary.Field(1).(*array.Boolean)
	require.False(t, containsNaNValues.IsNull(0))
	require.True(t, containsNaNValues.Value(0))
}

func TestInspectManifestsPromotedPartitionSummaryBounds(t *testing.T) {
	tests := []struct {
		name        string
		initialType iceberg.Type
		currentType iceberg.Type
		literal     iceberg.Literal
		expected    string
	}{
		{
			name:        "int to long",
			initialType: iceberg.PrimitiveTypes.Int32,
			currentType: iceberg.PrimitiveTypes.Int64,
			literal:     iceberg.Int32Literal(7),
			expected:    "7",
		},
		{
			name:        "float to double",
			initialType: iceberg.PrimitiveTypes.Float32,
			currentType: iceberg.PrimitiveTypes.Float64,
			literal:     iceberg.Float32Literal(1.5),
			expected:    "1.5",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			spec := partitionedSpec()
			initialSchema := iceberg.NewSchema(0, iceberg.NestedField{
				ID: 1, Name: "id", Type: tt.initialType, Required: true,
			})
			currentSchema := iceberg.NewSchema(1, iceberg.NestedField{
				ID: 1, Name: "id", Type: tt.currentType, Required: true,
			})
			bound, err := tt.literal.MarshalBinary()
			require.NoError(t, err)
			manifest := iceberg.NewManifestFile(2, "mem://default/table-location/metadata/promoted.avro",
				100, int32(spec.ID()), 1).
				SequenceNum(1, 1).
				Partitions([]iceberg.FieldSummary{{LowerBound: &bound, UpperBound: &bound}}).
				Build()
			tbl := inspectTableWithManifestListAndSchemas(t, initialSchema, currentSchema,
				spec, 2, []iceberg.ManifestFile{manifest})

			rr, err := tbl.Inspect().Manifests(context.Background())
			require.NoError(t, err)
			defer rr.Release()
			record := collectRecord(t, rr)
			defer record.Release()

			summaries := record.Column(11).(*array.List)
			start, end := summaries.ValueOffsets(0)
			require.EqualValues(t, 1, end-start)
			summary := summaries.ListValues().(*array.Struct)
			require.Equal(t, tt.expected, summary.Field(2).(*array.String).Value(0))
			require.Equal(t, tt.expected, summary.Field(3).(*array.String).Value(0))
		})
	}
}

func TestInspectManifestsDroppedPartitionSource(t *testing.T) {
	spec := partitionedSpec()
	initialSchema := simpleSchema()
	currentSchema := iceberg.NewSchema(1)
	bound, err := iceberg.Int32Literal(7).MarshalBinary()
	require.NoError(t, err)
	manifest := iceberg.NewManifestFile(2, "mem://default/table-location/metadata/dropped-source.avro",
		100, int32(spec.ID()), 1).
		SequenceNum(1, 1).
		Partitions([]iceberg.FieldSummary{{LowerBound: &bound, UpperBound: &bound}}).
		Build()
	tbl := inspectTableWithManifestListAndSchemas(t, initialSchema, currentSchema,
		spec, 2, []iceberg.ManifestFile{manifest})

	rr, err := tbl.Inspect().Manifests(context.Background())
	require.NoError(t, err)
	defer rr.Release()
	record := collectRecord(t, rr)
	defer record.Release()

	summaries := record.Column(11).(*array.List)
	start, end := summaries.ValueOffsets(0)
	require.EqualValues(t, 1, end-start)
	summary := summaries.ListValues().(*array.Struct)
	require.True(t, summary.Field(2).(*array.String).IsNull(0))
	require.True(t, summary.Field(3).(*array.String).IsNull(0))
}

func TestInspectManifestsDeleteCounts(t *testing.T) {
	spec := partitionedSpec()
	manifest := iceberg.NewManifestFile(2, "mem://default/table-location/metadata/delete-manifest.avro",
		100, int32(spec.ID()), 1).
		Content(iceberg.ManifestContentDeletes).
		SequenceNum(1, 1).
		AddedFiles(2).
		ExistingFiles(3).
		DeletedFiles(4).
		Build()
	tbl := inspectTableWithManifestList(t, spec, 2, []iceberg.ManifestFile{manifest})

	rr, err := tbl.Inspect().Manifests(context.Background())
	require.NoError(t, err)
	defer rr.Release()
	record := collectRecord(t, rr)
	defer record.Release()

	require.EqualValues(t, iceberg.ManifestContentDeletes, record.Column(0).(*array.Int32).Value(0))
	for _, col := range []int{5, 6, 7} {
		require.EqualValues(t, 0, record.Column(col).(*array.Int32).Value(0))
	}
	require.EqualValues(t, 2, record.Column(8).(*array.Int32).Value(0))
	require.EqualValues(t, 3, record.Column(9).(*array.Int32).Value(0))
	require.EqualValues(t, 4, record.Column(10).(*array.Int32).Value(0))
}

func TestInspectManifestsV1UnknownCounts(t *testing.T) {
	spec := partitionedSpec()
	manifest := iceberg.NewManifestFile(1, "mem://default/table-location/metadata/v1-manifest.avro",
		100, int32(spec.ID()), 1).
		AddedFiles(-1).
		ExistingFiles(-1).
		DeletedFiles(-1).
		Build()
	tbl := inspectTableWithManifestList(t, spec, 1, []iceberg.ManifestFile{manifest})

	rr, err := tbl.Inspect().Manifests(context.Background())
	require.NoError(t, err)
	defer rr.Release()
	record := collectRecord(t, rr)
	defer record.Release()

	for _, col := range []int{5, 6, 7} {
		require.True(t, record.Column(col).(*array.Int32).IsNull(0))
	}
	for _, col := range []int{8, 9, 10} {
		values := record.Column(col).(*array.Int32)
		require.False(t, values.IsNull(0))
		require.EqualValues(t, 0, values.Value(0))
	}
	summaries := record.Column(11).(*array.List)
	require.False(t, summaries.IsNull(0))
	start, end := summaries.ValueOffsets(0)
	require.EqualValues(t, 0, end-start)
}

func TestAppendManifestCountValidatesNegativeSentinels(t *testing.T) {
	tests := []struct {
		name     string
		version  int
		count    int32
		wantNull bool
		wantErr  bool
	}{
		{name: "v1 absent count", version: 1, count: -1, wantNull: true},
		{name: "v1 invalid negative count", version: 1, count: -2, wantErr: true},
		{name: "v2 negative count", version: 2, count: -1, wantErr: true},
		{name: "v3 negative count", version: 3, count: -1, wantErr: true},
		{name: "non-negative count", version: 2, count: 3},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			builder := array.NewInt32Builder(memory.DefaultAllocator)
			defer builder.Release()

			err := appendManifestCount(builder, tt.version, "added_data_files", tt.count)
			if tt.wantErr {
				require.ErrorContains(t, err, fmt.Sprintf("negative added_data_files count %d", tt.count))
				require.ErrorContains(t, err, fmt.Sprintf("manifest list version %d", tt.version))
				require.Zero(t, builder.Len())

				return
			}

			require.NoError(t, err)
			values := builder.NewInt32Array()
			defer values.Release()
			require.Equal(t, tt.wantNull, values.IsNull(0))
			if !tt.wantNull {
				require.Equal(t, tt.count, values.Value(0))
			}
		})
	}
}

func TestInspectManifestsRejectsNegativeAddedSnapshotID(t *testing.T) {
	spec := partitionedSpec()
	manifest := iceberg.NewManifestFile(1, "mem://default/table-location/metadata/v1-manifest.avro",
		100, int32(spec.ID()), -1).
		Build()
	tbl := inspectTableWithManifestList(t, spec, 1, []iceberg.ManifestFile{manifest})

	_, err := tbl.Inspect().Manifests(context.Background())
	require.ErrorContains(t, err, "negative added_snapshot_id -1")
}

func TestInspectManifestsRejectsNegativeCountsForV2AndV3(t *testing.T) {
	tests := []struct {
		name                     string
		version                  int
		content                  iceberg.ManifestContent
		added, existing, deleted int32
		invalidCountName         string
	}{
		{name: "v2 added data files", version: 2, added: -123, existing: 1, deleted: 1, invalidCountName: "added_data_files"},
		{name: "v2 existing data files", version: 2, added: 1, existing: -123, deleted: 1, invalidCountName: "existing_data_files"},
		{name: "v2 deleted data files", version: 2, added: 1, existing: 1, deleted: -123, invalidCountName: "deleted_data_files"},
		{name: "v3 added data files", version: 3, added: -123, existing: 1, deleted: 1, invalidCountName: "added_data_files"},
		{name: "v3 existing data files", version: 3, added: 1, existing: -123, deleted: 1, invalidCountName: "existing_data_files"},
		{name: "v3 deleted data files", version: 3, added: 1, existing: 1, deleted: -123, invalidCountName: "deleted_data_files"},
		{name: "v2 added delete files", version: 2, content: iceberg.ManifestContentDeletes, added: -123, existing: 1, deleted: 1, invalidCountName: "added_delete_files"},
		{name: "v2 existing delete files", version: 2, content: iceberg.ManifestContentDeletes, added: 1, existing: -123, deleted: 1, invalidCountName: "existing_delete_files"},
		{name: "v2 deleted delete files", version: 2, content: iceberg.ManifestContentDeletes, added: 1, existing: 1, deleted: -123, invalidCountName: "deleted_delete_files"},
		{name: "v3 added delete files", version: 3, content: iceberg.ManifestContentDeletes, added: -123, existing: 1, deleted: 1, invalidCountName: "added_delete_files"},
		{name: "v3 existing delete files", version: 3, content: iceberg.ManifestContentDeletes, added: 1, existing: -123, deleted: 1, invalidCountName: "existing_delete_files"},
		{name: "v3 deleted delete files", version: 3, content: iceberg.ManifestContentDeletes, added: 1, existing: 1, deleted: -123, invalidCountName: "deleted_delete_files"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			spec := partitionedSpec()
			manifest := iceberg.NewManifestFile(tt.version, "mem://default/table-location/metadata/negative-count.avro",
				100, int32(spec.ID()), 1).
				Content(tt.content).
				SequenceNum(1, 1).
				AddedFiles(tt.added).
				ExistingFiles(tt.existing).
				DeletedFiles(tt.deleted).
				Build()
			tbl := inspectTableWithManifestList(t, spec, tt.version, []iceberg.ManifestFile{manifest})

			_, err := tbl.Inspect().Manifests(context.Background())
			require.ErrorContains(t, err, fmt.Sprintf("negative %s count -123", tt.invalidCountName))
			require.ErrorContains(t, err, fmt.Sprintf("manifest list version %d", tt.version))
		})
	}
}

func TestInspectManifestsRejectsMissingPartitionSpec(t *testing.T) {
	spec := partitionedSpec()
	manifest := iceberg.NewManifestFile(2, "mem://default/table-location/metadata/missing-spec.avro",
		100, 999, 1).
		SequenceNum(1, 1).
		Partitions([]iceberg.FieldSummary{{}}).
		Build()
	tbl := inspectTableWithManifestList(t, spec, 2, []iceberg.ManifestFile{manifest})

	_, err := tbl.Inspect().Manifests(context.Background())
	require.Error(t, err)
	require.ErrorContains(t, err, "references missing partition spec 999")
}

func TestInspectManifestsAllowsMissingPartitionSpecWithoutSummaries(t *testing.T) {
	spec := partitionedSpec()
	manifest := iceberg.NewManifestFile(2, "mem://default/table-location/metadata/missing-spec.avro",
		100, 999, 1).
		SequenceNum(1, 1).
		Build()
	tbl := inspectTableWithManifestList(t, spec, 2, []iceberg.ManifestFile{manifest})

	rr, err := tbl.Inspect().Manifests(context.Background())
	require.NoError(t, err)
	defer rr.Release()
	record := collectRecord(t, rr)
	defer record.Release()

	summaries := record.Column(11).(*array.List)
	require.False(t, summaries.IsNull(0))
	start, end := summaries.ValueOffsets(0)
	require.EqualValues(t, 0, end-start)
}

func TestInspectManifestsReportsPartitionFieldNameInBoundErrors(t *testing.T) {
	spec := partitionedSpec()
	invalidBound := []byte{0x01}
	manifest := iceberg.NewManifestFile(2, "mem://default/table-location/metadata/invalid-bound.avro",
		100, int32(spec.ID()), 1).
		SequenceNum(1, 1).
		Partitions([]iceberg.FieldSummary{{LowerBound: &invalidBound}}).
		Build()
	tbl := inspectTableWithManifestList(t, spec, 2, []iceberg.ManifestFile{manifest})

	_, err := tbl.Inspect().Manifests(context.Background())
	require.ErrorContains(t, err, "partition field \"id\" lower bound")
}

func TestInspectManifestsWrapsFileIOFactoryError(t *testing.T) {
	spec := partitionedSpec()
	manifest := iceberg.NewManifestFile(2, "mem://default/table-location/metadata/manifest.avro",
		100, int32(spec.ID()), 1).
		SequenceNum(1, 1).
		Build()
	tbl := inspectTableWithManifestList(t, spec, 2, []iceberg.ManifestFile{manifest})
	factoryErr := errors.New("factory failed")
	tbl.fsF = func(context.Context) (iceio.IO, error) { return nil, factoryErr }

	_, err := tbl.Inspect().Manifests(context.Background())
	require.ErrorIs(t, err, factoryErr)
	require.ErrorContains(t, err, "inspect manifests: get file IO")
}

func TestInspectManifestsNoCurrentSnapshot(t *testing.T) {
	spec := partitionedSpec()
	meta, err := NewMetadata(simpleSchema(), &spec, UnsortedSortOrder, "mem://default/table-location", nil)
	require.NoError(t, err)
	tbl := New(Identifier{"db", "tbl"}, meta, "metadata.json", nil, nil)

	rr, err := tbl.Inspect().Manifests(context.Background())
	require.NoError(t, err)
	defer rr.Release()
	record := collectRecord(t, rr)
	defer record.Release()

	require.EqualValues(t, 0, record.NumRows())
	require.EqualValues(t, 12, record.NumCols())
}

func TestInspectManifestsRejectsExtraPartitionSummaries(t *testing.T) {
	spec := partitionedSpec()
	manifest := iceberg.NewManifestFile(2, "mem://default/table-location/metadata/extra-summary.avro",
		100, int32(spec.ID()), 1).
		SequenceNum(1, 1).
		Partitions([]iceberg.FieldSummary{{}, {}}).
		Build()
	tbl := inspectTableWithManifestList(t, spec, 2, []iceberg.ManifestFile{manifest})

	_, err := tbl.Inspect().Manifests(context.Background())
	require.Error(t, err)
	require.ErrorContains(t, err, "has 2 partition summaries")
}

func TestInspectManifestsRejectsUnknownContent(t *testing.T) {
	spec := partitionedSpec()
	manifest := iceberg.NewManifestFile(2, "mem://default/table-location/metadata/unknown-content.avro",
		100, int32(spec.ID()), 1).
		Content(iceberg.ManifestContent(2)).
		SequenceNum(1, 1).
		Build()
	tbl := inspectTableWithManifestList(t, spec, 2, []iceberg.ManifestFile{manifest})

	_, err := tbl.Inspect().Manifests(context.Background())
	require.Error(t, err)
	require.ErrorContains(t, err, "has unknown content 2")
}

func TestDataFilesSchema(t *testing.T) {
	sc := DataFilesSchema(&iceberg.StructType{FieldList: []iceberg.NestedField{
		{ID: 1000, Name: "bucket", Type: iceberg.PrimitiveTypes.Int32, Required: true},
	}})

	require.Equal(t, []string{
		"content", "file_path", "file_format", "spec_id", "partition",
		"record_count", "file_size_in_bytes", "column_sizes", "value_counts", "null_value_counts",
		"nan_value_counts", "lower_bounds", "upper_bounds", "key_metadata", "split_offsets",
		"equality_ids", "sort_order_id", "first_row_id", "referenced_data_file", "content_offset",
		"content_size_in_bytes",
	}, testFieldNames(sc))
	require.NotContains(t, testFieldNames(sc), "distinct_value_counts")

	fields := sc.Fields()
	require.Equal(t, 134, fields[0].ID)
	require.Equal(t, 100, fields[1].ID)
	require.Equal(t, 141, fields[3].ID)
	require.Equal(t, 102, fields[4].ID)
	require.Equal(t, 137, fields[10].ID)
	require.Equal(t, 145, fields[len(fields)-1].ID)

	unpartitioned := DataFilesSchema(&iceberg.StructType{})
	require.NotContains(t, testFieldNames(unpartitioned), "partition")
}

func inspectDataFilesTable(t *testing.T, spec iceberg.PartitionSpec, entries []iceberg.ManifestEntry) *Table {
	t.Helper()
	const snapshotID = int64(1)

	txn, memIO := createTestTransactionWithMemIO(t, spec)
	schema := simpleSchema()
	manifestPath := "mem://default/table-location/metadata/data-manifest.avro"
	manifestListPath := "mem://default/table-location/metadata/snap-1-manifest-list.avro"

	var manifestBuf bytes.Buffer
	manifest, err := iceberg.WriteManifest(manifestPath, &manifestBuf, 2, spec, schema, snapshotID, entries)
	require.NoError(t, err)
	require.NoError(t, memIO.WriteFile(manifestPath, manifestBuf.Bytes()))

	var listBuf bytes.Buffer
	sequenceNumber := int64(1)
	require.NoError(t, iceberg.WriteManifestList(2, &listBuf, snapshotID, nil, &sequenceNumber, 0,
		[]iceberg.ManifestFile{manifest}))
	require.NoError(t, memIO.WriteFile(manifestListPath, listBuf.Bytes()))

	snapID := snapshotID
	txn.meta.snapshotList = []Snapshot{{
		SnapshotID:     snapshotID,
		ManifestList:   manifestListPath,
		SequenceNumber: sequenceNumber,
	}}
	txn.meta.currentSnapshotID = &snapID
	built, err := txn.meta.Build()
	require.NoError(t, err)

	return New(Identifier{"db", "tbl"}, built, "metadata.json",
		func(context.Context) (iceio.IO, error) { return memIO, nil }, nil)
}

func inspectDataFileEntries(t *testing.T, spec iceberg.PartitionSpec, count int) []iceberg.ManifestEntry {
	t.Helper()
	const snapshotID = int64(1)

	entries := make([]iceberg.ManifestEntry, 0, count)
	for index := 0; index < count; index++ {
		path := "mem://default/table-location/data/live-" + strconv.Itoa(index) + ".parquet"
		file := newTestDataFile(t, spec, path, nil)
		sequenceNumber := int64(1)
		entries = append(entries, iceberg.NewManifestEntry(
			iceberg.EntryStatusADDED, int64Ptr(snapshotID), &sequenceNumber, &sequenceNumber, file))
	}

	return entries
}

func TestInspectDataFilesStreamsBatchesAndSkipsDeleted(t *testing.T) {
	const snapshotID = int64(1)
	spec := *iceberg.UnpartitionedSpec
	txn, memIO := createTestTransactionWithMemIO(t, spec)
	schema := simpleSchema()

	entries := make([]iceberg.ManifestEntry, 0, inspectRecordBatchSize+2)
	for index := 0; index < inspectRecordBatchSize+1; index++ {
		file := newTestDataFile(t, spec,
			"mem://default/table-location/data/live-"+strconv.Itoa(index)+".parquet", nil)
		sequenceNumber := int64(1)
		entries = append(entries, iceberg.NewManifestEntry(
			iceberg.EntryStatusADDED, int64Ptr(snapshotID), &sequenceNumber, &sequenceNumber, file))
	}
	deletedPath := "mem://default/table-location/data/deleted.parquet"
	deleted := newTestDataFile(t, spec, deletedPath, nil)
	deletedSequenceNumber := int64(1)
	entries = append(entries, iceberg.NewManifestEntry(
		iceberg.EntryStatusDELETED, int64Ptr(snapshotID), &deletedSequenceNumber, &deletedSequenceNumber, deleted))

	manifestPath := "mem://default/table-location/metadata/data-manifest.avro"
	manifestListPath := "mem://default/table-location/metadata/snap-1-manifest-list.avro"
	var manifestBuf bytes.Buffer
	manifest, err := iceberg.WriteManifest(manifestPath, &manifestBuf, 2, spec, schema, snapshotID, entries)
	require.NoError(t, err)
	require.NoError(t, memIO.WriteFile(manifestPath, manifestBuf.Bytes()))

	var listBuf bytes.Buffer
	sequenceNumber := int64(1)
	require.NoError(t, iceberg.WriteManifestList(2, &listBuf, snapshotID, nil, &sequenceNumber, 0,
		[]iceberg.ManifestFile{manifest}))
	require.NoError(t, memIO.WriteFile(manifestListPath, listBuf.Bytes()))

	snapID := snapshotID
	txn.meta.snapshotList = []Snapshot{{
		SnapshotID:     snapshotID,
		ManifestList:   manifestListPath,
		SequenceNumber: sequenceNumber,
	}}
	txn.meta.currentSnapshotID = &snapID
	built, err := txn.meta.Build()
	require.NoError(t, err)

	tbl := New(Identifier{"db", "tbl"}, built, "metadata.json",
		func(context.Context) (iceio.IO, error) { return memIO, nil }, nil)
	rr, err := tbl.Inspect().DataFiles(context.Background())
	require.NoError(t, err)
	defer rr.Release()

	var batchRows []int
	var paths []string
	for rr.Next() {
		record := rr.RecordBatch()
		batchRows = append(batchRows, int(record.NumRows()))
		filePaths := record.Column(1).(*array.String)
		for row := 0; row < filePaths.Len(); row++ {
			paths = append(paths, filePaths.Value(row))
		}
	}
	require.NoError(t, rr.Err())
	require.Equal(t, []int{inspectRecordBatchSize, 1}, batchRows)
	require.Len(t, paths, inspectRecordBatchSize+1)
	require.NotContains(t, paths, deletedPath)
}

func TestInspectDataFilesReturnsPartitionValues(t *testing.T) {
	const snapshotID = int64(1)
	spec := partitionedSpec()
	file := newTestDataFile(t, spec,
		"mem://default/table-location/data/partitioned.parquet", map[int]any{1000: int32(7)})
	sequenceNumber := int64(1)
	entry := iceberg.NewManifestEntry(
		iceberg.EntryStatusADDED, int64Ptr(snapshotID), &sequenceNumber, &sequenceNumber, file)
	tbl := inspectDataFilesTable(t, spec, []iceberg.ManifestEntry{entry})

	rr, err := tbl.Inspect().DataFiles(context.Background())
	require.NoError(t, err)
	defer rr.Release()

	record := collectRecord(t, rr)
	defer record.Release()

	require.EqualValues(t, 1, record.NumRows())
	partition := record.Column(4).(*array.Struct)
	values := partition.Field(0).(*array.Int32)
	require.EqualValues(t, 7, values.Value(0))
}

func TestInspectDataFilesEmitsEmptyBatchWhenAllEntriesAreDeleted(t *testing.T) {
	const snapshotID = int64(1)
	spec := *iceberg.UnpartitionedSpec
	file := newTestDataFile(t, spec,
		"mem://default/table-location/data/deleted.parquet", nil)
	sequenceNumber := int64(1)
	entry := iceberg.NewManifestEntry(
		iceberg.EntryStatusDELETED, int64Ptr(snapshotID), &sequenceNumber, &sequenceNumber, file)
	tbl := inspectDataFilesTable(t, spec, []iceberg.ManifestEntry{entry})

	rr, err := tbl.Inspect().DataFiles(context.Background())
	require.NoError(t, err)
	defer rr.Release()

	record := collectRecord(t, rr)
	defer record.Release()
	require.EqualValues(t, 0, record.NumRows())
}

func TestInspectDataFilesAllocator(t *testing.T) {
	for _, tt := range []struct {
		name    string
		abandon bool
	}{
		{name: "drains reader"},
		{name: "abandons reader after first batch", abandon: true},
	} {
		t.Run(tt.name, func(t *testing.T) {
			checked := memory.NewCheckedAllocator(memory.DefaultAllocator)
			t.Cleanup(func() { checked.AssertSize(t, 0) })

			spec := *iceberg.UnpartitionedSpec
			tbl := inspectDataFilesTable(t, spec, inspectDataFileEntries(t, spec, inspectRecordBatchSize+1))
			rr, err := tbl.Inspect(WithInspectAllocator(checked)).DataFiles(context.Background())
			require.NoError(t, err)

			if tt.abandon {
				require.True(t, rr.Next())
				rr.Release()

				return
			}

			for rr.Next() {
				// The reader owns the current batch until the next call to Next or
				// Release, so no extra retain is needed here.
			}
			require.NoError(t, rr.Err())
			rr.Release()
		})
	}
}

func TestAppendContentFileRecordUsesFieldIDs(t *testing.T) {
	fields := inspectContentFileFields(&iceberg.StructType{})
	reordered := make([]iceberg.NestedField, 0, len(fields))
	reordered = append(reordered, fields[1:]...)
	reordered = append(reordered, fields[0])
	schema := iceberg.NewSchema(0, reordered...)
	arrowSchema, err := SchemaToArrowSchema(schema, nil, true, false)
	require.NoError(t, err)

	bldr := array.NewRecordBuilder(memory.DefaultAllocator, arrowSchema)
	defer bldr.Release()
	file := newTestDataFile(t, *iceberg.UnpartitionedSpec,
		"mem://default/table-location/data/reordered.parquet", nil)
	require.NoError(t, appendContentFileRecord(bldr, &iceberg.StructType{}, file))

	record := bldr.NewRecordBatch()
	defer record.Release()
	pathIndex := record.Schema().FieldIndices("file_path")[0]
	countIndex := record.Schema().FieldIndices("record_count")[0]
	require.Equal(t, file.FilePath(), record.Column(pathIndex).(*array.String).Value(0))
	require.EqualValues(t, file.Count(), record.Column(countIndex).(*array.Int64).Value(0))
}

func TestInspectPartitionTypeUsesAllActiveSpecs(t *testing.T) {
	schema := iceberg.NewSchema(0,
		iceberg.NestedField{ID: 1, Name: "region", Type: iceberg.PrimitiveTypes.String, Required: true},
		iceberg.NestedField{ID: 2, Name: "category", Type: iceberg.PrimitiveTypes.String, Required: true},
	)
	oldSpec := iceberg.NewPartitionSpecID(0, iceberg.PartitionField{
		SourceIDs: []int{1}, FieldID: 1000, Name: "region", Transform: iceberg.IdentityTransform{},
	})
	newSpec := iceberg.NewPartitionSpecID(1, iceberg.PartitionField{
		SourceIDs: []int{2}, FieldID: 1001, Name: "category", Transform: iceberg.IdentityTransform{},
	})
	lastPartitionID := 1001
	meta := &metadataV2{commonMetadata: commonMetadata{
		FormatVersion:   2,
		UUID:            uuid.New(),
		LastColumnId:    2,
		SchemaList:      []*iceberg.Schema{schema},
		CurrentSchemaID: 0,
		Specs:           []iceberg.PartitionSpec{oldSpec, newSpec},
		DefaultSpecID:   1,
		LastPartitionID: &lastPartitionID,
		SnapshotRefs:    map[string]SnapshotRef{},
	}}

	partitionType, err := inspectPartitionType(meta)
	require.NoError(t, err)
	require.Equal(t, []int{1000, 1001}, []int{
		partitionType.FieldList[0].ID,
		partitionType.FieldList[1].ID,
	})
	require.Equal(t, []string{"region", "category"}, []string{
		partitionType.FieldList[0].Name,
		partitionType.FieldList[1].Name,
	})
}

func TestInspectPartitionTypeRejectsIncompatibleFieldReuse(t *testing.T) {
	schema := iceberg.NewSchema(0,
		iceberg.NestedField{ID: 1, Name: "first", Type: iceberg.PrimitiveTypes.Int32, Required: true},
		iceberg.NestedField{ID: 2, Name: "second", Type: iceberg.PrimitiveTypes.Int32, Required: true},
	)
	metadataFor := func(specs []iceberg.PartitionSpec) Metadata {
		lastPartitionID := 1000

		return &metadataV2{commonMetadata: commonMetadata{
			FormatVersion:   2,
			UUID:            uuid.New(),
			LastColumnId:    2,
			SchemaList:      []*iceberg.Schema{schema},
			CurrentSchemaID: 0,
			Specs:           specs,
			DefaultSpecID:   specs[len(specs)-1].ID(),
			LastPartitionID: &lastPartitionID,
		}}
	}
	field := func(specID, sourceID int, transform iceberg.Transform) iceberg.PartitionSpec {
		return iceberg.NewPartitionSpecID(specID, iceberg.PartitionField{
			SourceIDs: []int{sourceID}, FieldID: 1000, Name: "part", Transform: transform,
		})
	}

	tests := []struct {
		name  string
		specs []iceberg.PartitionSpec
		valid bool
	}{
		{
			name:  "void transition is compatible",
			specs: []iceberg.PartitionSpec{field(0, 1, iceberg.IdentityTransform{}), field(1, 1, iceberg.VoidTransform{})},
			valid: true,
		},
		{
			name:  "different source ids",
			specs: []iceberg.PartitionSpec{field(0, 1, iceberg.IdentityTransform{}), field(1, 2, iceberg.IdentityTransform{})},
		},
		{
			name:  "different transforms",
			specs: []iceberg.PartitionSpec{field(0, 1, iceberg.IdentityTransform{}), field(1, 1, iceberg.BucketTransform{NumBuckets: 16})},
		},
		{
			name: "void cannot hide incompatible history",
			specs: []iceberg.PartitionSpec{
				field(0, 1, iceberg.BucketTransform{NumBuckets: 16}),
				field(1, 1, iceberg.IdentityTransform{}),
				field(2, 1, iceberg.VoidTransform{}),
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := inspectPartitionType(metadataFor(tt.specs))
			if tt.valid {
				require.NoError(t, err)

				return
			}
			require.ErrorIs(t, err, iceberg.ErrInvalidPartitionSpec)
		})
	}
}

// TestInspectAllocatorOption verifies WithInspectAllocator routes allocations
// through the supplied allocator, and that all buffers are released.
func TestInspectAllocatorOption(t *testing.T) {
	checked := memory.NewCheckedAllocator(memory.DefaultAllocator)
	// AssertSize runs after the releases below, on every exit path, so an early
	// assertion failure cannot silently skip the leak check.
	t.Cleanup(func() { checked.AssertSize(t, 0) })
	tbl := snapshotsTestTable()

	rr, err := tbl.Inspect(WithInspectAllocator(checked)).Snapshots(context.Background())
	require.NoError(t, err)
	defer rr.Release()

	rec := collectRecord(t, rr)
	defer rec.Release()

	require.EqualValues(t, 2, rec.NumRows())
}

func TestInspectMetadataLogEntriesAllocator(t *testing.T) {
	checked := memory.NewCheckedAllocator(memory.DefaultAllocator)
	t.Cleanup(func() { checked.AssertSize(t, 0) })
	meta := &metadataV2{commonMetadata: commonMetadata{
		LastUpdatedMS: 2000,
		MetadataLog: []MetadataLogEntry{
			{MetadataFile: "/metadata/v1.json", TimestampMs: 1000},
		},
		SnapshotList: []Snapshot{{SnapshotID: 101, SequenceNumber: 7}},
		SnapshotLog:  []SnapshotLogEntry{{SnapshotID: 101, TimestampMs: 1500}},
	}}
	tbl := New(Identifier{"metadata-log-entries-allocator"}, meta, "/metadata/v2.json", nil, nil)

	rr, err := tbl.Inspect(WithInspectAllocator(checked)).MetadataLogEntries(context.Background())
	require.NoError(t, err)
	defer rr.Release()

	rec := collectRecord(t, rr)
	defer rec.Release()

	require.EqualValues(t, 2, rec.NumRows())
}

// mapRow reads one row of a string->string Arrow map column into a Go map.
func mapRow(t *testing.T, m *array.Map, row int) map[string]string {
	t.Helper()
	keys := m.Keys().(*array.String)
	values := m.Items().(*array.String)
	start, end := m.ValueOffsets(row)
	out := make(map[string]string, end-start)
	for j := start; j < end; j++ {
		out[keys.Value(int(j))] = values.Value(int(j))
	}

	return out
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

func TestInspectValueScalarDecimal(t *testing.T) {
	typ := iceberg.DecimalTypeOf(10, 2)
	arrowType := &arrow.Decimal128Type{Precision: 10, Scale: 2}
	value := iceberg.DecimalLiteral{Val: decimal128.FromI64(123), Scale: 2}

	got, err := inspectValueScalar(value, typ, arrowType)
	require.NoError(t, err)
	require.Equal(t, value.Val, got.(*scalar.Decimal128).Value)
}

func TestInspectValueScalarRejectsMismatchedPartitionValues(t *testing.T) {
	tests := []struct {
		name      string
		value     any
		typ       iceberg.Type
		arrowType arrow.DataType
		want      string
	}{
		{name: "date", value: int64(1), typ: iceberg.PrimitiveTypes.Date, arrowType: arrow.FixedWidthTypes.Date32, want: "unsupported date"},
		{name: "time", value: int64(1), typ: iceberg.PrimitiveTypes.Time, arrowType: arrow.FixedWidthTypes.Time64us, want: "unsupported time"},
		{name: "timestamp", value: int64(1), typ: iceberg.PrimitiveTypes.Timestamp, arrowType: arrow.FixedWidthTypes.Timestamp_us, want: "unsupported timestamp"},
		{name: "timestamp nanos", value: int64(1), typ: iceberg.PrimitiveTypes.TimestampNs, arrowType: arrow.FixedWidthTypes.Timestamp_ns, want: "unsupported nanosecond timestamp"},
		{name: "UUID", value: []byte("not-a-UUID"), typ: iceberg.PrimitiveTypes.UUID, arrowType: &arrow.FixedSizeBinaryType{ByteWidth: 16}, want: "unsupported UUID"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := inspectValueScalar(tt.value, tt.typ, tt.arrowType)
			require.ErrorContains(t, err, tt.want)
		})
	}
}

func TestInspectInt32ValueRejectsOverflow(t *testing.T) {
	if strconv.IntSize == 32 {
		t.Skip("int32 is the native int width")
	}

	_, err := inspectInt32Value(int(^uint32(0)>>1)+1, "field ID")
	require.ErrorContains(t, err, "does not fit in int32")
}
