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
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"strconv"
	"testing"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/decimal128"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/arrow-go/v18/arrow/scalar"
	"github.com/apache/arrow-go/v18/parquet"
	"github.com/apache/arrow-go/v18/parquet/pqarrow"
	"github.com/apache/iceberg-go"
	iceio "github.com/apache/iceberg-go/io"
	"github.com/apache/iceberg-go/table/dv"
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
	return inspectTestTable(Identifier{"history"}, map[string]SnapshotRef{
		MainBranch: {SnapshotID: 103, SnapshotRefType: BranchRef},
	})
}

func refsTestTable(snapshotRefs map[string]SnapshotRef) *Table {
	return inspectTestTable(Identifier{"refs"}, snapshotRefs)
}

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
		SnapshotRefs:       snapshotRefs,
	}}

	return New(identifier, meta, "", nil, nil)
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

func TestInspectRefsSchema(t *testing.T) {
	sc := RefsSchema()

	require.Equal(t, []string{
		"name",
		"type",
		"snapshot_id",
		"max_reference_age_in_ms",
		"min_snapshots_to_keep",
		"max_snapshot_age_in_ms",
	}, testFieldNames(sc))

	fields := sc.Fields()
	require.Equal(t, []int{1, 2, 3, 4, 5, 6}, []int{
		fields[0].ID,
		fields[1].ID,
		fields[2].ID,
		fields[3].ID,
		fields[4].ID,
		fields[5].ID,
	})
	require.Equal(t, iceberg.PrimitiveTypes.String, fields[0].Type)
	require.Equal(t, iceberg.PrimitiveTypes.String, fields[1].Type)
	require.Equal(t, iceberg.PrimitiveTypes.Int64, fields[2].Type)
	require.Equal(t, iceberg.PrimitiveTypes.Int64, fields[3].Type)
	require.Equal(t, iceberg.PrimitiveTypes.Int32, fields[4].Type)
	require.Equal(t, iceberg.PrimitiveTypes.Int64, fields[5].Type)
	require.True(t, fields[0].Required)
	require.True(t, fields[1].Required)
	require.True(t, fields[2].Required)
	require.False(t, fields[3].Required)
	require.False(t, fields[4].Required)
	require.False(t, fields[5].Required)
}

func TestInspectRefs(t *testing.T) {
	minSnapshotsToKeep := 2
	maxSnapshotAge := int64(3000)
	maxReferenceAge := int64(4000)
	tbl := refsTestTable(map[string]SnapshotRef{
		"main": {
			SnapshotID:         103,
			SnapshotRefType:    BranchRef,
			MinSnapshotsToKeep: &minSnapshotsToKeep,
			MaxSnapshotAgeMs:   &maxSnapshotAge,
		},
		"release": {
			SnapshotID:      102,
			SnapshotRefType: TagRef,
			MaxRefAgeMs:     &maxReferenceAge,
		},
	})

	rr, err := tbl.Inspect().Refs(context.Background())
	require.NoError(t, err)
	defer rr.Release()

	rec := collectRecord(t, rr)
	defer rec.Release()

	require.EqualValues(t, 2, rec.NumRows())
	require.EqualValues(t, 6, rec.NumCols())

	names := rec.Column(0).(*array.String)
	types := rec.Column(1).(*array.String)
	snapshotIDs := rec.Column(2).(*array.Int64)
	maxReferences := rec.Column(3).(*array.Int64)
	minSnapshots := rec.Column(4).(*array.Int32)
	maxSnapshots := rec.Column(5).(*array.Int64)

	// Map iteration is normalized to name order by the metadata-table reader.
	require.Equal(t, "main", names.Value(0))
	require.Equal(t, "release", names.Value(1))
	require.Equal(t, "BRANCH", types.Value(0))
	require.Equal(t, "TAG", types.Value(1))
	require.EqualValues(t, 103, snapshotIDs.Value(0))
	require.EqualValues(t, 102, snapshotIDs.Value(1))

	require.True(t, maxReferences.IsNull(0))
	require.EqualValues(t, maxReferenceAge, maxReferences.Value(1))
	require.EqualValues(t, minSnapshotsToKeep, minSnapshots.Value(0))
	require.True(t, minSnapshots.IsNull(1))
	require.EqualValues(t, maxSnapshotAge, maxSnapshots.Value(0))
	require.True(t, maxSnapshots.IsNull(1))
}

func TestInspectRefsEmpty(t *testing.T) {
	tbl := refsTestTable(map[string]SnapshotRef{})

	rr, err := tbl.Inspect().Refs(context.Background())
	require.NoError(t, err)
	defer rr.Release()

	rec := collectRecord(t, rr)
	defer rec.Release()

	require.EqualValues(t, 0, rec.NumRows())
	require.EqualValues(t, 6, rec.NumCols())
}

func TestInspectRefsAllocator(t *testing.T) {
	checked := memory.NewCheckedAllocator(memory.DefaultAllocator)
	t.Cleanup(func() { checked.AssertSize(t, 0) })
	tbl := refsTestTable(map[string]SnapshotRef{
		MainBranch: {SnapshotID: 103, SnapshotRefType: BranchRef},
	})

	rr, err := tbl.Inspect(WithInspectAllocator(checked)).Refs(context.Background())
	require.NoError(t, err)
	defer rr.Release()

	rec := collectRecord(t, rr)
	defer rec.Release()

	require.EqualValues(t, 1, rec.NumRows())
}

func TestInspectRefsRejectsMinSnapshotsToKeepOverflow(t *testing.T) {
	if strconv.IntSize < 64 {
		t.Skip("requires 64-bit int")
	}

	tooLarge := int64(math.MaxInt32) + 1
	minSnapshotsToKeep := int(tooLarge)
	tbl := refsTestTable(map[string]SnapshotRef{
		"main": {
			SnapshotID:         103,
			SnapshotRefType:    BranchRef,
			MinSnapshotsToKeep: &minSnapshotsToKeep,
		},
	})

	rr, err := tbl.Inspect().Refs(context.Background())
	require.ErrorContains(t, err, "min snapshots to keep 2147483648 is outside int32 range")
	require.Nil(t, rr)
}

func TestInspectRefsAcceptsMaxInt32MinSnapshotsToKeep(t *testing.T) {
	minSnapshotsToKeep := int(math.MaxInt32)
	tbl := refsTestTable(map[string]SnapshotRef{
		"main": {
			SnapshotID:         103,
			SnapshotRefType:    BranchRef,
			MinSnapshotsToKeep: &minSnapshotsToKeep,
		},
	})

	rr, err := tbl.Inspect().Refs(context.Background())
	require.NoError(t, err)
	defer rr.Release()

	rec := collectRecord(t, rr)
	defer rec.Release()

	minSnapshots := rec.Column(4).(*array.Int32)
	require.False(t, minSnapshots.IsNull(0))
	require.Equal(t, int32(math.MaxInt32), minSnapshots.Value(0))
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

func TestInspectDeleteFiles(t *testing.T) {
	const (
		snapshotID = int64(1)
		dataPath   = "mem://default/table-location/data/data.parquet"
	)
	spec := partitionedSpec()
	partition := map[int]any{1000: int32(7)}

	equalityBuilder, err := iceberg.NewDataFileBuilder(
		spec,
		iceberg.EntryContentEqDeletes,
		"mem://default/table-location/delete/equality.parquet",
		iceberg.ParquetFile,
		partition,
		nil,
		nil,
		2,
		2048,
	)
	require.NoError(t, err)
	equalityFile := equalityBuilder.
		ColumnSizes(map[int]int64{1: 128}).
		ValueCounts(map[int]int64{1: 2}).
		NullValueCounts(map[int]int64{1: 0}).
		NaNValueCounts(map[int]int64{1: 0}).
		LowerBoundValues(map[int][]byte{1: {0x01}}).
		UpperBoundValues(map[int][]byte{1: {0x14}}).
		KeyMetadata([]byte{0xab, 0xcd}).
		SplitOffsets([]int64{4, 16}).
		EqualityFieldIDs([]int{1}).
		SortOrderID(5).
		Build()
	positionFile := newTestPosDeleteFileForSpec(t, spec,
		"mem://default/table-location/delete/position.parquet", partition, dataPath)
	deletionVectorBuilder, err := iceberg.NewDataFileBuilder(
		spec,
		iceberg.EntryContentPosDeletes,
		"mem://default/table-location/delete/vector.puffin",
		iceberg.PuffinFile,
		partition,
		nil,
		nil,
		3,
		4096,
	)
	require.NoError(t, err)
	deletionVectorFile := deletionVectorBuilder.
		ReferencedDataFile(dataPath).
		ContentOffset(128).
		ContentSizeInBytes(512).
		Build()
	deletedFile := newTestPosDeleteFileForSpec(t, spec,
		"mem://default/table-location/delete/deleted.parquet", partition, dataPath)

	entry := func(status iceberg.ManifestEntryStatus, file iceberg.DataFile) iceberg.ManifestEntry {
		sequenceNumber := int64(1)

		return iceberg.NewManifestEntry(status, int64Ptr(snapshotID), &sequenceNumber, &sequenceNumber, file)
	}
	tbl := inspectFilesTableWithManifests(t, spec,
		inspectManifestSpec{
			content: iceberg.ManifestContentData,
			entries: []iceberg.ManifestEntry{entry(iceberg.EntryStatusADDED,
				newTestDataFile(t, spec, dataPath, partition))},
		},
		inspectManifestSpec{
			content: iceberg.ManifestContentDeletes,
			entries: []iceberg.ManifestEntry{
				entry(iceberg.EntryStatusADDED, equalityFile),
				entry(iceberg.EntryStatusADDED, positionFile),
				entry(iceberg.EntryStatusADDED, deletionVectorFile),
				entry(iceberg.EntryStatusDELETED, deletedFile),
			},
		})

	rr, err := tbl.Inspect().DeleteFiles(context.Background())
	require.NoError(t, err)
	defer rr.Release()

	record := collectRecord(t, rr)
	defer record.Release()
	require.EqualValues(t, 3, record.NumRows())
	require.EqualValues(t, 21, record.NumCols())

	content := record.Column(0).(*array.Int32)
	paths := record.Column(1).(*array.String)
	formats := record.Column(2).(*array.String)
	partitions := record.Column(4).(*array.Struct)
	partitionValues := partitions.Field(0).(*array.Int32)
	require.EqualValues(t, iceberg.EntryContentEqDeletes, content.Value(0))
	require.EqualValues(t, iceberg.EntryContentPosDeletes, content.Value(1))
	require.EqualValues(t, iceberg.EntryContentPosDeletes, content.Value(2))
	require.Equal(t, equalityFile.FilePath(), paths.Value(0))
	require.Equal(t, positionFile.FilePath(), paths.Value(1))
	require.Equal(t, deletionVectorFile.FilePath(), paths.Value(2))
	require.Equal(t, "PARQUET", formats.Value(0))
	require.Equal(t, "PARQUET", formats.Value(1))
	require.Equal(t, "PUFFIN", formats.Value(2))
	require.EqualValues(t, 7, partitionValues.Value(0))
	require.EqualValues(t, 7, partitionValues.Value(1))
	require.EqualValues(t, 7, partitionValues.Value(2))

	recordCounts := record.Column(5).(*array.Int64)
	fileSizes := record.Column(6).(*array.Int64)
	require.EqualValues(t, 2, recordCounts.Value(0))
	require.EqualValues(t, 1, recordCounts.Value(1))
	require.EqualValues(t, 3, recordCounts.Value(2))
	require.EqualValues(t, 2048, fileSizes.Value(0))
	require.EqualValues(t, 1, fileSizes.Value(1))
	require.EqualValues(t, 4096, fileSizes.Value(2))

	assertInt64Map := func(column int, key int32, value int64) {
		t.Helper()
		m := record.Column(column).(*array.Map)
		require.False(t, m.IsNull(0))
		start, end := m.ValueOffsets(0)
		require.EqualValues(t, 1, end-start)
		require.Equal(t, key, m.Keys().(*array.Int32).Value(int(start)))
		require.Equal(t, value, m.Items().(*array.Int64).Value(int(start)))
	}
	assertInt64Map(7, 1, 128)
	assertInt64Map(8, 1, 2)
	assertInt64Map(9, 1, 0)
	assertInt64Map(10, 1, 0)
	assertBinaryMap := func(column int, key int32, value []byte) {
		t.Helper()
		m := record.Column(column).(*array.Map)
		require.False(t, m.IsNull(0))
		start, end := m.ValueOffsets(0)
		require.EqualValues(t, 1, end-start)
		require.Equal(t, key, m.Keys().(*array.Int32).Value(int(start)))
		require.Equal(t, value, m.Items().(*array.Binary).Value(int(start)))
	}
	assertBinaryMap(11, 1, []byte{0x01})
	assertBinaryMap(12, 1, []byte{0x14})

	keyMetadata := record.Column(13).(*array.Binary)
	require.Equal(t, []byte{0xab, 0xcd}, keyMetadata.Value(0))
	splitOffsets := record.Column(14).(*array.List)
	require.Equal(t, []int64{4, 16}, splitOffsets.ListValues().(*array.Int64).Int64Values())
	equalityIDs := record.Column(15).(*array.List)
	require.Equal(t, []int32{1}, equalityIDs.ListValues().(*array.Int32).Int32Values())
	sortOrderIDs := record.Column(16).(*array.Int32)
	require.False(t, sortOrderIDs.IsNull(0))
	require.EqualValues(t, 5, sortOrderIDs.Value(0))
	referencedDataFiles := record.Column(18).(*array.String)
	require.True(t, referencedDataFiles.IsNull(0))
	require.Equal(t, dataPath, referencedDataFiles.Value(1))
	require.Equal(t, dataPath, referencedDataFiles.Value(2))

	contentOffsets := record.Column(19).(*array.Int64)
	contentSizes := record.Column(20).(*array.Int64)
	require.True(t, contentOffsets.IsNull(0))
	require.True(t, contentOffsets.IsNull(1))
	require.EqualValues(t, 128, contentOffsets.Value(2))
	require.True(t, contentSizes.IsNull(0))
	require.True(t, contentSizes.IsNull(1))
	require.EqualValues(t, 512, contentSizes.Value(2))
}

func TestInspectDeleteFilesEmpty(t *testing.T) {
	spec := *iceberg.UnpartitionedSpec
	meta := &metadataV2{commonMetadata: commonMetadata{
		SchemaList:      []*iceberg.Schema{simpleSchema()},
		CurrentSchemaID: 0,
		Specs:           []iceberg.PartitionSpec{spec},
		DefaultSpecID:   0,
	}}
	tbl := New(Identifier{"empty-delete-files"}, meta, "metadata.json", nil, nil)

	rr, err := tbl.Inspect().DeleteFiles(context.Background())
	require.NoError(t, err)
	defer rr.Release()

	record := collectRecord(t, rr)
	defer record.Release()
	require.EqualValues(t, 0, record.NumRows())
	require.EqualValues(t, 20, record.NumCols())
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
		require.NoError(t, memIO.WriteFile(manifestPath, manifestBuf.Bytes()))
		manifestFiles = append(manifestFiles, manifest)
	}

	manifestListPath := "mem://default/table-location/metadata/inspect-manifest-list.avro"
	var listBuf bytes.Buffer
	sequenceNumber := int64(1)
	require.NoError(t, iceberg.WriteManifestList(3, &listBuf, snapshotID, nil, &sequenceNumber, 0, manifestFiles))
	require.NoError(t, memIO.WriteFile(manifestListPath, listBuf.Bytes()))

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

func TestInspectAllManifestsSchema(t *testing.T) {
	sc := AllManifestsSchema()
	require.Equal(t,
		append(testFieldNames(ManifestsSchema()), "reference_snapshot_id", "key_metadata"),
		testFieldNames(sc))

	fields := sc.Fields()
	require.Equal(t, 18, fields[12].ID)
	require.True(t, fields[12].Required)
	require.Equal(t, iceberg.PrimitiveTypes.Int64, fields[12].Type)
	require.Equal(t, 19, fields[13].ID)
	require.False(t, fields[13].Required)
	require.Equal(t, iceberg.PrimitiveTypes.Binary, fields[13].Type)
}

func TestInspectAllManifestsPreservesSnapshotReferences(t *testing.T) {
	memFS := iceio.NewMemFS()
	schema := simpleSchema()
	const tableLocation = "mem://default/inspect-all-manifests"
	meta, err := NewMetadata(schema, iceberg.UnpartitionedSpec, UnsortedSortOrder, tableLocation,
		iceberg.Properties{PropertyFormatVersion: "2"})
	require.NoError(t, err)
	builder, err := MetadataBuilderFromBase(meta, "")
	require.NoError(t, err)

	manifest := iceberg.NewManifestFile(2, tableLocation+"/metadata/shared-manifest.avro", 100, 0, 1).
		SequenceNum(1, 1).
		AddedFiles(1).
		KeyMetadata([]byte{1, 2, 3}).
		Build()
	manifestListPath := tableLocation + "/metadata/shared-list.avro"
	var list bytes.Buffer
	sequenceNumber := int64(1)
	require.NoError(t, iceberg.WriteManifestList(2, &list, 1, nil, &sequenceNumber, 0,
		[]iceberg.ManifestFile{manifest}))
	require.NoError(t, memFS.WriteFile(manifestListPath, list.Bytes()))

	schemaID := schema.ID
	snapshotOne := int64(1)
	timestamp := meta.LastUpdatedMillis()
	require.NoError(t, builder.AddSnapshot(&Snapshot{
		SnapshotID: snapshotOne, SequenceNumber: 1, TimestampMs: timestamp + 1,
		ManifestList: manifestListPath, SchemaID: &schemaID,
	}))
	require.NoError(t, builder.AddSnapshot(&Snapshot{
		SnapshotID: 2, ParentSnapshotID: &snapshotOne, SequenceNumber: 2, TimestampMs: timestamp + 2,
		ManifestList: manifestListPath, SchemaID: &schemaID,
	}))
	require.NoError(t, builder.SetSnapshotRef(MainBranch, 2, BranchRef))
	built, err := builder.Build()
	require.NoError(t, err)
	tbl := New(Identifier{"db", "all_manifests"}, built, tableLocation+"/metadata/metadata.json",
		func(context.Context) (iceio.IO, error) { return memFS, nil }, nil)

	rr, err := tbl.Inspect().AllManifests(context.Background())
	require.NoError(t, err)
	defer rr.Release()
	record := collectRecord(t, rr)
	defer record.Release()

	require.EqualValues(t, 2, record.NumRows())
	paths := record.Column(1).(*array.String)
	require.Equal(t, manifest.FilePath(), paths.Value(0))
	require.Equal(t, manifest.FilePath(), paths.Value(1))
	references := record.Column(12).(*array.Int64)
	require.Equal(t, []int64{1, 2}, references.Int64Values())
	keyMetadata := record.Column(13).(*array.Binary)
	require.Equal(t, []byte{1, 2, 3}, keyMetadata.Value(0))
	require.Equal(t, []byte{1, 2, 3}, keyMetadata.Value(1))
}

func TestInspectAllManifestsStreamsRecordBatches(t *testing.T) {
	const manifestCount = inspectRecordBatchSize + 1

	manifests := make([]iceberg.ManifestFile, manifestCount)
	for idx := range manifests {
		manifests[idx] = iceberg.NewManifestFile(2,
			fmt.Sprintf("mem://default/table-location/metadata/manifest-%04d.avro", idx),
			100, 0, 1).
			SequenceNum(1, 1).
			AddedFiles(1).
			Build()
	}
	tbl := inspectTableWithManifestList(t, *iceberg.UnpartitionedSpec, 2, manifests)

	rr, err := tbl.Inspect().AllManifests(context.Background())
	require.NoError(t, err)
	defer rr.Release()

	var batchSizes []int64
	for rr.Next() {
		batchSizes = append(batchSizes, rr.RecordBatch().NumRows())
	}
	require.NoError(t, rr.Err())
	require.Equal(t, []int64{inspectRecordBatchSize, 1}, batchSizes)
}

func TestInspectAllManifestsAllocator(t *testing.T) {
	for _, tt := range []struct {
		name          string
		manifestCount int
		next          bool
	}{
		{name: "before first batch", manifestCount: 1},
		{name: "after first batch", manifestCount: inspectRecordBatchSize + 1, next: true},
	} {
		t.Run(tt.name, func(t *testing.T) {
			checked := memory.NewCheckedAllocator(memory.DefaultAllocator)
			t.Cleanup(func() { checked.AssertSize(t, 0) })

			manifests := make([]iceberg.ManifestFile, tt.manifestCount)
			for idx := range manifests {
				manifests[idx] = iceberg.NewManifestFile(2,
					fmt.Sprintf("mem://default/table-location/metadata/manifest-%04d.avro", idx),
					100, 0, 1).
					SequenceNum(1, 1).
					AddedFiles(1).
					Build()
			}
			tbl := inspectTableWithManifestList(t, *iceberg.UnpartitionedSpec, 2,
				manifests)

			rr, err := tbl.Inspect(WithInspectAllocator(checked)).AllManifests(context.Background())
			require.NoError(t, err)
			if tt.next {
				require.True(t, rr.Next())
			}
			rr.Release()
		})
	}
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

func TestInspectAllEntriesDeduplicatesSharedManifests(t *testing.T) {
	spec := *iceberg.UnpartitionedSpec
	txn, memIO := createTestTransactionWithMemIO(t, spec)
	schema := simpleSchema()
	snapshotOne, snapshotTwo := int64(1), int64(2)
	sequenceOne, sequenceTwo := int64(1), int64(2)

	sharedAdded := newTestDataFile(t, spec,
		"mem://default/table-location/data/shared-added.parquet", nil)
	sharedDeleted := newTestDataFile(t, spec,
		"mem://default/table-location/data/shared-deleted.parquet", nil)
	newFile := newTestDataFile(t, spec,
		"mem://default/table-location/data/new.parquet", nil)
	sharedEntries := []iceberg.ManifestEntry{
		iceberg.NewManifestEntry(iceberg.EntryStatusADDED, &snapshotOne,
			&sequenceOne, &sequenceOne, sharedAdded),
		iceberg.NewManifestEntry(iceberg.EntryStatusDELETED, &snapshotOne,
			&sequenceOne, &sequenceOne, sharedDeleted),
	}
	newEntries := []iceberg.ManifestEntry{
		iceberg.NewManifestEntry(iceberg.EntryStatusADDED, &snapshotTwo,
			&sequenceTwo, &sequenceTwo, newFile),
	}

	writeManifest := func(path string, snapshotID int64, entries []iceberg.ManifestEntry) iceberg.ManifestFile {
		var buf bytes.Buffer
		manifest, err := iceberg.WriteManifest(path, &buf, 2, spec, schema, snapshotID, entries)
		require.NoError(t, err)
		require.NoError(t, memIO.WriteFile(path, buf.Bytes()))

		return manifest
	}
	sharedManifest := writeManifest(
		"mem://default/table-location/metadata/shared.avro", snapshotOne, sharedEntries)
	newManifest := writeManifest(
		"mem://default/table-location/metadata/new.avro", snapshotTwo, newEntries)

	writeList := func(path string, snapshotID int64, parent *int64, sequenceNumber int64,
		manifests []iceberg.ManifestFile,
	) []iceberg.ManifestFile {
		var buf bytes.Buffer
		require.NoError(t, iceberg.WriteManifestList(2, &buf, snapshotID, parent,
			&sequenceNumber, 0, manifests))
		require.NoError(t, memIO.WriteFile(path, buf.Bytes()))
		written, err := iceberg.ReadManifestList(bytes.NewReader(buf.Bytes()))
		require.NoError(t, err)

		return written
	}
	listOne := "mem://default/table-location/metadata/snap-1.avro"
	listTwo := "mem://default/table-location/metadata/snap-2.avro"
	writtenOne := writeList(listOne, snapshotOne, nil, sequenceOne,
		[]iceberg.ManifestFile{sharedManifest})
	writeList(listTwo, snapshotTwo, &snapshotOne, sequenceTwo,
		[]iceberg.ManifestFile{writtenOne[0], newManifest})

	txn.meta.snapshotList = []Snapshot{
		{SnapshotID: snapshotOne, ManifestList: listOne, SequenceNumber: sequenceOne},
		{
			SnapshotID: snapshotTwo, ParentSnapshotID: &snapshotOne,
			ManifestList: listTwo, SequenceNumber: sequenceTwo,
		},
	}
	txn.meta.currentSnapshotID = &snapshotTwo
	built, err := txn.meta.Build()
	require.NoError(t, err)
	tbl := New(Identifier{"db", "tbl"}, built, "metadata.json",
		func(context.Context) (iceio.IO, error) { return memIO, nil }, nil)

	rr, err := tbl.Inspect().AllEntries(context.Background())
	require.NoError(t, err)
	defer rr.Release()
	record := collectRecord(t, rr)
	defer record.Release()

	require.EqualValues(t, 3, record.NumRows())
	status := record.Column(0).(*array.Int32)
	require.Equal(t, []int32{
		int32(iceberg.EntryStatusADDED),
		int32(iceberg.EntryStatusDELETED),
		int32(iceberg.EntryStatusADDED),
	}, status.Int32Values())
	dataFiles := record.Column(4).(*array.Struct)
	paths := dataFiles.Field(1).(*array.String)
	require.Equal(t, []string{
		sharedAdded.FilePath(),
		sharedDeleted.FilePath(),
		newFile.FilePath(),
	}, []string{paths.Value(0), paths.Value(1), paths.Value(2)})
}

func TestInspectAllEntriesIncludesDeleteManifests(t *testing.T) {
	tbl := inspectAllFilesTable(t)

	rr, err := tbl.Inspect().AllEntries(context.Background())
	require.NoError(t, err)
	defer rr.Release()

	record := collectRecord(t, rr)
	defer record.Release()
	require.EqualValues(t, 5, record.NumRows())

	statuses := record.Column(0).(*array.Int32)
	require.Equal(t, []int32{
		int32(iceberg.EntryStatusADDED),
		int32(iceberg.EntryStatusDELETED),
		int32(iceberg.EntryStatusADDED),
		int32(iceberg.EntryStatusADDED),
		int32(iceberg.EntryStatusADDED),
	}, statuses.Int32Values())

	dataFiles := record.Column(4).(*array.Struct)
	contents := dataFiles.Field(0).(*array.Int32)
	paths := dataFiles.Field(1).(*array.String)
	require.Equal(t, []int32{
		int32(iceberg.EntryContentData),
		int32(iceberg.EntryContentData),
		int32(iceberg.EntryContentData),
		int32(iceberg.EntryContentData),
		int32(iceberg.EntryContentPosDeletes),
	}, contents.Int32Values())
	require.Equal(t, []string{
		"mem://default/table-location/data/shared.parquet",
		"mem://default/table-location/data/deleted.parquet",
		"mem://default/table-location/data/shared.parquet",
		"mem://default/table-location/data/new.parquet",
		"mem://default/table-location/data/delete.parquet",
	}, []string{
		paths.Value(0), paths.Value(1), paths.Value(2), paths.Value(3), paths.Value(4),
	})
}

func TestInspectAllEntriesStreamsBatches(t *testing.T) {
	spec := *iceberg.UnpartitionedSpec
	tbl := inspectDataFilesTable(t, spec,
		inspectDataFileEntries(t, spec, inspectRecordBatchSize+1))

	rr, err := tbl.Inspect().AllEntries(context.Background())
	require.NoError(t, err)
	defer rr.Release()

	var batchRows []int
	for rr.Next() {
		batchRows = append(batchRows, int(rr.RecordBatch().NumRows()))
	}
	require.NoError(t, rr.Err())
	require.Equal(t, []int{inspectRecordBatchSize, 1}, batchRows)
}

func TestInspectAllEntriesEmptyTableEarlyRelease(t *testing.T) {
	checked := memory.NewCheckedAllocator(memory.DefaultAllocator)
	t.Cleanup(func() { checked.AssertSize(t, 0) })
	meta := &metadataV2{commonMetadata: commonMetadata{
		SchemaList:      []*iceberg.Schema{simpleSchema()},
		CurrentSchemaID: 0,
		Specs:           []iceberg.PartitionSpec{*iceberg.UnpartitionedSpec},
		DefaultSpecID:   0,
	}}
	tbl := New(Identifier{"empty-all-entries"}, meta, "metadata.json", nil, nil)

	rr, err := tbl.Inspect(WithInspectAllocator(checked)).AllEntries(context.Background())
	require.NoError(t, err)
	require.True(t, rr.Next())
	require.EqualValues(t, 0, rr.RecordBatch().NumRows())
	rr.Release()
}

func TestInspectAllEntriesStopsOnContextCancellation(t *testing.T) {
	tbl := inspectAllFilesTable(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	rr, err := tbl.Inspect().AllEntries(ctx)
	require.NoError(t, err)
	cancel()

	require.False(t, rr.Next())
	require.ErrorIs(t, rr.Err(), context.Canceled)
	rr.Release()
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

func TestDeleteFilesSchema(t *testing.T) {
	sc := DeleteFilesSchema(&iceberg.StructType{})
	names := testFieldNames(sc)
	require.Equal(t, testFieldNames(DataFilesSchema(&iceberg.StructType{})), names)
	require.Equal(t, "content", names[0])
	require.Equal(t, "file_path", names[1])
	require.Equal(t, "equality_ids", names[14])
	require.Equal(t, "referenced_data_file", names[17])
	require.Equal(t, 134, sc.Fields()[0].ID)
	require.Equal(t, 145, sc.Fields()[len(sc.Fields())-1].ID)
	require.NotContains(t, names, "partition")
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
	for index := range count {
		path := "mem://default/table-location/data/live-" + strconv.Itoa(index) + ".parquet"
		file := newTestDataFile(t, spec, path, nil)
		sequenceNumber := int64(1)
		entries = append(entries, iceberg.NewManifestEntry(
			iceberg.EntryStatusADDED, int64Ptr(snapshotID), &sequenceNumber, &sequenceNumber, file))
	}

	return entries
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
	require.NoError(t, fs.WriteFile(path, buf.Bytes()))

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
		require.NoError(t, memIO.WriteFile(path, buf.Bytes()))
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

func inspectFileRows(t *testing.T, rr array.RecordReader) (paths []string, contents []int32) {
	t.Helper()
	defer rr.Release()
	for rr.Next() {
		record := rr.RecordBatch()
		content := record.Column(0).(*array.Int32)
		filePath := record.Column(1).(*array.String)
		for row := range int(record.NumRows()) {
			contents = append(contents, content.Value(row))
			paths = append(paths, filePath.Value(row))
		}
	}
	require.NoError(t, rr.Err())

	return paths, contents
}

func TestInspectFilesTables(t *testing.T) {
	tbl := inspectAllFilesTable(t)
	tests := []struct {
		name        string
		read        func(context.Context) (array.RecordReader, error)
		wantPaths   []string
		wantContent []int32
	}{
		{
			name: "files",
			read: tbl.Inspect().Files,
			wantPaths: []string{
				"mem://default/table-location/data/shared.parquet",
				"mem://default/table-location/data/new.parquet",
				"mem://default/table-location/data/delete.parquet",
			},
			wantContent: []int32{
				int32(iceberg.EntryContentData), int32(iceberg.EntryContentData),
				int32(iceberg.EntryContentPosDeletes),
			},
		},
		{
			name: "all files deduplicates shared manifest and preserves duplicate rows",
			read: tbl.Inspect().AllFiles,
			wantPaths: []string{
				"mem://default/table-location/data/shared.parquet",
				"mem://default/table-location/data/shared.parquet",
				"mem://default/table-location/data/new.parquet",
				"mem://default/table-location/data/delete.parquet",
			},
			wantContent: []int32{
				int32(iceberg.EntryContentData), int32(iceberg.EntryContentData),
				int32(iceberg.EntryContentData),
				int32(iceberg.EntryContentPosDeletes),
			},
		},
		{
			name: "all data files",
			read: tbl.Inspect().AllDataFiles,
			wantPaths: []string{
				"mem://default/table-location/data/shared.parquet",
				"mem://default/table-location/data/shared.parquet",
				"mem://default/table-location/data/new.parquet",
			},
			wantContent: []int32{
				int32(iceberg.EntryContentData), int32(iceberg.EntryContentData),
				int32(iceberg.EntryContentData),
			},
		},
		{
			name:        "all delete files",
			read:        tbl.Inspect().AllDeleteFiles,
			wantPaths:   []string{"mem://default/table-location/data/delete.parquet"},
			wantContent: []int32{int32(iceberg.EntryContentPosDeletes)},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			rr, err := tt.read(context.Background())
			require.NoError(t, err)
			paths, contents := inspectFileRows(t, rr)
			require.Equal(t, tt.wantPaths, paths)
			require.Equal(t, tt.wantContent, contents)
		})
	}
}

func TestInspectAllFilesStopsOnContextCancellation(t *testing.T) {
	tbl := inspectAllFilesTable(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	rr, err := tbl.Inspect().AllFiles(ctx)
	require.NoError(t, err)

	cancel()
	require.False(t, rr.Next())
	require.ErrorIs(t, rr.Err(), context.Canceled)
	rr.Release()
}

func TestInspectFilesTablesEarlyRelease(t *testing.T) {
	checked := memory.NewCheckedAllocator(memory.DefaultAllocator)
	t.Cleanup(func() { checked.AssertSize(t, 0) })
	tbl := inspectAllFilesTable(t)
	reads := []struct {
		name string
		read func(context.Context) (array.RecordReader, error)
	}{
		{name: "files", read: tbl.Inspect(WithInspectAllocator(checked)).Files},
		{name: "data files", read: tbl.Inspect(WithInspectAllocator(checked)).DataFiles},
		{name: "delete files", read: tbl.Inspect(WithInspectAllocator(checked)).DeleteFiles},
		{name: "all files", read: tbl.Inspect(WithInspectAllocator(checked)).AllFiles},
		{name: "all data files", read: tbl.Inspect(WithInspectAllocator(checked)).AllDataFiles},
		{name: "all delete files", read: tbl.Inspect(WithInspectAllocator(checked)).AllDeleteFiles},
		{name: "all entries", read: tbl.Inspect(WithInspectAllocator(checked)).AllEntries},
	}

	for _, tt := range reads {
		t.Run(tt.name, func(t *testing.T) {
			rr, err := tt.read(context.Background())
			require.NoError(t, err)
			require.True(t, rr.Next())
			rr.Release()
		})
	}
}

func TestInspectAllFilesReadsManifestListsLazily(t *testing.T) {
	tbl := historyTestTable()
	fs := iceio.NewMemFS()
	tbl.fsF = func(context.Context) (iceio.IO, error) { return fs, nil }

	rr, err := tbl.Inspect().AllFiles(context.Background())
	require.NoError(t, err)
	require.False(t, rr.Next())
	require.ErrorContains(t, rr.Err(), "read snapshot 101 manifests")
	rr.Release()
}

func TestInspectAllFilesSchemasMatchFiles(t *testing.T) {
	partitionType := &iceberg.StructType{FieldList: []iceberg.NestedField{
		{ID: 1000, Name: "part", Type: iceberg.PrimitiveTypes.String, Required: false},
	}}
	want := DataFilesSchema(partitionType)
	for _, schema := range []*iceberg.Schema{
		FilesSchema(partitionType),
		AllFilesSchema(partitionType),
		AllDataFilesSchema(partitionType),
		AllDeleteFilesSchema(partitionType),
	} {
		require.True(t, want.Equals(schema))
	}
}

func TestInspectDataFilesStreamsBatchesAndSkipsDeleted(t *testing.T) {
	const snapshotID = int64(1)
	spec := *iceberg.UnpartitionedSpec
	txn, memIO := createTestTransactionWithMemIO(t, spec)
	schema := simpleSchema()

	entries := make([]iceberg.ManifestEntry, 0, inspectRecordBatchSize+2)
	for index := range inspectRecordBatchSize + 1 {
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
		for row := range filePaths.Len() {
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
	require.True(t, rr.Next())
	record := rr.RecordBatch()
	require.EqualValues(t, 0, record.NumRows())
	rr.Release()
}

func TestInspectDataFilesEmptyTableEarlyRelease(t *testing.T) {
	meta := &metadataV2{commonMetadata: commonMetadata{
		SchemaList:      []*iceberg.Schema{simpleSchema()},
		CurrentSchemaID: 0,
		Specs:           []iceberg.PartitionSpec{*iceberg.UnpartitionedSpec},
		DefaultSpecID:   0,
	}}
	tbl := New(Identifier{"empty-data-files"}, meta, "metadata.json", nil, nil)

	rr, err := tbl.Inspect().DataFiles(context.Background())
	require.NoError(t, err)
	require.True(t, rr.Next())
	require.EqualValues(t, 0, rr.RecordBatch().NumRows())
	rr.Release()
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

func TestAppendContentFileRecordUsesPartitionFieldIDs(t *testing.T) {
	partitionType := &iceberg.StructType{FieldList: []iceberg.NestedField{
		{ID: 1000, Name: "first", Type: iceberg.PrimitiveTypes.Int32},
		{ID: 1001, Name: "second", Type: iceberg.PrimitiveTypes.Int32},
	}}
	reorderedPartitionType := &iceberg.StructType{FieldList: []iceberg.NestedField{
		partitionType.FieldList[1], partitionType.FieldList[0],
	}}
	arrowSchema, err := SchemaToArrowSchema(DataFilesSchema(reorderedPartitionType), nil, true, false)
	require.NoError(t, err)

	bldr := array.NewRecordBuilder(memory.DefaultAllocator, arrowSchema)
	defer bldr.Release()
	spec := iceberg.NewPartitionSpec(
		iceberg.PartitionField{SourceIDs: []int{1}, FieldID: 1000, Name: "first", Transform: iceberg.IdentityTransform{}},
		iceberg.PartitionField{SourceIDs: []int{2}, FieldID: 1001, Name: "second", Transform: iceberg.IdentityTransform{}},
	)
	file := newTestDataFile(t, spec, "mem://default/table-location/data/reordered-partition.parquet", map[int]any{
		1000: int32(7), 1001: int32(9),
	})
	require.NoError(t, appendContentFileRecord(bldr, partitionType, file))

	record := bldr.NewRecordBatch()
	defer record.Release()
	partitionIndex := record.Schema().FieldIndices("partition")[0]
	partition := record.Column(partitionIndex).(*array.Struct)
	require.EqualValues(t, 9, partition.Field(0).(*array.Int32).Value(0))
	require.EqualValues(t, 7, partition.Field(1).(*array.Int32).Value(0))
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
			name:  "pointer void transition is compatible",
			specs: []iceberg.PartitionSpec{field(0, 1, iceberg.IdentityTransform{}), field(1, 1, &iceberg.VoidTransform{})},
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

	unknown, err := iceberg.ParseTransform("future_transform")
	require.NoError(t, err)
	_, err = inspectPartitionType(metadataFor([]iceberg.PartitionSpec{field(0, 1, unknown)}))
	require.ErrorIs(t, err, iceberg.ErrInvalidPartitionSpec)
}

func TestPartitionsSchema(t *testing.T) {
	partitionType := &iceberg.StructType{FieldList: []iceberg.NestedField{
		{ID: 1000, Name: "bucket", Type: iceberg.PrimitiveTypes.Int32, Required: true},
	}}
	sc := PartitionsSchema(partitionType)
	require.Equal(t, []string{
		"partition", "spec_id", "record_count", "file_count",
		"total_data_file_size_in_bytes", "position_delete_record_count", "position_delete_file_count",
		"equality_delete_record_count", "equality_delete_file_count", "last_updated_at",
		"last_updated_snapshot_id",
	}, testFieldNames(sc))
	require.Equal(t, 1, sc.Fields()[0].ID)
	require.Equal(t, 4, sc.Fields()[1].ID)
	require.Equal(t, 11, sc.Fields()[4].ID)
	require.Equal(t, 10, sc.Fields()[10].ID)

	unpartitioned := PartitionsSchema(&iceberg.StructType{})
	require.Equal(t, []string{
		"record_count", "file_count", "total_data_file_size_in_bytes",
		"position_delete_record_count", "position_delete_file_count", "equality_delete_record_count",
		"equality_delete_file_count", "last_updated_at", "last_updated_snapshot_id",
	}, testFieldNames(unpartitioned))
}

func TestInspectPartitionAggregateTreeHandlesBinaryAndNaNValues(t *testing.T) {
	tree := newInspectPartitionAggregateTree()
	aggregate := &inspectPartitionAggregate{specID: 1}
	record := partitionRecord{[]byte{1, 2, 3}, math.NaN()}
	tree.insert(record, aggregate)

	require.Same(t, aggregate, tree.lookup(partitionRecord{[]byte{1, 2, 3}, math.NaN()}))
	require.Nil(t, tree.lookup(partitionRecord{[]byte{1, 2, 4}, math.NaN()}))
}

func TestInspectPartitionsAggregatesDataAndDeletes(t *testing.T) {
	const snapshotID = int64(1)
	spec := partitionedSpec()
	txn, memIO := createTestTransactionWithMemIO(t, spec)
	schema := simpleSchema()
	partition := map[int]any{1000: int32(7)}
	dataSequenceNumber := int64(1)
	dataFiles := []iceberg.DataFile{
		newTestDataFileWithCount(t, spec,
			"mem://default/table-location/data/first.parquet", partition, 3),
		newTestDataFileWithCount(t, spec,
			"mem://default/table-location/data/second.parquet", partition, 2),
	}
	dataEntries := make([]iceberg.ManifestEntry, 0, len(dataFiles))
	for _, file := range dataFiles {
		dataEntries = append(dataEntries, iceberg.NewManifestEntry(
			iceberg.EntryStatusADDED, int64Ptr(snapshotID), &dataSequenceNumber, &dataSequenceNumber, file))
	}

	dataManifestPath := "mem://default/table-location/metadata/data-manifest.avro"
	var dataManifestBuf bytes.Buffer
	dataManifest, err := iceberg.WriteManifest(dataManifestPath, &dataManifestBuf, 2, spec, schema, snapshotID, dataEntries)
	require.NoError(t, err)
	require.NoError(t, memIO.WriteFile(dataManifestPath, dataManifestBuf.Bytes()))

	deleteFile := newTestPosDeleteFileForSpec(t, spec,
		"mem://default/table-location/delete/positions.parquet", partition,
		dataFiles[0].FilePath())
	equalityDeleteBuilder, err := iceberg.NewDataFileBuilder(
		spec,
		iceberg.EntryContentEqDeletes,
		"mem://default/table-location/delete/equality.parquet",
		iceberg.ParquetFile,
		partition,
		nil,
		nil,
		4,
		4,
	)
	require.NoError(t, err)
	equalityDeleteFile := equalityDeleteBuilder.EqualityFieldIDs([]int{1}).Build()
	deleteManifestPath := "mem://default/table-location/metadata/delete-manifest.avro"
	var deleteManifestBuf bytes.Buffer
	deleteWriter, err := iceberg.NewManifestWriter(2, &deleteManifestBuf, spec, schema, snapshotID,
		iceberg.WithManifestWriterContent(iceberg.ManifestContentDeletes))
	require.NoError(t, err)
	require.NoError(t, deleteWriter.Add(iceberg.NewManifestEntry(
		iceberg.EntryStatusADDED, int64Ptr(snapshotID), &dataSequenceNumber, &dataSequenceNumber, deleteFile)))
	require.NoError(t, deleteWriter.Add(iceberg.NewManifestEntry(
		iceberg.EntryStatusADDED, int64Ptr(snapshotID), &dataSequenceNumber, &dataSequenceNumber, equalityDeleteFile)))
	require.NoError(t, deleteWriter.Close())
	deleteManifest, err := deleteWriter.ToManifestFile(deleteManifestPath, int64(deleteManifestBuf.Len()),
		iceberg.WithManifestFileContent(iceberg.ManifestContentDeletes))
	require.NoError(t, err)
	require.NoError(t, memIO.WriteFile(deleteManifestPath, deleteManifestBuf.Bytes()))

	manifestListPath := "mem://default/table-location/metadata/snap-1-manifest-list.avro"
	var listBuf bytes.Buffer
	manifestListSequenceNumber := int64(1)
	require.NoError(t, iceberg.WriteManifestList(2, &listBuf, snapshotID, nil,
		&manifestListSequenceNumber, 0, []iceberg.ManifestFile{dataManifest, deleteManifest}))
	require.NoError(t, memIO.WriteFile(manifestListPath, listBuf.Bytes()))

	txn.meta.snapshotList = []Snapshot{{
		SnapshotID:     snapshotID,
		ManifestList:   manifestListPath,
		SequenceNumber: manifestListSequenceNumber,
		TimestampMs:    2000,
	}}
	txn.meta.currentSnapshotID = int64Ptr(snapshotID)
	built, err := txn.meta.Build()
	require.NoError(t, err)

	tbl := New(Identifier{"db", "tbl"}, built, "metadata.json",
		func(context.Context) (iceio.IO, error) { return memIO, nil }, nil)
	rr, err := tbl.Inspect().Partitions(context.Background())
	require.NoError(t, err)
	defer rr.Release()

	record := collectRecord(t, rr)
	defer record.Release()
	require.EqualValues(t, 1, record.NumRows())

	partitionValues := record.Column(0).(*array.Struct)
	require.EqualValues(t, 7, partitionValues.Field(0).(*array.Int32).Value(0))
	require.EqualValues(t, spec.ID(), record.Column(1).(*array.Int32).Value(0))
	require.EqualValues(t, 5, record.Column(2).(*array.Int64).Value(0))
	require.EqualValues(t, 2, record.Column(3).(*array.Int32).Value(0))
	require.EqualValues(t, 5, record.Column(4).(*array.Int64).Value(0))
	require.EqualValues(t, 1, record.Column(5).(*array.Int64).Value(0))
	require.EqualValues(t, 1, record.Column(6).(*array.Int32).Value(0))
	require.EqualValues(t, 4, record.Column(7).(*array.Int64).Value(0))
	require.EqualValues(t, 1, record.Column(8).(*array.Int32).Value(0))
	require.EqualValues(t, 2000*1000, record.Column(9).(*array.Timestamp).Value(0))
	require.EqualValues(t, snapshotID, record.Column(10).(*array.Int64).Value(0))
}

func TestInspectPartitionsLeavesSpecIDUnsetForExpiredSnapshot(t *testing.T) {
	const (
		expiredSnapshotID = int64(1)
		currentSnapshotID = int64(2)
	)
	spec := iceberg.NewPartitionSpecID(7, iceberg.PartitionField{
		SourceIDs: []int{1}, FieldID: 1000, Name: "id", Transform: iceberg.IdentityTransform{},
	})
	txn, memIO := createTestTransactionWithMemIO(t, spec)
	schema := simpleSchema()
	file := newTestDataFile(t, spec,
		"mem://default/table-location/data/expired.parquet", map[int]any{1000: int32(7)})
	sequenceNumber := int64(1)
	manifestPath := "mem://default/table-location/metadata/expired-manifest.avro"
	var manifestBuf bytes.Buffer
	_, err := iceberg.WriteManifest(manifestPath, &manifestBuf, 2, spec, schema, expiredSnapshotID,
		[]iceberg.ManifestEntry{iceberg.NewManifestEntry(
			iceberg.EntryStatusADDED, int64Ptr(expiredSnapshotID), &sequenceNumber, &sequenceNumber, file)})
	require.NoError(t, err)
	require.NoError(t, memIO.WriteFile(manifestPath, manifestBuf.Bytes()))
	manifest := iceberg.NewManifestFile(2, manifestPath, int64(manifestBuf.Len()), int32(spec.ID()), expiredSnapshotID).
		SequenceNum(sequenceNumber, sequenceNumber).
		AddedFiles(1).
		AddedRows(1).
		Build()

	manifestListPath := "mem://default/table-location/metadata/snap-2-manifest-list.avro"
	var listBuf bytes.Buffer
	currentSequenceNumber := int64(2)
	require.NoError(t, iceberg.WriteManifestList(2, &listBuf, currentSnapshotID, nil,
		&currentSequenceNumber, 0, []iceberg.ManifestFile{manifest}))
	require.NoError(t, memIO.WriteFile(manifestListPath, listBuf.Bytes()))

	txn.meta.snapshotList = []Snapshot{{
		SnapshotID:     currentSnapshotID,
		ManifestList:   manifestListPath,
		SequenceNumber: currentSequenceNumber,
		TimestampMs:    2000,
	}}
	txn.meta.currentSnapshotID = int64Ptr(currentSnapshotID)
	built, err := txn.meta.Build()
	require.NoError(t, err)

	tbl := New(Identifier{"db", "tbl"}, built, "metadata.json",
		func(context.Context) (iceio.IO, error) { return memIO, nil }, nil)
	rr, err := tbl.Inspect().Partitions(context.Background())
	require.NoError(t, err)
	defer rr.Release()

	record := collectRecord(t, rr)
	defer record.Release()
	require.EqualValues(t, 1, record.NumRows())
	require.EqualValues(t, 0, record.Column(1).(*array.Int32).Value(0))
	require.True(t, record.Column(9).IsNull(0))
	require.True(t, record.Column(10).IsNull(0))
}

func TestEntriesSchema(t *testing.T) {
	sc := EntriesSchema(&iceberg.StructType{})
	require.Equal(t, []string{"status", "snapshot_id", "sequence_number", "file_sequence_number", "data_file"}, testFieldNames(sc))
	require.Equal(t, 0, sc.Fields()[0].ID)
	require.Equal(t, 2, sc.Fields()[4].ID)
	require.True(t, sc.Fields()[4].Required)

	dataFile := sc.Fields()[4].Type.(*iceberg.StructType)
	require.Equal(t, 134, dataFile.FieldList[0].ID)
	require.Equal(t, "file_path", dataFile.FieldList[1].Name)
	require.Equal(t, 145, dataFile.FieldList[len(dataFile.FieldList)-1].ID)
}

func TestEntriesDataFileStructBuilderAppendsParent(t *testing.T) {
	arrowSchema, err := SchemaToArrowSchema(EntriesSchema(&iceberg.StructType{}), nil, true, false)
	require.NoError(t, err)

	bldr := array.NewRecordBuilder(memory.DefaultAllocator, arrowSchema)
	defer bldr.Release()
	bldr.Field(0).(*array.Int32Builder).Append(int32(iceberg.EntryStatusADDED))
	bldr.Field(1).(*array.Int64Builder).Append(7)
	bldr.Field(2).(*array.Int64Builder).Append(8)
	bldr.Field(3).(*array.Int64Builder).AppendNull()

	err = appendContentFile(bldr.Field(4).(*array.StructBuilder), &iceberg.StructType{}, &mockDataFile{
		path:        "data.parquet",
		contentType: iceberg.EntryContentData,
		format:      iceberg.ParquetFile,
	})
	require.NoError(t, err)

	rec := bldr.NewRecordBatch()
	defer rec.Release()
	require.EqualValues(t, 1, rec.NumRows())
	dataFile := rec.Column(4).(*array.Struct)
	require.EqualValues(t, 1, dataFile.Len())
	require.False(t, dataFile.IsNull(0))
}

func TestInspectEntriesIncludesDeletedEntries(t *testing.T) {
	const snapshotID = int64(1)
	spec := *iceberg.UnpartitionedSpec
	txn, memIO := createTestTransactionWithMemIO(t, spec)
	schema := simpleSchema()
	addedFile := newTestDataFile(t, spec,
		"mem://default/table-location/data/added.parquet", nil)
	deletedFile := newTestDataFile(t, spec,
		"mem://default/table-location/data/deleted.parquet", nil)
	sequenceNumber := int64(1)
	entries := []iceberg.ManifestEntry{
		iceberg.NewManifestEntry(iceberg.EntryStatusADDED, int64Ptr(snapshotID), &sequenceNumber, &sequenceNumber, addedFile),
		iceberg.NewManifestEntry(iceberg.EntryStatusDELETED, int64Ptr(snapshotID), &sequenceNumber, &sequenceNumber, deletedFile),
	}

	manifestPath := "mem://default/table-location/metadata/data-manifest.avro"
	manifestListPath := "mem://default/table-location/metadata/snap-1-manifest-list.avro"
	var manifestBuf bytes.Buffer
	manifest, err := iceberg.WriteManifest(manifestPath, &manifestBuf, 2, spec, schema, snapshotID, entries)
	require.NoError(t, err)
	require.NoError(t, memIO.WriteFile(manifestPath, manifestBuf.Bytes()))

	var listBuf bytes.Buffer
	require.NoError(t, iceberg.WriteManifestList(2, &listBuf, snapshotID, nil, &sequenceNumber, 0,
		[]iceberg.ManifestFile{manifest}))
	require.NoError(t, memIO.WriteFile(manifestListPath, listBuf.Bytes()))

	txn.meta.snapshotList = []Snapshot{{
		SnapshotID:     snapshotID,
		ManifestList:   manifestListPath,
		SequenceNumber: sequenceNumber,
	}}
	txn.meta.currentSnapshotID = int64Ptr(snapshotID)
	built, err := txn.meta.Build()
	require.NoError(t, err)

	tbl := New(Identifier{"db", "tbl"}, built, "metadata.json",
		func(context.Context) (iceio.IO, error) { return memIO, nil }, nil)
	rr, err := tbl.Inspect().Entries(context.Background())
	require.NoError(t, err)
	defer rr.Release()

	record := collectRecord(t, rr)
	defer record.Release()
	require.EqualValues(t, 2, record.NumRows())
	status := record.Column(0).(*array.Int32)
	require.EqualValues(t, iceberg.EntryStatusADDED, status.Value(0))
	require.EqualValues(t, iceberg.EntryStatusDELETED, status.Value(1))

	dataFiles := record.Column(4).(*array.Struct)
	require.False(t, dataFiles.IsNull(0))
	paths := dataFiles.Field(1).(*array.String)
	require.Equal(t, addedFile.FilePath(), paths.Value(0))
	require.Equal(t, deletedFile.FilePath(), paths.Value(1))
}

func TestAllEntriesSchemaMatchesEntries(t *testing.T) {
	partitionType := &iceberg.StructType{FieldList: []iceberg.NestedField{
		{ID: 1000, Name: "part", Type: iceberg.PrimitiveTypes.Int32, Required: false},
	}}
	require.True(t, EntriesSchema(partitionType).Equals(AllEntriesSchema(partitionType)))
}

func newInspectPositionDeletesMetadata(t *testing.T, formatVersion int) *MetadataBuilder {
	return newInspectPositionDeletesMetadataWithSchema(t, formatVersion, simpleSchema())
}

func newInspectPositionDeletesMetadataWithSchema(
	t *testing.T,
	formatVersion int,
	tableSchema *iceberg.Schema,
) *MetadataBuilder {
	t.Helper()

	mb, err := NewMetadataBuilder(formatVersion)
	require.NoError(t, err)
	require.NoError(t, mb.AddSchema(tableSchema))
	require.NoError(t, mb.SetCurrentSchemaID(0))
	require.NoError(t, mb.AddPartitionSpec(iceberg.UnpartitionedSpec, true))
	require.NoError(t, mb.SetDefaultSpecID(0))
	require.NoError(t, mb.SetLoc("mem://position-deletes/table"))
	addUnsortedSortOrder(t, mb)
	metadata, err := mb.Build()
	require.NoError(t, err)
	mb, err = MetadataBuilderFromBase(metadata, "")
	require.NoError(t, err)

	return mb
}

func inspectPositionDeletesTable(
	t *testing.T,
	formatVersion int,
	mb *MetadataBuilder,
	fs iceio.WriteFileIO,
	files []iceberg.DataFile,
) *Table {
	return inspectPositionDeletesTableWithSchema(t, formatVersion, mb, simpleSchema(), fs, files)
}

func inspectPositionDeletesTableWithSchema(
	t *testing.T,
	formatVersion int,
	mb *MetadataBuilder,
	tableSchema *iceberg.Schema,
	fs iceio.WriteFileIO,
	files []iceberg.DataFile,
) *Table {
	t.Helper()

	snapshotID := int64(1)
	sequenceNumber := int64(1)
	manifestPath := "mem://position-deletes/table/metadata/deletes.avro"
	manifestBuffer := &bytes.Buffer{}
	writer, err := iceberg.NewManifestWriter(
		formatVersion, manifestBuffer, *iceberg.UnpartitionedSpec,
		tableSchema, snapshotID,
		iceberg.WithManifestWriterContent(iceberg.ManifestContentDeletes),
	)
	require.NoError(t, err)
	for _, file := range files {
		require.NoError(t, writer.Add(iceberg.NewManifestEntry(
			iceberg.EntryStatusADDED, &snapshotID, &sequenceNumber, &sequenceNumber, file)))
	}
	manifest, err := writer.ToManifestFile(
		manifestPath, int64(manifestBuffer.Len()),
		iceberg.WithManifestFileContent(iceberg.ManifestContentDeletes),
	)
	require.NoError(t, err)
	require.NoError(t, fs.WriteFile(manifestPath, manifestBuffer.Bytes()))

	return inspectPositionDeletesTableWithManifests(
		t, formatVersion, mb, tableSchema, fs, []iceberg.ManifestFile{manifest})
}

func inspectPositionDeletesTableWithManifests(
	t *testing.T,
	formatVersion int,
	mb *MetadataBuilder,
	tableSchema *iceberg.Schema,
	fs iceio.WriteFileIO,
	manifests []iceberg.ManifestFile,
) *Table {
	t.Helper()

	snapshotID := int64(1)
	sequenceNumber := int64(1)
	manifestListPath := "mem://position-deletes/table/metadata/snap.avro"
	manifestListBuffer := &bytes.Buffer{}
	require.NoError(t, iceberg.WriteManifestList(
		formatVersion, manifestListBuffer, snapshotID, nil, &sequenceNumber, 0,
		manifests,
	))
	require.NoError(t, fs.WriteFile(manifestListPath, manifestListBuffer.Bytes()))

	schemaID := tableSchema.ID
	snapshot := &Snapshot{
		SnapshotID:     snapshotID,
		SequenceNumber: sequenceNumber,
		TimestampMs:    time.Now().UnixMilli(),
		ManifestList:   manifestListPath,
		SchemaID:       &schemaID,
	}
	if formatVersion >= 3 {
		firstRowID, addedRows := int64(0), int64(0)
		snapshot.FirstRowID = &firstRowID
		snapshot.AddedRows = &addedRows
	}
	require.NoError(t, mb.AddSnapshot(snapshot))
	require.NoError(t, mb.SetSnapshotRef(MainBranch, snapshotID, BranchRef))
	metadata, err := mb.Build()
	require.NoError(t, err)

	return New(
		Identifier{"db", "position_deletes"}, metadata, "metadata.json",
		func(context.Context) (iceio.IO, error) { return fs, nil }, nil,
	)
}

func TestInspectPositionDeletesParquet(t *testing.T) {
	ctx := context.Background()
	memFS := iceio.NewMemFS()
	deletePath := "mem://position-deletes/table/data/delete.parquet"
	dataPath := "mem://position-deletes/table/data/data.parquet"
	writePosDeleteParquetToMemFS(t, memFS, deletePath, `[
		{"file_path": "`+dataPath+`", "pos": 1},
		{"file_path": "`+dataPath+`", "pos": 3}
	]`)
	deleteFile := newPosDeleteFile(t, deletePath, 2, 128)
	tbl := inspectPositionDeletesTable(
		t, 2, newInspectPositionDeletesMetadata(t, 2), memFS,
		[]iceberg.DataFile{deleteFile},
	)

	rr, err := tbl.Inspect().PositionDeletes(ctx)
	require.NoError(t, err)
	defer rr.Release()
	rec := collectRecord(t, rr)
	defer rec.Release()

	require.EqualValues(t, 2, rec.NumRows())
	require.EqualValues(t, 5, rec.NumCols())
	filePaths := rec.Column(0).(*array.String)
	require.Equal(t, dataPath, filePaths.Value(0))
	require.Equal(t, dataPath, filePaths.Value(1))
	require.Equal(t, []int64{1, 3}, rec.Column(1).(*array.Int64).Int64Values())
	require.EqualValues(t, 2, rec.Column(2).NullN())
	require.Equal(t, []int32{0, 0}, rec.Column(3).(*array.Int32).Int32Values())
	deleteFilePaths := rec.Column(4).(*array.String)
	require.Equal(t, deletePath, deleteFilePaths.Value(0))
	require.Equal(t, deletePath, deleteFilePaths.Value(1))
}

func TestInspectPositionDeletesV3ParquetLeavesDVMetadataNull(t *testing.T) {
	ctx := context.Background()
	memFS := iceio.NewMemFS()
	deletePath := "mem://position-deletes/table/data/delete-v3.parquet"
	dataPath := "mem://position-deletes/table/data/data.parquet"
	writePosDeleteParquetToMemFS(t, memFS, deletePath, `[
		{"file_path": "`+dataPath+`", "pos": 1},
		{"file_path": "`+dataPath+`", "pos": 3}
	]`)
	const fileSize int64 = 128
	deleteFileBuilder, err := iceberg.NewDataFileBuilder(
		*iceberg.UnpartitionedSpec, iceberg.EntryContentPosDeletes,
		deletePath, iceberg.ParquetFile, nil, nil, nil, 2, fileSize)
	require.NoError(t, err)
	deleteFile := deleteFileBuilder.ContentSizeInBytes(fileSize / 2).Build()
	tbl := inspectPositionDeletesTable(
		t, 3, newInspectPositionDeletesMetadata(t, 3), memFS,
		[]iceberg.DataFile{deleteFile},
	)

	rr, err := tbl.Inspect().PositionDeletes(ctx)
	require.NoError(t, err)
	defer rr.Release()
	record := collectRecord(t, rr)
	defer record.Release()

	require.EqualValues(t, 2, record.NumRows())
	require.EqualValues(t, 7, record.NumCols())
	require.EqualValues(t, 2, record.Column(5).NullN())
	require.EqualValues(t, 2, record.Column(6).NullN())
}

func TestAppendParquetPositionDeleteRowsRejectsNegativePosition(t *testing.T) {
	memFS := iceio.NewMemFS()
	deletePath := "mem://position-deletes/table/data/delete-negative.parquet"
	dataPath := "mem://position-deletes/table/data/data.parquet"
	writePosDeleteParquetToMemFS(t, memFS, deletePath, `[
		{"file_path": "`+dataPath+`", "pos": -1}
	]`)

	rows := 0
	keepGoing, err := appendParquetPositionDeleteRows(
		context.Background(), memFS, newPosDeleteFile(t, deletePath, 1, 128),
		positionDeleteFileMeta{},
		func(positionDeleteFileMeta, string, int64, scalar.Scalar, bool) (bool, error) {
			rows++

			return true, nil
		},
	)
	require.False(t, keepGoing)
	require.ErrorContains(t, err, "negative pos -1")
	require.ErrorIs(t, err, iceberg.ErrInvalidSchema)
	require.Zero(t, rows)
}

func TestInspectPositionDeletesDefersLaterManifestReads(t *testing.T) {
	memFS := iceio.NewMemFS()
	tableSchema := simpleSchema()
	deletePath := "mem://position-deletes/table/data/delete-first.parquet"
	dataPath := "mem://position-deletes/table/data/data.parquet"
	var deleteRows bytes.Buffer
	deleteRows.WriteByte('[')
	for pos := range inspectRecordBatchSize {
		if pos > 0 {
			deleteRows.WriteByte(',')
		}
		fmt.Fprintf(&deleteRows, `{"file_path": %q, "pos": %d}`, dataPath, pos)
	}
	deleteRows.WriteByte(']')
	writePosDeleteParquetToMemFS(t, memFS, deletePath, deleteRows.String())

	snapshotID := int64(1)
	sequenceNumber := int64(1)
	firstManifest := writeInspectManifest(
		t, memFS, "mem://position-deletes/table/metadata/first-deletes.avro",
		*iceberg.UnpartitionedSpec, tableSchema, snapshotID, iceberg.ManifestContentDeletes,
		[]iceberg.ManifestEntry{iceberg.NewManifestEntry(
			iceberg.EntryStatusADDED, &snapshotID, &sequenceNumber, &sequenceNumber,
			newPosDeleteFile(t, deletePath, inspectRecordBatchSize, 128),
		)},
	)
	missingManifest := iceberg.NewManifestFile(
		2, "mem://position-deletes/table/metadata/not-opened.avro", 1, 0, snapshotID,
	).Content(iceberg.ManifestContentDeletes).AddedFiles(1).ExistingFiles(0).DeletedFiles(0).Build()
	tbl := inspectPositionDeletesTableWithManifests(
		t, 2, newInspectPositionDeletesMetadataWithSchema(t, 2, tableSchema), tableSchema,
		memFS, []iceberg.ManifestFile{firstManifest, missingManifest},
	)

	rr, err := tbl.Inspect().PositionDeletes(context.Background())
	require.NoError(t, err)
	defer rr.Release()
	require.True(t, rr.Next(), "reader error: %v", rr.Err())
	require.EqualValues(t, inspectRecordBatchSize, rr.RecordBatch().NumRows())
}

func TestAppendParquetPositionDeleteRowsRejectsNullRow(t *testing.T) {
	memFS := iceio.NewMemFS()
	deletePath := "mem://position-deletes/table/data/delete-null-row.parquet"
	dataPath := "mem://position-deletes/table/data/data.parquet"
	schema := arrow.NewSchema([]arrow.Field{
		{Name: "file_path", Type: arrow.BinaryTypes.String, Nullable: false},
		{Name: "pos", Type: arrow.PrimitiveTypes.Int64, Nullable: false},
		{Name: "row", Type: arrow.StructOf(arrow.Field{
			Name:     "id",
			Type:     arrow.PrimitiveTypes.Int32,
			Nullable: false,
		}), Nullable: true},
	}, nil)
	bldr := array.NewRecordBuilder(memory.DefaultAllocator, schema)
	bldr.Field(0).(*array.StringBuilder).Append(dataPath)
	bldr.Field(1).(*array.Int64Builder).Append(1)
	bldr.Field(2).(*array.StructBuilder).Append(false)
	record := bldr.NewRecordBatch()
	defer record.Release()
	defer bldr.Release()

	tbl := array.NewTableFromRecords(schema, []arrow.RecordBatch{record})
	defer tbl.Release()
	file, err := memFS.Create(deletePath)
	require.NoError(t, err)
	require.NoError(t, pqarrow.WriteTable(
		tbl, file, record.NumRows(),
		parquet.NewWriterProperties(parquet.WithStats(true)), pqarrow.DefaultWriterProps()))
	require.NoError(t, file.Close())

	rows := 0
	keepGoing, err := appendParquetPositionDeleteRows(
		context.Background(), memFS, newPosDeleteFile(t, deletePath, 1, 128),
		positionDeleteFileMeta{},
		func(positionDeleteFileMeta, string, int64, scalar.Scalar, bool) (bool, error) {
			rows++

			return true, nil
		},
	)
	require.False(t, keepGoing)
	require.ErrorIs(t, err, iceberg.ErrInvalidSchema)
	require.ErrorContains(t, err, "null row")
	require.Zero(t, rows)
}

func TestInspectPositionDeletesParquetProjectsEvolvedNestedRow(t *testing.T) {
	checked := memory.NewCheckedAllocator(memory.DefaultAllocator)
	t.Cleanup(func() { checked.AssertSize(t, 0) })

	const (
		deletePath = "mem://position-deletes/table/data/delete-evolved.parquet"
		dataPath   = "mem://position-deletes/table/data/data.parquet"
	)
	memFS := iceio.NewMemFS()
	fieldID := func(id int) arrow.Metadata {
		return arrow.MetadataFrom(map[string]string{ArrowParquetFieldIDKey: strconv.Itoa(id)})
	}

	tableListType := &iceberg.ListType{
		ElementID:       3,
		Element:         iceberg.PrimitiveTypes.Int64,
		ElementRequired: true,
	}
	tableMapType := &iceberg.MapType{
		KeyID:         4,
		KeyType:       iceberg.PrimitiveTypes.String,
		ValueID:       5,
		ValueType:     iceberg.PrimitiveTypes.Int64,
		ValueRequired: false,
	}
	tableSchema := iceberg.NewSchema(0,
		iceberg.NestedField{ID: 1, Name: "values", Type: tableListType, Required: true},
		iceberg.NestedField{ID: 2, Name: "attributes", Type: tableMapType, Required: true},
		iceberg.NestedField{ID: 6, Name: "amount", Type: iceberg.DecimalTypeOf(18, 2), Required: true},
	)

	sourceListType := arrow.ListOfField(arrow.Field{
		Name:     "element",
		Type:     arrow.PrimitiveTypes.Int32,
		Nullable: false,
		Metadata: fieldID(3),
	})
	sourceMapType := arrow.MapOfFields(
		arrow.Field{Name: "key", Type: arrow.BinaryTypes.String, Nullable: false, Metadata: fieldID(4)},
		arrow.Field{Name: "value", Type: arrow.PrimitiveTypes.Int32, Nullable: true, Metadata: fieldID(5)},
	)
	sourceDecimalType := &arrow.Decimal128Type{Precision: 9, Scale: 2}
	sourceRowType := arrow.StructOf(
		arrow.Field{Name: "values", Type: sourceListType, Nullable: true, Metadata: fieldID(1)},
		arrow.Field{Name: "attributes", Type: sourceMapType, Nullable: true, Metadata: fieldID(2)},
		arrow.Field{Name: "amount", Type: sourceDecimalType, Nullable: true, Metadata: fieldID(6)},
	)
	sourceSchema := arrow.NewSchema([]arrow.Field{
		{Name: "file_path", Type: arrow.BinaryTypes.String, Nullable: false},
		{Name: "pos", Type: arrow.PrimitiveTypes.Int64, Nullable: false},
		{Name: "row", Type: sourceRowType, Nullable: true},
	}, nil)

	recordBuilder := array.NewRecordBuilder(memory.DefaultAllocator, sourceSchema)
	recordBuilder.Field(0).(*array.StringBuilder).Append(dataPath)
	recordBuilder.Field(1).(*array.Int64Builder).Append(7)
	rowBuilder := recordBuilder.Field(2).(*array.StructBuilder)
	rowBuilder.Append(true)
	valuesBuilder := rowBuilder.FieldBuilder(0).(*array.ListBuilder)
	valuesBuilder.Append(true)
	valuesBuilder.ValueBuilder().(*array.Int32Builder).AppendValues([]int32{7, 9}, nil)
	attributesBuilder := rowBuilder.FieldBuilder(1).(*array.MapBuilder)
	attributesBuilder.Append(true)
	attributesBuilder.KeyBuilder().(*array.StringBuilder).Append("first")
	attributesBuilder.ItemBuilder().(*array.Int32Builder).Append(11)
	attributesBuilder.KeyBuilder().(*array.StringBuilder).Append("second")
	attributesBuilder.ItemBuilder().(*array.Int32Builder).Append(13)
	rowBuilder.FieldBuilder(2).(*array.Decimal128Builder).Append(decimal128.FromI64(12345))
	record := recordBuilder.NewRecordBatch()
	defer record.Release()
	defer recordBuilder.Release()

	arrowTable := array.NewTableFromRecords(sourceSchema, []arrow.RecordBatch{record})
	defer arrowTable.Release()
	file, err := memFS.Create(deletePath)
	require.NoError(t, err)
	require.NoError(t, pqarrow.WriteTable(
		arrowTable, file, record.NumRows(),
		parquet.NewWriterProperties(parquet.WithStats(true)), pqarrow.DefaultWriterProps()))
	require.NoError(t, file.Close())

	deleteFile := newPosDeleteFile(t, deletePath, 1, 128)
	tbl := inspectPositionDeletesTableWithSchema(
		t, 2, newInspectPositionDeletesMetadataWithSchema(t, 2, tableSchema),
		tableSchema, memFS, []iceberg.DataFile{deleteFile})
	rr, err := tbl.Inspect(WithInspectAllocator(checked)).PositionDeletes(context.Background())
	require.NoError(t, err)
	defer rr.Release()
	record = collectRecord(t, rr)
	defer record.Release()

	require.EqualValues(t, 1, record.NumRows())
	projectedRow := record.Column(2).(*array.Struct)
	require.False(t, projectedRow.IsNull(0))
	require.Equal(t, []int64{7, 9}, projectedRow.Field(0).(*array.List).ListValues().(*array.Int64).Int64Values())
	require.Equal(t, []int64{11, 13}, projectedRow.Field(1).(*array.Map).Items().(*array.Int64).Int64Values())
	amount := projectedRow.Field(2).(*array.Decimal128)
	require.Equal(t, decimal128.FromI64(12345), amount.Value(0))
	require.Equal(t, int32(18), amount.DataType().(*arrow.Decimal128Type).Precision)
}

func TestInspectPositionDeletesParquetUsesNameMappingAndInitialDefault(t *testing.T) {
	const (
		deletePath = "mem://position-deletes/table/data/delete-renamed.parquet"
		dataPath   = "mem://position-deletes/table/data/data.parquet"
	)
	memFS := iceio.NewMemFS()
	intPtr := func(value int) *int { return &value }

	tablePayloadType := &iceberg.StructType{FieldList: []iceberg.NestedField{
		{ID: 2, Name: "new_name", Type: iceberg.PrimitiveTypes.String, Required: true},
	}}
	tableSchema := iceberg.NewSchema(0,
		iceberg.NestedField{ID: 1, Name: "payload", Type: tablePayloadType, Required: true},
		iceberg.NestedField{ID: 4, Name: "status", Type: iceberg.PrimitiveTypes.String, Required: false, InitialDefault: "active"},
	)
	mapping := iceberg.NameMapping{
		{FieldID: intPtr(1), Names: []string{"payload", "old_payload"}, Fields: []iceberg.MappedField{
			{FieldID: intPtr(2), Names: []string{"new_name", "old_name"}},
		}},
		{FieldID: intPtr(4), Names: []string{"status"}},
	}
	metadata := newInspectPositionDeletesMetadataWithSchema(t, 3, tableSchema)
	mappingJSON, err := json.Marshal(mapping)
	require.NoError(t, err)
	require.NoError(t, metadata.SetProperties(iceberg.Properties{
		DefaultNameMappingKey: string(mappingJSON),
	}))

	sourcePayloadType := arrow.StructOf(arrow.Field{
		Name:     "old_name",
		Type:     arrow.BinaryTypes.String,
		Nullable: false,
	})
	sourceRowType := arrow.StructOf(arrow.Field{
		Name:     "old_payload",
		Type:     sourcePayloadType,
		Nullable: false,
	})
	sourceSchema := arrow.NewSchema([]arrow.Field{
		{Name: "file_path", Type: arrow.BinaryTypes.String, Nullable: false},
		{Name: "pos", Type: arrow.PrimitiveTypes.Int64, Nullable: false},
		{Name: "row", Type: sourceRowType, Nullable: false},
	}, nil)
	recordBuilder := array.NewRecordBuilder(memory.DefaultAllocator, sourceSchema)
	recordBuilder.Field(0).(*array.StringBuilder).Append(dataPath)
	recordBuilder.Field(1).(*array.Int64Builder).Append(7)
	rowBuilder := recordBuilder.Field(2).(*array.StructBuilder)
	rowBuilder.Append(true)
	payloadBuilder := rowBuilder.FieldBuilder(0).(*array.StructBuilder)
	payloadBuilder.Append(true)
	payloadBuilder.FieldBuilder(0).(*array.StringBuilder).Append("deleted")
	record := recordBuilder.NewRecordBatch()
	defer record.Release()
	defer recordBuilder.Release()

	arrowTable := array.NewTableFromRecords(sourceSchema, []arrow.RecordBatch{record})
	defer arrowTable.Release()
	file, err := memFS.Create(deletePath)
	require.NoError(t, err)
	require.NoError(t, pqarrow.WriteTable(
		arrowTable, file, record.NumRows(),
		parquet.NewWriterProperties(parquet.WithStats(true)), pqarrow.DefaultWriterProps()))
	require.NoError(t, file.Close())

	tbl := inspectPositionDeletesTableWithSchema(
		t, 3, metadata, tableSchema, memFS,
		[]iceberg.DataFile{newPosDeleteFile(t, deletePath, 1, 128)},
	)
	rr, err := tbl.Inspect().PositionDeletes(context.Background())
	require.NoError(t, err)
	defer rr.Release()
	result := collectRecord(t, rr)
	defer result.Release()

	projectedRow := result.Column(2).(*array.Struct)
	payload := projectedRow.Field(0).(*array.Struct)
	require.Equal(t, "deleted", payload.Field(0).(*array.String).Value(0))
	require.Equal(t, "active", projectedRow.Field(1).(*array.String).Value(0))
}

func TestInspectPositionDeletesRejectsUnsupportedFileFormat(t *testing.T) {
	memFS := iceio.NewMemFS()
	deletePath := "mem://position-deletes/table/data/delete.avro"
	builder, err := iceberg.NewDataFileBuilder(
		*iceberg.UnpartitionedSpec, iceberg.EntryContentPosDeletes,
		deletePath, iceberg.AvroFile, nil, nil, nil, 1, 128)
	require.NoError(t, err)

	tbl := inspectPositionDeletesTable(
		t, 2, newInspectPositionDeletesMetadata(t, 2), memFS,
		[]iceberg.DataFile{builder.Build()},
	)
	rr, err := tbl.Inspect().PositionDeletes(context.Background())
	require.NoError(t, err)
	defer rr.Release()

	require.False(t, rr.Next())
	require.ErrorIs(t, rr.Err(), iceberg.ErrNotImplemented)
	require.ErrorContains(t, rr.Err(), "unsupported position delete file format AVRO")
}

func TestInspectPositionDeletesParquetProjectsDateToTimestampsV3(t *testing.T) {
	const (
		deletePath = "mem://position-deletes/table/data/delete-date.parquet"
		dataPath   = "mem://position-deletes/table/data/data.parquet"
	)
	const dateValue = arrow.Date32(20_000)
	memFS := iceio.NewMemFS()

	fieldID := func(id int) arrow.Metadata {
		return arrow.MetadataFrom(map[string]string{ArrowParquetFieldIDKey: strconv.Itoa(id)})
	}
	tableSchema := iceberg.NewSchema(0,
		iceberg.NestedField{ID: 1, Name: "created_at", Type: iceberg.PrimitiveTypes.Timestamp, Required: true},
		iceberg.NestedField{ID: 2, Name: "created_at_ns", Type: iceberg.PrimitiveTypes.TimestampNs, Required: true},
	)
	sourceRowType := arrow.StructOf(
		arrow.Field{Name: "created_at", Type: arrow.FixedWidthTypes.Date32, Nullable: true, Metadata: fieldID(1)},
		arrow.Field{Name: "created_at_ns", Type: arrow.FixedWidthTypes.Date32, Nullable: true, Metadata: fieldID(2)},
	)
	sourceSchema := arrow.NewSchema([]arrow.Field{
		{Name: "file_path", Type: arrow.BinaryTypes.String, Nullable: false},
		{Name: "pos", Type: arrow.PrimitiveTypes.Int64, Nullable: false},
		{Name: "row", Type: sourceRowType, Nullable: true},
	}, nil)

	recordBuilder := array.NewRecordBuilder(memory.DefaultAllocator, sourceSchema)
	recordBuilder.Field(0).(*array.StringBuilder).Append(dataPath)
	recordBuilder.Field(1).(*array.Int64Builder).Append(7)
	rowBuilder := recordBuilder.Field(2).(*array.StructBuilder)
	rowBuilder.Append(true)
	rowBuilder.FieldBuilder(0).(*array.Date32Builder).Append(dateValue)
	rowBuilder.FieldBuilder(1).(*array.Date32Builder).Append(dateValue)
	record := recordBuilder.NewRecordBatch()
	defer record.Release()
	defer recordBuilder.Release()

	arrowTable := array.NewTableFromRecords(sourceSchema, []arrow.RecordBatch{record})
	defer arrowTable.Release()
	file, err := memFS.Create(deletePath)
	require.NoError(t, err)
	require.NoError(t, pqarrow.WriteTable(
		arrowTable, file, record.NumRows(),
		parquet.NewWriterProperties(parquet.WithStats(true)), pqarrow.DefaultWriterProps()))
	require.NoError(t, file.Close())

	deleteFile := newPosDeleteFile(t, deletePath, 1, 128)
	tbl := inspectPositionDeletesTableWithSchema(
		t, 3, newInspectPositionDeletesMetadataWithSchema(t, 3, tableSchema),
		tableSchema, memFS, []iceberg.DataFile{deleteFile})
	rr, err := tbl.Inspect().PositionDeletes(context.Background())
	require.NoError(t, err)
	defer rr.Release()
	record = collectRecord(t, rr)
	defer record.Release()

	projectedRow := record.Column(2).(*array.Struct)
	require.False(t, projectedRow.IsNull(0))
	micros := projectedRow.Field(0).(*array.Timestamp)
	microsType := micros.DataType().(*arrow.TimestampType)
	require.Equal(t, arrow.Microsecond, microsType.Unit)
	nanos := projectedRow.Field(1).(*array.Timestamp)
	nanosType := nanos.DataType().(*arrow.TimestampType)
	require.Equal(t, arrow.Nanosecond, nanosType.Unit)

	unitsPerDayMicros := int64(24 * time.Hour / time.Microsecond)
	unitsPerDayNanos := int64(24 * time.Hour / time.Nanosecond)
	require.Equal(t, arrow.Timestamp(int64(dateValue)*unitsPerDayMicros), micros.Value(0))
	require.Equal(t, arrow.Timestamp(int64(dateValue)*unitsPerDayNanos), nanos.Value(0))
}

func TestAppendParquetPositionDeleteRowsStopsOnContextCancellation(t *testing.T) {
	memFS := iceio.NewMemFS()
	deletePath := "mem://position-deletes/table/data/delete.parquet"
	dataPath := "mem://position-deletes/table/data/data.parquet"
	writePosDeleteParquetToMemFS(t, memFS, deletePath, `[
		{"file_path": "`+dataPath+`", "pos": 1},
		{"file_path": "`+dataPath+`", "pos": 2}
	]`)

	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	rows := 0
	keepGoing, err := appendParquetPositionDeleteRows(
		ctx, memFS, newPosDeleteFile(t, deletePath, 2, 128),
		positionDeleteFileMeta{},
		func(positionDeleteFileMeta, string, int64, scalar.Scalar, bool) (bool, error) {
			rows++
			cancel()

			return true, nil
		},
	)
	require.False(t, keepGoing)
	require.ErrorIs(t, err, context.Canceled)
	require.Equal(t, 1, rows)
}

func TestPositionDeleteRowProjection(t *testing.T) {
	tableSchema := iceberg.NewSchema(0,
		iceberg.NestedField{ID: 1, Name: "id", Type: iceberg.PrimitiveTypes.Int32, Required: true},
		iceberg.NestedField{ID: 2, Name: "data", Type: iceberg.PrimitiveTypes.String, Required: true},
	)
	outputSchema, err := SchemaToArrowSchema(
		PositionDeletesSchema(tableSchema, &iceberg.StructType{}, 2), nil, true, false)
	require.NoError(t, err)
	bldr := array.NewRecordBuilder(memory.DefaultAllocator, outputSchema)
	defer bldr.Release()
	appender, err := newPositionDeleteRecordAppender(bldr, &iceberg.StructType{}, nil, 2)
	require.NoError(t, err)

	sourceType := arrow.StructOf(arrow.Field{
		Name:     "data",
		Type:     arrow.BinaryTypes.String,
		Nullable: true,
		Metadata: arrow.MetadataFrom(map[string]string{ArrowParquetFieldIDKey: "2"}),
	})
	row := scalar.NewStructScalar([]scalar.Scalar{scalar.NewStringScalar("deleted")}, sourceType)
	deleteFile := newPosDeleteFile(t, "mem://position-deletes/table/data/delete.parquet", 1, 128)
	require.NoError(t, appender.append(deleteFile, "mem://position-deletes/table/data/data.parquet", 7, row))

	record := bldr.NewRecordBatch()
	defer record.Release()
	projected := record.Column(2).(*array.Struct)
	require.False(t, projected.IsNull(0))
	require.True(t, projected.Field(0).IsNull(0), "missing row fields should be null")
	require.Equal(t, "deleted", projected.Field(1).(*array.String).Value(0))
}

func TestPositionDeleteRowProjectionAllowsMissingRequiredNestedField(t *testing.T) {
	tableDetailsType := &iceberg.StructType{FieldList: []iceberg.NestedField{
		{ID: 2, Name: "present", Type: iceberg.PrimitiveTypes.String, Required: true},
		{ID: 3, Name: "missing", Type: iceberg.PrimitiveTypes.Int32, Required: true},
	}}
	tableSchema := iceberg.NewSchema(0,
		iceberg.NestedField{ID: 1, Name: "details", Type: tableDetailsType, Required: true},
	)
	outputSchema, err := SchemaToArrowSchema(
		PositionDeletesSchema(tableSchema, &iceberg.StructType{}, 2), nil, true, false)
	require.NoError(t, err)
	bldr := array.NewRecordBuilder(memory.DefaultAllocator, outputSchema)
	defer bldr.Release()
	appender, err := newPositionDeleteRecordAppender(bldr, &iceberg.StructType{}, nil, 2)
	require.NoError(t, err)

	fieldID := func(id int) arrow.Metadata {
		return arrow.MetadataFrom(map[string]string{ArrowParquetFieldIDKey: strconv.Itoa(id)})
	}
	sourceDetailsType := arrow.StructOf(arrow.Field{
		Name:     "present",
		Type:     arrow.BinaryTypes.String,
		Nullable: true,
		Metadata: fieldID(2),
	})
	sourceType := arrow.StructOf(arrow.Field{
		Name:     "details",
		Type:     sourceDetailsType,
		Nullable: true,
		Metadata: fieldID(1),
	})
	row := scalar.NewStructScalar([]scalar.Scalar{
		scalar.NewStructScalar([]scalar.Scalar{scalar.NewStringScalar("kept")}, sourceDetailsType),
	}, sourceType)
	defer row.Release()

	deleteFile := newPosDeleteFile(t, "mem://position-deletes/table/data/delete.parquet", 1, 128)
	require.NoError(t, appender.append(deleteFile, "mem://position-deletes/table/data/data.parquet", 7, row))

	record := bldr.NewRecordBatch()
	defer record.Release()
	details := record.Column(2).(*array.Struct).Field(0).(*array.Struct)
	require.Equal(t, "kept", details.Field(0).(*array.String).Value(0))
	require.True(t, details.Field(1).IsNull(0), "missing nested row fields should be null")
}

func TestPositionDeleteRowProjectionPromotesTypes(t *testing.T) {
	tableSchema := iceberg.NewSchema(0,
		iceberg.NestedField{ID: 1, Name: "id", Type: iceberg.PrimitiveTypes.Int64, Required: true},
		iceberg.NestedField{ID: 2, Name: "ratio", Type: iceberg.PrimitiveTypes.Float64, Required: true},
	)
	outputSchema, err := SchemaToArrowSchema(
		PositionDeletesSchema(tableSchema, &iceberg.StructType{}, 2), nil, true, false)
	require.NoError(t, err)
	bldr := array.NewRecordBuilder(memory.DefaultAllocator, outputSchema)
	defer bldr.Release()
	appender, err := newPositionDeleteRecordAppender(bldr, &iceberg.StructType{}, nil, 2)
	require.NoError(t, err)

	sourceType := arrow.StructOf(
		arrow.Field{
			Name:     "id",
			Type:     arrow.PrimitiveTypes.Int32,
			Nullable: true,
			Metadata: arrow.MetadataFrom(map[string]string{ArrowParquetFieldIDKey: "1"}),
		},
		arrow.Field{
			Name:     "ratio",
			Type:     arrow.PrimitiveTypes.Float32,
			Nullable: true,
			Metadata: arrow.MetadataFrom(map[string]string{ArrowParquetFieldIDKey: "2"}),
		},
	)
	row := scalar.NewStructScalar([]scalar.Scalar{
		scalar.NewInt32Scalar(7),
		scalar.NewFloat32Scalar(1.25),
	}, sourceType)
	deleteFile := newPosDeleteFile(t, "mem://position-deletes/table/data/delete.parquet", 1, 128)
	require.NoError(t, appender.append(deleteFile, "mem://position-deletes/table/data/data.parquet", 7, row))

	record := bldr.NewRecordBatch()
	defer record.Release()
	projected := record.Column(2).(*array.Struct)
	require.EqualValues(t, 7, projected.Field(0).(*array.Int64).Value(0))
	require.Equal(t, 1.25, projected.Field(1).(*array.Float64).Value(0))
}

func TestPositionDeleteRowProjectionPromotesLargeBinaryToBinary(t *testing.T) {
	tableSchema := iceberg.NewSchema(0,
		iceberg.NestedField{ID: 1, Name: "payload", Type: iceberg.PrimitiveTypes.Binary, Required: true},
	)
	outputSchema, err := SchemaToArrowSchema(
		PositionDeletesSchema(tableSchema, &iceberg.StructType{}, 2), nil, true, false)
	require.NoError(t, err)
	bldr := array.NewRecordBuilder(memory.DefaultAllocator, outputSchema)
	defer bldr.Release()
	appender, err := newPositionDeleteRecordAppender(bldr, &iceberg.StructType{}, nil, 2)
	require.NoError(t, err)

	fieldID := arrow.MetadataFrom(map[string]string{ArrowParquetFieldIDKey: "1"})
	sourceType := arrow.StructOf(arrow.Field{
		Name:     "payload",
		Type:     arrow.BinaryTypes.LargeBinary,
		Nullable: true,
		Metadata: fieldID,
	})
	buf := memory.NewBufferBytes([]byte("deleted"))
	value := scalar.NewLargeBinaryScalar(buf)
	buf.Release()
	row := scalar.NewStructScalar([]scalar.Scalar{value}, sourceType)
	defer row.Release()

	deleteFile := newPosDeleteFile(t, "mem://position-deletes/table/data/delete.parquet", 1, 128)
	require.NoError(t, appender.append(
		deleteFile, "mem://position-deletes/table/data/data.parquet", 7, row))

	record := bldr.NewRecordBatch()
	defer record.Release()
	payload := record.Column(2).(*array.Struct).Field(0).(*array.Binary)
	require.Equal(t, []byte("deleted"), payload.Value(0))
}

func TestPositionDeleteRowProjectionPromotesDateInV3(t *testing.T) {
	tableSchema := iceberg.NewSchema(0,
		iceberg.NestedField{ID: 1, Name: "created_at", Type: iceberg.PrimitiveTypes.Timestamp, Required: true},
		iceberg.NestedField{ID: 2, Name: "created_at_ns", Type: iceberg.PrimitiveTypes.TimestampNs, Required: true},
	)
	outputSchema, err := SchemaToArrowSchema(
		PositionDeletesSchema(tableSchema, &iceberg.StructType{}, 3), nil, true, false)
	require.NoError(t, err)
	bldr := array.NewRecordBuilder(memory.DefaultAllocator, outputSchema)
	defer bldr.Release()
	appender, err := newPositionDeleteRecordAppender(bldr, &iceberg.StructType{}, nil, 3)
	require.NoError(t, err)

	fieldID := func(id int) arrow.Metadata {
		return arrow.MetadataFrom(map[string]string{ArrowParquetFieldIDKey: strconv.Itoa(id)})
	}
	dateValue := arrow.Date32(20_000)
	sourceType := arrow.StructOf(
		arrow.Field{Name: "created_at", Type: arrow.FixedWidthTypes.Date32, Nullable: true, Metadata: fieldID(1)},
		arrow.Field{Name: "created_at_ns", Type: arrow.FixedWidthTypes.Date32, Nullable: true, Metadata: fieldID(2)},
	)
	row := scalar.NewStructScalar([]scalar.Scalar{
		scalar.NewDate32Scalar(dateValue),
		scalar.NewDate32Scalar(dateValue),
	}, sourceType)
	defer row.Release()

	deleteFile := newPosDeleteFile(t, "mem://position-deletes/table/data/delete.parquet", 1, 128)
	require.NoError(t, appender.append(
		deleteFile, "mem://position-deletes/table/data/data.parquet", 7, row))

	record := bldr.NewRecordBatch()
	defer record.Release()
	projected := record.Column(2).(*array.Struct)
	dateMicros := projected.Field(0).(*array.Timestamp)
	dateNanos := projected.Field(1).(*array.Timestamp)
	require.Equal(t, arrow.Microsecond, dateMicros.DataType().(*arrow.TimestampType).Unit)
	require.Equal(t, arrow.Nanosecond, dateNanos.DataType().(*arrow.TimestampType).Unit)
	require.Equal(t,
		arrow.Timestamp(int64(dateValue)*int64(24*time.Hour/time.Microsecond)), dateMicros.Value(0))
	require.Equal(t,
		arrow.Timestamp(int64(dateValue)*int64(24*time.Hour/time.Nanosecond)), dateNanos.Value(0))
}

func TestPositionDeleteDatePromotion(t *testing.T) {
	require.True(t, canPromoteDateToTimestamp(
		iceberg.PrimitiveTypes.Date, iceberg.PrimitiveTypes.Timestamp))
	require.True(t, canPromoteDateToTimestamp(
		iceberg.PrimitiveTypes.Date, iceberg.PrimitiveTypes.TimestampNs))
	require.False(t, canPromoteDateToTimestamp(
		iceberg.PrimitiveTypes.Date, iceberg.PrimitiveTypes.TimestampTz),
		"date promotion must not target a timezone-aware timestamp")
}

func TestPositionDeleteRowProjectionRejectsDatePromotionBeforeV3(t *testing.T) {
	tableSchema := iceberg.NewSchema(0,
		iceberg.NestedField{ID: 1, Name: "created_at", Type: iceberg.PrimitiveTypes.Timestamp, Required: true},
	)
	outputSchema, err := SchemaToArrowSchema(
		PositionDeletesSchema(tableSchema, &iceberg.StructType{}, 2), nil, true, false)
	require.NoError(t, err)
	bldr := array.NewRecordBuilder(memory.DefaultAllocator, outputSchema)
	defer bldr.Release()
	appender, err := newPositionDeleteRecordAppender(bldr, &iceberg.StructType{}, nil, 2)
	require.NoError(t, err)

	row := scalar.NewStructScalar([]scalar.Scalar{scalar.NewDate32Scalar(arrow.Date32(20_000))},
		arrow.StructOf(arrow.Field{
			Name:     "created_at",
			Type:     arrow.FixedWidthTypes.Date32,
			Nullable: true,
			Metadata: arrow.MetadataFrom(map[string]string{ArrowParquetFieldIDKey: "1"}),
		}))
	defer row.Release()

	deleteFile := newPosDeleteFile(t, "mem://position-deletes/table/data/delete.parquet", 1, 128)
	err = appender.append(deleteFile, "mem://position-deletes/table/data/data.parquet", 7, row)
	require.Error(t, err)
}

func TestPositionDeleteRowProjectionPromotesNestedValues(t *testing.T) {
	tableListType := &iceberg.ListType{
		ElementID:       3,
		Element:         iceberg.PrimitiveTypes.Int64,
		ElementRequired: true,
	}
	tableMapType := &iceberg.MapType{
		KeyID:         4,
		KeyType:       iceberg.PrimitiveTypes.String,
		ValueID:       5,
		ValueType:     iceberg.PrimitiveTypes.Int64,
		ValueRequired: false,
	}
	tableSchema := iceberg.NewSchema(0,
		iceberg.NestedField{ID: 1, Name: "values", Type: tableListType, Required: true},
		iceberg.NestedField{ID: 2, Name: "attributes", Type: tableMapType, Required: true},
		iceberg.NestedField{ID: 6, Name: "amount", Type: iceberg.DecimalTypeOf(18, 2), Required: true},
	)
	outputSchema, err := SchemaToArrowSchema(
		PositionDeletesSchema(tableSchema, &iceberg.StructType{}, 2), nil, true, false)
	require.NoError(t, err)
	bldr := array.NewRecordBuilder(memory.DefaultAllocator, outputSchema)
	defer bldr.Release()
	appender, err := newPositionDeleteRecordAppender(bldr, &iceberg.StructType{}, nil, 2)
	require.NoError(t, err)

	fieldID := func(id int) arrow.Metadata {
		return arrow.MetadataFrom(map[string]string{ArrowParquetFieldIDKey: strconv.Itoa(id)})
	}
	sourceListType := arrow.ListOfField(arrow.Field{
		Name:     "element",
		Type:     arrow.PrimitiveTypes.Int32,
		Nullable: false,
		Metadata: fieldID(3),
	})
	listBuilder := array.NewListBuilderWithField(memory.DefaultAllocator, sourceListType.ElemField())
	defer listBuilder.Release()
	listBuilder.Append(true)
	listValues := listBuilder.ValueBuilder().(*array.Int32Builder)
	listValues.Append(7)
	listValues.Append(9)
	listArray := listBuilder.NewListArray()
	defer listArray.Release()
	listValue := scalar.NewListScalar(listArray.ListValues())
	defer listValue.Release()

	sourceMapType := arrow.MapOfFields(
		arrow.Field{Name: "key", Type: arrow.BinaryTypes.String, Nullable: false, Metadata: fieldID(4)},
		arrow.Field{Name: "value", Type: arrow.PrimitiveTypes.Int32, Nullable: true, Metadata: fieldID(5)},
	)
	mapBuilder := array.NewMapBuilderWithType(memory.DefaultAllocator, sourceMapType)
	defer mapBuilder.Release()
	mapBuilder.Append(true)
	mapBuilder.KeyBuilder().(*array.StringBuilder).Append("first")
	mapBuilder.ItemBuilder().(*array.Int32Builder).Append(11)
	mapBuilder.KeyBuilder().(*array.StringBuilder).Append("second")
	mapBuilder.ItemBuilder().(*array.Int32Builder).Append(13)
	mapArray := mapBuilder.NewMapArray()
	defer mapArray.Release()
	mapValue := scalar.NewMapScalar(mapArray.ListValues())
	defer mapValue.Release()

	sourceDecimalType := &arrow.Decimal128Type{Precision: 9, Scale: 2}
	row := scalar.NewStructScalar([]scalar.Scalar{
		listValue,
		mapValue,
		scalar.NewDecimal128Scalar(decimal128.FromI64(12345), sourceDecimalType),
	}, arrow.StructOf(
		arrow.Field{Name: "values", Type: sourceListType, Nullable: true, Metadata: fieldID(1)},
		arrow.Field{Name: "attributes", Type: sourceMapType, Nullable: true, Metadata: fieldID(2)},
		arrow.Field{Name: "amount", Type: sourceDecimalType, Nullable: true, Metadata: fieldID(6)},
	))
	defer row.Release()

	deleteFile := newPosDeleteFile(t, "mem://position-deletes/table/data/delete.parquet", 1, 128)
	require.NoError(t, appender.append(
		deleteFile, "mem://position-deletes/table/data/data.parquet", 7, row))

	record := bldr.NewRecordBatch()
	defer record.Release()
	projected := record.Column(2).(*array.Struct)
	values := projected.Field(0).(*array.List)
	require.Equal(t, []int64{7, 9}, values.ListValues().(*array.Int64).Int64Values())
	attributes := projected.Field(1).(*array.Map)
	require.Equal(t, []int64{11, 13}, attributes.Items().(*array.Int64).Int64Values())
	amount := projected.Field(2).(*array.Decimal128)
	require.Equal(t, decimal128.FromI64(12345), amount.Value(0))
	require.Equal(t, int32(18), amount.DataType().(*arrow.Decimal128Type).Precision)
}

func TestPositionDeleteRowProjectionPromotesNestedStructValues(t *testing.T) {
	tableElementType := &iceberg.StructType{FieldList: []iceberg.NestedField{
		{ID: 8, Name: "count", Type: iceberg.PrimitiveTypes.Int64, Required: true},
		{ID: 9, Name: "name", Type: iceberg.PrimitiveTypes.String, Required: true},
	}}
	tableListType := &iceberg.ListType{
		ElementID:       7,
		Element:         tableElementType,
		ElementRequired: true,
	}
	tableSchema := iceberg.NewSchema(0,
		iceberg.NestedField{ID: 1, Name: "items", Type: tableListType, Required: true},
	)
	outputSchema, err := SchemaToArrowSchema(
		PositionDeletesSchema(tableSchema, &iceberg.StructType{}, 2), nil, true, false)
	require.NoError(t, err)
	bldr := array.NewRecordBuilder(memory.DefaultAllocator, outputSchema)
	defer bldr.Release()
	appender, err := newPositionDeleteRecordAppender(bldr, &iceberg.StructType{}, nil, 2)
	require.NoError(t, err)

	fieldID := func(id int) arrow.Metadata {
		return arrow.MetadataFrom(map[string]string{ArrowParquetFieldIDKey: strconv.Itoa(id)})
	}
	sourceElementType := arrow.StructOf(
		arrow.Field{Name: "name", Type: arrow.BinaryTypes.String, Nullable: true, Metadata: fieldID(9)},
		arrow.Field{Name: "count", Type: arrow.PrimitiveTypes.Int32, Nullable: true, Metadata: fieldID(8)},
	)
	sourceListType := arrow.ListOfField(arrow.Field{
		Name:     "element",
		Type:     sourceElementType,
		Nullable: false,
		Metadata: fieldID(7),
	})
	listBuilder := array.NewListBuilderWithField(memory.DefaultAllocator, sourceListType.ElemField())
	defer listBuilder.Release()
	listBuilder.Append(true)
	elementBuilder := listBuilder.ValueBuilder().(*array.StructBuilder)
	elementBuilder.Append(true)
	elementBuilder.FieldBuilder(0).(*array.StringBuilder).Append("nested")
	elementBuilder.FieldBuilder(1).(*array.Int32Builder).Append(17)
	listArray := listBuilder.NewListArray()
	defer listArray.Release()
	listValue := scalar.NewListScalar(listArray.ListValues())
	defer listValue.Release()

	row := scalar.NewStructScalar([]scalar.Scalar{listValue}, arrow.StructOf(
		arrow.Field{Name: "items", Type: sourceListType, Nullable: true, Metadata: fieldID(1)},
	))
	defer row.Release()

	deleteFile := newPosDeleteFile(t, "mem://position-deletes/table/data/delete.parquet", 1, 128)
	require.NoError(t, appender.append(
		deleteFile, "mem://position-deletes/table/data/data.parquet", 7, row))

	record := bldr.NewRecordBatch()
	defer record.Release()
	items := record.Column(2).(*array.Struct).Field(0).(*array.List).ListValues().(*array.Struct)
	require.EqualValues(t, 17, items.Field(0).(*array.Int64).Value(0))
	require.Equal(t, "nested", items.Field(1).(*array.String).Value(0))
}

func TestPositionDeleteRowProjectionRejectsNullMapKey(t *testing.T) {
	tableMapType := &iceberg.MapType{
		KeyID:         2,
		KeyType:       iceberg.PrimitiveTypes.String,
		ValueID:       3,
		ValueType:     iceberg.PrimitiveTypes.Int64,
		ValueRequired: false,
	}
	tableSchema := iceberg.NewSchema(0,
		iceberg.NestedField{ID: 1, Name: "attributes", Type: tableMapType, Required: true},
	)
	outputSchema, err := SchemaToArrowSchema(
		PositionDeletesSchema(tableSchema, &iceberg.StructType{}, 2), nil, true, false)
	require.NoError(t, err)
	bldr := array.NewRecordBuilder(memory.DefaultAllocator, outputSchema)
	defer bldr.Release()
	appender, err := newPositionDeleteRecordAppender(bldr, &iceberg.StructType{}, nil, 2)
	require.NoError(t, err)

	fieldID := func(id int) arrow.Metadata {
		return arrow.MetadataFrom(map[string]string{ArrowParquetFieldIDKey: strconv.Itoa(id)})
	}
	sourceEntryType := arrow.StructOf(
		arrow.Field{Name: "key", Type: arrow.BinaryTypes.String, Nullable: true, Metadata: fieldID(2)},
		arrow.Field{Name: "value", Type: arrow.PrimitiveTypes.Int32, Nullable: true, Metadata: fieldID(3)},
	)
	entryBuilder := array.NewStructBuilder(memory.DefaultAllocator, sourceEntryType)
	entryBuilder.Append(true)
	entryBuilder.FieldBuilder(0).(*array.StringBuilder).AppendNull()
	entryBuilder.FieldBuilder(1).(*array.Int32Builder).Append(11)
	entries := entryBuilder.NewArray()
	entryBuilder.Release()
	mapValue := scalar.NewMapScalar(entries)
	entries.Release()
	sourceMapType := arrow.MapOfFields(
		arrow.Field{Name: "key", Type: arrow.BinaryTypes.String, Nullable: false, Metadata: fieldID(2)},
		arrow.Field{Name: "value", Type: arrow.PrimitiveTypes.Int32, Nullable: true, Metadata: fieldID(3)},
	)
	row := scalar.NewStructScalar([]scalar.Scalar{mapValue}, arrow.StructOf(
		arrow.Field{Name: "attributes", Type: sourceMapType, Nullable: true, Metadata: fieldID(1)},
	))
	defer row.Release()

	deleteFile := newPosDeleteFile(t, "mem://position-deletes/table/data/delete.parquet", 1, 128)
	err = appender.append(deleteFile, "mem://position-deletes/table/data/data.parquet", 7, row)
	require.ErrorIs(t, err, iceberg.ErrInvalidSchema)
	require.ErrorContains(t, err, "null key")
}

func TestAppendPositionDeleteProjectedScalarRejectsNonStruct(t *testing.T) {
	builder := array.NewStructBuilder(memory.DefaultAllocator, arrow.StructOf(
		arrow.Field{Name: "value", Type: arrow.PrimitiveTypes.Int32, Nullable: true},
	))
	defer builder.Release()

	err := appendPositionDeleteProjectedScalar(builder, scalar.NewInt32Scalar(7))
	require.ErrorIs(t, err, iceberg.ErrInvalidSchema)
	require.ErrorContains(t, err, "want struct")
}

func TestInspectPositionDeletesStopsOnContextCancellation(t *testing.T) {
	metadata, err := newInspectPositionDeletesMetadata(t, 2).Build()
	require.NoError(t, err)
	memFS := iceio.NewMemFS()
	tbl := New(
		Identifier{"db", "position_deletes"}, metadata, "metadata.json",
		func(context.Context) (iceio.IO, error) { return memFS, nil }, nil,
	)

	ctx, cancel := context.WithCancel(context.Background())
	rr, err := tbl.Inspect().PositionDeletes(ctx)
	require.NoError(t, err)
	cancel()

	require.False(t, rr.Next())
	require.ErrorIs(t, rr.Err(), context.Canceled)
	rr.Release()
}

func TestInspectPositionDeletesEarlyRelease(t *testing.T) {
	for _, tt := range []struct {
		name      string
		populated bool
	}{
		{name: "populated", populated: true},
		{name: "empty"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			checked := memory.NewCheckedAllocator(memory.DefaultAllocator)
			t.Cleanup(func() { checked.AssertSize(t, 0) })

			memFS := iceio.NewMemFS()
			deletePath := "mem://position-deletes/table/data/delete.parquet"
			dataPath := "mem://position-deletes/table/data/data.parquet"
			var tbl *Table
			if tt.populated {
				writePosDeleteParquetToMemFS(t, memFS, deletePath,
					`[{"file_path": "`+dataPath+`", "pos": 1}]`)
				tbl = inspectPositionDeletesTable(
					t, 2, newInspectPositionDeletesMetadata(t, 2), memFS,
					[]iceberg.DataFile{newPosDeleteFile(t, deletePath, 1, 128)},
				)
			} else {
				metadata, buildErr := newInspectPositionDeletesMetadata(t, 2).Build()
				require.NoError(t, buildErr)
				tbl = New(
					Identifier{"db", "position_deletes"}, metadata, "metadata.json",
					func(context.Context) (iceio.IO, error) { return memFS, nil }, nil,
				)
			}
			inspect := tbl.Inspect(WithInspectAllocator(checked))

			var rr array.RecordReader
			var err error
			if tt.populated {
				rr, err = inspect.PositionDeletes(context.Background())
			} else {
				partitionType, partitionIDs, partitionErr := positionDeletesPartitionType(tbl.metadata)
				require.NoError(t, partitionErr)
				schema := PositionDeletesSchema(tbl.metadata.CurrentSchema(), partitionType, tbl.metadata.Version())
				arrowSchema, schemaErr := SchemaToArrowSchema(schema, nil, true, false)
				require.NoError(t, schemaErr)
				rr = inspect.positionDeleteRecordReader(
					context.Background(), arrowSchema, nil, nil, partitionType, partitionIDs, tbl.metadata.Version())
			}
			require.NoError(t, err)
			require.True(t, rr.Next())
			rr.Release()
		})
	}
}

func TestInspectPositionDeletesDeletionVector(t *testing.T) {
	ctx := context.Background()
	memFS := iceio.NewMemFS()
	dataPath := "mem://position-deletes/table/data/data.parquet"
	dvPath := "mem://position-deletes/table/metadata/deletes.puffin"
	dvWriter := dv.NewDVWriter(memFS, func(id int32) *iceberg.PartitionSpec {
		if id == 0 {
			return iceberg.UnpartitionedSpec
		}

		return nil
	})
	require.NoError(t, dvWriter.Add(dataPath, []int64{1, 3}, 0, nil))
	deleteFiles, err := dvWriter.Flush(ctx, dvPath)
	require.NoError(t, err)
	require.Len(t, deleteFiles, 1)
	tbl := inspectPositionDeletesTable(
		t, 3, newInspectPositionDeletesMetadata(t, 3), memFS, deleteFiles,
	)

	rr, err := tbl.Inspect().PositionDeletes(ctx)
	require.NoError(t, err)
	defer rr.Release()
	rec := collectRecord(t, rr)
	defer rec.Release()

	require.EqualValues(t, 2, rec.NumRows())
	require.EqualValues(t, 7, rec.NumCols())
	filePaths := rec.Column(0).(*array.String)
	require.Equal(t, dataPath, filePaths.Value(0))
	require.Equal(t, dataPath, filePaths.Value(1))
	require.Equal(t, []int64{1, 3}, rec.Column(1).(*array.Int64).Int64Values())
	deleteFilePaths := rec.Column(4).(*array.String)
	require.Equal(t, dvPath, deleteFilePaths.Value(0))
	require.Equal(t, dvPath, deleteFilePaths.Value(1))
	offset := *deleteFiles[0].ContentOffset()
	size := *deleteFiles[0].ContentSizeInBytes()
	require.Equal(t, []int64{offset, offset}, rec.Column(5).(*array.Int64).Int64Values())
	require.Equal(t, []int64{size, size}, rec.Column(6).(*array.Int64).Int64Values())
}

func TestPositionDeletesSchema(t *testing.T) {
	v2 := PositionDeletesSchema(simpleSchema(), &iceberg.StructType{}, 2)
	v2Fields := v2.Fields()
	require.Equal(t,
		[]string{"file_path", "pos", "row", "spec_id", "delete_file_path"},
		testFieldNames(v2),
	)
	require.Equal(t, []int{
		positionDeleteFilePathID,
		positionDeletePosID,
		positionDeleteRowID,
		positionDeleteSpecID,
		positionDeletePhysicalPathID,
	}, []int{
		v2Fields[0].ID,
		v2Fields[1].ID,
		v2Fields[2].ID,
		v2Fields[3].ID,
		v2Fields[4].ID,
	})

	v3 := PositionDeletesSchema(simpleSchema(), &iceberg.StructType{}, 3)
	v3Fields := v3.Fields()
	require.Equal(t,
		[]string{"file_path", "pos", "row", "spec_id", "delete_file_path", "content_offset", "content_size_in_bytes"},
		testFieldNames(v3),
	)
	require.Equal(t, positionDeleteContentOffsetID, v3Fields[5].ID)
	require.Equal(t, positionDeleteContentSizeID, v3Fields[6].ID)

	spec := partitionedSpec()
	metadata, err := NewMetadata(
		simpleSchema(), &spec, UnsortedSortOrder, "mem://position-deletes/table", nil,
	)
	require.NoError(t, err)
	partitionType, partitionIDs, err := positionDeletesPartitionType(metadata)
	require.NoError(t, err)
	require.Equal(t, map[int]int{1000: 2}, partitionIDs)
	require.Equal(t, 2, partitionType.FieldList[0].ID)
	partitioned := PositionDeletesSchema(simpleSchema(), partitionType, 2)
	require.Equal(t,
		[]string{"file_path", "pos", "row", "partition", "spec_id", "delete_file_path"},
		testFieldNames(partitioned),
	)
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
