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
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/iceberg-go"
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
