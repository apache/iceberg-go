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

package table_test

import (
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"runtime/pprof"
	"strings"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/iceberg-go"
	iceio "github.com/apache/iceberg-go/io"
	"github.com/apache/iceberg-go/table"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
)

// newV3MoRConflictTestTable builds a v3 table that deletes via merge-on-read
// (so position deletes are written as Puffin deletion vectors that record their
// referenced data file) and never retries commits, so a single conflict surfaces
// terminally.
func newV3MoRConflictTestTable(t *testing.T) *table.Table {
	t.Helper()

	location := filepath.ToSlash(t.TempDir())
	schema := iceberg.NewSchema(0,
		iceberg.NestedField{ID: 1, Name: "id", Type: iceberg.PrimitiveTypes.Int64, Required: true},
		iceberg.NestedField{ID: 2, Name: "data", Type: iceberg.PrimitiveTypes.String, Required: false},
	)
	meta, err := table.NewMetadata(schema, iceberg.UnpartitionedSpec, table.UnsortedSortOrder, location,
		iceberg.Properties{
			table.PropertyFormatVersion:        "3",
			table.WriteDeleteModeKey:           table.WriteModeMergeOnRead,
			table.CommitNumRetriesKey:          "2",
			table.CommitMinRetryWaitMsKey:      "1",
			table.CommitMaxRetryWaitMsKey:      "2",
			table.CommitTotalRetryTimeoutMsKey: "1000",
		})
	require.NoError(t, err)

	metaLoc := location + "/metadata/v1.metadata.json"
	fsF := func(context.Context) (iceio.IO, error) { return iceio.LocalFS{}, nil }
	cat := &concurrentTestCatalog{metadata: meta, location: metaLoc, fsF: fsF}

	return table.New(table.Identifier{"db", "mor_delete_conflict"}, meta, metaLoc, fsF, cat)
}

func appendTenRows(t *testing.T, tbl *table.Table) *table.Table {
	t.Helper()

	arrowSchema := arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int64, Nullable: false},
		{Name: "data", Type: arrow.BinaryTypes.String, Nullable: true},
	}, nil)
	data, err := array.TableFromJSON(memory.DefaultAllocator, arrowSchema, []string{
		`[{"id":1,"data":"a"},{"id":2,"data":"b"},{"id":3,"data":"c"},{"id":4,"data":"d"},` +
			`{"id":5,"data":"e"},{"id":6,"data":"f"},{"id":7,"data":"g"},{"id":8,"data":"h"},` +
			`{"id":9,"data":"i"},{"id":10,"data":"j"}]`,
	})
	require.NoError(t, err)
	defer data.Release()

	tbl, err = tbl.Append(context.Background(), array.NewTableReader(data, -1), nil)
	require.NoError(t, err)

	return tbl
}

// TestMergeOnReadDeleteConflict_ReferencedDataFileRemoved proves a merge-on-read
// DELETE is rejected when a concurrent commit removes the data file its freshly
// written position deletes reference. Without the existence check wired into the
// MoR delete path, the deletes would be silently orphaned against the
// rewritten data.
func TestMergeOnReadDeleteConflict_ReferencedDataFileRemoved(t *testing.T) {
	ctx := context.Background()
	tbl := appendTenRows(t, newV3MoRConflictTestTable(t))

	// Capture the single data file before any deletes so the concurrent commit
	// can rewrite it away.
	tasks, err := tbl.Scan().PlanFiles(ctx)
	require.NoError(t, err)
	require.Len(t, tasks, 1)
	dataFile := tasks[0].File

	// Stage a partial delete (id==7) but do not commit it yet: this writes a
	// deletion vector that references the single data file.
	txn := tbl.NewTransaction()
	require.NoError(t, txn.Delete(ctx, iceberg.EqualTo(iceberg.Reference("id"), int64(7)), nil))

	// A concurrent compaction rewrites that data file away (its manifest entry
	// becomes a DELETED tombstone), standing in for any commit that removes it.
	rtx := tbl.NewTransaction()
	require.NoError(t, rtx.NewRewrite(nil).DeleteFile(dataFile).Commit(ctx))
	_, err = rtx.Commit(ctx)
	require.NoError(t, err)

	// Committing the staged MoR delete must now detect the removed referenced
	// data file rather than orphan its deletion vector.
	_, err = txn.Commit(ctx)
	require.Error(t, err)
	require.ErrorIs(t, err, table.ErrDataFilesMissing)
	require.NotErrorIs(t, err, table.ErrCommitFailed)
	require.ErrorIs(t, err, table.ErrTransactionUnusable)
}

// A first deletion vector is replayable until another writer commits a
// vector for the same data file. The second writer must reject replay instead
// of leaving two live vectors, which makes all subsequent scans fail.
func TestMergeOnReadDeleteConflict_ConcurrentFirstDeletionVector(t *testing.T) {
	ctx := t.Context()
	tbl := appendTenRows(t, newV3MoRConflictTestTable(t))

	pending := tbl.NewTransaction()
	require.NoError(t, pending.Delete(ctx, iceberg.EqualTo(iceberg.Reference("id"), int64(7)), nil))

	// Both writers plan against the same file with no existing DV.
	peer, err := tbl.Delete(ctx, iceberg.EqualTo(iceberg.Reference("id"), int64(2)), nil)
	require.NoError(t, err)
	require.Equal(t, 1, liveDVCount(t, peer))

	_, err = pending.Commit(ctx)
	require.ErrorIs(t, err, table.ErrConflictingDeleteFiles)
	require.ErrorContains(t, err, "concurrent snapshot")
	require.Equal(t, 1, liveDVCount(t, peer))
	require.Equal(t, []int64{1, 3, 4, 5, 6, 7, 8, 9, 10}, idsInTable(t, peer))
}

// A replacement snapshot can add a first DV just like a merge-on-read delete.
// A staged delete must see that DV during replay and reject a second live one.
func TestMergeOnReadDeleteConflict_ConcurrentFirstDeletionVectorFromConversion(t *testing.T) {
	ctx := t.Context()
	tbl := appendTenRows(t, newV3MoRConflictTestTable(t))
	tbl = appendEqualityDelete(t, tbl, []int{1}, `[{"id":2}]`)

	pending := tbl.NewTransaction()
	require.NoError(t, pending.Delete(ctx, iceberg.EqualTo(iceberg.Reference("id"), int64(7)), nil))

	conversion := tbl.NewTransaction()
	result, err := conversion.ConvertEqualityDeletesToDeletionVectors(ctx)
	require.NoError(t, err)
	require.Equal(t, 1, result.AddedDeletionVectors)
	peer, err := conversion.Commit(ctx)
	require.NoError(t, err)
	require.Equal(t, table.OpReplace, peer.CurrentSnapshot().Summary.Operation)
	require.Equal(t, 1, liveDVCount(t, peer))
	require.Equal(t, []int64{1, 3, 4, 5, 6, 7, 8, 9, 10}, idsInTable(t, peer))

	_, err = pending.Commit(ctx)
	require.ErrorIs(t, err, table.ErrConflictingDeleteFiles)
	require.ErrorContains(t, err, "concurrent snapshot")
	require.Equal(t, 1, liveDVCount(t, peer))
	require.Equal(t, []int64{1, 3, 4, 5, 6, 7, 8, 9, 10}, idsInTable(t, peer))
}

// TestMergeOnReadDeleteConflict_NoConflictCommits is the control: a MoR delete
// with no concurrent removal commits cleanly and deletes only the targeted row.
func TestMergeOnReadDeleteConflict_NoConflictCommits(t *testing.T) {
	ctx := context.Background()
	tbl := appendTenRows(t, newV3MoRConflictTestTable(t))

	tbl, err := tbl.Delete(ctx, iceberg.EqualTo(iceberg.Reference("id"), int64(7)), nil)
	require.NoError(t, err)

	require.Equal(t, []int64{1, 2, 3, 4, 5, 6, 8, 9, 10}, idsInTable(t, tbl))
}

// TestMergeOnReadDeleteOfDataFileAddedInSameTransaction proves a merge-on-read
// DELETE may target a data file added earlier in the same transaction:
// the file is not on the branch head yet, but it lands atomically with the deletion vector.
func TestMergeOnReadDeleteOfDataFileAddedInSameTransaction(t *testing.T) {
	ctx := context.Background()
	tbl := appendTenRows(t, newV3MoRConflictTestTable(t))

	path := tbl.Location() + "/data/added-in-txn.parquet"
	writeRowDeltaDataFile(t, tbl, path, 11, 12)

	txn := tbl.NewTransaction()
	require.NoError(t, txn.AddDataFilesByPath(ctx, []string{path}, nil, false))
	require.NoError(t, txn.Delete(ctx, iceberg.EqualTo(iceberg.Reference("id"), int64(11)), nil))
	tbl, err := txn.Commit(ctx)
	require.NoError(t, err)

	require.Equal(t, []int64{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 12}, idsInTable(t, tbl))
}

type failPuffinOpenIO struct{ iceio.LocalFS }

func (f failPuffinOpenIO) Open(ctx context.Context, name string) (iceio.File, error) {
	if strings.HasSuffix(name, ".puffin") {
		return nil, errors.New("puffin read failed")
	}

	return f.LocalFS.Open(ctx, name)
}

func deleteScanGoroutines() (scan, sequencer int) {
	var b strings.Builder
	_ = pprof.Lookup("goroutine").WriteTo(&b, 2)
	for g := range strings.SplitSeq(b.String(), "\n\n") {
		if strings.Contains(g, "makePositionDeleteRecordsForFilter") {
			scan++
		}
		if strings.Contains(g, "MakeSequencedChanWithDiscard") {
			sequencer++
		}
	}

	return scan, sequencer
}

// A v3 merge-on-read delete whose deletion-vector read fails must not leave a
// scan behind. Not parallel, so other tests' scans don't count.
func TestMergeOnReadDeleteDVReadErrorStopsScan(t *testing.T) {
	ctx := t.Context()
	tbl := appendTenRows(t, newV3MoRConflictTestTable(t))
	tbl, err := tbl.Delete(ctx, iceberg.EqualTo(iceberg.Reference("id"), int64(1)), nil)
	require.NoError(t, err)

	failing := table.New(tbl.Identifier(), tbl.Metadata(), tbl.MetadataLocation(),
		func(context.Context) (iceio.IO, error) { return failPuffinOpenIO{}, nil }, nil)
	scanBefore, seqBefore := deleteScanGoroutines()
	err = failing.NewTransaction().Delete(ctx, iceberg.EqualTo(iceberg.Reference("id"), int64(3)), nil)
	require.ErrorContains(t, err, "puffin read failed")
	scanAfter, seqAfter := deleteScanGoroutines()
	require.LessOrEqual(t, scanAfter, scanBefore)
	require.LessOrEqual(t, seqAfter, seqBefore)
}

// A v2 merge-on-read delete whose existing position-delete file cannot be read
// must report the read failure instead of writing deletes without it.
func TestMergeOnReadDeletePositionDeleteReadErrorSurfaces(t *testing.T) {
	ctx := t.Context()
	tbl := appendTenRows(t, newV2MoRTestTable(t))
	tbl, err := tbl.Delete(ctx, iceberg.EqualTo(iceberg.Reference("id"), int64(1)), nil)
	require.NoError(t, err)

	tasks, err := tbl.Scan().PlanFiles(ctx)
	require.NoError(t, err)
	require.NotEmpty(t, tasks)
	require.NotEmpty(t, tasks[0].DeleteFiles)

	failing := table.New(tbl.Identifier(), tbl.Metadata(), tbl.MetadataLocation(),
		func(context.Context) (iceio.IO, error) {
			return failOpenIO{path: tasks[0].DeleteFiles[0].FilePath()}, nil
		}, nil)
	scanBefore, seqBefore := deleteScanGoroutines()
	err = failing.NewTransaction().Delete(ctx, iceberg.EqualTo(iceberg.Reference("id"), int64(3)), nil)
	require.ErrorContains(t, err, "position delete read failed")
	scanAfter, seqAfter := deleteScanGoroutines()
	require.LessOrEqual(t, scanAfter, scanBefore)
	require.LessOrEqual(t, seqAfter, seqBefore)
}

func newV2MoRTestTable(t *testing.T) *table.Table {
	t.Helper()

	location := filepath.ToSlash(t.TempDir())
	schema := iceberg.NewSchema(0,
		iceberg.NestedField{ID: 1, Name: "id", Type: iceberg.PrimitiveTypes.Int64, Required: true},
		iceberg.NestedField{ID: 2, Name: "data", Type: iceberg.PrimitiveTypes.String, Required: false},
	)
	meta, err := table.NewMetadata(schema, iceberg.UnpartitionedSpec, table.UnsortedSortOrder, location,
		iceberg.Properties{
			table.PropertyFormatVersion: "2",
			table.WriteDeleteModeKey:    table.WriteModeMergeOnRead,
		})
	require.NoError(t, err)

	metaLoc := location + "/metadata/v1.metadata.json"
	fsF := func(context.Context) (iceio.IO, error) { return iceio.LocalFS{}, nil }
	cat := &concurrentTestCatalog{metadata: meta, location: metaLoc, fsF: fsF}

	return table.New(table.Identifier{"db", "mor_delete_read"}, meta, metaLoc, fsF, cat)
}

type failOpenIO struct {
	iceio.LocalFS
	path string
}

func (f failOpenIO) Open(ctx context.Context, name string) (iceio.File, error) {
	if name == f.path {
		return nil, errors.New("position delete read failed")
	}

	return f.LocalFS.Open(ctx, name)
}

// A v3 merge-on-read delete that fails after planning, while the position-delete
// writer rejects an unknown partition transform, must not leave a scan behind.
func TestMergeOnReadDeleteWriterErrorStopsScan(t *testing.T) {
	ctx := t.Context()
	location := filepath.ToSlash(strings.ReplaceAll(t.TempDir(), "#", ""))
	schema := iceberg.NewSchema(0,
		iceberg.NestedField{ID: 1, Name: "foo", Type: iceberg.PrimitiveTypes.Bool},
		iceberg.NestedField{ID: 2, Name: "bar", Type: iceberg.PrimitiveTypes.String},
		iceberg.NestedField{ID: 4, Name: "baz", Type: iceberg.PrimitiveTypes.Int32},
		iceberg.NestedField{ID: 10, Name: "qux", Type: iceberg.PrimitiveTypes.Date})
	arrSchema := arrow.NewSchema([]arrow.Field{
		{Name: "foo", Type: arrow.FixedWidthTypes.Boolean, Nullable: true},
		{Name: "bar", Type: arrow.BinaryTypes.String, Nullable: true},
		{Name: "baz", Type: arrow.PrimitiveTypes.Int32, Nullable: true},
		{Name: "qux", Type: arrow.FixedWidthTypes.Date32, Nullable: true},
	}, nil)
	unknown, err := iceberg.ParseTransform("custom_transform[42]")
	require.NoError(t, err)
	fsF := func(context.Context) (iceio.IO, error) { return iceio.LocalFS{}, nil }

	identity := iceberg.NewPartitionSpec(iceberg.PartitionField{
		SourceIDs: []int{4}, FieldID: 1000, Transform: iceberg.IdentityTransform{}, Name: "baz",
	})
	meta, err := table.NewMetadata(schema, &identity, table.UnsortedSortOrder, location,
		iceberg.Properties{
			table.PropertyFormatVersion: "3",
			table.WriteDeleteModeKey:    table.WriteModeMergeOnRead,
		})
	require.NoError(t, err)
	metaLoc := fmt.Sprintf("%s/metadata/%05d-%s.metadata.json", location, 1, uuid.New().String())
	tbl := table.New(table.Identifier{"default", "unknown_transform"}, meta, metaLoc, fsF, &mockedCatalog{meta})

	arrTable, err := array.TableFromJSON(memory.DefaultAllocator, arrSchema, []string{
		`[{"foo": false, "bar": "delete_me", "baz": 123, "qux": "2024-01-01"},
		  {"foo": true, "bar": "keep_this", "baz": 123, "qux": "2024-01-02"}]`,
	})
	require.NoError(t, err)
	defer arrTable.Release()
	tbl, err = tbl.OverwriteTable(ctx, arrTable, 1, nil)
	require.NoError(t, err)

	evolved := iceberg.NewPartitionSpec(iceberg.PartitionField{
		SourceIDs: []int{4}, FieldID: 1001, Transform: unknown, Name: "baz_custom",
	})
	builder, err := table.MetadataBuilderFromBase(tbl.Metadata(), tbl.MetadataLocation())
	require.NoError(t, err)
	require.NoError(t, builder.AddPartitionSpec(&evolved, false))
	require.NoError(t, builder.SetDefaultSpecID(-1))
	evolvedMeta, err := builder.Build()
	require.NoError(t, err)
	tbl = table.New(tbl.Identifier(), evolvedMeta, tbl.MetadataLocation(), fsF, &mockedCatalog{evolvedMeta})

	scanBefore, seqBefore := deleteScanGoroutines()
	_, err = tbl.Delete(ctx, iceberg.EqualTo(iceberg.Reference("bar"), "delete_me"), nil)
	require.ErrorIs(t, err, iceberg.ErrInvalidTransform)
	scanAfter, seqAfter := deleteScanGoroutines()
	require.LessOrEqual(t, scanAfter, scanBefore)
	require.LessOrEqual(t, seqAfter, seqBefore)
}
