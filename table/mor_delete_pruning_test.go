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
	"path/filepath"
	"slices"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/iceberg-go"
	iceio "github.com/apache/iceberg-go/io"
	"github.com/apache/iceberg-go/table"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestMergeOnReadDeleteAcrossPrunedRowGroups guards the row-group pruning hazard
// in generated position deletes: a merge-on-read DELETE whose filter prunes a
// leading row group must still delete the right physical rows. enrichRecords-
// WithPosDeleteFields stamps each surviving row's position from a rowPositionCursor
// seeded with the surviving row groups, so a pruned group's rows are still counted
// and the survivors keep their original file positions.
//
// The file has two row groups (ids 1..5, then 6..10). Deleting id == 7 matches
// only the second group, so the first is pruned by stats; a dense position
// count would target physical position 1 (id=2) instead of 6 (id=7).
func TestMergeOnReadDeleteAcrossPrunedRowGroups(t *testing.T) {
	ctx := context.Background()
	tbl := newMergeOnReadTestTable(t)

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

	tbl, err = tbl.Append(ctx, array.NewTableReader(data, -1), nil)
	require.NoError(t, err)
	require.Equal(t, []int64{1, 2, 3, 4, 5, 6, 7, 8, 9, 10}, idsInTable(t, tbl))

	tbl, err = tbl.Delete(ctx, iceberg.EqualTo(iceberg.Reference("id"), int64(7)), nil)
	require.NoError(t, err)

	assert.Equal(t, []int64{1, 2, 3, 4, 5, 6, 8, 9, 10}, idsInTable(t, tbl),
		"only id=7 must be deleted; a pruned leading row group must not shift positions")
}

// newMergeOnReadTestTable builds a v2 table that deletes via merge-on-read and
// caps row groups at 5 rows so a 10-row append spans two of them.
func newMergeOnReadTestTable(t *testing.T) *table.Table {
	t.Helper()

	return newMergeOnReadTestTableVersion(t, "2")
}

// newMergeOnReadTestTableVersion builds a merge-on-read table at the given
// format version, capping row groups at 5 rows so a 10-row append spans two.
func newMergeOnReadTestTableVersion(t *testing.T, formatVersion string) *table.Table {
	t.Helper()

	location := filepath.ToSlash(t.TempDir())
	schema := iceberg.NewSchema(0,
		iceberg.NestedField{ID: 1, Name: "id", Type: iceberg.PrimitiveTypes.Int64, Required: true},
		iceberg.NestedField{ID: 2, Name: "data", Type: iceberg.PrimitiveTypes.String, Required: false},
	)
	meta, err := table.NewMetadata(schema, iceberg.UnpartitionedSpec, table.UnsortedSortOrder, location,
		iceberg.Properties{
			table.PropertyFormatVersion:   formatVersion,
			table.WriteDeleteModeKey:      table.WriteModeMergeOnRead,
			table.ParquetRowGroupLimitKey: "5",
		})
	require.NoError(t, err)

	metaLoc := location + "/metadata/v1.metadata.json"
	fsF := func(context.Context) (iceio.IO, error) { return iceio.LocalFS{}, nil }
	cat := &concurrentTestCatalog{metadata: meta, location: metaLoc, fsF: fsF}

	return table.New(table.Identifier{"db", "mor_delete_test"}, meta, metaLoc, fsF, cat)
}

// TestV3MergeOnReadDoubleDeleteMergesDeletionVector guards against a second
// merge-on-read DELETE on a data file that already has a deletion vector
// writing a second live DV. The spec permits at most one DV per data file, so
// the new deletes must merge into the existing DV (which is then superseded),
// not accumulate as a parallel one. A second live DV corrupts the table:
// buildDVIndex rejects it at scan time with "can't index multiple deletion
// vectors". See issue #1372.
func TestV3MergeOnReadDoubleDeleteMergesDeletionVector(t *testing.T) {
	ctx := context.Background()
	tbl := newMergeOnReadTestTableVersion(t, "3")

	arrowSchema := arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int64, Nullable: false},
		{Name: "data", Type: arrow.BinaryTypes.String, Nullable: true},
	}, nil)
	data, err := array.TableFromJSON(memory.DefaultAllocator, arrowSchema, []string{
		`[{"id":1,"data":"a"},{"id":2,"data":"b"},{"id":3,"data":"c"},{"id":4,"data":"d"},{"id":5,"data":"e"}]`,
	})
	require.NoError(t, err)
	defer data.Release()

	tbl, err = tbl.Append(ctx, array.NewTableReader(data, -1), nil)
	require.NoError(t, err)

	// First delete writes DV #1 against the single data file.
	tbl, err = tbl.Delete(ctx, iceberg.EqualTo(iceberg.Reference("id"), int64(2)), nil)
	require.NoError(t, err)
	require.Equal(t, []int64{1, 3, 4, 5}, idsInTable(t, tbl))
	require.Equal(t, 1, liveDVCount(t, tbl), "first delete must write exactly one DV")

	// Second delete on the same data file must merge into DV #1, not add a
	// second live DV.
	tbl, err = tbl.Delete(ctx, iceberg.EqualTo(iceberg.Reference("id"), int64(4)), nil)
	require.NoError(t, err)

	require.Equal(t, 1, liveDVCount(t, tbl),
		"the two deletes must collapse into a single merged deletion vector")
	assert.Equal(t, []int64{1, 3, 5}, idsInTable(t, tbl),
		"both id=2 and id=4 must be deleted and no previously deleted row may resurrect")
}

// TestV3MergeOnReadTripleDeleteMergesDeletionVector extends the double-delete
// guard to a third delete. The second delete supersedes DV #1, leaving it as a
// DELETED-status entry in the deleted-files manifest against the same data file.
// The third delete's collectExistingDVs must skip that ghost entry; otherwise it
// seeds the merged DV from the stale first bitmap (resurrecting id=4, removed by
// the second delete) and tries to remove a DV that no longer exists. See #1372.
func TestV3MergeOnReadTripleDeleteMergesDeletionVector(t *testing.T) {
	ctx := context.Background()
	tbl := newMergeOnReadTestTableVersion(t, "3")

	arrowSchema := arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int64, Nullable: false},
		{Name: "data", Type: arrow.BinaryTypes.String, Nullable: true},
	}, nil)
	data, err := array.TableFromJSON(memory.DefaultAllocator, arrowSchema, []string{
		`[{"id":1,"data":"a"},{"id":2,"data":"b"},{"id":3,"data":"c"},{"id":4,"data":"d"},{"id":5,"data":"e"}]`,
	})
	require.NoError(t, err)
	defer data.Release()

	tbl, err = tbl.Append(ctx, array.NewTableReader(data, -1), nil)
	require.NoError(t, err)

	tbl, err = tbl.Delete(ctx, iceberg.EqualTo(iceberg.Reference("id"), int64(2)), nil)
	require.NoError(t, err)
	require.Equal(t, 1, liveDVCount(t, tbl), "first delete must write exactly one DV")

	tbl, err = tbl.Delete(ctx, iceberg.EqualTo(iceberg.Reference("id"), int64(4)), nil)
	require.NoError(t, err)
	require.Equal(t, 1, liveDVCount(t, tbl), "second delete must merge into the single DV")

	// Third delete: DV #1 is now a superseded DELETED entry. collectExistingDVs
	// must ignore it and seed only from the live merged DV.
	tbl, err = tbl.Delete(ctx, iceberg.EqualTo(iceberg.Reference("id"), int64(1)), nil)
	require.NoError(t, err)

	require.Equal(t, 1, liveDVCount(t, tbl),
		"the three deletes must collapse into a single merged deletion vector")
	assert.Equal(t, []int64{3, 5}, idsInTable(t, tbl),
		"id=1, id=2 and id=4 must all be deleted and none may resurrect")
}

// TestV3MergeOnReadDoubleDeletePartitionedMergesDeletionVectors runs the merge
// on a partitioned table where a single delete touches two data files that each
// already carry a DV. It exercises the per-file partition-context capture in the
// DV seed loop, which the single unpartitioned data file does not.
func TestV3MergeOnReadDoubleDeletePartitionedMergesDeletionVectors(t *testing.T) {
	ctx := context.Background()
	tbl := newPartitionedMergeOnReadTestTable(t)

	arrowSchema := arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int64, Nullable: false},
		{Name: "category", Type: arrow.BinaryTypes.String, Nullable: false},
	}, nil)
	// Two partitions ("x", "y") produce two data files.
	data, err := array.TableFromJSON(memory.DefaultAllocator, arrowSchema, []string{
		`[{"id":1,"category":"x"},{"id":2,"category":"x"},{"id":3,"category":"x"},` +
			`{"id":4,"category":"y"},{"id":5,"category":"y"},{"id":6,"category":"y"}]`,
	})
	require.NoError(t, err)
	defer data.Release()

	tbl, err = tbl.Append(ctx, array.NewTableReader(data, -1), nil)
	require.NoError(t, err)
	require.Equal(t, []int64{1, 2, 3, 4, 5, 6}, idsInTable(t, tbl))

	// Seed a DV on each partition's data file.
	tbl, err = tbl.Delete(ctx, iceberg.EqualTo(iceberg.Reference("id"), int64(2)), nil)
	require.NoError(t, err)
	tbl, err = tbl.Delete(ctx, iceberg.EqualTo(iceberg.Reference("id"), int64(5)), nil)
	require.NoError(t, err)
	require.Equal(t, 2, liveDVCount(t, tbl), "one live DV per partition data file")

	// A single delete touching both partitions must merge into each existing DV
	// rather than add parallel ones — seeding both from their own partition ctx.
	tbl, err = tbl.Delete(ctx, iceberg.NewOr(
		iceberg.EqualTo(iceberg.Reference("id"), int64(1)),
		iceberg.EqualTo(iceberg.Reference("id"), int64(4)),
	), nil)
	require.NoError(t, err)

	require.Equal(t, 2, liveDVCount(t, tbl),
		"each partition data file must keep exactly one merged DV")
	assert.Equal(t, []int64{3, 6}, idsInTable(t, tbl),
		"ids 1,2 (x) and 4,5 (y) must be deleted and none may resurrect")
}

// newPartitionedMergeOnReadTestTable builds a v3 merge-on-read table partitioned
// by identity(category) so distinct category values land in distinct data files.
func newPartitionedMergeOnReadTestTable(t *testing.T) *table.Table {
	t.Helper()

	location := filepath.ToSlash(t.TempDir())
	schema := iceberg.NewSchema(0,
		iceberg.NestedField{ID: 1, Name: "id", Type: iceberg.PrimitiveTypes.Int64, Required: true},
		iceberg.NestedField{ID: 2, Name: "category", Type: iceberg.PrimitiveTypes.String, Required: true},
	)
	spec := iceberg.NewPartitionSpec(iceberg.PartitionField{
		SourceIDs: []int{2},
		FieldID:   1000,
		Transform: iceberg.IdentityTransform{},
		Name:      "category",
	})
	meta, err := table.NewMetadata(schema, &spec, table.UnsortedSortOrder, location,
		iceberg.Properties{
			table.PropertyFormatVersion: "3",
			table.WriteDeleteModeKey:    table.WriteModeMergeOnRead,
		})
	require.NoError(t, err)

	metaLoc := location + "/metadata/v1.metadata.json"
	fsF := func(context.Context) (iceio.IO, error) { return iceio.LocalFS{}, nil }
	cat := &concurrentTestCatalog{metadata: meta, location: metaLoc, fsF: fsF}

	return table.New(table.Identifier{"db", "mor_delete_partitioned_test"}, meta, metaLoc, fsF, cat)
}

// liveDVCount returns the number of live deletion-vector entries in the table's
// current snapshot.
func liveDVCount(t *testing.T, tbl *table.Table) int {
	t.Helper()

	fs, err := tbl.FS(context.Background())
	require.NoError(t, err)

	snapshot := tbl.CurrentSnapshot()
	require.NotNil(t, snapshot)
	manifests, err := snapshot.Manifests(fs)
	require.NoError(t, err)

	count := 0
	for _, m := range manifests {
		for entry, err := range m.Entries(fs, false) {
			require.NoError(t, err)
			if entry.Status() == iceberg.EntryStatusDELETED {
				continue
			}
			df := entry.DataFile()
			if df.FileFormat() == iceberg.PuffinFile &&
				df.ContentType() == iceberg.EntryContentPosDeletes &&
				df.ReferencedDataFile() != nil {
				count++
			}
		}
	}

	return count
}

// idsInTable scans the table and returns the surviving id values, sorted.
func idsInTable(t *testing.T, tbl *table.Table) []int64 {
	t.Helper()

	_, itr, err := tbl.Scan().ToArrowRecords(context.Background())
	require.NoError(t, err)

	var ids []int64
	for rec, err := range itr {
		require.NoError(t, err)
		idIdx := rec.Schema().FieldIndices("id")
		require.NotEmpty(t, idIdx)
		col := rec.Column(idIdx[0]).(*array.Int64)
		for i := range int(rec.NumRows()) {
			ids = append(ids, col.Value(i))
		}
		rec.Release()
	}
	slices.Sort(ids)

	return ids
}
