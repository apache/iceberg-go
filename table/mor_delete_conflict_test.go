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
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/iceberg-go"
	iceio "github.com/apache/iceberg-go/io"
	"github.com/apache/iceberg-go/table"
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
