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
	"fmt"
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

// newCoWConflictTestTable builds a table that deletes via merge-on-read (so a
// concurrent delete writes delete files rather than rewriting data) and
// retries commits, so a lost CAS race is refreshed and replayed. Both the
// delete and update isolation levels are set to isolation.
func newCoWConflictTestTable(t *testing.T, formatVersion string, isolation table.IsolationLevel) *table.Table {
	t.Helper()

	location := filepath.ToSlash(t.TempDir())
	schema := iceberg.NewSchema(0,
		iceberg.NestedField{ID: 1, Name: "id", Type: iceberg.PrimitiveTypes.Int64, Required: true},
		iceberg.NestedField{ID: 2, Name: "data", Type: iceberg.PrimitiveTypes.String, Required: false},
	)
	meta, err := table.NewMetadata(schema, iceberg.UnpartitionedSpec, table.UnsortedSortOrder, location,
		iceberg.Properties{
			table.PropertyFormatVersion:        formatVersion,
			table.WriteDeleteModeKey:           table.WriteModeMergeOnRead,
			table.WriteDeleteIsolationLevelKey: string(isolation),
			table.WriteUpdateIsolationLevelKey: string(isolation),
			table.CommitNumRetriesKey:          "2",
			table.CommitMinRetryWaitMsKey:      "1",
			table.CommitMaxRetryWaitMsKey:      "2",
			table.CommitTotalRetryTimeoutMsKey: "1000",
		})
	require.NoError(t, err)

	metaLoc := location + "/metadata/v1.metadata.json"
	fsF := func(context.Context) (iceio.IO, error) { return iceio.LocalFS{}, nil }
	cat := &concurrentTestCatalog{metadata: meta, location: metaLoc, fsF: fsF}

	return table.New(table.Identifier{"db", "cow_delete_conflict"}, meta, metaLoc, fsF, cat)
}

func cowTestRecords(t *testing.T, rowsJSON string) array.RecordReader {
	t.Helper()

	arrowSchema := arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int64, Nullable: false},
		{Name: "data", Type: arrow.BinaryTypes.String, Nullable: true},
	}, nil)
	data, err := array.TableFromJSON(memory.DefaultAllocator, arrowSchema, []string{rowsJSON})
	require.NoError(t, err)
	t.Cleanup(data.Release)

	return array.NewTableReader(data, -1)
}

// stageCopyOnWrite stages, without committing, a copy-on-write removal of the
// rows matching filter through either Delete or Overwrite (with no new rows).
func stageCopyOnWrite(t *testing.T, tbl *table.Table, op string, filter iceberg.BooleanExpression) *table.Transaction {
	t.Helper()
	ctx := context.Background()

	txn := tbl.NewTransaction()
	switch op {
	case "delete":
		require.NoError(t, txn.SetProperties(iceberg.Properties{
			table.WriteDeleteModeKey: table.WriteModeCopyOnWrite,
		}))
		require.NoError(t, txn.Delete(ctx, filter, nil))
	case "overwrite":
		require.NoError(t, txn.Overwrite(ctx, cowTestRecords(t, `[]`), nil, table.WithOverwriteFilter(filter)))
	default:
		t.Fatalf("unknown op %q", op)
	}

	return txn
}

var (
	cowFormatVersions = []string{"2", "3"}
	cowIsolations     = []table.IsolationLevel{table.IsolationSerializable, table.IsolationSnapshot}
	cowOps            = []string{"delete", "overwrite"}
)

// TestCopyOnWriteConflict_ConcurrentDeleteOnRemovedFile proves a copy-on-write
// Delete or Overwrite is rejected, at every isolation level, when a concurrent
// commit added a delete against a data file it removes. Without the validator,
// refresh-and-replay swaps the original file for a rewrite built from the stale
// snapshot, dropping the concurrent delete and resurrecting its row (#2090).
func TestCopyOnWriteConflict_ConcurrentDeleteOnRemovedFile(t *testing.T) {
	removals := []struct {
		name   string
		filter iceberg.BooleanExpression
	}{
		// id==2 matches part of the file, so it is rewritten.
		{"rewritten", iceberg.EqualTo(iceberg.Reference("id"), int64(2))},
		// id>=1 matches every row, so the file is dropped outright.
		{"fully deleted", iceberg.GreaterThanEqual(iceberg.Reference("id"), int64(1))},
	}

	for _, version := range cowFormatVersions {
		for _, isolation := range cowIsolations {
			for _, op := range cowOps {
				for _, removal := range removals {
					name := fmt.Sprintf("v%s/%s/%s/%s", version, isolation, op, removal.name)
					t.Run(name, func(t *testing.T) {
						ctx := context.Background()
						tbl := appendTenRows(t, newCoWConflictTestTable(t, version, isolation))

						txn := stageCopyOnWrite(t, tbl, op, removal.filter)

						// A concurrent merge-on-read delete of id==4 commits first.
						_, err := tbl.Delete(ctx, iceberg.EqualTo(iceberg.Reference("id"), int64(4)), nil)
						require.NoError(t, err)

						_, err = txn.Commit(ctx)
						require.ErrorIs(t, err, table.ErrConflictingDeleteFiles)
					})
				}
			}
		}
	}
}

// TestCopyOnWriteConflict_ConcurrentAppendCommits is the control: a concurrent
// commit that adds no deletes (an append outside the filter) must not trip the
// validator, so the replayed copy-on-write commit lands with both changes.
func TestCopyOnWriteConflict_ConcurrentAppendCommits(t *testing.T) {
	for _, version := range cowFormatVersions {
		for _, isolation := range cowIsolations {
			for _, op := range cowOps {
				t.Run(fmt.Sprintf("v%s/%s/%s", version, isolation, op), func(t *testing.T) {
					ctx := context.Background()
					tbl := appendTenRows(t, newCoWConflictTestTable(t, version, isolation))

					txn := stageCopyOnWrite(t, tbl, op, iceberg.EqualTo(iceberg.Reference("id"), int64(2)))

					_, err := tbl.Append(ctx, cowTestRecords(t, `[{"id":11,"data":"k"},{"id":12,"data":"l"}]`), nil)
					require.NoError(t, err)

					committed, err := txn.Commit(ctx)
					require.NoError(t, err)
					require.Equal(t, []int64{1, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12}, idsInTable(t, committed))
				})
			}
		}
	}
}
