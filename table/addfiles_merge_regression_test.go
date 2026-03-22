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

// Regression test for: AddFiles() ignoring commit.manifest-merge.enabled.
//
// Root cause: transaction.go hardcoded .fastAppend() in AddFiles(), bypassing
// appendSnapshotProducer() which reads ManifestMergeEnabledKey.
//
// Java reference: BaseTable.newAppend() always uses MergeAppend (merge default=true).
// AddDataFiles() and Append() in Go both correctly call appendSnapshotProducer().
// Only AddFiles() was missing the call.
//
// Fix: change line 780 in transaction.go from:
//   updater := t.updateSnapshot(fs, snapshotProps, OpAppend).fastAppend()
// to:
//   updater := t.appendSnapshotProducer(fs, snapshotProps)

import (
	"context"
	"fmt"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/arrow-go/v18/parquet/pqarrow"
	"github.com/apache/iceberg-go"
	iceio "github.com/apache/iceberg-go/io"
	"github.com/apache/iceberg-go/table"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// mergeCatalog is a minimal in-memory catalog that supports sequential commits
// by applying updates to its stored metadata on each CommitTable call.
type mergeCatalog struct {
	meta table.Metadata
}

func (c *mergeCatalog) LoadTable(_ context.Context, _ table.Identifier) (*table.Table, error) {
	return nil, nil
}

func (c *mergeCatalog) CommitTable(_ context.Context, _ table.Identifier, _ []table.Requirement, updates []table.Update) (table.Metadata, string, error) {
	meta, err := table.UpdateTableMetadata(c.meta, updates, "")
	if err != nil {
		return nil, "", err
	}
	c.meta = meta
	return meta, "", nil
}

// TestAddFilesRespectsMergeEnabled verifies that AddFiles() respects the
// commit.manifest-merge.enabled table property.
//
// Before fix: 3 sequential AddFiles() commits produce 3 manifests because
// fastAppend() is hardcoded and the merge property is ignored.
//
// After fix: with minCountToMerge=2, the 3rd commit triggers a merge and
// produces 1 merged manifest.
func TestAddFilesRespectsMergeEnabled(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	fs := iceio.LocalFS{}

	// Minimal Arrow schema matching the Iceberg schema below.
	arrowSchema := arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int32, Nullable: false},
	}, nil)

	// writeParquet creates a single-row Parquet file at the given path.
	writeParquet := func(path string) {
		bldr := array.NewInt32Builder(memory.DefaultAllocator)
		defer bldr.Release()
		bldr.AppendValues([]int32{1}, nil)
		col := bldr.NewArray()
		defer col.Release()

		rec := array.NewRecord(arrowSchema, []arrow.Array{col}, 1)
		defer rec.Release()
		arrTbl := array.NewTableFromRecords(arrowSchema, []arrow.Record{rec})
		defer arrTbl.Release()

		fo, err := fs.Create(path)
		require.NoError(t, err)
		require.NoError(t, pqarrow.WriteTable(arrTbl, fo, arrTbl.NumRows(),
			nil, pqarrow.DefaultWriterProps()))
	}

	schema := iceberg.NewSchema(1,
		iceberg.NestedField{ID: 1, Name: "id", Type: iceberg.PrimitiveTypes.Int32, Required: true},
	)

	// Create table with manifest merging enabled and minCountToMerge=2.
	// With minCount=2, a commit that accumulates >2 manifests triggers a merge.
	meta, err := table.NewMetadata(schema, iceberg.UnpartitionedSpec,
		table.UnsortedSortOrder, dir, iceberg.Properties{
			table.ManifestMergeEnabledKey:  "true",
			table.ManifestMinMergeCountKey: "2",
		})
	require.NoError(t, err)

	cat := &mergeCatalog{meta: meta}
	ident := table.Identifier{"default", "test_addfiles_merge"}
	tbl := table.New(ident, meta, dir+"/metadata/00000.json",
		func(_ context.Context) (iceio.IO, error) { return fs, nil },
		cat,
	)

	// Perform 3 separate AddFiles commits — simulating the committer's 1-per-batch pattern.
	// minCountToMerge=2 means the 3rd commit should trigger a merge.
	const numCommits = 3
	for i := range numCommits {
		filePath := fmt.Sprintf("%s/data-%d.parquet", dir, i)
		writeParquet(filePath)

		txn := tbl.NewTransaction()
		require.NoError(t, txn.AddFiles(ctx, []string{filePath}, nil, false),
			"AddFiles commit %d failed", i+1)
		tbl, err = txn.Commit(ctx)
		require.NoError(t, err, "Commit %d failed", i+1)
	}

	snap := tbl.CurrentSnapshot()
	require.NotNil(t, snap, "table should have a current snapshot after commits")

	manifests, err := snap.Manifests(fs)
	require.NoError(t, err, "reading manifests from HEAD snapshot failed")

	// After fix: merge fires on commit 3 (accumulated 3 > minCount 2) → 1 merged manifest.
	// Before fix: 3 manifests (fastAppend carries all forward without merging).
	assert.Equal(t, 1, len(manifests),
		"expected 1 merged manifest after %d AddFiles commits with manifest merge enabled "+
			"(minCountToMerge=2), got %d manifests — "+
			"AddFiles() is not respecting commit.manifest-merge.enabled",
		numCommits, len(manifests))
}
