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
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
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

// activeFiles sums the added + existing data files across manifests. It mirrors
// the production accounting in manifestActiveFiles: a manifest that reports a
// negative (unknown) count fails the test rather than being silently folded in,
// so a count we can't trust never masquerades as a valid total.
func activeFiles(t *testing.T, manifests []iceberg.ManifestFile) int {
	t.Helper()
	var n int
	for _, m := range manifests {
		added, existing := m.AddedDataFiles(), m.ExistingDataFiles()
		require.False(t, added < 0 || existing < 0,
			"manifest %s reports an unknown data-file count", m.FilePath())
		n += int(added) + int(existing)
	}

	return n
}

// rewriteArrowSchema is the single-column schema used by the rewrite tests.
var rewriteArrowSchema = arrow.NewSchema([]arrow.Field{
	{Name: "id", Type: arrow.PrimitiveTypes.Int32, Nullable: false},
}, nil)

// writeOneRowParquet writes a one-row Parquet file at path.
func writeOneRowParquet(t *testing.T, fs iceio.WriteFileIO, path string) {
	t.Helper()

	bldr := array.NewInt32Builder(memory.DefaultAllocator)
	defer bldr.Release()

	bldr.AppendValues([]int32{1}, nil)
	col := bldr.NewArray()
	defer col.Release()

	rec := array.NewRecordBatch(rewriteArrowSchema, []arrow.Array{col}, 1)
	defer rec.Release()

	arrTbl := array.NewTableFromRecords(rewriteArrowSchema, []arrow.RecordBatch{rec})
	defer arrTbl.Release()

	fo, err := fs.Create(path)
	require.NoError(t, err)
	require.NoError(t, pqarrow.WriteTable(arrTbl, fo, arrTbl.NumRows(),
		nil, pqarrow.DefaultWriterProps()))
}

func rewriteSchema() *iceberg.Schema {
	return iceberg.NewSchema(1,
		iceberg.NestedField{ID: 1, Name: "id", Type: iceberg.PrimitiveTypes.Int32, Required: true},
	)
}

// appendSeparateManifests performs n single-row appends, each in its own
// commit, leaving n separate data manifests behind (merge must be disabled on
// the table). It returns the table after the final commit.
func appendSeparateManifests(t *testing.T, ctx context.Context, tbl *table.Table, fs iceio.WriteFileIO, dir, prefix string, n int) *table.Table {
	t.Helper()
	var err error
	for i := range n {
		filePath := fmt.Sprintf("%s/%s-%d.parquet", dir, prefix, i)
		writeOneRowParquet(t, fs, filePath)

		txn := tbl.NewTransaction()
		require.NoError(t, txn.AddFiles(ctx, []string{filePath}, nil, false))
		tbl, err = txn.Commit(ctx)
		require.NoError(t, err)
	}

	return tbl
}

// TestRewriteManifests merges several small data manifests into one without
// changing the data files they reference.
func TestRewriteManifests(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	fs := iceio.LocalFS{}

	// Merge disabled, so each append leaves its own manifest behind.
	meta, err := table.NewMetadata(rewriteSchema(), iceberg.UnpartitionedSpec,
		table.UnsortedSortOrder, dir, iceberg.Properties{
			table.ManifestMergeEnabledKey: "false",
		})
	require.NoError(t, err)

	cat := &mergeCatalog{meta: meta}
	ident := table.Identifier{"default", "test_rewrite_manifests"}
	tbl := table.New(ident, meta, dir+"/metadata/00000.json",
		func(_ context.Context) (iceio.IO, error) { return fs, nil },
		cat,
	)

	const numCommits = 3
	tbl = appendSeparateManifests(t, ctx, tbl, fs, dir, "data", numCommits)

	before, err := tbl.CurrentSnapshot().Manifests(fs)
	require.NoError(t, err)
	require.Len(t, before, numCommits, "each append should leave its own manifest")
	wantFiles := activeFiles(t, before)
	require.Equal(t, numCommits, wantFiles)

	txn := tbl.NewTransaction()
	res, err := txn.RewriteManifests(ctx)
	require.NoError(t, err)
	tbl, err = txn.Commit(ctx)
	require.NoError(t, err)

	assert.Len(t, res.AddedManifests, 1)
	assert.Len(t, res.RewrittenManifests, numCommits)

	snap := tbl.CurrentSnapshot()
	require.NotNil(t, snap)
	assert.Equal(t, table.OpReplace, snap.Summary.Operation)
	assert.Equal(t, "1", snap.Summary.Properties["manifests-created"])
	assert.Equal(t, strconv.Itoa(numCommits), snap.Summary.Properties["manifests-replaced"])
	// All eligible: nothing is kept, and every input file is accounted for.
	assert.Equal(t, "0", snap.Summary.Properties["manifests-kept"])
	assert.Equal(t, strconv.Itoa(numCommits), snap.Summary.Properties["entries-processed"])

	after, err := snap.Manifests(fs)
	require.NoError(t, err)
	assert.Len(t, after, 1, "manifests should be merged into one")
	assert.Equal(t, wantFiles, activeFiles(t, after), "rewrite must preserve the data file count")
}

// TestRewriteManifestsMultipleBins forces a small target size so the merge emits
// more than one output manifest — the manifests-created > 1 path the default
// target size (far larger than any test manifest) never exercises.
func TestRewriteManifestsMultipleBins(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	fs := iceio.LocalFS{}

	meta, err := table.NewMetadata(rewriteSchema(), iceberg.UnpartitionedSpec,
		table.UnsortedSortOrder, dir, iceberg.Properties{
			table.ManifestMergeEnabledKey: "false",
		})
	require.NoError(t, err)

	cat := &mergeCatalog{meta: meta}
	tbl := table.New(table.Identifier{"default", "bins"}, meta, dir+"/metadata/00000.json",
		func(_ context.Context) (iceio.IO, error) { return fs, nil }, cat)

	const numData = 4
	tbl = appendSeparateManifests(t, ctx, tbl, fs, dir, "data", numData)

	before, err := tbl.CurrentSnapshot().Manifests(fs)
	require.NoError(t, err)
	require.Len(t, before, numData)

	// Target just large enough for two manifests per bin: any two fit
	// (2*max ≥ a+b), a third never does (3*min > 2*max for near-equal sizes), so
	// four inputs pack into two bins of two.
	var maxLen int64
	for _, m := range before {
		maxLen = max(maxLen, m.Length())
	}

	txn := tbl.NewTransaction()
	res, err := txn.RewriteManifests(ctx, table.WithManifestTargetSize(2*maxLen))
	require.NoError(t, err)
	tbl, err = txn.Commit(ctx)
	require.NoError(t, err)

	assert.Len(t, res.AddedManifests, 2, "four inputs pack into two output manifests")
	assert.Len(t, res.RewrittenManifests, numData)

	snap := tbl.CurrentSnapshot()
	require.NotNil(t, snap)
	assert.Equal(t, "2", snap.Summary.Properties["manifests-created"])
	assert.Equal(t, strconv.Itoa(numData), snap.Summary.Properties["manifests-replaced"])

	after, err := snap.Manifests(fs)
	require.NoError(t, err)
	assert.Len(t, after, 2, "all data merges into two manifests")
	assert.Equal(t, numData, activeFiles(t, after), "rewrite must preserve the data file count")
}

// TestRewriteManifestsNoSnapshot is a no-op (empty result, no error) when the
// table has no current snapshot, so callers can skip staging an empty REPLACE.
func TestRewriteManifestsNoSnapshot(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	fs := iceio.LocalFS{}

	meta, err := table.NewMetadata(rewriteSchema(), iceberg.UnpartitionedSpec,
		table.UnsortedSortOrder, dir, nil)
	require.NoError(t, err)

	cat := &mergeCatalog{meta: meta}
	tbl := table.New(table.Identifier{"default", "empty"}, meta, dir+"/metadata/00000.json",
		func(_ context.Context) (iceio.IO, error) { return fs, nil }, cat)

	txn := tbl.NewTransaction()
	res, err := txn.RewriteManifests(ctx)
	require.NoError(t, err)
	require.NotNil(t, res)
	assert.Empty(t, res.AddedManifests)
	assert.Empty(t, res.RewrittenManifests)
	assert.True(t, res.IsNoOp())
	assert.Equal(t, table.NoOpNoSnapshot, res.NoOpReason,
		"a missing snapshot must be distinguishable from an already-optimal layout")
}

// TestRewriteManifestsAlreadyOptimal is a no-op when a single data manifest
// already holds everything: there is nothing to merge, so the rewrite writes no
// manifest list, stages nothing, and a following Commit leaves the snapshot
// untouched.
func TestRewriteManifestsAlreadyOptimal(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	track := &trackingFS{}
	fsF := func(_ context.Context) (iceio.IO, error) { return track, nil }

	// Merge enabled so the single append leaves exactly one manifest.
	meta, err := table.NewMetadata(rewriteSchema(), iceberg.UnpartitionedSpec,
		table.UnsortedSortOrder, dir, nil)
	require.NoError(t, err)

	cat := &mergeCatalog{meta: meta}
	tbl := table.New(table.Identifier{"default", "optimal"}, meta, dir+"/metadata/00000.json", fsF, cat)

	tbl = appendSeparateManifests(t, ctx, tbl, track, dir, "data", 1)

	before, err := tbl.CurrentSnapshot().Manifests(track)
	require.NoError(t, err)
	require.Len(t, before, 1)
	beforeSnapshotID := tbl.CurrentSnapshot().SnapshotID

	preExisting := track.snapshotCreated()

	txn := tbl.NewTransaction()
	res, err := txn.RewriteManifests(ctx)
	require.NoError(t, err)
	assert.Empty(t, res.AddedManifests, "a single manifest is already optimal")
	assert.Empty(t, res.RewrittenManifests)
	assert.True(t, res.IsNoOp())
	assert.Equal(t, table.NoOpAlreadyOptimal, res.NoOpReason)

	// The no-op must not have written a new manifest list before the empty
	// result was returned.
	for p := range track.snapshotCreated() {
		if _, ok := preExisting[p]; ok {
			continue
		}
		assert.Falsef(t, strings.HasPrefix(filepath.Base(p), "snap-"),
			"no-op rewrite wrote an unreferenced manifest list %s", p)
	}

	// Nothing was staged, so committing the transaction is a true no-op: the
	// current snapshot is unchanged, not an empty REPLACE.
	committed, err := txn.Commit(ctx)
	require.NoError(t, err)
	require.NotNil(t, committed.CurrentSnapshot())
	assert.Equal(t, beforeSnapshotID, committed.CurrentSnapshot().SnapshotID,
		"a no-op rewrite must not stage an empty REPLACE snapshot")
}

// TestRewriteManifestsUnknownSpecID errors when the requested partition spec id
// does not exist on the table.
func TestRewriteManifestsUnknownSpecID(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	fs := iceio.LocalFS{}

	meta, err := table.NewMetadata(rewriteSchema(), iceberg.UnpartitionedSpec,
		table.UnsortedSortOrder, dir, iceberg.Properties{
			table.ManifestMergeEnabledKey: "false",
		})
	require.NoError(t, err)

	cat := &mergeCatalog{meta: meta}
	tbl := table.New(table.Identifier{"default", "spec"}, meta, dir+"/metadata/00000.json",
		func(_ context.Context) (iceio.IO, error) { return fs, nil }, cat)

	// Need a current snapshot, otherwise the no-op short-circuit fires first.
	tbl = appendSeparateManifests(t, ctx, tbl, fs, dir, "data", 1)

	txn := tbl.NewTransaction()
	_, err = txn.RewriteManifests(ctx, table.WithRewriteSpecID(999))
	require.Error(t, err)
	// The underlying sentinel must survive wrapping so callers can branch on it.
	assert.ErrorIs(t, err, table.ErrPartitionSpecNotFound)
	assert.Contains(t, err.Error(), "unknown partition spec id 999")
}

// TestRewriteManifestsPredicate rewrites only the manifests the predicate
// selects and leaves the rest untouched.
func TestRewriteManifestsPredicate(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	fs := iceio.LocalFS{}

	meta, err := table.NewMetadata(rewriteSchema(), iceberg.UnpartitionedSpec,
		table.UnsortedSortOrder, dir, iceberg.Properties{
			table.ManifestMergeEnabledKey: "false",
		})
	require.NoError(t, err)

	cat := &mergeCatalog{meta: meta}
	tbl := table.New(table.Identifier{"default", "pred"}, meta, dir+"/metadata/00000.json",
		func(_ context.Context) (iceio.IO, error) { return fs, nil }, cat)

	const numCommits = 3
	tbl = appendSeparateManifests(t, ctx, tbl, fs, dir, "data", numCommits)

	before, err := tbl.CurrentSnapshot().Manifests(fs)
	require.NoError(t, err)
	require.Len(t, before, numCommits)

	// Select two of the three manifests by path; the third must be left alone.
	selected := map[string]struct{}{
		before[0].FilePath(): {},
		before[1].FilePath(): {},
	}
	pred := func(m iceberg.ManifestFile) bool {
		_, ok := selected[m.FilePath()]

		return ok
	}

	txn := tbl.NewTransaction()
	res, err := txn.RewriteManifests(ctx, table.WithRewriteManifestPredicate(pred))
	require.NoError(t, err)
	tbl, err = txn.Commit(ctx)
	require.NoError(t, err)

	assert.Len(t, res.AddedManifests, 1, "the two selected manifests merge into one")
	assert.Len(t, res.RewrittenManifests, 2)

	snap := tbl.CurrentSnapshot()
	require.NotNil(t, snap)
	assert.Equal(t, "1", snap.Summary.Properties["manifests-created"])
	assert.Equal(t, "2", snap.Summary.Properties["manifests-replaced"])
	assert.Equal(t, "1", snap.Summary.Properties["manifests-kept"])

	after, err := snap.Manifests(fs)
	require.NoError(t, err)
	assert.Len(t, after, 2, "one merged manifest plus the untouched one")
	assert.Equal(t, numCommits, activeFiles(t, after), "no data file may be dropped")

	// The untouched manifest must survive verbatim.
	var keptPaths []string
	for _, m := range after {
		keptPaths = append(keptPaths, m.FilePath())
	}
	assert.Contains(t, keptPaths, before[2].FilePath(), "unselected manifest must be kept as-is")
}

// TestRewriteManifestsSpecIDFilter rewrites only the manifests of the requested
// partition spec and leaves manifests written under another spec untouched.
func TestRewriteManifestsSpecIDFilter(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	fs := iceio.LocalFS{}

	meta, err := table.NewMetadata(rewriteSchema(), iceberg.UnpartitionedSpec,
		table.UnsortedSortOrder, dir, iceberg.Properties{
			table.ManifestMergeEnabledKey: "false",
		})
	require.NoError(t, err)

	cat := &mergeCatalog{meta: meta}
	tbl := table.New(table.Identifier{"default", "specfilter"}, meta, dir+"/metadata/00000.json",
		func(_ context.Context) (iceio.IO, error) { return fs, nil }, cat)

	// Two manifests under the original unpartitioned spec (id 0).
	tbl = appendSeparateManifests(t, ctx, tbl, fs, dir, "spec0", 2)

	// Evolve the spec, then add a file so it lands in a manifest of the new spec.
	txn := tbl.NewTransaction()
	require.NoError(t, table.NewUpdateSpec(txn, false).
		AddField("id", iceberg.IdentityTransform{}, "id_part").Commit())
	tbl, err = txn.Commit(ctx)
	require.NoError(t, err)

	specOneFile := dir + "/spec1-0.parquet"
	writeOneRowParquet(t, fs, specOneFile)
	txn = tbl.NewTransaction()
	require.NoError(t, txn.AddFiles(ctx, []string{specOneFile}, nil, false))
	tbl, err = txn.Commit(ctx)
	require.NoError(t, err)

	before, err := tbl.CurrentSnapshot().Manifests(fs)
	require.NoError(t, err)
	require.Len(t, before, 3, "two spec-0 manifests plus one spec-1 manifest")

	// Find the lone spec-1 manifest so we can prove it survives verbatim.
	var specOnePath string
	specCount := map[int]int{}
	for _, m := range before {
		specCount[int(m.PartitionSpecID())]++
		if int(m.PartitionSpecID()) == 1 {
			specOnePath = m.FilePath()
		}
	}
	require.Equal(t, 2, specCount[0], "setup must leave two spec-0 manifests")
	require.Equal(t, 1, specCount[1], "setup must leave one spec-1 manifest")

	txn = tbl.NewTransaction()
	res, err := txn.RewriteManifests(ctx, table.WithRewriteSpecID(0))
	require.NoError(t, err)
	tbl, err = txn.Commit(ctx)
	require.NoError(t, err)

	assert.Len(t, res.AddedManifests, 1, "the two spec-0 manifests merge into one")
	assert.Len(t, res.RewrittenManifests, 2)
	for _, m := range res.RewrittenManifests {
		assert.EqualValues(t, 0, m.PartitionSpecID(), "only spec-0 manifests may be rewritten")
	}

	snap := tbl.CurrentSnapshot()
	require.NotNil(t, snap)
	assert.Equal(t, "1", snap.Summary.Properties["manifests-created"])
	assert.Equal(t, "2", snap.Summary.Properties["manifests-replaced"])
	assert.Equal(t, "1", snap.Summary.Properties["manifests-kept"], "the spec-1 manifest is kept")

	after, err := snap.Manifests(fs)
	require.NoError(t, err)
	assert.Len(t, after, 2, "one merged spec-0 manifest plus the untouched spec-1 manifest")
	assert.Equal(t, 3, activeFiles(t, after), "no data file may be dropped")

	var keptPaths []string
	for _, m := range after {
		keptPaths = append(keptPaths, m.FilePath())
	}
	assert.Contains(t, keptPaths, specOnePath, "spec-1 manifest must be kept as-is")
}

// classifyManifests counts data vs delete manifests and returns the path of the
// (single) delete manifest, if any.
func classifyManifests(t *testing.T, manifests []iceberg.ManifestFile) (data, delete int, deletePath string) {
	t.Helper()
	for _, m := range manifests {
		if m.ManifestContent() == iceberg.ManifestContentData {
			data++
		} else {
			delete++
			deletePath = m.FilePath()
		}
	}

	return data, delete, deletePath
}

// TestRewriteManifestsLeavesDeleteManifests verifies that a rewrite merges only
// data manifests and leaves delete manifests untouched.
func TestRewriteManifestsLeavesDeleteManifests(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	fs := iceio.LocalFS{}

	// Delete files require format version >= 2.
	meta, err := table.NewMetadata(rewriteSchema(), iceberg.UnpartitionedSpec,
		table.UnsortedSortOrder, dir, iceberg.Properties{
			table.PropertyFormatVersion:   "2",
			table.ManifestMergeEnabledKey: "false",
		})
	require.NoError(t, err)

	cat := &mergeCatalog{meta: meta}
	tbl := table.New(table.Identifier{"default", "del"}, meta, dir+"/metadata/00000.json",
		func(_ context.Context) (iceio.IO, error) { return fs, nil }, cat)

	const numData = 2
	tbl = appendSeparateManifests(t, ctx, tbl, fs, dir, "data", numData)

	// Add a positional-delete file. RowDelta only records its metadata, so the
	// referenced path need not exist on disk. This leaves a delete manifest the
	// rewrite must not touch.
	txn := tbl.NewTransaction()
	require.NoError(t, txn.NewRowDelta(nil).
		AddDeletes(buildPosDeleteFile(t, dir+"/data/pos-del.parquet")).
		Commit(ctx))
	tbl, err = txn.Commit(ctx)
	require.NoError(t, err)

	before, err := tbl.CurrentSnapshot().Manifests(fs)
	require.NoError(t, err)
	dataN, delN, beforeDeletePath := classifyManifests(t, before)
	require.Equal(t, numData, dataN)
	require.Equal(t, 1, delN, "setup must leave one delete manifest")

	txn = tbl.NewTransaction()
	res, err := txn.RewriteManifests(ctx)
	require.NoError(t, err)
	tbl, err = txn.Commit(ctx)
	require.NoError(t, err)

	assert.Len(t, res.AddedManifests, 1)
	assert.Len(t, res.RewrittenManifests, numData, "only data manifests are rewritten")

	snap := tbl.CurrentSnapshot()
	require.NotNil(t, snap)
	assert.Equal(t, "1", snap.Summary.Properties["manifests-created"])
	assert.Equal(t, strconv.Itoa(numData), snap.Summary.Properties["manifests-replaced"])
	assert.Equal(t, "1", snap.Summary.Properties["manifests-kept"], "the delete manifest is kept")

	after, err := snap.Manifests(fs)
	require.NoError(t, err)
	afterData, afterDel, afterDeletePath := classifyManifests(t, after)
	assert.Equal(t, 1, afterData, "data manifests merged into one")
	assert.Equal(t, 1, afterDel, "delete manifest preserved")
	assert.Equal(t, beforeDeletePath, afterDeletePath, "delete manifest must survive verbatim")
}

// trackingFS wraps LocalFS and records the paths it creates and removes so a
// test can assert that orphaned manifests are cleaned up after a commit.
type trackingFS struct {
	iceio.LocalFS
	mu      sync.Mutex
	created []string
	removed []string
}

func (f *trackingFS) Create(name string) (iceio.FileWriter, error) {
	f.mu.Lock()
	f.created = append(f.created, name)
	f.mu.Unlock()

	return f.LocalFS.Create(name)
}

func (f *trackingFS) Remove(name string) error {
	f.mu.Lock()
	f.removed = append(f.removed, name)
	f.mu.Unlock()

	return f.LocalFS.Remove(name)
}

func (f *trackingFS) snapshotCreated() map[string]struct{} {
	f.mu.Lock()
	defer f.mu.Unlock()
	out := make(map[string]struct{}, len(f.created))
	for _, p := range f.created {
		out[p] = struct{}{}
	}

	return out
}

func (f *trackingFS) wasRemoved(path string) bool {
	f.mu.Lock()
	defer f.mu.Unlock()
	for _, p := range f.removed {
		if p == path {
			return true
		}
	}

	return false
}

// mergedManifestsCreated returns the data-manifest .avro files (those whose
// basename is not a "snap-" manifest list) the rewrite wrote after preExisting
// was captured.
func mergedManifestsCreated(track *trackingFS, preExisting map[string]struct{}) []string {
	var out []string
	for p := range track.snapshotCreated() {
		if _, ok := preExisting[p]; ok {
			continue
		}
		if !strings.HasSuffix(p, ".avro") || strings.HasPrefix(filepath.Base(p), "snap-") {
			continue
		}
		out = append(out, p)
	}

	return out
}

// stagedHeadCatalog simulates a string of concurrent writers: each retry's
// LoadTable advances the head to the next pre-built metadata, so the rewrite
// must rebuild — and supersede the previous attempt's merged manifest — on
// every retry, not just the first (which a static head would not exercise). It
// returns ErrCommitFailed for the first `failures` CommitTable calls.
type stagedHeadCatalog struct {
	current          table.Metadata
	heads            []table.Metadata
	headIdx          int
	failures         int
	commitTableCalls atomic.Int32
	location         string
	fsF              func(context.Context) (iceio.IO, error)
}

func (c *stagedHeadCatalog) LoadTable(_ context.Context, _ table.Identifier) (*table.Table, error) {
	if c.headIdx < len(c.heads) {
		c.current = c.heads[c.headIdx]
		c.headIdx++
	}

	return table.New(table.Identifier{"default", "staged"}, c.current,
		c.location+"/metadata/current.json", c.fsF, c), nil
}

func (c *stagedHeadCatalog) CommitTable(_ context.Context, _ table.Identifier, _ []table.Requirement, updates []table.Update) (table.Metadata, string, error) {
	n := c.commitTableCalls.Add(1)
	if int(n) <= c.failures {
		return nil, "", fmt.Errorf("%w: simulated 409 conflict", table.ErrCommitFailed)
	}

	meta, err := table.UpdateTableMetadata(c.current, updates, "")
	if err != nil {
		return nil, "", err
	}
	c.current = meta

	return meta, c.location + "/metadata/final.json", nil
}

// appendOne commits a single new data file onto tbl and returns the new table.
func appendOne(t *testing.T, ctx context.Context, tbl *table.Table, fs iceio.WriteFileIO, dir, name string) *table.Table {
	t.Helper()
	path := fmt.Sprintf("%s/%s.parquet", dir, name)
	writeOneRowParquet(t, fs, path)

	txn := tbl.NewTransaction()
	require.NoError(t, txn.AddFiles(ctx, []string{path}, nil, false))
	out, err := txn.Commit(ctx)
	require.NoError(t, err)

	return out
}

// stagedRewriteHeads builds the stale base plus two successive concurrent heads
// (each adds one data manifest the rewrite must inherit) and returns them. The
// metadata carries fast-retry properties so the rewrite's commit retries
// quickly. numRetries sets commit.retry.num-retries.
func stagedRewriteHeads(t *testing.T, ctx context.Context, dir, numRetries string, fsF func(context.Context) (iceio.IO, error), track iceio.WriteFileIO) (h0, h1, h2 table.Metadata) {
	t.Helper()
	props := iceberg.Properties{
		table.PropertyFormatVersion:        "2",
		table.ManifestMergeEnabledKey:      "false",
		table.CommitMinRetryWaitMsKey:      "0",
		table.CommitMaxRetryWaitMsKey:      "0",
		table.CommitTotalRetryTimeoutMsKey: "60000",
		table.CommitNumRetriesKey:          numRetries,
	}
	meta, err := table.NewMetadata(rewriteSchema(), iceberg.UnpartitionedSpec,
		table.UnsortedSortOrder, dir, props)
	require.NoError(t, err)

	setup := &mergeCatalog{meta: meta}
	tbl := table.New(table.Identifier{"default", "staged"}, meta, dir+"/metadata/00000.json", fsF, setup)
	tbl = appendSeparateManifests(t, ctx, tbl, track, dir, "stale", 3)
	h0 = tbl.Metadata()
	tbl = appendOne(t, ctx, tbl, track, dir, "concurrent-1")
	h1 = tbl.Metadata()
	tbl = appendOne(t, ctx, tbl, track, dir, "concurrent-2")
	h2 = tbl.Metadata()

	return h0, h1, h2
}

// TestRewriteManifestsCleansEverySupersededGeneration forces two conflicts, each
// against a freshly-advanced head, so the rewrite rebuilds twice and accumulates
// two generations of superseded manifests. It asserts every superseded
// generation is cleaned up — not just the most recent one.
func TestRewriteManifestsCleansEverySupersededGeneration(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	track := &trackingFS{}
	fsF := func(_ context.Context) (iceio.IO, error) { return track, nil }

	h0, h1, h2 := stagedRewriteHeads(t, ctx, dir, "2", fsF, track)

	// Two conflicts then success: attempts 0 and 1 are superseded, attempt 2 wins.
	cat := &stagedHeadCatalog{current: h0, heads: []table.Metadata{h1, h2}, failures: 2, location: dir, fsF: fsF}
	tbl := table.New(table.Identifier{"default", "staged"}, h0, dir+"/metadata/00000.json", fsF, cat)

	preExisting := track.snapshotCreated()

	txn := tbl.NewTransaction()
	res, err := txn.RewriteManifests(ctx)
	require.NoError(t, err)
	committed, err := txn.Commit(ctx)
	require.NoError(t, err)

	require.EqualValues(t, 3, cat.commitTableCalls.Load(),
		"expected two conflicts followed by one successful commit")

	snap := committed.CurrentSnapshot()
	require.NotNil(t, snap)
	after, err := snap.Manifests(track)
	require.NoError(t, err)
	require.Len(t, after, 1, "all inherited manifests merge into one")
	assert.Len(t, res.AddedManifests, 1)
	// The winner merged the fully-advanced head: 3 stale + 2 concurrent.
	assert.Len(t, res.RewrittenManifests, 5)

	merged := mergedManifestsCreated(track, preExisting)
	require.Len(t, merged, 3, "each of the three attempts writes one merged manifest")

	referenced := map[string]struct{}{}
	for _, m := range after {
		referenced[m.FilePath()] = struct{}{}
	}
	var removed int
	for _, p := range merged {
		if _, ok := referenced[p]; ok {
			continue
		}
		assert.Truef(t, track.wasRemoved(p),
			"superseded manifest %s was leaked across retries", p)
		removed++
	}
	assert.Equal(t, 2, removed, "both superseded generations must be cleaned up")
}

// TestRewriteManifestsCleansSupersededOnExhaustedRetries asserts that when every
// commit attempt fails with ErrCommitFailed, every merged manifest written —
// the superseded attempts and the final one — is cleaned up rather than leaked.
func TestRewriteManifestsCleansSupersededOnExhaustedRetries(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	track := &trackingFS{}
	fsF := func(_ context.Context) (iceio.IO, error) { return track, nil }

	h0, h1, h2 := stagedRewriteHeads(t, ctx, dir, "2", fsF, track) // 3 attempts

	// Never succeed: every attempt conflicts, exhausting the retry budget.
	cat := &stagedHeadCatalog{current: h0, heads: []table.Metadata{h1, h2}, failures: 99, location: dir, fsF: fsF}
	tbl := table.New(table.Identifier{"default", "staged"}, h0, dir+"/metadata/00000.json", fsF, cat)

	preExisting := track.snapshotCreated()

	txn := tbl.NewTransaction()
	_, err := txn.RewriteManifests(ctx)
	require.NoError(t, err)
	_, err = txn.Commit(ctx)
	require.Error(t, err, "commit must fail once retries are exhausted")
	require.ErrorIs(t, err, table.ErrCommitFailed)

	// Three attempts wrote three merged manifests. None is referenced (no commit
	// succeeded), so all three — the superseded ones and the final attempt's —
	// must be cleaned on the exhausted-failure path.
	merged := mergedManifestsCreated(track, preExisting)
	require.Len(t, merged, 3, "three attempts each write one merged manifest")

	for _, p := range merged {
		assert.Truef(t, track.wasRemoved(p), "orphaned manifest %s must be cleaned on the failure path", p)
	}
}

// TestRewriteManifestsStaleCountsAfterOCCRetry forces a commit conflict and
// asserts that the RewriteManifestsResult and the committed summary reflect the
// winning attempt — the one rebuilt against the fresh parent — not the stale
// attempt 0. It also asserts that the manifests written by the superseded
// attempt are deleted rather than leaked.
func TestRewriteManifestsStaleCountsAfterOCCRetry(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	track := &trackingFS{}
	fsF := func(_ context.Context) (iceio.IO, error) { return track, nil }

	props := iceberg.Properties{
		table.PropertyFormatVersion:        "2",
		table.ManifestMergeEnabledKey:      "false",
		table.CommitMinRetryWaitMsKey:      "0",
		table.CommitMaxRetryWaitMsKey:      "0",
		table.CommitTotalRetryTimeoutMsKey: "60000",
		table.CommitNumRetriesKey:          "2",
	}
	meta, err := table.NewMetadata(rewriteSchema(), iceberg.UnpartitionedSpec,
		table.UnsortedSortOrder, dir, props)
	require.NoError(t, err)

	cat := &occScenarioCatalog{current: meta, conflictsLeft: 0, location: dir}
	ident := table.Identifier{"default", "occ_rewrite"}
	tbl := table.New(ident, meta, dir+"/metadata/00000.json", fsF, cat)

	// Writer A's stale view: three separate data manifests.
	const staleCount = 3
	tbl = appendSeparateManifests(t, ctx, tbl, track, dir, "stale", staleCount)
	staleTbl := tbl // metadata frozen at three manifests

	// A concurrent writer adds a fourth manifest the catalog now knows about
	// but Writer A does not. The rewrite, on retry, must re-merge all four.
	const freshCount = staleCount + 1
	_ = appendSeparateManifests(t, ctx, tbl, track, dir, "fresh", 1)
	freshSnap := cat.current.CurrentSnapshot()
	require.NotNil(t, freshSnap)
	freshManifests, err := freshSnap.Manifests(track)
	require.NoError(t, err)
	require.Len(t, freshManifests, freshCount, "catalog should now hold four manifests")

	// Force exactly one conflict so Writer A retries against the fresh parent.
	cat.conflictsLeft = 1
	cat.commitTableCalls.Store(0) // ignore the setup commits

	preExisting := track.snapshotCreated()

	txn := staleTbl.NewTransaction()
	res, err := txn.RewriteManifests(ctx)
	require.NoError(t, err)
	committed, err := txn.Commit(ctx)
	require.NoError(t, err)

	require.EqualValues(t, 2, cat.commitTableCalls.Load(),
		"expected one conflict followed by one successful commit")

	// The result must reflect the winning attempt: four manifests replaced,
	// not the three Writer A saw at attempt 0.
	assert.Len(t, res.RewrittenManifests, freshCount,
		"result must report the fresh parent's manifests, not attempt 0's")
	assert.Len(t, res.AddedManifests, 1)

	snap := committed.CurrentSnapshot()
	require.NotNil(t, snap)
	assert.Equal(t, "1", snap.Summary.Properties["manifests-created"])
	assert.Equal(t, strconv.Itoa(freshCount), snap.Summary.Properties["manifests-replaced"],
		"summary must reflect the winning attempt's counts")
	assert.Equal(t, strconv.Itoa(freshCount), snap.Summary.Properties["entries-processed"])

	after, err := snap.Manifests(track)
	require.NoError(t, err)
	assert.Len(t, after, 1, "all four manifests merge into one")
	assert.Equal(t, freshCount, activeFiles(t, after))

	// No orphan leak: every .avro file written during the rewrite that the
	// committed snapshot does not reference must have been removed. The
	// pre-existing data manifests (written before the rewrite) are exempt.
	referenced := map[string]struct{}{snap.ManifestList: {}}
	for _, m := range after {
		referenced[m.FilePath()] = struct{}{}
	}
	for p := range track.snapshotCreated() {
		if !strings.HasSuffix(p, ".avro") {
			continue
		}
		if _, ok := preExisting[p]; ok {
			continue
		}
		if _, ok := referenced[p]; ok {
			continue
		}
		assert.Truef(t, track.wasRemoved(p),
			"manifest %s written by the superseded attempt was leaked, not cleaned up", p)
	}
}
