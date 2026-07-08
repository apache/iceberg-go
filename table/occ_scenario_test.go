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

// Scenario-level regression test for the OCC manifest-list rebuild fix.
//
// Ported from the integration test suite to serve as a fast, local regression
// guard.  Uses real on-disk Parquet and Avro files (local FS temp dir) but
// does not require Docker or a remote catalog.

import (
	"context"
	"fmt"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/iceberg-go"
	iceio "github.com/apache/iceberg-go/io"
	"github.com/apache/iceberg-go/table"
	"github.com/stretchr/testify/suite"
)

// ---------------------------------------------------------------------------
// Mock catalog
// ---------------------------------------------------------------------------

// occScenarioCatalog simulates N CAS conflicts (HTTP 409) followed by a
// successful commit.  LoadTable always returns the current metadata so that
// the retry loop can rebase against it.
type occScenarioCatalog struct {
	current          table.Metadata
	conflictsLeft    int
	loadTableCalls   atomic.Int32
	commitTableCalls atomic.Int32
	location         string
}

func (c *occScenarioCatalog) LoadTable(_ context.Context, _ table.Identifier) (*table.Table, error) {
	c.loadTableCalls.Add(1)

	return table.New(
		[]string{"default", "occ_scenario_test"},
		c.current,
		c.location+"/metadata/current.metadata.json",
		func(_ context.Context) (iceio.IO, error) { return iceio.LocalFS{}, nil },
		c,
	), nil
}

func (c *occScenarioCatalog) CommitTable(
	_ context.Context,
	_ table.Identifier,
	_ []table.Requirement,
	updates []table.Update,
) (table.Metadata, string, error) {
	c.commitTableCalls.Add(1)

	if c.conflictsLeft > 0 {
		c.conflictsLeft--

		return nil, "", fmt.Errorf("%w: simulated 409 conflict", table.ErrCommitFailed)
	}

	newMeta, err := table.UpdateTableMetadata(c.current, updates, "")
	if err != nil {
		return nil, "", err
	}

	c.current = newMeta

	return newMeta, c.location + "/metadata/final.metadata.json", nil
}

// ---------------------------------------------------------------------------
// Test suite
// ---------------------------------------------------------------------------

type OCCScenarioTestSuite struct {
	suite.Suite
	ctx      context.Context
	location string
}

func TestOCCScenario(t *testing.T) {
	suite.Run(t, new(OCCScenarioTestSuite))
}

func (s *OCCScenarioTestSuite) SetupSuite() {
	s.ctx = context.Background()
}

func (s *OCCScenarioTestSuite) SetupTest() {
	s.location = filepath.ToSlash(s.T().TempDir())
}

func (s *OCCScenarioTestSuite) makeTable(
	conflicts int,
	extraProps iceberg.Properties,
) (*table.Table, *occScenarioCatalog) {
	schema := iceberg.NewSchema(0,
		iceberg.NestedField{ID: 1, Name: "id", Type: iceberg.PrimitiveTypes.Int64},
		iceberg.NestedField{ID: 2, Name: "val", Type: iceberg.PrimitiveTypes.String},
	)

	props := iceberg.Properties{table.PropertyFormatVersion: "2"}
	for k, v := range extraProps {
		props[k] = v
	}

	meta, err := table.NewMetadata(schema, iceberg.UnpartitionedSpec,
		table.UnsortedSortOrder, s.location, props)
	s.Require().NoError(err)

	cat := &occScenarioCatalog{
		current:       meta,
		conflictsLeft: conflicts,
		location:      s.location,
	}

	tbl := table.New(
		[]string{"default", "occ_scenario_test"},
		meta,
		s.location+"/metadata/v1.metadata.json",
		func(_ context.Context) (iceio.IO, error) { return iceio.LocalFS{}, nil },
		cat,
	)

	return tbl, cat
}

func (s *OCCScenarioTestSuite) makeArrowTable() arrow.Table {
	mem := memory.DefaultAllocator
	sc := arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int64, Nullable: true},
		{Name: "val", Type: arrow.BinaryTypes.String, Nullable: true},
	}, nil)

	tbl, err := array.TableFromJSON(mem, sc, []string{
		`[{"id": 1, "val": "hello"}]`,
	})
	s.Require().NoError(err)

	return tbl
}

// ---------------------------------------------------------------------------
// Regression test
// ---------------------------------------------------------------------------

// TestManifestListInheritedAfterConflict is a regression test for the bug
// where a retried snapshot reused the original manifest list (built against a
// stale parent) instead of inheriting the manifests already committed by
// concurrent writers.
//
// Scenario:
//   - Writer B commits one row to an empty table (snapshot B, manifest B).
//   - Writer A starts a transaction from the empty base (no knowledge of B).
//   - Writer A's first commit attempt fails with ErrCommitFailed (409).
//   - doCommit reloads and gets snapshot B as the fresh head.
//   - The rebuildManifestList closure rewrites the manifest list to contain
//     manifest A (Writer A's file) and manifest B (inherited).
//   - Writer A's second attempt succeeds.
//
// Without the fix the final snapshot contains only manifest A and a scan
// returns 1 row instead of 2.
func (s *OCCScenarioTestSuite) TestManifestListInheritedAfterConflict() {
	props := iceberg.Properties{
		table.CommitMinRetryWaitMsKey:      "0",
		table.CommitMaxRetryWaitMsKey:      "0",
		table.CommitTotalRetryTimeoutMsKey: "60000",
		table.CommitNumRetriesKey:          "2",
	}

	// Step 1: commit Writer B's row to an empty table.
	// This writes real Parquet + manifest Avro files to s.location/metadata/.
	emptyTbl, catB := s.makeTable(0, props)
	rowB := s.makeArrowTable()
	defer rowB.Release()

	txB := emptyTbl.NewTransaction()
	s.Require().NoError(txB.AppendTable(s.ctx, rowB, rowB.NumRows(), nil))

	_, err := txB.Commit(s.ctx)
	s.Require().NoError(err, "Writer B must commit successfully")

	metaAfterB := catB.current // catalog state after B's commit (real files on disk)

	// Step 2: Writer A's catalog starts with B's committed state.
	// It returns ErrCommitFailed once (simulating B having committed just
	// before A), then accepts the retry.
	catA := &occScenarioCatalog{
		current:       metaAfterB,
		conflictsLeft: 1,
		location:      s.location,
	}

	// Step 3: Writer A's table starts from the EMPTY base — it loaded the
	// table before B committed.
	writerATable := table.New(
		emptyTbl.Identifier(),
		emptyTbl.Metadata(), // empty: no knowledge of B's snapshot yet
		s.location+"/metadata/v1.metadata.json",
		func(_ context.Context) (iceio.IO, error) { return iceio.LocalFS{}, nil },
		catA,
	)

	rowA := s.makeArrowTable()
	defer rowA.Release()

	txA := writerATable.NewTransaction()
	s.Require().NoError(txA.AppendTable(s.ctx, rowA, rowA.NumRows(), nil))

	_, err = txA.Commit(s.ctx)
	s.Require().NoError(err, "Writer A must succeed after one conflict retry")

	s.Equal(int32(2), catA.commitTableCalls.Load(),
		"expected 2 commit attempts: 1 conflict + 1 success")

	// Step 4: the final snapshot must reference BOTH manifests.
	// manifest A (Writer A's data file) + manifest B (inherited from B's snapshot).
	// Without the fix, only manifest A is present and the count is 1.
	finalSnap := catA.current.CurrentSnapshot()
	s.Require().NotNil(finalSnap, "committed table must have a current snapshot")

	fio := iceio.LocalFS{}
	manifests, err := finalSnap.Manifests(fio)
	s.Require().NoError(err, "must be able to read manifest list from disk")
	s.Require().Len(manifests, 2,
		"expected 2 manifests (Writer A + Writer B); got %d — "+
			"manifest list not inherited on retry", len(manifests))

	// Each manifest must have exactly 1 data file (added or existing).
	for i, mf := range manifests {
		count := mf.AddedDataFiles() + mf.ExistingDataFiles()
		s.EqualValues(1, count,
			"manifest[%d] should have 1 data file, got %d", i, count)
	}
}

// TestV3RowLineageCorrectAfterOCCRetry verifies that row lineage (first-row-id,
// added-rows, next-row-id) is correctly recomputed on OCC retry for v3 tables.
//
// Scenario:
//   - Writer B commits 1 row to an empty v3 table (next-row-id advances 0→1).
//   - Writer A starts from the empty base (stale next-row-id=0) and appends 1 row.
//   - Writer A's first commit attempt fails (409 conflict).
//   - On retry, rebuildManifestList re-derives first-row-id from fresh metadata
//     (next-row-id=1 after B's commit) so that Writer A's first-row-id = 1.
//   - The final metadata next-row-id = 2 (gap-free: B claimed [0,1), A claimed [1,2)).
//
// This guards against the stale-ID bug the reviewer identified: if the retry
// reused the first-row-id computed at attempt 0, validateAndUpdateRowLineage
// would reject the commit because first-row-id (0) < next-row-id (1).
func (s *OCCScenarioTestSuite) TestV3RowLineageCorrectAfterOCCRetry() {
	props := iceberg.Properties{
		table.CommitMinRetryWaitMsKey:      "0",
		table.CommitMaxRetryWaitMsKey:      "0",
		table.CommitTotalRetryTimeoutMsKey: "60000",
		table.CommitNumRetriesKey:          "2",
	}

	// Step 1: Writer B commits 1 row to an empty v3 table.
	emptyTbl, catB := s.makeV3Table(0, props)
	rowB := s.makeArrowTable()
	defer rowB.Release()

	txB := emptyTbl.NewTransaction()
	s.Require().NoError(txB.AppendTable(s.ctx, rowB, rowB.NumRows(), nil))

	_, err := txB.Commit(s.ctx)
	s.Require().NoError(err, "Writer B must commit successfully")

	metaAfterB := catB.current
	s.Equal(int64(1), metaAfterB.NextRowID(),
		"after Writer B: next-row-id must be 1 (one row committed)")

	snapB := metaAfterB.CurrentSnapshot()
	s.Require().NotNil(snapB)
	s.Require().NotNil(snapB.FirstRowID)
	s.Equal(int64(0), *snapB.FirstRowID, "Writer B first-row-id starts at 0")

	// Step 2: Writer A's catalog starts with B's committed state but returns
	// one conflict before accepting.
	catA := &occScenarioCatalog{
		current:       metaAfterB,
		conflictsLeft: 1,
		location:      s.location,
	}

	// Writer A's table starts from the EMPTY base — stale next-row-id=0.
	writerATable := table.New(
		emptyTbl.Identifier(),
		emptyTbl.Metadata(),
		s.location+"/metadata/v1.metadata.json",
		func(_ context.Context) (iceio.IO, error) { return iceio.LocalFS{}, nil },
		catA,
	)

	rowA := s.makeArrowTable()
	defer rowA.Release()

	txA := writerATable.NewTransaction()
	s.Require().NoError(txA.AppendTable(s.ctx, rowA, rowA.NumRows(), nil))

	_, err = txA.Commit(s.ctx)
	s.Require().NoError(err, "Writer A must succeed after one conflict retry")

	s.Equal(int32(2), catA.commitTableCalls.Load(),
		"expected 2 commit attempts: 1 conflict + 1 success")

	// Step 3: Validate row lineage in final metadata.
	finalMeta := catA.current
	s.Equal(int64(2), finalMeta.NextRowID(),
		"final next-row-id must be 2: B claimed [0,1), A claimed [1,2)")

	finalSnap := finalMeta.CurrentSnapshot()
	s.Require().NotNil(finalSnap)
	s.Require().NotNil(finalSnap.FirstRowID)
	s.Require().NotNil(finalSnap.AddedRows)
	s.Equal(int64(1), *finalSnap.FirstRowID,
		"Writer A's first-row-id must be 1 (rebased against B's next-row-id)")
	s.Equal(int64(1), *finalSnap.AddedRows,
		"Writer A added exactly 1 row")

	// Step 4: Validate manifest-level first_row_id is persisted correctly.
	fio := iceio.LocalFS{}
	manifests, err := finalSnap.Manifests(fio)
	s.Require().NoError(err)
	s.Require().Len(manifests, 2,
		"expected 2 manifests (Writer A + inherited Writer B)")

	// The new manifest (from Writer A's snapshot) should have first_row_id = 1.
	var writerAManifest iceberg.ManifestFile
	for _, mf := range manifests {
		if mf.SnapshotID() == finalSnap.SnapshotID {
			writerAManifest = mf
		}
	}
	s.Require().NotNil(writerAManifest, "must find Writer A's manifest")
	s.Require().NotNil(writerAManifest.FirstRowID(),
		"v3 data manifest must have first_row_id set")
	s.Equal(int64(1), *writerAManifest.FirstRowID(),
		"Writer A manifest first_row_id must equal snapshot first-row-id (rebased)")
}

// TestV3MergeAppendRowLineageAfterOCCRetry verifies that merge-append
// correctly maintains row lineage under OCC retry. The reviewer's concern was
// that manifest merging could lose the manifest-level first_row_id. Since the
// manifest-list writer (not the manifest itself) assigns first_row_id, merged
// manifests correctly receive their value at list-write time from fresh metadata.
//
// Scenario:
//   - Writer B commits 3 rows to an empty v3 table with merge enabled.
//   - Writer A (merge-append) starts from the empty base and appends 2 rows.
//   - First attempt fails (409). Retry re-derives first-row-id from fresh
//     metadata (next-row-id=3 after B).
//   - Final metadata: next-row-id = 3 + (rows assigned in merged manifest).
func (s *OCCScenarioTestSuite) TestV3MergeAppendRowLineageAfterOCCRetry() {
	props := iceberg.Properties{
		table.CommitMinRetryWaitMsKey:      "0",
		table.CommitMaxRetryWaitMsKey:      "0",
		table.CommitTotalRetryTimeoutMsKey: "60000",
		table.CommitNumRetriesKey:          "2",
		table.ManifestMergeEnabledKey:      "true",
		table.ManifestMinMergeCountKey:     "2",
	}

	// Step 1: Writer B commits 3 rows.
	emptyTbl, catB := s.makeV3Table(0, props)
	rowB := s.makeArrowTableN(3)
	defer rowB.Release()

	txB := emptyTbl.NewTransaction()
	s.Require().NoError(txB.AppendTable(s.ctx, rowB, rowB.NumRows(), nil))
	_, err := txB.Commit(s.ctx)
	s.Require().NoError(err, "Writer B must commit")

	metaAfterB := catB.current
	s.Equal(int64(3), metaAfterB.NextRowID())

	// Step 2: Writer A with merge-append, starting from stale empty base.
	catA := &occScenarioCatalog{
		current:       metaAfterB,
		conflictsLeft: 1,
		location:      s.location,
	}

	writerATable := table.New(
		emptyTbl.Identifier(),
		emptyTbl.Metadata(),
		s.location+"/metadata/v1.metadata.json",
		func(_ context.Context) (iceio.IO, error) { return iceio.LocalFS{}, nil },
		catA,
	)

	rowA := s.makeArrowTableN(2)
	defer rowA.Release()

	txA := writerATable.NewTransaction()
	s.Require().NoError(txA.AppendTable(s.ctx, rowA, rowA.NumRows(), nil))
	_, err = txA.Commit(s.ctx)
	s.Require().NoError(err, "Writer A (merge-append) must succeed after retry")

	s.Equal(int32(2), catA.commitTableCalls.Load())

	// Step 3: Validate row lineage.
	finalMeta := catA.current
	finalSnap := finalMeta.CurrentSnapshot()
	s.Require().NotNil(finalSnap)
	s.Require().NotNil(finalSnap.FirstRowID)
	s.Require().NotNil(finalSnap.AddedRows)

	s.Equal(int64(3), *finalSnap.FirstRowID,
		"merge-append first-row-id must be rebased to fresh next-row-id=3")
	s.Greater(*finalSnap.AddedRows, int64(0),
		"merge-append must report positive added-rows")

	expectedNextRowID := *finalSnap.FirstRowID + *finalSnap.AddedRows
	s.Equal(expectedNextRowID, finalMeta.NextRowID(),
		"final next-row-id = first-row-id + added-rows (gap-free)")

	// Step 4: All data manifests in the final snapshot must have first_row_id set.
	fio := iceio.LocalFS{}
	manifests, err := finalSnap.Manifests(fio)
	s.Require().NoError(err)
	for i, mf := range manifests {
		if mf.ManifestContent() == iceberg.ManifestContentData {
			s.NotNil(mf.FirstRowID(),
				"v3 data manifest[%d] must have first_row_id assigned by list writer", i)
		}
	}
}

// TestReplaceFilesNoResurrectionAfterConflict is a regression test for the OCC
// retry-replay resurrection bug. When a replace/overwrite commit retries after
// a conflict, the rebuild must re-apply the file removal against the FRESH
// parent — not graft attempt-0's stale "own" manifests onto a fresh parent
// that still lists the removed files. Otherwise the removed files come back
// alive next to their replacement and their rows are duplicated.
//
// Scenario:
//   - Base: file0 = rows [1,2,3] committed.
//   - Concurrent peer: appends filePeer = row [4]; catalog head is file0+peer.
//   - Writer A (stale base = only file0) replaces file0 with a compacted file
//     holding the same rows [1,2,3]. First attempt conflicts; the retry rebases
//     onto the fresh head.
//
// Correct: live rows = {1,2,3,4} (compacted + peer), file0 dropped → 4 rows.
// Buggy:   file0 resurrected → rows [1,2,3] duplicated → 7 rows, 3 live files.
func (s *OCCScenarioTestSuite) TestReplaceFilesNoResurrectionAfterConflict() {
	props := iceberg.Properties{
		table.CommitMinRetryWaitMsKey:      "0",
		table.CommitMaxRetryWaitMsKey:      "0",
		table.CommitTotalRetryTimeoutMsKey: "60000",
		table.CommitNumRetriesKey:          "2",
		// Snapshot isolation lets the replace commit past the concurrent
		// append the same way compaction's rewrite semantics do; the bug we
		// are guarding lives in the retry rebuild, not the validator.
		table.WriteUpdateIsolationLevelKey: string(table.IsolationSnapshot),
	}

	localFS := func(_ context.Context) (iceio.IO, error) { return iceio.LocalFS{}, nil }

	// Step 1: base table with file0 = rows [1,2,3].
	baseTbl, cat := s.makeTable(0, props)
	arrowSc, err := table.SchemaToArrowSchema(baseTbl.Schema(), nil, false, false)
	s.Require().NoError(err)

	file0Path := s.location + "/data/file0.parquet"
	writeParquetFile(s.T(), file0Path, arrowSc,
		`[{"id":1,"val":"a"},{"id":2,"val":"b"},{"id":3,"val":"c"}]`)

	tx := baseTbl.NewTransaction()
	s.Require().NoError(tx.AddFiles(s.ctx, []string{file0Path}, nil, false))
	_, err = tx.Commit(s.ctx)
	s.Require().NoError(err)
	metaBase := cat.current

	// Step 2: a concurrent peer appends filePeer = row [4] onto metaBase.
	peerCat := &occScenarioCatalog{current: metaBase, location: s.location}
	peerTbl := table.New(baseTbl.Identifier(), metaBase,
		s.location+"/metadata/base.metadata.json", localFS, peerCat)

	peerPath := s.location + "/data/file_peer.parquet"
	writeParquetFile(s.T(), peerPath, arrowSc, `[{"id":4,"val":"d"}]`)

	ptx := peerTbl.NewTransaction()
	s.Require().NoError(ptx.AddFiles(s.ctx, []string{peerPath}, nil, false))
	_, err = ptx.Commit(s.ctx)
	s.Require().NoError(err)
	metaPeer := peerCat.current

	// Step 3: Writer A replaces file0 -> compacted, starting from the stale
	// metaBase; its catalog is at metaPeer and returns one 409 before success.
	catA := &occScenarioCatalog{current: metaPeer, conflictsLeft: 1, location: s.location}
	writerA := table.New(baseTbl.Identifier(), metaBase,
		s.location+"/metadata/base.metadata.json", localFS, catA)

	tasks, err := writerA.Scan().PlanFiles(s.ctx)
	s.Require().NoError(err)
	s.Require().Len(tasks, 1, "Writer A's stale base must see exactly file0")
	oldFile := tasks[0].File

	compactedPath := s.location + "/data/compacted.parquet"
	writeParquetFile(s.T(), compactedPath, arrowSc,
		`[{"id":1,"val":"a"},{"id":2,"val":"b"},{"id":3,"val":"c"}]`)
	newBuilder, err := iceberg.NewDataFileBuilder(
		*iceberg.UnpartitionedSpec, iceberg.EntryContentData,
		compactedPath, iceberg.ParquetFile, nil, nil, nil, 3, 512)
	s.Require().NoError(err)
	newFile := newBuilder.Build()

	txA := writerA.NewTransaction()
	s.Require().NoError(txA.ReplaceFiles(s.ctx,
		[]iceberg.DataFile{oldFile}, []iceberg.DataFile{newFile}, nil, nil))
	committedA, err := txA.Commit(s.ctx)
	s.Require().NoError(err, "Writer A replace must succeed after one conflict retry")
	s.Equal(int32(2), catA.commitTableCalls.Load(),
		"expected 2 commit attempts: 1 conflict + 1 success")

	// The replaced file0 must NOT be resurrected: exactly the compacted file
	// and the peer's file are live.
	finalSnap := catA.current.CurrentSnapshot()
	s.Require().NotNil(finalSnap)
	live := s.liveDataFilePaths(finalSnap)
	s.NotContains(live, oldFile.FilePath(), "replaced file0 must not be resurrected on retry")
	s.Len(live, 2, "exactly {compacted, peer} must be live after retry")

	// A full scan must return 4 rows, not 7.
	assertRowCount(s.T(), committedA, 4)
}

// liveDataFilePaths returns the paths of all non-deleted data-file entries
// referenced by snap's manifest list.
func (s *OCCScenarioTestSuite) liveDataFilePaths(snap *table.Snapshot) []string {
	fio := iceio.LocalFS{}
	manifests, err := snap.Manifests(fio)
	s.Require().NoError(err)

	var paths []string
	for _, mf := range manifests {
		if mf.ManifestContent() != iceberg.ManifestContentData {
			continue
		}
		for e, err := range mf.Entries(fio, false) {
			s.Require().NoError(err)
			if e.Status() == iceberg.EntryStatusDELETED {
				continue
			}
			paths = append(paths, e.DataFile().FilePath())
		}
	}

	return paths
}

func (s *OCCScenarioTestSuite) makeArrowTableN(n int) arrow.Table {
	mem := memory.DefaultAllocator
	sc := arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int64, Nullable: true},
		{Name: "val", Type: arrow.BinaryTypes.String, Nullable: true},
	}, nil)

	rows := make([]string, n)
	for i := range n {
		rows[i] = fmt.Sprintf(`{"id": %d, "val": "row-%d"}`, i+1, i+1)
	}
	jsonStr := "[" + strings.Join(rows, ",") + "]"

	tbl, err := array.TableFromJSON(mem, sc, []string{jsonStr})
	s.Require().NoError(err)

	return tbl
}

func (s *OCCScenarioTestSuite) makeV3Table(
	conflicts int,
	extraProps iceberg.Properties,
) (*table.Table, *occScenarioCatalog) {
	schema := iceberg.NewSchema(0,
		iceberg.NestedField{ID: 1, Name: "id", Type: iceberg.PrimitiveTypes.Int64},
		iceberg.NestedField{ID: 2, Name: "val", Type: iceberg.PrimitiveTypes.String},
	)

	props := iceberg.Properties{table.PropertyFormatVersion: "3"}
	for k, v := range extraProps {
		props[k] = v
	}

	meta, err := table.NewMetadata(schema, iceberg.UnpartitionedSpec,
		table.UnsortedSortOrder, s.location, props)
	s.Require().NoError(err)

	cat := &occScenarioCatalog{
		current:       meta,
		conflictsLeft: conflicts,
		location:      s.location,
	}

	tbl := table.New(
		[]string{"default", "occ_scenario_test"},
		meta,
		s.location+"/metadata/v1.metadata.json",
		func(_ context.Context) (iceio.IO, error) { return iceio.LocalFS{}, nil },
		cat,
	)

	return tbl, cat
}
