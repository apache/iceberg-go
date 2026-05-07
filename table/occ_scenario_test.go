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
