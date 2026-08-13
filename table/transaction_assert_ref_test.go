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

	"github.com/DataDog/iceberg-go"
	iceio "github.com/DataDog/iceberg-go/io"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

var assertRefRetryProps = iceberg.Properties{
	CommitNumRetriesKey:     "2",
	CommitMinRetryWaitMsKey: "1",
	CommitMaxRetryWaitMsKey: "2",
}

// newAssertRefTestTable builds a writer-side table over the given base
// metadata, backed by a headTrackingCatalog seeded with catalogMeta —
// the catalog state at commit time, which may differ from the writer's
// view to simulate an interleaved commit.
func newAssertRefTestTable(t *testing.T, base, catalogMeta Metadata) (*Table, *headTrackingCatalog) {
	t.Helper()

	cat := &headTrackingCatalog{metadata: catalogMeta}
	tbl := New(Identifier{"db", "assert-ref-test"}, base, "metadata.json",
		func(context.Context) (iceio.IO, error) { return iceio.LocalFS{}, nil }, cat)

	return tbl, cat
}

// newProducerAssertRefTable builds an empty v2 table over a temp dir so
// subtests combining an explicit ref requirement with a producer can
// run a real fast-append commit.
func newProducerAssertRefTable(t *testing.T) (*Table, *headTrackingCatalog, string) {
	t.Helper()

	dir := t.TempDir()
	schema := iceberg.NewSchema(0,
		iceberg.NestedField{ID: 1, Name: "id", Type: iceberg.PrimitiveTypes.Int64, Required: true},
	)
	props := iceberg.Properties{PropertyFormatVersion: "2"}
	for k, v := range assertRefRetryProps {
		props[k] = v
	}
	base, err := NewMetadata(schema, iceberg.UnpartitionedSpec, UnsortedSortOrder, dir, props)
	require.NoError(t, err)
	tbl, cat := newAssertRefTestTable(t, base, base)

	return tbl, cat, dir
}

// commitPropsWithAssertedRef runs the motivating flow on tx: stage a
// bookkeeping property, require branch unchanged, and commit, returning
// the commit error.
func commitPropsWithAssertedRef(t *testing.T, tx *Transaction, branch string) error {
	t.Helper()

	require.NoError(t, tx.SetProperties(iceberg.Properties{"offsets": "42"}))
	require.NoError(t, tx.AssertRefSnapshotID(branch))
	_, err := tx.Commit(t.Context())

	return err
}

// Why: a transaction carrying only metadata updates (e.g. SetProperties
// used for exactly-once bookkeeping) has its implicit branch assertion
// rewritten to the fresh head between retries and replayed, so the
// writer cannot detect that the branch changed between its read and the
// commit; callers need an explicit requirement — and the retry loop's
// refresh-and-replay must not rewrite it to the new head, which would
// silently void the compare-and-swap.
// Condition: a properties-only transaction with AssertRefSnapshotID,
// committed against a catalog whose branch head did or did not change
// in between.
// Assertion: the commit succeeds when the branch is unchanged and fails
// with ErrCommitFailed — with the properties left uncommitted — when it
// changed.
func TestTransactionAssertRefSnapshotID(t *testing.T) {
	head := int64(100)

	t.Run("succeeds when the branch is unchanged", func(t *testing.T) {
		base := newConflictTestMetadataWithProps(t, &head, assertRefRetryProps)
		tbl, cat := newAssertRefTestTable(t, base, base)

		require.NoError(t, commitPropsWithAssertedRef(t, tbl.NewTransaction(), MainBranch))
		assert.Equal(t, int32(1), cat.attempts.Load())
		assert.Equal(t, "42", cat.metadata.Properties()["offsets"])
	})

	t.Run("fails when a concurrent commit changed the branch", func(t *testing.T) {
		base := newConflictTestMetadataWithProps(t, &head, assertRefRetryProps)
		changed := graftSnapshotOnto(t, base, MainBranch, 200)
		tbl, cat := newAssertRefTestTable(t, base, changed)

		err := commitPropsWithAssertedRef(t, tbl.NewTransaction(), MainBranch)
		assert.ErrorIs(t, err, ErrCommitFailed)
		assert.NotContains(t, cat.metadata.Properties(), "offsets",
			"a failed compare-and-swap must not apply the properties")
		assert.Equal(t, int32(1), cat.attempts.Load(),
			"the retry's pre-flight must fail the pinned assertion fast instead of rewriting it or re-submitting it")
	})

	t.Run("requires branch absence when the branch does not exist", func(t *testing.T) {
		base := newConflictTestMetadataWithProps(t, nil, assertRefRetryProps)

		// Catalog state: a peer created the branch after the writer's read.
		builder, err := MetadataBuilderFromBase(base, "")
		require.NoError(t, err)
		require.NoError(t, builder.AddSnapshot(&Snapshot{
			SnapshotID:     head,
			SequenceNumber: 1,
			TimestampMs:    base.LastUpdatedMillis() + 1,
			Summary:        &Summary{Operation: OpAppend},
		}))
		require.NoError(t, builder.SetSnapshotRef(MainBranch, head, BranchRef))
		created, err := builder.Build()
		require.NoError(t, err)

		tbl, cat := newAssertRefTestTable(t, base, created)

		err = commitPropsWithAssertedRef(t, tbl.NewTransaction(), MainBranch)
		assert.ErrorIs(t, err, ErrCommitFailed,
			"a branch created concurrently must fail a requirement asserting its absence")
		assert.NotContains(t, cat.metadata.Properties(), "offsets")
		assert.Equal(t, int32(1), cat.attempts.Load())
	})

	t.Run("empty branch defaults to the transaction's target branch", func(t *testing.T) {
		base := newConflictTestMetadataWithProps(t, &head, assertRefRetryProps)
		changed := graftSnapshotOnto(t, base, MainBranch, 200)
		tbl, _ := newAssertRefTestTable(t, base, changed)

		// NewTransaction targets main, so the empty-branch requirement
		// asserts main.
		err := commitPropsWithAssertedRef(t, tbl.NewTransaction(), "")
		assert.ErrorIs(t, err, ErrCommitFailed)
	})

	t.Run("empty branch on a branch transaction asserts that branch", func(t *testing.T) {
		base := newConflictTestMetadataWithProps(t, &head, assertRefRetryProps)

		// The writer's base has a "feature" branch alongside main.
		builder, err := MetadataBuilderFromBase(base, "")
		require.NoError(t, err)
		require.NoError(t, builder.SetSnapshotRef("feature", head, BranchRef))
		withFeature, err := builder.Build()
		require.NoError(t, err)

		// Catalog state: feature changed, main unchanged — only a
		// requirement on feature can catch it.
		changed := graftSnapshotOnto(t, withFeature, "feature", 200)
		tbl, cat := newAssertRefTestTable(t, withFeature, changed)

		err = commitPropsWithAssertedRef(t, tbl.NewTransactionOnBranch("feature"), "")
		assert.ErrorIs(t, err, ErrCommitFailed)
		assert.NotContains(t, cat.metadata.Properties(), "offsets")
	})
}

// Why: ref assertions are deduplicated by (type, ref), so assertions
// for distinct branches must both be enforced, an explicit requirement
// and a producer assertion requiring the same snapshot must collapse to
// one, and two assertions for the same ref requiring different snapshot
// ids must be rejected as conflicting rather than silently collapsed.
// Condition/Assertion: per subtest.
func TestTransactionRequirementDedup(t *testing.T) {
	head := int64(100)

	t.Run("independent branches are both enforced", func(t *testing.T) {
		base := newConflictTestMetadataWithProps(t, &head, assertRefRetryProps)

		// Catalog state: main unmoved, but a peer created the "audit"
		// branch in between — only the audit assertion can catch it.
		builder, err := MetadataBuilderFromBase(base, "")
		require.NoError(t, err)
		require.NoError(t, builder.SetSnapshotRef("audit", head, BranchRef))
		withAudit, err := builder.Build()
		require.NoError(t, err)

		tbl, cat := newAssertRefTestTable(t, base, withAudit)
		tx := tbl.NewTransaction()
		require.NoError(t, tx.SetProperties(iceberg.Properties{"offsets": "42"}))
		require.NoError(t, tx.AssertRefSnapshotID(MainBranch))
		require.NoError(t, tx.AssertRefSnapshotID("audit")) // requires absence
		assert.Len(t, refAssertions(tx), 2,
			"assertions for distinct branches must both be kept")

		_, err = tx.Commit(t.Context())
		assert.ErrorIs(t, err, ErrCommitFailed)
		assert.NotContains(t, cat.metadata.Properties(), "offsets")
	})

	t.Run("same ref with the same required id dedupes to one", func(t *testing.T) {
		base := newConflictTestMetadataWithProps(t, &head, assertRefRetryProps)
		tbl, _ := newAssertRefTestTable(t, base, base)

		tx := tbl.NewTransaction()
		require.NoError(t, tx.AssertRefSnapshotID(MainBranch))
		// A producer-built assertion for the same branch requires the
		// same base head; it must coexist with the explicit requirement
		// as one assertion.
		require.NoError(t, tx.apply(nil, []Requirement{AssertRefSnapshotID(MainBranch, &head)}))
		assert.Len(t, refAssertions(tx), 1)
	})

	t.Run("same ref with conflicting ids errors", func(t *testing.T) {
		base := newConflictTestMetadataWithProps(t, &head, assertRefRetryProps)
		tbl, _ := newAssertRefTestTable(t, base, base)

		tx := tbl.NewTransaction()
		require.NoError(t, tx.AssertRefSnapshotID(MainBranch))

		other := int64(200)
		err := tx.apply(nil, []Requirement{AssertRefSnapshotID(MainBranch, &other)})
		require.Error(t, err)
		assert.Contains(t, err.Error(), "conflicting snapshot-id assertions")

		err = tx.apply(nil, []Requirement{AssertRefSnapshotID(MainBranch, nil)})
		require.Error(t, err, "required id vs required absence must also conflict")
		assert.Contains(t, err.Error(), "conflicting snapshot-id assertions")
	})

	// The explicit requirement and a fast-append producer assert the
	// same base state (main's absence on this empty table), so they must
	// collapse to one requirement no matter which registers first.
	t.Run("explicit requirement coexists with a producer commit", func(t *testing.T) {
		tbl, cat, dir := newProducerAssertRefTable(t)

		tx := tbl.NewTransaction()
		require.NoError(t, tx.AssertRefSnapshotID(MainBranch))
		require.NoError(t, tx.AddDataFiles(t.Context(),
			[]iceberg.DataFile{newTestDataFile(t, *iceberg.UnpartitionedSpec, dir+"/data/f1.parquet", nil)}, nil))
		assert.Len(t, refAssertions(tx), 1)

		_, err := tx.Commit(t.Context())
		require.NoError(t, err)
		require.NotNil(t, cat.metadata.CurrentSnapshot(),
			"the producer commit must still create the branch")
	})

	t.Run("explicit requirement after a producer commit asserts the same base state", func(t *testing.T) {
		tbl, cat, dir := newProducerAssertRefTable(t)

		tx := tbl.NewTransaction()
		require.NoError(t, tx.AddDataFiles(t.Context(),
			[]iceberg.DataFile{newTestDataFile(t, *iceberg.UnpartitionedSpec, dir+"/data/f1.parquet", nil)}, nil))
		require.NoError(t, tx.AssertRefSnapshotID(MainBranch))
		assert.Len(t, refAssertions(tx), 1,
			"a requirement registered after the producer must dedupe, not assert the staged snapshot")

		_, err := tx.Commit(t.Context())
		require.NoError(t, err)
		require.NotNil(t, cat.metadata.CurrentSnapshot())
	})
}
