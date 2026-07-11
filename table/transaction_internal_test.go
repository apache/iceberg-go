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
	"time"

	"github.com/apache/iceberg-go"
	iceio "github.com/apache/iceberg-go/io"
	"github.com/stretchr/testify/require"
)

func TestTransactionApplyKeepsDistinctRequirementsOfSameType(t *testing.T) {
	txn := newTransactionWithSnapshotRefs(t)

	mainSnapshotID := int64(10)
	featureSnapshotID := int64(20)

	err := txn.apply(nil, []Requirement{
		AssertRefSnapshotID(MainBranch, &mainSnapshotID),
		AssertRefSnapshotID("feature", &featureSnapshotID),
	})
	require.NoError(t, err)

	require.Len(t, txn.reqs, 2)
	requireContainsRefSnapshotRequirement(t, txn.reqs, MainBranch, &mainSnapshotID)
	requireContainsRefSnapshotRequirement(t, txn.reqs, "feature", &featureSnapshotID)
}

func TestExpireSnapshotsWithOlderThanDoesNotExpireSnapshotRefs(t *testing.T) {
	txn := newTransactionWithSnapshotRefs(t)
	now := time.Now().UnixMilli()
	oldTimestamp := time.Now().Add(-8 * 24 * time.Hour).UnixMilli()

	txn.meta.snapshotList[0].TimestampMs = oldTimestamp
	txn.meta.snapshotList[1].TimestampMs = now
	require.NoError(t, txn.meta.SetSnapshotRef(MainBranch, 20, BranchRef))
	require.NoError(t, txn.meta.SetSnapshotRef("old-branch", 10, BranchRef))
	require.NoError(t, txn.meta.SetSnapshotRef("old-tag", 10, TagRef))
	txn.meta.lastUpdatedMS = now

	require.NoError(t, txn.ExpireSnapshots(WithOlderThan(7*24*time.Hour)))

	_, branchExists := txn.meta.refs["old-branch"]
	_, tagExists := txn.meta.refs["old-tag"]
	require.True(t, branchExists, "WithOlderThan must not expire a branch without max-ref-age-ms")
	require.True(t, tagExists, "WithOlderThan must not expire a tag without max-ref-age-ms")
}

func TestTransactionApplyDedupesEquivalentRequirementsWithinAndAcrossCalls(t *testing.T) {
	txn := newTransactionWithSnapshotRefs(t)

	mainSnapshotID := int64(10)
	first := AssertRefSnapshotID(MainBranch, &mainSnapshotID)
	second := AssertRefSnapshotID(MainBranch, &mainSnapshotID)

	err := txn.apply(nil, []Requirement{first, second})
	require.NoError(t, err)
	require.Len(t, txn.reqs, 1)
	requireContainsRefSnapshotRequirement(t, txn.reqs, MainBranch, &mainSnapshotID)

	err = txn.apply(nil, []Requirement{AssertRefSnapshotID(MainBranch, &mainSnapshotID)})
	require.NoError(t, err)
	require.Len(t, txn.reqs, 1)
	requireContainsRefSnapshotRequirement(t, txn.reqs, MainBranch, &mainSnapshotID)
}

func TestNewTransactionOnBranchWithErrorReturnsTransactionInitError(t *testing.T) {
	baseMeta, err := NewMetadata(simpleSchema(), iceberg.UnpartitionedSpec, UnsortedSortOrder, "table-location", nil)
	require.NoError(t, err, "new metadata")

	txn, err := New(Identifier{"db", "broken"}, brokenMetadata{
		Metadata: baseMeta,
	}, "metadata.json", func(context.Context) (iceio.IO, error) {
		return nil, nil
	}, nil).NewTransactionOnBranchWithError(MainBranch)
	require.Error(t, err, "expected metadata builder initialization to fail")
	require.ErrorContains(t, err, "current schema is missing")
	require.ErrorIs(t, err, ErrInvalidMetadata)
	require.Nil(t, txn)
}

func TestNewTransactionOnBranchKeepsLegacySignatureAndFailsOnUse(t *testing.T) {
	baseMeta, err := NewMetadata(simpleSchema(), iceberg.UnpartitionedSpec, UnsortedSortOrder, "table-location", nil)
	require.NoError(t, err, "new metadata")

	txn := New(Identifier{"db", "broken"}, brokenMetadata{
		Metadata: baseMeta,
	}, "metadata.json", func(context.Context) (iceio.IO, error) {
		return nil, nil
	}, nil).NewTransaction()

	t.Run("set properties returns init error", func(t *testing.T) {
		err := txn.SetProperties(iceberg.Properties{"k": "v"})
		require.ErrorContains(t, err, "current schema is missing")
	})

	t.Run("update schema no longer panics", func(t *testing.T) {
		var err error
		require.NotPanics(t, func() {
			err = txn.UpdateSchema(true, false).
				AddColumn([]string{"new_col"}, iceberg.PrimitiveTypes.String, "", false, nil).
				Commit()
		})
		require.ErrorContains(t, err, "current schema is missing")
	})

	t.Run("update spec returns init error", func(t *testing.T) {
		err := txn.UpdateSpec(true).AddIdentity("id").Commit()
		require.ErrorContains(t, err, "current schema is missing")
	})

	t.Run("table commit returns init error", func(t *testing.T) {
		_, err := txn.TableCommit()
		require.ErrorContains(t, err, "current schema is missing")
	})

	t.Run("write equality deletes returns init error", func(t *testing.T) {
		_, err := txn.WriteEqualityDeletes(context.Background(), []int{1}, nil)
		require.ErrorContains(t, err, "current schema is missing")
	})

	t.Run("commit returns init error", func(t *testing.T) {
		_, err := txn.Commit(context.Background())
		require.ErrorIs(t, err, ErrInvalidMetadata)
		require.ErrorContains(t, err, "current schema is missing")
	})

	t.Run("row delta commit returns init error", func(t *testing.T) {
		rowFile, dataErr := iceberg.NewDataFileBuilder(
			*iceberg.UnpartitionedSpec,
			iceberg.EntryContentData,
			"file://data.parquet",
			iceberg.ParquetFile,
			nil,
			nil,
			nil,
			10,
			10,
		)
		require.NoError(t, dataErr, "new data file builder")

		rd := txn.NewRowDelta(nil).AddRows(rowFile.Build())
		err := rd.Commit(context.Background())
		require.ErrorContains(t, err, "current schema is missing")
	})
}

func TestTransactionEnsureInitializedNilReceiver(t *testing.T) {
	var txn *Transaction

	err := txn.ensureInitialized()
	require.ErrorIs(t, err, ErrInvalidMetadata)
	require.ErrorContains(t, err, "transaction is nil")
}

type brokenMetadata struct {
	Metadata
}

func (m brokenMetadata) CurrentSchema() *iceberg.Schema {
	return nil
}

func TestTransactionApplyKeepsMetadataUnchangedOnUpdateFailure(t *testing.T) {
	txn, _ := createTestTransactionWithMemIO(t, *iceberg.UnpartitionedSpec)
	baseMeta, err := txn.meta.Build()
	require.NoError(t, err)

	updates := []Update{
		NewUpgradeFormatVersionUpdate(baseMeta.Version() + 1),
		NewSetCurrentSchemaUpdate(9999),
	}

	err = txn.apply(updates, nil)
	require.Error(t, err)

	postMeta, err := txn.meta.Build()
	require.NoError(t, err)
	require.True(t, baseMeta.Equals(postMeta))
	require.Len(t, txn.reqs, 0)
}

// TestTransactionApplyDedupesSameRefAssertionsNewTable covers two appends in a
// single new-table transaction: the first asserts main must not exist, and the
// second (after the builder has created main) asserts main == the new snapshot.
// Only the first main == nil assertion, which reflects the pre-transaction base
// state, must be retained.
func TestTransactionApplyDedupesSameRefAssertionsNewTable(t *testing.T) {
	txn, _ := createTestTransactionWithMemIO(t, *iceberg.UnpartitionedSpec)

	// First append: main does not exist yet in the pre-transaction metadata.
	err := txn.apply(nil, []Requirement{AssertRefSnapshotID(MainBranch, nil)})
	require.NoError(t, err)
	require.Len(t, txn.reqs, 1)
	requireContainsRefSnapshotRequirement(t, txn.reqs, MainBranch, nil)

	// Simulate the first append creating main -> 10 in the transaction builder.
	require.NoError(t, txn.meta.AddSnapshot(&Snapshot{
		SnapshotID:     10,
		SequenceNumber: 1,
		ManifestList:   "mem://default/table-location/metadata/manifest-10.avro",
		Summary:        &Summary{Operation: OpAppend},
		TimestampMs:    time.Now().UnixMilli(),
	}))
	require.NoError(t, txn.meta.SetSnapshotRef(MainBranch, 10, BranchRef))

	// Second append asserts main == 10; it must dedupe against the first
	// assertion for main rather than adding a contradictory base-state check.
	newHead := int64(10)
	err = txn.apply(nil, []Requirement{AssertRefSnapshotID(MainBranch, &newHead)})
	require.NoError(t, err)
	require.Len(t, txn.reqs, 1)
	requireContainsRefSnapshotRequirement(t, txn.reqs, MainBranch, nil)
}

// TestTransactionApplyDedupesSameRefAssertionsExistingTable covers two appends
// on an existing table: the first asserts the original base head, and the second
// (after the builder has advanced main) asserts the new head. Only the original
// base-head assertion must be retained.
func TestTransactionApplyDedupesSameRefAssertionsExistingTable(t *testing.T) {
	txn := newTransactionWithSnapshotRefs(t) // main -> 10, feature -> 20

	base := int64(10)
	err := txn.apply(nil, []Requirement{AssertRefSnapshotID(MainBranch, &base)})
	require.NoError(t, err)
	require.Len(t, txn.reqs, 1)
	requireContainsRefSnapshotRequirement(t, txn.reqs, MainBranch, &base)

	// Simulate the first append advancing main -> 30.
	require.NoError(t, txn.meta.AddSnapshot(&Snapshot{
		SnapshotID:       30,
		ParentSnapshotID: transactionTestPtr(base),
		SequenceNumber:   3,
		ManifestList:     "mem://default/table-location/metadata/manifest-30.avro",
		Summary:          &Summary{Operation: OpAppend},
		TimestampMs:      time.Now().UnixMilli(),
	}))
	require.NoError(t, txn.meta.SetSnapshotRef(MainBranch, 30, BranchRef))

	// Second append asserts main == 30; it must dedupe against the original
	// base-head assertion, which is the only one kept.
	newHead := int64(30)
	err = txn.apply(nil, []Requirement{AssertRefSnapshotID(MainBranch, &newHead)})
	require.NoError(t, err)
	require.Len(t, txn.reqs, 1)
	requireContainsRefSnapshotRequirement(t, txn.reqs, MainBranch, &base)
}

// TestTransactionApplyKeepsRefAssertionsForDistinctRefs confirms that dedupe by
// ref name still lets assertions for different refs both survive, even when they
// assert the same snapshot id.
func TestTransactionApplyKeepsRefAssertionsForDistinctRefs(t *testing.T) {
	txn := newTransactionWithSnapshotRefs(t) // main -> 10, feature -> 20

	base := int64(10)
	err := txn.apply(nil, []Requirement{
		AssertRefSnapshotID(MainBranch, &base),
		AssertRefSnapshotID("feature", transactionTestPtr(int64(20))),
	})
	require.NoError(t, err)
	require.Len(t, txn.reqs, 2)
	requireContainsRefSnapshotRequirement(t, txn.reqs, MainBranch, &base)
	requireContainsRefSnapshotRequirement(t, txn.reqs, "feature", transactionTestPtr(int64(20)))
}

// TestTransactionApplyDedupesIdenticalNonRefRequirements confirms that non-ref
// requirements keep the canonical JSON dedupe key: identical requirements
// collapse while distinct ones survive.
func TestTransactionApplyDedupesIdenticalNonRefRequirements(t *testing.T) {
	txn, _ := createTestTransactionWithMemIO(t, *iceberg.UnpartitionedSpec)

	err := txn.apply(nil, []Requirement{
		AssertCurrentSchemaID(0),
		AssertCurrentSchemaID(0),
		AssertDefaultSpecID(0),
	})
	require.NoError(t, err)
	require.Len(t, txn.reqs, 2)

	var schemaAsserts, specAsserts int
	for _, r := range txn.reqs {
		switch r.GetType() {
		case reqAssertCurrentSchemaID:
			schemaAsserts++
		case reqAssertDefaultSpecID:
			specAsserts++
		}
	}
	require.Equal(t, 1, schemaAsserts)
	require.Equal(t, 1, specAsserts)
}

func newTransactionWithSnapshotRefs(t *testing.T) *Transaction {
	t.Helper()

	txn, _ := createTestTransactionWithMemIO(t, *iceberg.UnpartitionedSpec)
	now := time.Now().UnixMilli()

	require.NoError(t, txn.meta.AddSnapshot(&Snapshot{
		SnapshotID:     10,
		SequenceNumber: 1,
		ManifestList:   "mem://default/table-location/metadata/manifest-10.avro",
		Summary:        &Summary{Operation: OpAppend},
		TimestampMs:    now,
	}))

	require.NoError(t, txn.meta.AddSnapshot(&Snapshot{
		SnapshotID:       20,
		ParentSnapshotID: transactionTestPtr(int64(10)),
		SequenceNumber:   2,
		ManifestList:     "mem://default/table-location/metadata/manifest-20.avro",
		Summary:          &Summary{Operation: OpAppend},
		TimestampMs:      now + 1,
	}))

	require.NoError(t, txn.meta.SetSnapshotRef(MainBranch, 10, BranchRef))
	require.NoError(t, txn.meta.SetSnapshotRef("feature", 20, BranchRef))

	return txn
}

func requireContainsRefSnapshotRequirement(t *testing.T, requirements []Requirement, ref string, snapshotID *int64) {
	t.Helper()

	for _, requirement := range requirements {
		actual, ok := requirement.(*assertRefSnapshotID)
		if ok && actual.Ref == ref && transactionTestInt64PtrEqual(actual.SnapshotID, snapshotID) {
			return
		}
	}

	t.Fatalf("expected assertRefSnapshotID requirement for ref %q and snapshot id %v not found", ref, snapshotID)
}

func transactionTestPtr[T any](v T) *T {
	return &v
}

func transactionTestInt64PtrEqual(left, right *int64) bool {
	if left == nil || right == nil {
		return left == right
	}

	return *left == *right
}
