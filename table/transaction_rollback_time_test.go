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
	"testing"

	"github.com/apache/iceberg-go"
	"github.com/stretchr/testify/require"
)

func TestRollbackToTimeStrictlyBeforeTimestamp(t *testing.T) {
	base := newConflictTestMetadataWithChain(t, []int64{10, 20, 30})
	first := base.SnapshotByID(10)
	middle := base.SnapshotByID(20)
	latest := base.SnapshotByID(30)
	require.NotNil(t, first)
	require.NotNil(t, middle)
	require.NotNil(t, latest)

	tests := []struct {
		name         string
		timestamp    int64
		wantSnapshot int64
		wantError    bool
	}{
		{name: "equal to latest chooses parent", timestamp: latest.TimestampMs, wantSnapshot: 20},
		{name: "equal to middle chooses grandparent", timestamp: middle.TimestampMs, wantSnapshot: 10},
		{name: "after latest is a no-op", timestamp: latest.TimestampMs + 1, wantSnapshot: 30},
		{name: "equal to first has no older ancestor", timestamp: first.TimestampMs, wantError: true},
		{name: "before first has no older ancestor", timestamp: first.TimestampMs - 1, wantError: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tbl, _ := newAssertRefTestTable(t, base, base)
			tx := tbl.NewTransaction()
			err := tx.RollbackToTime(tt.timestamp)
			if tt.wantError {
				require.ErrorIs(t, err, iceberg.ErrInvalidArgument)
				require.Equal(t, int64(30), tx.meta.refs[MainBranch].SnapshotID)
				require.Empty(t, tx.meta.updates)
				return
			}

			require.NoError(t, err)
			require.Equal(t, tt.wantSnapshot, tx.meta.refs[MainBranch].SnapshotID)
			if tt.wantSnapshot == 30 {
				require.Empty(t, tx.meta.updates)
			} else {
				requireContainsRefSnapshotRequirement(t, tx.reqs, MainBranch, new(int64(30)))
			}
		})
	}
}

func TestRollbackToTimeOnBranchPreservesOtherRefsAndRetention(t *testing.T) {
	base := newConflictTestMetadataWithChain(t, []int64{10, 20, 30})
	builder, err := MetadataBuilderFromBase(base, "")
	require.NoError(t, err)
	require.NoError(t, builder.SetSnapshotRef("feature", 20, BranchRef,
		WithMinSnapshotsToKeep(5), WithMaxSnapshotAgeMs(1000), WithMaxRefAgeMs(2000)))
	require.NoError(t, builder.SetSnapshotRef("audit", 30, TagRef, WithMaxRefAgeMs(3000)))
	withRefs, err := builder.Build()
	require.NoError(t, err)

	tbl, cat := newAssertRefTestTable(t, withRefs, withRefs)
	tx, err := tbl.NewTransactionOnBranch("feature")
	require.NoError(t, err)
	require.NoError(t, tx.RollbackToTime(withRefs.SnapshotByID(20).TimestampMs))

	feature := tx.meta.refs["feature"]
	require.Equal(t, int64(10), feature.SnapshotID)
	require.Equal(t, 5, *feature.MinSnapshotsToKeep)
	require.Equal(t, int64(1000), *feature.MaxSnapshotAgeMs)
	require.Equal(t, int64(2000), *feature.MaxRefAgeMs)
	require.Equal(t, int64(30), tx.meta.refs[MainBranch].SnapshotID)
	require.Equal(t, int64(30), tx.meta.refs["audit"].SnapshotID)
	requireContainsRefSnapshotRequirement(t, tx.reqs, "feature", new(int64(20)))

	_, err = tx.Commit(t.Context())
	require.NoError(t, err)
	require.Equal(t, int64(10), cat.metadata.SnapshotByName("feature").SnapshotID)
	require.Equal(t, int64(30), cat.metadata.CurrentSnapshot().SnapshotID)
}

func TestRollbackToTimeIgnoresOrphanSnapshots(t *testing.T) {
	base := newConflictTestMetadataWithChain(t, []int64{10, 20, 30})
	builder, err := MetadataBuilderFromBase(base, "")
	require.NoError(t, err)
	orphan := Snapshot{
		SnapshotID:     40,
		SequenceNumber: 4,
		TimestampMs:    base.LastUpdatedMillis() + 1,
		Summary:        &Summary{Operation: OpAppend},
	}
	require.NoError(t, builder.AddSnapshot(&orphan))
	withOrphan, err := builder.Build()
	require.NoError(t, err)

	tbl, _ := newAssertRefTestTable(t, withOrphan, withOrphan)
	tx := tbl.NewTransaction()
	require.NoError(t, tx.RollbackToTime(orphan.TimestampMs+1))
	require.Equal(t, int64(30), tx.meta.refs[MainBranch].SnapshotID)
	require.Empty(t, tx.meta.updates)
}

func TestRollbackToTimePinsBranchHeadAgainstConcurrentMove(t *testing.T) {
	base := newConflictTestMetadataWithChain(t, []int64{10, 20, 30})
	concurrent := graftSnapshotOnto(t, base, MainBranch, 40)
	tbl, cat := newAssertRefTestTable(t, base, concurrent)
	tx := tbl.NewTransaction()
	require.NoError(t, tx.RollbackToTime(base.SnapshotByID(30).TimestampMs))

	_, err := tx.Commit(t.Context())
	require.ErrorIs(t, err, ErrCommitFailed)
	require.Equal(t, int64(40), cat.metadata.CurrentSnapshot().SnapshotID)
	require.Equal(t, int32(1), cat.attempts.Load())
}

func TestRollbackToTimeRejectsMissingAndTagBranches(t *testing.T) {
	base := newConflictTestMetadataWithChain(t, []int64{10, 20})
	tbl, _ := newAssertRefTestTable(t, base, base)

	tx := tbl.NewTransaction()
	tx.branch = "missing"
	require.ErrorIs(t, tx.RollbackToTime(base.SnapshotByID(20).TimestampMs), iceberg.ErrInvalidArgument)

	builder, err := MetadataBuilderFromBase(base, "")
	require.NoError(t, err)
	require.NoError(t, builder.SetSnapshotRef("audit", 20, TagRef))
	withTag, err := builder.Build()
	require.NoError(t, err)
	tbl, _ = newAssertRefTestTable(t, withTag, withTag)
	tx = tbl.NewTransaction()
	tx.branch = "audit"
	require.ErrorIs(t, tx.RollbackToTime(base.SnapshotByID(20).TimestampMs), iceberg.ErrInvalidArgument)
}

func TestRollbackToTimeRejectsNilTransaction(t *testing.T) {
	var tx *Transaction
	require.ErrorIs(t, tx.RollbackToTime(0), ErrInvalidMetadata)
}
