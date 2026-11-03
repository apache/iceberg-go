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
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package table

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestCherryPickSnapshotRejectsMissingSnapshot(t *testing.T) {
	txn := newTransactionWithSnapshotRefs(t)
	err := txn.CherryPickSnapshot(t.Context(), 99999, nil)
	require.ErrorIs(t, err, ErrSnapshotNotFound)
	require.NotContains(t, err.Error(), "snapshot-loading-mode=refs")
}

func TestCherryPickSnapshotRejectsNonAppendOperation(t *testing.T) {
	for _, op := range []Operation{OpDelete, OpOverwrite, OpReplace} {
		t.Run(string(op), func(t *testing.T) {
			txn := newTransactionWithSnapshotRefs(t)
			// The source is a direct child of main in the shared fixture,
			// which Java allows to fast-forward for any operation. Move main
			// onto a divergent lineage to exercise unsupported replay.
			source := txn.meta.snapshotList[1]
			require.NoError(t, txn.meta.AddSnapshot(&Snapshot{
				SnapshotID:       30,
				ParentSnapshotID: new(int64(10)),
				SequenceNumber:   3,
				ManifestList:     "mem://default/table-location/metadata/manifest-30.avro",
				Summary:          &Summary{Operation: OpAppend},
				TimestampMs:      source.TimestampMs + 1,
			}))
			require.NoError(t, txn.meta.SetSnapshotRef(MainBranch, 30, BranchRef))
			source.Summary.Operation = op
			err := txn.CherryPickSnapshot(t.Context(), 20, nil)
			require.ErrorIs(t, err, ErrInvalidOperation)
		})
	}
}

// REST snapshot-loading-mode=refs can provide both ref heads while omitting
// the snapshots between them. An incomplete walk cannot prove that the
// source snapshot or its WAP ID was never published on the target branch.
func TestCherryPickSnapshotRejectsIncompleteTargetAncestry(t *testing.T) {
	txn := newTransactionWithSnapshotRefs(t)
	txn.tbl.snapshotHistoryRefsOnly = true
	source := txn.meta.snapshotList[1]

	missingParent := int64(999)
	require.NoError(t, txn.meta.AddSnapshot(&Snapshot{
		SnapshotID:       30,
		ParentSnapshotID: &missingParent,
		SequenceNumber:   3,
		ManifestList:     "mem://default/table-location/metadata/manifest-30.avro",
		Summary:          &Summary{Operation: OpAppend},
		TimestampMs:      source.TimestampMs + 1,
	}))
	require.NoError(t, txn.meta.SetSnapshotRef(MainBranch, 30, BranchRef))

	err := txn.CherryPickSnapshot(t.Context(), source.SnapshotID, nil)
	require.ErrorIs(t, err, ErrInvalidOperation)
	require.ErrorContains(t, err, "snapshot-loading-mode=refs")
	require.Empty(t, txn.pinnedRefs, "rejected cherry-pick must not stage ref fences")
}

// Expiration legitimately truncates the oldest lineage even when all available
// snapshots were loaded. Unlike REST refs mode, this is not a reason to reject.
func TestCherryPickSnapshotAllowsExpiredTargetAncestry(t *testing.T) {
	txn := newTransactionWithSnapshotRefs(t)
	source := txn.meta.snapshotList[1]
	missingParent := int64(999)
	require.NoError(t, txn.meta.AddSnapshot(&Snapshot{
		SnapshotID:       30,
		ParentSnapshotID: &missingParent,
		SequenceNumber:   3,
		ManifestList:     "mem://default/table-location/metadata/manifest-30.avro",
		Summary:          &Summary{Operation: OpAppend},
		TimestampMs:      source.TimestampMs + 1,
	}))
	require.NoError(t, txn.meta.SetSnapshotRef(MainBranch, 30, BranchRef))
	source.Summary.Operation = OpDelete

	err := txn.CherryPickSnapshot(t.Context(), source.SnapshotID, nil)
	require.ErrorIs(t, err, ErrInvalidOperation)
	require.ErrorContains(t, err, "not an append")
	require.NotContains(t, err.Error(), "incomplete target ancestry")
}

func TestCherryPickSnapshotChecksContextBeforeManifestIO(t *testing.T) {
	txn := newTransactionWithSnapshotRefs(t)
	ctx, cancel := context.WithCancel(t.Context())
	cancel()

	err := txn.CherryPickSnapshot(ctx, 20, nil)
	require.True(t, errors.Is(err, context.Canceled))
}
