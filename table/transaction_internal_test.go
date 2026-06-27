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
	"time"

	"github.com/apache/iceberg-go"
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
