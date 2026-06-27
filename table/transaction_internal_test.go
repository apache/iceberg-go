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
	requireContainsRequirement(t, txn.reqs, AssertRefSnapshotID(MainBranch, &mainSnapshotID))
	requireContainsRequirement(t, txn.reqs, AssertRefSnapshotID("feature", &featureSnapshotID))
}

func TestTransactionApplyDedupesEquivalentRequirementsWithinAndAcrossCalls(t *testing.T) {
	txn := newTransactionWithSnapshotRefs(t)

	mainSnapshotID := int64(10)
	first := AssertRefSnapshotID(MainBranch, &mainSnapshotID)
	second := AssertRefSnapshotID(MainBranch, &mainSnapshotID)

	err := txn.apply(nil, []Requirement{first, second})
	require.NoError(t, err)
	require.Len(t, txn.reqs, 1)
	requireContainsRequirement(t, txn.reqs, first)

	err = txn.apply(nil, []Requirement{AssertRefSnapshotID(MainBranch, &mainSnapshotID)})
	require.NoError(t, err)
	require.Len(t, txn.reqs, 1)
	requireContainsRequirement(t, txn.reqs, first)
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

func requireContainsRequirement(t *testing.T, requirements []Requirement, expected Requirement) {
	t.Helper()

	expectedKey, err := requirementSemanticKey(expected)
	require.NoError(t, err)

	for _, requirement := range requirements {
		actualKey, keyErr := requirementSemanticKey(requirement)
		require.NoError(t, keyErr)
		if actualKey == expectedKey {
			return
		}
	}

	t.Fatalf("expected requirement %s not found", expectedKey)
}

func transactionTestPtr[T any](v T) *T {
	return &v
}
