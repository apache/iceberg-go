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
	"fmt"

	"github.com/apache/iceberg-go"
)

// RollbackToTime stages a rollback of the transaction's branch to the
// reachable snapshot with the latest timestamp strictly before timestampMs.
// Like Java ManageSnapshots.rollbackToTime, this walks snapshot ancestry
// rather than the table's snapshot log. A snapshot on another branch, or an
// orphan snapshot, is never a rollback target.
//
// The rollback is pinned to the branch head observed by this transaction,
// using the same conflict checks as RollbackToSnapshot. The timestamp is
// resolved when staging, not re-selected after concurrent commits.
func (t *Transaction) RollbackToTime(timestampMs int64) error {
	meta, err := t.txnMeta()
	if err != nil {
		return err
	}

	branch := t.branch
	if branch == "" {
		branch = MainBranch
	}

	ref, ok := meta.refs[branch]
	if !ok {
		return fmt.Errorf("%w: cannot rollback: branch %q does not exist", iceberg.ErrInvalidArgument, branch)
	}
	if ref.Type != BranchRef {
		return fmt.Errorf("%w: cannot rollback: ref %q is a %s, not a branch", iceberg.ErrInvalidArgument, branch, ref.Type)
	}

	// Metadata may have been modified in this transaction, so resolve the
	// staged branch head and ancestors, not the table's immutable base.
	lookup := func(id int64) *Snapshot {
		snapshot, _ := meta.SnapshotByID(id)
		return snapshot
	}
	if lookup(ref.SnapshotID) == nil {
		return fmt.Errorf("cannot rollback: branch %q references unknown snapshot %d", branch, ref.SnapshotID)
	}

	ancestors := AncestorsOf(ref.SnapshotID, lookup)
	var target *Snapshot
	for i := range ancestors {
		snapshot := &ancestors[i]
		if snapshot.TimestampMs < timestampMs && (target == nil || snapshot.TimestampMs > target.TimestampMs) {
			target = snapshot
		}
	}
	if target == nil {
		return fmt.Errorf("%w: no ancestor of branch %q is older than timestamp %d", iceberg.ErrInvalidArgument, branch, timestampMs)
	}

	return t.RollbackToSnapshot(target.SnapshotID)
}
