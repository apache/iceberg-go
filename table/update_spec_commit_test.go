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
	"fmt"
	"sync/atomic"
	"testing"

	"github.com/apache/iceberg-go"
	iceio "github.com/apache/iceberg-go/io"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// reqValidatingCatalog mimics a real catalog's optimistic-concurrency check:
// CommitTable validates every requirement against the catalog's current
// metadata (rejecting with ErrCommitFailed on a mismatch) before applying the
// updates. failFirst rejects the first N attempts with ErrCommitFailed
// regardless of the requirements, to exercise the retry path.
type reqValidatingCatalog struct {
	metadata  Metadata
	lastReqs  []Requirement
	attempts  atomic.Int32
	failFirst int32
}

func (c *reqValidatingCatalog) LoadTable(_ context.Context, ident Identifier) (*Table, error) {
	return New(ident, c.metadata, "",
		func(context.Context) (iceio.IO, error) { return iceio.LocalFS{}, nil }, c), nil
}

func (c *reqValidatingCatalog) CommitTable(_ context.Context, _ Identifier, reqs []Requirement, updates []Update) (Metadata, string, error) {
	c.lastReqs = reqs
	if n := c.attempts.Add(1); n <= c.failFirst {
		return nil, "", fmt.Errorf("peer advanced table: %w", ErrCommitFailed)
	}
	for _, r := range reqs {
		if err := r.Validate(c.metadata); err != nil {
			return nil, "", fmt.Errorf("%w: %v", ErrCommitFailed, err)
		}
	}
	meta, err := UpdateTableMetadata(c.metadata, updates, "")
	if err != nil {
		return nil, "", err
	}
	c.metadata = meta

	return meta, "", nil
}

func lastAssignedPartitionAssertions(reqs []Requirement) []int {
	var out []int
	for _, r := range reqs {
		if a, ok := r.(*assertLastAssignedPartitionId); ok {
			out = append(out, a.LastAssignedPartitionID)
		}
	}

	return out
}

// TestChainedUpdateSpecCommitsToCatalog exercises two UpdateSpec operations
// chained in a single transaction and committed through a
// requirement-validating catalog. Each operation reads the transaction's
// staged (and advancing) last-assigned partition id, so without type-based
// deduplication the transaction would send both the base and the staged value
// as AssertLastAssignedPartitionID requirements. Those are mutually
// contradictory against one catalog snapshot and cannot be repaired by retry
// (doCommit only rewrites ref-snapshot assertions), so the commit would fail.
// The transaction must instead send exactly one requirement carrying the
// pre-transaction (base) value.
func TestChainedUpdateSpecCommitsToCatalog(t *testing.T) {
	t.Run("clean commit sends a single base-valued requirement", func(t *testing.T) {
		cat := &reqValidatingCatalog{}
		tbl := newRetryTestTable(t, cat, nil)
		cat.metadata = tbl.Metadata()

		baseLastPartitionID := tbl.Metadata().LastPartitionSpecID()
		require.NotNil(t, baseLastPartitionID)

		txn := tbl.NewTransaction()
		require.NoError(t, txn.UpdateSpec(false).
			AddField("id", iceberg.IdentityTransform{}, "id_identity").
			Commit())
		require.NoError(t, txn.UpdateSpec(false).
			AddField("id", iceberg.BucketTransform{NumBuckets: 4}, "id_bucket").
			Commit())

		committed, err := txn.Commit(t.Context())
		require.NoError(t, err)

		// Both chained partition fields survive to the committed spec, and the
		// second field's id continues from the first (staged) one rather than
		// colliding with or reusing it.
		committedSpec := committed.Spec()
		require.Equal(t, 2, committedSpec.NumFields())
		fieldIDs := make([]int, 0, committedSpec.NumFields())
		for _, f := range committedSpec.Fields() {
			fieldIDs = append(fieldIDs, f.FieldID)
		}
		assert.ElementsMatch(t,
			[]int{iceberg.PartitionDataIDStart, iceberg.PartitionDataIDStart + 1}, fieldIDs)

		// Both partition specs are genuinely new relative to the committed
		// catalog, so isNewPartitionSpec (which reads staged metadata) must not
		// suppress either AddPartitionSpecUpdate: the catalog ends up with the
		// base spec plus both added specs, all with distinct ids.
		specIDs := map[int]struct{}{}
		for _, s := range committed.Metadata().PartitionSpecs() {
			specIDs[s.ID()] = struct{}{}
		}
		assert.Len(t, specIDs, len(committed.Metadata().PartitionSpecs()),
			"partition spec ids must be distinct (no collision across chained updates)")
		assert.GreaterOrEqual(t, len(specIDs), 3,
			"expected the base spec plus both newly added specs")

		// Exactly one partition-id assertion reached the catalog, holding the
		// base value rather than the staged, advanced value.
		partitionAssertions := lastAssignedPartitionAssertions(cat.lastReqs)
		require.Len(t, partitionAssertions, 1)
		assert.Equal(t, *baseLastPartitionID, partitionAssertions[0])
	})

	t.Run("retries and succeeds when the catalog rejects the first attempt", func(t *testing.T) {
		cat := &reqValidatingCatalog{failFirst: 1}
		tbl := newRetryTestTable(t, cat, iceberg.Properties{
			CommitNumRetriesKey:     "3",
			CommitMinRetryWaitMsKey: "1",
			CommitMaxRetryWaitMsKey: "2",
		})
		cat.metadata = tbl.Metadata()

		txn := tbl.NewTransaction()
		require.NoError(t, txn.UpdateSpec(false).
			AddField("id", iceberg.IdentityTransform{}, "id_identity").
			Commit())
		require.NoError(t, txn.UpdateSpec(false).
			AddField("id", iceberg.BucketTransform{NumBuckets: 4}, "id_bucket").
			Commit())

		committed, err := txn.Commit(t.Context())
		require.NoError(t, err)
		committedSpec := committed.Spec()
		require.Equal(t, 2, committedSpec.NumFields())

		// One rejected attempt followed by one successful retry: the
		// consistent, single base-valued requirement is what lets the retry
		// succeed.
		assert.Equal(t, int32(2), cat.attempts.Load())
		require.Len(t, lastAssignedPartitionAssertions(cat.lastReqs), 1)
	})
}
