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

// End-to-end coverage for refresh-and-replay in doCommit's retry
// loop. The framework was wired in #934; this PR makes it close the
// loop:
//
//   - On ErrCommitFailed, reload the catalog's current metadata.
//   - Build a fresh conflictContext and re-run the registered
//     validators against (base = writer's view, current = fresh).
//   - Rewrite AssertRefSnapshotID requirements to point at the new
//     branch head before re-submitting, so a non-conflicting peer
//     advance does not deterministically reject every retry.

import (
	"context"
	"fmt"
	"sync/atomic"
	"testing"

	"github.com/apache/iceberg-go"
	iceio "github.com/apache/iceberg-go/io"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// headTrackingCatalog enforces Requirement.Validate against its
// current metadata on every CommitTable call and returns
// ErrCommitFailed-wrapped errors on assertion failures. Successful
// commits advance the metadata (mirroring the REST catalog's
// optimistic-concurrency behavior). The seedHead variant lets a test
// initialize the catalog with metadata at a different head from the
// writer's view, simulating a concurrent peer that already committed.
type headTrackingCatalog struct {
	metadata Metadata
	attempts atomic.Int32
}

func (c *headTrackingCatalog) LoadTable(_ context.Context, ident Identifier) (*Table, error) {
	return New(ident, c.metadata, "",
		func(context.Context) (iceio.IO, error) { return iceio.LocalFS{}, nil }, c), nil
}

func (c *headTrackingCatalog) CommitTable(_ context.Context, _ Identifier, reqs []Requirement, updates []Update) (Metadata, string, error) {
	c.attempts.Add(1)
	for _, req := range reqs {
		if err := req.Validate(c.metadata); err != nil {
			return nil, "", fmt.Errorf("%w: %w", ErrCommitFailed, err)
		}
	}
	meta, err := UpdateTableMetadata(c.metadata, updates, "")
	if err != nil {
		return nil, "", err
	}
	c.metadata = meta

	return meta, "", nil
}

// graftSnapshotOnto builds a metadata that adds a child snapshot on
// top of base's current branch head. Returns the post-graft metadata
// and the new head id.
func graftSnapshotOnto(t *testing.T, base Metadata, branch string, childID int64) Metadata {
	t.Helper()

	parent := base.SnapshotByName(branch)
	require.NotNil(t, parent, "branch %q must exist on base", branch)

	builder, err := MetadataBuilderFromBase(base, "")
	require.NoError(t, err)
	parentID := parent.SnapshotID
	require.NoError(t, builder.AddSnapshot(&Snapshot{
		SnapshotID:       childID,
		ParentSnapshotID: &parentID,
		SequenceNumber:   parent.SequenceNumber + 1,
		TimestampMs:      base.LastUpdatedMillis() + 1,
		Summary:          &Summary{Operation: OpAppend},
	}))
	require.NoError(t, builder.SetSnapshotRef(branch, childID, BranchRef))

	out, err := builder.Build()
	require.NoError(t, err)

	return out
}

// TestDoCommit_RefreshAndReplaySucceedsAfterPeerAdvance is the
// headline scenario for #830: a peer advanced the branch with a
// non-conflicting commit, the writer's first attempt fails on
// AssertRefSnapshotID, and the retry refreshes, validates against
// the new head (no conflict), rewrites the assertion, and succeeds.
//
// Without requirement-rewriting the retry would re-submit the same
// stale assertion and burn the entire retry budget.
func TestDoCommit_RefreshAndReplaySucceedsAfterPeerAdvance(t *testing.T) {
	writerHead := int64(100)
	peerHead := int64(200)

	// Writer's view of the table — branch points at S100.
	writerBase := newConflictTestMetadataWithProps(t, &writerHead, iceberg.Properties{
		CommitNumRetriesKey:     "4",
		CommitMinRetryWaitMsKey: "1",
		CommitMaxRetryWaitMsKey: "2",
	})

	// Peer commit that already happened: catalog state has S200 on
	// top of S100 as the branch head.
	advanced := graftSnapshotOnto(t, writerBase, MainBranch, peerHead)
	cat := &headTrackingCatalog{metadata: advanced}

	tbl := New(Identifier{"db", "refresh-test"}, writerBase, "metadata.json",
		func(context.Context) (iceio.IO, error) { return iceio.LocalFS{}, nil }, cat)

	reqs := []Requirement{AssertRefSnapshotID(MainBranch, &writerHead)}
	noOpValidator := func(*conflictContext) error { return nil }

	_, err := tbl.doCommit(context.Background(), nil, reqs,
		withCommitBranch(MainBranch),
		withCommitValidators(noOpValidator))

	require.NoError(t, err, "retry must succeed once the assertion is rewritten to the peer head")
	assert.Equal(t, int32(2), cat.attempts.Load(),
		"first attempt fails on stale assertion; second attempt succeeds with rewritten assertion")
}

// TestDoCommit_ValidatorRejectsOnRefresh proves the loop bails
// terminally when refresh-and-replay surfaces a real semantic
// conflict — refresh runs, the validator decides the commit cannot
// safely replay, and we exit immediately instead of burning the
// retry budget.
func TestDoCommit_ValidatorRejectsOnRefresh(t *testing.T) {
	writerHead := int64(100)
	peerHead := int64(200)

	writerBase := newConflictTestMetadataWithProps(t, &writerHead, iceberg.Properties{
		CommitNumRetriesKey:     "4",
		CommitMinRetryWaitMsKey: "1",
		CommitMaxRetryWaitMsKey: "2",
	})
	advanced := graftSnapshotOnto(t, writerBase, MainBranch, peerHead)
	cat := &headTrackingCatalog{metadata: advanced}
	tbl := New(Identifier{"db", "refresh-reject"}, writerBase, "metadata.json",
		func(context.Context) (iceio.IO, error) { return iceio.LocalFS{}, nil }, cat)

	reqs := []Requirement{AssertRefSnapshotID(MainBranch, &writerHead)}
	// Reject only when the cc actually walks a concurrent snapshot —
	// otherwise the validator would also fire on attempt 0 (where
	// base == current and cc.concurrent is empty), and the test
	// would not be exercising the refresh-and-replay path.
	rejectOnConcurrent := func(cc *conflictContext) error {
		if len(cc.concurrent) > 0 {
			return ErrConflictingDataFiles
		}

		return nil
	}

	_, err := tbl.doCommit(context.Background(), nil, reqs,
		withCommitBranch(MainBranch),
		withCommitValidators(rejectOnConcurrent))

	require.Error(t, err)
	assert.ErrorIs(t, err, ErrConflictingDataFiles)
	// First attempt: catalog rejects (stale assertion). Retry: refresh
	// + validator rejects → terminal exit before any further attempts.
	assert.Equal(t, int32(1), cat.attempts.Load(),
		"validator rejection on retry must abort before re-issuing CommitTable")
}

// TestRewriteRefSnapshotRequirements covers the helper directly:
// only the matching ref's assertion is rewritten; other requirements
// pass through untouched; an empty-branch / nil-fresh / missing-
// branch input returns the slice unchanged.
func TestRewriteRefSnapshotRequirements(t *testing.T) {
	old := int64(50)
	other := int64(60)
	uid := uuid.New()

	mainAssert := AssertRefSnapshotID(MainBranch, &old)
	otherAssert := AssertRefSnapshotID("other-branch", &other)
	uuidAssert := AssertTableUUID(uid)
	reqs := []Requirement{mainAssert, otherAssert, uuidAssert}

	newHead := int64(99)
	fresh := newConflictTestMetadata(t, &newHead)

	out := rewriteRefSnapshotRequirements(reqs, MainBranch, fresh)
	require.Len(t, out, 3)

	a, ok := out[0].(*assertRefSnapshotID)
	require.True(t, ok)
	require.Equal(t, MainBranch, a.Ref)
	require.NotNil(t, a.SnapshotID)
	assert.Equal(t, newHead, *a.SnapshotID, "main ref assertion must point at the fresh head")

	// Other-branch assertion and UUID assertion pass through unchanged.
	assert.Same(t, otherAssert, out[1])
	assert.Same(t, uuidAssert, out[2])

	// Edge cases: empty branch / nil fresh / branch missing on fresh
	// return the slice unchanged.
	noBranchOut := rewriteRefSnapshotRequirements(reqs, "", fresh)
	assert.Equal(t, reqs, noBranchOut)

	nilFreshOut := rewriteRefSnapshotRequirements(reqs, MainBranch, nil)
	assert.Equal(t, reqs, nilFreshOut)

	missingBranchOut := rewriteRefSnapshotRequirements(reqs, "non-existent", fresh)
	assert.Equal(t, reqs, missingBranchOut)
}
