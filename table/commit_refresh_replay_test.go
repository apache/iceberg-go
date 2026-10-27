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
	"errors"
	"fmt"
	"strings"
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
	loads    atomic.Int32
}

type identifierCapturingCatalog struct {
	metadata         Metadata
	loadIdentifier   Identifier
	commitIdentifier Identifier
	failNextCommit   bool
}

func (c *headTrackingCatalog) LoadTable(_ context.Context, ident Identifier) (*Table, error) {
	c.loads.Add(1)

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

func (c *identifierCapturingCatalog) LoadTable(_ context.Context, ident Identifier) (*Table, error) {
	c.loadIdentifier = ident

	return New(ident, c.metadata, c.metadata.Location(),
		func(context.Context) (iceio.IO, error) { return iceio.LocalFS{}, nil }, c), nil
}

func (c *identifierCapturingCatalog) CommitTable(_ context.Context, ident Identifier, _ []Requirement, updates []Update) (Metadata, string, error) {
	c.commitIdentifier = ident
	if c.failNextCommit {
		c.failNextCommit = false

		return nil, "", ErrCommitFailed
	}

	updated, err := UpdateTableMetadata(c.metadata, updates, "")
	if err != nil {
		return nil, "", err
	}
	c.metadata = updated

	return updated, updated.Location(), nil
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

// Why: after a refresh, a rejection that does not depend on timing fails
// every later attempt and every re-commit the same way, so the transaction
// must become unusable rather than leave an errors.Is(ErrCommitFailed) loop
// spinning, and its message must not tell the caller to try again. A
// validator that fails to read must stay resumable.
func TestResumeDeterministicRejectionsMakeTransactionUnusable(t *testing.T) {
	head := int64(100)
	noRetries := iceberg.Properties{}
	// Rejections formatted like the validators in conflict_validation.go.
	rejectConcurrent := func(sentinel error) conflictValidatorFunc {
		return func(cc *conflictContext) error {
			if len(cc.concurrent) > 0 {
				return fmt.Errorf("%w: snapshot %d added f.parquet", sentinel, cc.concurrent[0].SnapshotID)
			}

			return nil
		}
	}
	withFeature := func(t *testing.T, base Metadata) Metadata {
		b, err := MetadataBuilderFromBase(base, "")
		require.NoError(t, err)
		require.NoError(t, b.SetSnapshotRef("feature", head, BranchRef))
		out, err := b.Build()
		require.NoError(t, err)

		return out
	}
	rebuild := func(t *testing.T, base Metadata, edit func(*MetadataBuilder)) Metadata {
		b, err := MetadataBuilderFromBase(base, "")
		require.NoError(t, err)
		edit(b)
		out, err := b.Build()
		require.NoError(t, err)

		return out
	}

	tests := []struct {
		name                string
		branch              string
		stage               func(t *testing.T, tx *Transaction)
		validator           conflictValidatorFunc
		peer                func(t *testing.T, base Metadata) Metadata
		want                error
		message             string
		matchesCommitFailed bool
		// mentionsRetry marks validator causes, whose text includes
		// ErrCommitFailed's.
		mentionsRetry bool
	}{
		{
			name: "diverged base", validator: func(*conflictContext) error { return nil },
			peer: func(t *testing.T, base Metadata) Metadata {
				return rebuild(t, base, func(b *MetadataBuilder) {
					require.NoError(t, b.AddSnapshot(&Snapshot{
						SnapshotID: 300, SequenceNumber: 2, TimestampMs: base.LastUpdatedMillis() + 1,
						Summary: &Summary{Operation: OpAppend},
					}))
					require.NoError(t, b.SetSnapshotRef(MainBranch, 300, BranchRef))
				})
			},
			want:    ErrCommitDiverged,
			message: "transaction cannot be committed again: base diverged: commit diverged: base snapshot is no longer on the branch: ",
		},
		{
			name: "conflicting data files", validator: rejectConcurrent(ErrConflictingDataFiles),
			peer:                func(t *testing.T, base Metadata) Metadata { return graftSnapshotOnto(t, base, MainBranch, 200) },
			want:                ErrConflictingDataFiles,
			message:             "transaction cannot be committed again: validation conflict: commit failed, refresh and try again: concurrent data files added: snapshot 200 added f.parquet",
			matchesCommitFailed: true, mentionsRetry: true,
		},
		{
			name: "conflicting delete files", validator: rejectConcurrent(ErrConflictingDeleteFiles),
			peer:                func(t *testing.T, base Metadata) Metadata { return graftSnapshotOnto(t, base, MainBranch, 200) },
			want:                ErrConflictingDeleteFiles,
			message:             "transaction cannot be committed again: validation conflict: commit failed, refresh and try again: concurrent delete files added: snapshot 200 added f.parquet",
			matchesCommitFailed: true, mentionsRetry: true,
		},
		{
			name: "data files missing", validator: rejectConcurrent(ErrDataFilesMissing),
			peer:                func(t *testing.T, base Metadata) Metadata { return graftSnapshotOnto(t, base, MainBranch, 200) },
			want:                ErrDataFilesMissing,
			message:             "transaction cannot be committed again: validation conflict: commit failed, refresh and try again: referenced data files missing: snapshot 200 added f.parquet",
			matchesCommitFailed: true, mentionsRetry: true,
		},
		{
			name: "wrapped conflict from any validator",
			validator: func(cc *conflictContext) error {
				if len(cc.concurrent) > 0 {
					return fmt.Errorf("custom validator: %w", ErrCommitFailed)
				}

				return nil
			},
			peer:                func(t *testing.T, base Metadata) Metadata { return graftSnapshotOnto(t, base, MainBranch, 200) },
			message:             "transaction cannot be committed again: validation conflict: custom validator: commit failed, refresh and try again",
			matchesCommitFailed: true, mentionsRetry: true,
		},
		{
			name: "commit branch became a tag", branch: "feature",
			peer: func(t *testing.T, base Metadata) Metadata {
				return rebuild(t, base, func(b *MetadataBuilder) {
					require.NoError(t, b.RemoveSnapshotRef("feature"))
					require.NoError(t, b.SetSnapshotRef("feature", head, TagRef))
				})
			},
			message: `transaction cannot be committed again: commit branch is no longer a branch: requirement failed: ref "feature" is a tag; tags cannot be transaction targets`,
		},
		{
			name: "commit branch was deleted", branch: "feature",
			peer: func(t *testing.T, base Metadata) Metadata {
				return rebuild(t, base, func(b *MetadataBuilder) { require.NoError(t, b.RemoveSnapshotRef("feature")) })
			},
			message:             `transaction cannot be committed again: commit branch is no longer a branch: requirement failed: branch or tag "feature" is missing, expected 100`,
			matchesCommitFailed: true,
		},
		{
			name: "stale schema",
			stage: func(t *testing.T, tx *Transaction) {
				require.NoError(t, tx.UpdateSchema(true, false).
					AddColumn([]string{"writer"}, iceberg.PrimitiveTypes.String, "", false, nil).Commit())
			},
			peer: func(t *testing.T, base Metadata) Metadata {
				return rebuild(t, base, func(b *MetadataBuilder) {
					require.NoError(t, b.AddSchema(iceberg.NewSchema(1,
						iceberg.NestedField{ID: 1, Name: "id", Type: iceberg.PrimitiveTypes.Int64, Required: true},
						iceberg.NestedField{ID: 2, Name: "peer", Type: iceberg.PrimitiveTypes.String},
					)))
					require.NoError(t, b.SetCurrentSchemaID(-1))
				})
			},
			message: "transaction cannot be committed again: requirement no longer holds after refresh: " +
				"Requirement failed: current schema id has changed: expected 0, found 1",
			matchesCommitFailed: true,
		},
		{
			name:  "stale default sort order",
			stage: func(t *testing.T, tx *Transaction) { require.NoError(t, tx.AssertDefaultShape()) },
			peer: func(t *testing.T, base Metadata) Metadata {
				return rebuild(t, base, func(b *MetadataBuilder) {
					order, err := NewSortOrder(1, []SortField{{
						SourceIDs: []int{1}, Transform: iceberg.IdentityTransform{}, Direction: SortASC, NullOrder: NullsFirst,
					}})
					require.NoError(t, err)
					require.NoError(t, b.AddSortOrder(&order))
					require.NoError(t, b.SetDefaultSortOrderID(-1))
				})
			},
			message: "transaction cannot be committed again: requirement no longer holds after refresh: " +
				"requirement failed: default sort order id has changed: expected 0, found 1",
			matchesCommitFailed: true,
		},
		{
			name:  "pinned ref moved",
			stage: func(t *testing.T, tx *Transaction) { require.NoError(t, tx.AssertRefSnapshotID(MainBranch)) },
			peer:  func(t *testing.T, base Metadata) Metadata { return graftSnapshotOnto(t, base, MainBranch, 200) },
			message: "transaction cannot be committed again: requirement no longer holds after refresh: " +
				`requirement failed: branch "main" has changed: expected id 100, found 200`,
			matchesCommitFailed: true,
		},
		{
			name:  "non-replayable commit whose branch moved",
			stage: func(_ *testing.T, tx *Transaction) { tx.noReplay = true },
			peer:  func(t *testing.T, base Metadata) Metadata { return graftSnapshotOnto(t, base, MainBranch, 200) },
			message: "transaction cannot be committed again: requirement no longer holds after refresh (" + noReplayHint + "): " +
				`requirement failed: branch "main" has changed: expected id 100, found 200`,
			matchesCommitFailed: true,
		},
		{
			name: "table was replaced",
			peer: func(t *testing.T, base Metadata) Metadata {
				return rebuild(t, base, func(b *MetadataBuilder) { require.NoError(t, b.SetUUID(uuid.New())) })
			},
			want:    ErrInvalidMetadata,
			message: "transaction cannot be committed again: table was replaced: ",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			base := withFeature(t, newConflictTestMetadataWithProps(t, &head, noRetries))
			tbl, cat := newAssertRefTestTable(t, base, tt.peer(t, base))
			tx := tbl.NewTransaction()
			if tt.branch != "" {
				branchTx, err := tbl.NewTransactionOnBranch(tt.branch)
				require.NoError(t, err)
				tx = branchTx
			}
			require.NoError(t, tx.SetProperties(iceberg.Properties{"k": "v"}))
			if tt.stage != nil {
				tt.stage(t, tx)
			}
			if tt.validator != nil {
				tx.addValidator(tt.validator)
			}

			_, err := tx.Commit(t.Context())
			require.ErrorIs(t, err, ErrCommitFailed)
			require.NotErrorIs(t, err, ErrTransactionUnusable)
			require.Equal(t, int32(1), cat.attempts.Load())
			require.Equal(t, int32(0), cat.loads.Load(), "retries are disabled")

			_, err = tx.Commit(t.Context())
			require.ErrorIs(t, err, ErrTransactionUnusable)
			assert.Equal(t, int32(1), cat.loads.Load(), "the re-commit refreshes once")
			if tt.want != nil {
				require.ErrorIs(t, err, tt.want)
			}
			if tt.matchesCommitFailed {
				require.ErrorIs(t, err, ErrCommitFailed)
			} else {
				require.NotErrorIs(t, err, ErrCommitFailed, "a caller retrying on ErrCommitFailed must stop")
			}
			assert.True(t, strings.HasPrefix(err.Error(), tt.message), err.Error())
			if !tt.mentionsRetry {
				assert.NotContains(t, err.Error(), "refresh and try again")
			}
			assert.Equal(t, int32(1), cat.attempts.Load(), "the resume must stop before the catalog")

			_, err = tx.Commit(t.Context())
			assert.EqualError(t, err, "transaction cannot be committed again: a previous commit failed, build a new transaction")
			assert.Equal(t, int32(1), cat.attempts.Load())
			assert.Equal(t, int32(1), cat.loads.Load(), "an unusable transaction must not refresh again")
		})
	}

	t.Run("validator read failure stays resumable", func(t *testing.T) {
		base := newConflictTestMetadataWithProps(t, &head, noRetries)
		tbl, cat := newAssertRefTestTable(t, base, graftSnapshotOnto(t, base, MainBranch, 200))
		tx := tbl.NewTransaction()
		require.NoError(t, tx.SetProperties(iceberg.Properties{"k": "v"}))
		readFailures := 1
		tx.addValidator(func(cc *conflictContext) error {
			if len(cc.concurrent) > 0 && readFailures > 0 {
				readFailures--

				return errors.New("simulated manifest read failure")
			}

			return nil
		})

		_, err := tx.Commit(t.Context())
		require.ErrorIs(t, err, ErrCommitFailed)

		_, err = tx.Commit(t.Context())
		require.EqualError(t, err, "simulated manifest read failure")

		_, err = tx.Commit(t.Context())
		require.NoError(t, err)
		assert.Equal(t, int32(2), cat.attempts.Load())
	})
}

// A stale schema requirement cannot be rebased, but a valid one must not
// prevent retrying against a changed branch head.
func TestDoCommit_NonRebasedRequirementFailsAfterOneRefresh(t *testing.T) {
	writerHead := int64(100)
	props := iceberg.Properties{
		CommitNumRetriesKey:     "3",
		CommitMinRetryWaitMsKey: "1",
		CommitMaxRetryWaitMsKey: "2",
	}

	stalePeerSchema := func(t *testing.T, props iceberg.Properties) (*headTrackingCatalog, *Transaction) {
		writerBase := newConflictTestMetadataWithProps(t, &writerHead, props)

		builder, err := MetadataBuilderFromBase(writerBase, "")
		require.NoError(t, err)
		peerSchema := iceberg.NewSchema(1,
			iceberg.NestedField{ID: 1, Name: "id", Type: iceberg.PrimitiveTypes.Int64, Required: true},
			iceberg.NestedField{ID: 2, Name: "peer", Type: iceberg.PrimitiveTypes.String},
		)
		require.NoError(t, builder.AddSchema(peerSchema))
		require.NoError(t, builder.SetCurrentSchemaID(-1))
		peerChanged, err := builder.Build()
		require.NoError(t, err)

		tbl, cat := newAssertRefTestTable(t, writerBase, peerChanged)
		tx := tbl.NewTransaction()
		require.NoError(t, tx.UpdateSchema(true, false).
			AddColumn([]string{"writer"}, iceberg.PrimitiveTypes.String, "", false, nil).
			Commit())

		return cat, tx
	}

	const staleSchema = "transaction cannot be committed again: requirement no longer holds after refresh: " +
		"Requirement failed: current schema id has changed: expected 0, found 1"

	t.Run("stale schema update fails after one refresh", func(t *testing.T) {
		cat, tx := stalePeerSchema(t, props)

		_, err := tx.Commit(t.Context())
		require.ErrorIs(t, err, ErrCommitFailed)
		require.ErrorIs(t, err, ErrTransactionUnusable)
		assert.EqualError(t, err, staleSchema)
		assert.Equal(t, int32(1), cat.attempts.Load(), "the stale requirement must not be resubmitted")
		assert.Equal(t, int32(1), cat.loads.Load(), "the commit must stop after the first refresh")
		assert.Equal(t, 1, cat.metadata.CurrentSchema().ID, "the peer's schema must survive")
	})

	t.Run("requirements that still hold do not block the rebase", func(t *testing.T) {
		writerBase := newConflictTestMetadataWithProps(t, &writerHead, props)
		advanced := graftSnapshotOnto(t, writerBase, MainBranch, 200)
		tbl, cat := newAssertRefTestTable(t, writerBase, advanced)

		reqs := []Requirement{
			AssertRefSnapshotID(MainBranch, &writerHead),
			AssertCurrentSchemaID(writerBase.CurrentSchema().ID),
			AssertTableUUID(writerBase.TableUUID()),
		}
		_, err := tbl.doCommit(t.Context(),
			[]Update{NewSetPropertiesUpdate(iceberg.Properties{"k": "v"})}, reqs,
			withCommitBranch(MainBranch))
		require.NoError(t, err)
		assert.Equal(t, int32(2), cat.attempts.Load())
		assert.Equal(t, int32(1), cat.loads.Load())
		assert.Equal(t, "v", cat.metadata.Properties()["k"])
	})
}

func TestRefreshPassesIdentifierCopy(t *testing.T) {
	branchHead := int64(1)
	meta := newConflictTestMetadataWithProps(t, &branchHead, nil)
	cat := &identifierCapturingCatalog{metadata: meta}
	identifier := Identifier{"db", "refresh_copy_test"}

	tbl := New(identifier, meta, "file:///tmp/test-refresh-copy/metadata/v1.metadata.json",
		func(context.Context) (iceio.IO, error) { return iceio.LocalFS{}, nil }, cat)

	err := tbl.Refresh(context.Background())
	require.NoError(t, err)
	assert.Equal(t, identifier, tbl.Identifier())

	cat.loadIdentifier[0] = "corrupt"
	assert.Equal(t, identifier, tbl.Identifier())
}

func TestDoCommitPassesIdentifierCopy(t *testing.T) {
	branchHead := int64(1)
	meta := newConflictTestMetadataWithProps(t, &branchHead, nil)
	cat := &identifierCapturingCatalog{metadata: meta}
	identifier := Identifier{"db", "do_commit_copy_test"}

	tbl := New(identifier, meta, "file:///tmp/test-do-commit-copy/metadata/v1.metadata.json",
		func(context.Context) (iceio.IO, error) { return iceio.LocalFS{}, nil }, cat)
	tx := tbl.NewTransaction()

	require.NoError(t, tx.SetProperties(map[string]string{"k": "v"}))
	updated, err := tx.Commit(context.Background())
	require.NoError(t, err)

	assert.Equal(t, identifier, tbl.Identifier())
	assert.Equal(t, identifier, updated.Identifier())

	cat.commitIdentifier[0] = "corrupt"
	assert.Equal(t, identifier, tbl.Identifier())
	assert.Equal(t, identifier, updated.Identifier())
}

func TestDoCommitRetryPassesIdentifierCopy(t *testing.T) {
	branchHead := int64(1)
	meta := newConflictTestMetadataWithProps(t, &branchHead, iceberg.Properties{
		CommitNumRetriesKey:     "1",
		CommitMinRetryWaitMsKey: "1",
		CommitMaxRetryWaitMsKey: "2",
	})
	cat := &identifierCapturingCatalog{metadata: meta, failNextCommit: true}
	identifier := Identifier{"db", "do-commit-retry-copy-test"}

	tbl := New(identifier, meta, "file:///tmp/test-do-commit-retry-copy/metadata/v1.metadata.json",
		func(context.Context) (iceio.IO, error) { return iceio.LocalFS{}, nil }, cat)

	_, err := tbl.doCommit(context.Background(), nil, nil, withCommitBranch(MainBranch))
	require.NoError(t, err)
	require.NotNil(t, cat.loadIdentifier)

	cat.loadIdentifier[0] = "corrupt"
	assert.Equal(t, identifier, tbl.Identifier())
	cat.commitIdentifier[0] = "corrupt"
	assert.Equal(t, identifier, tbl.Identifier())
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

	out := rewriteRefSnapshotRequirements(reqs, MainBranch, fresh, nil)
	require.Len(t, out, 3)

	a, ok := out[0].(*assertRefSnapshotID)
	require.True(t, ok)
	require.Equal(t, MainBranch, a.Ref)
	require.NotNil(t, a.SnapshotID)
	assert.Equal(t, newHead, *a.SnapshotID, "main ref assertion must point at the fresh head")

	// Other-branch assertion and UUID assertion pass through unchanged.
	assert.Same(t, otherAssert, out[1])
	assert.Same(t, uuidAssert, out[2])

	// A pinned branch carries an explicit compare-and-swap requirement:
	// its assertion must survive the rewrite untouched.
	pinnedOut := rewriteRefSnapshotRequirements(reqs, MainBranch, fresh,
		map[string]struct{}{MainBranch: {}})
	assert.Same(t, mainAssert, pinnedOut[0],
		"a pinned assertion must not be rewritten to the fresh head")

	// Edge cases: empty branch / nil fresh / branch missing on fresh
	// return the slice unchanged.
	noBranchOut := rewriteRefSnapshotRequirements(reqs, "", fresh, nil)
	assert.Equal(t, reqs, noBranchOut)

	nilFreshOut := rewriteRefSnapshotRequirements(reqs, MainBranch, nil, nil)
	assert.Equal(t, reqs, nilFreshOut)

	missingBranchOut := rewriteRefSnapshotRequirements(reqs, "non-existent", fresh, nil)
	assert.Equal(t, reqs, missingBranchOut)
}
