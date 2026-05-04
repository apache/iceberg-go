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
	"errors"
	"testing"

	"github.com/apache/iceberg-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestReadIsolationLevel(t *testing.T) {
	tests := []struct {
		name   string
		props  iceberg.Properties
		key    string
		defVal IsolationLevel
		want   IsolationLevel
	}{
		{
			name:   "missing key falls back to default",
			props:  iceberg.Properties{},
			key:    WriteDeleteIsolationLevelKey,
			defVal: IsolationSerializable,
			want:   IsolationSerializable,
		},
		{
			name:   "explicit serializable",
			props:  iceberg.Properties{WriteDeleteIsolationLevelKey: "serializable"},
			key:    WriteDeleteIsolationLevelKey,
			defVal: IsolationSnapshot,
			want:   IsolationSerializable,
		},
		{
			name:   "explicit snapshot",
			props:  iceberg.Properties{WriteDeleteIsolationLevelKey: "snapshot"},
			key:    WriteDeleteIsolationLevelKey,
			defVal: IsolationSerializable,
			want:   IsolationSnapshot,
		},
		{
			name:   "unrecognized value falls back to default",
			props:  iceberg.Properties{WriteDeleteIsolationLevelKey: "repeatable-read"},
			key:    WriteDeleteIsolationLevelKey,
			defVal: IsolationSerializable,
			want:   IsolationSerializable,
		},
		{
			name:   "empty string falls back to default",
			props:  iceberg.Properties{WriteDeleteIsolationLevelKey: ""},
			key:    WriteDeleteIsolationLevelKey,
			defVal: IsolationSnapshot,
			want:   IsolationSnapshot,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := readIsolationLevel(tt.props, tt.key, tt.defVal)
			assert.Equal(t, tt.want, got)
		})
	}
}

func TestRetryableConflictErrors_WrapCommitFailed(t *testing.T) {
	// The retryable sentinels must be recognizable to the retry
	// machinery as a commit-level conflict.
	retryable := []error{
		ErrConflictingDataFiles,
		ErrConflictingDeleteFiles,
		ErrDataFilesMissing,
	}
	for _, s := range retryable {
		assert.ErrorIsf(t, s, ErrCommitFailed, "%v should wrap ErrCommitFailed", s)
	}
}

func TestErrCommitDiverged_IsTerminal(t *testing.T) {
	// Divergence is terminal for the current attempt — retrying the
	// same updates will produce the same divergence. It therefore
	// MUST NOT wrap ErrCommitFailed, which is the retry machinery's
	// trigger.
	assert.False(t, errors.Is(ErrCommitDiverged, ErrCommitFailed),
		"ErrCommitDiverged must not wrap ErrCommitFailed — it is a terminal error")
}

func newConflictTestMetadata(t *testing.T, branchHead *int64) Metadata {
	t.Helper()

	return newConflictTestMetadataWithProps(t, branchHead, nil)
}

// newConflictTestMetadataWithProps mirrors newConflictTestMetadata but
// merges extraProps over the default property set. Used by tests that
// need to drive doCommit's retry budget knobs.
func newConflictTestMetadataWithProps(t *testing.T, branchHead *int64, extraProps iceberg.Properties) Metadata {
	t.Helper()
	schema := iceberg.NewSchema(0,
		iceberg.NestedField{ID: 1, Name: "id", Type: iceberg.PrimitiveTypes.Int64, Required: true},
	)
	props := iceberg.Properties{PropertyFormatVersion: "2"}
	for k, v := range extraProps {
		props[k] = v
	}
	meta, err := NewMetadata(schema, iceberg.UnpartitionedSpec, UnsortedSortOrder, "file:///tmp/conflict-test", props)
	require.NoError(t, err)

	if branchHead == nil {
		return meta
	}

	// Graft a synthetic snapshot whose timestamp sits just past the
	// metadata's last-updated timestamp so AddSnapshot's monotonicity
	// check accepts it.
	builder, err := MetadataBuilderFromBase(meta, "")
	require.NoError(t, err)
	snap := Snapshot{
		SnapshotID:     *branchHead,
		SequenceNumber: 1,
		TimestampMs:    meta.LastUpdatedMillis() + 1,
		Summary:        &Summary{Operation: OpAppend},
	}
	require.NoError(t, builder.AddSnapshot(&snap))
	require.NoError(t, builder.SetSnapshotRef(MainBranch, *branchHead, BranchRef))
	out, err := builder.Build()
	require.NoError(t, err)

	return out
}

// newConflictTestMetadataWithChain builds metadata whose branch head
// reaches back through ids[0]→ids[1]→...→ids[n-1] via ParentSnapshotID.
// ids[len-1] becomes the branch head; ids[0] is the chain root.
func newConflictTestMetadataWithChain(t *testing.T, ids []int64) Metadata {
	t.Helper()
	require.NotEmpty(t, ids)

	schema := iceberg.NewSchema(0,
		iceberg.NestedField{ID: 1, Name: "id", Type: iceberg.PrimitiveTypes.Int64, Required: true},
	)
	props := iceberg.Properties{PropertyFormatVersion: "2"}
	meta, err := NewMetadata(schema, iceberg.UnpartitionedSpec, UnsortedSortOrder, "file:///tmp/conflict-test", props)
	require.NoError(t, err)

	builder, err := MetadataBuilderFromBase(meta, "")
	require.NoError(t, err)

	for i, id := range ids {
		snap := Snapshot{
			SnapshotID:     id,
			SequenceNumber: int64(i + 1),
			TimestampMs:    meta.LastUpdatedMillis() + int64(i+1),
			Summary:        &Summary{Operation: OpAppend},
		}
		if i > 0 {
			parent := ids[i-1]
			snap.ParentSnapshotID = &parent
		}
		require.NoError(t, builder.AddSnapshot(&snap))
	}
	require.NoError(t, builder.SetSnapshotRef(MainBranch, ids[len(ids)-1], BranchRef))
	out, err := builder.Build()
	require.NoError(t, err)

	return out
}

func TestNewConflictContext_NoConcurrentCommits(t *testing.T) {
	// base and current point at the same snapshot → zero concurrent
	// snapshots, no error.
	head := int64(42)
	meta := newConflictTestMetadata(t, &head)

	ctx, err := newConflictContext(meta, meta, MainBranch, nil, true)
	require.NoError(t, err)
	assert.Empty(t, ctx.concurrent)
}

func TestNewConflictContext_WriterHasNoBranchView(t *testing.T) {
	// Writer loaded the table before any snapshot was committed on
	// the branch (empty base). A snapshot then appeared on the
	// branch — it must be reported as concurrent so validators that
	// care about concurrent writes (e.g. RowDelta under
	// SERIALIZABLE) can inspect its data files.
	base := newConflictTestMetadata(t, nil)
	head := int64(7)
	current := newConflictTestMetadata(t, &head)

	ctx, err := newConflictContext(base, current, MainBranch, nil, true)
	require.NoError(t, err)
	require.Len(t, ctx.concurrent, 1)
	assert.Equal(t, int64(7), ctx.concurrent[0].SnapshotID)
}

func TestNewConflictContext_EmptyBaseEnumeratesFullAncestry(t *testing.T) {
	// Empty base, current branch head reaches back through a chain of
	// three snapshots — all three must surface as concurrent in
	// reverse-chronological order so validators see the full set the
	// writer never observed.
	base := newConflictTestMetadata(t, nil)
	current := newConflictTestMetadataWithChain(t, []int64{10, 11, 12})

	ctx, err := newConflictContext(base, current, MainBranch, nil, true)
	require.NoError(t, err)
	require.Len(t, ctx.concurrent, 3)
	assert.Equal(t, int64(12), ctx.concurrent[0].SnapshotID)
	assert.Equal(t, int64(11), ctx.concurrent[1].SnapshotID)
	assert.Equal(t, int64(10), ctx.concurrent[2].SnapshotID)
}

func TestNewConflictContext_MissingCurrentBranch(t *testing.T) {
	// Branch was deleted concurrently — cannot validate, must refresh
	// and rebuild. The error is terminal, not retryable.
	head := int64(5)
	base := newConflictTestMetadata(t, &head)
	current := newConflictTestMetadata(t, nil)

	_, err := newConflictContext(base, current, MainBranch, nil, true)
	require.Error(t, err)
	assert.ErrorIs(t, err, ErrCommitDiverged)
	assert.False(t, errors.Is(err, ErrCommitFailed),
		"divergence must not be retryable")
}

func TestNewConflictContext_BaseNotInCurrentAncestry(t *testing.T) {
	// base and current both have the branch ref, but current's head
	// ancestry does not reach base's head (forked history, expired
	// base). Must return ErrCommitDiverged, not retryable.
	baseHead := int64(50)
	currentHead := int64(99)
	base := newConflictTestMetadata(t, &baseHead)
	current := newConflictTestMetadata(t, &currentHead)

	_, err := newConflictContext(base, current, MainBranch, nil, true)
	require.Error(t, err)
	assert.ErrorIs(t, err, ErrCommitDiverged)
	assert.False(t, errors.Is(err, ErrCommitFailed),
		"divergent ancestry must not be retryable")
	assert.Contains(t, err.Error(), "ancestry", "message should explain the ancestry gap")
}

func TestValidateDataFilesExist_EmptyInput(t *testing.T) {
	// Empty referencedPaths must short-circuit to nil without
	// touching metadata or the filesystem.
	head := int64(1)
	meta := newConflictTestMetadata(t, &head)
	ctx, err := newConflictContext(meta, meta, MainBranch, nil, true)
	require.NoError(t, err)

	require.NoError(t, validateDataFilesExist(ctx, nil))
	require.NoError(t, validateDataFilesExist(ctx, []string{}))
}

func TestValidateNoNewDeletesForRewrittenFiles_EmptyInputs(t *testing.T) {
	// Empty rewrittenPaths OR empty concurrent snapshots must both
	// short-circuit to nil.
	head := int64(1)
	meta := newConflictTestMetadata(t, &head)
	ctx, err := newConflictContext(meta, meta, MainBranch, nil, true)
	require.NoError(t, err)

	// Empty rewrittenPaths.
	require.NoError(t, validateNoNewDeletesForRewrittenFiles(ctx, nil))
	require.NoError(t, validateNoNewDeletesForRewrittenFiles(ctx, []string{}))

	// Non-empty rewrittenPaths but no concurrent snapshots.
	require.NoError(t, validateNoNewDeletesForRewrittenFiles(ctx, []string{"a.parquet"}))
}

func TestValidateAddedDataFilesMatchingFilter_NoConcurrent(t *testing.T) {
	// With zero concurrent snapshots the validator must return nil
	// regardless of filter.
	head := int64(1)
	meta := newConflictTestMetadata(t, &head)
	ctx, err := newConflictContext(meta, meta, MainBranch, nil, true)
	require.NoError(t, err)

	require.NoError(t, validateAddedDataFilesMatchingFilter(ctx, iceberg.AlwaysTrue{}))
	require.NoError(t, validateAddedDataFilesMatchingFilter(ctx, nil))
}

func TestValidateNoConflictingDataFiles_SnapshotIsolationIsNoOp(t *testing.T) {
	// Under snapshot isolation the validator is a no-op — it must not
	// even attempt to enumerate concurrent snapshots.
	head := int64(1)
	meta := newConflictTestMetadata(t, &head)
	ctx, err := newConflictContext(meta, meta, MainBranch, nil, true)
	require.NoError(t, err)

	require.NoError(t, validateNoConflictingDataFiles(ctx, iceberg.AlwaysTrue{}, IsolationSnapshot))
}
