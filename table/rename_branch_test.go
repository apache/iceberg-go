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
	"encoding/json"
	"fmt"
	"maps"
	"testing"

	"github.com/apache/iceberg-go"
	iceio "github.com/apache/iceberg-go/io"
	"github.com/stretchr/testify/require"
)

type renameBranchTestCatalog struct {
	metadata      Metadata
	reqs          []Requirement
	updates       []Update
	commits       int
	loads         int
	failFirst     bool
	firstConflict func()
}

func (c *renameBranchTestCatalog) CommitTable(_ context.Context, _ Identifier, reqs []Requirement, updates []Update) (Metadata, string, error) {
	c.commits++
	c.reqs = reqs
	c.updates = updates
	if c.failFirst && c.commits == 1 {
		if c.firstConflict != nil {
			c.firstConflict()
		}

		return nil, "", ErrCommitFailed
	}
	for _, req := range reqs {
		if err := req.Validate(c.metadata); err != nil {
			return nil, "", fmt.Errorf("%w: %w", ErrCommitFailed, err)
		}
	}
	builder, err := MetadataBuilderFromBase(c.metadata, "/metadata/current.json")
	if err != nil {
		return nil, "", err
	}
	for _, update := range updates {
		if err := update.Apply(builder); err != nil {
			return nil, "", err
		}
	}
	metadata, err := builder.Build()
	if err != nil {
		return nil, "", err
	}
	c.metadata = metadata

	return metadata, "/metadata/renamed.json", nil
}

func (c *renameBranchTestCatalog) LoadTable(_ context.Context, ident Identifier) (*Table, error) {
	c.loads++

	return New(ident, c.metadata, "/metadata/current.json",
		func(context.Context) (iceio.IO, error) { return iceio.LocalFS{}, nil }, c), nil
}

func newRenameBranchTestTable(t *testing.T) (*Table, *renameBranchTestCatalog) {
	t.Helper()
	builder := builderWithoutChanges(2)
	schemaID := builder.currentSchemaID
	snapshot := Snapshot{
		SnapshotID:   42,
		TimestampMs:  builder.base.LastUpdatedMillis() + 1,
		ManifestList: "/snap-42.avro",
		SchemaID:     &schemaID,
		Summary:      &Summary{Operation: OpAppend},
	}
	require.NoError(t, builder.AddSnapshot(&snapshot))
	require.NoError(t, builder.SetSnapshotRef(MainBranch, 42, BranchRef))
	require.NoError(t, builder.SetSnapshotRef("dev", 42, BranchRef,
		WithMaxRefAgeMs(1000), WithMaxSnapshotAgeMs(2000), WithMinSnapshotsToKeep(3)))
	meta, err := builder.Build()
	require.NoError(t, err)
	cat := &renameBranchTestCatalog{metadata: meta}

	return New(Identifier{"db", "test"}, meta, "/metadata/current.json",
		func(context.Context) (iceio.IO, error) { return iceio.LocalFS{}, nil }, cat), cat
}

func TestRenameBranchPreservesMetadata(t *testing.T) {
	tbl, cat := newRenameBranchTestTable(t)
	originalRefs := maps.Collect(tbl.Metadata().Refs())

	updated, err := tbl.RenameBranch(context.Background(), "dev", "review")
	require.NoError(t, err)
	require.Equal(t, 1, cat.commits)
	require.Len(t, cat.reqs, 4)
	require.Len(t, cat.updates, 2)
	require.Equal(t, "/metadata/renamed.json", updated.MetadataLocation())

	refs := maps.Collect(updated.Metadata().Refs())
	require.NotContains(t, refs, "dev")
	require.Equal(t, originalRefs["dev"], refs["review"])
	require.Equal(t, originalRefs[MainBranch], refs[MainBranch])
	require.Equal(t, int64(42), updated.CurrentSnapshot().SnapshotID)
	require.Contains(t, maps.Collect(tbl.Metadata().Refs()), "dev", "original table is unchanged")
}

func TestRenameBranchCommitWireFormat(t *testing.T) {
	tbl, cat := newRenameBranchTestTable(t)
	_, err := tbl.RenameBranch(t.Context(), "dev", "review")
	require.NoError(t, err)

	require.Len(t, cat.reqs, 4)
	sourceReq, err := json.Marshal(cat.reqs[0])
	require.NoError(t, err)
	require.JSONEq(t, `{"type":"assert-ref-snapshot-id","ref":"dev","snapshot-id":42}`, string(sourceReq))

	destinationReq, err := json.Marshal(cat.reqs[1])
	require.NoError(t, err)
	require.JSONEq(t, `{"type":"assert-ref-snapshot-id","ref":"review","snapshot-id":null}`, string(destinationReq))

	require.Len(t, cat.updates, 2)
	createUpdate, err := json.Marshal(cat.updates[0])
	require.NoError(t, err)
	require.JSONEq(t,
		`{"action":"set-snapshot-ref","ref-name":"review","type":"branch","snapshot-id":42,`+
			`"max-ref-age-ms":1000,"max-snapshot-age-ms":2000,"min-snapshots-to-keep":3}`,
		string(createUpdate),
	)

	removeUpdate, err := json.Marshal(cat.updates[1])
	require.NoError(t, err)
	require.JSONEq(t, `{"action":"remove-snapshot-ref","ref-name":"dev"}`, string(removeUpdate))
}

func TestRenameBranchPreservesOptionalRetention(t *testing.T) {
	for _, tc := range []struct {
		name string
		opts []setSnapshotRefOption
	}{
		{name: "unset"},
		{name: "only max ref age", opts: []setSnapshotRefOption{WithMaxRefAgeMs(1000)}},
		{name: "only min snapshots", opts: []setSnapshotRefOption{WithMinSnapshotsToKeep(2)}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			tbl, cat := newRenameBranchTestTable(t)
			builder, err := MetadataBuilderFromBase(cat.metadata, "/metadata/current.json")
			require.NoError(t, err)
			require.NoError(t, builder.SetSnapshotRef("feature", 42, BranchRef, tc.opts...))
			cat.metadata, err = builder.Build()
			require.NoError(t, err)
			tbl.metadata = cat.metadata

			expected := maps.Collect(tbl.Metadata().Refs())["feature"]
			renamed, err := tbl.RenameBranch(t.Context(), "feature", "renamed")
			require.NoError(t, err)
			refs := maps.Collect(renamed.Metadata().Refs())
			require.Equal(t, expected, refs["renamed"])
			require.NotContains(t, refs, "feature")

			// An unset policy must stay absent in the REST update, not become
			// an explicit default retention value.
			updateJSON, err := json.Marshal(cat.updates[0])
			require.NoError(t, err)
			var fields map[string]json.RawMessage
			require.NoError(t, json.Unmarshal(updateJSON, &fields))
			if expected.MaxRefAgeMs == nil {
				require.NotContains(t, fields, "max-ref-age-ms")
			}
			if expected.MaxSnapshotAgeMs == nil {
				require.NotContains(t, fields, "max-snapshot-age-ms")
			}
			if expected.MinSnapshotsToKeep == nil {
				require.NotContains(t, fields, "min-snapshots-to-keep")
			}
		})
	}
}

func TestRenameBranchRejectsInvalidRefs(t *testing.T) {
	for _, tc := range []struct {
		name, from, to string
		change         func(t *testing.T, c *renameBranchTestCatalog)
	}{
		{name: "missing source", from: "missing", to: "review"},
		{name: "empty source", from: "", to: "review"},
		{name: "empty destination", from: "dev", to: ""},
		{name: "rename main", from: MainBranch, to: "review"},
		{name: "replace main", from: "dev", to: MainBranch},
		{name: "same name", from: "dev", to: "dev"},
		{name: "existing destination", from: "dev", to: "review", change: func(t *testing.T, cat *renameBranchTestCatalog) {
			t.Helper()
			setRenameBranchTestRef(t, cat, "review", 42, BranchRef)
		}},
		{name: "existing destination tag", from: "dev", to: "review", change: func(t *testing.T, cat *renameBranchTestCatalog) {
			t.Helper()
			setRenameBranchTestRef(t, cat, "review", 42, TagRef)
		}},
		{name: "source is tag", from: "dev", to: "review", change: func(t *testing.T, cat *renameBranchTestCatalog) {
			t.Helper()
			setRenameBranchTestRef(t, cat, "dev", 42, TagRef)
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			tbl, cat := newRenameBranchTestTable(t)
			if tc.change != nil {
				tc.change(t, cat)
				tbl.metadata = cat.metadata
			}
			_, err := tbl.RenameBranch(context.Background(), tc.from, tc.to)
			require.ErrorIs(t, err, iceberg.ErrInvalidArgument)
			require.Zero(t, cat.commits)
		})
	}
}

func TestRenameBranchPreservesConcurrentUnrelatedRef(t *testing.T) {
	tbl, cat := newRenameBranchTestTable(t)
	setRenameBranchTestRef(t, cat, "other", 42, TagRef)

	updated, err := tbl.RenameBranch(t.Context(), "dev", "review")
	require.NoError(t, err)
	refs := maps.Collect(updated.Metadata().Refs())
	require.NotContains(t, refs, "dev")
	require.Contains(t, refs, "review")
	require.Equal(t, TagRef, refs["other"].Type)
}

func TestRenameBranchRejectsConcurrentSourceMove(t *testing.T) {
	tbl, cat := newRenameBranchTestTable(t)
	builder, err := MetadataBuilderFromBase(cat.metadata, "/metadata/current.json")
	require.NoError(t, err)
	schemaID := builder.currentSchemaID
	parentID := int64(42)
	snapshot := Snapshot{
		SnapshotID:       43,
		ParentSnapshotID: &parentID,
		SequenceNumber:   builder.currentLastSequenceNumber() + 1,
		TimestampMs:      builder.base.LastUpdatedMillis() + 1,
		ManifestList:     "/snap-43.avro",
		SchemaID:         &schemaID,
		Summary:          &Summary{Operation: OpAppend},
	}
	require.NoError(t, builder.AddSnapshot(&snapshot))
	require.NoError(t, builder.SetSnapshotRef("dev", 43, BranchRef))
	cat.metadata, err = builder.Build()
	require.NoError(t, err)

	_, err = tbl.RenameBranch(context.Background(), "dev", "review")
	require.Error(t, err)
	require.Equal(t, 1, cat.commits)
	require.Len(t, cat.reqs, 4)
	refs := maps.Collect(cat.metadata.Refs())
	require.Equal(t, int64(43), refs["dev"].SnapshotID)
	require.NotContains(t, refs, "review")
}

func TestRenameBranchRejectsConcurrentSourceTypeChange(t *testing.T) {
	tbl, cat := newRenameBranchTestTable(t)
	setRenameBranchTestRef(t, cat, "dev", 42, TagRef)

	_, err := tbl.RenameBranch(t.Context(), "dev", "review")
	require.Error(t, err)
	require.Equal(t, 1, cat.commits)
	refs := maps.Collect(cat.metadata.Refs())
	require.Equal(t, TagRef, refs["dev"].Type)
	require.NotContains(t, refs, "review")
}

func TestRenameBranchRejectsConcurrentSourceDeletion(t *testing.T) {
	tbl, cat := newRenameBranchTestTable(t)
	builder, err := MetadataBuilderFromBase(cat.metadata, "/metadata/current.json")
	require.NoError(t, err)
	require.NoError(t, builder.RemoveSnapshotRef("dev"))
	cat.metadata, err = builder.Build()
	require.NoError(t, err)

	_, err = tbl.RenameBranch(t.Context(), "dev", "review")
	require.Error(t, err)
	require.Equal(t, 1, cat.commits)
	refs := maps.Collect(cat.metadata.Refs())
	require.NotContains(t, refs, "dev")
	require.NotContains(t, refs, "review")
}

func TestRenameBranchRejectsConcurrentDestinationCreation(t *testing.T) {
	tbl, cat := newRenameBranchTestTable(t)
	setRenameBranchTestRef(t, cat, "review", 42, TagRef)

	_, err := tbl.RenameBranch(context.Background(), "dev", "review")
	require.Error(t, err)
	require.Equal(t, 1, cat.commits)
	require.Len(t, cat.reqs, 4, "source and destination checks must both reach the catalog")
	refs := maps.Collect(cat.metadata.Refs())
	require.Contains(t, refs, "dev")
	require.Equal(t, TagRef, refs["review"].Type)
}

func setRenameBranchTestRef(t *testing.T, cat *renameBranchTestCatalog, name string, id int64, refType RefType) {
	t.Helper()
	builder, err := MetadataBuilderFromBase(cat.metadata, "/metadata/current.json")
	require.NoError(t, err)
	require.NoError(t, builder.SetSnapshotRef(name, id, refType))
	cat.metadata, err = builder.Build()
	require.NoError(t, err)
}

func TestRenameBranchRespectsCancellation(t *testing.T) {
	tbl, cat := newRenameBranchTestTable(t)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	_, err := tbl.RenameBranch(ctx, "dev", "review")
	require.ErrorIs(t, err, context.Canceled)
	require.Zero(t, cat.commits)
}

func TestTransactionRenameBranchWithOtherUpdates(t *testing.T) {
	tbl, cat := newRenameBranchTestTable(t)
	tx := tbl.NewTransaction()

	require.NoError(t, tx.SetProperties(iceberg.Properties{"combined": "true"}))
	require.NoError(t, tx.RenameBranch("dev", "review"))
	require.Zero(t, cat.commits, "staging must not commit the table")

	updated, err := tx.Commit(t.Context())
	require.NoError(t, err)
	require.Equal(t, 1, cat.commits)
	require.Equal(t, "true", updated.Properties()["combined"])
	require.Len(t, cat.updates, 3)

	refs := maps.Collect(updated.Metadata().Refs())
	require.NotContains(t, refs, "dev")
	require.Contains(t, refs, "review")
	require.Equal(t, int64(42), refs["review"].SnapshotID)
}

func TestTransactionRenameBranchFailedStageKeepsEarlierUpdates(t *testing.T) {
	tbl, cat := newRenameBranchTestTable(t)
	tx := tbl.NewTransaction()
	require.NoError(t, tx.SetProperties(iceberg.Properties{"keep": "yes"}))

	require.ErrorIs(t, tx.RenameBranch("dev", MainBranch), iceberg.ErrInvalidArgument)
	updated, err := tx.Commit(t.Context())
	require.NoError(t, err)
	require.Equal(t, "yes", updated.Properties()["keep"])
	require.Len(t, cat.updates, 1)
	require.Contains(t, maps.Collect(updated.Metadata().Refs()), "dev")
}

func enableRenameBranchRetries(t *testing.T, tbl *Table, cat *renameBranchTestCatalog) {
	t.Helper()
	builder, err := MetadataBuilderFromBase(cat.metadata, "/metadata/current.json")
	require.NoError(t, err)
	require.NoError(t, builder.SetProperties(iceberg.Properties{
		CommitNumRetriesKey:     "2",
		CommitMinRetryWaitMsKey: "1",
		CommitMaxRetryWaitMsKey: "2",
	}))
	meta, err := builder.Build()
	require.NoError(t, err)
	cat.metadata = meta
	tbl.metadata = meta
}

func TestRenameBranchRetriesUnrelatedConflict(t *testing.T) {
	tbl, cat := newRenameBranchTestTable(t)
	enableRenameBranchRetries(t, tbl, cat)
	cat.failFirst = true

	updated, err := tbl.RenameBranch(t.Context(), "dev", "review")
	require.NoError(t, err)
	require.Equal(t, 2, cat.commits)
	require.Equal(t, 1, cat.loads)
	refs := maps.Collect(updated.Metadata().Refs())
	require.NotContains(t, refs, "dev")
	require.Equal(t, int64(42), refs["review"].SnapshotID)
}

func TestRenameBranchRetryCannotOverwriteNewDestination(t *testing.T) {
	tbl, cat := newRenameBranchTestTable(t)
	enableRenameBranchRetries(t, tbl, cat)
	cat.failFirst = true
	cat.firstConflict = func() {
		setRenameBranchTestRef(t, cat, "review", 42, TagRef)
	}

	_, err := tbl.RenameBranch(t.Context(), "dev", "review")
	require.ErrorIs(t, err, ErrTransactionUnusable)
	require.Equal(t, 1, cat.commits, "the pinned destination must fail before a retry")
	require.Equal(t, 1, cat.loads)
	refs := maps.Collect(cat.metadata.Refs())
	require.Contains(t, refs, "dev")
	require.Equal(t, TagRef, refs["review"].Type)
}
