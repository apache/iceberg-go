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
	"maps"
	"testing"

	"github.com/apache/iceberg-go"
	"github.com/stretchr/testify/require"
)

type renameBranchTestCatalog struct {
	metadata Metadata
	reqs     []Requirement
	updates  []Update
	commits  int
}

func (c *renameBranchTestCatalog) CommitTable(_ context.Context, _ Identifier, reqs []Requirement, updates []Update) (Metadata, string, error) {
	c.commits++
	c.reqs = reqs
	c.updates = updates
	for _, req := range reqs {
		if err := req.Validate(c.metadata); err != nil {
			return nil, "", err
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
	return &Table{identifier: ident, metadata: c.metadata, metadataLocation: "/metadata/current.json", cat: c}, nil
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

	return &Table{identifier: Identifier{"db", "test"}, metadata: meta, metadataLocation: "/metadata/current.json", cat: cat}, cat
}

func TestRenameBranchPreservesMetadata(t *testing.T) {
	tbl, cat := newRenameBranchTestTable(t)
	originalRefs := maps.Collect(tbl.Metadata().Refs())

	updated, err := tbl.RenameBranch(context.Background(), "dev", "review")
	require.NoError(t, err)
	require.Equal(t, 1, cat.commits)
	require.Len(t, cat.reqs, 3)
	require.Len(t, cat.updates, 2)
	require.Equal(t, "/metadata/renamed.json", updated.MetadataLocation())

	refs := maps.Collect(updated.Metadata().Refs())
	require.NotContains(t, refs, "dev")
	require.Equal(t, originalRefs["dev"], refs["review"])
	require.Equal(t, originalRefs[MainBranch], refs[MainBranch])
	require.Equal(t, int64(42), updated.CurrentSnapshot().SnapshotID)
	require.Contains(t, maps.Collect(tbl.Metadata().Refs()), "dev", "original table is unchanged")
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

func TestRenameBranchRejectsConcurrentSourceMove(t *testing.T) {
	tbl, cat := newRenameBranchTestTable(t)
	builder, err := MetadataBuilderFromBase(cat.metadata, "/metadata/current.json")
	require.NoError(t, err)
	schemaID := builder.currentSchemaID
	parentID := int64(42)
	snapshot := Snapshot{SnapshotID: 43, ParentSnapshotID: &parentID, TimestampMs: builder.base.LastUpdatedMillis() + 1,
		ManifestList: "/snap-43.avro", SchemaID: &schemaID, Summary: &Summary{Operation: OpAppend}}
	require.NoError(t, builder.AddSnapshot(&snapshot))
	require.NoError(t, builder.SetSnapshotRef("dev", 43, BranchRef))
	cat.metadata, err = builder.Build()
	require.NoError(t, err)

	_, err = tbl.RenameBranch(context.Background(), "dev", "review")
	require.Error(t, err)
	require.Equal(t, 1, cat.commits)
	require.Len(t, cat.reqs, 3)
	refs := maps.Collect(cat.metadata.Refs())
	require.Equal(t, int64(43), refs["dev"].SnapshotID)
	require.NotContains(t, refs, "review")
}

func TestRenameBranchRejectsConcurrentDestinationCreation(t *testing.T) {
	tbl, cat := newRenameBranchTestTable(t)
	setRenameBranchTestRef(t, cat, "review", 42, TagRef)

	_, err := tbl.RenameBranch(context.Background(), "dev", "review")
	require.Error(t, err)
	require.Equal(t, 1, cat.commits)
	require.Len(t, cat.reqs, 3, "source and destination checks must both reach the catalog")
	refs := maps.Collect(cat.metadata.Refs())
	require.Contains(t, refs, "dev")
	require.Equal(t, TagRef, refs["review"].SnapshotRefType)
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
