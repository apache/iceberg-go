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

package main

import (
	"context"
	"strings"
	"testing"

	"github.com/apache/iceberg-go"
	icebergio "github.com/apache/iceberg-go/io"
	"github.com/apache/iceberg-go/table"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// CLI test catalog validates the same requirements as an optimistic catalog.
type branchPromotionCatalog struct {
	*branchTagCommitCatalog
	meta table.Metadata
}

func (c *branchPromotionCatalog) LoadTable(_ context.Context, ident table.Identifier) (*table.Table, error) {
	return table.New(ident, c.meta, "",
		func(context.Context) (icebergio.IO, error) { return icebergio.LocalFS{}, nil }, c), nil
}

func (c *branchPromotionCatalog) CommitTable(_ context.Context, _ table.Identifier,
	reqs []table.Requirement, updates []table.Update,
) (table.Metadata, string, error) {
	for _, r := range reqs {
		if err := r.Validate(c.meta); err != nil {
			return nil, "", err
		}
	}
	changed, err := table.UpdateTableMetadata(c.meta, updates, "")
	if err != nil {
		return nil, "", err
	}
	c.meta = changed

	return changed, "", nil
}

func newBranchPromotionTestCatalog(t *testing.T) *branchPromotionCatalog {
	t.Helper()
	original, err := table.ParseMetadataBytes([]byte(branchTagTestMetadata))
	require.NoError(t, err)
	b, err := table.MetadataBuilderFromBase(original, "")
	require.NoError(t, err)
	parent := int64(5000)
	require.NoError(t, b.AddSnapshot(&table.Snapshot{
		SnapshotID: 6000, ParentSnapshotID: &parent,
		SequenceNumber: 2, TimestampMs: original.SnapshotByID(5000).TimestampMs + 1000,
		Summary: &table.Summary{Operation: table.OpAppend},
	}))
	require.NoError(t, b.SetSnapshotRef("audit", 6000, table.BranchRef))
	meta, err := b.Build()
	require.NoError(t, err)

	return &branchPromotionCatalog{meta: meta}
}

type branchPromotionOutput struct {
	textOutput
	message string
	lastErr error
}

func (o *branchPromotionOutput) Text(s string)   { o.message = s }
func (o *branchPromotionOutput) Error(err error) { o.lastErr = err }

func TestRunBranchFastForward(t *testing.T) {
	cat := newBranchPromotionTestCatalog(t)
	out := &branchPromotionOutput{}
	runBranchFastForward(context.Background(), out, cat, &BranchFastForwardCmd{
		TableID: "db.events", BranchName: "feature-branch", SourceRef: "audit", Yes: true,
	})
	require.NoError(t, out.lastErr)
	assert.Equal(t, int64(6000), cat.meta.SnapshotByName("feature-branch").SnapshotID)
	assert.Equal(t, int64(6000), cat.meta.SnapshotByName("audit").SnapshotID)
	assert.True(t, strings.Contains(out.message, "6000"))

	// A tag can be a source and a missing target branch can be created.
	cat = newBranchPromotionTestCatalog(t)
	out = &branchPromotionOutput{}
	runBranchFastForward(context.Background(), out, cat, &BranchFastForwardCmd{
		TableID: "db.events", BranchName: "new-branch", SourceRef: "v1.0", Yes: true,
	})
	require.NoError(t, out.lastErr)
	assert.Equal(t, int64(5000), cat.meta.SnapshotByName("new-branch").SnapshotID)
}

func TestRunBranchReplace(t *testing.T) {
	cat := newBranchPromotionTestCatalog(t)
	out := &branchPromotionOutput{}
	runBranchReplace(context.Background(), out, cat, &BranchReplaceCmd{
		TableID: "db.events", BranchName: "feature-branch", SnapshotID: 6000, Yes: true,
	})
	require.NoError(t, out.lastErr)
	assert.Equal(t, int64(6000), cat.meta.SnapshotByName("feature-branch").SnapshotID)

	out = &branchPromotionOutput{}
	code := captureBranchTagExit(func() {
		runBranchReplace(context.Background(), out, cat, &BranchReplaceCmd{
			TableID: "db.events", BranchName: "missing", SnapshotID: 999, Yes: true,
		})
	})
	assert.Equal(t, 1, code)
	require.Error(t, out.lastErr)
	assert.ErrorIs(t, out.lastErr, iceberg.ErrInvalidArgument)
}

func TestRunBranchFastForwardRejectsInvalidNameBeforeLoading(t *testing.T) {
	cat := &loadTrackingCatalog{}
	out := &branchPromotionOutput{}
	code := captureBranchTagExit(func() {
		runBranchFastForward(context.Background(), out, cat, &BranchFastForwardCmd{
			TableID: "db.events", BranchName: "  ", SourceRef: "audit", Yes: true,
		})
	})
	assert.Equal(t, 1, code)
	require.Error(t, out.lastErr)
	assert.Zero(t, cat.loadCount)
}
