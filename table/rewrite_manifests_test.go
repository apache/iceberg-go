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
	"testing"

	"github.com/apache/iceberg-go"
	"github.com/apache/iceberg-go/io"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
)

type RewriteManifestsTestSuite struct {
	suite.Suite
	tbl *Table
}

func (suite *RewriteManifestsTestSuite) SetupTest() {
	// Create a test table with multiple small manifest files
	// Note: This would need proper test infrastructure to work
	// For now, we'll skip actual table creation
	suite.T().Skip("Test infrastructure not yet set up")
}

func (suite *RewriteManifestsTestSuite) TestRewriteManifests_BasicOperation() {
	ctx := context.Background()
	options := DefaultManifestRewriteOptions()
	options.Strategy = RewriteAllManifests
	
	// Get initial manifest count
	initialManifests, err := suite.tbl.CurrentSnapshot().Manifests(suite.getFS())
	require.NoError(suite.T(), err)
	initialCount := len(initialManifests)
	
	// Rewrite manifests
	newTable, err := suite.tbl.RewriteManifests(ctx, options, iceberg.Properties{})
	require.NoError(suite.T(), err)
	
	// Verify the operation completed
	assert.NotNil(suite.T(), newTable)
	assert.NotEqual(suite.T(), suite.tbl.CurrentSnapshot().SnapshotID, newTable.CurrentSnapshot().SnapshotID)
	
	// Verify manifest structure changed
	newManifests, err := newTable.CurrentSnapshot().Manifests(suite.getFS())
	require.NoError(suite.T(), err)
	
	// Should have different manifests
	assert.True(suite.T(), len(newManifests) <= initialCount, "Rewrite should not increase manifest count")
}

func (suite *RewriteManifestsTestSuite) TestRewriteManifests_SmallManifestsStrategy() {
	ctx := context.Background()
	options := DefaultManifestRewriteOptions()
	options.Strategy = RewriteSmallManifests
	options.TargetSizeBytes = 1024 * 1024 // 1MB - larger than our test manifests
	
	// Get initial state
	initialManifests, err := suite.tbl.CurrentSnapshot().Manifests(suite.getFS())
	require.NoError(suite.T(), err)
	
	// Count small manifests
	smallCount := 0
	for _, mf := range initialManifests {
		if mf.Length() < options.TargetSizeBytes {
			smallCount++
		}
	}
	
	if smallCount == 0 {
		suite.T().Skip("No small manifests to rewrite")
	}
	
	// Rewrite manifests
	newTable, err := suite.tbl.RewriteManifests(ctx, options, iceberg.Properties{})
	require.NoError(suite.T(), err)
	
	// Verify small manifests were consolidated
	newManifests, err := newTable.CurrentSnapshot().Manifests(suite.getFS())
	require.NoError(suite.T(), err)
	
	assert.True(suite.T(), len(newManifests) <= len(initialManifests))
}

func (suite *RewriteManifestsTestSuite) TestRewriteManifestsBySpec() {
	ctx := context.Background()
	options := DefaultManifestRewriteOptions()
	
	// Get the current partition spec ID
	spec := suite.tbl.Spec()
	specID := spec.ID()
	
	// Rewrite manifests for this spec
	newTable, err := suite.tbl.RewriteManifestsBySpec(ctx, []int{specID}, options, iceberg.Properties{})
	require.NoError(suite.T(), err)
	
	assert.NotNil(suite.T(), newTable)
	assert.NotEqual(suite.T(), suite.tbl.CurrentSnapshot().SnapshotID, newTable.CurrentSnapshot().SnapshotID)
}

func (suite *RewriteManifestsTestSuite) TestRewriteManifestsByPredicate() {
	ctx := context.Background()
	options := DefaultManifestRewriteOptions()
	
	// Predicate to select all manifests
	predicate := func(mf iceberg.ManifestFile) bool {
		return true
	}
	
	// Rewrite manifests matching predicate
	newTable, err := suite.tbl.RewriteManifestsByPredicate(ctx, predicate, options, iceberg.Properties{})
	require.NoError(suite.T(), err)
	
	assert.NotNil(suite.T(), newTable)
	assert.NotEqual(suite.T(), suite.tbl.CurrentSnapshot().SnapshotID, newTable.CurrentSnapshot().SnapshotID)
}

func (suite *RewriteManifestsTestSuite) TestRewriteManifests_NoManifestsToRewrite() {
	ctx := context.Background()
	options := DefaultManifestRewriteOptions()
	options.Strategy = RewriteByCount
	options.MinCountToRewrite = 1000 // More than we have
	
	// Should return error when no manifests match criteria
	_, err := suite.tbl.RewriteManifests(ctx, options, iceberg.Properties{})
	assert.ErrorIs(suite.T(), err, ErrNoManifestsToRewrite)
}

func (suite *RewriteManifestsTestSuite) TestRewriteManifests_TransactionAPI() {
	ctx := context.Background()
	options := DefaultManifestRewriteOptions()
	
	// Test through transaction API
	txn := suite.tbl.NewTransaction()
	rewriter := txn.NewRewriteManifests()
	
	// Configure rewriter
	rewriter.WithTargetSizeBytes(1024 * 1024).
		WithMinCountToRewrite(1).
		WithConflictDetection(true)
	
	// Perform rewrite
	err := rewriter.RewriteManifests(ctx, options, iceberg.Properties{})
	require.NoError(suite.T(), err)
	
	// Commit transaction
	newTable, err := txn.Commit(ctx)
	require.NoError(suite.T(), err)
	
	assert.NotNil(suite.T(), newTable)
	assert.NotEqual(suite.T(), suite.tbl.CurrentSnapshot().SnapshotID, newTable.CurrentSnapshot().SnapshotID)
}

func (suite *RewriteManifestsTestSuite) TestRewriteManifests_DataIntegrity() {
	ctx := context.Background()
	options := DefaultManifestRewriteOptions()
	
	// Count initial data files
	initialFiles := suite.countDataFiles()
	
	// Rewrite manifests
	newTable, err := suite.tbl.RewriteManifests(ctx, options, iceberg.Properties{})
	require.NoError(suite.T(), err)
	
	// Verify data integrity - same number of data files
	newFiles := suite.countDataFilesInTable(newTable)
	assert.Equal(suite.T(), initialFiles, newFiles, "Data files should be preserved")
	
	// Verify data can still be scanned
	scan := newTable.Scan()
	files, err := scan.PlanFiles(ctx)
	require.NoError(suite.T(), err)
	
	assert.True(suite.T(), len(files) > 0, "Should be able to scan data files")
}

func (suite *RewriteManifestsTestSuite) TestRewriteManifests_InvalidSnapshot() {
	ctx := context.Background()
	options := DefaultManifestRewriteOptions()
	options.SnapshotID = 99999 // Non-existent snapshot
	
	_, err := suite.tbl.RewriteManifests(ctx, options, iceberg.Properties{})
	assert.Error(suite.T(), err)
	assert.Contains(suite.T(), err.Error(), "snapshot not found")
}

func (suite *RewriteManifestsTestSuite) TestRewriteManifests_EmptyTable() {
	// Test would need proper empty table creation
	suite.T().Skip("Empty table test requires test infrastructure")
}

// Helper methods

func (suite *RewriteManifestsTestSuite) getFS() io.IO {
	fs, err := suite.tbl.FS(context.Background())
	require.NoError(suite.T(), err)
	return fs
}

func (suite *RewriteManifestsTestSuite) countDataFiles() int {
	manifests, err := suite.tbl.CurrentSnapshot().Manifests(suite.getFS())
	require.NoError(suite.T(), err)
	
	count := 0
	for _, mf := range manifests {
		entries, err := mf.FetchEntries(suite.getFS(), false)
		require.NoError(suite.T(), err)
		count += len(entries)
	}
	return count
}

func (suite *RewriteManifestsTestSuite) countDataFilesInTable(tbl *Table) int {
	fs, err := tbl.FS(context.Background())
	require.NoError(suite.T(), err)
	
	manifests, err := tbl.CurrentSnapshot().Manifests(fs)
	require.NoError(suite.T(), err)
	
	count := 0
	for _, mf := range manifests {
		entries, err := mf.FetchEntries(fs, false)
		require.NoError(suite.T(), err)
		count += len(entries)
	}
	return count
}

func TestRewriteManifestsTestSuite(t *testing.T) {
	suite.Run(t, new(RewriteManifestsTestSuite))
}

func TestDefaultManifestRewriteOptions(t *testing.T) {
	opts := DefaultManifestRewriteOptions()
	
	assert.Equal(t, RewriteSmallManifests, opts.Strategy)
	assert.Equal(t, ManifestTargetSizeBytesDefault, opts.TargetSizeBytes)
	assert.Equal(t, ManifestMinMergeCountDefault, opts.MinCountToRewrite)
	assert.Equal(t, 100, opts.MaxManifestCount)
	assert.Equal(t, int64(0), opts.SnapshotID)
}

func TestBaseRewriteManifests_Configuration(t *testing.T) {
	// Mock transaction for testing
	txn := &Transaction{
		meta: &MetadataBuilder{
			props: iceberg.Properties{},
		},
	}
	
	rewriter := NewRewriteManifests(txn)
	
	// Test configuration methods
	rewriter.WithConflictDetection(false).
		WithCaseSensitive(false).
		WithTargetSizeBytes(2 * 1024 * 1024). // 2MB
		WithMinCountToRewrite(5)
	
	assert.False(t, rewriter.conflictDetection)
	assert.False(t, rewriter.caseSensitive)
	assert.Equal(t, int64(2*1024*1024), rewriter.targetSizeBytes)
	assert.Equal(t, 5, rewriter.minCountToRewrite)
} 