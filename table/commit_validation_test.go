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

// Wiring smokes for the PR 2.4 doCommit pre-flight. These prove the
// transaction's validators slice is actually drained before
// cat.CommitTable, and that ErrCommitDiverged is terminal.
// Behavioral rejection coverage of the individual validators lives
// in conflict_validation_test.go (PR 2.3).

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"

	"github.com/apache/iceberg-go"
	iceio "github.com/apache/iceberg-go/io"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type countingCatalog struct {
	metadata Metadata
	attempts atomic.Int32
}

func (c *countingCatalog) LoadTable(context.Context, Identifier) (*Table, error) {
	return nil, nil
}

func (c *countingCatalog) CommitTable(_ context.Context, _ Identifier, _ []Requirement, updates []Update) (Metadata, string, error) {
	c.attempts.Add(1)
	meta, err := UpdateTableMetadata(c.metadata, updates, "")
	if err != nil {
		return nil, "", err
	}
	c.metadata = meta

	return meta, "", nil
}

func newValidationTestTable(t *testing.T, props iceberg.Properties) (*Table, *countingCatalog) {
	t.Helper()

	schema := iceberg.NewSchema(0,
		iceberg.NestedField{ID: 1, Name: "id", Type: iceberg.PrimitiveTypes.Int64, Required: true},
	)
	merged := iceberg.Properties{PropertyFormatVersion: "2"}
	for k, v := range props {
		merged[k] = v
	}
	meta, err := NewMetadata(schema, iceberg.UnpartitionedSpec, UnsortedSortOrder, "file:///tmp/validation-test", merged)
	require.NoError(t, err)

	// Graft a synthetic snapshot so the main branch has a head —
	// doCommit's pre-flight short-circuits on missing-branch, and we
	// want the validator to actually run.
	head := int64(100)
	builder, err := MetadataBuilderFromBase(meta, "")
	require.NoError(t, err)
	require.NoError(t, builder.AddSnapshot(&Snapshot{
		SnapshotID:     head,
		SequenceNumber: 1,
		TimestampMs:    meta.LastUpdatedMillis() + 1,
		Summary:        &Summary{Operation: OpAppend},
	}))
	require.NoError(t, builder.SetSnapshotRef(MainBranch, head, BranchRef))
	meta, err = builder.Build()
	require.NoError(t, err)

	cat := &countingCatalog{metadata: meta}
	tbl := New(Identifier{"db", "validation"}, meta, "file:///tmp/validation-test/metadata/v1.metadata.json",
		func(context.Context) (iceio.IO, error) { return iceio.LocalFS{}, nil }, cat)

	return tbl, cat
}

// TestDoCommit_ValidatorRejectsTerminatesPreFlight proves the pre-
// flight wiring: when a validator rejects with a wrapped
// ErrCommitFailed sentinel, doCommit surfaces it and never reaches
// cat.CommitTable. The sentinel is structurally retryable; PR 2.5
// will add refresh-and-replay that actually makes the retry
// meaningful.
func TestDoCommit_ValidatorRejectsTerminatesPreFlight(t *testing.T) {
	tbl, cat := newValidationTestTable(t, nil)

	reject := func(cc *conflictContext) error { return ErrConflictingDataFiles }

	_, err := tbl.doCommit(context.Background(), nil, nil,
		withCommitBranch(MainBranch),
		withCommitValidators(reject))
	require.Error(t, err)
	assert.ErrorIs(t, err, ErrConflictingDataFiles)
	assert.ErrorIs(t, err, ErrCommitFailed)
	assert.Equal(t, int32(0), cat.attempts.Load(),
		"validator rejected before CommitTable was called")
}

// TestDoCommit_DivergedSentinelIsTerminal: ErrCommitDiverged does not
// wrap ErrCommitFailed, so doCommit returns it without entering the
// retry loop. Mirrors Java's ValidationException.
func TestDoCommit_DivergedSentinelIsTerminal(t *testing.T) {
	tbl, cat := newValidationTestTable(t, iceberg.Properties{
		CommitNumRetriesKey:     "4",
		CommitMinRetryWaitMsKey: "1",
		CommitMaxRetryWaitMsKey: "2",
	})

	diverge := func(cc *conflictContext) error { return ErrCommitDiverged }

	_, err := tbl.doCommit(context.Background(), nil, nil,
		withCommitBranch(MainBranch),
		withCommitValidators(diverge))
	require.Error(t, err)
	assert.ErrorIs(t, err, ErrCommitDiverged)
	assert.False(t, errors.Is(err, ErrCommitFailed), "divergence must not be retryable")
	assert.Equal(t, int32(0), cat.attempts.Load(),
		"diverged validator must not retry or call CommitTable")
}

// TestRowDelta_RegistersValidatorOnTransaction proves RowDelta.Commit
// registers its validator on txn.validators so the eventual
// Transaction.Commit → doCommit picks it up.
func TestRowDelta_RegistersValidatorOnTransaction(t *testing.T) {
	tbl, _ := newValidationTestTable(t, nil)
	txn := tbl.NewTransaction()
	rd := txn.NewRowDelta(nil)
	rd.AddDeletes(newTestPosDeleteFile(t, "pos-1.parquet", nil))

	require.NoError(t, rd.Commit(context.Background()))
	require.NotEmpty(t, txn.validators,
		"RowDelta.Commit must append at least one validator to the transaction")
}

// TestFastAppend_CommitLeavesValidatorsEmpty asserts that
// fastAppendFiles.commit() does not register a validator on
// txn.validators. Fast-appends are commutative and need no
// conflict validation; accumulating no-op closures is wasteful.
func TestFastAppend_CommitLeavesValidatorsEmpty(t *testing.T) {
	trackIO := newTrackingIO()
	spec := iceberg.NewPartitionSpec()
	txn := createTestTransaction(t, trackIO, spec)

	sp := newFastAppendFilesProducer(OpAppend, txn, trackIO, nil, nil)
	sp.appendDataFile(newTestDataFile(t, spec, "file://data.parquet", nil))

	_, _, err := sp.commit(context.Background())
	require.NoError(t, err)
	assert.Empty(t, txn.validators,
		"fastAppendFiles.commit must not register a validator on the transaction")
}

// TestMergeAppend_CommitLeavesValidatorsEmpty pins the same behaviour
// for mergeAppendFiles, which inherits needsValidation() via embedding.
func TestMergeAppend_CommitLeavesValidatorsEmpty(t *testing.T) {
	trackIO := newTrackingIO()
	spec := iceberg.NewPartitionSpec()
	txn := createTestTransaction(t, trackIO, spec)

	sp := newMergeAppendFilesProducer(OpAppend, txn, trackIO, nil, nil)
	sp.appendDataFile(newTestDataFile(t, spec, "file://data.parquet", nil))

	_, _, err := sp.commit(context.Background())
	require.NoError(t, err)
	assert.Empty(t, txn.validators,
		"mergeAppendFiles.commit must not register a validator on the transaction")
}
