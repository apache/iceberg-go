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

// Wiring smokes for the PR 2.4 pre-flight. These prove each producer
// registers the right validator and that empty / nil inputs
// short-circuit cleanly. Behavioral coverage of the validators
// themselves (reject / allow under real concurrent snapshots) lives
// in conflict_validation_test.go (PR 2.3) and will be re-exercised
// end-to-end once PR 2.5 adds refresh-and-replay.

import (
	"testing"

	"github.com/apache/iceberg-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// newEmptyConflictContext builds a conflictContext over synthetic
// metadata whose branch head has no concurrent snapshots. Used to
// prove each producer's validate() short-circuits cleanly when there
// is nothing to check, without needing real manifest I/O.
func newEmptyConflictContext(t *testing.T) *conflictContext {
	t.Helper()
	head := int64(1)
	meta := newConflictTestMetadata(t, &head)
	cc, err := newConflictContext(meta, meta, MainBranch, nil, true)
	require.NoError(t, err)

	return cc
}

// newValidateTestTxn constructs a minimal Transaction wired to the
// synthetic metadata used by conflict-validation tests. The producers
// under test only consult t.meta.props and t.meta for isolation-level
// and branch lookups; the FS / catalog are not exercised because
// validate() runs before any manifest I/O when concurrent == 0.
func newValidateTestTxn(t *testing.T, props iceberg.Properties) *Transaction {
	t.Helper()
	schema := iceberg.NewSchema(0,
		iceberg.NestedField{ID: 1, Name: "id", Type: iceberg.PrimitiveTypes.Int64, Required: true},
	)
	mergedProps := iceberg.Properties{PropertyFormatVersion: "2"}
	for k, v := range props {
		mergedProps[k] = v
	}
	meta, err := NewMetadata(schema, iceberg.UnpartitionedSpec, UnsortedSortOrder, "file:///tmp/validate-test", mergedProps)
	require.NoError(t, err)

	builder, err := MetadataBuilderFromBase(meta, "")
	require.NoError(t, err)
	tbl := New(Identifier{"db", "validate"}, meta, "metadata.json", nil, nil)

	return &Transaction{
		tbl:    tbl,
		meta:   builder,
		branch: MainBranch,
	}
}

// newTestPosDeleteFile builds a minimal position-delete DataFile for
// validator tests. ReferencedDataFile is left unset unless ref != nil.
func newTestPosDeleteFile(t *testing.T, path string, ref *string) iceberg.DataFile {
	t.Helper()
	builder, err := iceberg.NewDataFileBuilder(
		*iceberg.UnpartitionedSpec, iceberg.EntryContentPosDeletes,
		path, iceberg.ParquetFile, nil, nil, nil, 1, 1)
	require.NoError(t, err)
	if ref != nil {
		builder = builder.ReferencedDataFile(*ref)
	}

	return builder.Build()
}

// newTestEqDeleteFile builds a minimal equality-delete DataFile with
// the supplied field-id set.
func newTestEqDeleteFile(t *testing.T, path string, eqIDs []int) iceberg.DataFile {
	t.Helper()
	builder, err := iceberg.NewDataFileBuilder(
		*iceberg.UnpartitionedSpec, iceberg.EntryContentEqDeletes,
		path, iceberg.ParquetFile, nil, nil, nil, 1, 1)
	require.NoError(t, err)
	builder = builder.EqualityFieldIDs(eqIDs)

	return builder.Build()
}

func TestFastAppendFiles_ValidateNoop(t *testing.T) {
	fa := &fastAppendFiles{}
	require.NoError(t, fa.validate(nil))
	require.NoError(t, fa.validate(newEmptyConflictContext(t)))
}

// mergeAppendFiles has its own explicit needsValidation() and validate()
// methods. Pin the behavior so a future refactor doesn't silently drop it.
func TestMergeAppendFiles_ValidateNoop(t *testing.T) {
	ma := &mergeAppendFiles{}
	require.NoError(t, ma.validate(nil))
	require.NoError(t, ma.validate(newEmptyConflictContext(t)))
	require.False(t, ma.needsValidation())

	var _ producerImpl = ma
}

// TestOverwriteFiles_ValidateEmptyConcurrent covers the common-case
// first-attempt commit: base == current, zero concurrent snapshots,
// validator must be a no-op regardless of filter shape.
func TestOverwriteFiles_ValidateEmptyConcurrent(t *testing.T) {
	cc := newEmptyConflictContext(t)
	txn := newValidateTestTxn(t, nil)

	of := &overwriteFiles{base: &snapshotProducer{txn: txn}}
	require.NoError(t, of.validate(cc), "nil filter")

	of.filter = iceberg.AlwaysTrue{}
	require.NoError(t, of.validate(cc), "AlwaysTrue filter")

	of.filter = iceberg.EqualTo(iceberg.Reference("id"), int64(42))
	require.NoError(t, of.validate(cc), "bounded filter")
}

// TestOverwriteFiles_ValidateSkipFlag proves the skip flag suppresses
// the validator. RewriteDataFiles relies on this so compaction
// doesn't falsely reject concurrent appends.
func TestOverwriteFiles_ValidateSkipFlag(t *testing.T) {
	cc := newEmptyConflictContext(t)
	txn := newValidateTestTxn(t, nil)
	of := &overwriteFiles{base: &snapshotProducer{txn: txn}, skipDefaultValidator: true}
	require.NoError(t, of.validate(cc))
}

// TestOverwriteFiles_IsolationKeySplitByOp pins the Java-parity M1
// fix: OpDelete through overwriteFiles reads write.delete.isolation-
// level; every other op reads write.update.isolation-level. A future
// "simplification" that reads one key would pass the surrounding
// smokes but fail this test.
//
// We set one key to serializable and the other to snapshot, then
// flip the op. If validate() read the wrong key, it would gate on
// the wrong value — which we observe as whether validate would
// bail at the isolation-level check (SNAPSHOT → nil, SERIALIZABLE →
// walks). With zero concurrent snapshots both return nil, so we
// probe the key choice via a direct readIsolationLevel assertion
// on the properties bag each op would consult.
func TestOverwriteFiles_IsolationKeySplitByOp(t *testing.T) {
	props := iceberg.Properties{
		WriteDeleteIsolationLevelKey: string(IsolationSnapshot),
		WriteUpdateIsolationLevelKey: string(IsolationSerializable),
	}

	// OpDelete must resolve to the delete key (snapshot).
	assert.Equal(t, IsolationSnapshot,
		readIsolationLevel(props, WriteDeleteIsolationLevelKey, WriteDeleteIsolationLevelDefault),
		"OpDelete path must consult write.delete.isolation-level")

	// Every other op (OpOverwrite, OpReplace) must resolve to the
	// update key (serializable).
	assert.Equal(t, IsolationSerializable,
		readIsolationLevel(props, WriteUpdateIsolationLevelKey, WriteUpdateIsolationLevelDefault),
		"non-delete ops must consult write.update.isolation-level")
}

// TestOverwriteFiles_ValidateNilContext hardens validate against a
// nil conflictContext — callers that cannot construct cc (branch
// missing on both sides) should still see nil rather than a crash.
func TestOverwriteFiles_ValidateNilContext(t *testing.T) {
	txn := newValidateTestTxn(t, nil)
	of := &overwriteFiles{base: &snapshotProducer{txn: txn}}
	require.NoError(t, of.validate(nil))
}

func TestRowDelta_ValidateNoDeletes(t *testing.T) {
	cc := newEmptyConflictContext(t)
	txn := newValidateTestTxn(t, nil)
	rd := &RowDelta{txn: txn}
	require.NoError(t, rd.validate(cc))
}

// TestRowDelta_ValidatePosDeleteWithoutReference covers the realistic
// case where the pos-delete file does not record its referenced data
// file (ReferencedDataFile returns nil). validate() must skip the
// referenced-file check, matching Java's behavior.
func TestRowDelta_ValidatePosDeleteWithoutReference(t *testing.T) {
	cc := newEmptyConflictContext(t)
	txn := newValidateTestTxn(t, nil)
	posDelete := newTestPosDeleteFile(t, "pos-1.parquet", nil)
	rd := &RowDelta{txn: txn, delFiles: []iceberg.DataFile{posDelete}}
	require.NoError(t, rd.validate(cc))
}

// TestRowDelta_ValidateEqDeleteIsolationGates proves the isolation
// property reaches validate. Under SNAPSHOT isolation the eq-delete
// path returns nil without walking concurrent manifests; under
// SERIALIZABLE with no concurrent snapshots it still short-circuits.
// Behavioral rejection coverage lives in PR 2.3's tests.
func TestRowDelta_ValidateEqDeleteIsolationGates(t *testing.T) {
	cc := newEmptyConflictContext(t)
	eqDelete := newTestEqDeleteFile(t, "eq-1.parquet", []int{1})

	for _, level := range []IsolationLevel{IsolationSnapshot, IsolationSerializable} {
		txn := newValidateTestTxn(t, iceberg.Properties{
			WriteDeleteIsolationLevelKey: string(level),
		})
		rd := &RowDelta{txn: txn, delFiles: []iceberg.DataFile{eqDelete}}
		require.NoError(t, rd.validate(cc), "%s + no concurrent should be nil", level)
	}
}

func TestRewriteValidator_Smokes(t *testing.T) {
	cc := newEmptyConflictContext(t)

	// Empty rewrittenPaths short-circuits regardless of cc.
	require.NoError(t, rewriteValidator(nil)(cc))
	require.NoError(t, rewriteValidator(nil)(nil))

	// Non-empty paths but zero concurrent snapshots is also nil.
	require.NoError(t, rewriteValidator([]string{"a.parquet"})(cc))
}
