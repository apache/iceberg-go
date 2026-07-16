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
	"bytes"
	"errors"
	"fmt"
	"math"
	"path/filepath"
	"testing"
	"time"

	"github.com/apache/arrow-go/v18/arrow/decimal128"
	"github.com/apache/iceberg-go"
	"github.com/apache/iceberg-go/internal"
	iceio "github.com/apache/iceberg-go/io"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// writeTestDeleteManifest writes a single-entry delete-content manifest
// for delFile and returns its ManifestFile descriptor. Mirrors
// writeTestManifest in partition_conflict_test.go, but forces
// ManifestContentDeletes on both the writer and the resulting
// ManifestFile so forEachAddedEntry's ManifestContentDeletes filter
// picks the entry up.
func writeTestDeleteManifest(
	t *testing.T,
	dir string,
	spec iceberg.PartitionSpec,
	schema *iceberg.Schema,
	snapshotID int64,
	delFile iceberg.DataFile,
) iceberg.ManifestFile {
	t.Helper()

	entry := iceberg.NewManifestEntryBuilder(iceberg.EntryStatusADDED, &snapshotID, delFile).
		SequenceNum(1).
		Build()

	manifestPath := filepath.ToSlash(filepath.Join(dir, fmt.Sprintf("delete-manifest-%d.avro", snapshotID)))
	var buf bytes.Buffer
	cnt := &internal.CountingWriter{W: &buf}
	writer, err := iceberg.NewManifestWriter(2, cnt, spec, schema, snapshotID,
		iceberg.WithManifestWriterContent(iceberg.ManifestContentDeletes))
	require.NoError(t, err)
	require.NoError(t, writer.Add(entry))
	require.NoError(t, writer.Close())

	mf, err := writer.ToManifestFile(manifestPath, cnt.Count,
		iceberg.WithManifestFileContent(iceberg.ManifestContentDeletes))
	require.NoError(t, err)

	fs := iceio.LocalFS{}
	f, err := fs.Create(manifestPath)
	require.NoError(t, err)
	_, err = f.Write(buf.Bytes())
	require.NoError(t, err)
	require.NoError(t, f.Close())

	return mf
}

// runPosDeleteConflictCheck builds a concurrent snapshot carrying delFile
// as a single ADDED delete-manifest entry and runs
// validateNoNewDeletesForRewrittenFiles against rewrittenFiles. Used by
// the pos-delete and eq-delete matching-behavior tests, which only care
// about the resulting error, not the surrounding metadata plumbing.
func runPosDeleteConflictCheck(t *testing.T, rewrittenFiles []iceberg.DataFile, delFile iceberg.DataFile) error {
	t.Helper()
	dir := filepath.ToSlash(t.TempDir())

	schema := iceberg.NewSchema(0,
		iceberg.NestedField{ID: 1, Name: "id", Type: iceberg.PrimitiveTypes.Int32, Required: true},
	)
	spec := *iceberg.UnpartitionedSpec
	if len(delFile.Partition()) > 0 {
		spec = iceberg.NewPartitionSpecID(int(delFile.SpecID()), iceberg.PartitionField{
			SourceIDs: []int{1}, FieldID: 1000, Name: "id", Transform: iceberg.IdentityTransform{},
		})
	}

	writerBaseID := int64(1)
	concSnapshotID := int64(2)

	props := iceberg.Properties{PropertyFormatVersion: "2"}
	baseMeta, err := NewMetadata(schema, &spec, UnsortedSortOrder, dir, props)
	require.NoError(t, err)

	baseBuilder, err := MetadataBuilderFromBase(baseMeta, "")
	require.NoError(t, err)
	baseSnap := Snapshot{
		SnapshotID:     writerBaseID,
		SequenceNumber: 1,
		TimestampMs:    baseMeta.LastUpdatedMillis() + 1,
		Summary:        &Summary{Operation: OpAppend},
	}
	require.NoError(t, baseBuilder.AddSnapshot(&baseSnap))
	require.NoError(t, baseBuilder.SetSnapshotRef(MainBranch, writerBaseID, BranchRef))
	writerBaseMeta, err := baseBuilder.Build()
	require.NoError(t, err)

	mf := writeTestDeleteManifest(t, dir, spec, schema, concSnapshotID, delFile)
	listPath := writeTestManifestList(t, dir, concSnapshotID, []iceberg.ManifestFile{mf})

	ctx := buildPartitionedContext(t, writerBaseMeta, listPath, writerBaseID, concSnapshotID)
	require.Len(t, ctx.concurrent, 1)

	return validateNoNewDeletesForRewrittenFiles(ctx, rewrittenFiles)
}

func TestReadIsolationLevel(t *testing.T) {
	tests := []struct {
		name    string
		props   iceberg.Properties
		key     string
		defVal  IsolationLevel
		want    IsolationLevel
		wantErr bool
	}{
		{
			name:    "missing key falls back to default",
			props:   iceberg.Properties{},
			key:     WriteDeleteIsolationLevelKey,
			defVal:  IsolationSerializable,
			want:    IsolationSerializable,
			wantErr: false,
		},
		{
			name:    "explicit serializable",
			props:   iceberg.Properties{WriteDeleteIsolationLevelKey: "serializable"},
			key:     WriteDeleteIsolationLevelKey,
			defVal:  IsolationSnapshot,
			want:    IsolationSerializable,
			wantErr: false,
		},
		{
			name:    "explicit snapshot",
			props:   iceberg.Properties{WriteDeleteIsolationLevelKey: "snapshot"},
			key:     WriteDeleteIsolationLevelKey,
			defVal:  IsolationSerializable,
			want:    IsolationSnapshot,
			wantErr: false,
		},
		{
			name:    "explicit snapshot (mixed case)",
			props:   iceberg.Properties{WriteDeleteIsolationLevelKey: "Snapshot"},
			key:     WriteDeleteIsolationLevelKey,
			defVal:  IsolationSerializable,
			want:    IsolationSnapshot,
			wantErr: false,
		},
		{
			name:    "unrecognized value returns error",
			props:   iceberg.Properties{WriteDeleteIsolationLevelKey: "repeatable-read"},
			key:     WriteDeleteIsolationLevelKey,
			defVal:  IsolationSerializable,
			want:    IsolationSerializable,
			wantErr: true,
		},
		{
			name:    "empty string falls back to default",
			props:   iceberg.Properties{WriteDeleteIsolationLevelKey: ""},
			key:     WriteDeleteIsolationLevelKey,
			defVal:  IsolationSnapshot,
			want:    IsolationSnapshot,
			wantErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := readIsolationLevel(tt.props, tt.key, tt.defVal)
			if tt.wantErr {
				assert.ErrorIs(t, err, ErrInvalidIsolationLevel)
				assert.Equal(t, tt.want, got)

				return
			}

			assert.NoError(t, err)
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
	// Empty rewrittenFiles OR empty concurrent snapshots must both
	// short-circuit to nil.
	head := int64(1)
	meta := newConflictTestMetadata(t, &head)
	ctx, err := newConflictContext(meta, meta, MainBranch, nil, true)
	require.NoError(t, err)

	// Empty rewrittenFiles.
	require.NoError(t, validateNoNewDeletesForRewrittenFiles(ctx, nil))
	require.NoError(t, validateNoNewDeletesForRewrittenFiles(ctx, []iceberg.DataFile{}))

	// Non-empty rewrittenFiles but no concurrent snapshots.
	rewritten := newTestDataFile(t, *iceberg.UnpartitionedSpec, "a.parquet", nil)
	require.NoError(t, validateNoNewDeletesForRewrittenFiles(ctx, []iceberg.DataFile{rewritten}))
}

// posDeleteBoundsFile builds a position-delete DataFile whose file_path
// lower/upper bounds are both set to path, without ReferencedDataFile —
// the shape a v2 writer produces when it records bounds but not the
// referenced_data_file column.
func posDeleteBoundsFile(t *testing.T, spec iceberg.PartitionSpec, delPath, path string, partition map[int]any) iceberg.DataFile {
	t.Helper()

	bound, err := iceberg.StringLiteral(path).MarshalBinary()
	require.NoError(t, err)

	builder, err := iceberg.NewDataFileBuilder(
		spec, iceberg.EntryContentPosDeletes, delPath, iceberg.ParquetFile,
		partition, nil, nil, 1, 1)
	require.NoError(t, err)

	return builder.
		LowerBoundValues(map[int][]byte{filePathFieldID: bound}).
		UpperBoundValues(map[int][]byte{filePathFieldID: bound}).
		Build()
}

// posDeletePartitionScopedFile builds a position-delete DataFile with no
// ReferencedDataFile and no (or non-degenerate) file_path bounds — the
// shape Java resolves purely by (specID, partition) overlap.
func posDeletePartitionScopedFile(t *testing.T, spec iceberg.PartitionSpec, delPath string, partition map[int]any) iceberg.DataFile {
	t.Helper()

	builder, err := iceberg.NewDataFileBuilder(
		spec, iceberg.EntryContentPosDeletes, delPath, iceberg.ParquetFile,
		partition, nil, nil, 1, 1)
	require.NoError(t, err)

	return builder.Build()
}

// posDeleteUnequalBoundsFile builds a position-delete DataFile whose
// file_path lower/upper bounds differ — unresolvable to a single path,
// so callers must fall back to the partition-scoped rule.
func posDeleteUnequalBoundsFile(t *testing.T, spec iceberg.PartitionSpec, delPath, lowerPath, upperPath string, partition map[int]any) iceberg.DataFile {
	t.Helper()

	lower, err := iceberg.StringLiteral(lowerPath).MarshalBinary()
	require.NoError(t, err)
	upper, err := iceberg.StringLiteral(upperPath).MarshalBinary()
	require.NoError(t, err)

	builder, err := iceberg.NewDataFileBuilder(
		spec, iceberg.EntryContentPosDeletes, delPath, iceberg.ParquetFile,
		partition, nil, nil, 1, 1)
	require.NoError(t, err)

	return builder.
		LowerBoundValues(map[int][]byte{filePathFieldID: lower}).
		UpperBoundValues(map[int][]byte{filePathFieldID: upper}).
		Build()
}

func TestReferencedDataFilePath(t *testing.T) {
	unpart := *iceberg.UnpartitionedSpec

	t.Run("explicit reference wins", func(t *testing.T) {
		builder, err := iceberg.NewDataFileBuilder(
			unpart, iceberg.EntryContentPosDeletes, "del.parquet", iceberg.ParquetFile,
			nil, nil, nil, 1, 1)
		require.NoError(t, err)
		df := builder.ReferencedDataFile("data-a.parquet").Build()

		assert.Equal(t, "data-a.parquet", referencedDataFilePath(df))
	})

	t.Run("equal bounds resolve to the bound path", func(t *testing.T) {
		df := posDeleteBoundsFile(t, unpart, "del.parquet", "data-b.parquet", nil)
		assert.Equal(t, "data-b.parquet", referencedDataFilePath(df))
	})

	t.Run("unequal bounds are unresolvable", func(t *testing.T) {
		df := posDeleteUnequalBoundsFile(t, unpart, "del.parquet", "data-a.parquet", "data-b.parquet", nil)
		assert.Empty(t, referencedDataFilePath(df))
	})

	t.Run("no reference and no bounds are unresolvable", func(t *testing.T) {
		df := posDeletePartitionScopedFile(t, unpart, "del.parquet", nil)
		assert.Empty(t, referencedDataFilePath(df))
	})
}

// mustPartitionConflictKey unwraps partitionConflictKey for test
// tuples that only contain supported value types.
func mustPartitionConflictKey(t *testing.T, specID int32, partition map[int]any) string {
	t.Helper()
	key, err := partitionConflictKey(specID, partition)
	require.NoError(t, err)

	return key
}

func TestPartitionConflictKey(t *testing.T) {
	sampleUUID := uuid.MustParse("12345678-9abc-def0-1234-56789abcdef0")
	dec := iceberg.Decimal{Val: decimal128.FromI64(12345), Scale: 2}
	decOther := iceberg.Decimal{Val: decimal128.FromI64(12346), Scale: 2}
	decOtherScale := iceberg.Decimal{Val: decimal128.FromI64(12345), Scale: 3}

	equal := []struct {
		name string
		a, b map[int]any
	}{
		{"field order is irrelevant", map[int]any{1: int32(1), 2: "x"}, map[int]any{2: "x", 1: int32(1)}},
		{"nil vs empty tuple", nil, map[int]any{}},
		{"bool", map[int]any{1: true}, map[int]any{1: true}},
		{"nil value", map[int]any{1: nil}, map[int]any{1: nil}},
		{"int widths unify", map[int]any{1: int32(7)}, map[int]any{1: int64(7)}},
		{"int unifies too", map[int]any{1: int(7)}, map[int]any{1: int64(7)}},
		{"Date vs raw int32", map[int]any{1: iceberg.Date(19000)}, map[int]any{1: int32(19000)}},
		{"Time vs raw int64", map[int]any{1: iceberg.Time(123456)}, map[int]any{1: int64(123456)}},
		{"Timestamp vs raw int64", map[int]any{1: iceberg.Timestamp(99)}, map[int]any{1: int64(99)}},
		{"TimestampNano vs raw int64", map[int]any{1: iceberg.TimestampNano(99)}, map[int]any{1: int64(99)}},
		{"float32 vs float64", map[int]any{1: float32(1.5)}, map[int]any{1: float64(1.5)}},
		{"NaN matches NaN", map[int]any{1: math.NaN()}, map[int]any{1: math.NaN()}},
		{"string", map[int]any{1: "us-east-1"}, map[int]any{1: "us-east-1"}},
		{"bytes", map[int]any{1: []byte{0xde, 0xad}}, map[int]any{1: []byte{0xde, 0xad}}},
		{"uuid vs its raw bytes", map[int]any{1: sampleUUID}, map[int]any{1: sampleUUID[:]}},
		{"Decimal vs DecimalLiteral", map[int]any{1: dec}, map[int]any{1: iceberg.DecimalLiteral(dec)}},
	}
	for _, tt := range equal {
		t.Run("equal/"+tt.name, func(t *testing.T) {
			assert.Equal(t,
				mustPartitionConflictKey(t, 0, tt.a),
				mustPartitionConflictKey(t, 0, tt.b))
		})
	}

	distinct := []struct {
		name string
		a, b map[int]any
	}{
		{"bool", map[int]any{1: true}, map[int]any{1: false}},
		{"nil vs zero int", map[int]any{1: nil}, map[int]any{1: int64(0)}},
		{"int", map[int]any{1: int64(7)}, map[int]any{1: int64(8)}},
		{"Date", map[int]any{1: iceberg.Date(19000)}, map[int]any{1: iceberg.Date(19001)}},
		{"float", map[int]any{1: 1.5}, map[int]any{1: 1.25}},
		{"NaN vs number", map[int]any{1: math.NaN()}, map[int]any{1: 0.0}},
		{"string", map[int]any{1: "x"}, map[int]any{1: "y"}},
		{"string vs its bytes", map[int]any{1: "x"}, map[int]any{1: []byte("x")}},
		{"bytes", map[int]any{1: []byte{0xde}}, map[int]any{1: []byte{0xad}}},
		{"uuid", map[int]any{1: sampleUUID}, map[int]any{1: uuid.Nil}},
		{"decimal value", map[int]any{1: dec}, map[int]any{1: decOther}},
		{"decimal scale", map[int]any{1: dec}, map[int]any{1: decOtherScale}},
		{"different field ids", map[int]any{1: int64(1)}, map[int]any{2: int64(1)}},
		{
			"injectivity: separator inside a string value",
			map[int]any{1: "a;2:b"},
			map[int]any{1: "a", 2: "b"},
		},
	}
	for _, tt := range distinct {
		t.Run("distinct/"+tt.name, func(t *testing.T) {
			assert.NotEqual(t,
				mustPartitionConflictKey(t, 0, tt.a),
				mustPartitionConflictKey(t, 0, tt.b))
		})
	}

	t.Run("different spec ids never collide", func(t *testing.T) {
		tuple := map[int]any{1: int32(1), 2: "x"}
		assert.NotEqual(t,
			mustPartitionConflictKey(t, 0, tuple),
			mustPartitionConflictKey(t, 1, tuple))
		assert.NotEqual(t,
			mustPartitionConflictKey(t, 0, nil),
			mustPartitionConflictKey(t, 1, nil))
	})

	t.Run("unsupported types fail closed", func(t *testing.T) {
		for _, v := range []any{struct{}{}, time.Now()} {
			_, err := partitionConflictKey(0, map[int]any{1: v})
			require.Error(t, err)
			assert.Contains(t, err.Error(), "unsupported partition value type")
		}
	})
}

// TestValidateNoNewDeletesForRewrittenFiles_PosDeleteMatching drives the
// validator through a synthetic concurrent snapshot carrying a single
// position-delete entry, covering every resolution path documented on
// referencedDataFilePath and the partition fallback in
// validateNoNewDeletesForRewrittenFiles.
func TestValidateNoNewDeletesForRewrittenFiles_PosDeleteMatching(t *testing.T) {
	unpart := *iceberg.UnpartitionedSpec
	spec0 := iceberg.NewPartitionSpecID(0, iceberg.PartitionField{
		SourceIDs: []int{1}, FieldID: 1000, Name: "id", Transform: iceberg.IdentityTransform{},
	})
	spec1 := iceberg.NewPartitionSpecID(1, iceberg.PartitionField{
		SourceIDs: []int{1}, FieldID: 1000, Name: "id", Transform: iceberg.IdentityTransform{},
	})

	rewrittenUnpart := newTestDataFile(t, unpart, "data-rewritten.parquet", nil)
	rewrittenPart := newTestDataFile(t, spec0, "data-part.parquet", map[int]any{1000: int32(7)})

	tests := []struct {
		name           string
		rewrittenFiles []iceberg.DataFile
		delFile        iceberg.DataFile
		wantConflict   bool
	}{
		{
			name:           "explicit reference matching rewritten path conflicts",
			rewrittenFiles: []iceberg.DataFile{rewrittenUnpart},
			delFile: func() iceberg.DataFile {
				b, err := iceberg.NewDataFileBuilder(unpart, iceberg.EntryContentPosDeletes,
					"del.parquet", iceberg.ParquetFile, nil, nil, nil, 1, 1)
				require.NoError(t, err)

				return b.ReferencedDataFile("data-rewritten.parquet").Build()
			}(),
			wantConflict: true,
		},
		{
			name:           "equal bounds naming rewritten path conflicts (main bug)",
			rewrittenFiles: []iceberg.DataFile{rewrittenUnpart},
			delFile:        posDeleteBoundsFile(t, unpart, "del.parquet", "data-rewritten.parquet", nil),
			wantConflict:   true,
		},
		{
			name:           "equal bounds naming a non-rewritten path do not conflict",
			rewrittenFiles: []iceberg.DataFile{rewrittenUnpart},
			delFile:        posDeleteBoundsFile(t, unpart, "del.parquet", "data-other.parquet", nil),
			wantConflict:   false,
		},
		{
			name:           "unequal bounds fall back to partition match and conflict",
			rewrittenFiles: []iceberg.DataFile{rewrittenPart},
			delFile: posDeleteUnequalBoundsFile(t, spec0, "del.parquet",
				"data-a.parquet", "data-b.parquet", map[int]any{1000: int32(7)}),
			wantConflict: true,
		},
		{
			name:           "no reference, no bounds, same spec+partition conflicts",
			rewrittenFiles: []iceberg.DataFile{rewrittenPart},
			delFile:        posDeletePartitionScopedFile(t, spec0, "del.parquet", map[int]any{1000: int32(7)}),
			wantConflict:   true,
		},
		{
			name:           "different partition tuple, same spec does not conflict",
			rewrittenFiles: []iceberg.DataFile{rewrittenPart},
			delFile:        posDeletePartitionScopedFile(t, spec0, "del.parquet", map[int]any{1000: int32(9)}),
			wantConflict:   false,
		},
		{
			name:           "unpartitioned table: partition-scoped delete conflicts with any rewritten file",
			rewrittenFiles: []iceberg.DataFile{rewrittenUnpart},
			delFile:        posDeletePartitionScopedFile(t, unpart, "del.parquet", nil),
			wantConflict:   true,
		},
		{
			name:           "different specID, same-looking tuple does not conflict",
			rewrittenFiles: []iceberg.DataFile{rewrittenPart},
			delFile:        posDeletePartitionScopedFile(t, spec1, "del.parquet", map[int]any{1000: int32(7)}),
			wantConflict:   false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := runPosDeleteConflictCheck(t, tt.rewrittenFiles, tt.delFile)
			if tt.wantConflict {
				require.Error(t, err)
				assert.ErrorIs(t, err, ErrConflictingDeleteFiles)
			} else {
				require.NoError(t, err)
			}
		})
	}
}

// TestValidateNoNewDeletesForRewrittenFiles_EqDeleteAlwaysConflicts pins
// the pre-existing conservative eq-delete rule: any concurrent eq-delete
// during a rewrite is a conflict, regardless of partition overlap.
func TestValidateNoNewDeletesForRewrittenFiles_EqDeleteAlwaysConflicts(t *testing.T) {
	unpart := *iceberg.UnpartitionedSpec
	rewritten := newTestDataFile(t, unpart, "data-rewritten.parquet", nil)

	eqBuilder, err := iceberg.NewDataFileBuilder(unpart, iceberg.EntryContentEqDeletes,
		"eq.parquet", iceberg.ParquetFile, nil, nil, nil, 1, 1)
	require.NoError(t, err)
	eqDel := eqBuilder.EqualityFieldIDs([]int{1}).Build()

	err = runPosDeleteConflictCheck(t, []iceberg.DataFile{rewritten}, eqDel)
	require.Error(t, err)
	assert.ErrorIs(t, err, ErrConflictingDeleteFiles)
}

// TestValidateNoNewDeletesForRewrittenFiles_UnsupportedPartitionType
// pins the fail-closed contract: a rewritten file whose partition
// carries a value type outside partitionConflictKey's closed set must
// fail the validation loudly instead of silently skipping the file.
// Only the rewritten side is testable — the delete side is decoded
// from real Avro manifests whose value domain is exactly the closed
// set, so an unsupported type cannot reach it.
func TestValidateNoNewDeletesForRewrittenFiles_UnsupportedPartitionType(t *testing.T) {
	spec := iceberg.NewPartitionSpecID(0, iceberg.PartitionField{
		SourceIDs: []int{1}, FieldID: 1000, Name: "id", Transform: iceberg.IdentityTransform{},
	})

	rewritten := newTestDataFile(t, spec, "data-part.parquet", map[int]any{1000: struct{}{}})
	delFile := posDeletePartitionScopedFile(t, spec, "del.parquet", map[int]any{1000: int32(7)})

	err := runPosDeleteConflictCheck(t, []iceberg.DataFile{rewritten}, delFile)
	require.Error(t, err)
	assert.NotErrorIs(t, err, ErrConflictingDeleteFiles,
		"a key-encoding failure is not a conflict; it must surface as its own error")
	assert.Contains(t, err.Error(), "unsupported partition value type")
	assert.Contains(t, err.Error(), "data-part.parquet",
		"error must identify the offending file")
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
