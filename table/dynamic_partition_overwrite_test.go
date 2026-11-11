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
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package table

import (
	"context"
	"slices"
	"testing"

	"github.com/apache/iceberg-go"
	iceio "github.com/apache/iceberg-go/io"
	"github.com/stretchr/testify/require"
)

func TestDynamicPartitionMatcher(t *testing.T) {
	spec := iceberg.NewPartitionSpecID(7,
		iceberg.PartitionField{
			SourceIDs: []int{1},
			FieldID:   1000,
			Name:      "bucket_id",
			Transform: iceberg.BucketTransform{NumBuckets: 16},
		},
		iceberg.PartitionField{
			SourceIDs: []int{2},
			FieldID:   1001,
			Name:      "region",
			Transform: iceberg.IdentityTransform{},
		},
	)

	files := []iceberg.DataFile{
		newTestDataFile(t, spec, "file://new-1.parquet",
			map[int]any{1000: int32(4), 1001: "north"}),
		newTestDataFile(t, spec, "file://new-2.parquet",
			map[int]any{1000: int32(4), 1001: "north"}),
		newTestDataFile(t, spec, "file://new-3.parquet",
			map[int]any{1000: int32(5), 1001: "south"}),
	}
	keys, err := dynamicPartitionKeys(files)
	require.NoError(t, err)
	require.Len(t, keys, 2)
	for _, tc := range []struct {
		partition map[int]any
		want      bool
	}{
		{map[int]any{1000: int32(4), 1001: "north"}, true},
		{map[int]any{1000: int64(5), 1001: "south"}, true},
		{map[int]any{1000: int32(4), 1001: "south"}, false},
		{map[int]any{1000: int32(7), 1001: "north"}, false},
	} {
		key, err := canonicalPartitionKey(int32(spec.ID()), tc.partition)
		require.NoError(t, err)
		_, found := keys[key]
		require.Equal(t, tc.want, found)
	}
}

func TestDynamicOverwriteManifestCandidates(t *testing.T) {
	t.Run("identity partition keeps matches and unknown summaries", func(t *testing.T) {
		schema := simpleSchema()
		spec := iceberg.NewPartitionSpecID(4, iceberg.PartitionField{
			SourceIDs: []int{1}, FieldID: 1000, Name: "id", Transform: iceberg.IdentityTransform{},
		})
		summary := func(lower, upper int32) iceberg.FieldSummary {
			lo, err := iceberg.Int32Literal(lower).MarshalBinary()
			require.NoError(t, err)
			hi, err := iceberg.Int32Literal(upper).MarshalBinary()
			require.NoError(t, err)

			return iceberg.FieldSummary{LowerBound: &lo, UpperBound: &hi}
		}
		manifests := []iceberg.ManifestFile{
			iceberg.NewManifestBuilder(2, "matching.avro", 1, int32(spec.ID()), 1).
				Partitions([]iceberg.FieldSummary{summary(2, 2)}).Build(),
			iceberg.NewManifestBuilder(2, "disjoint.avro", 1, int32(spec.ID()), 1).
				Partitions([]iceberg.FieldSummary{summary(10, 20)}).Build(),
			iceberg.NewManifestBuilder(2, "unknown-summary.avro", 1, int32(spec.ID()), 1).Build(),
			iceberg.NewManifestBuilder(2, "old-spec.avro", 1, 3, 1).
				Partitions([]iceberg.FieldSummary{summary(2, 2)}).Build(),
		}

		candidates, err := dynamicOverwriteManifestCandidates(t.Context(), manifests, spec, schema,
			[]map[int]any{{1000: int32(2)}})
		require.NoError(t, err)
		require.Equal(t, []bool{true, false, true, false}, candidates)
	})

	t.Run("day transform uses projected partition bounds", func(t *testing.T) {
		schema := iceberg.NewSchema(0,
			iceberg.NestedField{ID: 2, Name: "event_date", Type: iceberg.PrimitiveTypes.Date, Required: true},
		)
		spec := iceberg.NewPartitionSpecID(5, iceberg.PartitionField{
			SourceIDs: []int{2}, FieldID: 1000, Name: "event_day", Transform: iceberg.DayTransform{},
		})
		summary := func(lower, upper int32) iceberg.FieldSummary {
			lo, err := iceberg.Int32Literal(lower).MarshalBinary()
			require.NoError(t, err)
			hi, err := iceberg.Int32Literal(upper).MarshalBinary()
			require.NoError(t, err)

			return iceberg.FieldSummary{LowerBound: &lo, UpperBound: &hi}
		}
		manifests := []iceberg.ManifestFile{
			iceberg.NewManifestBuilder(2, "same-day.avro", 1, int32(spec.ID()), 1).
				Partitions([]iceberg.FieldSummary{summary(20000, 20000)}).Build(),
			iceberg.NewManifestBuilder(2, "other-day.avro", 1, int32(spec.ID()), 1).
				Partitions([]iceberg.FieldSummary{summary(20001, 20002)}).Build(),
		}

		candidates, err := dynamicOverwriteManifestCandidates(t.Context(), manifests, spec, schema,
			[]map[int]any{{1000: int32(20000)}})
		require.NoError(t, err)
		require.Equal(t, []bool{true, false}, candidates)
	})

	t.Run("bucket transform uses projected partition bounds", func(t *testing.T) {
		schema := simpleSchema()
		spec := iceberg.NewPartitionSpecID(7, iceberg.PartitionField{
			SourceIDs: []int{1}, FieldID: 1000, Name: "id_bucket", Transform: iceberg.BucketTransform{NumBuckets: 16},
		})
		summary := func(value int32) iceberg.FieldSummary {
			bound, err := iceberg.Int32Literal(value).MarshalBinary()
			require.NoError(t, err)

			return iceberg.FieldSummary{LowerBound: &bound, UpperBound: &bound}
		}
		manifests := []iceberg.ManifestFile{
			iceberg.NewManifestBuilder(2, "same-bucket.avro", 1, int32(spec.ID()), 1).
				Partitions([]iceberg.FieldSummary{summary(4)}).Build(),
			iceberg.NewManifestBuilder(2, "other-bucket.avro", 1, int32(spec.ID()), 1).
				Partitions([]iceberg.FieldSummary{summary(5)}).Build(),
		}

		candidates, err := dynamicOverwriteManifestCandidates(t.Context(), manifests, spec, schema,
			[]map[int]any{{1000: int32(4)}})
		require.NoError(t, err)
		require.Equal(t, []bool{true, false}, candidates)
	})

	t.Run("null partition uses contains-null summary", func(t *testing.T) {
		schema := iceberg.NewSchema(0,
			iceberg.NestedField{ID: 2, Name: "region", Type: iceberg.PrimitiveTypes.String, Required: false},
		)
		spec := iceberg.NewPartitionSpecID(6, iceberg.PartitionField{
			SourceIDs: []int{2}, FieldID: 1000, Name: "region", Transform: iceberg.IdentityTransform{},
		})
		lower, err := iceberg.StringLiteral("a").MarshalBinary()
		require.NoError(t, err)
		upper, err := iceberg.StringLiteral("z").MarshalBinary()
		require.NoError(t, err)
		manifests := []iceberg.ManifestFile{
			iceberg.NewManifestBuilder(2, "nonnull.avro", 1, int32(spec.ID()), 1).
				Partitions([]iceberg.FieldSummary{{LowerBound: &lower, UpperBound: &upper}}).Build(),
			iceberg.NewManifestBuilder(2, "null.avro", 1, int32(spec.ID()), 1).
				Partitions([]iceberg.FieldSummary{{ContainsNull: true}}).Build(),
		}

		candidates, err := dynamicOverwriteManifestCandidates(t.Context(), manifests, spec, schema,
			[]map[int]any{{1000: nil}})
		require.NoError(t, err)
		require.Equal(t, []bool{false, true}, candidates)
	})
	t.Run("unpartitioned also selects old-spec manifests", func(t *testing.T) {
		spec := iceberg.NewPartitionSpecID(7)
		manifests := []iceberg.ManifestFile{
			iceberg.NewManifestBuilder(2, "old-spec.avro", 1, 3, 1).Build(),
			iceberg.NewManifestBuilder(2, "current-spec.avro", 1, int32(spec.ID()), 1).Build(),
		}
		got, err := dynamicOverwriteManifestCandidates(t.Context(), manifests, spec, simpleSchema(),
			[]map[int]any{{}})
		require.NoError(t, err)
		require.Equal(t, []bool{true, true}, got)
	})

	t.Run("v1 manifest with unknown spec metadata stays included", func(t *testing.T) {
		schema := simpleSchema()
		spec := iceberg.NewPartitionSpecID(4, iceberg.PartitionField{
			SourceIDs: []int{1}, FieldID: 1000, Name: "id", Transform: iceberg.IdentityTransform{},
		})
		manifests := []iceberg.ManifestFile{iceberg.NewManifestBuilder(1, "v1.avro", 1, 0, 1).Build()}
		got, err := dynamicOverwriteManifestCandidates(t.Context(), manifests, spec, schema,
			[]map[int]any{{1000: int32(2)}})
		require.NoError(t, err)
		require.Equal(t, []bool{true}, got)
	})
}

func TestDynamicOverwriteManifestCandidatesIncompleteSummaries(t *testing.T) {
	schema := iceberg.NewSchema(0,
		iceberg.NestedField{ID: 1, Name: "id", Type: iceberg.PrimitiveTypes.Int32, Required: true},
		iceberg.NestedField{ID: 2, Name: "region", Type: iceberg.PrimitiveTypes.String},
	)
	spec := iceberg.NewPartitionSpecID(4,
		iceberg.PartitionField{SourceIDs: []int{1}, FieldID: 1000, Name: "id", Transform: iceberg.IdentityTransform{}},
		iceberg.PartitionField{SourceIDs: []int{2}, FieldID: 1001, Name: "region", Transform: iceberg.IdentityTransform{}},
	)
	match, err := iceberg.Int32Literal(2).MarshalBinary()
	require.NoError(t, err)
	other, err := iceberg.Int32Literal(10).MarshalBinary()
	require.NoError(t, err)

	manifests := []iceberg.ManifestFile{
		// The evaluator indexes one summary per field. Never evaluate a
		// truncated list, which could otherwise panic.
		iceberg.NewManifestBuilder(2, "short.avro", 1, int32(spec.ID()), 1).
			Partitions([]iceberg.FieldSummary{{LowerBound: &match, UpperBound: &match}}).Build(),
		// One missing bound is not evidence that values are all null.
		iceberg.NewManifestBuilder(2, "one-bound.avro", 1, int32(spec.ID()), 1).
			Partitions([]iceberg.FieldSummary{{LowerBound: &match}, {ContainsNull: true}}).Build(),
		// Missing non-null bounds without null or NaN markers are unknown.
		iceberg.NewManifestBuilder(2, "no-bounds.avro", 1, int32(spec.ID()), 1).
			Partitions([]iceberg.FieldSummary{{}, {ContainsNull: true}}).Build(),
		// A complete disjoint summary is still safe to prune.
		iceberg.NewManifestBuilder(2, "disjoint.avro", 1, int32(spec.ID()), 1).
			Partitions([]iceberg.FieldSummary{
				{LowerBound: &other, UpperBound: &other},
				{ContainsNull: true},
			}).Build(),
	}

	got, err := dynamicOverwriteManifestCandidates(t.Context(), manifests, spec, schema,
		[]map[int]any{{1000: int32(2), 1001: nil}})
	require.NoError(t, err)
	require.Equal(t, []bool{true, true, true, false}, got)
}

func TestDynamicOverwriteOldSpecDataWithoutLiveCounts(t *testing.T) {
	oldSpec := iceberg.NewManifestBuilder(2, "old-zero-counts.avro", 1, 3, 1).Build()
	require.False(t, oldSpec.HasAddedFiles())
	require.False(t, oldSpec.HasExistingFiles())
	require.True(t, mayHaveOldSpecData([]iceberg.ManifestFile{oldSpec}, 4))

	currentSpec := iceberg.NewManifestBuilder(2, "current.avro", 1, 4, 1).Build()
	require.False(t, mayHaveOldSpecData([]iceberg.ManifestFile{currentSpec}, 4))
}

func TestDynamicPartitionOverwriteDataFiles(t *testing.T) {
	ctx := context.Background()
	spec := partitionedSpec()
	seed, fs := createTestTransactionWithMemIO(t, spec)

	original := []iceberg.DataFile{
		newTestDataFile(t, spec, "file://old-a.parquet", map[int]any{1000: int32(1)}),
		newTestDataFile(t, spec, "file://old-a2.parquet", map[int]any{1000: int32(1)}),
		newTestDataFile(t, spec, "file://old-b.parquet", map[int]any{1000: int32(2)}),
	}
	require.NoError(t, seed.AddDataFiles(ctx, original, nil))
	seedMeta, err := seed.meta.Build()
	require.NoError(t, err)
	tbl := New(Identifier{"db", "tbl"}, seedMeta, "metadata.json",
		func(context.Context) (iceio.IO, error) { return fs, nil }, nil)

	txn := tbl.NewTransaction()
	replacement := []iceberg.DataFile{
		newTestDataFile(t, spec, "file://new-a.parquet", map[int]any{1000: int32(1)}),
		newTestDataFile(t, spec, "file://new-a2.parquet", map[int]any{1000: int32(1)}),
	}

	require.NoError(t, txn.DynamicPartitionOverwriteDataFiles(ctx, replacement,
		iceberg.Properties{"source": "dynamic-test"}))

	updated, err := txn.meta.Build()
	require.NoError(t, err)
	snapshot := updated.CurrentSnapshot()
	require.NotNil(t, snapshot)
	require.NotNil(t, snapshot.Summary)
	require.Equal(t, OpOverwrite, snapshot.Summary.Operation)
	require.Equal(t, "true", snapshot.Summary.Properties["replace-partitions"])
	require.Equal(t, "dynamic-test", snapshot.Summary.Properties["source"])

	var paths []string
	for df, err := range snapshot.dataFiles(ctx, fs, nil) {
		require.NoError(t, err)
		if df.ContentType() == iceberg.EntryContentData {
			paths = append(paths, df.FilePath())
		}
	}
	slices.Sort(paths)
	require.Equal(t, []string{"file://new-a.parquet", "file://new-a2.parquet", "file://old-b.parquet"}, paths)
	require.NotContains(t, txn.pinnedRefs, MainBranch, "existing branches use partition-scoped conflict validation")
}

func TestDynamicPartitionOverwriteRejectsAlreadyLivePath(t *testing.T) {
	spec := partitionedSpec()
	seed, fs := createTestTransactionWithMemIO(t, spec)
	existing := newTestDataFile(t, spec, "file://existing.parquet", map[int]any{1000: int32(1)})
	require.NoError(t, seed.AddDataFiles(t.Context(), []iceberg.DataFile{existing}, nil))
	seedMeta, err := seed.meta.Build()
	require.NoError(t, err)
	tbl := New(Identifier{"db", "tbl"}, seedMeta, "metadata.json",
		func(context.Context) (iceio.IO, error) { return fs, nil }, nil)

	txn := tbl.NewTransaction()
	err = txn.DynamicPartitionOverwriteDataFiles(t.Context(), []iceberg.DataFile{existing}, nil)
	require.ErrorIs(t, err, ErrInvalidOperation)
	require.ErrorContains(t, err, "already exists")
	require.Empty(t, txn.meta.updates)
	require.Empty(t, txn.pinnedRefs)
}

func TestDynamicPartitionOverwriteAppendsNewPartition(t *testing.T) {
	ctx := context.Background()
	spec := partitionedSpec()
	txn, fs := createTestTransactionWithMemIO(t, spec)
	newFile := newTestDataFile(t, spec, "file://first.parquet", map[int]any{1000: int32(10)})

	require.NoError(t, txn.DynamicPartitionOverwriteDataFiles(ctx, []iceberg.DataFile{newFile}, nil))
	metadata, err := txn.meta.Build()
	require.NoError(t, err)
	snapshot := metadata.CurrentSnapshot()
	require.NotNil(t, snapshot)
	var files []string
	for df, err := range snapshot.dataFiles(ctx, fs, nil) {
		require.NoError(t, err)
		files = append(files, df.FilePath())
	}
	require.Equal(t, []string{"file://first.parquet"}, files)
	require.Equal(t, OpOverwrite, snapshot.Summary.Operation)
	require.Equal(t, "true", snapshot.Summary.Properties["replace-partitions"])
	require.Contains(t, txn.pinnedRefs, MainBranch)
}

func TestDynamicPartitionOverwriteUnpartitionedReplacesAllFiles(t *testing.T) {
	ctx := context.Background()
	spec := iceberg.NewPartitionSpec()
	seed, fs := createTestTransactionWithMemIO(t, spec)
	original := []iceberg.DataFile{
		newTestDataFile(t, spec, "file://old-1.parquet", nil),
		newTestDataFile(t, spec, "file://old-2.parquet", nil),
	}
	require.NoError(t, seed.AddDataFiles(ctx, original, nil))
	seedMeta, err := seed.meta.Build()
	require.NoError(t, err)
	tbl := New(Identifier{"db", "tbl"}, seedMeta, "metadata.json",
		func(context.Context) (iceio.IO, error) { return fs, nil }, nil)

	txn := tbl.NewTransaction()
	replacement := newTestDataFile(t, spec, "file://replacement.parquet", nil)
	require.NoError(t, txn.DynamicPartitionOverwriteDataFiles(ctx, []iceberg.DataFile{replacement}, nil))
	updated, err := txn.meta.Build()
	require.NoError(t, err)
	snapshot := updated.CurrentSnapshot()
	require.Equal(t, OpOverwrite, snapshot.Summary.Operation)
	require.Equal(t, "true", snapshot.Summary.Properties["replace-partitions"])
	var paths []string
	for df, err := range snapshot.dataFiles(ctx, fs, nil) {
		require.NoError(t, err)
		if df.ContentType() == iceberg.EntryContentData {
			paths = append(paths, df.FilePath())
		}
	}
	require.Equal(t, []string{"file://replacement.parquet"}, paths)
}

func TestDynamicPartitionOverwriteEmptyInputIsNoOp(t *testing.T) {
	txn, _ := createTestTransactionWithMemIO(t, partitionedSpec())
	require.NoError(t, txn.DynamicPartitionOverwriteDataFiles(t.Context(), nil, nil))
	require.Empty(t, txn.meta.updates)
	require.Empty(t, txn.pinnedRefs)
}

func TestDynamicPartitionOverwriteRespectsCanceledContext(t *testing.T) {
	spec := partitionedSpec()
	txn, _ := createTestTransactionWithMemIO(t, spec)
	ctx, cancel := context.WithCancel(t.Context())
	cancel()

	file := newTestDataFile(t, spec, "file://canceled.parquet", map[int]any{1000: int32(5)})
	err := txn.DynamicPartitionOverwriteDataFiles(ctx, []iceberg.DataFile{file}, nil)
	require.ErrorIs(t, err, context.Canceled)
	require.Empty(t, txn.meta.updates)
	require.Empty(t, txn.pinnedRefs)
}

func TestDynamicPartitionOverwriteV3RowLineage(t *testing.T) {
	ctx := context.Background()
	spec := partitionedSpec()
	txn, _ := createTestTransactionWithMemIO(t, spec)
	require.NoError(t, txn.meta.SetFormatVersion(3))
	file := newTestDataFile(t, spec, "file://v3.parquet", map[int]any{1000: int32(1)})
	require.NoError(t, txn.DynamicPartitionOverwriteDataFiles(ctx, []iceberg.DataFile{file}, nil))
	meta, err := txn.meta.Build()
	require.NoError(t, err)
	snapshot := meta.CurrentSnapshot()
	require.NotNil(t, snapshot)
	require.Equal(t, OpOverwrite, snapshot.Summary.Operation)
	require.NotNil(t, snapshot.FirstRowID)
	require.NotNil(t, snapshot.AddedRows)
}

func TestDynamicPartitionOverwriteRejectsUnsupportedLayouts(t *testing.T) {
	ctx := context.Background()
	t.Run("wrong incoming spec", func(t *testing.T) {
		spec := partitionedSpec()
		txn, _ := createTestTransactionWithMemIO(t, spec)
		otherSpec := iceberg.NewPartitionSpec(iceberg.PartitionField{
			SourceIDs: []int{1}, FieldID: 1001, Name: "bucket_id", Transform: iceberg.BucketTransform{NumBuckets: 16},
		})
		require.NoError(t, txn.meta.AddPartitionSpec(&otherSpec, false))
		require.NoError(t, txn.meta.SetDefaultSpecID(-1))
		updatesBefore := slices.Clone(txn.meta.updates)
		file := newTestDataFile(t, spec, "file://wrong-spec.parquet", map[int]any{1000: int32(1)})
		err := txn.DynamicPartitionOverwriteDataFiles(ctx, []iceberg.DataFile{file}, nil)
		require.ErrorIs(t, err, ErrInvalidOperation)
		require.ErrorContains(t, err, "current spec")
		require.Equal(t, updatesBefore, txn.meta.updates)
	})

	t.Run("nil transaction", func(t *testing.T) {
		var txn *Transaction
		require.Error(t, txn.DynamicPartitionOverwriteDataFiles(ctx, nil, nil))
	})
}

func TestDynamicPartitionOverwriteV1(t *testing.T) {
	ctx := t.Context()
	spec := partitionedSpec()
	seed, fs := createTestTransactionWithMemIO(t, spec)
	seed.meta.formatVersion = 1
	old := []iceberg.DataFile{
		newTestDataFile(t, spec, "file://v1-old-a.parquet", map[int]any{1000: int32(1)}),
		newTestDataFile(t, spec, "file://v1-old-b.parquet", map[int]any{1000: int32(2)}),
	}
	require.NoError(t, seed.AddDataFiles(ctx, old, nil))
	base, err := seed.meta.Build()
	require.NoError(t, err)
	require.Equal(t, 1, base.FormatVersion())
	tbl := New(Identifier{"db", "tbl"}, base, "metadata.json", func(context.Context) (iceio.IO, error) { return fs, nil }, nil)
	tx := tbl.NewTransaction()
	replacement := newTestDataFile(t, spec, "file://v1-new-a.parquet", map[int]any{1000: int32(1)})
	require.NoError(t, tx.DynamicPartitionOverwriteDataFiles(ctx, []iceberg.DataFile{replacement}, nil))
	updated, err := tx.meta.Build()
	require.NoError(t, err)
	snapshot := updated.CurrentSnapshot()
	require.NotNil(t, snapshot)
	require.Equal(t, OpOverwrite, snapshot.Summary.Operation)
	var paths []string
	for file, err := range snapshot.dataFiles(ctx, fs, nil) {
		require.NoError(t, err)
		if file.ContentType() == iceberg.EntryContentData {
			paths = append(paths, file.FilePath())
		}
	}
	require.ElementsMatch(t, []string{"file://v1-new-a.parquet", "file://v1-old-b.parquet"}, paths)
}

func TestDynamicPartitionOverwriteKeepsLiveOldSpec(t *testing.T) {
	ctx := t.Context()
	oldSpec := partitionedSpec()
	seed, fs := createTestTransactionWithMemIO(t, oldSpec)
	old := newTestDataFile(t, oldSpec, "file://old-spec.parquet", map[int]any{1000: int32(1)})
	require.NoError(t, seed.AddDataFiles(ctx, []iceberg.DataFile{old}, nil))
	oldDelete := newTestPosDeleteFileForSpec(t, oldSpec, "file://old-spec-delete.parquet",
		map[int]any{1000: int32(1)}, old.FilePath())
	require.NoError(t, seed.NewRowDelta(nil).AddDeletes(oldDelete).Commit(ctx))
	base, err := seed.meta.Build()
	require.NoError(t, err)

	tbl := New(Identifier{"db", "tbl"}, base, "metadata.json",
		func(context.Context) (iceio.IO, error) { return fs, nil }, nil)
	evolve := tbl.NewTransaction()
	nextSpec := iceberg.NewPartitionSpec(iceberg.PartitionField{
		SourceIDs: []int{1}, FieldID: 1001, Name: "bucket_id", Transform: iceberg.BucketTransform{NumBuckets: 16},
	})
	require.NoError(t, evolve.meta.AddPartitionSpec(&nextSpec, false))
	require.NoError(t, evolve.meta.SetDefaultSpecID(-1))
	evolved, err := evolve.meta.Build()
	require.NoError(t, err)

	txn := New(Identifier{"db", "tbl"}, evolved, "metadata.json",
		func(context.Context) (iceio.IO, error) { return fs, nil }, nil).NewTransaction()
	currentSpec := evolved.PartitionSpec()
	var partitionFieldID int
	for _, field := range currentSpec.Fields() {
		partitionFieldID = field.FieldID

		break
	}
	require.NotZero(t, partitionFieldID)
	replacement := newTestDataFile(t, currentSpec, "file://new-spec.parquet",
		map[int]any{partitionFieldID: int32(1)})
	require.NoError(t, txn.DynamicPartitionOverwriteDataFiles(ctx, []iceberg.DataFile{replacement}, nil))
	updated, err := txn.meta.Build()
	require.NoError(t, err)
	snapshot := updated.CurrentSnapshot()
	require.NotNil(t, snapshot)

	var liveData []string
	for file, err := range snapshot.dataFiles(ctx, fs, nil) {
		require.NoError(t, err)
		if file.ContentType() == iceberg.EntryContentData {
			liveData = append(liveData, file.FilePath())
		}
	}
	require.ElementsMatch(t, []string{"file://old-spec.parquet", "file://new-spec.parquet"}, liveData)

	var liveDeletes []string
	for entry, err := range snapshot.entries(ctx, fs, iceberg.ManifestContentDeletes, true) {
		require.NoError(t, err)
		liveDeletes = append(liveDeletes, entry.DataFile().FilePath())
	}
	require.Equal(t, []string{"file://old-spec-delete.parquet"}, liveDeletes)
}

func TestDynamicPartitionOverwriteUnpartitionedAfterSpecEvolution(t *testing.T) {
	ctx := t.Context()
	oldSpec := partitionedSpec()
	seed, fs := createTestTransactionWithMemIO(t, oldSpec)
	oldData := newTestDataFile(t, oldSpec, "file://old-spec.parquet", map[int]any{1000: int32(1)})
	require.NoError(t, seed.AddDataFiles(ctx, []iceberg.DataFile{oldData}, nil))
	base, err := seed.meta.Build()
	require.NoError(t, err)

	fsF := func(context.Context) (iceio.IO, error) { return fs, nil }
	evolve := New(Identifier{"db", "tbl"}, base, "metadata.json", fsF, nil).NewTransaction()
	emptySpec := iceberg.NewPartitionSpec()
	require.NoError(t, evolve.meta.AddPartitionSpec(&emptySpec, false))
	require.NoError(t, evolve.meta.SetDefaultSpecID(-1))
	evolved, err := evolve.meta.Build()
	require.NoError(t, err)
	currentSpec := evolved.PartitionSpec()
	require.True(t, currentSpec.IsUnpartitioned())

	// Equality deletes written with an unpartitioned spec are global.
	builder, err := iceberg.NewDataFileBuilder(currentSpec, iceberg.EntryContentEqDeletes,
		"file://global-delete.parquet", iceberg.ParquetFile, nil, nil, nil, 1, 1)
	require.NoError(t, err)
	eqDelete := builder.EqualityFieldIDs([]int{1}).Build()
	addDelete := New(Identifier{"db", "tbl"}, evolved, "metadata.json", fsF, nil).NewTransaction()
	require.NoError(t, addDelete.NewRowDelta(nil).AddDeletes(eqDelete).Commit(ctx))
	withDelete, err := addDelete.meta.Build()
	require.NoError(t, err)

	overwrite := New(Identifier{"db", "tbl"}, withDelete, "metadata.json", fsF, nil).NewTransaction()
	replacement := newTestDataFile(t, currentSpec, "file://replacement.parquet", nil)
	require.NoError(t, overwrite.DynamicPartitionOverwriteDataFiles(ctx, []iceberg.DataFile{replacement}, nil))
	updated, err := overwrite.meta.Build()
	require.NoError(t, err)

	var dataPaths []string
	for df, err := range updated.CurrentSnapshot().dataFiles(ctx, fs, nil) {
		require.NoError(t, err)
		dataPaths = append(dataPaths, df.FilePath())
	}
	require.Equal(t, []string{"file://replacement.parquet"}, dataPaths,
		"unpartitioned overwrite must replace data under old specs too")

	var deletePaths []string
	for entry, err := range updated.CurrentSnapshot().entries(ctx, fs, iceberg.ManifestContentDeletes, true) {
		require.NoError(t, err)
		deletePaths = append(deletePaths, entry.DataFile().FilePath())
	}
	require.Empty(t, deletePaths, "no global delete may survive a full-table replacement")
}

func TestDynamicPartitionOverwriteKeepsDeleteForSurvivingOldSpecData(t *testing.T) {
	ctx := t.Context()
	oldSpec := partitionedSpec()
	seed, fs := createTestTransactionWithMemIO(t, oldSpec)
	oldData := newTestDataFile(t, oldSpec, "file://old-spec.parquet", map[int]any{1000: int32(1)})
	require.NoError(t, seed.AddDataFiles(ctx, []iceberg.DataFile{oldData}, nil))
	base, err := seed.meta.Build()
	require.NoError(t, err)

	fsF := func(context.Context) (iceio.IO, error) { return fs, nil }
	evolve := New(Identifier{"db", "tbl"}, base, "metadata.json", fsF, nil).NewTransaction()
	newSpec := iceberg.NewPartitionSpec(iceberg.PartitionField{
		SourceIDs: []int{1}, FieldID: 1001, Name: "id_bucket", Transform: iceberg.BucketTransform{NumBuckets: 16},
	})
	require.NoError(t, evolve.meta.AddPartitionSpec(&newSpec, false))
	require.NoError(t, evolve.meta.SetDefaultSpecID(-1))
	evolved, err := evolve.meta.Build()
	require.NoError(t, err)
	currentSpec := evolved.PartitionSpec()
	var fieldID int
	for _, field := range currentSpec.Fields() {
		fieldID = field.FieldID

		break
	}
	require.NotZero(t, fieldID)
	nullPartition := map[int]any{fieldID: nil}

	// A v2 position delete for an old-spec data file carries that file's
	// spec and partition; overwriting the new spec's partitions must keep it.
	posDelete := newTestPosDeleteFileForSpec(t, oldSpec, "file://old-spec-delete.parquet",
		map[int]any{1000: int32(1)}, oldData.FilePath())
	addDelete := New(Identifier{"db", "tbl"}, evolved, "metadata.json", fsF, nil).NewTransaction()
	require.NoError(t, addDelete.NewRowDelta(nil).AddDeletes(posDelete).Commit(ctx))
	withDelete, err := addDelete.meta.Build()
	require.NoError(t, err)

	overwrite := New(Identifier{"db", "tbl"}, withDelete, "metadata.json", fsF, nil).NewTransaction()
	replacement := newTestDataFile(t, currentSpec, "file://new-spec.parquet", nullPartition)
	require.NoError(t, overwrite.DynamicPartitionOverwriteDataFiles(ctx, []iceberg.DataFile{replacement}, nil))
	updated, err := overwrite.meta.Build()
	require.NoError(t, err)

	var dataPaths []string
	for df, err := range updated.CurrentSnapshot().dataFiles(ctx, fs, nil) {
		require.NoError(t, err)
		if df.ContentType() == iceberg.EntryContentData {
			dataPaths = append(dataPaths, df.FilePath())
		}
	}
	require.ElementsMatch(t, []string{oldData.FilePath(), replacement.FilePath()}, dataPaths)

	var deletePaths []string
	for entry, err := range updated.CurrentSnapshot().entries(ctx, fs, iceberg.ManifestContentDeletes, true) {
		require.NoError(t, err)
		deletePaths = append(deletePaths, entry.DataFile().FilePath())
	}
	require.Equal(t, []string{posDelete.FilePath()}, deletePaths,
		"deletes for surviving old-spec data must not be dropped")
}

func TestDynamicPartitionOverwriteWithoutAutoNameMapping(t *testing.T) {
	ctx := t.Context()
	spec := partitionedSpec()
	txn, _ := createTestTransactionWithMemIO(t, spec)
	file := newTestDataFile(t, spec, "file://no-name-mapping.parquet", map[int]any{1000: int32(1)})
	require.NoError(t, txn.DynamicPartitionOverwriteDataFiles(ctx, []iceberg.DataFile{file}, nil, WithoutAutoNameMapping()))
	updated, err := txn.meta.Build()
	require.NoError(t, err)
	require.Nil(t, updated.NameMapping())
}

func TestDynamicPartitionOverwriteRejectsStagedDefaultSpecChange(t *testing.T) {
	baseSpec := iceberg.NewPartitionSpec()
	txn, _ := createTestTransactionWithMemIO(t, baseSpec)
	changedSpec := iceberg.NewPartitionSpec(iceberg.PartitionField{
		SourceIDs: []int{1}, FieldID: 1000, Name: "id", Transform: iceberg.IdentityTransform{},
	})
	require.NoError(t, txn.meta.AddPartitionSpec(&changedSpec, false))
	require.NoError(t, txn.meta.SetDefaultSpecID(-1))
	spec, err := txn.meta.CurrentSpec()
	require.NoError(t, err)
	file := newTestDataFile(t, *spec, "file://staged-spec.parquet",
		map[int]any{1000: int32(1)})
	err = txn.DynamicPartitionOverwriteDataFiles(t.Context(), []iceberg.DataFile{file}, nil)
	require.ErrorIs(t, err, ErrCommitFailed)
	require.ErrorContains(t, err, "default spec id has changed")
}

func TestDynamicPartitionOverwriteRemovesOnlyTouchedPartitionDeletes(t *testing.T) {
	ctx := t.Context()
	spec := partitionedSpec()
	seed, fs := createTestTransactionWithMemIO(t, spec)
	dataA := newTestDataFile(t, spec, "file://old-a.parquet", map[int]any{1000: int32(1)})
	dataB := newTestDataFile(t, spec, "file://old-b.parquet", map[int]any{1000: int32(2)})
	require.NoError(t, seed.AddDataFiles(ctx, []iceberg.DataFile{dataA, dataB}, nil))
	base, err := seed.meta.Build()
	require.NoError(t, err)
	fsF := func(context.Context) (iceio.IO, error) { return fs, nil }
	tbl := New(Identifier{"db", "tbl"}, base, "metadata.json", fsF, nil)

	deleteA := newTestPosDeleteFileForSpec(t, spec, "file://delete-a.parquet", map[int]any{1000: int32(1)}, dataA.FilePath())
	deleteB := newTestPosDeleteFileForSpec(t, spec, "file://delete-b.parquet", map[int]any{1000: int32(2)}, dataB.FilePath())
	addDeletes := tbl.NewTransaction()
	require.NoError(t, addDeletes.NewRowDelta(nil).AddDeletes(deleteA, deleteB).Commit(ctx))
	withDeletes, err := addDeletes.meta.Build()
	require.NoError(t, err)

	overwrite := New(Identifier{"db", "tbl"}, withDeletes, "metadata.json", fsF, nil).NewTransaction()
	replacement := newTestDataFile(t, spec, "file://new-a.parquet", map[int]any{1000: int32(1)})
	require.NoError(t, overwrite.DynamicPartitionOverwriteDataFiles(ctx, []iceberg.DataFile{replacement}, nil))
	updated, err := overwrite.meta.Build()
	require.NoError(t, err)
	snapshot := updated.CurrentSnapshot()
	var liveDeletes []string
	for entry, err := range snapshot.entries(ctx, fs, iceberg.ManifestContentDeletes, true) {
		require.NoError(t, err)
		liveDeletes = append(liveDeletes, entry.DataFile().FilePath())
	}
	require.Equal(t, []string{"file://delete-b.parquet"}, liveDeletes)
}

func TestDynamicPartitionOverwritePrunedUnpartitionedAfterSpecEvolution(t *testing.T) {
	ctx := t.Context()
	oldSpec := partitionedSpec()
	seed, fs := createTestTransactionWithMemIO(t, oldSpec)
	oldData := newTestDataFile(t, oldSpec, "file://old-spec.parquet", map[int]any{1000: int32(1)})
	require.NoError(t, seed.AddDataFiles(ctx, []iceberg.DataFile{oldData}, nil))
	base, err := seed.meta.Build()
	require.NoError(t, err)

	fsF := func(context.Context) (iceio.IO, error) { return fs, nil }
	evolve := New(Identifier{"db", "tbl"}, base, "metadata.json", fsF, nil).NewTransaction()
	emptySpec := iceberg.NewPartitionSpec()
	require.NoError(t, evolve.meta.AddPartitionSpec(&emptySpec, false))
	require.NoError(t, evolve.meta.SetDefaultSpecID(-1))
	evolved, err := evolve.meta.Build()
	require.NoError(t, err)
	currentSpec := evolved.PartitionSpec()
	require.True(t, currentSpec.IsUnpartitioned())

	// Equality deletes written with an unpartitioned spec are global.
	builder, err := iceberg.NewDataFileBuilder(currentSpec, iceberg.EntryContentEqDeletes,
		"file://global-delete.parquet", iceberg.ParquetFile, nil, nil, nil, 1, 1)
	require.NoError(t, err)
	eqDelete := builder.EqualityFieldIDs([]int{1}).Build()
	addDelete := New(Identifier{"db", "tbl"}, evolved, "metadata.json", fsF, nil).NewTransaction()
	require.NoError(t, addDelete.NewRowDelta(nil).AddDeletes(eqDelete).Commit(ctx))
	withDelete, err := addDelete.meta.Build()
	require.NoError(t, err)

	overwrite := New(Identifier{"db", "tbl"}, withDelete, "metadata.json", fsF, nil).NewTransaction()
	replacement := newTestDataFile(t, currentSpec, "file://replacement.parquet", nil)
	require.NoError(t, overwrite.dynamicPartitionOverwriteDataFiles(ctx, []iceberg.DataFile{replacement}, nil, false))
	updated, err := overwrite.meta.Build()
	require.NoError(t, err)

	var dataPaths []string
	for df, err := range updated.CurrentSnapshot().dataFiles(ctx, fs, nil) {
		require.NoError(t, err)
		dataPaths = append(dataPaths, df.FilePath())
	}
	require.Equal(t, []string{"file://replacement.parquet"}, dataPaths,
		"unpartitioned overwrite must replace data under old specs too")

	var deletePaths []string
	for entry, err := range updated.CurrentSnapshot().entries(ctx, fs, iceberg.ManifestContentDeletes, true) {
		require.NoError(t, err)
		deletePaths = append(deletePaths, entry.DataFile().FilePath())
	}
	require.Empty(t, deletePaths, "no global delete may survive a full-table replacement")
}

func TestDynamicPartitionOverwritePrunedKeepsDeleteForSurvivingOldSpecData(t *testing.T) {
	ctx := t.Context()
	oldSpec := partitionedSpec()
	seed, fs := createTestTransactionWithMemIO(t, oldSpec)
	oldData := newTestDataFile(t, oldSpec, "file://old-spec.parquet", map[int]any{1000: int32(1)})
	require.NoError(t, seed.AddDataFiles(ctx, []iceberg.DataFile{oldData}, nil))
	base, err := seed.meta.Build()
	require.NoError(t, err)

	fsF := func(context.Context) (iceio.IO, error) { return fs, nil }
	evolve := New(Identifier{"db", "tbl"}, base, "metadata.json", fsF, nil).NewTransaction()
	newSpec := iceberg.NewPartitionSpec(iceberg.PartitionField{
		SourceIDs: []int{1}, FieldID: 1001, Name: "id_bucket", Transform: iceberg.BucketTransform{NumBuckets: 16},
	})
	require.NoError(t, evolve.meta.AddPartitionSpec(&newSpec, false))
	require.NoError(t, evolve.meta.SetDefaultSpecID(-1))
	evolved, err := evolve.meta.Build()
	require.NoError(t, err)
	currentSpec := evolved.PartitionSpec()
	var fieldID int
	for _, field := range currentSpec.Fields() {
		fieldID = field.FieldID

		break
	}
	require.NotZero(t, fieldID)
	nullPartition := map[int]any{fieldID: nil}

	// A v2 position delete for an old-spec data file carries that file's
	// spec and partition; overwriting the new spec's partitions must keep it.
	posDelete := newTestPosDeleteFileForSpec(t, oldSpec, "file://old-spec-delete.parquet",
		map[int]any{1000: int32(1)}, oldData.FilePath())
	addDelete := New(Identifier{"db", "tbl"}, evolved, "metadata.json", fsF, nil).NewTransaction()
	require.NoError(t, addDelete.NewRowDelta(nil).AddDeletes(posDelete).Commit(ctx))
	withDelete, err := addDelete.meta.Build()
	require.NoError(t, err)

	overwrite := New(Identifier{"db", "tbl"}, withDelete, "metadata.json", fsF, nil).NewTransaction()
	replacement := newTestDataFile(t, currentSpec, "file://new-spec.parquet", nullPartition)
	require.NoError(t, overwrite.dynamicPartitionOverwriteDataFiles(ctx, []iceberg.DataFile{replacement}, nil, false))
	updated, err := overwrite.meta.Build()
	require.NoError(t, err)

	var dataPaths []string
	for df, err := range updated.CurrentSnapshot().dataFiles(ctx, fs, nil) {
		require.NoError(t, err)
		if df.ContentType() == iceberg.EntryContentData {
			dataPaths = append(dataPaths, df.FilePath())
		}
	}
	require.ElementsMatch(t, []string{oldData.FilePath(), replacement.FilePath()}, dataPaths)

	var deletePaths []string
	for entry, err := range updated.CurrentSnapshot().entries(ctx, fs, iceberg.ManifestContentDeletes, true) {
		require.NoError(t, err)
		deletePaths = append(deletePaths, entry.DataFile().FilePath())
	}
	require.Equal(t, []string{posDelete.FilePath()}, deletePaths,
		"deletes for surviving old-spec data must not be dropped")
}
