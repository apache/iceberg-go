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
	"math"
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

func TestDynamicPartitionValueEquality(t *testing.T) {
	key := func(value any) string {
		result, err := canonicalPartitionKey(0, map[int]any{1000: value})
		require.NoError(t, err)

		return result
	}
	require.Equal(t, key(nil), key(nil))
	require.Equal(t, key([]byte{0, 1, 255}), key([]byte{0, 1, 255}))
	require.NotEqual(t, key([]byte{0, 1}), key([]byte{1, 0}))
	require.Equal(t, key(int32(1)), key(int64(1)))
	require.Equal(t, key(iceberg.Date(1)), key(int32(1)))
	require.Equal(t, key(float32(math.NaN())), key(float32(math.NaN())))
	require.Equal(t, key(math.NaN()), key(math.NaN()))
	require.NotEqual(t, key(math.NaN()), key(0.0))
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
	for df, err := range snapshot.dataFiles(fs, nil) {
		require.NoError(t, err)
		if df.ContentType() == iceberg.EntryContentData {
			paths = append(paths, df.FilePath())
		}
	}
	slices.Sort(paths)
	require.Equal(t, []string{"file://new-a.parquet", "file://new-a2.parquet", "file://old-b.parquet"}, paths)
	require.Contains(t, txn.pinnedRefs, MainBranch, "concurrent appends must not bypass the overwrite")
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
	for df, err := range snapshot.dataFiles(fs, nil) {
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
	for df, err := range snapshot.dataFiles(fs, nil) {
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

func TestDynamicPartitionOverwriteRejectsUnsupportedLayouts(t *testing.T) {
	ctx := context.Background()
	t.Run("format v3", func(t *testing.T) {
		spec := partitionedSpec()
		txn, _ := createTestTransactionWithMemIO(t, spec)
		txn.meta.formatVersion = 3
		err := txn.DynamicPartitionOverwriteDataFiles(ctx, nil, nil)
		require.ErrorIs(t, err, ErrInvalidOperation)
		require.ErrorContains(t, err, "v3")
	})

	t.Run("wrong incoming spec", func(t *testing.T) {
		spec := partitionedSpec()
		txn, _ := createTestTransactionWithMemIO(t, spec)
		otherSpec := iceberg.NewPartitionSpecID(9, iceberg.PartitionField{
			SourceIDs: []int{1}, FieldID: 1000, Name: "id", Transform: iceberg.IdentityTransform{},
		})
		file := newTestDataFile(t, otherSpec, "file://wrong-spec.parquet", map[int]any{1000: int32(1)})
		err := txn.DynamicPartitionOverwriteDataFiles(ctx, []iceberg.DataFile{file}, nil)
		require.Error(t, err)
		require.Empty(t, txn.meta.updates)
	})

	t.Run("nil transaction", func(t *testing.T) {
		var txn *Transaction
		require.Error(t, txn.DynamicPartitionOverwriteDataFiles(ctx, nil, nil))
	})
}
