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

package table_test

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/apache/iceberg-go"
	iceio "github.com/apache/iceberg-go/io"
	"github.com/apache/iceberg-go/table"
	"github.com/stretchr/testify/require"
)

// A v1 table with an externally written, bucket-partitioned Parquet file.
// Its rows share one bucket, which is required by the Iceberg specification.
func makeV1BucketImportFixture(t *testing.T) (*table.Table, iceio.LocalFS, string, int, int32) {
	t.Helper()

	dir := t.TempDir()
	schema := iceberg.NewSchema(1,
		iceberg.NestedField{ID: 1, Name: "id", Type: iceberg.PrimitiveTypes.Int32, Required: true},
	)
	bucket := iceberg.BucketTransform{NumBuckets: 8}
	spec := iceberg.NewPartitionSpecID(0, iceberg.PartitionField{
		SourceIDs: []int{1}, FieldID: 1000, Name: "id_bucket", Transform: bucket,
	})
	meta, err := table.NewMetadata(schema, &spec, table.UnsortedSortOrder, dir, iceberg.Properties{
		table.PropertyFormatVersion: "1",
	})
	require.NoError(t, err)
	require.Equal(t, 1, meta.FormatVersion())

	parquetBytes, _ := parquetMetaFromSchema(t, schema, "[{\"id\":7},{\"id\":7}]")
	path := filepath.Join(dir, "data.parquet")
	require.NoError(t, os.WriteFile(path, parquetBytes, 0o600))

	result := bucket.Apply(iceberg.Optional[iceberg.Literal]{
		Valid: true,
		Val:   iceberg.Int32Literal(7),
	})
	require.True(t, result.Valid)
	bucketID := result.Val.(iceberg.Int32Literal).Value()
	currentSpec := meta.PartitionSpec()
	fieldID := currentSpec.Field(0).FieldID

	fs := iceio.LocalFS{}
	cat := &mergeCatalog{meta: meta}
	tbl := table.New(table.Identifier{"default", "bucket_import"}, meta, filepath.Join(dir, "metadata", "00000.json"),
		func(context.Context) (iceio.IO, error) { return fs, nil }, cat)

	return tbl, fs, path, fieldID, bucketID
}

func TestV1FileToDataFileExplicitBucketPartition(t *testing.T) {
	tbl, fs, path, fieldID, bucketID := makeV1BucketImportFixture(t)
	spec := tbl.Metadata().PartitionSpec()

	_, err := table.FileToDataFileWithOptions(t.Context(), fs, path, tbl.Metadata().CurrentSchema(), spec, 0, nil)
	require.ErrorContains(t, err, "cannot infer partition field")

	file, err := table.FileToDataFileWithOptions(t.Context(), fs, path, tbl.Metadata().CurrentSchema(), spec, 0, nil,
		table.WithFilePartitionValues(map[int]any{fieldID: bucketID}))
	require.NoError(t, err)
	require.Equal(t, bucketID, file.Partition()[fieldID])
	require.EqualValues(t, 2, file.Count())
	require.Positive(t, file.FileSizeBytes())

	t.Run("invalid bucket type", func(t *testing.T) {
		_, err := table.FileToDataFileWithOptions(t.Context(), fs, path, tbl.Metadata().CurrentSchema(), spec, 0, nil,
			table.WithFilePartitionValues(map[int]any{fieldID: int(1)}))
		require.ErrorIs(t, err, iceberg.ErrInvalidArgument)
	})

	t.Run("out of range bucket", func(t *testing.T) {
		_, err := table.FileToDataFileWithOptions(t.Context(), fs, path, tbl.Metadata().CurrentSchema(), spec, 0, nil,
			table.WithFilePartitionValues(map[int]any{fieldID: int32(8)}))
		require.ErrorIs(t, err, iceberg.ErrInvalidArgument)
	})

	t.Run("negative bucket", func(t *testing.T) {
		_, err := table.FileToDataFileWithOptions(t.Context(), fs, path, tbl.Metadata().CurrentSchema(), spec, 0, nil,
			table.WithFilePartitionValues(map[int]any{fieldID: int32(-1)}))
		require.ErrorIs(t, err, iceberg.ErrInvalidArgument)
	})

	t.Run("unknown partition field", func(t *testing.T) {
		_, err := table.FileToDataFileWithOptions(t.Context(), fs, path, tbl.Metadata().CurrentSchema(), spec, 0, nil,
			table.WithFilePartitionValues(map[int]any{fieldID + 1000: bucketID}))
		require.ErrorIs(t, err, iceberg.ErrInvalidArgument)
	})

	t.Run("null bucket", func(t *testing.T) {
		file, err := table.FileToDataFileWithOptions(t.Context(), fs, path, tbl.Metadata().CurrentSchema(), spec, 0, nil,
			table.WithFilePartitionValues(map[int]any{fieldID: nil}))
		require.NoError(t, err)
		value, exists := file.Partition()[fieldID]
		require.True(t, exists)
		require.Nil(t, value)
	})
}

func TestV1ImportVoidPartitionWithoutExplicitValue(t *testing.T) {
	tbl, fs, path, _, _ := makeV1BucketImportFixture(t)
	spec := iceberg.NewPartitionSpecID(1, iceberg.PartitionField{
		SourceIDs: []int{1}, FieldID: 1000, Name: "old_id_bucket", Transform: iceberg.VoidTransform{},
	})

	file, err := table.FileToDataFileWithOptions(t.Context(), fs, path, tbl.Metadata().CurrentSchema(), spec, 0, nil)
	require.NoError(t, err)
	value, ok := file.Partition()[1000]
	require.True(t, ok)
	require.Nil(t, value)

	_, err = table.FileToDataFileWithOptions(t.Context(), fs, path, tbl.Metadata().CurrentSchema(), spec, 0, nil,
		table.WithFilePartitionValues(map[int]any{1000: int32(0)}))
	require.ErrorIs(t, err, iceberg.ErrInvalidArgument)
}

func TestV1AddDataFilesByPathBucketPartitionCommit(t *testing.T) {
	tbl, fs, path, fieldID, bucketID := makeV1BucketImportFixture(t)
	tx := tbl.NewTransaction()

	err := tx.AddDataFilesByPath(t.Context(), []string{path}, nil, false,
		table.WithAddDataFilesByPathPartitionValues(map[string]map[int]any{
			path: {fieldID: bucketID},
		}))
	require.NoError(t, err)

	committed, err := tx.Commit(t.Context())
	require.NoError(t, err)
	require.Equal(t, 1, committed.Metadata().FormatVersion())

	snapshot := committed.CurrentSnapshot()
	require.NotNil(t, snapshot)
	manifests, err := snapshot.Manifests(t.Context(), fs)
	require.NoError(t, err)
	require.NotEmpty(t, manifests)

	var imported int
	for _, manifest := range manifests {
		for entry, err := range manifest.Entries(t.Context(), fs, true) {
			require.NoError(t, err)
			if entry.DataFile().FilePath() == path {
				imported++
				require.Equal(t, bucketID, entry.DataFile().Partition()[fieldID])
			}
		}
	}
	require.Equal(t, 1, imported)
}

func TestV1AddDataFilesByPathValidatesPartitionPaths(t *testing.T) {
	tbl, _, path, fieldID, bucketID := makeV1BucketImportFixture(t)
	tx := tbl.NewTransaction()

	err := tx.AddDataFilesByPath(t.Context(), []string{path}, nil, false,
		table.WithAddDataFilesByPathPartitionValues(map[string]map[int]any{
			"other.parquet": {fieldID: bucketID},
		}))
	require.ErrorIs(t, err, iceberg.ErrInvalidArgument)

	// A rejected option must not poison the transaction. Import without the
	// bucket value still fails rather than writing a mispartitioned file.
	err = tx.AddDataFilesByPath(t.Context(), []string{path}, nil, false)
	require.ErrorContains(t, err, "cannot infer partition field")
}

func TestV1FileToDataFileExplicitPartitionValues(t *testing.T) {
	tbl, fs, path, _, bucketID := makeV1BucketImportFixture(t)
	spec := iceberg.NewPartitionSpecID(1,
		iceberg.PartitionField{SourceIDs: []int{1}, FieldID: 1000, Name: "bucket", Transform: iceberg.BucketTransform{NumBuckets: 8}},
		iceberg.PartitionField{SourceIDs: []int{1}, FieldID: 1001, Name: "id", Transform: iceberg.IdentityTransform{}},
		iceberg.PartitionField{SourceIDs: []int{1}, FieldID: 1002, Name: "old", Transform: iceberg.VoidTransform{}},
	)
	convert := func(values map[int]any) (iceberg.DataFile, error) {
		return table.FileToDataFileWithOptions(t.Context(), fs, path, tbl.Metadata().CurrentSchema(), spec, 0, nil,
			table.WithFilePartitionValues(values))
	}

	_, err := convert(map[int]any{1000: bucketID})
	require.ErrorContains(t, err, "cannot infer partition field")
	_, err = convert(map[int]any{1000: bucketID, 1001: "7"})
	require.ErrorIs(t, err, iceberg.ErrInvalidArgument)
	file, err := convert(map[int]any{1000: bucketID, 1001: int32(7)})
	require.NoError(t, err)
	require.Equal(t, int32(7), file.Partition()[1001])
	require.Nil(t, file.Partition()[1002])

	// A supplied value cannot override the footer for identity partitions.
	_, err = convert(map[int]any{1000: bucketID, 1001: int32(99)})
	require.ErrorIs(t, err, iceberg.ErrInvalidArgument)
	_, err = convert(map[int]any{1000: bucketID, 1001: nil})
	require.ErrorIs(t, err, iceberg.ErrInvalidArgument)
}

func TestV1FileToDataFileChecksSuppliedOrderPreservingBounds(t *testing.T) {
	tbl, fs, path, _, _ := makeV1BucketImportFixture(t)
	spec := iceberg.NewPartitionSpecID(1,
		iceberg.PartitionField{SourceIDs: []int{1}, FieldID: 1000, Name: "id", Transform: iceberg.IdentityTransform{}},
		iceberg.PartitionField{SourceIDs: []int{1}, FieldID: 1001, Name: "id_truncated", Transform: iceberg.TruncateTransform{Width: 10}},
	)
	convert := func(values map[int]any) (iceberg.DataFile, error) {
		return table.FileToDataFileWithOptions(t.Context(), fs, path, tbl.Metadata().CurrentSchema(), spec, 0, nil,
			table.WithFilePartitionValues(values))
	}

	file, err := convert(map[int]any{1000: int32(7), 1001: int32(0)})
	require.NoError(t, err)
	require.Equal(t, int32(7), file.Partition()[1000])
	require.Equal(t, int32(0), file.Partition()[1001])

	inferred, err := convert(nil)
	require.NoError(t, err)
	require.Equal(t, file.Partition(), inferred.Partition())

	for _, tc := range []struct {
		name   string
		values map[int]any
	}{
		{"incorrect identity", map[int]any{1000: int32(99), 1001: int32(0)}},
		{"incorrect truncate", map[int]any{1000: int32(7), 1001: int32(10)}},
		{"null identity over non-null rows", map[int]any{1000: nil, 1001: int32(0)}},
		{"null truncate over non-null rows", map[int]any{1000: int32(7), 1001: nil}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, err := convert(tc.values)
			require.ErrorIs(t, err, iceberg.ErrInvalidArgument)
			require.ErrorContains(t, err, "does not match Parquet bounds")
		})
	}
}

func TestV1AddDataFilesByPathRejectsSuppliedIdentityMismatch(t *testing.T) {
	_, fs, path, _, _ := makeV1BucketImportFixture(t)
	dir := filepath.Dir(path)
	schema := iceberg.NewSchema(1,
		iceberg.NestedField{ID: 1, Name: "id", Type: iceberg.PrimitiveTypes.Int32, Required: true},
	)
	spec := iceberg.NewPartitionSpecID(0, iceberg.PartitionField{
		SourceIDs: []int{1}, FieldID: 1000, Name: "id", Transform: iceberg.IdentityTransform{},
	})
	meta, err := table.NewMetadata(schema, &spec, table.UnsortedSortOrder, dir, iceberg.Properties{
		table.PropertyFormatVersion: "1",
	})
	require.NoError(t, err)
	tbl := table.New(table.Identifier{"default", "identity_import"}, meta, filepath.Join(dir, "metadata", "00000.json"),
		func(context.Context) (iceio.IO, error) { return fs, nil }, &mergeCatalog{meta: meta})

	for _, tc := range []struct {
		name  string
		value any
	}{
		{"incorrect identity", int32(99)},
		{"null identity", nil},
	} {
		t.Run(tc.name, func(t *testing.T) {
			tx := tbl.NewTransaction()
			err := tx.AddDataFilesByPath(t.Context(), []string{path}, nil, false,
				table.WithAddDataFilesByPathPartitionValues(map[string]map[int]any{
					path: {1000: tc.value},
				}))
			require.ErrorIs(t, err, iceberg.ErrInvalidArgument)
			require.ErrorContains(t, err, "does not match Parquet bounds")
		})
	}

	tx := tbl.NewTransaction()
	require.NoError(t, tx.AddDataFilesByPath(t.Context(), []string{path}, nil, false,
		table.WithAddDataFilesByPathPartitionValues(map[string]map[int]any{
			path: {1000: int32(7)},
		})))
}

func TestV1ImportPartitionOptionsSnapshotCallerValues(t *testing.T) {
	tbl, fs, path, fieldID, bucketID := makeV1BucketImportFixture(t)
	values := map[int]any{fieldID: bucketID}
	option := table.WithFilePartitionValues(values)
	values[fieldID] = int32(-1)
	file, err := table.FileToDataFileWithOptions(t.Context(), fs, path,
		tbl.Metadata().CurrentSchema(), tbl.Metadata().PartitionSpec(), 0, nil, option)
	require.NoError(t, err)
	require.Equal(t, bucketID, file.Partition()[fieldID])

	byPath := map[string]map[int]any{path: {fieldID: bucketID}}
	addOption := table.WithAddDataFilesByPathPartitionValues(byPath)
	byPath[path][fieldID] = int32(-1)
	tx := tbl.NewTransaction()
	require.NoError(t, tx.AddDataFilesByPath(t.Context(), []string{path}, nil, false, addOption))
	committed, err := tx.Commit(t.Context())
	require.NoError(t, err)
	manifests, err := committed.CurrentSnapshot().Manifests(t.Context(), fs)
	require.NoError(t, err)
	require.NotEmpty(t, manifests)
	var found bool
	for _, manifest := range manifests {
		for entry, entryErr := range manifest.Entries(t.Context(), fs, true) {
			require.NoError(t, entryErr)
			if entry.DataFile().FilePath() == path {
				found = true
				require.Equal(t, bucketID, entry.DataFile().Partition()[fieldID])
			}
		}
	}
	require.True(t, found)
}
