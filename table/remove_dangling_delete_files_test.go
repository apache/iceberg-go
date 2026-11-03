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
	"testing"

	"github.com/apache/iceberg-go"
	"github.com/stretchr/testify/require"
)

func TestDecideDanglingDeleteFiles(t *testing.T) {
	schema := iceberg.NewSchema(0,
		iceberg.NestedField{ID: 1, Name: "id", Type: iceberg.PrimitiveTypes.Int64, Required: true},
	)
	spec := iceberg.NewPartitionSpec(iceberg.PartitionField{
		SourceIDs: []int{1}, FieldID: iceberg.PartitionDataIDStart,
		Name: "id", Transform: iceberg.IdentityTransform{},
	})
	meta, err := NewMetadata(schema, &spec, UnsortedSortOrder, t.TempDir(), nil)
	require.NoError(t, err)

	partition := map[int]any{iceberg.PartitionDataIDStart: int64(7)}
	key, err := canonicalPartitionKey(int32(spec.ID()), partition)
	require.NoError(t, err)
	makeDelete := func(path string, content iceberg.ManifestEntryContent, referenced string) iceberg.DataFile {
		t.Helper()
		b, err := iceberg.NewDataFileBuilder(spec, content, path, iceberg.ParquetFile, partition, nil, nil, 1, 128)
		require.NoError(t, err)
		if referenced != "" {
			b.ReferencedDataFile(referenced)
		}
		if content == iceberg.EntryContentEqDeletes {
			b.EqualityFieldIDs([]int{1})
		}

		return b.Build()
	}

	candidates := []danglingDeleteCandidate{
		{file: makeDelete("pos-applicable", iceberg.EntryContentPosDeletes, "live-data"), sequence: 1},
		{file: makeDelete("pos-absent", iceberg.EntryContentPosDeletes, "missing-data"), sequence: 2},
		{file: makeDelete("pos-old", iceberg.EntryContentPosDeletes, ""), sequence: 0},
		{file: makeDelete("pos-same-seq", iceberg.EntryContentPosDeletes, ""), sequence: 1},
		{file: makeDelete("eq-same-seq", iceberg.EntryContentEqDeletes, ""), sequence: 1},
		{file: makeDelete("eq-newer", iceberg.EntryContentEqDeletes, ""), sequence: 2},
	}
	// Even equal file_path bounds must not turn a partition-scoped delete
	// into an explicit single-file delete during metadata cleanup.
	unscoped, err := iceberg.NewDataFileBuilder(spec, iceberg.EntryContentPosDeletes,
		"pos-bounds-only", iceberg.ParquetFile, partition, nil, nil, 1, 128)
	require.NoError(t, err)
	unscoped.LowerBoundValues(map[int][]byte{filePathFieldID: []byte("missing-data")})
	unscoped.UpperBoundValues(map[int][]byte{filePathFieldID: []byte("missing-data")})
	candidates = append(candidates, danglingDeleteCandidate{file: unscoped.Build(), sequence: 2})

	surviving := map[string]danglingDataFile{
		"live-data": {partition: key, sequence: 1},
	}
	got, err := decideDanglingDeleteFiles(meta, candidates, surviving, map[string]int64{key: 1}, 1)
	require.NoError(t, err)
	var paths []string
	for _, df := range got {
		paths = append(paths, df.FilePath())
	}
	require.ElementsMatch(t, []string{"pos-absent", "pos-old", "eq-same-seq"}, paths)
}

func TestRemoveDanglingDeleteFilesUnpartitionedNoop(t *testing.T) {
	schema := iceberg.NewSchema(0,
		iceberg.NestedField{ID: 1, Name: "id", Type: iceberg.PrimitiveTypes.Int64, Required: true},
	)
	meta, err := NewMetadata(schema, iceberg.UnpartitionedSpec, UnsortedSortOrder, t.TempDir(), nil)
	require.NoError(t, err)
	tbl := New([]string{"db", "example"}, meta, "", nil, nil)
	updated, count, err := tbl.RemoveDanglingDeleteFiles(t.Context())
	require.NoError(t, err)
	require.Zero(t, count)
	require.Equal(t, tbl.Metadata(), updated.Metadata())
}

func TestDecideDanglingDeleteFilesGlobalEquality(t *testing.T) {
	schema := iceberg.NewSchema(0,
		iceberg.NestedField{ID: 1, Name: "id", Type: iceberg.PrimitiveTypes.Int64, Required: true},
	)
	meta, err := NewMetadata(schema, iceberg.UnpartitionedSpec, UnsortedSortOrder, t.TempDir(), nil)
	require.NoError(t, err)

	makeEqualityDelete := func(path string) iceberg.DataFile {
		t.Helper()
		builder, err := iceberg.NewDataFileBuilder(*iceberg.UnpartitionedSpec,
			iceberg.EntryContentEqDeletes, path, iceberg.ParquetFile, nil, nil, nil, 1, 128)
		require.NoError(t, err)
		return builder.EqualityFieldIDs([]int{1}).Build()
	}
	candidates := []danglingDeleteCandidate{
		{file: makeEqualityDelete("same-sequence"), sequence: 1},
		{file: makeEqualityDelete("newer-sequence"), sequence: 2},
	}
	// An equality delete written under an unpartitioned spec applies to
	// data in any partition. The global minimum sequence is authoritative.
	got, err := decideDanglingDeleteFiles(meta, candidates,
		map[string]danglingDataFile{}, map[string]int64{}, 1)
	require.NoError(t, err)
	require.Len(t, got, 1)
	require.Equal(t, "same-sequence", got[0].FilePath())
}
