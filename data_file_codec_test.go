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

package iceberg

import (
	"maps"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestManifestEntrySchemaForCaches(t *testing.T) {
	spec, schema, _ := fullyPopulatedDataFileForCodec(t, 2)
	s1, _, err := manifestEntrySchemaFor(spec, schema, 2)
	require.NoError(t, err)
	s2, _, err := manifestEntrySchemaFor(spec, schema, 2)
	require.NoError(t, err)
	require.Same(t, s1, s2, "schema should be cached by (specID, version)")
}

func TestMarshalAvroEntryDoesNotMutate(t *testing.T) {
	spec, schema, df := fullyPopulatedDataFileForCodec(t, 2)
	impl := df.(*dataFile)
	before := maps.Clone(impl.PartitionData)
	_, err := impl.MarshalAvroEntry(spec, schema, 2)
	require.NoError(t, err)
	require.Equal(t, before, impl.PartitionData, "MarshalAvroEntry must not mutate the source DataFile")
}

func fullyPopulatedDataFileForCodec(t *testing.T, version int) (PartitionSpec, *Schema, DataFile) {
	t.Helper()
	schema := NewSchema(123,
		NestedField{ID: 1, Name: "id", Type: Int64Type{}, Required: true},
		NestedField{ID: 2, Name: "name", Type: StringType{}},
	)
	spec := NewPartitionSpecID(7,
		PartitionField{SourceIDs: []int{1}, FieldID: 1000, Name: "id_part", Transform: IdentityTransform{}},
	)
	builder, err := NewDataFileBuilder(
		spec,
		EntryContentData,
		"s3://bucket/ns/tbl/data/part-0000.parquet",
		ParquetFile,
		map[int]any{1000: int64(42)},
		map[int]string{},
		map[int]int{},
		1024,
		1024*1024,
	)
	require.NoError(t, err)
	builder.
		ColumnSizes(map[int]int64{1: 512, 2: 256}).
		ValueCounts(map[int]int64{1: 1024, 2: 1024}).
		NullValueCounts(map[int]int64{1: 0, 2: 4}).
		NaNValueCounts(map[int]int64{1: 0, 2: 0}).
		LowerBoundValues(map[int][]byte{1: {0x01}, 2: []byte("a")}).
		UpperBoundValues(map[int][]byte{1: {0xff}, 2: []byte("z")}).
		SplitOffsets([]int64{0, 4096}).
		SortOrderID(0).
		KeyMetadata([]byte("kms-key-1"))
	if version < 3 {
		builder.DistinctValueCounts(map[int]int64{1: 64, 2: 128})
	}
	if version >= 2 {
		builder.EqualityFieldIDs([]int{1})
	}
	if version >= 3 {
		builder.FirstRowID(0).
			ReferencedDataFile("s3://bucket/ns/tbl/data/source.parquet").
			ContentOffset(128).
			ContentSizeInBytes(2048)
	}

	return spec, schema, builder.Build()
}
