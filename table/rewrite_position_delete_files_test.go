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

	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/iceberg-go"
	"github.com/stretchr/testify/require"
)

func TestSortUniquePositionDeleteRows(t *testing.T) {
	rows := []positionDeleteRow{
		{path: "b.parquet", position: 4},
		{path: "a.parquet", position: 10},
		{path: "a.parquet", position: 1},
		{path: "b.parquet", position: 4},
		{path: "a.parquet", position: 1},
		{path: "b.parquet", position: 2},
	}
	got := sortUniquePositionDeleteRows(rows)
	require.Equal(t, []positionDeleteRow{
		{path: "a.parquet", position: 1},
		{path: "a.parquet", position: 10},
		{path: "b.parquet", position: 2},
		{path: "b.parquet", position: 4},
	}, got)
}

func TestSplitPositionDeleteRewriteGroup(t *testing.T) {
	var files []iceberg.DataFile
	for _, path := range []string{"d", "b", "a", "c"} {
		b, err := iceberg.NewDataFileBuilder(*iceberg.UnpartitionedSpec, iceberg.EntryContentPosDeletes,
			path, iceberg.ParquetFile, nil, nil, nil, 1, 10)
		require.NoError(t, err)
		files = append(files, b.Build())
	}
	sequences := map[string]int64{"a": 2, "b": 9, "c": 5, "d": 7}
	group := &positionDeleteRewriteGroup{files: files, sequences: sequences}
	groups := splitPositionDeleteRewriteGroup(group, 20)
	require.Len(t, groups, 2)
	require.Equal(t, "a", groups[0].files[0].FilePath())
	require.Equal(t, "b", groups[0].files[1].FilePath())
	require.Equal(t, int64(9), groups[0].maxSequence)
	require.Equal(t, int64(2), groups[0].sequences["a"])
	require.Equal(t, "c", groups[1].files[0].FilePath())
	require.Equal(t, "d", groups[1].files[1].FilePath())
	require.Equal(t, int64(7), groups[1].maxSequence)
	require.Equal(t, int64(5), groups[1].sequences["c"])
	require.Empty(t, splitPositionDeleteRewriteGroup(group, 9))
}

func TestRewritePositionDeleteFilesEmptySnapshot(t *testing.T) {
	schema := iceberg.NewSchema(0,
		iceberg.NestedField{ID: 1, Name: "id", Type: iceberg.PrimitiveTypes.Int64, Required: true},
	)
	meta, err := NewMetadata(schema, iceberg.UnpartitionedSpec, UnsortedSortOrder, t.TempDir(), nil)
	require.NoError(t, err)
	tbl := New([]string{"db", "example"}, meta, "", nil, nil)
	updated, rewritten, err := tbl.RewritePositionDeleteFiles(t.Context(), PositionDeleteRewriteOptions{})
	require.NoError(t, err)
	require.Zero(t, rewritten)
	require.Equal(t, tbl.Metadata(), updated.Metadata())
}

func TestRewritePositionDeleteFilesRejectsV3(t *testing.T) {
	schema := iceberg.NewSchema(0,
		iceberg.NestedField{ID: 1, Name: "id", Type: iceberg.PrimitiveTypes.Int64, Required: true},
	)
	meta, err := NewMetadata(schema, iceberg.UnpartitionedSpec, UnsortedSortOrder, t.TempDir(),
		iceberg.Properties{PropertyFormatVersion: "3"})
	require.NoError(t, err)
	tbl := New([]string{"db", "example"}, meta, "", nil, nil)
	_, _, err = tbl.RewritePositionDeleteFiles(t.Context(), PositionDeleteRewriteOptions{})
	require.ErrorIs(t, err, iceberg.ErrNotImplemented)
}

func TestRewritePositionDeletePathInScope(t *testing.T) {
	builder, err := iceberg.NewDataFileBuilder(*iceberg.UnpartitionedSpec,
		iceberg.EntryContentPosDeletes, "delete.parquet", iceberg.ParquetFile,
		nil, nil, nil, 2, 128)
	require.NoError(t, err)
	builder.LowerBoundValues(map[int][]byte{filePathFieldID: []byte("old-metrics-path")})
	builder.UpperBoundValues(map[int][]byte{filePathFieldID: []byte("old-metrics-path")})
	require.True(t, rewritePositionDeletePathInScope(builder.Build(), "live.parquet"),
		"file_path statistics do not define the referenced_data_file scope")

	builder.ReferencedDataFile("target.parquet")
	require.True(t, rewritePositionDeletePathInScope(builder.Build(), "target.parquet"))
	require.False(t, rewritePositionDeletePathInScope(builder.Build(), "other.parquet"))
}

func TestValidateRewritePositionDeletePositions(t *testing.T) {
	for _, tc := range []struct {
		name    string
		values  []int64
		valid   []bool
		invalid bool
	}{
		{name: "valid", values: []int64{0, 42}},
		{name: "negative", values: []int64{-1}, invalid: true},
		{name: "null", values: []int64{0}, valid: []bool{false}, invalid: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			builder := array.NewInt64Builder(memory.DefaultAllocator)
			builder.AppendValues(tc.values, tc.valid)
			positions := builder.NewInt64Array()
			builder.Release()
			err := validateRewritePositionDeletePositions("delete.parquet", positions)
			positions.Release()
			if tc.invalid {
				require.ErrorIs(t, err, iceberg.ErrInvalidSchema)
			} else {
				require.NoError(t, err)
			}
		})
	}
}
