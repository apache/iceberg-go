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
	"bytes"
	"strings"
	"testing"

	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/arrow-go/v18/parquet"
	"github.com/apache/arrow-go/v18/parquet/file"
	"github.com/apache/arrow-go/v18/parquet/metadata"
	"github.com/apache/arrow-go/v18/parquet/pqarrow"
	"github.com/apache/iceberg-go"
	"github.com/apache/iceberg-go/table"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func parquetMetaFromSchema(t *testing.T, sch *iceberg.Schema, jsonRows string) ([]byte, *metadata.FileMetaData) {
	t.Helper()

	arrowSch, err := table.SchemaToArrowSchema(sch, nil, true, false)
	require.NoError(t, err)

	rec, _, err := array.RecordFromJSON(memory.DefaultAllocator, arrowSch, strings.NewReader(jsonRows))
	require.NoError(t, err)
	defer rec.Release()

	var buf bytes.Buffer
	wr, err := pqarrow.NewFileWriter(arrowSch, &buf,
		parquet.NewWriterProperties(parquet.WithStats(true)),
		pqarrow.DefaultWriterProps())
	require.NoError(t, err)
	require.NoError(t, wr.Write(rec))
	require.NoError(t, wr.Close())

	rdr, err := file.NewParquetReader(bytes.NewReader(buf.Bytes()))
	require.NoError(t, err)
	defer rdr.Close()

	return buf.Bytes(), rdr.MetaData()
}

var dataSchema = iceberg.NewSchema(0,
	iceberg.NestedField{ID: 1, Name: "id", Type: iceberg.PrimitiveTypes.Int32, Required: false},
	iceberg.NestedField{ID: 2, Name: "name", Type: iceberg.PrimitiveTypes.String, Required: false},
)

const dataRows = `[
	{"id": 1, "name": "alpha"},
	{"id": 2, "name": "beta"},
	{"id": 3, "name": "gamma"}
]`

func decodeInt32Bound(t *testing.T, raw []byte) int32 {
	t.Helper()
	lit, err := iceberg.LiteralFromBytes(iceberg.PrimitiveTypes.Int32, raw)
	require.NoError(t, err)
	v, ok := lit.(iceberg.Int32Literal)
	require.True(t, ok, "expected Int32Literal, got %T", lit)

	return v.Value()
}

func decodeStringBound(t *testing.T, raw []byte) string {
	t.Helper()
	lit, err := iceberg.LiteralFromBytes(iceberg.PrimitiveTypes.String, raw)
	require.NoError(t, err)
	v, ok := lit.(iceberg.StringLiteral)
	require.True(t, ok, "expected StringLiteral, got %T", lit)

	return string(v)
}

func TestDataFileFromMetadata(t *testing.T) {
	unpartitioned := iceberg.NewPartitionSpec()

	type want struct {
		content          iceberg.ManifestEntryContent
		recordCount      int64
		equalityFieldIDs []int
		boundIDs         []int
		partition        map[int]any
	}

	type subtest struct {
		name   string
		sch    *iceberg.Schema
		rows   string
		args   func(sch *iceberg.Schema, pqBytes []byte, pqMeta *metadata.FileMetaData) table.DataFileArgs
		want   want
		verify func(t *testing.T, df iceberg.DataFile)
	}

	cases := []subtest{
		{
			name: "data file: full column stats with real bounds",
			sch:  dataSchema,
			rows: dataRows,
			args: func(sch *iceberg.Schema, pqBytes []byte, pqMeta *metadata.FileMetaData) table.DataFileArgs {
				return table.DataFileArgs{
					Schema:   sch,
					Spec:     unpartitioned,
					Format:   iceberg.ParquetFile,
					Metadata: pqMeta,
					FilePath: "s3://bucket/data/test-data-001.parquet",
					FileSize: int64(len(pqBytes)),
					Content:  iceberg.EntryContentData,
				}
			},
			want: want{
				content:     iceberg.EntryContentData,
				recordCount: 3,
				boundIDs:    []int{1, 2},
			},
			verify: func(t *testing.T, df iceberg.DataFile) {
				// id column (field 1) lower/upper from the JSON rows.
				assert.EqualValues(t, 1, decodeInt32Bound(t, df.LowerBoundValues()[1]))
				assert.EqualValues(t, 3, decodeInt32Bound(t, df.UpperBoundValues()[1]))
				// name column (field 2) lexicographic min/max are "alpha" / "gamma".
				assert.Equal(t, "alpha", decodeStringBound(t, df.LowerBoundValues()[2]))
				assert.Equal(t, "gamma", decodeStringBound(t, df.UpperBoundValues()[2]))
				assert.Nil(t, df.EqualityFieldIDs(), "data files must not have equality field IDs")
			},
		},
		{
			name: "equality-delete file: EqualityFieldIDs round-trips",
			sch: iceberg.NewSchema(0,
				iceberg.NestedField{ID: 1, Name: "id", Type: iceberg.PrimitiveTypes.Int32, Required: true},
			),
			rows: `[{"id": 10}, {"id": 20}]`,
			args: func(sch *iceberg.Schema, pqBytes []byte, pqMeta *metadata.FileMetaData) table.DataFileArgs {
				return table.DataFileArgs{
					Schema:           sch,
					Spec:             unpartitioned,
					Format:           iceberg.ParquetFile,
					Metadata:         pqMeta,
					FilePath:         "s3://bucket/data/eq-del-001.parquet",
					FileSize:         int64(len(pqBytes)),
					Content:          iceberg.EntryContentEqDeletes,
					EqualityFieldIDs: []int{1},
				}
			},
			want: want{
				content:          iceberg.EntryContentEqDeletes,
				recordCount:      2,
				equalityFieldIDs: []int{1},
				boundIDs:         []int{1},
			},
			verify: func(t *testing.T, df iceberg.DataFile) {
				assert.EqualValues(t, 10, decodeInt32Bound(t, df.LowerBoundValues()[1]))
				assert.EqualValues(t, 20, decodeInt32Bound(t, df.UpperBoundValues()[1]))
			},
		},
		{
			name: "partitioned data file: identity transform plumbs partition values",
			sch:  dataSchema,
			rows: `[
				{"id": 7, "name": "alpha"},
				{"id": 7, "name": "beta"}
			]`,
			args: func(sch *iceberg.Schema, pqBytes []byte, pqMeta *metadata.FileMetaData) table.DataFileArgs {
				spec := iceberg.NewPartitionSpec(iceberg.PartitionField{
					SourceIDs: []int{1},
					FieldID:   1000,
					Name:      "id",
					Transform: iceberg.IdentityTransform{},
				})

				return table.DataFileArgs{
					Schema:          sch,
					Spec:            spec,
					Format:          iceberg.ParquetFile,
					Metadata:        pqMeta,
					FilePath:        "s3://bucket/data/id=7/test-data-001.parquet",
					FileSize:        int64(len(pqBytes)),
					Content:         iceberg.EntryContentData,
					PartitionValues: map[int]any{1000: int32(7)},
				}
			},
			want: want{
				content:     iceberg.EntryContentData,
				recordCount: 2,
				boundIDs:    []int{1, 2},
				partition:   map[int]any{1000: int32(7)},
			},
			verify: func(t *testing.T, df iceberg.DataFile) {
				assert.EqualValues(t, 7, decodeInt32Bound(t, df.LowerBoundValues()[1]))
				assert.EqualValues(t, 7, decodeInt32Bound(t, df.UpperBoundValues()[1]))
			},
		},
		{
			name: "positional-delete file: reserved field IDs in schema",
			sch:  iceberg.PositionalDeleteSchema,
			rows: `[
				{"file_path": "s3://bucket/data/data-001.parquet", "pos": 0},
				{"file_path": "s3://bucket/data/data-001.parquet", "pos": 5}
			]`,
			args: func(sch *iceberg.Schema, pqBytes []byte, pqMeta *metadata.FileMetaData) table.DataFileArgs {
				return table.DataFileArgs{
					Schema:   sch,
					Spec:     unpartitioned,
					Format:   iceberg.ParquetFile,
					Metadata: pqMeta,
					FilePath: "s3://bucket/data/pos-del-001.parquet",
					FileSize: int64(len(pqBytes)),
					Content:  iceberg.EntryContentPosDeletes,
				}
			},
			want: want{
				content:     iceberg.EntryContentPosDeletes,
				recordCount: 2,
				boundIDs:    []int{2147483545, 2147483546},
			},
			verify: func(t *testing.T, df iceberg.DataFile) {
				assert.Nil(t, df.EqualityFieldIDs(),
					"positional delete files must not have equality field IDs")
			},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			pqBytes, pqMeta := parquetMetaFromSchema(t, tc.sch, tc.rows)
			args := tc.args(tc.sch, pqBytes, pqMeta)

			df, err := table.DataFileFromMetadata(args)
			require.NoError(t, err)
			require.NotNil(t, df)

			assert.Equal(t, tc.want.content, df.ContentType())
			assert.Equal(t, iceberg.ParquetFile, df.FileFormat())
			assert.Equal(t, args.FilePath, df.FilePath())
			assert.Equal(t, int64(len(pqBytes)), df.FileSizeBytes())
			assert.EqualValues(t, tc.want.recordCount, df.Count())
			assert.Equal(t, tc.want.equalityFieldIDs, df.EqualityFieldIDs())

			if tc.want.partition != nil {
				assert.Equal(t, tc.want.partition, df.Partition(),
					"partition data must round-trip from PartitionValues onto DataFile.Partition()")
			}

			assert.Len(t, df.ValueCounts(), len(tc.want.boundIDs))
			assert.Len(t, df.LowerBoundValues(), len(tc.want.boundIDs))
			assert.Len(t, df.UpperBoundValues(), len(tc.want.boundIDs))
			for _, id := range tc.want.boundIDs {
				assert.Contains(t, df.LowerBoundValues(), id,
					"missing lower bound for field id %d", id)
				assert.Contains(t, df.UpperBoundValues(), id,
					"missing upper bound for field id %d", id)
			}

			if tc.verify != nil {
				tc.verify(t, df)
			}
		})
	}
}

func TestDataFileFromMetadata_FirstRowID(t *testing.T) {
	pqBytes, pqMeta := parquetMetaFromSchema(t, dataSchema, dataRows)
	firstRowID := int64(42)

	df, err := table.DataFileFromMetadata(table.DataFileArgs{
		Schema:     dataSchema,
		Spec:       iceberg.NewPartitionSpec(),
		Format:     iceberg.ParquetFile,
		Metadata:   pqMeta,
		FilePath:   "s3://bucket/data/v3-data.parquet",
		FileSize:   int64(len(pqBytes)),
		Content:    iceberg.EntryContentData,
		FirstRowID: &firstRowID,
	})
	require.NoError(t, err)
	require.NotNil(t, df.FirstRowID(), "FirstRowID must be plumbed through for v3 callers")
	assert.Equal(t, firstRowID, *df.FirstRowID())
}

func TestDataFileFromMetadata_Errors(t *testing.T) {
	pqBytes, pqMeta := parquetMetaFromSchema(t, dataSchema, dataRows)
	baseSize := int64(len(pqBytes))

	baseDataArgs := func() table.DataFileArgs {
		return table.DataFileArgs{
			Schema:   dataSchema,
			Spec:     iceberg.NewPartitionSpec(),
			Format:   iceberg.ParquetFile,
			Metadata: pqMeta,
			FilePath: "s3://bucket/data/err.parquet",
			FileSize: baseSize,
			Content:  iceberg.EntryContentData,
		}
	}

	cases := []struct {
		name    string
		mutate  func(a *table.DataFileArgs)
		wantSub string
	}{
		{
			name:    "nil schema",
			mutate:  func(a *table.DataFileArgs) { a.Schema = nil },
			wantSub: "schema is required",
		},
		{
			name:    "nil metadata",
			mutate:  func(a *table.DataFileArgs) { a.Metadata = nil },
			wantSub: "file metadata is required",
		},
		{
			name:    "empty file path",
			mutate:  func(a *table.DataFileArgs) { a.FilePath = "" },
			wantSub: "file path is required",
		},
		{
			name:    "non-positive file size",
			mutate:  func(a *table.DataFileArgs) { a.FileSize = 0 },
			wantSub: "file size must be greater than 0",
		},
		{
			name:    "invalid content value",
			mutate:  func(a *table.DataFileArgs) { a.Content = iceberg.ManifestEntryContent(99) },
			wantSub: "invalid content",
		},
		{
			name: "equality-delete file with empty EqualityFieldIDs",
			mutate: func(a *table.DataFileArgs) {
				a.Content = iceberg.EntryContentEqDeletes
				a.EqualityFieldIDs = nil
			},
			wantSub: "EqualityFieldIDs is required",
		},
		{
			name: "data file with non-empty EqualityFieldIDs",
			mutate: func(a *table.DataFileArgs) {
				a.EqualityFieldIDs = []int{1}
			},
			wantSub: "EqualityFieldIDs must be empty",
		},
		{
			name: "positional-delete file with nonzero SortOrderID",
			mutate: func(a *table.DataFileArgs) {
				a.Schema = iceberg.PositionalDeleteSchema
				a.Content = iceberg.EntryContentPosDeletes
				a.SortOrderID = 1
				_, posMeta := parquetMetaFromSchema(t, iceberg.PositionalDeleteSchema,
					`[{"file_path": "s3://b/d.parquet", "pos": 0}]`)
				a.Metadata = posMeta
			},
			wantSub: "position delete file claims sort order id 1",
		},
		{
			name: "equality-delete with unknown field id",
			mutate: func(a *table.DataFileArgs) {
				a.Content = iceberg.EntryContentEqDeletes
				a.EqualityFieldIDs = []int{999}
			},
			wantSub: "field id 999",
		},
		{
			name: "positional-delete schema missing reserved field IDs",
			mutate: func(a *table.DataFileArgs) {
				a.Content = iceberg.EntryContentPosDeletes
			},
			wantSub: "positional delete schema requires reserved field id",
		},
		{
			name: "partitioned spec missing partition values",
			mutate: func(a *table.DataFileArgs) {
				a.Spec = iceberg.NewPartitionSpec(iceberg.PartitionField{
					SourceIDs: []int{1},
					FieldID:   1000,
					Name:      "id_bucket",
					Transform: iceberg.BucketTransform{NumBuckets: 4},
				})
			},
			wantSub: "missing partition value",
		},
		{
			name: "partition value for unknown field id",
			mutate: func(a *table.DataFileArgs) {
				a.Spec = iceberg.NewPartitionSpec(iceberg.PartitionField{
					SourceIDs: []int{1},
					FieldID:   1000,
					Name:      "id_bucket",
					Transform: iceberg.BucketTransform{NumBuckets: 4},
				})
				a.PartitionValues = map[int]any{1000: 0, 1001: 0}
			},
			wantSub: "field id 1001",
		},
		{
			name: "multi-value partition field is recovered into an error",
			mutate: func(a *table.DataFileArgs) {
				a.Spec = iceberg.NewPartitionSpec(iceberg.PartitionField{
					SourceIDs: []int{1},
					FieldID:   1000,
					Name:      "id",
					Transform: iceberg.IdentityTransform{},
				})
				a.PartitionValues = map[int]any{1000: int32(1)}
			},
			wantSub: "more than one value for partition field",
		},
		{
			name: "positional-delete schema reserved field has wrong type",
			mutate: func(a *table.DataFileArgs) {
				a.Content = iceberg.EntryContentPosDeletes
				a.Schema = iceberg.NewSchema(0,
					iceberg.NestedField{ID: 2147483546, Name: "file_path", Type: iceberg.PrimitiveTypes.String, Required: true},
					iceberg.NestedField{ID: 2147483545, Name: "pos", Type: iceberg.PrimitiveTypes.String, Required: true},
				)
			},
			wantSub: "must have type",
		},
		{
			name: "invalid metrics mode property fails stats plan",
			mutate: func(a *table.DataFileArgs) {
				a.Properties = iceberg.Properties{
					table.DefaultWriteMetricsModeKey: "truncate(0)",
				}
			},
			wantSub: "failed to build stats plan",
		},
		{
			name: "wrong metadata concrete type",
			mutate: func(a *table.DataFileArgs) {
				a.Metadata = "not a *metadata.FileMetaData"
			},
			wantSub: "unsupported metadata type",
		},
		{
			name: "typed-nil metadata pointer",
			mutate: func(a *table.DataFileArgs) {
				a.Metadata = (*metadata.FileMetaData)(nil)
			},
			wantSub: "file metadata is required",
		},
		{
			name: "unsupported file format",
			mutate: func(a *table.DataFileArgs) {
				a.Format = iceberg.AvroFile
			},
			wantSub: "unsupported file format",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			args := baseDataArgs()
			tc.mutate(&args)

			df, err := table.DataFileFromMetadata(args)
			require.Error(t, err, "expected validation error, got DataFile %+v", df)
			require.Nil(t, df)
			assert.Contains(t, err.Error(), tc.wantSub)
		})
	}
}
