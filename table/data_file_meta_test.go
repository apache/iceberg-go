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
	iceio "github.com/apache/iceberg-go/io"
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
		{
			name: "partitioned data file: caller value wins over a non-constant column",
			sch:  dataSchema,
			rows: `[
				{"id": 1, "name": "alpha"},
				{"id": 5, "name": "beta"},
				{"id": 10, "name": "gamma"}
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
					FilePath:        "s3://bucket/data/id=42/range.parquet",
					FileSize:        int64(len(pqBytes)),
					Content:         iceberg.EntryContentData,
					PartitionValues: map[int]any{1000: int32(42)},
				}
			},
			want: want{
				content:     iceberg.EntryContentData,
				recordCount: 3,
				boundIDs:    []int{1, 2},
				partition:   map[int]any{1000: int32(42)},
			},
			verify: func(t *testing.T, df iceberg.DataFile) {
				assert.EqualValues(t, 1, decodeInt32Bound(t, df.LowerBoundValues()[1]))
				assert.EqualValues(t, 10, decodeInt32Bound(t, df.UpperBoundValues()[1]))
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
		Schema:        dataSchema,
		Spec:          iceberg.NewPartitionSpec(),
		Format:        iceberg.ParquetFile,
		Metadata:      pqMeta,
		FilePath:      "s3://bucket/data/v3-data.parquet",
		FileSize:      int64(len(pqBytes)),
		Content:       iceberg.EntryContentData,
		FormatVersion: 3,
		FirstRowID:    &firstRowID,
	})
	require.NoError(t, err)
	require.NotNil(t, df.FirstRowID(), "FirstRowID must be plumbed through for v3 callers")
	assert.Equal(t, firstRowID, *df.FirstRowID())
}

func TestDataFileFromMetadata_FirstRowIDBelowV3Errors(t *testing.T) {
	pqBytes, pqMeta := parquetMetaFromSchema(t, dataSchema, dataRows)
	firstRowID := int64(42)

	for _, version := range []int{0, 1, 2} {
		t.Run("v"+formatVersionStr(version), func(t *testing.T) {
			df, err := table.DataFileFromMetadata(table.DataFileArgs{
				Schema:        dataSchema,
				Spec:          iceberg.NewPartitionSpec(),
				Format:        iceberg.ParquetFile,
				Metadata:      pqMeta,
				FilePath:      "s3://bucket/data/pre-v3-data.parquet",
				FileSize:      int64(len(pqBytes)),
				Content:       iceberg.EntryContentData,
				FormatVersion: version,
				FirstRowID:    &firstRowID,
			})
			require.Nil(t, df, "FirstRowID must not be silently dropped below v3")
			require.ErrorContains(t, err, "requires table format version >= 3")
		})
	}
}

func TestDataFileFromMetadata_PosDeleteSortOrderAccepted(t *testing.T) {
	_, posMeta := parquetMetaFromSchema(t, iceberg.PositionalDeleteSchema,
		`[{"file_path": "s3://b/d.parquet", "pos": 0}]`)

	df, err := table.DataFileFromMetadata(table.DataFileArgs{
		Schema:      iceberg.PositionalDeleteSchema,
		Spec:        iceberg.NewPartitionSpec(),
		Format:      iceberg.ParquetFile,
		Metadata:    posMeta,
		FilePath:    "s3://bucket/data/pos-del-sorted.parquet",
		FileSize:    1024,
		Content:     iceberg.EntryContentPosDeletes,
		SortOrderID: 1,
	})
	require.NoError(t, err)
	require.NotNil(t, df.SortOrderID())
	assert.Equal(t, 1, *df.SortOrderID())
}

func TestDataFileFromMetadata_EqualityFieldFloatRejected(t *testing.T) {
	sch := iceberg.NewSchema(0,
		iceberg.NestedField{ID: 1, Name: "id", Type: iceberg.PrimitiveTypes.Int32, Required: true},
		iceberg.NestedField{ID: 2, Name: "amount", Type: iceberg.PrimitiveTypes.Float64, Required: false},
	)
	pqBytes, pqMeta := parquetMetaFromSchema(t, sch, `[{"id": 1, "amount": 1.5}]`)

	df, err := table.DataFileFromMetadata(table.DataFileArgs{
		Schema:           sch,
		Spec:             iceberg.NewPartitionSpec(),
		Format:           iceberg.ParquetFile,
		Metadata:         pqMeta,
		FilePath:         "s3://bucket/data/eq-del-float.parquet",
		FileSize:         int64(len(pqBytes)),
		Content:          iceberg.EntryContentEqDeletes,
		EqualityFieldIDs: []int{2},
	})
	require.Nil(t, df, "floating-point equality field must be rejected, matching the writer path")
	require.ErrorContains(t, err, "floating-point")
}

func TestDataFileFromMetadata_VoidPartitionAllowsNil(t *testing.T) {
	pqBytes, pqMeta := parquetMetaFromSchema(t, dataSchema, dataRows)

	spec := iceberg.NewPartitionSpec(iceberg.PartitionField{
		SourceIDs: []int{1},
		FieldID:   1000,
		Name:      "id_void",
		Transform: iceberg.VoidTransform{},
	})

	df, err := table.DataFileFromMetadata(table.DataFileArgs{
		Schema:          dataSchema,
		Spec:            spec,
		Format:          iceberg.ParquetFile,
		Metadata:        pqMeta,
		FilePath:        "s3://bucket/data/void.parquet",
		FileSize:        int64(len(pqBytes)),
		Content:         iceberg.EntryContentData,
		PartitionValues: map[int]any{1000: nil},
	})
	require.NoError(t, err)
	require.NotNil(t, df)

	part := df.Partition()
	require.Contains(t, part, 1000, "void partition field must be recorded on the DataFile")
	assert.Nil(t, part[1000], "void transform partition value must be nil")
}

func TestDataFileFromMetadata_AbsentPartitionKeyEqualsNil(t *testing.T) {
	spec := iceberg.NewPartitionSpec(
		iceberg.PartitionField{SourceIDs: []int{1}, FieldID: 1000, Name: "id", Transform: iceberg.IdentityTransform{}},
		iceberg.PartitionField{SourceIDs: []int{1}, FieldID: 1001, Name: "id_bucket", Transform: iceberg.BucketTransform{NumBuckets: 4}},
	)

	pqBytes, pqMeta := parquetMetaFromSchema(t, dataSchema, `[
		{"id": 7, "name": "alpha"},
		{"id": 7, "name": "beta"}
	]`)

	df, err := table.DataFileFromMetadata(table.DataFileArgs{
		Schema:          dataSchema,
		Spec:            spec,
		Format:          iceberg.ParquetFile,
		Metadata:        pqMeta,
		FilePath:        "s3://bucket/data/id=7/absent.parquet",
		FileSize:        int64(len(pqBytes)),
		Content:         iceberg.EntryContentData,
		PartitionValues: map[int]any{1000: nil},
	})
	require.NoError(t, err, "a present-nil value and an absent key must both be accepted")
	require.NotNil(t, df)

	part := df.Partition()
	assert.EqualValues(t, 7, part[1000],
		"a present-with-nil order-preserving partition key must be inferred from stats")
	assert.Nil(t, part[1001],
		"an absent non-order-preserving (bucket) partition key must stay nil")
}

func TestDataFileFromMetadata_NilPartitionValueInferredFromStats(t *testing.T) {
	spec := iceberg.NewPartitionSpec(iceberg.PartitionField{
		SourceIDs: []int{1},
		FieldID:   1000,
		Name:      "id",
		Transform: iceberg.IdentityTransform{},
	})

	t.Run("single-valued column infers the partition value", func(t *testing.T) {
		pqBytes, pqMeta := parquetMetaFromSchema(t, dataSchema, `[
			{"id": 7, "name": "alpha"},
			{"id": 7, "name": "beta"}
		]`)

		df, err := table.DataFileFromMetadata(table.DataFileArgs{
			Schema:          dataSchema,
			Spec:            spec,
			Format:          iceberg.ParquetFile,
			Metadata:        pqMeta,
			FilePath:        "s3://bucket/data/id=7/inferred.parquet",
			FileSize:        int64(len(pqBytes)),
			Content:         iceberg.EntryContentData,
			PartitionValues: map[int]any{1000: nil},
		})
		require.NoError(t, err)
		assert.EqualValues(t, 7, df.Partition()[1000],
			"a nil partition value must be inferred from the file's column statistics when the column is single-valued")
	})

	t.Run("multi-valued column cannot infer and errors", func(t *testing.T) {
		pqBytes, pqMeta := parquetMetaFromSchema(t, dataSchema, `[
			{"id": 1, "name": "alpha"},
			{"id": 9, "name": "beta"}
		]`)

		df, err := table.DataFileFromMetadata(table.DataFileArgs{
			Schema:          dataSchema,
			Spec:            spec,
			Format:          iceberg.ParquetFile,
			Metadata:        pqMeta,
			FilePath:        "s3://bucket/data/range.parquet",
			FileSize:        int64(len(pqBytes)),
			Content:         iceberg.EntryContentData,
			PartitionValues: map[int]any{1000: nil},
		})
		require.Nil(t, df, "no DataFile must be returned when inference is ambiguous")
		require.ErrorContains(t, err, "cannot infer partition value")
	})
}

func TestDataFileFromMetadata_ReferencedDataFile(t *testing.T) {
	const dataPath = "s3://bucket/data/data-001.parquet"

	_, fileScopedMeta := parquetMetaFromSchema(t, iceberg.PositionalDeleteSchema,
		`[{"file_path": "`+dataPath+`", "pos": 0},
		  {"file_path": "`+dataPath+`", "pos": 5}]`)

	_, partitionScopedMeta := parquetMetaFromSchema(t, iceberg.PositionalDeleteSchema,
		`[{"file_path": "`+dataPath+`", "pos": 0},
		  {"file_path": "s3://bucket/data/data-002.parquet", "pos": 3}]`)

	t.Run("file-scoped sets referenced_data_file", func(t *testing.T) {
		ref := dataPath
		df, err := table.DataFileFromMetadata(table.DataFileArgs{
			Schema:             iceberg.PositionalDeleteSchema,
			Spec:               iceberg.NewPartitionSpec(),
			Format:             iceberg.ParquetFile,
			Metadata:           fileScopedMeta,
			FilePath:           "s3://bucket/data/pos-del-001.parquet",
			FileSize:           1024,
			Content:            iceberg.EntryContentPosDeletes,
			ReferencedDataFile: &ref,
		})
		require.NoError(t, err)
		require.NotNil(t, df.ReferencedDataFile())
		assert.Equal(t, dataPath, *df.ReferencedDataFile())
	})

	t.Run("partition-scoped leaves referenced_data_file nil", func(t *testing.T) {
		df, err := table.DataFileFromMetadata(table.DataFileArgs{
			Schema:   iceberg.PositionalDeleteSchema,
			Spec:     iceberg.NewPartitionSpec(),
			Format:   iceberg.ParquetFile,
			Metadata: partitionScopedMeta,
			FilePath: "s3://bucket/data/pos-del-002.parquet",
			FileSize: 1024,
			Content:  iceberg.EntryContentPosDeletes,
			// ReferencedDataFile intentionally nil.
		})
		require.NoError(t, err)
		assert.Nil(t, df.ReferencedDataFile(),
			"a partition-scoped position delete must not record a referenced data file")
	})

	t.Run("ReferencedDataFile that does not scope to a single file is rejected", func(t *testing.T) {
		// partitionScopedMeta spans two data files, so its file_path bounds are
		// not equal; claiming a single referenced data file must fail.
		ref := dataPath
		df, err := table.DataFileFromMetadata(table.DataFileArgs{
			Schema:             iceberg.PositionalDeleteSchema,
			Spec:               iceberg.NewPartitionSpec(),
			Format:             iceberg.ParquetFile,
			Metadata:           partitionScopedMeta,
			FilePath:           "s3://bucket/data/pos-del-003.parquet",
			FileSize:           1024,
			Content:            iceberg.EntryContentPosDeletes,
			ReferencedDataFile: &ref,
		})
		require.Nil(t, df, "a delete file spanning multiple data files must not be accepted as file-scoped")
		require.ErrorContains(t, err, "does not match the position-delete file_path bounds")
	})

	t.Run("ReferencedDataFile pointing at the wrong data file is rejected", func(t *testing.T) {
		// The delete file scopes to dataPath, but the caller claims a different file.
		ref := "s3://bucket/data/some-other-file.parquet"
		df, err := table.DataFileFromMetadata(table.DataFileArgs{
			Schema:             iceberg.PositionalDeleteSchema,
			Spec:               iceberg.NewPartitionSpec(),
			Format:             iceberg.ParquetFile,
			Metadata:           fileScopedMeta,
			FilePath:           "s3://bucket/data/pos-del-004.parquet",
			FileSize:           1024,
			Content:            iceberg.EntryContentPosDeletes,
			ReferencedDataFile: &ref,
		})
		require.Nil(t, df)
		require.ErrorContains(t, err, "does not match the position-delete file_path bounds")
	})
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
			wantSub: "equality_ids is required",
		},
		{
			name: "data file with non-empty EqualityFieldIDs",
			mutate: func(a *table.DataFileArgs) {
				a.EqualityFieldIDs = []int{1}
			},
			wantSub: "equality_ids must be empty",
		},
		{
			name: "equality-delete with unknown field id",
			mutate: func(a *table.DataFileArgs) {
				a.Content = iceberg.EntryContentEqDeletes
				a.EqualityFieldIDs = []int{999}
			},
			wantSub: "field ID 999 not found in table schema",
		},
		{
			name: "positional-delete schema missing reserved field IDs",
			mutate: func(a *table.DataFileArgs) {
				a.Content = iceberg.EntryContentPosDeletes
			},
			wantSub: "positional delete schema requires reserved field id",
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
			name: "partition values supplied for an unpartitioned spec",
			mutate: func(a *table.DataFileArgs) {
				a.PartitionValues = map[int]any{1000: int32(7)}
			},
			wantSub: "partition values supplied for an unpartitioned spec",
		},
		{
			name: "FirstRowID set on an equality-delete file",
			mutate: func(a *table.DataFileArgs) {
				a.Content = iceberg.EntryContentEqDeletes
				a.EqualityFieldIDs = []int{1}
				v := int64(7)
				a.FirstRowID = &v
			},
			wantSub: "first_row_id must be nil for delete files",
		},
		{
			name: "ReferencedDataFile set on a data file",
			mutate: func(a *table.DataFileArgs) {
				ref := "s3://bucket/data/data-001.parquet"
				a.ReferencedDataFile = &ref
			},
			wantSub: "referenced_data_file may only be set for position-delete files",
		},
		{
			name: "ReferencedDataFile empty on a positional-delete file",
			mutate: func(a *table.DataFileArgs) {
				a.Schema = iceberg.PositionalDeleteSchema
				a.Content = iceberg.EntryContentPosDeletes
				_, posMeta := parquetMetaFromSchema(t, iceberg.PositionalDeleteSchema,
					`[{"file_path": "s3://b/d.parquet", "pos": 0}]`)
				a.Metadata = posMeta
				empty := ""
				a.ReferencedDataFile = &empty
			},
			wantSub: "referenced_data_file must be non-empty when set",
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
			wantSub: "metadata pointer is nil",
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
			require.Nil(t, df, "expected no DataFile on error, got %+v", df)
			require.ErrorContains(t, err, tc.wantSub)
		})
	}
}

func parquetMetaMultiRowGroup(t *testing.T, sch *iceberg.Schema, jsonRows string, maxRowGroupLen int64) ([]byte, *metadata.FileMetaData) {
	t.Helper()

	arrowSch, err := table.SchemaToArrowSchema(sch, nil, true, false)
	require.NoError(t, err)

	rec, _, err := array.RecordFromJSON(memory.DefaultAllocator, arrowSch, strings.NewReader(jsonRows))
	require.NoError(t, err)
	defer rec.Release()

	var buf bytes.Buffer
	wr, err := pqarrow.NewFileWriter(arrowSch, &buf,
		parquet.NewWriterProperties(parquet.WithStats(true), parquet.WithMaxRowGroupLength(maxRowGroupLen)),
		pqarrow.DefaultWriterProps())
	require.NoError(t, err)
	require.NoError(t, wr.Write(rec))
	require.NoError(t, wr.Close())

	rdr, err := file.NewParquetReader(bytes.NewReader(buf.Bytes()))
	require.NoError(t, err)
	defer rdr.Close()

	return buf.Bytes(), rdr.MetaData()
}

func TestDataFileFromMetadata_FirstRowIDNil(t *testing.T) {
	pqBytes, pqMeta := parquetMetaFromSchema(t, dataSchema, dataRows)

	df, err := table.DataFileFromMetadata(table.DataFileArgs{
		Schema:   dataSchema,
		Spec:     iceberg.NewPartitionSpec(),
		Format:   iceberg.ParquetFile,
		Metadata: pqMeta,
		FilePath: "s3://bucket/data/no-first-row-id.parquet",
		FileSize: int64(len(pqBytes)),
		Content:  iceberg.EntryContentData,
	})
	require.NoError(t, err)
	assert.Nil(t, df.FirstRowID(),
		"FirstRowID must be nil when the caller does not supply one (v1/v2 default)")
}

func TestDataFileFromMetadata_MetricsModes(t *testing.T) {
	pqBytes, pqMeta := parquetMetaFromSchema(t, dataSchema, dataRows)

	base := func() table.DataFileArgs {
		return table.DataFileArgs{
			Schema:   dataSchema,
			Spec:     iceberg.NewPartitionSpec(),
			Format:   iceberg.ParquetFile,
			Metadata: pqMeta,
			FilePath: "s3://bucket/data/metrics.parquet",
			FileSize: int64(len(pqBytes)),
			Content:  iceberg.EntryContentData,
		}
	}

	t.Run("none suppresses all column stats", func(t *testing.T) {
		a := base()
		a.Properties = iceberg.Properties{table.DefaultWriteMetricsModeKey: "none"}
		df, err := table.DataFileFromMetadata(a)
		require.NoError(t, err)
		assert.Empty(t, df.LowerBoundValues())
		assert.Empty(t, df.UpperBoundValues())
		assert.Empty(t, df.ValueCounts())
		assert.Empty(t, df.NullValueCounts())
		assert.EqualValues(t, 3, df.Count(), "record count is independent of metrics mode")
	})

	t.Run("counts keeps counts but drops bounds", func(t *testing.T) {
		a := base()
		a.Properties = iceberg.Properties{table.DefaultWriteMetricsModeKey: "counts"}
		df, err := table.DataFileFromMetadata(a)
		require.NoError(t, err)
		assert.Empty(t, df.LowerBoundValues())
		assert.Empty(t, df.UpperBoundValues())
		assert.NotEmpty(t, df.ValueCounts())
	})

	t.Run("truncate(2) truncates string bounds, keeps numeric bounds", func(t *testing.T) {
		a := base()
		a.Properties = iceberg.Properties{table.DefaultWriteMetricsModeKey: "truncate(2)"}
		df, err := table.DataFileFromMetadata(a)
		require.NoError(t, err)
		assert.EqualValues(t, 1, decodeInt32Bound(t, df.LowerBoundValues()[1]))
		assert.EqualValues(t, 3, decodeInt32Bound(t, df.UpperBoundValues()[1]))
		assert.Equal(t, "al", decodeStringBound(t, df.LowerBoundValues()[2]))
		assert.LessOrEqual(t, len(df.LowerBoundValues()[2]), 2, "string lower bound must be truncated to <= 2 bytes")
	})
}

func TestDataFileFromMetadata_NullCountsAndColumnSizes(t *testing.T) {
	rows := `[
		{"id": 1, "name": "a"},
		{"id": 2, "name": null},
		{"id": 3, "name": null}
	]`
	pqBytes, pqMeta := parquetMetaFromSchema(t, dataSchema, rows)

	df, err := table.DataFileFromMetadata(table.DataFileArgs{
		Schema:   dataSchema,
		Spec:     iceberg.NewPartitionSpec(),
		Format:   iceberg.ParquetFile,
		Metadata: pqMeta,
		FilePath: "s3://bucket/data/nulls.parquet",
		FileSize: int64(len(pqBytes)),
		Content:  iceberg.EntryContentData,
	})
	require.NoError(t, err)

	assert.EqualValues(t, 2, df.NullValueCounts()[2], "name column has 2 nulls")
	assert.EqualValues(t, 0, df.NullValueCounts()[1], "id column has no nulls")

	require.NotEmpty(t, df.ColumnSizes(), "column sizes must be populated")
	assert.Contains(t, df.ColumnSizes(), 1)
	assert.Contains(t, df.ColumnSizes(), 2)
}

func TestDataFileFromMetadata_MultiRowGroup(t *testing.T) {
	rows := `[
		{"id": 1, "name": "a"},
		{"id": 2, "name": "b"},
		{"id": 3, "name": "c"},
		{"id": 4, "name": "d"}
	]`

	pqBytes, pqMeta := parquetMetaMultiRowGroup(t, dataSchema, rows, 1)
	require.Greater(t, pqMeta.NumRowGroups(), 1, "fixture must produce multiple row groups")

	df, err := table.DataFileFromMetadata(table.DataFileArgs{
		Schema:   dataSchema,
		Spec:     iceberg.NewPartitionSpec(),
		Format:   iceberg.ParquetFile,
		Metadata: pqMeta,
		FilePath: "s3://bucket/data/multi-rg.parquet",
		FileSize: int64(len(pqBytes)),
		Content:  iceberg.EntryContentData,
	})
	require.NoError(t, err)

	assert.EqualValues(t, 4, df.Count(), "record count must aggregate across row groups")
	assert.EqualValues(t, 1, decodeInt32Bound(t, df.LowerBoundValues()[1]),
		"lower bound must aggregate across row groups")
	assert.EqualValues(t, 4, decodeInt32Bound(t, df.UpperBoundValues()[1]),
		"upper bound must aggregate across row groups")
	assert.Len(t, df.SplitOffsets(), pqMeta.NumRowGroups(),
		"one split offset per row group")
}

func TestDataFileFromMetadata_SchemaMismatchYieldsError(t *testing.T) {
	pqBytes, pqMeta := parquetMetaFromSchema(t, dataSchema, dataRows)

	mismatched := iceberg.NewSchema(0,
		iceberg.NestedField{ID: 1, Name: "id", Type: iceberg.PrimitiveTypes.Int32, Required: false},
	)

	df, err := table.DataFileFromMetadata(table.DataFileArgs{
		Schema:   mismatched,
		Spec:     iceberg.NewPartitionSpec(),
		Format:   iceberg.ParquetFile,
		Metadata: pqMeta,
		FilePath: "s3://bucket/data/mismatch.parquet",
		FileSize: int64(len(pqBytes)),
		Content:  iceberg.EntryContentData,
	})
	require.Error(t, err)
	require.Nil(t, df)
	require.ErrorContains(t, err, "error encountered during extracting stats and building the data file")
	require.ErrorContains(t, err, "not found in column mapping")
}

func TestDataFileFromMetadata_RenamedColumnErrors(t *testing.T) {
	pqBytes, pqMeta := parquetMetaFromSchema(t, dataSchema, dataRows)

	renamed := iceberg.NewSchema(0,
		iceberg.NestedField{ID: 1, Name: "id", Type: iceberg.PrimitiveTypes.Int32, Required: false},
		iceberg.NestedField{ID: 2, Name: "label", Type: iceberg.PrimitiveTypes.String, Required: false},
	)

	df, err := table.DataFileFromMetadata(table.DataFileArgs{
		Schema:   renamed,
		Spec:     iceberg.NewPartitionSpec(),
		Format:   iceberg.ParquetFile,
		Metadata: pqMeta,
		FilePath: "s3://bucket/data/renamed.parquet",
		FileSize: int64(len(pqBytes)),
		Content:  iceberg.EntryContentData,
	})
	require.Nil(t, df, "a renamed column must not silently yield a DataFile with missing bounds")
	require.ErrorContains(t, err, "not found in column mapping")
}

func TestDataFileFromMetadata_EndToEndAddDataFiles(t *testing.T) {
	tbl := newRowDeltaCommitTestTable(t)
	sch := tbl.Schema()

	pqBytes, pqMeta := parquetMetaFromSchema(t, sch, `[
		{"id": 1, "data": "a"},
		{"id": 2, "data": null},
		{"id": 3, "data": "c"}
	]`)

	df, err := table.DataFileFromMetadata(table.DataFileArgs{
		Schema:   sch,
		Spec:     iceberg.NewPartitionSpec(),
		Format:   iceberg.ParquetFile,
		Metadata: pqMeta,
		FilePath: "s3://bucket/test/data/add-data-files.parquet",
		FileSize: int64(len(pqBytes)),
		Content:  iceberg.EntryContentData,
	})
	require.NoError(t, err)

	tx := tbl.NewTransaction()
	require.NoError(t, tx.AddDataFiles(t.Context(), []iceberg.DataFile{df}, nil))

	result, err := tx.Commit(t.Context())
	require.NoError(t, err)

	snap := result.CurrentSnapshot()
	require.NotNil(t, snap)
	assert.Equal(t, table.OpAppend, snap.Summary.Operation)
	assert.Equal(t, "1", snap.Summary.Properties["added-data-files"])
	assert.Equal(t, "3", snap.Summary.Properties["added-records"])

	manifests, err := snap.Manifests(iceio.LocalFS{})
	require.NoError(t, err)

	var checked bool
	for _, m := range manifests {
		if m.ManifestContent() != iceberg.ManifestContentData {
			continue
		}
		for e, err := range m.Entries(iceio.LocalFS{}, true) {
			require.NoError(t, err)
			committed := e.DataFile()

			lower, err := iceberg.LiteralFromBytes(iceberg.PrimitiveTypes.Int64, committed.LowerBoundValues()[1])
			require.NoError(t, err)
			assert.EqualValues(t, 1, lower.(iceberg.Int64Literal).Value(),
				"id lower bound must survive the manifest round-trip")
			upper, err := iceberg.LiteralFromBytes(iceberg.PrimitiveTypes.Int64, committed.UpperBoundValues()[1])
			require.NoError(t, err)
			assert.EqualValues(t, 3, upper.(iceberg.Int64Literal).Value(),
				"id upper bound must survive the manifest round-trip")

			assert.EqualValues(t, 1, committed.NullValueCounts()[2],
				"data column null count must survive the manifest round-trip")

			assert.Contains(t, committed.ColumnSizes(), 1,
				"column sizes must survive the manifest round-trip")
			checked = true
		}
	}
	assert.True(t, checked, "expected a committed data-file manifest entry to inspect")
}

func TestDataFileFromMetadata_V3AddDataFilesRequiresFirstRowID(t *testing.T) {
	tbl := newRowDeltaCommitTestTableVersion(t, 3)
	sch := tbl.Schema()

	rows := `[{"id": 1, "data": "a"}, {"id": 2, "data": "b"}]`

	t.Run("missing first_row_id is rejected at commit time", func(t *testing.T) {
		pqBytes, pqMeta := parquetMetaFromSchema(t, sch, rows)
		df, err := table.DataFileFromMetadata(table.DataFileArgs{
			Schema:   sch,
			Spec:     iceberg.NewPartitionSpec(),
			Format:   iceberg.ParquetFile,
			Metadata: pqMeta,
			FilePath: "s3://bucket/test/data/v3-no-rowid.parquet",
			FileSize: int64(len(pqBytes)),
			Content:  iceberg.EntryContentData,
			// FirstRowID intentionally unset.
		})
		require.NoError(t, err)
		require.Nil(t, df.FirstRowID())

		tx := tbl.NewTransaction()
		err = tx.AddDataFiles(t.Context(), []iceberg.DataFile{df}, nil)
		require.ErrorContains(t, err, "first_row_id")
		require.ErrorContains(t, err, "required for v3")
	})

	t.Run("setting first_row_id lets v3 AddDataFiles commit", func(t *testing.T) {
		pqBytes, pqMeta := parquetMetaFromSchema(t, sch, rows)
		firstRowID := int64(0)
		df, err := table.DataFileFromMetadata(table.DataFileArgs{
			Schema:        sch,
			Spec:          iceberg.NewPartitionSpec(),
			Format:        iceberg.ParquetFile,
			Metadata:      pqMeta,
			FilePath:      "s3://bucket/test/data/v3-with-rowid.parquet",
			FileSize:      int64(len(pqBytes)),
			Content:       iceberg.EntryContentData,
			FormatVersion: 3,
			FirstRowID:    &firstRowID,
		})
		require.NoError(t, err)
		require.NotNil(t, df.FirstRowID())

		tx := tbl.NewTransaction()
		require.NoError(t, tx.AddDataFiles(t.Context(), []iceberg.DataFile{df}, nil))

		result, err := tx.Commit(t.Context())
		require.NoError(t, err)

		snap := result.CurrentSnapshot()
		require.NotNil(t, snap)
		assert.Equal(t, "1", snap.Summary.Properties["added-data-files"])
	})
}

func TestDataFileFromMetadata_V3ReplaceDataFilesRequiresFirstRowID(t *testing.T) {
	tbl := newRowDeltaCommitTestTableVersion(t, 3)
	sch := tbl.Schema()
	rows := `[{"id": 1, "data": "a"}]`

	firstID := int64(0)
	initBytes, initMeta := parquetMetaFromSchema(t, sch, rows)
	initial, err := table.DataFileFromMetadata(table.DataFileArgs{
		Schema:        sch,
		Spec:          iceberg.NewPartitionSpec(),
		Format:        iceberg.ParquetFile,
		Metadata:      initMeta,
		FilePath:      "s3://bucket/test/data/v3-replace-initial.parquet",
		FileSize:      int64(len(initBytes)),
		Content:       iceberg.EntryContentData,
		FormatVersion: 3,
		FirstRowID:    &firstID,
	})
	require.NoError(t, err)

	tx := tbl.NewTransaction()
	require.NoError(t, tx.AddDataFiles(t.Context(), []iceberg.DataFile{initial}, nil))
	tbl, err = tx.Commit(t.Context())
	require.NoError(t, err)

	addBytes, addMeta := parquetMetaFromSchema(t, sch, rows)
	replacement, err := table.DataFileFromMetadata(table.DataFileArgs{
		Schema:   sch,
		Spec:     iceberg.NewPartitionSpec(),
		Format:   iceberg.ParquetFile,
		Metadata: addMeta,
		FilePath: "s3://bucket/test/data/v3-replace-add.parquet",
		FileSize: int64(len(addBytes)),
		Content:  iceberg.EntryContentData,
	})
	require.NoError(t, err)
	require.Nil(t, replacement.FirstRowID())

	tx = tbl.NewTransaction()
	err = tx.ReplaceDataFilesWithDataFiles(t.Context(),
		[]iceberg.DataFile{initial}, []iceberg.DataFile{replacement}, nil)
	require.ErrorContains(t, err, "first_row_id")
	require.ErrorContains(t, err, "required for v3")
}

func TestDataFileFromMetadata_V3RewriteAcceptsNilFirstRowID(t *testing.T) {
	tbl := newRowDeltaCommitTestTableVersion(t, 3)
	sch := tbl.Schema()
	rows := `[{"id": 1, "data": "a"}]`

	firstID := int64(0)
	initBytes, initMeta := parquetMetaFromSchema(t, sch, rows)
	initial, err := table.DataFileFromMetadata(table.DataFileArgs{
		Schema:        sch,
		Spec:          iceberg.NewPartitionSpec(),
		Format:        iceberg.ParquetFile,
		Metadata:      initMeta,
		FilePath:      "s3://bucket/test/data/v3-rewrite-initial.parquet",
		FileSize:      int64(len(initBytes)),
		Content:       iceberg.EntryContentData,
		FormatVersion: 3,
		FirstRowID:    &firstID,
	})
	require.NoError(t, err)

	tx := tbl.NewTransaction()
	require.NoError(t, tx.AddDataFiles(t.Context(), []iceberg.DataFile{initial}, nil))
	tbl, err = tx.Commit(t.Context())
	require.NoError(t, err)

	addBytes, addMeta := parquetMetaFromSchema(t, sch, rows)
	replacement, err := table.DataFileFromMetadata(table.DataFileArgs{
		Schema:   sch,
		Spec:     iceberg.NewPartitionSpec(),
		Format:   iceberg.ParquetFile,
		Metadata: addMeta,
		FilePath: "s3://bucket/test/data/v3-rewrite-add.parquet",
		FileSize: int64(len(addBytes)),
		Content:  iceberg.EntryContentData,
	})
	require.NoError(t, err)
	require.Nil(t, replacement.FirstRowID(),
		"rewrite semantics must accept a nil first_row_id; inheritance assigns it at commit")

	tx = tbl.NewTransaction()
	rw := tx.NewRewrite(nil)
	rw.DeleteFile(initial)
	rw.AddDataFile(replacement)
	require.NoError(t, rw.Commit(t.Context()))

	result, err := tx.Commit(t.Context())
	require.NoError(t, err)
	require.NotNil(t, result.CurrentSnapshot())
}

func TestDataFileFromMetadata_EndToEndRowDelta(t *testing.T) {
	tbl := newRowDeltaCommitTestTableVersion(t, 3)
	sch := tbl.Schema()

	const dataPath = "s3://bucket/test/data/insert.parquet"

	dataBytes, dataMeta := parquetMetaFromSchema(t, sch, `[
		{"id": 1, "data": "a"},
		{"id": 2, "data": "b"}
	]`)
	dataFile, err := table.DataFileFromMetadata(table.DataFileArgs{
		Schema:   sch,
		Spec:     iceberg.NewPartitionSpec(),
		Format:   iceberg.ParquetFile,
		Metadata: dataMeta,
		FilePath: dataPath,
		FileSize: int64(len(dataBytes)),
		Content:  iceberg.EntryContentData,
	})
	require.NoError(t, err)

	posBytes, posMeta := parquetMetaFromSchema(t, iceberg.PositionalDeleteSchema,
		`[{"file_path": "`+dataPath+`", "pos": 0}]`)
	posDelete, err := table.DataFileFromMetadata(table.DataFileArgs{
		Schema:   iceberg.PositionalDeleteSchema,
		Spec:     iceberg.NewPartitionSpec(),
		Format:   iceberg.ParquetFile,
		Metadata: posMeta,
		FilePath: "s3://bucket/test/data/pos-del.parquet",
		FileSize: int64(len(posBytes)),
		Content:  iceberg.EntryContentPosDeletes,
	})
	require.NoError(t, err)

	tx := tbl.NewTransaction()
	rd := tx.NewRowDelta(nil)
	rd.AddRows(dataFile)
	rd.AddDeletes(posDelete)
	require.NoError(t, rd.Commit(t.Context()))

	result, err := tx.Commit(t.Context())
	require.NoError(t, err)

	snap := result.CurrentSnapshot()
	require.NotNil(t, snap)
	assert.Equal(t, table.OpOverwrite, snap.Summary.Operation)
	assert.Equal(t, "1", snap.Summary.Properties["added-data-files"])
	assert.Equal(t, "1", snap.Summary.Properties["added-delete-files"])
	assert.Equal(t, "2", snap.Summary.Properties["added-records"])

	manifests, err := snap.Manifests(iceio.LocalFS{})
	require.NoError(t, err)

	var sawPosDelete bool
	for _, m := range manifests {
		if m.ManifestContent() != iceberg.ManifestContentDeletes {
			continue
		}
		for e, err := range m.Entries(iceio.LocalFS{}, true) {
			require.NoError(t, err)
			require.Equal(t, iceberg.EntryContentPosDeletes, e.DataFile().ContentType())
			sawPosDelete = true
		}
	}
	assert.True(t, sawPosDelete, "expected a committed positional-delete entry")
}
