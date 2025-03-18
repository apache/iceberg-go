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
	"context"
	"fmt"
	"strconv"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/arrow-go/v18/parquet/pqarrow"
	"github.com/apache/iceberg-go"
	"github.com/apache/iceberg-go/internal"
	iceio "github.com/apache/iceberg-go/io"
	"github.com/apache/iceberg-go/table"
	"github.com/google/uuid"
	"github.com/pterm/pterm"
	"github.com/stretchr/testify/suite"
)

type TableTestSuite struct {
	suite.Suite

	tbl *table.Table
}

func TestTable(t *testing.T) {
	suite.Run(t, new(TableTestSuite))
}

func (t *TableTestSuite) SetupSuite() {
	var mockfs internal.MockFS
	mockfs.Test(t.T())
	mockfs.On("Open", "s3://bucket/test/location/uuid.metadata.json").
		Return(&internal.MockFile{Contents: bytes.NewReader([]byte(table.ExampleTableMetadataV2))}, nil)
	defer mockfs.AssertExpectations(t.T())

	tbl, err := table.NewFromLocation([]string{"foo"}, "s3://bucket/test/location/uuid.metadata.json", &mockfs, nil)
	t.Require().NoError(err)
	t.Require().NotNil(tbl)

	t.Equal([]string{"foo"}, tbl.Identifier())
	t.Equal("s3://bucket/test/location/uuid.metadata.json", tbl.MetadataLocation())
	t.Equal(&mockfs, tbl.FS())

	t.tbl = tbl
}

func (t *TableTestSuite) TestNewTableFromReadFile() {
	var mockfsReadFile internal.MockFSReadFile
	mockfsReadFile.Test(t.T())
	mockfsReadFile.On("ReadFile", "s3://bucket/test/location/uuid.metadata.json").
		Return([]byte(table.ExampleTableMetadataV2), nil)
	defer mockfsReadFile.AssertExpectations(t.T())

	tbl2, err := table.NewFromLocation([]string{"foo"}, "s3://bucket/test/location/uuid.metadata.json", &mockfsReadFile, nil)
	t.Require().NoError(err)
	t.Require().NotNil(tbl2)

	t.True(t.tbl.Equals(*tbl2))
}

func (t *TableTestSuite) TestSchema() {
	t.True(t.tbl.Schema().Equals(iceberg.NewSchemaWithIdentifiers(1, []int{1, 2},
		iceberg.NestedField{ID: 1, Name: "x", Type: iceberg.PrimitiveTypes.Int64, Required: true},
		iceberg.NestedField{ID: 2, Name: "y", Type: iceberg.PrimitiveTypes.Int64, Required: true, Doc: "comment"},
		iceberg.NestedField{ID: 3, Name: "z", Type: iceberg.PrimitiveTypes.Int64, Required: true},
	)))
}

func (t *TableTestSuite) TestPartitionSpec() {
	t.Equal(iceberg.NewPartitionSpec(
		iceberg.PartitionField{SourceID: 1, FieldID: 1000, Transform: iceberg.IdentityTransform{}, Name: "x"},
	), t.tbl.Spec())
}

func (t *TableTestSuite) TestSortOrder() {
	t.Equal(table.SortOrder{
		OrderID: 3,
		Fields: []table.SortField{
			{SourceID: 2, Transform: iceberg.IdentityTransform{}, Direction: table.SortASC, NullOrder: table.NullsFirst},
			{SourceID: 3, Transform: iceberg.BucketTransform{NumBuckets: 4}, Direction: table.SortDESC, NullOrder: table.NullsLast},
		},
	}, t.tbl.SortOrder())
}

func (t *TableTestSuite) TestLocation() {
	t.Equal("s3://bucket/test/location", t.tbl.Location())
}

func (t *TableTestSuite) TestSnapshot() {
	var (
		parentSnapshotID int64 = 3051729675574597004
		one                    = 1
		manifestList           = "s3://a/b/2.avro"
	)

	testSnapshot := table.Snapshot{
		SnapshotID:       3055729675574597004,
		ParentSnapshotID: &parentSnapshotID,
		SequenceNumber:   1,
		TimestampMs:      1555100955770,
		ManifestList:     manifestList,
		Summary:          &table.Summary{Operation: table.OpAppend, Properties: map[string]string{}},
		SchemaID:         &one,
	}
	t.True(testSnapshot.Equals(*t.tbl.CurrentSnapshot()))

	t.True(testSnapshot.Equals(*t.tbl.SnapshotByID(3055729675574597004)))
}

func (t *TableTestSuite) TestSnapshotByName() {
	testSnapshot := table.Snapshot{
		SnapshotID:   3051729675574597004,
		TimestampMs:  1515100955770,
		ManifestList: "s3://a/b/1.avro",
		Summary:      &table.Summary{Operation: table.OpAppend},
	}

	t.True(testSnapshot.Equals(*t.tbl.SnapshotByName("test")))
}

type TableWritingTestSuite struct {
	suite.Suite

	tableSchema      *iceberg.Schema
	arrSchema        *arrow.Schema
	arrTbl           arrow.Table
	arrSchemaWithIDs *arrow.Schema
	arrTblWithIDs    arrow.Table
	arrSchemaUpdated *arrow.Schema
	arrTblUpdated    arrow.Table

	tableSchemaPromotedTypes *iceberg.Schema
	arrSchemaPromotedTypes   *arrow.Schema
	arrTablePromotedTypes    arrow.Table

	location      string
	formatVersion int
}

func (t *TableWritingTestSuite) SetupSuite() {
	mem := memory.DefaultAllocator

	t.tableSchema = iceberg.NewSchema(0,
		iceberg.NestedField{ID: 1, Name: "foo", Type: iceberg.PrimitiveTypes.Bool},
		iceberg.NestedField{ID: 2, Name: "bar", Type: iceberg.PrimitiveTypes.String},
		iceberg.NestedField{ID: 4, Name: "baz", Type: iceberg.PrimitiveTypes.Int32},
		iceberg.NestedField{ID: 10, Name: "qux", Type: iceberg.PrimitiveTypes.Date})

	t.arrSchema = arrow.NewSchema([]arrow.Field{
		{Name: "foo", Type: arrow.FixedWidthTypes.Boolean, Nullable: true},
		{Name: "bar", Type: arrow.BinaryTypes.String, Nullable: true},
		{Name: "baz", Type: arrow.PrimitiveTypes.Int32, Nullable: true},
		{Name: "qux", Type: arrow.PrimitiveTypes.Date32, Nullable: true},
	}, nil)

	var err error
	t.arrTbl, err = array.TableFromJSON(mem, t.arrSchema, []string{
		`[{"foo": true, "bar": "bar_string", "baz": 123, "qux": "2024-03-07"}]`,
	})
	t.Require().NoError(err)

	t.arrSchemaWithIDs = arrow.NewSchema([]arrow.Field{
		{
			Name: "foo", Type: arrow.FixedWidthTypes.Boolean,
			Metadata: arrow.MetadataFrom(map[string]string{"PARQUET:field_id": "1"}),
		},
		{
			Name: "bar", Type: arrow.BinaryTypes.String,
			Metadata: arrow.MetadataFrom(map[string]string{"PARQUET:field_id": "2"}),
		},
		{
			Name: "baz", Type: arrow.PrimitiveTypes.Int32,
			Metadata: arrow.MetadataFrom(map[string]string{"PARQUET:field_id": "3"}),
		},
		{
			Name: "qux", Type: arrow.PrimitiveTypes.Date32,
			Metadata: arrow.MetadataFrom(map[string]string{"PARQUET:field_id": "4"}),
		},
	}, nil)

	t.arrTblWithIDs, err = array.TableFromJSON(mem, t.arrSchemaWithIDs, []string{
		`[{"foo": true, "bar": "bar_string", "baz": 123, "qux": "2024-03-07"}]`,
	})
	t.Require().NoError(err)

	t.arrSchemaUpdated = arrow.NewSchema([]arrow.Field{
		{Name: "foo", Type: arrow.FixedWidthTypes.Boolean, Nullable: true},
		{Name: "baz", Type: arrow.PrimitiveTypes.Int32, Nullable: true},
		{Name: "qux", Type: arrow.PrimitiveTypes.Date32, Nullable: true},
		{Name: "quux", Type: arrow.PrimitiveTypes.Int32, Nullable: true},
	}, nil)

	t.arrTblUpdated, err = array.TableFromJSON(mem, t.arrSchemaUpdated, []string{
		`[{"foo": true, "baz": 123, "qux": "2024-03-07", "quux": 234}]`,
	})
	t.Require().NoError(err)

	t.tableSchemaPromotedTypes = iceberg.NewSchema(0,
		iceberg.NestedField{ID: 1, Name: "long", Type: iceberg.PrimitiveTypes.Int64},
		iceberg.NestedField{
			ID: 2, Name: "list",
			Type:     &iceberg.ListType{ElementID: 4, Element: iceberg.PrimitiveTypes.Int64},
			Required: true,
		},
		iceberg.NestedField{
			ID: 3, Name: "map",
			Type: &iceberg.MapType{
				KeyID:     5,
				KeyType:   iceberg.PrimitiveTypes.String,
				ValueID:   6,
				ValueType: iceberg.PrimitiveTypes.Int64,
			},
			Required: true,
		},
		iceberg.NestedField{ID: 7, Name: "double", Type: iceberg.PrimitiveTypes.Float64})
	// arrow-go needs to implement cast_extension for [16]byte -> uuid
	// iceberg.NestedField{ID: 8, Name: "uuid", Type: iceberg.PrimitiveTypes.UUID})

	t.arrSchemaPromotedTypes = arrow.NewSchema([]arrow.Field{
		{Name: "long", Type: arrow.PrimitiveTypes.Int32, Nullable: true},
		{Name: "list", Type: arrow.ListOf(arrow.PrimitiveTypes.Int32), Nullable: false},
		{Name: "map", Type: arrow.MapOf(arrow.BinaryTypes.String, arrow.PrimitiveTypes.Int32), Nullable: false},
		{Name: "double", Type: arrow.PrimitiveTypes.Float32, Nullable: true},
	}, nil)
	// arrow-go needs to implement cast_extension for [16]byte -> uuid
	// {Name: "uuid", Type: &arrow.FixedSizeBinaryType{ByteWidth: 16}, Nullable: true}}, nil)

	t.arrTablePromotedTypes, err = array.TableFromJSON(mem, t.arrSchemaPromotedTypes, []string{
		`[
			{"long": 1, "list": [1, 1], "map": [{"key": "a", "value": 1}], "double": 1.1, "uuid": "cVp4705TQImb+TrQ7pv1RQ=="},
			{"long": 9, "list": [2, 2], "map": [{"key": "b", "value": 2}], "double": 9.2, "uuid": "l12HVF5KREqWl/R25AMM3g=="}
		]`,
	})
	t.Require().NoError(err)
}

func (t *TableWritingTestSuite) SetupTest() {
	t.location = t.T().TempDir()
}

func (t *TableWritingTestSuite) TearDownSuite() {
	t.arrTbl.Release()
	t.arrTblUpdated.Release()
	t.arrTblWithIDs.Release()
}

func (t *TableWritingTestSuite) getMetadataLoc() string {
	return fmt.Sprintf("%s/metadata/%05d-%s.metadata.json",
		t.location, 1, uuid.New().String())
}

func (t *TableWritingTestSuite) writeParquet(fio iceio.WriteFileIO, filePath string, arrTbl arrow.Table) {
	fo, err := fio.Create(filePath)
	t.Require().NoError(err)

	t.Require().NoError(pqarrow.WriteTable(arrTbl, fo, arrTbl.NumRows(),
		nil, pqarrow.DefaultWriterProps()))
}

func (t *TableWritingTestSuite) createTable(identifier table.Identifier, formatVersion int, spec iceberg.PartitionSpec, sc *iceberg.Schema) *table.Table {
	meta, err := table.NewMetadata(sc, &spec, table.UnsortedSortOrder,
		t.getMetadataLoc(), iceberg.Properties{"format-version": strconv.Itoa(formatVersion)})
	t.Require().NoError(err)

	return table.New(identifier, meta, t.location, iceio.LocalFS{}, nil)
}

func (t *TableWritingTestSuite) TestAddFilesUnpartitioned() {
	ident := table.Identifier{"default", "unpartitioned_table_v" + strconv.Itoa(t.formatVersion)}
	tbl := t.createTable(ident, t.formatVersion,
		*iceberg.UnpartitionedSpec, t.tableSchema)

	t.NotNil(tbl)

	files := make([]string, 0)
	for i := range 5 {
		filePath := fmt.Sprintf("%s/unpartitioned/test-%d.parquet", t.location, i)
		t.writeParquet(tbl.FS().(iceio.WriteFileIO), filePath, t.arrTbl)
		files = append(files, filePath)
	}

	tx := tbl.NewTransaction()
	t.Require().NoError(tx.AddFiles(files, nil, false))

	stagedTbl, err := tx.StagedTable()
	t.Require().NoError(err)
	t.NotNil(stagedTbl.NameMapping())

	t.Equal(stagedTbl.CurrentSnapshot().Summary,
		&table.Summary{
			Operation: table.OpAppend,
			Properties: iceberg.Properties{
				"added-data-files":       "5",
				"added-files-size":       "3660",
				"added-records":          "5",
				"total-data-files":       "5",
				"total-delete-files":     "0",
				"total-equality-deletes": "0",
				"total-files-size":       "3660",
				"total-position-deletes": "0",
				"total-records":          "5",
			},
		})

	scan, err := tx.Scan()
	t.Require().NoError(err)

	contents, err := scan.ToArrowTable(context.Background())
	t.Require().NoError(err)
	defer contents.Release()

	t.EqualValues(5, contents.NumRows())
}

func (t *TableWritingTestSuite) TestAddFilesFileNotFound() {
	ident := table.Identifier{"default", "unpartitioned_table_file_not_found_v" + strconv.Itoa(t.formatVersion)}
	tbl := t.createTable(ident, t.formatVersion,
		*iceberg.UnpartitionedSpec, t.tableSchema)

	t.NotNil(tbl)

	files := make([]string, 0)
	for i := range 5 {
		filePath := fmt.Sprintf("%s/unpartitioned_file_not_found/test-%d.parquet", t.location, i)
		t.writeParquet(tbl.FS().(iceio.WriteFileIO), filePath, t.arrTbl)
		files = append(files, filePath)
	}

	files = append(files, t.location+"/unpartitioned_file_not_found/unknown.parquet")
	tx := tbl.NewTransaction()
	err := tx.AddFiles(files, nil, false)
	t.Error(err)
	t.ErrorContains(err, "no such file or directory")
}

func (t *TableWritingTestSuite) TestAddFilesUnpartitionedHasFieldIDs() {
	ident := table.Identifier{"default", "unpartitioned_table_with_ids_v" + strconv.Itoa(t.formatVersion)}
	tbl := t.createTable(ident, t.formatVersion,
		*iceberg.UnpartitionedSpec, t.tableSchema)

	t.NotNil(tbl)

	files := make([]string, 0)
	for i := range 5 {
		filePath := fmt.Sprintf("%s/unpartitioned_with_ids/test-%d.parquet", t.location, i)
		t.writeParquet(tbl.FS().(iceio.WriteFileIO), filePath, t.arrTblWithIDs)
		files = append(files, filePath)
	}

	tx := tbl.NewTransaction()
	err := tx.AddFiles(files, nil, false)
	t.Error(err)
	t.ErrorIs(err, iceberg.ErrNotImplemented)
}

func (t *TableWritingTestSuite) TestAddFilesFailsSchemaMismatch() {
	ident := table.Identifier{"default", "unpartitioned_table_schema_mismatch_v" + strconv.Itoa(t.formatVersion)}
	tbl := t.createTable(ident, t.formatVersion,
		*iceberg.UnpartitionedSpec, t.tableSchema)
	t.Require().NotNil(tbl)

	wrongSchema := arrow.NewSchema([]arrow.Field{
		{Name: "foo", Type: arrow.FixedWidthTypes.Boolean, Nullable: true},
		{Name: "bar", Type: arrow.BinaryTypes.String, Nullable: true},
		{Name: "baz", Type: arrow.BinaryTypes.String, Nullable: true}, // should be int32
		{Name: "qux", Type: arrow.PrimitiveTypes.Date32, Nullable: true},
	}, nil)

	filePath := fmt.Sprintf("%s/unpartitioned_schema_mismatch_v%d/test.parquet", t.location, t.formatVersion)
	mismatchTable, err := array.TableFromJSON(memory.DefaultAllocator, wrongSchema, []string{
		`[{"foo": true, "bar": "bar_string", "baz": "123", "qux": "2024-03-07"},
		  {"foo": false, "bar": "bar_string", "baz": "456", "qux": "2024-03-07"}]`,
	})
	t.Require().NoError(err)
	t.writeParquet(tbl.FS().(iceio.WriteFileIO), filePath, mismatchTable)

	files := []string{filePath}

	pterm.DisableOutput() // disable error to console
	defer pterm.EnableOutput()

	tx := tbl.NewTransaction()
	err = tx.AddFiles(files, nil, false)
	t.Error(err)
	t.EqualError(err, `error encountered during parquet file conversion: error encountered during schema visitor: mismatch in fields:
   | Table Field              | Requested Field         
✅ | 1: foo: optional boolean | 1: foo: optional boolean
✅ | 2: bar: optional string  | 2: bar: optional string 
❌ | 3: baz: optional int     | 3: baz: optional string 
✅ | 4: qux: optional date    | 4: qux: optional date   
`)
}

func (t *TableWritingTestSuite) TestAddFilesPartitionedTable() {
	ident := table.Identifier{"default", "partitioned_table_v" + strconv.Itoa(t.formatVersion)}
	spec := iceberg.NewPartitionSpec(
		iceberg.PartitionField{SourceID: 4, FieldID: 1000, Transform: iceberg.IdentityTransform{}, Name: "baz"},
		iceberg.PartitionField{SourceID: 10, FieldID: 1001, Transform: iceberg.MonthTransform{}, Name: "qux_month"})

	tbl := t.createTable(ident, t.formatVersion,
		spec, t.tableSchema)

	t.NotNil(tbl)

	dates := []string{
		"2024-03-07", "2024-03-08", "2024-03-16", "2024-03-18", "2024-03-19",
	}

	files := make([]string, 0)
	for i := range 5 {
		filePath := fmt.Sprintf("%s/partitioned_table/test-%d.parquet", t.location, i)
		table, err := array.TableFromJSON(memory.DefaultAllocator, t.arrSchema, []string{
			`[{"foo": true, "bar": "bar_string", "baz": 123, "qux": "` + dates[i] + `"}]`,
		})
		t.Require().NoError(err)
		defer table.Release()

		t.writeParquet(tbl.FS().(iceio.WriteFileIO), filePath, table)
		files = append(files, filePath)
	}

	tx := tbl.NewTransaction()
	t.Require().NoError(tx.AddFiles(files, nil, false))

	stagedTbl, err := tx.StagedTable()
	t.Require().NoError(err)
	t.NotNil(stagedTbl.NameMapping())

	t.Equal(stagedTbl.CurrentSnapshot().Summary,
		&table.Summary{
			Operation: table.OpAppend,
			Properties: iceberg.Properties{
				"added-data-files":        "5",
				"added-files-size":        "3660",
				"added-records":           "5",
				"changed-partition-count": "1",
				"total-data-files":        "5",
				"total-delete-files":      "0",
				"total-equality-deletes":  "0",
				"total-files-size":        "3660",
				"total-position-deletes":  "0",
				"total-records":           "5",
			},
		})

	m, err := stagedTbl.CurrentSnapshot().Manifests(tbl.FS())
	t.Require().NoError(err)

	for _, manifest := range m {
		entries, err := manifest.FetchEntries(tbl.FS(), false)
		t.Require().NoError(err)

		for _, e := range entries {
			t.Equal(map[string]any{
				"baz": 123, "qux_month": 650,
			}, e.DataFile().Partition())
		}
	}
}

func (t *TableWritingTestSuite) TestAddFilesToBucketPartitionedTableFails() {
	ident := table.Identifier{"default", "partitioned_table_bucket_fails_v" + strconv.Itoa(t.formatVersion)}
	spec := iceberg.NewPartitionSpec(
		iceberg.PartitionField{SourceID: 4, FieldID: 1000, Transform: iceberg.BucketTransform{NumBuckets: 3}, Name: "baz_bucket_3"})

	tbl := t.createTable(ident, t.formatVersion, spec, t.tableSchema)
	files := make([]string, 0)
	for i := range 5 {
		filePath := fmt.Sprintf("%s/partitioned_table/test-%d.parquet", t.location, i)
		table, err := array.TableFromJSON(memory.DefaultAllocator, t.arrSchema, []string{
			`[{"foo": true, "bar": "bar_string", "baz": ` + strconv.Itoa(i) + `, "qux": "2024-03-07"}]`,
		})
		t.Require().NoError(err)
		defer table.Release()

		t.writeParquet(tbl.FS().(iceio.WriteFileIO), filePath, table)
		files = append(files, filePath)
	}

	tx := tbl.NewTransaction()
	err := tx.AddFiles(files, nil, false)
	t.Error(err)
	t.ErrorContains(err, "cannot infer partition value from parquet metadata for a non-linear partition field: baz_bucket_3 with transform bucket[3]")
}

func (t *TableWritingTestSuite) TestAddFilesToPartitionedTableFailsLowerAndUpperMismatch() {
	ident := table.Identifier{"default", "partitioned_table_bucket_fails_v" + strconv.Itoa(t.formatVersion)}
	spec := iceberg.NewPartitionSpec(
		iceberg.PartitionField{SourceID: 4, FieldID: 1000, Transform: iceberg.IdentityTransform{}, Name: "baz"})

	tbl := t.createTable(ident, t.formatVersion, spec, t.tableSchema)
	files := make([]string, 0)
	for i := range 5 {
		filePath := fmt.Sprintf("%s/partitioned_table/test-%d.parquet", t.location, i)
		table, err := array.TableFromJSON(memory.DefaultAllocator, t.arrSchema, []string{
			`[
				{"foo": true, "bar": "bar_string", "baz": 123, "qux": "2024-03-07"},
				{"foo": true, "bar": "bar_string", "baz": 124, "qux": "2024-03-07"}
			]`,
		})
		t.Require().NoError(err)
		defer table.Release()

		t.writeParquet(tbl.FS().(iceio.WriteFileIO), filePath, table)
		files = append(files, filePath)
	}

	tx := tbl.NewTransaction()
	err := tx.AddFiles(files, nil, false)
	t.Error(err)
	t.ErrorContains(err, "cannot infer partition value from parquet metadata as there is more than one value for partition field: baz. (low: 123, high: 124)")
}

func (t *TableWritingTestSuite) TestAddFilesWithLargeAndRegular() {
	ident := table.Identifier{"default", "unpartitioned_with_large_types_v" + strconv.Itoa(t.formatVersion)}
	ice := iceberg.NewSchema(0,
		iceberg.NestedField{ID: 1, Name: "foo", Type: iceberg.PrimitiveTypes.String, Required: true})

	arrowSchema := arrow.NewSchema([]arrow.Field{
		{Name: "foo", Type: arrow.BinaryTypes.String},
	}, nil)
	arrowSchemaLarge := arrow.NewSchema([]arrow.Field{
		{Name: "foo", Type: arrow.BinaryTypes.LargeString},
	}, nil)

	tbl := t.createTable(ident, t.formatVersion, *iceberg.UnpartitionedSpec, ice)
	t.Require().NotNil(tbl)

	filePath := fmt.Sprintf("%s/unpartitioned_with_large_types/v%d/test-0.parquet", t.location, t.formatVersion)
	filePathLarge := fmt.Sprintf("%s/unpartitioned_with_large_types/v%d/test-1.parquet", t.location, t.formatVersion)

	arrTable, err := array.TableFromJSON(memory.DefaultAllocator, arrowSchema, []string{
		`[{"foo": "bar"}]`,
	})
	t.Require().NoError(err)
	defer arrTable.Release()

	arrTableLarge, err := array.TableFromJSON(memory.DefaultAllocator, arrowSchemaLarge,
		[]string{`[{"foo": "bar"}]`})
	t.Require().NoError(err)
	defer arrTableLarge.Release()

	t.writeParquet(tbl.FS().(iceio.WriteFileIO), filePath, arrTable)
	t.writeParquet(tbl.FS().(iceio.WriteFileIO), filePathLarge, arrTableLarge)

	tx := tbl.NewTransaction()
	t.Require().NoError(tx.AddFiles([]string{filePath, filePathLarge}, nil, false))

	scan, err := tx.Scan(table.WithOptions(iceberg.Properties{
		table.ScanOptionArrowUseLargeTypes: "true",
	}))
	t.Require().NoError(err)

	result, err := scan.ToArrowTable(context.Background())
	t.Require().NoError(err)
	defer result.Release()

	t.EqualValues(2, result.NumRows())
	t.Truef(arrowSchemaLarge.Equal(result.Schema()), "expected schema: %s, got: %s", arrowSchemaLarge, result.Schema())
}

func (t *TableWritingTestSuite) TestAddFilesValidUpcast() {
	ident := table.Identifier{"default", "test_table_with_valid_upcast_v" + strconv.Itoa(t.formatVersion)}
	tbl := t.createTable(ident, t.formatVersion, *iceberg.UnpartitionedSpec, t.tableSchemaPromotedTypes)

	filePath := fmt.Sprintf("%s/test_table_with_valid_upcast_v%d/test.parquet", t.location, t.formatVersion)
	t.writeParquet(tbl.FS().(iceio.WriteFileIO), filePath, t.arrTablePromotedTypes)

	tx := tbl.NewTransaction()
	t.Require().NoError(tx.AddFiles([]string{filePath}, nil, false))

	scan, err := tx.Scan()
	t.Require().NoError(err)

	written, err := scan.ToArrowTable(context.Background())
	t.Require().NoError(err)
	defer written.Release()

	t.EqualValues(2, written.NumRows())
	expectedSchema := arrow.NewSchema([]arrow.Field{
		{Name: "long", Type: arrow.PrimitiveTypes.Int64, Nullable: true},
		{Name: "list", Type: arrow.ListOf(arrow.PrimitiveTypes.Int64), Nullable: false},
		{Name: "map", Type: arrow.MapOf(arrow.BinaryTypes.String, arrow.PrimitiveTypes.Int64), Nullable: false},
		{Name: "double", Type: arrow.PrimitiveTypes.Float64, Nullable: true},
	}, nil)
	t.True(expectedSchema.Equal(written.Schema()))
}

func dropColFromTable(idx int, tbl arrow.Table) arrow.Table {
	if idx < 0 || idx >= int(tbl.NumCols()) {
		panic("invalid column to drop")
	}

	fields := tbl.Schema().Fields()
	fields = append(fields[:idx], fields[idx+1:]...)

	cols := make([]arrow.Column, 0, tbl.NumCols()-1)
	for i := 0; i < int(tbl.NumCols()); i++ {
		if i == idx {
			continue
		}
		cols = append(cols, *tbl.Column(i))
	}

	return array.NewTable(arrow.NewSchema(fields, nil), cols, tbl.NumRows())
}

func (t *TableWritingTestSuite) TestAddFilesSubsetOfSchema() {
	ident := table.Identifier{"default", "test_table_with_subset_of_schema_v" + strconv.Itoa(t.formatVersion)}
	tbl := t.createTable(ident, t.formatVersion, *iceberg.UnpartitionedSpec, t.tableSchema)

	filePath := fmt.Sprintf("%s/test_table_with_subset_of_schema_v%d/test.parquet", t.location, t.formatVersion)
	withoutCol := dropColFromTable(0, t.arrTbl)
	defer withoutCol.Release()
	t.writeParquet(tbl.FS().(iceio.WriteFileIO), filePath, withoutCol)

	tx := tbl.NewTransaction()
	t.Require().NoError(tx.AddFiles([]string{filePath}, nil, false))

	scan, err := tx.Scan()
	t.Require().NoError(err)

	written, err := scan.ToArrowTable(context.Background())
	t.Require().NoError(err)
	defer written.Release()

	t.EqualValues(1, written.NumRows())
	expectedSchema := arrow.NewSchema([]arrow.Field{
		{Name: "foo", Type: arrow.FixedWidthTypes.Boolean, Nullable: true},
		{Name: "bar", Type: arrow.BinaryTypes.String, Nullable: true},
		{Name: "baz", Type: arrow.PrimitiveTypes.Int32, Nullable: true},
		{Name: "qux", Type: arrow.PrimitiveTypes.Date32, Nullable: true},
	}, nil)
	t.True(expectedSchema.Equal(written.Schema()), expectedSchema.String(), written.Schema().String())

	result, err := array.TableFromJSON(memory.DefaultAllocator, t.arrSchema, []string{
		`[{"foo": null, "bar": "bar_string", "baz": 123, "qux": "2024-03-07"}]`,
	})
	t.Require().NoError(err)
	defer result.Release()

	t.True(array.TableEqual(result, written))
}

func (t *TableWritingTestSuite) TestAddFilesDuplicateFilesInFilePaths() {
	ident := table.Identifier{"default", "test_table_with_duplicate_files_v" + strconv.Itoa(t.formatVersion)}
	tbl := t.createTable(ident, t.formatVersion, *iceberg.UnpartitionedSpec, t.tableSchema)

	filePath := fmt.Sprintf("%s/test_table_with_duplicate_files_v%d/test.parquet", t.location, t.formatVersion)
	t.writeParquet(tbl.FS().(iceio.WriteFileIO), filePath, t.arrTbl)

	tx := tbl.NewTransaction()
	err := tx.AddFiles([]string{filePath, filePath}, nil, false)
	t.Error(err)
	t.ErrorContains(err, "file paths must be unique for AddFiles")
}

func (t *TableWritingTestSuite) TestAddFilesReferencedByCurrentSnapshot() {
	ident := table.Identifier{"default", "add_files_referenced_v" + strconv.Itoa(t.formatVersion)}
	tbl := t.createTable(ident, t.formatVersion,
		*iceberg.UnpartitionedSpec, t.tableSchema)

	t.NotNil(tbl)

	files := make([]string, 0)
	for i := range 5 {
		filePath := fmt.Sprintf("%s/add_files_referenced/test-%d.parquet", t.location, i)
		t.writeParquet(tbl.FS().(iceio.WriteFileIO), filePath, t.arrTbl)
		files = append(files, filePath)
	}

	tx := tbl.NewTransaction()
	t.Require().NoError(tx.AddFiles(files, nil, false))

	err := tx.AddFiles(files[len(files)-1:], nil, false)
	t.Error(err)
	t.ErrorContains(err, "cannot add files that are already referenced by table, files:")
}

func (t *TableWritingTestSuite) TestAddFilesReferencedCurrentSnapshotIgnoreDuplicates() {
	ident := table.Identifier{"default", "add_files_referenced_v" + strconv.Itoa(t.formatVersion)}
	tbl := t.createTable(ident, t.formatVersion,
		*iceberg.UnpartitionedSpec, t.tableSchema)

	t.NotNil(tbl)

	files := make([]string, 0)
	for i := range 5 {
		filePath := fmt.Sprintf("%s/add_files_referenced/test-%d.parquet", t.location, i)
		t.writeParquet(tbl.FS().(iceio.WriteFileIO), filePath, t.arrTbl)
		files = append(files, filePath)
	}

	tx := tbl.NewTransaction()
	t.Require().NoError(tx.AddFiles(files, nil, false))

	t.Require().NoError(tx.AddFiles(files[len(files)-1:], nil, true))
	staged, err := tx.StagedTable()
	t.Require().NoError(err)

	added, existing, deleted := []int32{}, []int32{}, []int32{}
	for m, err := range staged.AllManifests() {
		t.Require().NoError(err)
		added = append(added, m.AddedDataFiles())
		existing = append(existing, m.ExistingDataFiles())
		deleted = append(deleted, m.DeletedDataFiles())
	}

	t.Equal([]int32{5, 1, 5}, added)
	t.Equal([]int32{0, 0, 0}, existing)
	t.Equal([]int32{0, 0, 0}, deleted)
}

func TestTableWriting(t *testing.T) {
	suite.Run(t, &TableWritingTestSuite{formatVersion: 1})
	suite.Run(t, &TableWritingTestSuite{formatVersion: 2})
}
