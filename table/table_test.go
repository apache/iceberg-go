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

	contents, err := stagedTbl.Scan().ToArrowTable(context.Background())
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

func TestTableWriting(t *testing.T) {
	suite.Run(t, &TableWritingTestSuite{formatVersion: 1})
	suite.Run(t, &TableWritingTestSuite{formatVersion: 2})
}
