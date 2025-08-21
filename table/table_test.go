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
	"io/fs"
	"log"
	"os"
	"path/filepath"
	"runtime"
	"slices"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/arrow-go/v18/parquet/pqarrow"
	"github.com/apache/iceberg-go"
	"github.com/apache/iceberg-go/catalog"
	"github.com/apache/iceberg-go/catalog/sql"
	"github.com/apache/iceberg-go/internal"
	iceio "github.com/apache/iceberg-go/io"
	"github.com/apache/iceberg-go/table"
	"github.com/google/uuid"
	"github.com/pterm/pterm"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	"github.com/uptrace/bun/driver/sqliteshim"
)

type TableTestSuite struct {
	suite.Suite

	tbl *table.Table
}

func TestTable(t *testing.T) {
	suite.Run(t, new(TableTestSuite))
}

func mustFS(t *testing.T, tbl *table.Table) iceio.IO {
	r, err := tbl.FS(context.Background())
	require.NoError(t, err)

	return r
}

func (t *TableTestSuite) SetupSuite() {
	var mockfs internal.MockFS
	mockfs.Test(t.T())
	mockfs.On("Open", "s3://bucket/test/location/uuid.metadata.json").
		Return(&internal.MockFile{Contents: bytes.NewReader([]byte(table.ExampleTableMetadataV2))}, nil)
	defer mockfs.AssertExpectations(t.T())

	tbl, err := table.NewFromLocation(
		context.Background(),
		[]string{"foo"},
		"s3://bucket/test/location/uuid.metadata.json",
		func(ctx context.Context) (iceio.IO, error) {
			return &mockfs, nil
		},
		nil,
	)
	t.Require().NoError(err)
	t.Require().NotNil(tbl)

	t.Equal([]string{"foo"}, tbl.Identifier())
	t.Equal("s3://bucket/test/location/uuid.metadata.json", tbl.MetadataLocation())
	t.Equal(&mockfs, mustFS(t.T(), tbl))

	t.tbl = tbl
}

func (t *TableTestSuite) TestNewTableFromReadFile() {
	var mockfsReadFile internal.MockFSReadFile
	mockfsReadFile.Test(t.T())
	mockfsReadFile.On("ReadFile", "s3://bucket/test/location/uuid.metadata.json").
		Return([]byte(table.ExampleTableMetadataV2), nil)
	defer mockfsReadFile.AssertExpectations(t.T())

	tbl2, err := table.NewFromLocation(
		t.T().Context(),
		[]string{"foo"},
		"s3://bucket/test/location/uuid.metadata.json",
		func(ctx context.Context) (iceio.IO, error) {
			return &mockfsReadFile, nil
		},
		nil,
	)
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

	ctx context.Context

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
	t.ctx = context.Background()
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
	t.location = filepath.ToSlash(strings.Replace(t.T().TempDir(), "#", "", -1))
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
		t.location, iceberg.Properties{"format-version": strconv.Itoa(formatVersion)})
	t.Require().NoError(err)

	return table.New(
		identifier,
		meta,
		t.getMetadataLoc(),
		func(ctx context.Context) (iceio.IO, error) {
			return iceio.LocalFS{}, nil
		},
		nil,
	)
}

func (t *TableWritingTestSuite) TestAddFilesUnpartitioned() {
	ident := table.Identifier{"default", "unpartitioned_table_v" + strconv.Itoa(t.formatVersion)}
	tbl := t.createTable(ident, t.formatVersion,
		*iceberg.UnpartitionedSpec, t.tableSchema)

	t.NotNil(tbl)

	files := make([]string, 0)
	for i := range 5 {
		filePath := fmt.Sprintf("%s/unpartitioned/test-%d.parquet", t.location, i)
		t.writeParquet(mustFS(t.T(), tbl).(iceio.WriteFileIO), filePath, t.arrTbl)
		files = append(files, filePath)
	}

	tx := tbl.NewTransaction()
	t.Require().NoError(tx.AddFiles(t.ctx, files, nil, false))

	stagedTbl, err := tx.StagedTable()
	t.Require().NoError(err)
	t.NotNil(stagedTbl.NameMapping())

	t.Equal(stagedTbl.CurrentSnapshot().Summary,
		&table.Summary{
			Operation: table.OpAppend,
			Properties: iceberg.Properties{
				"added-data-files":       "5",
				"added-files-size":       "3600",
				"added-records":          "5",
				"total-data-files":       "5",
				"total-delete-files":     "0",
				"total-equality-deletes": "0",
				"total-files-size":       "3600",
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
		t.writeParquet(mustFS(t.T(), tbl).(iceio.WriteFileIO), filePath, t.arrTbl)
		files = append(files, filePath)
	}

	files = append(files, t.location+"/unpartitioned_file_not_found/unknown.parquet")
	tx := tbl.NewTransaction()
	err := tx.AddFiles(t.ctx, files, nil, false)
	t.Error(err)
	t.ErrorIs(err, fs.ErrNotExist)
}

func (t *TableWritingTestSuite) TestAddFilesUnpartitionedHasFieldIDs() {
	ident := table.Identifier{"default", "unpartitioned_table_with_ids_v" + strconv.Itoa(t.formatVersion)}
	tbl := t.createTable(ident, t.formatVersion,
		*iceberg.UnpartitionedSpec, t.tableSchema)

	t.NotNil(tbl)

	files := make([]string, 0)
	for i := range 5 {
		filePath := fmt.Sprintf("%s/unpartitioned_with_ids/test-%d.parquet", t.location, i)
		t.writeParquet(mustFS(t.T(), tbl).(iceio.WriteFileIO), filePath, t.arrTblWithIDs)
		files = append(files, filePath)
	}

	tx := tbl.NewTransaction()
	err := tx.AddFiles(t.ctx, files, nil, false)
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
	t.writeParquet(mustFS(t.T(), tbl).(iceio.WriteFileIO), filePath, mismatchTable)

	files := []string{filePath}

	pterm.DisableOutput() // disable error to console
	defer pterm.EnableOutput()

	tx := tbl.NewTransaction()
	err = tx.AddFiles(t.ctx, files, nil, false)
	t.Error(err)
	t.EqualError(err, `error encountered during schema visitor: mismatch in fields:
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

		t.writeParquet(mustFS(t.T(), tbl).(iceio.WriteFileIO), filePath, table)
		files = append(files, filePath)
	}

	tx := tbl.NewTransaction()
	t.Require().NoError(tx.AddFiles(t.ctx, files, nil, false))

	stagedTbl, err := tx.StagedTable()
	t.Require().NoError(err)
	t.NotNil(stagedTbl.NameMapping())

	t.Equal(stagedTbl.CurrentSnapshot().Summary,
		&table.Summary{
			Operation: table.OpAppend,
			Properties: iceberg.Properties{
				"added-data-files":        "5",
				"added-files-size":        "3600",
				"added-records":           "5",
				"changed-partition-count": "1",
				"total-data-files":        "5",
				"total-delete-files":      "0",
				"total-equality-deletes":  "0",
				"total-files-size":        "3600",
				"total-position-deletes":  "0",
				"total-records":           "5",
			},
		})

	m, err := stagedTbl.CurrentSnapshot().Manifests(mustFS(t.T(), tbl))
	t.Require().NoError(err)

	for _, manifest := range m {
		entries, err := manifest.FetchEntries(mustFS(t.T(), tbl), false)
		t.Require().NoError(err)

		for _, e := range entries {
			t.Equal(map[int]any{
				1000: 123, 1001: 650,
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

		t.writeParquet(mustFS(t.T(), tbl).(iceio.WriteFileIO), filePath, table)
		files = append(files, filePath)
	}

	tx := tbl.NewTransaction()
	err := tx.AddFiles(t.ctx, files, nil, false)
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

		t.writeParquet(mustFS(t.T(), tbl).(iceio.WriteFileIO), filePath, table)
		files = append(files, filePath)
	}

	tx := tbl.NewTransaction()
	err := tx.AddFiles(t.ctx, files, nil, false)
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

	t.writeParquet(mustFS(t.T(), tbl).(iceio.WriteFileIO), filePath, arrTable)
	t.writeParquet(mustFS(t.T(), tbl).(iceio.WriteFileIO), filePathLarge, arrTableLarge)

	tx := tbl.NewTransaction()
	t.Require().NoError(tx.AddFiles(t.ctx, []string{filePath, filePathLarge}, nil, false))

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
	t.writeParquet(mustFS(t.T(), tbl).(iceio.WriteFileIO), filePath, t.arrTablePromotedTypes)

	tx := tbl.NewTransaction()
	t.Require().NoError(tx.AddFiles(t.ctx, []string{filePath}, nil, false))

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
	t.writeParquet(mustFS(t.T(), tbl).(iceio.WriteFileIO), filePath, withoutCol)

	tx := tbl.NewTransaction()
	t.Require().NoError(tx.AddFiles(t.ctx, []string{filePath}, nil, false))

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
	t.writeParquet(mustFS(t.T(), tbl).(iceio.WriteFileIO), filePath, t.arrTbl)

	tx := tbl.NewTransaction()
	err := tx.AddFiles(t.ctx, []string{filePath, filePath}, nil, false)
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
		t.writeParquet(mustFS(t.T(), tbl).(iceio.WriteFileIO), filePath, t.arrTbl)
		files = append(files, filePath)
	}

	tx := tbl.NewTransaction()
	t.Require().NoError(tx.AddFiles(t.ctx, files, nil, false))

	err := tx.AddFiles(t.ctx, files[len(files)-1:], nil, false)
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
		t.writeParquet(mustFS(t.T(), tbl).(iceio.WriteFileIO), filePath, t.arrTbl)
		files = append(files, filePath)
	}

	tx := tbl.NewTransaction()
	t.Require().NoError(tx.AddFiles(t.ctx, files, nil, false))

	t.Require().NoError(tx.AddFiles(t.ctx, files[len(files)-1:], nil, true))
	staged, err := tx.StagedTable()
	t.Require().NoError(err)

	added, existing, deleted := []int32{}, []int32{}, []int32{}
	for m, err := range staged.AllManifests(t.T().Context()) {
		t.Require().NoError(err)
		added = append(added, m.AddedDataFiles())
		existing = append(existing, m.ExistingDataFiles())
		deleted = append(deleted, m.DeletedDataFiles())
	}

	t.Equal([]int32{5, 1, 5}, added)
	t.Equal([]int32{0, 0, 0}, existing)
	t.Equal([]int32{0, 0, 0}, deleted)
}

type mockedCatalog struct{
	metadata table.Metadata
}

func (m *mockedCatalog) LoadTable(ctx context.Context, ident table.Identifier) (*table.Table, error) {
	return nil, nil
}

func (m *mockedCatalog) CommitTable(ctx context.Context, ident table.Identifier, reqs []table.Requirement, updates []table.Update) (table.Metadata, string, error) {
	bldr, err := table.MetadataBuilderFromBase(m.metadata)
	if err != nil {
		return nil, "", err
	}

	for _, u := range updates {
		if err := u.Apply(bldr); err != nil {
			return nil, "", err
		}
	}

	meta, err := bldr.Build()
	if err != nil {
		return nil, "", err
	}

	m.metadata = meta

	return meta, "", nil
}

func (t *TableWritingTestSuite) TestReplaceDataFiles() {
	fs := iceio.LocalFS{}

	files := make([]string, 0)
	for i := range 5 {
		filePath := fmt.Sprintf("%s/replace_data_files_v%d/data-%d.parquet", t.location, t.formatVersion, i)
		t.writeParquet(fs, filePath, t.arrTablePromotedTypes)
		files = append(files, filePath)
	}

	ident := table.Identifier{"default", "replace_data_files_v" + strconv.Itoa(t.formatVersion)}
	meta, err := table.NewMetadata(t.tableSchemaPromotedTypes, iceberg.UnpartitionedSpec,
		table.UnsortedSortOrder, t.location, iceberg.Properties{"format-version": strconv.Itoa(t.formatVersion)})
	t.Require().NoError(err)

	ctx := context.Background()

	tbl := table.New(
		ident,
		meta,
		t.getMetadataLoc(),
		func(ctx context.Context) (iceio.IO, error) {
			return fs, nil
		},
		&mockedCatalog{meta},
	)
	for i := range 5 {
		tx := tbl.NewTransaction()
		t.Require().NoError(tx.AddFiles(ctx, files[i:i+1], nil, false))
		tbl, err = tx.Commit(ctx)
		t.Require().NoError(err)
	}

	mflist, err := tbl.CurrentSnapshot().Manifests(mustFS(t.T(), tbl))
	t.Require().NoError(err)
	t.Len(mflist, 5)

	// create a parquet file that is essentially as if we merged two of
	// the data files together
	cols := make([]arrow.Column, 0, t.arrTablePromotedTypes.NumCols())
	for i := range int(t.arrTablePromotedTypes.NumCols()) {
		chkd := t.arrTablePromotedTypes.Column(i).Data()
		duplicated := arrow.NewChunked(chkd.DataType(), append(chkd.Chunks(), chkd.Chunks()...))
		defer duplicated.Release()

		col := arrow.NewColumn(t.arrSchemaPromotedTypes.Fields()[i], duplicated)
		defer col.Release()

		cols = append(cols, *col)
	}

	combined := array.NewTable(t.arrSchemaPromotedTypes, cols, -1)
	defer combined.Release()

	combinedFilePath := fmt.Sprintf("%s/replace_data_files_v%d/combined.parquet", t.location, t.formatVersion)
	t.writeParquet(fs, combinedFilePath, combined)

	tx := tbl.NewTransaction()
	t.Require().NoError(tx.ReplaceDataFiles(ctx, files[:2], []string{combinedFilePath}, nil))

	staged, err := tx.StagedTable()
	t.Require().NoError(err)

	t.Equal(&table.Summary{
		Operation: table.OpOverwrite,
		Properties: iceberg.Properties{
			"added-data-files":       "1",
			"added-files-size":       "1068",
			"added-records":          "4",
			"deleted-data-files":     "2",
			"deleted-records":        "4",
			"removed-files-size":     "2136",
			"total-data-files":       "4",
			"total-delete-files":     "0",
			"total-equality-deletes": "0",
			"total-files-size":       "4272",
			"total-position-deletes": "0",
			"total-records":          "10",
		},
	}, staged.CurrentSnapshot().Summary)
}

func (t *TableWritingTestSuite) TestExpireSnapshots() {
	fs := iceio.LocalFS{}

	files := make([]string, 0)
	for i := range 5 {
		filePath := fmt.Sprintf("%s/replace_data_files_v%d/data-%d.parquet", t.location, t.formatVersion, i)
		t.writeParquet(fs, filePath, t.arrTablePromotedTypes)
		files = append(files, filePath)
	}

	ident := table.Identifier{"default", "replace_data_files_v" + strconv.Itoa(t.formatVersion)}
	meta, err := table.NewMetadata(t.tableSchemaPromotedTypes, iceberg.UnpartitionedSpec,
		table.UnsortedSortOrder, t.location, iceberg.Properties{"format-version": strconv.Itoa(t.formatVersion)})
	t.Require().NoError(err)

	ctx := context.Background()

	tbl := table.New(
		ident,
		meta,
		t.getMetadataLoc(),
		func(ctx context.Context) (iceio.IO, error) {
			return fs, nil
		},
		&mockedCatalog{meta},
	)

	tblfs, err := tbl.FS(ctx)

	t.Require().NoError(err)

	for i := range 5 {
		tx := tbl.NewTransaction()
		t.Require().NoError(tx.AddFiles(ctx, files[i:i+1], nil, false))
		tbl, err = tx.Commit(ctx)
		t.Require().NoError(err)
	}

	mflist, err := tbl.CurrentSnapshot().Manifests(tblfs)
	t.Require().NoError(err)
	t.Len(mflist, 5)
	t.Require().Equal(5, len(tbl.Metadata().Snapshots()))

	tx := tbl.NewTransaction()
	t.Require().NoError(tx.ExpireSnapshots(table.WithOlderThan(0), table.WithRetainLast(2)))
	tbl, err = tx.Commit(ctx)
	t.Require().NoError(err)
	t.Require().Equal(2, len(tbl.Metadata().Snapshots()))
	t.Require().Equal(2, len(slices.Collect(tbl.Metadata().SnapshotLogs())))
}

func (t *TableWritingTestSuite) TestWriteSpecialCharacterColumn() {
	ident := table.Identifier{"default", "write_special_character_column"}
	colNameWithSpecialChar := "letter/abc"

	s := iceberg.NewSchema(0,
		iceberg.NestedField{ID: 1, Name: colNameWithSpecialChar, Type: iceberg.PrimitiveTypes.String},
		iceberg.NestedField{ID: 2, Name: "id", Type: iceberg.PrimitiveTypes.Int32},
		iceberg.NestedField{ID: 3, Name: "name", Type: iceberg.PrimitiveTypes.String, Required: true},
		iceberg.NestedField{ID: 4, Name: "address", Required: true, Type: &iceberg.StructType{
			FieldList: []iceberg.NestedField{
				{ID: 5, Name: "street", Type: iceberg.PrimitiveTypes.String, Required: true},
				{ID: 6, Name: "city", Type: iceberg.PrimitiveTypes.String, Required: true},
				{ID: 7, Name: "zip", Type: iceberg.PrimitiveTypes.Int32, Required: true},
				{ID: 8, Name: colNameWithSpecialChar, Type: iceberg.PrimitiveTypes.String, Required: true},
			},
		}})

	arrowSchema := arrow.NewSchema([]arrow.Field{
		{Name: colNameWithSpecialChar, Type: arrow.BinaryTypes.String, Nullable: true},
		{Name: "id", Type: arrow.PrimitiveTypes.Int32, Nullable: true},
		{Name: "name", Type: arrow.BinaryTypes.String},
		{Name: "address", Type: arrow.StructOf(
			arrow.Field{Name: "street", Type: arrow.BinaryTypes.String},
			arrow.Field{Name: "city", Type: arrow.BinaryTypes.String},
			arrow.Field{Name: "zip", Type: arrow.PrimitiveTypes.Int32},
			arrow.Field{Name: colNameWithSpecialChar, Type: arrow.BinaryTypes.String},
		)},
	}, nil)

	arrowTable, err := array.TableFromJSON(memory.DefaultAllocator, arrowSchema, []string{
		`[
			{
				"letter/abc": "a",
				"id": 1,
				"name": "AB",
				"address": {"street": "123", "city": "SFO", "zip": 12345, "letter/abc": "a"}
			},
			{
				"letter/abc": null,
				"id": 2,
				"name": "CD",
				"address": {"street": "456", "city": "SW", "zip": 67890, "letter/abc": "b"}
			},
			{
				"letter/abc": "z",
				"id": 3,
				"name": "EF",
				"address": {"street": "789", "city": "Random", "zip": 10112, "letter/abc": "c"}
			}
		 ]`,
	})
	t.Require().NoError(err)
	defer arrowTable.Release()

	tbl := t.createTable(ident, t.formatVersion, *iceberg.UnpartitionedSpec, s)
	rdr := array.NewTableReader(arrowTable, 1)
	defer rdr.Release()

	tx := tbl.NewTransaction()
	t.Require().NoError(tx.Append(t.ctx, rdr, nil))

	scan, err := tx.Scan()
	t.Require().NoError(err)

	result, err := scan.ToArrowTable(t.ctx)
	t.Require().NoError(err)
	defer result.Release()

	t.True(array.TableEqual(arrowTable, result), "expected:\n %s\ngot:\n %s", arrowTable, result)
}

func (t *TableWritingTestSuite) getInMemCatalog() catalog.Catalog {
	cat, err := catalog.Load(context.Background(), "default", iceberg.Properties{
		"uri":          ":memory:",
		"type":         "sql",
		sql.DriverKey:  sqliteshim.ShimName,
		sql.DialectKey: string(sql.SQLite),
		"warehouse":    "file://" + t.location,
	})
	t.Require().NoError(err)

	return cat
}

func (t *TableWritingTestSuite) createTableWithProps(identifier table.Identifier, props iceberg.Properties, sc *iceberg.Schema) *table.Table {
	cat := t.getInMemCatalog()
	cat.DropTable(t.ctx, identifier)
	cat.DropNamespace(t.ctx, catalog.NamespaceFromIdent(identifier))

	t.Require().NoError(cat.CreateNamespace(t.ctx, catalog.NamespaceFromIdent(identifier), nil))
	tbl, err := cat.CreateTable(t.ctx, identifier, sc, catalog.WithProperties(props),
		catalog.WithLocation("file://"+t.location))

	t.Require().NoError(err)

	return tbl
}

func tableSchema() *iceberg.Schema {
	return iceberg.NewSchema(0,
		iceberg.NestedField{ID: 1, Name: "bool", Type: iceberg.PrimitiveTypes.Bool},
		iceberg.NestedField{ID: 2, Name: "string", Type: iceberg.PrimitiveTypes.String},
		iceberg.NestedField{ID: 3, Name: "string_long", Type: iceberg.PrimitiveTypes.String},
		iceberg.NestedField{ID: 4, Name: "int", Type: iceberg.PrimitiveTypes.Int32},
		iceberg.NestedField{ID: 5, Name: "long", Type: iceberg.PrimitiveTypes.Int64},
		iceberg.NestedField{ID: 6, Name: "float", Type: iceberg.PrimitiveTypes.Float32},
		iceberg.NestedField{ID: 7, Name: "double", Type: iceberg.PrimitiveTypes.Float64},
		iceberg.NestedField{ID: 8, Name: "time", Type: iceberg.PrimitiveTypes.Time},
		iceberg.NestedField{ID: 9, Name: "timestamp", Type: iceberg.PrimitiveTypes.Timestamp},
		iceberg.NestedField{ID: 10, Name: "timestamptz", Type: iceberg.PrimitiveTypes.TimestampTz},
		iceberg.NestedField{ID: 11, Name: "date", Type: iceberg.PrimitiveTypes.Date},
		iceberg.NestedField{ID: 12, Name: "uuid", Type: iceberg.PrimitiveTypes.UUID},
		iceberg.NestedField{ID: 13, Name: "binary", Type: iceberg.PrimitiveTypes.Binary},
		iceberg.NestedField{ID: 14, Name: "fixed", Type: iceberg.FixedTypeOf(16)},
		iceberg.NestedField{ID: 15, Name: "small_dec", Type: iceberg.DecimalTypeOf(8, 2)},
		iceberg.NestedField{ID: 16, Name: "med_dec", Type: iceberg.DecimalTypeOf(16, 2)},
		iceberg.NestedField{ID: 17, Name: "large_dec", Type: iceberg.DecimalTypeOf(24, 2)},
	)
}

func arrowTableWithNull() arrow.Table {
	sc, err := table.SchemaToArrowSchema(tableSchema(), nil, true, false)
	if err != nil {
		panic(err)
	}

	arrTable, err := array.TableFromJSON(memory.DefaultAllocator, sc, []string{
		`[
			{
				"bool": false,
				"string": "a",
				"string_long": "` + strings.Repeat("a", 22) + `",
				"int": 1,
				"long": 1,
				"float": 0.0,
				"double": 0.0,
				"time": "00:00:01.000000",
				"timestamp": "2023-01-01T19:25:00.000000+08:00",
				"timestamptz": "2023-01-01T19:25:00.000000Z",
				"date": "2023-01-01",
				"uuid": "00000000-0000-0000-0000-000000000000",
				"binary": "AQ==",
				"fixed": "AAAAAAAAAAAAAAAAAAAAAA==",
				"small_dec": "123456.78",
				"med_dec": "12345678901234.56",
				"large_dec": "1234567890123456789012.34"
			},
			{
				"bool": null,
				"string": null,
				"string_long": null,
				"int": null,
				"long": null,
				"float": null,
				"double": null,
				"time": null,
				"timestamp": null,
				"timestamptz": null,
				"date": null,
				"uuid": null,
				"binary": null,
				"fixed": null,
				"small_dec": null,
				"med_dec": null,
				"large_dec": null
			},
			{
				"bool": true,
				"string": "z",
				"string_long": "` + strings.Repeat("z", 22) + `",
				"int": 9,
				"long": 9,
				"float": 0.9,
				"double": 0.9,
				"time": "00:00:03.000000",
				"timestamp": "2023-03-01T19:25:00.000000+08:00",
				"timestamptz": "2023-03-01T19:25:00.000000Z",
				"date": "2023-03-01",
				"uuid": "11111111-1111-1111-1111-111111111111",
				"binary": "Eg==",
				"fixed": "EREREREREREREREREREREQ==",
				"small_dec": "876543.21",
				"med_dec": "65432109876543.21",
				"large_dec": "4321098765432109876543.21"
			}
		 ]`,
	})
	if err != nil {
		panic(err)
	}

	return arrTable
}

func (t *TableWritingTestSuite) validateManifestFileLength(fs iceio.IO, m iceberg.ManifestFile) {
	f, err := fs.Open(m.FilePath())
	t.Require().NoError(err)
	defer f.Close()

	info, err := f.Stat()
	t.Require().NoError(err)

	t.EqualValues(info.Size(), m.Length(), "expected size: %d, got: %d", info.Size(), m.Length())
}

func (t *TableWritingTestSuite) TestMergeManifests() {
	tblA := t.createTableWithProps(table.Identifier{"default", "merge_manifest_a"},
		iceberg.Properties{
			table.ParquetCompressionKey:    "snappy",
			table.ManifestMergeEnabledKey:  "true",
			table.ManifestMinMergeCountKey: "1",
			"format-version":               strconv.Itoa(t.formatVersion),
		}, tableSchema())

	tblB := t.createTableWithProps(table.Identifier{"default", "merge_manifest_b"},
		iceberg.Properties{
			table.ParquetCompressionKey:      "snappy",
			table.ManifestMergeEnabledKey:    "true",
			table.ManifestMinMergeCountKey:   "1",
			table.ManifestTargetSizeBytesKey: "1",
			"format-version":                 strconv.Itoa(t.formatVersion),
		}, tableSchema())

	tblC := t.createTableWithProps(table.Identifier{"default", "merge_manifest_c"},
		iceberg.Properties{
			table.ParquetCompressionKey:    "snappy",
			table.ManifestMinMergeCountKey: "1",
			"format-version":               strconv.Itoa(t.formatVersion),
		}, tableSchema())

	arrTable := arrowTableWithNull()
	defer arrTable.Release()

	var err error
	// tblA should merge all manifests into 1
	tblA, err = tblA.AppendTable(t.ctx, arrTable, 1, nil)
	t.Require().NoError(err)
	tblA, err = tblA.AppendTable(t.ctx, arrTable, 1, nil)
	t.Require().NoError(err)
	tblA, err = tblA.AppendTable(t.ctx, arrTable, 1, nil)
	t.Require().NoError(err)

	// tblB should not merge any manifests because the target size is too small
	tblB, err = tblB.AppendTable(t.ctx, arrTable, 1, nil)
	t.Require().NoError(err)
	tblB, err = tblB.AppendTable(t.ctx, arrTable, 1, nil)
	t.Require().NoError(err)
	tblB, err = tblB.AppendTable(t.ctx, arrTable, 1, nil)
	t.Require().NoError(err)

	// tblC should not merge any manifests because merging is disabled
	tblC, err = tblC.AppendTable(t.ctx, arrTable, 1, nil)
	t.Require().NoError(err)
	tblC, err = tblC.AppendTable(t.ctx, arrTable, 1, nil)
	t.Require().NoError(err)
	tblC, err = tblC.AppendTable(t.ctx, arrTable, 1, nil)
	t.Require().NoError(err)

	manifestList, err := tblA.CurrentSnapshot().Manifests(mustFS(t.T(), tblA))
	t.Require().NoError(err)
	t.Len(manifestList, 1)
	t.validateManifestFileLength(mustFS(t.T(), tblA), manifestList[0])

	entries, err := manifestList[0].FetchEntries(mustFS(t.T(), tblA), false)
	t.Require().NoError(err)
	t.Len(entries, 3)

	// entries should match the snapshot ID they were added in
	snapshotList := tblA.Metadata().Snapshots()
	slices.Reverse(snapshotList)
	for i, entry := range entries {
		t.Equal(snapshotList[i].SnapshotID, entry.SnapshotID())
		if t.formatVersion > 1 {
			t.EqualValues(3-i, entry.SequenceNum())
		}
	}

	manifestList, err = tblB.CurrentSnapshot().Manifests(mustFS(t.T(), tblB))
	t.Require().NoError(err)
	t.Len(manifestList, 3)

	for _, m := range manifestList {
		t.validateManifestFileLength(mustFS(t.T(), tblB), m)
	}

	manifestList, err = tblC.CurrentSnapshot().Manifests(mustFS(t.T(), tblC))
	t.Require().NoError(err)
	t.Len(manifestList, 3)

	for _, m := range manifestList {
		t.validateManifestFileLength(mustFS(t.T(), tblC), m)
	}

	resultA, err := tblA.Scan().ToArrowTable(t.ctx)
	t.Require().NoError(err)
	defer resultA.Release()

	resultB, err := tblB.Scan().ToArrowTable(t.ctx)
	t.Require().NoError(err)
	defer resultB.Release()

	resultC, err := tblC.Scan().ToArrowTable(t.ctx)
	t.Require().NoError(err)
	defer resultC.Release()

	// tblA and tblC should contain the same data
	t.True(array.TableEqual(resultA, resultC), "expected:\n %s\ngot:\n %s", resultA, resultC)
	// tblB and tblC should contain the same data
	t.True(array.TableEqual(resultB, resultC), "expected:\n %s\ngot:\n %s", resultB, resultC)
}

func TestTableWriting(t *testing.T) {
	suite.Run(t, &TableWritingTestSuite{formatVersion: 1})
	suite.Run(t, &TableWritingTestSuite{formatVersion: 2})
}

func TestNullableStructRequiredField(t *testing.T) {
	loc := filepath.ToSlash(t.TempDir())

	cat, err := catalog.Load(context.Background(), "default", iceberg.Properties{
		"uri":          ":memory:",
		"type":         "sql",
		sql.DriverKey:  sqliteshim.ShimName,
		sql.DialectKey: string(sql.SQLite),
		"warehouse":    "file://" + loc,
	})
	require.NoError(t, err)

	arrowSchema := arrow.NewSchema([]arrow.Field{
		{
			Name: "analytic", Type: arrow.StructOf(
				arrow.Field{Name: "category", Type: arrow.BinaryTypes.String, Nullable: true},
				arrow.Field{Name: "desc", Type: arrow.BinaryTypes.String, Nullable: true},
				arrow.Field{Name: "name", Type: arrow.BinaryTypes.String, Nullable: true},
				arrow.Field{Name: "related_analytics", Type: arrow.ListOf(
					arrow.StructOf(
						arrow.Field{Name: "category", Type: arrow.BinaryTypes.String, Nullable: true},
						arrow.Field{Name: "desc", Type: arrow.BinaryTypes.String, Nullable: true},
						arrow.Field{Name: "name", Type: arrow.BinaryTypes.String, Nullable: true},
						arrow.Field{Name: "type", Type: arrow.BinaryTypes.String, Nullable: true},
						arrow.Field{Name: "type_id", Type: arrow.PrimitiveTypes.Int32, Nullable: false},
						arrow.Field{Name: "uid", Type: arrow.BinaryTypes.String, Nullable: true},
						arrow.Field{Name: "version", Type: arrow.BinaryTypes.String, Nullable: true},
					),
				), Nullable: true},
				arrow.Field{Name: "type", Type: arrow.BinaryTypes.String, Nullable: true},
				arrow.Field{Name: "type_id", Type: arrow.PrimitiveTypes.Int32, Nullable: false},
				arrow.Field{Name: "uid", Type: arrow.BinaryTypes.String, Nullable: true},
				arrow.Field{Name: "version", Type: arrow.BinaryTypes.String, Nullable: true},
			), Nullable: true,
		},
		{Name: "uid", Type: arrow.BinaryTypes.String, Nullable: true},
	}, nil)

	sc, err := table.ArrowSchemaToIcebergWithFreshIDs(arrowSchema, false)
	require.NoError(t, err)

	require.NoError(t, cat.CreateNamespace(t.Context(), table.Identifier{"testing"}, nil))
	tbl, err := cat.CreateTable(t.Context(), table.Identifier{"testing", "nullable_struct_required_field"}, sc,
		catalog.WithProperties(iceberg.Properties{"format-version": "2"}),
		catalog.WithLocation("file://"+loc))
	require.NoError(t, err)
	require.NotNil(t, tbl)

	bldr := array.NewRecordBuilder(memory.DefaultAllocator, arrowSchema)
	defer bldr.Release()

	const N = 100
	bldr.Field(0).AppendNulls(N)
	bldr.Field(1).AppendNulls(N)

	rec := bldr.NewRecord()
	defer rec.Release()

	arrTable := array.NewTableFromRecords(arrowSchema, []arrow.Record{rec})
	defer arrTable.Release()

	tx := tbl.NewTransaction()
	require.NoError(t, tx.AppendTable(t.Context(), arrTable, N, nil))
	stagedTbl, err := tx.StagedTable()
	require.NoError(t, err)
	require.NotNil(t, stagedTbl)
}

type DeleteOldMetadataMockedCatalog struct{
	metadata table.Metadata
}

func (m *DeleteOldMetadataMockedCatalog) LoadTable(ctx context.Context, ident table.Identifier) (*table.Table, error) {
	return nil, nil
}

func (m *DeleteOldMetadataMockedCatalog) CommitTable(ctx context.Context, ident table.Identifier, reqs []table.Requirement, updates []table.Update) (table.Metadata, string, error) {
	bldr, err := table.MetadataBuilderFromBase(m.metadata)
	if err != nil {
		return nil, "", err
	}

	location := m.metadata.Location()

	randid := uuid.New().String()
	metdatafile := fmt.Sprintf("%s/metadata/%s.metadata.json", location, randid)

	// removing old metadata files
	bldr.TrimMetadataLogs(0)

	bldr.AppendMetadataLog(table.MetadataLogEntry{
		MetadataFile: metdatafile,
		TimestampMs:  time.Now().UnixMilli(),
	})

	for _, u := range updates {
		if err := u.Apply(bldr); err != nil {
			return nil, "", err
		}
	}

	meta, err := bldr.Build()
	if err != nil {
		return nil, "", err
	}

	m.metadata = meta

	return meta, metdatafile, nil
}

func createMetadataFile(metadatadir, metadataFile string) error {
	// Ensure the directory exists
	metadataDir := filepath.Dir(metadatadir)
	err := os.MkdirAll(metadataDir, 0o755)
	if err != nil {
		return err
	}

	err = os.WriteFile(metadataFile, []byte("first commit metadata content"), 0o644)

	return err
}

func (t *TableWritingTestSuite) TestDeleteOldMetadataLogsErrorOnFileNotFound() {
	// capture logs to validate that no error is logged
	var logBuf bytes.Buffer
	log.SetOutput(&logBuf)
	defer log.SetOutput(os.Stderr) // restore default output

	fs := iceio.LocalFS{}
	files := make([]string, 0)
	for i := range 5 {
		filePath := fmt.Sprintf("%s/file_v%d/data-%d.parquet", t.location, t.formatVersion, i)
		t.writeParquet(fs, filePath, t.arrTablePromotedTypes)
		files = append(files, filePath)
	}

	ident := table.Identifier{"default", "file_v" + strconv.Itoa(t.formatVersion)}
	meta, err := table.NewMetadata(t.tableSchemaPromotedTypes, iceberg.UnpartitionedSpec,
		table.UnsortedSortOrder, t.location, iceberg.Properties{"format-version": strconv.Itoa(t.formatVersion), "write.metadata.delete-after-commit.enabled": "true"})
	t.Require().NoError(err)

	tbl := table.New(ident, meta, t.getMetadataLoc(), func(ctx context.Context) (iceio.IO, error) {
		return fs, nil
	}, &DeleteOldMetadataMockedCatalog{meta})
	ctx := context.Background()

	// transaction 1 to create metadata file
	tx := tbl.NewTransaction()
	tx.AddFiles(ctx, files[0:1], nil, false)
	tbl_new, err := tx.Commit(ctx)
	t.Require().NoError(err)

	// transaction 2 to add files
	tx_new := tbl_new.NewTransaction()
	tx_new.AddFiles(ctx, files[1:2], nil, false)

	_, err = tx_new.Commit(ctx)
	t.Require().NoError(err)

	// validate that error is logged
	logOutput := logBuf.String()
	t.Contains(logOutput, "Warning: Failed to delete old metadata file")
	if runtime.GOOS == "windows" {
		t.Contains(logOutput, "The system cannot find the file specified")
	} else {
		t.Contains(logOutput, "no such file or directory")
	}
}

func (t *TableWritingTestSuite) TestDeleteOldMetadataNoErrorLogsOnFileFound() {
	// capture logs to validate that no error is logged
	var logBuf bytes.Buffer
	log.SetOutput(&logBuf)
	defer log.SetOutput(os.Stderr) // restore default output

	fs := iceio.LocalFS{}
	files := make([]string, 0)
	for i := range 5 {
		filePath := fmt.Sprintf("%s/file_v%d/data-%d.parquet", t.location, t.formatVersion, i)
		t.writeParquet(fs, filePath, t.arrTablePromotedTypes)
		files = append(files, filePath)
	}

	ident := table.Identifier{"default", "file_v" + strconv.Itoa(t.formatVersion)}
	meta, err := table.NewMetadata(t.tableSchemaPromotedTypes, iceberg.UnpartitionedSpec, table.UnsortedSortOrder, t.location, iceberg.Properties{"format-version": strconv.Itoa(t.formatVersion), "write.metadata.delete-after-commit.enabled": "true"})
	t.Require().NoError(err)

	tbl := table.New(
		ident,
		meta,
		t.getMetadataLoc(),
		func(ctx context.Context) (iceio.IO, error) {
			return fs, nil
		},
		&DeleteOldMetadataMockedCatalog{meta},
	)

	ctx := context.Background()

	// transaction 1 to create metadata file
	tx := tbl.NewTransaction()
	tx.AddFiles(ctx, files[0:1], nil, false)
	tbl_new, err := tx.Commit(ctx)
	t.Require().NoError(err)

	// Now we have the first metadata location - create the file there so that deleteOldMetadata does not log an error
	firstMetadataLoc := tbl_new.MetadataLocation()
	metadataFile := tbl_new.MetadataLocation()
	err = createMetadataFile(firstMetadataLoc, metadataFile)
	t.Require().NoError(err)

	// transaction 2 to add files
	tx_new := tbl_new.NewTransaction()
	tx_new.AddFiles(ctx, files[1:2], nil, false)
	_, err = tx_new.Commit(ctx)
	t.Require().NoError(err)

	// validate that no error is logged
	logOutput := logBuf.String()
	t.NotContains(logOutput, "Warning: Failed to delete old metadata file")
	t.NotContains(logOutput, "no such file or directory")
}

func (t *TableTestSuite) TestRefresh() {
	cat, err := catalog.Load(context.Background(), "default", iceberg.Properties{
		"uri":          ":memory:",
		"type":         "sql",
		sql.DriverKey:  sqliteshim.ShimName,
		sql.DialectKey: string(sql.SQLite),
		"warehouse":    "file://" + t.T().TempDir(),
	})
	t.Require().NoError(err)

	ident := table.Identifier{"test", "refresh_table"}
	t.Require().NoError(cat.CreateNamespace(context.Background(), catalog.NamespaceFromIdent(ident), nil))

	tbl, err := cat.CreateTable(context.Background(), ident, t.tbl.Schema(),
		catalog.WithProperties(iceberg.Properties{"original": "true"}))
	t.Require().NoError(err)
	t.Require().NotNil(tbl)

	originalProperties := tbl.Properties()
	originalIdentifier := tbl.Identifier()
	originalLocation := tbl.Location()
	originalSchema := tbl.Schema()
	originalSpec := tbl.Spec()

	_, _, err = cat.CommitTable(context.Background(), tbl.Identifier(), nil, []table.Update{
		table.NewSetPropertiesUpdate(iceberg.Properties{
			"refreshed": "true",
			"timestamp": strconv.FormatInt(time.Now().Unix(), 10),
		}),
	})
	t.Require().NoError(err)

	err = tbl.Refresh(context.Background())
	t.Require().NoError(err)
	t.Require().NotNil(tbl)

	t.Equal("true", tbl.Properties()["refreshed"])
	t.NotEqual(originalProperties, tbl.Properties())

	t.Equal(originalIdentifier, tbl.Identifier())
	t.Equal(originalLocation, tbl.Location())
	t.True(originalSchema.Equals(tbl.Schema()))
	t.Equal(originalSpec, tbl.Spec())
}
