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
	"encoding/json"
	"fmt"
	"testing"

	"github.com/apache/iceberg-go"
	"github.com/apache/iceberg-go/internal"
	"github.com/apache/iceberg-go/table"
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
		Return(&internal.MockFile{Contents: bytes.NewReader([]byte(ExampleTableMetadataV2))}, nil)
	defer mockfs.AssertExpectations(t.T())

	tbl, err := table.NewFromLocation([]string{"foo"}, "s3://bucket/test/location/uuid.metadata.json", &mockfs)
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
		Return([]byte(ExampleTableMetadataV2), nil)
	defer mockfsReadFile.AssertExpectations(t.T())

	tbl2, err := table.NewFromLocation([]string{"foo"}, "s3://bucket/test/location/uuid.metadata.json", &mockfsReadFile)
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

func (t *TableTestSuite) TestNewTable() {

	identifier := table.Identifier{"records"}

	schema := iceberg.NewSchemaWithIdentifiers(1, []int{1},
		iceberg.NestedField{
			ID: 1, Name: "vendor_id", Type: iceberg.PrimitiveTypes.Int64, Required: true},
		iceberg.NestedField{
			ID: 2, Name: "name", Type: iceberg.PrimitiveTypes.String},
		iceberg.NestedField{
			ID: 3, Name: "datetime", Type: iceberg.PrimitiveTypes.TimestampTz})

	partSpec := iceberg.NewPartitionSpec(iceberg.PartitionField{
		SourceID: 3, FieldID: 1000, Name: "datetime", Transform: iceberg.DayTransform{}})

	location := "s3://bucket/test/location"

	tbl, err := table.NewTable(identifier, schema, partSpec, table.UnsortedSortOrder, location, nil)
	t.Require().NoError(err)
	t.Require().Equal(identifier, tbl.Identifier())

	data, err := json.Marshal(tbl.Metadata())
	t.Require().NoError(err)
	fmt.Println(string(data))
}
