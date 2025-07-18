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

//go:build integration

package table_test

import (
	"context"
	"github.com/apache/iceberg-go/internal/recipe"
	"strings"
	"testing"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/arrow-go/v18/parquet"
	"github.com/apache/arrow-go/v18/parquet/pqarrow"
	"github.com/apache/iceberg-go"
	"github.com/apache/iceberg-go/catalog"
	"github.com/apache/iceberg-go/catalog/rest"
	iceio "github.com/apache/iceberg-go/io"
	"github.com/apache/iceberg-go/table"
	"github.com/stretchr/testify/suite"
	"github.com/testcontainers/testcontainers-go/modules/compose"
)

type SparkIntegrationTestSuite struct {
	suite.Suite

	ctx   context.Context
	cat   catalog.Catalog
	props iceberg.Properties
	stack *compose.DockerCompose
}

func (s *SparkIntegrationTestSuite) SetupSuite() {
	var err error
	s.stack, err = recipe.Start(s.T())
	s.Require().NoError(err)
}

func (s *SparkIntegrationTestSuite) SetupTest() {
	s.ctx = context.Background()
	s.props = iceberg.Properties{
		iceio.S3Region:          "us-east-1",
		iceio.S3EndpointURL:     "http://localhost:9000",
		iceio.S3AccessKeyID:     "admin",
		iceio.S3SecretAccessKey: "password",
	}

	cat, err := rest.NewCatalog(s.ctx, "rest", "http://localhost:8181", rest.WithAdditionalProps(s.props))
	s.Require().NoError(err)
	s.cat = cat
}

func (s *SparkIntegrationTestSuite) TestSetProperties() {
	icebergSchema := iceberg.NewSchema(0,
		iceberg.NestedField{ID: 1, Name: "foo", Type: iceberg.PrimitiveTypes.Bool},
		iceberg.NestedField{ID: 2, Name: "bar", Type: iceberg.PrimitiveTypes.String},
		iceberg.NestedField{ID: 3, Name: "baz", Type: iceberg.PrimitiveTypes.Int32},
	)

	tbl, err := s.cat.CreateTable(s.ctx, catalog.ToIdentifier("default", "go_test_set_properties"), icebergSchema)
	s.Require().NoError(err)

	tx := tbl.NewTransaction()

	err = tx.SetProperties(iceberg.Properties{
		table.ParquetCompressionKey:      "snappy",
		table.ManifestMergeEnabledKey:    "true",
		table.ManifestMinMergeCountKey:   "1",
		table.ManifestTargetSizeBytesKey: "1",
	})
	s.Require().NoError(err)

	_, err = tx.Commit(s.ctx)
	s.Require().NoError(err)

	err = recipe.ExecuteSpark(s.T(), s.stack, "./validation.py", "--test", "testSetProperties")
	s.Require().NoError(err)
}

func (s *SparkIntegrationTestSuite) TestAddFile() {
	const filename = "s3://warehouse/default/test_partitioned_by_days/data/ts_day=2023-03-13/supertest.parquet"

	tbl, err := s.cat.LoadTable(s.ctx, catalog.ToIdentifier("default", "test_partitioned_by_days"), nil)
	s.Require().NoError(err)

	sc, err := table.SchemaToArrowSchema(tbl.Schema(), nil, false, false)
	if err != nil {
		panic(err)
	}

	bldr := array.NewRecordBuilder(memory.DefaultAllocator, sc)
	defer bldr.Release()

	tm := time.Date(2023, 03, 13, 13, 22, 0, 0, time.UTC)
	ts, _ := arrow.TimestampFromTime(tm, arrow.Microsecond)
	bldr.Field(0).(*array.Date32Builder).Append(arrow.Date32FromTime(tm))
	bldr.Field(1).(*array.TimestampBuilder).Append(ts)
	bldr.Field(2).(*array.Int32Builder).Append(13)
	bldr.Field(3).(*array.StringBuilder).Append("m")

	rec := bldr.NewRecord()
	defer rec.Release()

	fw, err := mustFS(s.T(), tbl).(iceio.WriteFileIO).Create(filename)
	if err != nil {
		panic(err)
	}
	defer fw.Close()

	if err := pqarrow.WriteTable(array.NewTableFromRecords(sc, []arrow.Record{rec}), fw, rec.NumRows(), parquet.NewWriterProperties(), pqarrow.DefaultWriterProps()); err != nil {
		panic(err)
	}

	tx := tbl.NewTransaction()
	err = tx.AddFiles(s.ctx, []string{filename}, nil, false)
	s.Require().NoError(err)

	tbl, err = tx.Commit(s.ctx)
	s.Require().NoError(err)

	err = recipe.ExecuteSpark(s.T(), s.stack, "./validation.py", "--test", "TestAddedFile")
	s.Require().NoError(err)
}

func (s *SparkIntegrationTestSuite) TestDifferentDataTypes() {
	icebergSchema := iceberg.NewSchema(0,
		iceberg.NestedField{ID: 1, Name: "bool", Type: iceberg.PrimitiveTypes.Bool},
		iceberg.NestedField{ID: 2, Name: "string", Type: iceberg.PrimitiveTypes.String},
		iceberg.NestedField{ID: 3, Name: "string_long", Type: iceberg.PrimitiveTypes.String},
		iceberg.NestedField{ID: 4, Name: "int", Type: iceberg.PrimitiveTypes.Int32},
		iceberg.NestedField{ID: 5, Name: "long", Type: iceberg.PrimitiveTypes.Int64},
		iceberg.NestedField{ID: 6, Name: "float", Type: iceberg.PrimitiveTypes.Float32},
		iceberg.NestedField{ID: 7, Name: "double", Type: iceberg.PrimitiveTypes.Float64},
		iceberg.NestedField{ID: 8, Name: "timestamp", Type: iceberg.PrimitiveTypes.Timestamp},
		iceberg.NestedField{ID: 9, Name: "timestamptz", Type: iceberg.PrimitiveTypes.TimestampTz},
		iceberg.NestedField{ID: 10, Name: "date", Type: iceberg.PrimitiveTypes.Date},
		iceberg.NestedField{ID: 11, Name: "uuid", Type: iceberg.PrimitiveTypes.UUID},
		iceberg.NestedField{ID: 12, Name: "binary", Type: iceberg.PrimitiveTypes.Binary},
		iceberg.NestedField{ID: 13, Name: "fixed", Type: iceberg.FixedTypeOf(16)},
		iceberg.NestedField{ID: 14, Name: "small_dec", Type: iceberg.DecimalTypeOf(8, 2)},
		iceberg.NestedField{ID: 15, Name: "med_dec", Type: iceberg.DecimalTypeOf(16, 2)},
		iceberg.NestedField{ID: 16, Name: "large_dec", Type: iceberg.DecimalTypeOf(24, 2)},
		iceberg.NestedField{ID: 17, Name: "list", Type: &iceberg.ListType{
			ElementID: 18,
			Element:   iceberg.PrimitiveTypes.Int32},
		},
	)

	arrowSchema, err := table.SchemaToArrowSchema(icebergSchema, nil, true, false)
	s.Require().NoError(err)

	arrTable, err := array.TableFromJSON(memory.DefaultAllocator, arrowSchema, []string{
		`[
			{
				"bool": false,
				"string": "a",
				"string_long": "` + strings.Repeat("a", 22) + `",
				"int": 1,
				"long": 1,
				"float": 0.0,
				"double": 0.0,
				"timestamp": "2023-01-01T19:25:00.000000+08:00",
				"timestamptz": "2023-01-01T19:25:00.000000Z",
				"date": "2023-01-01",
				"uuid": "00000000-0000-0000-0000-000000000000",
				"binary": "AQ==",
				"fixed": "AAAAAAAAAAAAAAAAAAAAAA==",
				"small_dec": "123456.78",
				"med_dec": "12345678901234.56",
				"large_dec": "1234567890123456789012.34",
				"list": [1, 2, 3]
			},
			{
				"bool": null,
				"string": null,
				"string_long": null,
				"int": null,
				"long": null,
				"float": null,
				"double": null,
				"timestamp": null,
				"timestamptz": null,
				"date": null,
				"uuid": null,
				"binary": null,
				"fixed": null,
				"small_dec": null,
				"med_dec": null,
				"large_dec": null,
				"list": null
			},
			{
				"bool": true,
				"string": "z",
				"string_long": "` + strings.Repeat("z", 22) + `",
				"int": 9,
				"long": 9,
				"float": 0.9,
				"double": 0.9,
				"timestamp": "2023-03-01T19:25:00.000000+08:00",
				"timestamptz": "2023-03-01T19:25:00.000000Z",
				"date": "2023-03-01",
				"uuid": "11111111-1111-1111-1111-111111111111",
				"binary": "Eg==",
				"fixed": "EREREREREREREREREREREQ==",
				"small_dec": "876543.21",
				"med_dec": "65432109876543.21",
				"large_dec": "4321098765432109876543.21",
				"list": [-1, -2, -3]
			}
		 ]`,
	})
	s.Require().NoError(err)

	tbl, err := s.cat.CreateTable(s.ctx, catalog.ToIdentifier("default", "go_test_different_data_types"), icebergSchema)
	s.Require().NoError(err)

	tx := tbl.NewTransaction()
	err = tx.AppendTable(s.ctx, arrTable, 1, nil)
	s.Require().NoError(err)

	_, err = tx.Commit(s.ctx)
	s.Require().NoError(err)

	err = recipe.ExecuteSpark(s.T(), s.stack, "./validation.py", "--test", "TestReadDifferentDataTypes")
	s.Require().NoError(err)
}

func (s *SparkIntegrationTestSuite) TestUpdateSpec() {
	icebergSchema := iceberg.NewSchema(0,
		iceberg.NestedField{ID: 1, Name: "foo", Type: iceberg.PrimitiveTypes.Bool},
		iceberg.NestedField{ID: 2, Name: "bar", Type: iceberg.PrimitiveTypes.String},
		iceberg.NestedField{ID: 3, Name: "baz", Type: iceberg.PrimitiveTypes.Int32},
	)

	partitionSpec := iceberg.NewPartitionSpec(
		iceberg.PartitionField{SourceID: 3, FieldID: 1000, Transform: iceberg.TruncateTransform{Width: 5}, Name: "baz_truncate"},
	)

	tbl, err := s.cat.CreateTable(
		s.ctx,
		catalog.ToIdentifier("default", "go_test_update_spec"),
		icebergSchema,
		catalog.WithPartitionSpec(&partitionSpec),
	)
	s.Require().NoError(err)

	tx := tbl.NewTransaction()
	err = tx.UpdateSpec(false).
		AddField("baz", iceberg.BucketTransform{NumBuckets: 3}, "").
		Commit()
	s.Require().NoError(err)
	_, err = tx.Commit(s.ctx)

	err = recipe.ExecuteSpark(s.T(), s.stack, "./validation.py", "--test", "TestReadSpecUpdate")
	s.Require().NoError(err)
}

func TestSparkIntegration(t *testing.T) {
	suite.Run(t, new(SparkIntegrationTestSuite))
}
