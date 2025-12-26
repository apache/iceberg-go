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
	"io"
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
	"github.com/apache/iceberg-go/internal"
	"github.com/apache/iceberg-go/internal/recipe"
	iceio "github.com/apache/iceberg-go/io"
	"github.com/apache/iceberg-go/table"
	"github.com/stretchr/testify/assert"
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
	if s.stack == nil {
		s.T().Skip("skipping test, AWS_S3_ENDPOINT is set")
	}
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

	spark, err := s.stack.ServiceContainer(s.T().Context(), "spark-iceberg")
	s.Require().NoError(err)

	_, stdout, err := spark.Exec(s.ctx, []string{"ipython", "./run_spark_count_sql.py"})
	s.Require().NoError(err)

	output, err := io.ReadAll(stdout)
	s.Require().NoError(err)
	strings.HasSuffix(string(output), `
+--------+
|count(1)|
+--------+
|      13|
+--------+
`)
}

func TestSparkIntegration(t *testing.T) {
	suite.Run(t, new(SparkIntegrationTestSuite))
}

// Dynamic Partition Overwrite Tests
func TestDynamicPartitionOverwrite_UnpartitionedTable(t *testing.T) {
	// Create a table with no partition spec
	schema := iceberg.NewSchema(0,
		iceberg.NestedField{ID: 1, Name: "id", Type: iceberg.PrimitiveTypes.Int32, Required: true},
		iceberg.NestedField{ID: 2, Name: "data", Type: iceberg.PrimitiveTypes.String, Required: false},
	)

	metadata, err := table.NewMetadataBuilder()
	assert.NoError(t, err)
	_, err = metadata.AddSchema(schema, 2, true)
	assert.NoError(t, err)
	_, err = metadata.SetCurrentSchemaID(0)
	assert.NoError(t, err)
	_, err = metadata.AddPartitionSpec(iceberg.UnpartitionedSpec, true)
	assert.NoError(t, err)
	_, err = metadata.SetDefaultSpecID(0)
	assert.NoError(t, err)
	_, err = metadata.SetFormatVersion(2)
	assert.NoError(t, err)

	meta, err := metadata.Build()
	assert.NoError(t, err)

	var mockfs internal.MockFS
	mockfs.Test(t)

	table := table.New(
		[]string{"test", "table"},
		meta,
		"test/location",
		func(ctx context.Context) (iceio.IO, error) {
			return &mockfs, nil
		},
		nil,
	)

	txn := table.NewTransaction()

	// Create test data
	mem := memory.DefaultAllocator
	builder := array.NewInt32Builder(mem)
	builder.AppendValues([]int32{1, 2, 3}, nil)
	idArray := builder.NewArray()

	builder2 := array.NewStringBuilder(mem)
	builder2.AppendValues([]string{"a", "b", "c"}, nil)
	dataArray := builder2.NewArray()

	arrowSchema := arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int32, Nullable: false},
		{Name: "data", Type: arrow.BinaryTypes.String, Nullable: true},
	}, nil)

	record := array.NewRecord(arrowSchema, []arrow.Array{idArray, dataArray}, 3)
	tableData := array.NewTableFromRecords(arrowSchema, []arrow.Record{record})

	// Should return an error for an unpartitioned table
	err = txn.DynamicPartitionOverwrite(context.Background(), tableData, 1000, iceberg.Properties{})
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "cannot apply dynamic overwrite on an unpartitioned table")
}

func TestDynamicPartitionOverwrite_NonIdentityTransform(t *testing.T) {
	// Create a table with a non-identity transform
	schema := iceberg.NewSchema(0,
		iceberg.NestedField{ID: 1, Name: "id", Type: iceberg.PrimitiveTypes.Int32, Required: true},
		iceberg.NestedField{ID: 2, Name: "data", Type: iceberg.PrimitiveTypes.String, Required: false},
	)

	partitionSpec := iceberg.NewPartitionSpec(
		iceberg.PartitionField{
			SourceID:  1,
			FieldID:   1000,
			Name:      "id_bucket",
			Transform: iceberg.BucketTransform{NumBuckets: 4},
		},
	)

	metadata, err := table.NewMetadataBuilder()
	assert.NoError(t, err)
	_, err = metadata.AddSchema(schema, 2, true)
	assert.NoError(t, err)
	_, err = metadata.SetCurrentSchemaID(0)
	assert.NoError(t, err)
	_, err = metadata.AddPartitionSpec(&partitionSpec, true)
	assert.NoError(t, err)
	_, err = metadata.SetDefaultSpecID(0)
	assert.NoError(t, err)
	_, err = metadata.SetFormatVersion(2)
	assert.NoError(t, err)

	meta, err := metadata.Build()
	assert.NoError(t, err)

	var mockfs internal.MockFS
	mockfs.Test(t)

	table := table.New(
		[]string{"test", "table"},
		meta,
		"test/location",
		func(ctx context.Context) (iceio.IO, error) {
			return &mockfs, nil
		},
		nil,
	)

	txn := table.NewTransaction()

	// Create test data
	mem := memory.DefaultAllocator
	builder := array.NewInt32Builder(mem)
	builder.AppendValues([]int32{1, 2, 3}, nil)
	idArray := builder.NewArray()

	builder2 := array.NewStringBuilder(mem)
	builder2.AppendValues([]string{"a", "b", "c"}, nil)
	dataArray := builder2.NewArray()

	arrowSchema := arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int32, Nullable: false},
		{Name: "data", Type: arrow.BinaryTypes.String, Nullable: true},
	}, nil)

	record := array.NewRecord(arrowSchema, []arrow.Array{idArray, dataArray}, 3)
	tableData := array.NewTableFromRecords(arrowSchema, []arrow.Record{record})

	// Should return an error for non-identity transform
	err = txn.DynamicPartitionOverwrite(context.Background(), tableData, 1000, iceberg.Properties{})
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "dynamic overwrite currently only supports identity-transform fields in partition spec (limitation, not spec requirement)")
}

func TestDynamicPartitionOverwrite_EmptyTable(t *testing.T) {
	// Create a table with identity transform
	schema := iceberg.NewSchema(0,
		iceberg.NestedField{ID: 1, Name: "id", Type: iceberg.PrimitiveTypes.Int32, Required: true},
		iceberg.NestedField{ID: 2, Name: "data", Type: iceberg.PrimitiveTypes.String, Required: false},
	)

	partitionSpec := iceberg.NewPartitionSpec(
		iceberg.PartitionField{
			SourceID:  1,
			FieldID:   1000,
			Name:      "id",
			Transform: iceberg.IdentityTransform{},
		},
	)

	metadata, err := table.NewMetadataBuilder()
	assert.NoError(t, err)
	_, err = metadata.AddSchema(schema, 2, true)
	assert.NoError(t, err)
	_, err = metadata.SetCurrentSchemaID(0)
	assert.NoError(t, err)
	_, err = metadata.AddPartitionSpec(&partitionSpec, true)
	assert.NoError(t, err)
	_, err = metadata.SetDefaultSpecID(0)
	assert.NoError(t, err)

	_, err = metadata.SetFormatVersion(2)
	assert.NoError(t, err)

	meta, err := metadata.Build()
	assert.NoError(t, err)

	var mockfs internal.MockFS
	mockfs.Test(t)

	tbl := table.New(
		[]string{"test", "table"},
		meta,
		"test/location",
		func(ctx context.Context) (iceio.IO, error) {
			return &mockfs, nil
		},
		nil,
	)

	txn := tbl.NewTransaction()

	// Create empty test data (empty arrays for each field)
	arrowSchema := arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int32, Nullable: false},
		{Name: "data", Type: arrow.BinaryTypes.String, Nullable: true},
	}, nil)
	mem := memory.DefaultAllocator
	idArr := array.NewInt32Builder(mem).NewArray()
	dataArr := array.NewStringBuilder(mem).NewArray()
	record := array.NewRecord(arrowSchema, []arrow.Array{idArr, dataArr}, 0)
	tableData := array.NewTableFromRecords(arrowSchema, []arrow.Record{record})

	// Should return no error for an empty table
	err = txn.DynamicPartitionOverwrite(context.Background(), tableData, 1000, iceberg.Properties{})
	assert.NoError(t, err)
}

// TODO: Find a way to test the happy path of DynamicPartitionOverwrite
