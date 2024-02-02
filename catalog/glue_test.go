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

package catalog

import (
	"context"
	"os"
	"testing"

	"github.com/apache/iceberg-go"
	"github.com/apache/iceberg-go/table"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/glue"
	"github.com/aws/aws-sdk-go-v2/service/glue/types"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

type mockGlueClient struct {
	mock.Mock
}

func (m *mockGlueClient) GetTable(ctx context.Context, params *glue.GetTableInput, optFns ...func(*glue.Options)) (*glue.GetTableOutput, error) {
	args := m.Called(ctx, params, optFns)
	return args.Get(0).(*glue.GetTableOutput), args.Error(1)
}

func (m *mockGlueClient) GetTables(ctx context.Context, params *glue.GetTablesInput, optFns ...func(*glue.Options)) (*glue.GetTablesOutput, error) {
	args := m.Called(ctx, params, optFns)
	return args.Get(0).(*glue.GetTablesOutput), args.Error(1)
}

func (m *mockGlueClient) CreateTable(ctx context.Context, params *glue.CreateTableInput, optFns ...func(*glue.Options)) (*glue.CreateTableOutput, error) {
	args := m.Called(ctx, params, optFns)
	return args.Get(0).(*glue.CreateTableOutput), args.Error(1)
}

func TestGlueGetTable(t *testing.T) {
	assert := require.New(t)

	mockGlueSvc := &mockGlueClient{}

	mockGlueSvc.On("GetTable", mock.Anything, &glue.GetTableInput{
		DatabaseName: aws.String("test_database"),
		Name:         aws.String("test_table"),
	}, mock.Anything).Return(&glue.GetTableOutput{
		Table: &types.Table{
			Parameters: map[string]string{
				"table_type":        "ICEBERG",
				"metadata_location": "s3://test-bucket/test_table/metadata/abc123-123.metadata.json",
			},
		},
	}, nil)

	glueCatalog := &GlueCatalog{
		glueSvc: mockGlueSvc,
	}

	location, err := glueCatalog.getTable(context.TODO(), "test_database", "test_table")
	assert.NoError(err)
	assert.Equal("s3://test-bucket/test_table/metadata/abc123-123.metadata.json", location)
}

func TestGlueListTables(t *testing.T) {
	assert := require.New(t)

	mockGlueSvc := &mockGlueClient{}

	mockGlueSvc.On("GetTables", mock.Anything, &glue.GetTablesInput{
		DatabaseName: aws.String("test_database"),
	}, mock.Anything).Return(&glue.GetTablesOutput{
		TableList: []types.Table{
			{
				Name: aws.String("test_table"),
				Parameters: map[string]string{
					"table_type":        "ICEBERG",
					"metadata_location": "s3://test-bucket/test_table/metadata/abc123-123.metadata.json",
				},
			},
			{
				Name: aws.String("other_table"),
				Parameters: map[string]string{
					"metadata_location": "s3://test-bucket/other_table/",
				},
			},
		},
	}, nil).Once()

	glueCatalog := &GlueCatalog{
		glueSvc: mockGlueSvc,
	}

	tables, err := glueCatalog.ListTables(context.TODO(), GlueDatabaseIdentifier("test_database"))
	assert.NoError(err)
	assert.Len(tables, 1)
	assert.Equal([]string{"test_database", "test_table"}, tables[0])
}

func TestGlueListTableIntegration(t *testing.T) {
	if os.Getenv("TEST_DATABASE_NAME") == "" {
		t.Skip()
	}
	if os.Getenv("TEST_TABLE_NAME") == "" {
		t.Skip()
	}
	assert := require.New(t)

	awscfg, err := config.LoadDefaultConfig(context.TODO(), config.WithClientLogMode(aws.LogRequest|aws.LogResponse))
	assert.NoError(err)

	catalog := NewGlueCatalog(WithAwsConfig(awscfg))

	tables, err := catalog.ListTables(context.TODO(), GlueDatabaseIdentifier(os.Getenv("TEST_DATABASE_NAME")))
	assert.NoError(err)
	assert.Equal([]string{os.Getenv("TEST_DATABASE_NAME"), os.Getenv("TEST_TABLE_NAME")}, tables[1])
}

func TestGlueLoadTableIntegration(t *testing.T) {
	if os.Getenv("TEST_DATABASE_NAME") == "" {
		t.Skip()
	}
	if os.Getenv("TEST_TABLE_NAME") == "" {
		t.Skip()
	}
	if os.Getenv("TEST_TABLE_LOCATION") == "" {
		t.Skip()
	}

	assert := require.New(t)

	awscfg, err := config.LoadDefaultConfig(context.TODO(), config.WithClientLogMode(aws.LogRequest|aws.LogResponse))
	assert.NoError(err)

	catalog := NewGlueCatalog(WithAwsConfig(awscfg))

	table, err := catalog.LoadTable(context.TODO(), []string{os.Getenv("TEST_DATABASE_NAME"), os.Getenv("TEST_TABLE_NAME")}, nil)
	assert.NoError(err)
	assert.Equal([]string{os.Getenv("TEST_TABLE_NAME")}, table.Identifier())
}

func TestGlueCreateTableIntegration(t *testing.T) {
	if os.Getenv("TEST_DATABASE_NAME") == "" {
		t.Skip()
	}
	if os.Getenv("TEST_CREATE_TABLE_NAME") == "" {
		t.Skip()
	}
	if os.Getenv("TEST_CREATE_TABLE_LOCATION") == "" {
		t.Skip()
	}

	assert := require.New(t)

	location := os.Getenv("TEST_CREATE_TABLE_LOCATION")

	schema := iceberg.NewSchemaWithIdentifiers(1, []int{},
		iceberg.NestedField{
			ID: 1, Name: "vendor_id", Type: iceberg.PrimitiveTypes.String},
		iceberg.NestedField{
			ID: 2, Name: "name", Type: iceberg.PrimitiveTypes.String},
		iceberg.NestedField{
			ID: 3, Name: "datetime", Type: iceberg.PrimitiveTypes.TimestampTz})
	partSpec := iceberg.NewPartitionSpec(iceberg.PartitionField{
		SourceID: 3, FieldID: 1000, Name: "datetime", Transform: iceberg.DayTransform{}})

	props := map[string]string{
		"write.target-file-size-bytes": "536870912",
		"write.format.default":         "parquet",
	}

	awscfg, err := config.LoadDefaultConfig(context.TODO(), config.WithClientLogMode(aws.LogRequest|aws.LogResponse))
	assert.NoError(err)

	catalog := NewGlueCatalog(WithAwsConfig(awscfg))

	table, err := catalog.CreateTable(context.TODO(),
		[]string{os.Getenv("TEST_DATABASE_NAME"), os.Getenv("TEST_CREATE_TABLE_NAME")}, schema, partSpec, table.UnsortedSortOrder, location, props)
	assert.NoError(err)
	assert.Equal([]string{os.Getenv("TEST_DATABASE_NAME"), os.Getenv("TEST_CREATE_TABLE_NAME")}, table.Identifier())
}
