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

package glue

import (
	"context"
	"errors"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/apache/iceberg-go"
	"github.com/apache/iceberg-go/catalog"
	"github.com/apache/iceberg-go/table"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/glue"
	"github.com/aws/aws-sdk-go-v2/service/glue/types"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

type mockGlueClient struct {
	mock.Mock
}

func (m *mockGlueClient) CreateTable(ctx context.Context, params *glue.CreateTableInput, optFns ...func(*glue.Options)) (*glue.CreateTableOutput, error) {
	args := m.Called(ctx, params, optFns)

	return args.Get(0).(*glue.CreateTableOutput), args.Error(1)
}

func (m *mockGlueClient) GetTable(ctx context.Context, params *glue.GetTableInput, optFns ...func(*glue.Options)) (*glue.GetTableOutput, error) {
	args := m.Called(ctx, params, optFns)

	return args.Get(0).(*glue.GetTableOutput), args.Error(1)
}

func (m *mockGlueClient) GetTables(ctx context.Context, params *glue.GetTablesInput, optFns ...func(*glue.Options)) (*glue.GetTablesOutput, error) {
	args := m.Called(ctx, params, optFns)

	return args.Get(0).(*glue.GetTablesOutput), args.Error(1)
}

func (m *mockGlueClient) DeleteTable(ctx context.Context, params *glue.DeleteTableInput, optFns ...func(*glue.Options)) (*glue.DeleteTableOutput, error) {
	args := m.Called(ctx, params, optFns)

	return args.Get(0).(*glue.DeleteTableOutput), args.Error(1)
}

func (m *mockGlueClient) GetDatabase(ctx context.Context, params *glue.GetDatabaseInput, optFns ...func(*glue.Options)) (*glue.GetDatabaseOutput, error) {
	args := m.Called(ctx, params, optFns)

	return args.Get(0).(*glue.GetDatabaseOutput), args.Error(1)
}

func (m *mockGlueClient) GetDatabases(ctx context.Context, params *glue.GetDatabasesInput, optFns ...func(*glue.Options)) (*glue.GetDatabasesOutput, error) {
	args := m.Called(ctx, params, optFns)

	return args.Get(0).(*glue.GetDatabasesOutput), args.Error(1)
}

func (m *mockGlueClient) CreateDatabase(ctx context.Context, params *glue.CreateDatabaseInput, optFns ...func(*glue.Options)) (*glue.CreateDatabaseOutput, error) {
	args := m.Called(ctx, params, optFns)

	return args.Get(0).(*glue.CreateDatabaseOutput), args.Error(1)
}

func (m *mockGlueClient) DeleteDatabase(ctx context.Context, params *glue.DeleteDatabaseInput, optFns ...func(*glue.Options)) (*glue.DeleteDatabaseOutput, error) {
	args := m.Called(ctx, params, optFns)

	return args.Get(0).(*glue.DeleteDatabaseOutput), args.Error(1)
}

func (m *mockGlueClient) UpdateDatabase(ctx context.Context, params *glue.UpdateDatabaseInput, optFns ...func(*glue.Options)) (*glue.UpdateDatabaseOutput, error) {
	args := m.Called(ctx, params, optFns)

	return args.Get(0).(*glue.UpdateDatabaseOutput), args.Error(1)
}

var testIcebergGlueTable1 = types.Table{
	Name: aws.String("test_table"),
	Parameters: map[string]string{
		tableTypePropsKey:        "ICEBERG",
		metadataLocationPropsKey: "s3://test-bucket/test_table/metadata/abc123-123.metadata.json",
	},
}

var testIcebergGlueTable2 = types.Table{
	Name: aws.String("test_table2"),
	Parameters: map[string]string{
		tableTypePropsKey:        "ICEBERG",
		metadataLocationPropsKey: "s3://test-bucket/test_table/metadata/abc456-456.metadata.json",
	},
}

var testIcebergGlueTable3 = types.Table{
	Name: aws.String("test_table3"),
	Parameters: map[string]string{
		tableTypePropsKey:        "ICEBERG",
		metadataLocationPropsKey: "s3://test-bucket/test_table/metadata/abc789-789.metadata.json",
	},
}

var testIcebergGlueTable4 = types.Table{
	Name: aws.String("test_table4"),
	Parameters: map[string]string{
		tableTypePropsKey:        "ICEBERG",
		metadataLocationPropsKey: "s3://test-bucket/test_table/metadata/abc123-789.metadata.json",
	},
}

var testIcebergGlueTable5 = types.Table{
	Name: aws.String("test_table5"),
	Parameters: map[string]string{
		tableTypePropsKey:        "ICEBERG",
		metadataLocationPropsKey: "s3://test-bucket/test_table/metadata/abc12345-789.metadata.json",
	},
}

var testNonIcebergGlueTable = types.Table{
	Name: aws.String("other_table"),
	Parameters: map[string]string{
		metadataLocationPropsKey: "s3://test-bucket/other_table/",
	},
}

func TestGlueGetTable(t *testing.T) {
	assert := require.New(t)

	mockGlueSvc := &mockGlueClient{}

	mockGlueSvc.On("GetTable", mock.Anything, &glue.GetTableInput{
		DatabaseName: aws.String("test_database"),
		Name:         aws.String("test_table"),
	}, mock.Anything).Return(&glue.GetTableOutput{Table: &testIcebergGlueTable1}, nil)

	glueCatalog := &Catalog{
		glueSvc: mockGlueSvc,
	}

	tbl, err := glueCatalog.getTable(context.TODO(), "test_database", "test_table")
	assert.NoError(err)
	assert.Equal("s3://test-bucket/test_table/metadata/abc123-123.metadata.json", tbl.Parameters[metadataLocationPropsKey])
}

func TestGlueListTables(t *testing.T) {
	assert := require.New(t)

	mockGlueSvc := &mockGlueClient{}

	mockGlueSvc.On("GetTables", mock.Anything, &glue.GetTablesInput{
		DatabaseName: aws.String("test_database"),
	}, mock.Anything).Return(&glue.GetTablesOutput{
		TableList: []types.Table{testIcebergGlueTable1, testNonIcebergGlueTable},
	}, nil).Once()

	glueCatalog := &Catalog{
		glueSvc: mockGlueSvc,
	}

	var lastErr error
	tbls := make([]table.Identifier, 0)
	iter := glueCatalog.ListTables(context.TODO(), DatabaseIdentifier("test_database"))

	for tbl, err := range iter {
		tbls = append(tbls, tbl)
		if err != nil {
			lastErr = err
		}
	}
	assert.NoError(lastErr)
	assert.Len(tbls, 1)
	assert.Equal([]string{"test_database", "test_table"}, tbls[0])
}

func TestGlueListTablesPagination(t *testing.T) {
	assert := require.New(t)

	mockGlueSvc := &mockGlueClient{}

	// First page
	mockGlueSvc.On("GetTables", mock.Anything, &glue.GetTablesInput{
		DatabaseName: aws.String("test_database"),
	}, mock.Anything).Return(&glue.GetTablesOutput{
		TableList: []types.Table{
			testIcebergGlueTable1,
			testIcebergGlueTable2,
		},
		NextToken: aws.String("token1"),
	}, nil).Once()

	// Second page
	mockGlueSvc.On("GetTables", mock.Anything, &glue.GetTablesInput{
		DatabaseName: aws.String("test_database"),
		NextToken:    aws.String("token1"),
	}, mock.Anything).Return(&glue.GetTablesOutput{
		TableList: []types.Table{
			testIcebergGlueTable3,
			testIcebergGlueTable4,
		},
		NextToken: aws.String("token2"),
	}, nil).Once()

	// Third page
	mockGlueSvc.On("GetTables", mock.Anything, &glue.GetTablesInput{
		DatabaseName: aws.String("test_database"),
		NextToken:    aws.String("token2"),
	}, mock.Anything).Return(&glue.GetTablesOutput{
		TableList: []types.Table{
			testIcebergGlueTable5,
			testNonIcebergGlueTable,
		},
	}, nil).Once()

	glueCatalog := &Catalog{
		glueSvc: mockGlueSvc,
	}

	var lastErr error
	tbls := make([]table.Identifier, 0)
	iter := glueCatalog.ListTables(context.TODO(), DatabaseIdentifier("test_database"))

	for tbl, err := range iter {
		tbls = append(tbls, tbl)
		if err != nil {
			lastErr = err
		}
	}

	assert.NoError(lastErr)
	assert.Len(tbls, 5) // Only Iceberg tables should be included
	assert.Equal([]string{"test_database", "test_table"}, tbls[0])
	assert.Equal([]string{"test_database", "test_table2"}, tbls[1])
	assert.Equal([]string{"test_database", "test_table3"}, tbls[2])
	assert.Equal([]string{"test_database", "test_table4"}, tbls[3])
	assert.Equal([]string{"test_database", "test_table5"}, tbls[4])

	mockGlueSvc.AssertExpectations(t)
}

func TestGlueListTablesError(t *testing.T) {
	assert := require.New(t)

	mockGlueSvc := &mockGlueClient{}

	// First page succeeds
	mockGlueSvc.On("GetTables", mock.Anything, &glue.GetTablesInput{
		DatabaseName: aws.String("test_database"),
	}, mock.Anything).Return(&glue.GetTablesOutput{
		TableList: []types.Table{
			testIcebergGlueTable1,
		},
		NextToken: aws.String("token1"),
	}, nil).Once()

	mockGlueSvc.On("GetTables", mock.Anything, &glue.GetTablesInput{
		DatabaseName: aws.String("test_database"),
		NextToken:    aws.String("token1"),
	}, mock.Anything).Return(&glue.GetTablesOutput{}, errors.New("token expired")).Once()

	glueCatalog := &Catalog{
		glueSvc: mockGlueSvc,
	}

	var lastErr error
	tbls := make([]table.Identifier, 0)
	iter := glueCatalog.ListTables(context.TODO(), DatabaseIdentifier("test_database"))

	for tbl, err := range iter {
		if err != nil {
			lastErr = err

			break
		}
		tbls = append(tbls, tbl)
	}

	assert.Error(lastErr)
	assert.Contains(lastErr.Error(), "token expired")
	assert.Len(tbls, 1)
	assert.Equal([]string{"test_database", "test_table"}, tbls[0])

	mockGlueSvc.AssertExpectations(t)
}

func TestGlueListNamespaces(t *testing.T) {
	assert := require.New(t)

	mockGlueSvc := &mockGlueClient{}

	mockGlueSvc.On("GetDatabases", mock.Anything, &glue.GetDatabasesInput{}, mock.Anything).Return(&glue.GetDatabasesOutput{
		DatabaseList: []types.Database{
			{
				Name: aws.String("test_database"),
				Parameters: map[string]string{
					"database_type": "ICEBERG",
				},
			},
			{
				Name:       aws.String("other_database"),
				Parameters: map[string]string{},
			},
		},
	}, nil).Once()

	glueCatalog := &Catalog{
		glueSvc: mockGlueSvc,
	}

	databases, err := glueCatalog.ListNamespaces(context.TODO(), nil)
	assert.NoError(err)
	assert.Len(databases, 1)
	assert.Equal([]string{"test_database"}, databases[0])
}

func TestGlueDropTable(t *testing.T) {
	assert := require.New(t)

	mockGlueSvc := &mockGlueClient{}

	mockGlueSvc.On("GetTable", mock.Anything, &glue.GetTableInput{
		DatabaseName: aws.String("test_database"),
		Name:         aws.String("test_table"),
	}, mock.Anything).Return(&glue.GetTableOutput{
		Table: &testIcebergGlueTable1,
	}, nil).Once()

	mockGlueSvc.On("DeleteTable", mock.Anything, &glue.DeleteTableInput{
		DatabaseName: aws.String("test_database"),
		Name:         aws.String("test_table"),
	}, mock.Anything).Return(&glue.DeleteTableOutput{}, nil).Once()

	glueCatalog := &Catalog{
		glueSvc: mockGlueSvc,
	}

	err := glueCatalog.DropTable(context.TODO(), TableIdentifier("test_database", "test_table"))
	assert.NoError(err)
}

func TestGlueCreateNamespace(t *testing.T) {
	assert := require.New(t)

	mockGlueSvc := &mockGlueClient{}

	mockGlueSvc.On("CreateDatabase", mock.Anything, &glue.CreateDatabaseInput{
		DatabaseInput: &types.DatabaseInput{
			Name: aws.String("test_namespace"),
			Parameters: map[string]string{
				databaseTypePropsKey: glueTypeIceberg,
				descriptionPropsKey:  "Test Description",
				locationPropsKey:     "s3://test-location",
			},
		},
	}, mock.Anything).Return(&glue.CreateDatabaseOutput{}, nil).Once()

	glueCatalog := &Catalog{
		glueSvc: mockGlueSvc,
	}

	props := map[string]string{
		descriptionPropsKey: "Test Description",
		locationPropsKey:    "s3://test-location",
	}

	err := glueCatalog.CreateNamespace(context.TODO(), DatabaseIdentifier("test_namespace"), props)
	assert.NoError(err)
}

func TestGlueDropNamespace(t *testing.T) {
	assert := require.New(t)

	mockGlueSvc := &mockGlueClient{}

	mockGlueSvc.On("GetDatabase", mock.Anything, &glue.GetDatabaseInput{
		Name: aws.String("test_namespace"),
	}, mock.Anything).Return(&glue.GetDatabaseOutput{
		Database: &types.Database{
			Name: aws.String("test_namespace"),
			Parameters: map[string]string{
				"database_type": "ICEBERG",
			},
		},
	}, nil).Once()

	mockGlueSvc.On("DeleteDatabase", mock.Anything, &glue.DeleteDatabaseInput{
		Name: aws.String("test_namespace"),
	}, mock.Anything).Return(&glue.DeleteDatabaseOutput{}, nil).Once()

	glueCatalog := &Catalog{
		glueSvc: mockGlueSvc,
	}

	err := glueCatalog.DropNamespace(context.TODO(), DatabaseIdentifier("test_namespace"))
	assert.NoError(err)
}

func TestGlueCheckNamespaceExists(t *testing.T) {
	assert := require.New(t)
	mockGlueSvc := &mockGlueClient{}
	mockGlueSvc.On("GetDatabase", mock.Anything, &glue.GetDatabaseInput{
		Name: aws.String("test_namespace"),
	}, mock.Anything).Return(&glue.GetDatabaseOutput{
		Database: &types.Database{
			Name: aws.String("test_namespace"),
			Parameters: map[string]string{
				"database_type": "ICEBERG",
			},
		},
	}, nil).Once()
	glueCatalog := &Catalog{
		glueSvc: mockGlueSvc,
	}
	exists, err := glueCatalog.CheckNamespaceExists(context.TODO(), DatabaseIdentifier("test_namespace"))
	assert.NoError(err)
	assert.True(exists)
}

func TestGlueCheckNamespaceNotExists(t *testing.T) {
	assert := require.New(t)
	mockGlueSvc := &mockGlueClient{}

	mockGlueSvc.On("GetDatabase", mock.Anything, &glue.GetDatabaseInput{
		Name: aws.String("nonexistent_namespace"),
	}, mock.Anything).Return(&glue.GetDatabaseOutput{},
		&types.EntityNotFoundException{Message: aws.String("Database not found")}).Once()

	glueCatalog := &Catalog{
		glueSvc: mockGlueSvc,
	}

	exists, err := glueCatalog.CheckNamespaceExists(context.TODO(), DatabaseIdentifier("nonexistent_namespace"))
	assert.Error(err)
	assert.False(exists)
	assert.ErrorContains(err, "Database not found")
}

func TestGlueUpdateNamespaceProperties(t *testing.T) {
	tests := []struct {
		name        string
		initial     map[string]string
		updates     map[string]string
		removals    []string
		expected    catalog.PropertiesUpdateSummary
		shouldError bool
	}{
		{
			name: "Overlapping removals and updates",
			initial: map[string]string{
				"key1": "value1",
				"key2": "value2",
			},
			updates: map[string]string{
				"key1": "new_value1",
				"key3": "value3",
			},
			removals:    []string{"key1"},
			shouldError: true,
		},
		{
			name: "Some keys in removals are missing",
			initial: map[string]string{
				"key1": "value1",
				"key2": "value2",
			},
			updates: map[string]string{
				"key3": "value3",
			},
			removals: []string{"key4"},
			expected: catalog.PropertiesUpdateSummary{
				Removed: []string{},
				Updated: []string{"key3"},
				Missing: []string{"key4"},
			},
			shouldError: false,
		},
		{
			name: "No changes to some properties",
			initial: map[string]string{
				"key1": "value1",
				"key2": "value2",
			},
			updates: map[string]string{
				"key1": "value1",
				"key3": "value3",
			},
			removals: []string{},
			expected: catalog.PropertiesUpdateSummary{
				Removed: []string{},
				Updated: []string{"key3"},
				Missing: []string{},
			},
			shouldError: false,
		},
		{
			name: "Happy path with updates and removals",
			initial: map[string]string{
				"key1": "value1",
				"key2": "value2",
				"key4": "value4",
			},
			updates: map[string]string{
				"key2": "new_value2",
			},
			removals: []string{"key4"},
			expected: catalog.PropertiesUpdateSummary{
				Removed: []string{"key4"},
				Updated: []string{"key2"},
				Missing: []string{},
			},
			shouldError: false,
		},
		{
			name: "Happy path with only updates",
			initial: map[string]string{
				"key1": "value1",
				"key2": "value2",
			},
			updates: map[string]string{
				"key2": "new_value2",
			},
			removals: []string{},
			expected: catalog.PropertiesUpdateSummary{
				Removed: []string{},
				Updated: []string{"key2"},
				Missing: []string{},
			},
			shouldError: false,
		},
		{
			name: "Happy path with only removals",
			initial: map[string]string{
				"key1": "value1",
				"key2": "value2",
				"key3": "value3",
			},
			updates:  map[string]string{},
			removals: []string{"key2", "key3"},
			expected: catalog.PropertiesUpdateSummary{
				Removed: []string{"key2", "key3"},
				Updated: []string{},
				Missing: []string{},
			},
			shouldError: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert := require.New(t)

			mockGlueSvc := &mockGlueClient{}

			tt.initial[databaseTypePropsKey] = glueTypeIceberg

			mockGlueSvc.On("GetDatabase", mock.Anything, &glue.GetDatabaseInput{
				Name: aws.String("test_namespace"),
			}, mock.Anything).Return(&glue.GetDatabaseOutput{
				Database: &types.Database{
					Name:       aws.String("test_namespace"),
					Parameters: tt.initial,
				},
			}, nil).Once()

			if !tt.shouldError {
				mockGlueSvc.On("UpdateDatabase", mock.Anything, mock.Anything, mock.Anything).Return(&glue.UpdateDatabaseOutput{}, nil).Once()
			}

			glueCatalog := &Catalog{
				glueSvc: mockGlueSvc,
			}

			summary, err := glueCatalog.UpdateNamespaceProperties(context.TODO(), DatabaseIdentifier("test_namespace"), tt.removals, tt.updates)
			if tt.shouldError {
				assert.Error(err)
			} else {
				assert.NoError(err)
				assert.EqualValues(tt.expected.Removed, summary.Removed)
				assert.EqualValues(tt.expected.Updated, summary.Updated)
				assert.EqualValues(tt.expected.Missing, summary.Missing)
			}
		})
	}
}

func TestGlueRenameTable(t *testing.T) {
	t.Skip("Skipping this test temporarily because LoadTable is not testable due to the dependency on the IO.")

	assert := require.New(t)

	mockGlueSvc := &mockGlueClient{}

	// Mock GetTable response
	mockGlueSvc.On("GetTable", mock.Anything, &glue.GetTableInput{
		DatabaseName: aws.String("test_database"),
		Name:         aws.String("test_table"),
	}, mock.Anything).Return(&glue.GetTableOutput{
		Table: &types.Table{
			Name: aws.String("test_table"),
			Parameters: map[string]string{
				tableTypePropsKey: glueTypeIceberg,
			},
			Owner:             aws.String("owner"),
			Description:       aws.String("description"),
			StorageDescriptor: &types.StorageDescriptor{},
		},
	}, nil).Once()

	mockGlueSvc.On("GetTable", mock.Anything, &glue.GetTableInput{
		DatabaseName: aws.String("test_database"),
		Name:         aws.String("new_test_table"),
	}, mock.Anything).Return(&glue.GetTableOutput{
		Table: &types.Table{
			Name: aws.String("new_test_table"),
			Parameters: map[string]string{
				tableTypePropsKey:        glueTypeIceberg,
				metadataLocationPropsKey: "s3://test-bucket/new_test_table/metadata/abc123-123.metadata.json",
			},
			Owner:             aws.String("owner"),
			Description:       aws.String("description"),
			StorageDescriptor: &types.StorageDescriptor{},
		},
	}, nil).Once()

	// Mock CreateTable response
	mockGlueSvc.On("CreateTable", mock.Anything, &glue.CreateTableInput{
		DatabaseName: aws.String("test_database"),
		TableInput: &types.TableInput{
			Name:              aws.String("new_test_table"),
			Owner:             aws.String("owner"),
			Description:       aws.String("description"),
			Parameters:        map[string]string{tableTypePropsKey: glueTypeIceberg},
			StorageDescriptor: &types.StorageDescriptor{},
		},
	}, mock.Anything).Return(&glue.CreateTableOutput{}, nil).Once()

	// Mock DeleteTable response for old table
	mockGlueSvc.On("DeleteTable", mock.Anything, &glue.DeleteTableInput{
		DatabaseName: aws.String("test_database"),
		Name:         aws.String("test_table"),
	}, mock.Anything).Return(&glue.DeleteTableOutput{}, nil).Once()

	glueCatalog := &Catalog{
		glueSvc: mockGlueSvc,
	}

	renamedTable, err := glueCatalog.RenameTable(context.TODO(), TableIdentifier("test_database", "test_table"), TableIdentifier("test_database", "new_test_table"))
	assert.NoError(err)
	assert.Equal("new_test_table", renamedTable.Identifier()[1])
}

func TestGlueRenameTable_DeleteTableFailureRollback(t *testing.T) {
	assert := require.New(t)

	mockGlueSvc := &mockGlueClient{}

	// Mock GetTable response
	mockGlueSvc.On("GetTable", mock.Anything, &glue.GetTableInput{
		DatabaseName: aws.String("test_database"),
		Name:         aws.String("test_table"),
	}, mock.Anything).Return(&glue.GetTableOutput{
		Table: &types.Table{
			Name: aws.String("test_table"),
			Parameters: map[string]string{
				tableTypePropsKey: glueTypeIceberg,
			},
			Owner:             aws.String("owner"),
			Description:       aws.String("description"),
			StorageDescriptor: &types.StorageDescriptor{},
		},
	}, nil).Once()

	// Mock CreateTable response
	mockGlueSvc.On("CreateTable", mock.Anything, &glue.CreateTableInput{
		DatabaseName: aws.String("test_database"),
		TableInput: &types.TableInput{
			Name:              aws.String("new_test_table"),
			Owner:             aws.String("owner"),
			Description:       aws.String("description"),
			Parameters:        map[string]string{tableTypePropsKey: glueTypeIceberg},
			StorageDescriptor: &types.StorageDescriptor{},
		},
	}, mock.Anything).Return(&glue.CreateTableOutput{}, nil).Once()

	// Mock DeleteTable response for old table (fail)
	mockGlueSvc.On("DeleteTable", mock.Anything, &glue.DeleteTableInput{
		DatabaseName: aws.String("test_database"),
		Name:         aws.String("test_table"),
	}, mock.Anything).Return(&glue.DeleteTableOutput{}, errors.New("delete table failed")).Once()

	// Mock DeleteTable response for rollback (new table)
	mockGlueSvc.On("DeleteTable", mock.Anything, &glue.DeleteTableInput{
		DatabaseName: aws.String("test_database"),
		Name:         aws.String("new_test_table"),
	}, mock.Anything).Return(&glue.DeleteTableOutput{}, nil).Once()

	glueCatalog := &Catalog{
		glueSvc: mockGlueSvc,
	}

	renamedTable, err := glueCatalog.RenameTable(context.TODO(), TableIdentifier("test_database", "test_table"), TableIdentifier("test_database", "new_test_table"))
	assert.Error(err)
	assert.Nil(renamedTable)
	mockGlueSvc.AssertCalled(t, "DeleteTable", mock.Anything, &glue.DeleteTableInput{
		DatabaseName: aws.String("test_database"),
		Name:         aws.String("new_test_table"),
	}, mock.Anything)
}

func TestGlueListTablesIntegration(t *testing.T) {
	if os.Getenv("TEST_DATABASE_NAME") == "" {
		t.Skip()
	}
	if os.Getenv("TEST_TABLE_NAME") == "" {
		t.Skip()
	}
	assert := require.New(t)

	awsCfg, err := config.LoadDefaultConfig(context.TODO(), config.WithClientLogMode(aws.LogRequest|aws.LogResponse))
	assert.NoError(err)

	ctlg := NewCatalog(WithAwsConfig(awsCfg))

	iter := ctlg.ListTables(context.TODO(), DatabaseIdentifier(os.Getenv("TEST_DATABASE_NAME")))
	var lastErr error
	tbls := make([]table.Identifier, 0)
	for tbl, err := range iter {
		tbls = append(tbls, tbl)
		if err != nil {
			lastErr = err
		}
	}

	assert.NoError(lastErr)
	assert.Equal([]string{os.Getenv("TEST_DATABASE_NAME"), os.Getenv("TEST_TABLE_NAME")}, tbls[1])
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

	awsCfg, err := config.LoadDefaultConfig(context.TODO(), config.WithClientLogMode(aws.LogRequest|aws.LogResponse))
	assert.NoError(err)

	ctlg := NewCatalog(WithAwsConfig(awsCfg))

	tbl, err := ctlg.LoadTable(context.TODO(), []string{os.Getenv("TEST_DATABASE_NAME"), os.Getenv("TEST_TABLE_NAME")}, nil)
	assert.NoError(err)
	assert.Equal([]string{os.Getenv("TEST_TABLE_NAME")}, tbl.Identifier())
}

func TestGlueListNamespacesIntegration(t *testing.T) {
	if os.Getenv("TEST_DATABASE_NAME") == "" {
		t.Skip()
	}
	assert := require.New(t)

	awsCfg, err := config.LoadDefaultConfig(context.TODO(), config.WithClientLogMode(aws.LogRequest|aws.LogResponse))
	assert.NoError(err)

	ctlg := NewCatalog(WithAwsConfig(awsCfg))

	namespaces, err := ctlg.ListNamespaces(context.TODO(), nil)
	assert.NoError(err)
	assert.Contains(namespaces, []string{os.Getenv("TEST_DATABASE_NAME")})
}

func TestGlueCreateTableSuccessIntegration(t *testing.T) {
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
	sourceTableName := os.Getenv("TEST_TABLE_NAME")
	dbName := os.Getenv("TEST_DATABASE_NAME")
	metadataLocation := os.Getenv("TEST_TABLE_LOCATION")
	awsCfg, err := config.LoadDefaultConfig(context.TODO(), config.WithClientLogMode(aws.LogRequest|aws.LogResponse))
	assert.NoError(err)
	ctlg := NewCatalog(WithAwsConfig(awsCfg))
	sourceTable, err := ctlg.LoadTable(context.TODO(), []string{dbName, sourceTableName}, nil)
	assert.NoError(err)
	assert.Equal([]string{sourceTableName}, sourceTable.Identifier())
	newTableName := fmt.Sprintf("%d_%s", time.Now().UnixNano(), sourceTableName)
	createOpts := []catalog.CreateTableOpt{
		catalog.WithLocation(metadataLocation),
	}
	newTable, err := ctlg.CreateTable(context.TODO(), TableIdentifier(dbName, newTableName), sourceTable.Schema(), createOpts...)
	defer func() {
		cleanupErr := ctlg.DropTable(context.TODO(), TableIdentifier(dbName, newTableName))
		if cleanupErr != nil {
			t.Logf("Warning: Failed to clean up table %s.%s: %v", dbName, newTableName, cleanupErr)
		}
		// Clean up the newly created metadata file in S3
		if newTable != nil {
			s3Client := s3.NewFromConfig(awsCfg)
			metadataLoc := newTable.MetadataLocation()
			if strings.HasPrefix(metadataLoc, "s3://") {
				parts := strings.SplitN(strings.TrimPrefix(metadataLoc, "s3://"), "/", 2)
				if len(parts) == 2 {
					bucket := parts[0]
					key := parts[1]
					_, deleteErr := s3Client.DeleteObject(context.TODO(), &s3.DeleteObjectInput{
						Bucket: aws.String(bucket),
						Key:    aws.String(key),
					})
					if deleteErr != nil {
						t.Logf("Warning: Failed to delete metadata file at %s: %v", metadataLoc, deleteErr)
					}
				}
			}
		}
	}()
	assert.True(false)
	assert.NoError(err)
	assert.Equal([]string{newTableName}, newTable.Identifier())

	tableNewLoaded, err := ctlg.LoadTable(context.TODO(), []string{dbName, newTableName}, nil)
	assert.NoError(err)
	assert.Equal([]string{newTableName}, tableNewLoaded.Identifier())
	assert.Equal(sourceTable.Schema().Fields(), tableNewLoaded.Schema().Fields())
	assert.Contains(tableNewLoaded.MetadataLocation(), metadataLocation)

	glueClient := glue.NewFromConfig(awsCfg)
	tableResponse, err := glueClient.GetTable(context.TODO(), &glue.GetTableInput{
		DatabaseName: aws.String(dbName),
		Name:         aws.String(newTableName),
	})
	assert.NoError(err)
	assert.Equal("EXTERNAL_TABLE", aws.ToString(tableResponse.Table.TableType))
	assert.Equal(glueTypeIceberg, tableResponse.Table.Parameters[tableTypePropsKey])
}

func TestGlueCreateTableInvalidMetadataRollback(t *testing.T) {
	if os.Getenv("TEST_DATABASE_NAME") == "" {
		t.Skip()
	}
	if os.Getenv("TEST_TABLE_NAME") == "" {
		t.Skip()
	}
	assert := require.New(t)
	// Use a non-existent S3 location for metadata
	invalidMetadataLocation := "s3://nonexistent-test-bucket"
	dbName := os.Getenv("TEST_DATABASE_NAME")
	sourceTableName := os.Getenv("TEST_TABLE_NAME")
	awsCfg, err := config.LoadDefaultConfig(context.TODO(), config.WithClientLogMode(aws.LogRequest|aws.LogResponse))
	assert.NoError(err)
	ctlg := NewCatalog(WithAwsConfig(awsCfg))
	sourceTable, err := ctlg.LoadTable(context.TODO(), []string{dbName, sourceTableName}, nil)
	assert.NoError(err)
	newTableName := fmt.Sprintf("%d_%s", time.Now().UnixNano(), sourceTableName)
	createOpts := []catalog.CreateTableOpt{
		catalog.WithLocation(invalidMetadataLocation),
	}
	_, err = ctlg.CreateTable(context.TODO(), TableIdentifier(dbName, newTableName), sourceTable.Schema(), createOpts...)
	assert.Error(err, "expected error when creating table with invalid metadata location")
	_, err = ctlg.LoadTable(context.TODO(), []string{dbName, newTableName}, nil)
	assert.Error(err, "expected table to not exist after failed creation")
	assert.True(strings.Contains(err.Error(), "EntityNotFoundException: Entity Not Found"), "expected EntityNotFoundException error")
	// Verify that the table was not left in the catalog
	tablesIter := ctlg.ListTables(context.TODO(), DatabaseIdentifier(dbName)) // TODO: Implement CheckTableExists
	found := false
	for tbl, err := range tablesIter {
		if tbl[1] == newTableName {
			found = true

			break
		}
		assert.NoError(err)
	}
	assert.False(found, "expected table to be rolled back and not exist in the catalog")
}

func TestGlueCreateTableRollbackOnInvalidMetadata(t *testing.T) {
	assert := require.New(t)
	mockGlueSvc := &mockGlueClient{}
	schema := iceberg.NewSchemaWithIdentifiers(1, []int{1},
		iceberg.NestedField{ID: 1, Name: "id", Type: iceberg.Int64Type{}, Required: true},
		iceberg.NestedField{ID: 2, Name: "name", Type: iceberg.StringType{}, Required: true},
	)
	mockGlueSvc.On("CreateTable", mock.Anything, mock.Anything, mock.Anything).Return(&glue.CreateTableOutput{}, nil)
	mockGlueSvc.On("GetTable", mock.Anything, &glue.GetTableInput{
		DatabaseName: aws.String("test_database"),
		Name:         aws.String("test_rollback_table"),
	}, mock.Anything).Return(&glue.GetTableOutput{
		Table: &types.Table{
			Name: aws.String("test_rollback_table"),
		},
	}, nil)
	mockGlueSvc.On("DeleteTable", mock.Anything, &glue.DeleteTableInput{
		DatabaseName: aws.String("test_database"),
		Name:         aws.String("test_rollback_table"),
	}, mock.Anything).Return(&glue.DeleteTableOutput{}, nil)
	glueCatalog := &Catalog{
		glueSvc: mockGlueSvc,
		awsCfg:  &aws.Config{},
	}
	_, err := glueCatalog.CreateTable(context.TODO(),
		TableIdentifier("test_database", "test_rollback_table"),
		schema,
		catalog.WithLocation("s3://non-existent-test-bucket"))
	// Should fail because LoadTable will fail to load the nonexistent metadata
	assert.Error(err)
	assert.Contains(err.Error(), "failed to create table")
	mockGlueSvc.AssertCalled(t, "CreateTable", mock.Anything, mock.Anything, mock.Anything)
	mockGlueSvc.AssertCalled(t, "DeleteTable", mock.Anything, mock.Anything, mock.Anything)
	mockGlueSvc.AssertCalled(t, "GetTable", mock.Anything, mock.Anything, mock.Anything)
}

func TestRegisterTableMetadataNotFound(t *testing.T) {
	assert := require.New(t)
	mockGlueSvc := &mockGlueClient{}
	awsCfg, err := config.LoadDefaultConfig(context.TODO())
	assert.NoError(err)
	cat := &Catalog{
		glueSvc: mockGlueSvc,
		awsCfg:  &awsCfg,
	}
	_, err = cat.RegisterTable(context.Background(), catalog.ToIdentifier("test_db", "test_table"), "s3://nonexistent-bucket/metadata/metadata.json")
	assert.Error(err)
	assert.Contains(err.Error(), "failed to read table metadata from s3://nonexistent-bucket/metadata/metadata.json")
}

func TestRegisterTableIntegration(t *testing.T) {
	if os.Getenv("TEST_DATABASE_NAME") == "" {
		t.Skip()
	}
	if os.Getenv("TEST_TABLE_LOCATION") == "" {
		t.Skip()
	}
	assert := require.New(t)
	dbName := os.Getenv("TEST_DATABASE_NAME")
	tableName := "test_register_table_integration"
	// Metadata location example s3://test-bucket/metadata/0000-0000-0000.metadata.json
	metadataLocation := os.Getenv("TEST_TABLE_LOCATION")

	awsCfg, err := config.LoadDefaultConfig(context.TODO(), config.WithClientLogMode(aws.LogRequest|aws.LogResponse))
	assert.NoError(err)
	ctlg := NewCatalog(WithAwsConfig(awsCfg))

	// Drop table if it exists
	_ = ctlg.DropTable(context.TODO(), TableIdentifier(dbName, tableName))

	tbl, err := ctlg.RegisterTable(context.TODO(), TableIdentifier(dbName, tableName), metadataLocation)
	defer func() {
		err = ctlg.DropTable(context.TODO(), TableIdentifier(dbName, tableName))
		assert.NoError(err)
	}()
	assert.NoError(err)
	assert.Equal([]string{tableName}, tbl.Identifier())
	assert.Equal(metadataLocation, tbl.MetadataLocation())
}
