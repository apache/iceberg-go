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
	"errors"
	"os"
	"testing"

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

var testIcebergGlueTable = types.Table{
	Name: aws.String("test_table"),
	Parameters: map[string]string{
		tableTypePropsKey:        "ICEBERG",
		metadataLocationPropsKey: "s3://test-bucket/test_table/metadata/abc123-123.metadata.json",
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
	}, mock.Anything).Return(&glue.GetTableOutput{Table: &testIcebergGlueTable}, nil)

	glueCatalog := &GlueCatalog{
		glueSvc: mockGlueSvc,
	}

	table, err := glueCatalog.getTable(context.TODO(), "test_database", "test_table")
	assert.NoError(err)
	assert.Equal("s3://test-bucket/test_table/metadata/abc123-123.metadata.json", table.Parameters[metadataLocationPropsKey])
}

func TestGlueListTables(t *testing.T) {
	assert := require.New(t)

	mockGlueSvc := &mockGlueClient{}

	mockGlueSvc.On("GetTables", mock.Anything, &glue.GetTablesInput{
		DatabaseName: aws.String("test_database"),
	}, mock.Anything).Return(&glue.GetTablesOutput{
		TableList: []types.Table{testIcebergGlueTable, testNonIcebergGlueTable},
	}, nil).Once()

	glueCatalog := &GlueCatalog{
		glueSvc: mockGlueSvc,
	}

	tables, err := glueCatalog.ListTables(context.TODO(), GlueDatabaseIdentifier("test_database"))
	assert.NoError(err)
	assert.Len(tables, 1)
	assert.Equal([]string{"test_database", "test_table"}, tables[0])
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

	glueCatalog := &GlueCatalog{
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
		Table: &testIcebergGlueTable,
	}, nil).Once()

	mockGlueSvc.On("DeleteTable", mock.Anything, &glue.DeleteTableInput{
		DatabaseName: aws.String("test_database"),
		Name:         aws.String("test_table"),
	}, mock.Anything).Return(&glue.DeleteTableOutput{}, nil).Once()

	glueCatalog := &GlueCatalog{
		glueSvc: mockGlueSvc,
	}

	err := glueCatalog.DropTable(context.TODO(), GlueTableIdentifier("test_database", "test_table"))
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

	glueCatalog := &GlueCatalog{
		glueSvc: mockGlueSvc,
	}

	props := map[string]string{
		descriptionPropsKey: "Test Description",
		locationPropsKey:    "s3://test-location",
	}

	err := glueCatalog.CreateNamespace(context.TODO(), GlueDatabaseIdentifier("test_namespace"), props)
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

	glueCatalog := &GlueCatalog{
		glueSvc: mockGlueSvc,
	}

	err := glueCatalog.DropNamespace(context.TODO(), GlueDatabaseIdentifier("test_namespace"))
	assert.NoError(err)
}

func TestGlueUpdateNamespaceProperties(t *testing.T) {
	tests := []struct {
		name        string
		initial     map[string]string
		updates     map[string]string
		removals    []string
		expected    PropertiesUpdateSummary
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
			expected: PropertiesUpdateSummary{
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
			expected: PropertiesUpdateSummary{
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
			expected: PropertiesUpdateSummary{
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
			expected: PropertiesUpdateSummary{
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
			expected: PropertiesUpdateSummary{
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

			glueCatalog := &GlueCatalog{
				glueSvc: mockGlueSvc,
			}

			summary, err := glueCatalog.UpdateNamespaceProperties(context.TODO(), GlueDatabaseIdentifier("test_namespace"), tt.removals, tt.updates)
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

	glueCatalog := &GlueCatalog{
		glueSvc: mockGlueSvc,
	}

	renamedTable, err := glueCatalog.RenameTable(context.TODO(), GlueTableIdentifier("test_database", "test_table"), GlueTableIdentifier("test_database", "new_test_table"))
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

	glueCatalog := &GlueCatalog{
		glueSvc: mockGlueSvc,
	}

	renamedTable, err := glueCatalog.RenameTable(context.TODO(), GlueTableIdentifier("test_database", "test_table"), GlueTableIdentifier("test_database", "new_test_table"))
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

func TestGlueListNamespacesIntegration(t *testing.T) {
	if os.Getenv("TEST_DATABASE_NAME") == "" {
		t.Skip()
	}
	assert := require.New(t)

	awscfg, err := config.LoadDefaultConfig(context.TODO(), config.WithClientLogMode(aws.LogRequest|aws.LogResponse))
	assert.NoError(err)

	catalog := NewGlueCatalog(WithAwsConfig(awscfg))

	namespaces, err := catalog.ListNamespaces(context.TODO(), nil)
	assert.NoError(err)
	assert.Contains(namespaces, []string{os.Getenv("TEST_DATABASE_NAME")})
}
