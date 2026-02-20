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

package hive

import (
	"context"
	"errors"
	"testing"

	"github.com/apache/iceberg-go"
	"github.com/apache/iceberg-go/catalog"
	"github.com/apache/iceberg-go/table"
	"github.com/beltran/gohive/hive_metastore"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

type mockHiveClient struct {
	mock.Mock
}

func (m *mockHiveClient) GetDatabase(ctx context.Context, name string) (*hive_metastore.Database, error) {
	args := m.Called(ctx, name)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}

	return args.Get(0).(*hive_metastore.Database), args.Error(1)
}

func (m *mockHiveClient) GetAllDatabases(ctx context.Context) ([]string, error) {
	args := m.Called(ctx)

	return args.Get(0).([]string), args.Error(1)
}

func (m *mockHiveClient) CreateDatabase(ctx context.Context, database *hive_metastore.Database) error {
	args := m.Called(ctx, database)

	return args.Error(0)
}

func (m *mockHiveClient) DropDatabase(ctx context.Context, name string, deleteData, cascade bool) error {
	args := m.Called(ctx, name, deleteData, cascade)

	return args.Error(0)
}

func (m *mockHiveClient) AlterDatabase(ctx context.Context, name string, database *hive_metastore.Database) error {
	args := m.Called(ctx, name, database)

	return args.Error(0)
}

func (m *mockHiveClient) GetTable(ctx context.Context, dbName, tableName string) (*hive_metastore.Table, error) {
	args := m.Called(ctx, dbName, tableName)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}

	return args.Get(0).(*hive_metastore.Table), args.Error(1)
}

func (m *mockHiveClient) GetTables(ctx context.Context, dbName, pattern string) ([]string, error) {
	args := m.Called(ctx, dbName, pattern)

	return args.Get(0).([]string), args.Error(1)
}

func (m *mockHiveClient) CreateTable(ctx context.Context, tbl *hive_metastore.Table) error {
	args := m.Called(ctx, tbl)

	return args.Error(0)
}

func (m *mockHiveClient) DropTable(ctx context.Context, dbName, tableName string, deleteData bool) error {
	args := m.Called(ctx, dbName, tableName, deleteData)

	return args.Error(0)
}

func (m *mockHiveClient) AlterTable(ctx context.Context, dbName, tableName string, newTable *hive_metastore.Table) error {
	args := m.Called(ctx, dbName, tableName, newTable)

	return args.Error(0)
}

func (m *mockHiveClient) Lock(ctx context.Context, request *hive_metastore.LockRequest) (*hive_metastore.LockResponse, error) {
	args := m.Called(ctx, request)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}

	return args.Get(0).(*hive_metastore.LockResponse), args.Error(1)
}

func (m *mockHiveClient) CheckLock(ctx context.Context, lockId int64) (*hive_metastore.LockResponse, error) {
	args := m.Called(ctx, lockId)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}

	return args.Get(0).(*hive_metastore.LockResponse), args.Error(1)
}

func (m *mockHiveClient) Unlock(ctx context.Context, lockId int64) error {
	args := m.Called(ctx, lockId)

	return args.Error(0)
}

func (m *mockHiveClient) Close() error {
	args := m.Called()

	return args.Error(0)
}

// Test data

var testIcebergHiveTable1 = &hive_metastore.Table{
	TableName: "test_table",
	DbName:    "test_database",
	TableType: TableTypeExternalTable,
	Parameters: map[string]string{
		TableTypeKey:        TableTypeIceberg,
		MetadataLocationKey: "s3://test-bucket/test_table/metadata/abc123-123.metadata.json",
	},
	Sd: &hive_metastore.StorageDescriptor{
		Location: "s3://test-bucket/test_table",
	},
}

var testIcebergHiveTable2 = &hive_metastore.Table{
	TableName: "test_table2",
	DbName:    "test_database",
	TableType: TableTypeExternalTable,
	Parameters: map[string]string{
		TableTypeKey:        TableTypeIceberg,
		MetadataLocationKey: "s3://test-bucket/test_table2/metadata/abc456-456.metadata.json",
	},
	Sd: &hive_metastore.StorageDescriptor{
		Location: "s3://test-bucket/test_table2",
	},
}

var testNonIcebergHiveTable = &hive_metastore.Table{
	TableName: "other_table",
	DbName:    "test_database",
	TableType: TableTypeExternalTable,
	Parameters: map[string]string{
		"some_param": "some_value",
	},
}

var testSchema = iceberg.NewSchemaWithIdentifiers(0, []int{},
	iceberg.NestedField{ID: 1, Name: "foo", Type: iceberg.PrimitiveTypes.String},
	iceberg.NestedField{ID: 2, Name: "bar", Type: iceberg.PrimitiveTypes.Int32, Required: true},
	iceberg.NestedField{ID: 3, Name: "baz", Type: iceberg.PrimitiveTypes.Bool})

var testIcebergHiveView = &hive_metastore.Table{
	TableName: "test_view",
	DbName:    "test_database",
	TableType: TableTypeVirtualView,
	Parameters: map[string]string{
		TableTypeKey:        TableTypeIcebergView,
		MetadataLocationKey: "s3://test-bucket/test_view/metadata/view-1234.metadata.json",
	},
	Sd: &hive_metastore.StorageDescriptor{
		Location: "s3://test-bucket/test_view",
	},
}

// Error helpers for mocking
var (
	errNoSuchObject     = errors.New("NoSuchObjectException: object not found")
	errAlreadyExists    = errors.New("AlreadyExistsException: object already exists")
	errInvalidOperation = errors.New("InvalidOperationException: Database is not empty")
)

// Tests

func TestHiveListTables(t *testing.T) {
	assert := require.New(t)

	mockClient := &mockHiveClient{}

	mockClient.On("GetTables", mock.Anything, "test_database", "*").
		Return([]string{"test_table", "test_table2", "other_table"}, nil).Once()

	// Mock individual GetTable calls for each table
	mockClient.On("GetTable", mock.Anything, "test_database", "test_table").
		Return(testIcebergHiveTable1, nil).Once()
	mockClient.On("GetTable", mock.Anything, "test_database", "test_table2").
		Return(testIcebergHiveTable2, nil).Once()
	mockClient.On("GetTable", mock.Anything, "test_database", "other_table").
		Return(testNonIcebergHiveTable, nil).Once()

	hiveCatalog := NewCatalogWithClient(mockClient, iceberg.Properties{})

	var lastErr error
	tbls := make([]table.Identifier, 0)
	iter := hiveCatalog.ListTables(context.TODO(), DatabaseIdentifier("test_database"))

	for tbl, err := range iter {
		if err != nil {
			lastErr = err

			break
		}
		tbls = append(tbls, tbl)
	}

	assert.NoError(lastErr)
	assert.Len(tbls, 2) // Only Iceberg tables
	assert.Contains(tbls, table.Identifier{"test_database", "test_table"})
	assert.Contains(tbls, table.Identifier{"test_database", "test_table2"})

	mockClient.AssertExpectations(t)
}

func TestHiveListTablesEmpty(t *testing.T) {
	assert := require.New(t)

	mockClient := &mockHiveClient{}

	mockClient.On("GetTables", mock.Anything, "empty_database", "*").
		Return([]string{}, nil).Once()

	hiveCatalog := NewCatalogWithClient(mockClient, iceberg.Properties{})

	var lastErr error
	tbls := make([]table.Identifier, 0)
	iter := hiveCatalog.ListTables(context.TODO(), DatabaseIdentifier("empty_database"))

	for tbl, err := range iter {
		if err != nil {
			lastErr = err

			break
		}
		tbls = append(tbls, tbl)
	}

	assert.NoError(lastErr)
	assert.Len(tbls, 0)

	mockClient.AssertExpectations(t)
}

func TestHiveListNamespaces(t *testing.T) {
	assert := require.New(t)

	mockClient := &mockHiveClient{}

	mockClient.On("GetAllDatabases", mock.Anything).
		Return([]string{"database1", "database2", "database3"}, nil).Once()

	hiveCatalog := NewCatalogWithClient(mockClient, iceberg.Properties{})

	namespaces, err := hiveCatalog.ListNamespaces(context.TODO(), nil)
	assert.NoError(err)
	assert.Len(namespaces, 3)
	assert.Equal([]string{"database1"}, namespaces[0])
	assert.Equal([]string{"database2"}, namespaces[1])
	assert.Equal([]string{"database3"}, namespaces[2])

	mockClient.AssertExpectations(t)
}

func TestHiveListNamespacesHierarchicalError(t *testing.T) {
	assert := require.New(t)

	mockClient := &mockHiveClient{}
	hiveCatalog := NewCatalogWithClient(mockClient, iceberg.Properties{})

	_, err := hiveCatalog.ListNamespaces(context.TODO(), []string{"parent"})
	assert.Error(err)
	assert.Contains(err.Error(), "hierarchical namespace is not supported")
}

func TestHiveCreateNamespace(t *testing.T) {
	assert := require.New(t)

	mockClient := &mockHiveClient{}

	mockClient.On("CreateDatabase", mock.Anything, mock.MatchedBy(func(db *hive_metastore.Database) bool {
		return db.Name == "new_database" &&
			db.Description == "Test Description" &&
			db.LocationUri == "s3://test-location"
	})).Return(nil).Once()

	hiveCatalog := NewCatalogWithClient(mockClient, iceberg.Properties{})

	props := map[string]string{
		"comment":  "Test Description",
		"location": "s3://test-location",
	}

	err := hiveCatalog.CreateNamespace(context.TODO(), DatabaseIdentifier("new_database"), props)
	assert.NoError(err)

	mockClient.AssertExpectations(t)
}

func TestHiveCreateNamespaceAlreadyExists(t *testing.T) {
	assert := require.New(t)

	mockClient := &mockHiveClient{}

	mockClient.On("CreateDatabase", mock.Anything, mock.Anything).
		Return(errAlreadyExists).Once()

	hiveCatalog := NewCatalogWithClient(mockClient, iceberg.Properties{})

	err := hiveCatalog.CreateNamespace(context.TODO(), DatabaseIdentifier("existing_database"), nil)
	assert.Error(err)
	assert.True(errors.Is(err, catalog.ErrNamespaceAlreadyExists))

	mockClient.AssertExpectations(t)
}

func TestHiveDropNamespace(t *testing.T) {
	assert := require.New(t)

	mockClient := &mockHiveClient{}

	mockClient.On("GetDatabase", mock.Anything, "test_namespace").
		Return(&hive_metastore.Database{Name: "test_namespace"}, nil).Once()

	mockClient.On("DropDatabase", mock.Anything, "test_namespace", false, false).
		Return(nil).Once()

	hiveCatalog := NewCatalogWithClient(mockClient, iceberg.Properties{})

	err := hiveCatalog.DropNamespace(context.TODO(), DatabaseIdentifier("test_namespace"))
	assert.NoError(err)

	mockClient.AssertExpectations(t)
}

func TestHiveDropNamespaceNotExists(t *testing.T) {
	assert := require.New(t)

	mockClient := &mockHiveClient{}

	mockClient.On("GetDatabase", mock.Anything, "nonexistent").
		Return(nil, errNoSuchObject).Once()

	hiveCatalog := NewCatalogWithClient(mockClient, iceberg.Properties{})

	err := hiveCatalog.DropNamespace(context.TODO(), DatabaseIdentifier("nonexistent"))
	assert.Error(err)
	assert.True(errors.Is(err, catalog.ErrNoSuchNamespace))

	mockClient.AssertExpectations(t)
}

func TestHiveDropNamespaceNotEmpty(t *testing.T) {
	assert := require.New(t)

	mockClient := &mockHiveClient{}

	mockClient.On("GetDatabase", mock.Anything, "nonempty_db").
		Return(&hive_metastore.Database{Name: "nonempty_db"}, nil).Once()

	mockClient.On("DropDatabase", mock.Anything, "nonempty_db", false, false).
		Return(errInvalidOperation).Once()

	hiveCatalog := NewCatalogWithClient(mockClient, iceberg.Properties{})

	err := hiveCatalog.DropNamespace(context.TODO(), DatabaseIdentifier("nonempty_db"))
	assert.Error(err)
	assert.True(errors.Is(err, catalog.ErrNamespaceNotEmpty))

	mockClient.AssertExpectations(t)
}

func TestHiveCheckNamespaceExists(t *testing.T) {
	assert := require.New(t)

	mockClient := &mockHiveClient{}

	mockClient.On("GetDatabase", mock.Anything, "existing_db").
		Return(&hive_metastore.Database{Name: "existing_db"}, nil).Once()

	hiveCatalog := NewCatalogWithClient(mockClient, iceberg.Properties{})

	exists, err := hiveCatalog.CheckNamespaceExists(context.TODO(), DatabaseIdentifier("existing_db"))
	assert.NoError(err)
	assert.True(exists)

	mockClient.AssertExpectations(t)
}

func TestHiveCheckNamespaceNotExists(t *testing.T) {
	assert := require.New(t)

	mockClient := &mockHiveClient{}

	mockClient.On("GetDatabase", mock.Anything, "nonexistent_db").
		Return(nil, errNoSuchObject).Once()

	hiveCatalog := NewCatalogWithClient(mockClient, iceberg.Properties{})

	exists, err := hiveCatalog.CheckNamespaceExists(context.TODO(), DatabaseIdentifier("nonexistent_db"))
	assert.NoError(err)
	assert.False(exists)

	mockClient.AssertExpectations(t)
}

func TestHiveLoadNamespaceProperties(t *testing.T) {
	assert := require.New(t)

	mockClient := &mockHiveClient{}

	mockClient.On("GetDatabase", mock.Anything, "test_db").
		Return(&hive_metastore.Database{
			Name:        "test_db",
			Description: "Test database",
			LocationUri: "s3://test-bucket/test_db",
			Parameters: map[string]string{
				"custom_param": "custom_value",
			},
		}, nil).Once()

	hiveCatalog := NewCatalogWithClient(mockClient, iceberg.Properties{})

	props, err := hiveCatalog.LoadNamespaceProperties(context.TODO(), DatabaseIdentifier("test_db"))
	assert.NoError(err)
	assert.Equal("s3://test-bucket/test_db", props["location"])
	assert.Equal("Test database", props["comment"])
	assert.Equal("custom_value", props["custom_param"])

	mockClient.AssertExpectations(t)
}

func TestHiveCheckTableExists(t *testing.T) {
	assert := require.New(t)

	mockClient := &mockHiveClient{}

	mockClient.On("GetTable", mock.Anything, "test_database", "test_table").
		Return(testIcebergHiveTable1, nil).Once()

	hiveCatalog := NewCatalogWithClient(mockClient, iceberg.Properties{})

	exists, err := hiveCatalog.CheckTableExists(context.TODO(), TableIdentifier("test_database", "test_table"))
	assert.NoError(err)
	assert.True(exists)

	mockClient.AssertExpectations(t)
}

func TestHiveCheckTableNotExists(t *testing.T) {
	assert := require.New(t)

	mockClient := &mockHiveClient{}

	mockClient.On("GetTable", mock.Anything, "test_database", "nonexistent").
		Return(nil, errNoSuchObject).Once()

	hiveCatalog := NewCatalogWithClient(mockClient, iceberg.Properties{})

	exists, err := hiveCatalog.CheckTableExists(context.TODO(), TableIdentifier("test_database", "nonexistent"))
	assert.NoError(err)
	assert.False(exists)

	mockClient.AssertExpectations(t)
}

func TestHiveCheckTableExistsNonIceberg(t *testing.T) {
	assert := require.New(t)

	mockClient := &mockHiveClient{}

	mockClient.On("GetTable", mock.Anything, "test_database", "other_table").
		Return(testNonIcebergHiveTable, nil).Once()

	hiveCatalog := NewCatalogWithClient(mockClient, iceberg.Properties{})

	exists, err := hiveCatalog.CheckTableExists(context.TODO(), TableIdentifier("test_database", "other_table"))
	assert.NoError(err)
	assert.False(exists) // Non-Iceberg table should return false

	mockClient.AssertExpectations(t)
}

func TestHiveDropTable(t *testing.T) {
	assert := require.New(t)

	mockClient := &mockHiveClient{}

	mockClient.On("GetTable", mock.Anything, "test_database", "test_table").
		Return(testIcebergHiveTable1, nil).Once()

	mockClient.On("DropTable", mock.Anything, "test_database", "test_table", false).
		Return(nil).Once()

	hiveCatalog := NewCatalogWithClient(mockClient, iceberg.Properties{})

	err := hiveCatalog.DropTable(context.TODO(), TableIdentifier("test_database", "test_table"))
	assert.NoError(err)

	mockClient.AssertExpectations(t)
}

func TestHiveDropTableNotExists(t *testing.T) {
	assert := require.New(t)

	mockClient := &mockHiveClient{}

	mockClient.On("GetTable", mock.Anything, "test_database", "nonexistent").
		Return(nil, errNoSuchObject).Once()

	hiveCatalog := NewCatalogWithClient(mockClient, iceberg.Properties{})

	err := hiveCatalog.DropTable(context.TODO(), TableIdentifier("test_database", "nonexistent"))
	assert.Error(err)
	assert.True(errors.Is(err, catalog.ErrNoSuchTable))

	mockClient.AssertExpectations(t)
}

func TestHiveDropTableNonIceberg(t *testing.T) {
	assert := require.New(t)

	mockClient := &mockHiveClient{}

	mockClient.On("GetTable", mock.Anything, "test_database", "other_table").
		Return(testNonIcebergHiveTable, nil).Once()

	hiveCatalog := NewCatalogWithClient(mockClient, iceberg.Properties{})

	err := hiveCatalog.DropTable(context.TODO(), TableIdentifier("test_database", "other_table"))
	assert.Error(err)
	assert.Contains(err.Error(), "is not an Iceberg table")

	mockClient.AssertExpectations(t)
}

func TestHiveCatalogType(t *testing.T) {
	assert := require.New(t)

	mockClient := &mockHiveClient{}
	hiveCatalog := NewCatalogWithClient(mockClient, iceberg.Properties{})

	assert.Equal(catalog.Hive, hiveCatalog.CatalogType())
}

func TestHiveCreateTableConflictsWithView(t *testing.T) {
	assert := require.New(t)

	mockClient := &mockHiveClient{}

	// A view already exists with the same identifier.
	mockClient.On("GetTable", mock.Anything, "test_database", "test_table").
		Return(testIcebergHiveView, nil).Once()

	hiveCatalog := NewCatalogWithClient(mockClient, iceberg.Properties{})

	_, err := hiveCatalog.CreateTable(
		context.TODO(),
		TableIdentifier("test_database", "test_table"),
		testSchema,
	)
	assert.Error(err)
	assert.True(errors.Is(err, catalog.ErrViewAlreadyExists))

	mockClient.AssertExpectations(t)
}

func TestHiveCheckViewExists(t *testing.T) {
	assert := require.New(t)

	mockClient := &mockHiveClient{}

	mockClient.On("GetTable", mock.Anything, "test_database", "test_view").
		Return(testIcebergHiveView, nil).Once()

	hiveCatalog := NewCatalogWithClient(mockClient, iceberg.Properties{})

	exists, err := hiveCatalog.CheckViewExists(context.TODO(), TableIdentifier("test_database", "test_view"))
	assert.NoError(err)
	assert.True(exists)

	mockClient.AssertExpectations(t)
}

func TestHiveCheckViewNotExists(t *testing.T) {
	assert := require.New(t)

	mockClient := &mockHiveClient{}

	mockClient.On("GetTable", mock.Anything, "test_database", "nonexistent_view").
		Return(nil, errNoSuchObject).Once()

	hiveCatalog := NewCatalogWithClient(mockClient, iceberg.Properties{})

	exists, err := hiveCatalog.CheckViewExists(context.TODO(), TableIdentifier("test_database", "nonexistent_view"))
	assert.NoError(err)
	assert.False(exists)

	mockClient.AssertExpectations(t)
}

func TestHiveListViews(t *testing.T) {
	assert := require.New(t)

	mockClient := &mockHiveClient{}

	// One Iceberg view and one non-Iceberg table.
	mockClient.On("GetTables", mock.Anything, "test_database", "*").
		Return([]string{"test_view", "other_table"}, nil).Once()

	mockClient.On("GetDatabase", mock.Anything, "test_database").
		Return(&hive_metastore.Database{Name: "test_database"}, nil).Once()

	mockClient.On("GetTable", mock.Anything, "test_database", "test_view").
		Return(testIcebergHiveView, nil).Once()
	mockClient.On("GetTable", mock.Anything, "test_database", "other_table").
		Return(testNonIcebergHiveTable, nil).Once()

	hiveCatalog := NewCatalogWithClient(mockClient, iceberg.Properties{})

	iter := hiveCatalog.ListViews(context.TODO(), DatabaseIdentifier("test_database"))

	var (
		lastErr error
		views   []table.Identifier
	)

	for v, err := range iter {
		if err != nil {
			lastErr = err

			break
		}
		views = append(views, v)
	}

	assert.NoError(lastErr)
	assert.Len(views, 1)
	assert.Equal(TableIdentifier("test_database", "test_view"), views[0])

	mockClient.AssertExpectations(t)
}

func TestHiveLoadView(t *testing.T) {
	assert := require.New(t)

	mockClient := &mockHiveClient{}

	mockClient.On("GetTable", mock.Anything, "test_database", "test_view").
		Return(testIcebergHiveView, nil).Once()

	// Metadata location in the fixture is s3://... which we cannot load in unit tests.
	// We only assert that LoadView returns an error when the metadata is unreachable.
	props := iceberg.Properties{
		"warehouse": "file:///tmp",
	}
	hiveCatalog := NewCatalogWithClient(mockClient, props)

	_, err := hiveCatalog.LoadView(context.TODO(), TableIdentifier("test_database", "test_view"))
	assert.Error(err)

	mockClient.AssertExpectations(t)
}

func TestIsIcebergTable(t *testing.T) {
	tests := []struct {
		name     string
		table    *hive_metastore.Table
		expected bool
	}{
		{
			name:     "iceberg table uppercase",
			table:    testIcebergHiveTable1,
			expected: true,
		},
		{
			name: "iceberg table lowercase",
			table: &hive_metastore.Table{
				Parameters: map[string]string{TableTypeKey: "iceberg"},
			},
			expected: true,
		},
		{
			name: "iceberg table mixed case",
			table: &hive_metastore.Table{
				Parameters: map[string]string{TableTypeKey: "IcEbErG"},
			},
			expected: true,
		},
		{
			name:     "non-iceberg table",
			table:    testNonIcebergHiveTable,
			expected: false,
		},
		{
			name:     "nil table",
			table:    nil,
			expected: false,
		},
		{
			name:     "table without parameters",
			table:    &hive_metastore.Table{},
			expected: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert := require.New(t)
			assert.Equal(tt.expected, isIcebergTable(tt.table))
		})
	}
}

func TestIsIcebergView(t *testing.T) {
	tests := []struct {
		name     string
		table    *hive_metastore.Table
		expected bool
	}{
		{
			name:     "iceberg view",
			table:    testIcebergHiveView,
			expected: true,
		},
		{
			name: "iceberg view mixed case",
			table: &hive_metastore.Table{
				TableName: "test_view_mixed",
				DbName:    "test_database",
				TableType: "virtual_view",
				Parameters: map[string]string{
					TableTypeKey: "IcEbErG_ViEw",
				},
			},
			expected: true,
		},
		{
			name: "non-virtual-table-type",
			table: &hive_metastore.Table{
				TableName: "not_view",
				DbName:    "test_database",
				TableType: TableTypeExternalTable,
				Parameters: map[string]string{
					TableTypeKey: TableTypeIcebergView,
				},
			},
			expected: false,
		},
		{
			name:     "nil table",
			table:    nil,
			expected: false,
		},
		{
			name:     "no parameters",
			table:    &hive_metastore.Table{TableType: TableTypeVirtualView},
			expected: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert := require.New(t)
			assert.Equal(tt.expected, isIcebergView(tt.table))
		})
	}
}

func TestIcebergTypeToHiveType(t *testing.T) {
	tests := []struct {
		name         string
		icebergType  iceberg.Type
		expectedHive string
	}{
		{"boolean", iceberg.PrimitiveTypes.Bool, "boolean"},
		{"int32", iceberg.PrimitiveTypes.Int32, "int"},
		{"int64", iceberg.PrimitiveTypes.Int64, "bigint"},
		{"float32", iceberg.PrimitiveTypes.Float32, "float"},
		{"float64", iceberg.PrimitiveTypes.Float64, "double"},
		{"date", iceberg.PrimitiveTypes.Date, "date"},
		{"time", iceberg.PrimitiveTypes.Time, "string"},
		{"timestamp", iceberg.PrimitiveTypes.Timestamp, "timestamp"},
		{"timestamptz", iceberg.PrimitiveTypes.TimestampTz, "timestamp"},
		{"string", iceberg.PrimitiveTypes.String, "string"},
		{"uuid", iceberg.PrimitiveTypes.UUID, "string"},
		{"binary", iceberg.PrimitiveTypes.Binary, "binary"},
		{"decimal", iceberg.DecimalTypeOf(10, 2), "decimal(10,2)"},
		{"fixed", iceberg.FixedTypeOf(16), "binary(16)"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert := require.New(t)
			assert.Equal(tt.expectedHive, icebergTypeToHiveType(tt.icebergType))
		})
	}
}

func TestSchemaToHiveColumns(t *testing.T) {
	assert := require.New(t)

	columns := schemaToHiveColumns(testSchema)
	assert.Len(columns, 3)

	// Check first column
	assert.Equal("foo", columns[0].Name)
	assert.Equal("string", columns[0].Type)

	// Check second column
	assert.Equal("bar", columns[1].Name)
	assert.Equal("int", columns[1].Type)

	// Check third column
	assert.Equal("baz", columns[2].Name)
	assert.Equal("boolean", columns[2].Type)
}

func TestUpdateNamespaceProperties(t *testing.T) {
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
			},
			removals:    []string{"key1"},
			shouldError: true,
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
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert := require.New(t)

			mockClient := &mockHiveClient{}

			mockClient.On("GetDatabase", mock.Anything, "test_namespace").
				Return(&hive_metastore.Database{
					Name:       "test_namespace",
					Parameters: tt.initial,
				}, nil).Once()

			if !tt.shouldError {
				mockClient.On("AlterDatabase", mock.Anything, "test_namespace", mock.Anything).
					Return(nil).Once()
			}

			hiveCatalog := NewCatalogWithClient(mockClient, iceberg.Properties{})

			summary, err := hiveCatalog.UpdateNamespaceProperties(context.TODO(), DatabaseIdentifier("test_namespace"), tt.removals, tt.updates)
			if tt.shouldError {
				assert.Error(err)
			} else {
				assert.NoError(err)
				assert.ElementsMatch(tt.expected.Removed, summary.Removed)
				assert.ElementsMatch(tt.expected.Updated, summary.Updated)
				assert.ElementsMatch(tt.expected.Missing, summary.Missing)
			}
		})
	}
}

func TestIdentifierValidation(t *testing.T) {
	t.Run("valid table identifier", func(t *testing.T) {
		assert := require.New(t)
		db, tbl, err := identifierToTableName([]string{"database", "table"})
		assert.NoError(err)
		assert.Equal("database", db)
		assert.Equal("table", tbl)
	})

	t.Run("invalid table identifier - too short", func(t *testing.T) {
		assert := require.New(t)
		_, _, err := identifierToTableName([]string{"database"})
		assert.Error(err)
	})

	t.Run("invalid table identifier - too long", func(t *testing.T) {
		assert := require.New(t)
		_, _, err := identifierToTableName([]string{"a", "b", "c"})
		assert.Error(err)
	})

	t.Run("valid database identifier", func(t *testing.T) {
		assert := require.New(t)
		db, err := identifierToDatabase([]string{"database"})
		assert.NoError(err)
		assert.Equal("database", db)
	})

	t.Run("invalid database identifier", func(t *testing.T) {
		assert := require.New(t)
		_, err := identifierToDatabase([]string{"a", "b"})
		assert.Error(err)
	})
}
