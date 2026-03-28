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

package hive

import (
	"context"
	"errors"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/apache/iceberg-go"
	"github.com/apache/iceberg-go/catalog"
	"github.com/apache/iceberg-go/table"
	"github.com/apache/iceberg-go/view"
	"github.com/stretchr/testify/require"
)

// Integration tests for the Hive Metastore catalog.
// These tests require a running Hive Metastore instance.
//
// To run these tests:
// 1. Start the Hive Metastore using Docker:
//    cd internal/recipe && docker-compose up -d hive-metastore
//
// 2. Set the required environment variables:
//    export TEST_HIVE_URI=thrift://localhost:9083
//    export TEST_HIVE_DATABASE=test_db
//    export TEST_TABLE_LOCATION=/tmp/iceberg/warehouse
//
// 3. Run the tests:
//    go test -tags=integration -v ./catalog/hive/...

func getTestHiveURI() string {
	uri := os.Getenv("TEST_HIVE_URI")
	if uri == "" {
		return "thrift://localhost:9083"
	}
	return uri
}

func getTestDatabase() string {
	db := os.Getenv("TEST_HIVE_DATABASE")
	if db == "" {
		return "test_iceberg_db"
	}
	return db
}

func getTestTableLocation() string {
	loc := os.Getenv("TEST_TABLE_LOCATION")
	if loc == "" {
		return "/tmp/iceberg/warehouse"
	}
	return loc
}

func createTestCatalog(t *testing.T) *Catalog {
	t.Helper()

	props := iceberg.Properties{
		URI:       getTestHiveURI(),
		Warehouse: getTestTableLocation(),
	}

	cat, err := NewCatalog(props)
	require.NoError(t, err)

	return cat
}

func TestHiveIntegrationListNamespaces(t *testing.T) {
	assert := require.New(t)

	cat := createTestCatalog(t)
	defer cat.Close()

	namespaces, err := cat.ListNamespaces(context.TODO(), nil)
	assert.NoError(err)
	assert.NotNil(namespaces)

	t.Logf("Found %d namespaces", len(namespaces))
	for _, ns := range namespaces {
		t.Logf("  - %v", ns)
	}
}

func TestHiveIntegrationCreateAndDropNamespace(t *testing.T) {
	assert := require.New(t)

	cat := createTestCatalog(t)
	defer cat.Close()

	dbName := fmt.Sprintf("test_db_%d", time.Now().UnixNano())

	// Create namespace
	props := iceberg.Properties{
		"comment":  "Test database for integration tests",
		"location": getTestTableLocation() + "/" + dbName,
	}

	err := cat.CreateNamespace(context.TODO(), DatabaseIdentifier(dbName), props)
	assert.NoError(err)

	// Check it exists
	exists, err := cat.CheckNamespaceExists(context.TODO(), DatabaseIdentifier(dbName))
	assert.NoError(err)
	assert.True(exists)

	// Load properties
	loadedProps, err := cat.LoadNamespaceProperties(context.TODO(), DatabaseIdentifier(dbName))
	assert.NoError(err)
	assert.Equal("Test database for integration tests", loadedProps["comment"])

	// Drop namespace
	err = cat.DropNamespace(context.TODO(), DatabaseIdentifier(dbName))
	assert.NoError(err)

	// Verify it's gone
	exists, err = cat.CheckNamespaceExists(context.TODO(), DatabaseIdentifier(dbName))
	assert.NoError(err)
	assert.False(exists)
}

func TestHiveIntegrationUpdateNamespaceProperties(t *testing.T) {
	assert := require.New(t)

	cat := createTestCatalog(t)
	defer cat.Close()

	dbName := fmt.Sprintf("test_db_%d", time.Now().UnixNano())

	// Create namespace with initial properties
	initialProps := iceberg.Properties{
		"location": fmt.Sprintf("/tmp/iceberg-warehouse/%s", dbName),
		"key1":     "value1",
		"key2":     "value2",
	}

	err := cat.CreateNamespace(context.TODO(), DatabaseIdentifier(dbName), initialProps)
	assert.NoError(err)
	defer cat.DropNamespace(context.TODO(), DatabaseIdentifier(dbName))

	// Update properties
	updates := iceberg.Properties{
		"key2": "updated_value2",
		"key3": "value3",
	}
	removals := []string{"key1"}

	summary, err := cat.UpdateNamespaceProperties(context.TODO(), DatabaseIdentifier(dbName), removals, updates)
	assert.NoError(err)
	assert.Contains(summary.Removed, "key1")
	assert.Contains(summary.Updated, "key2")
	assert.Contains(summary.Updated, "key3")

	// Verify updates
	props, err := cat.LoadNamespaceProperties(context.TODO(), DatabaseIdentifier(dbName))
	assert.NoError(err)
	assert.Equal("updated_value2", props["key2"])
	assert.Equal("value3", props["key3"])
	_, exists := props["key1"]
	assert.False(exists)
}

func TestHiveIntegrationCreateAndListTables(t *testing.T) {
	assert := require.New(t)

	cat := createTestCatalog(t)
	defer cat.Close()

	dbName := fmt.Sprintf("test_db_%d", time.Now().UnixNano())
	tableName := "test_table"

	// Create namespace
	err := cat.CreateNamespace(context.TODO(), DatabaseIdentifier(dbName), iceberg.Properties{
		"location": getTestTableLocation() + "/" + dbName,
	})
	assert.NoError(err)
	defer cat.DropNamespace(context.TODO(), DatabaseIdentifier(dbName))

	// Create table
	schema := iceberg.NewSchemaWithIdentifiers(0, []int{1},
		iceberg.NestedField{ID: 1, Name: "id", Type: iceberg.PrimitiveTypes.Int64, Required: true},
		iceberg.NestedField{ID: 2, Name: "name", Type: iceberg.PrimitiveTypes.String},
	)

	tableLocation := getTestTableLocation() + "/" + dbName + "/" + tableName
	tbl, err := cat.CreateTable(context.TODO(), TableIdentifier(dbName, tableName), schema,
		catalog.WithLocation(tableLocation),
	)
	assert.NoError(err)
	assert.NotNil(tbl)
	defer cat.DropTable(context.TODO(), TableIdentifier(dbName, tableName))

	// Verify table exists
	exists, err := cat.CheckTableExists(context.TODO(), TableIdentifier(dbName, tableName))
	assert.NoError(err)
	assert.True(exists)

	// List tables
	tables := make([][]string, 0)
	for tblIdent, err := range cat.ListTables(context.TODO(), DatabaseIdentifier(dbName)) {
		assert.NoError(err)
		tables = append(tables, tblIdent)
	}
	assert.Len(tables, 1)
	assert.Equal([]string{dbName, tableName}, tables[0])

	// Load table
	loadedTable, err := cat.LoadTable(context.TODO(), TableIdentifier(dbName, tableName))
	assert.NoError(err)
	assert.NotNil(loadedTable)
	assert.True(schema.Equals(loadedTable.Schema()))
}

func TestHiveIntegrationRegisterTable(t *testing.T) {
	assert := require.New(t)

	cat := createTestCatalog(t)
	defer cat.Close()

	dbName := fmt.Sprintf("test_db_%d", time.Now().UnixNano())
	origName := "orig_table"
	regName := "registered_table"

	err := cat.CreateNamespace(context.TODO(), DatabaseIdentifier(dbName), iceberg.Properties{
		"location": getTestTableLocation() + "/" + dbName,
	})
	assert.NoError(err)
	defer cat.DropNamespace(context.TODO(), DatabaseIdentifier(dbName))

	schema := iceberg.NewSchemaWithIdentifiers(0, []int{1},
		iceberg.NestedField{ID: 1, Name: "id", Type: iceberg.PrimitiveTypes.Int64, Required: true},
		iceberg.NestedField{ID: 2, Name: "name", Type: iceberg.PrimitiveTypes.String},
	)

	tableLocation := getTestTableLocation() + "/" + dbName + "/" + origName
	origTbl, err := cat.CreateTable(context.TODO(), TableIdentifier(dbName, origName), schema,
		catalog.WithLocation(tableLocation),
	)
	assert.NoError(err)
	metadataLocation := origTbl.MetadataLocation()

	err = cat.DropTable(context.TODO(), TableIdentifier(dbName, origName))
	assert.NoError(err)

	regTbl, err := cat.RegisterTable(context.TODO(), TableIdentifier(dbName, regName), metadataLocation)
	assert.NoError(err)
	assert.NotNil(regTbl)
	assert.Equal(metadataLocation, regTbl.MetadataLocation())
	assert.True(schema.Equals(regTbl.Schema()))

	defer func() {
		assert.NoError(cat.DropTable(context.TODO(), TableIdentifier(dbName, regName)))
	}()
}

func TestHiveIntegrationRenameTable(t *testing.T) {
	assert := require.New(t)

	cat := createTestCatalog(t)
	defer cat.Close()

	dbName := fmt.Sprintf("test_db_%d", time.Now().UnixNano())
	oldTableName := "old_table"
	newTableName := "new_table"

	// Create namespace
	err := cat.CreateNamespace(context.TODO(), DatabaseIdentifier(dbName), iceberg.Properties{
		"location": getTestTableLocation() + "/" + dbName,
	})
	assert.NoError(err)
	defer cat.DropNamespace(context.TODO(), DatabaseIdentifier(dbName))

	// Create table
	schema := iceberg.NewSchemaWithIdentifiers(0, []int{1},
		iceberg.NestedField{ID: 1, Name: "id", Type: iceberg.PrimitiveTypes.Int64, Required: true},
	)

	tableLocation := getTestTableLocation() + "/" + dbName + "/" + oldTableName
	_, err = cat.CreateTable(context.TODO(), TableIdentifier(dbName, oldTableName), schema,
		catalog.WithLocation(tableLocation),
	)
	assert.NoError(err)

	// Rename table
	renamedTable, err := cat.RenameTable(context.TODO(),
		TableIdentifier(dbName, oldTableName),
		TableIdentifier(dbName, newTableName),
	)
	assert.NoError(err)
	assert.NotNil(renamedTable)
	defer cat.DropTable(context.TODO(), TableIdentifier(dbName, newTableName))

	// Verify old table doesn't exist
	exists, err := cat.CheckTableExists(context.TODO(), TableIdentifier(dbName, oldTableName))
	assert.NoError(err)
	assert.False(exists)

	// Verify new table exists
	exists, err = cat.CheckTableExists(context.TODO(), TableIdentifier(dbName, newTableName))
	assert.NoError(err)
	assert.True(exists)
}

func TestHiveIntegrationDropTable(t *testing.T) {
	assert := require.New(t)

	cat := createTestCatalog(t)
	defer cat.Close()

	dbName := fmt.Sprintf("test_db_%d", time.Now().UnixNano())
	tableName := "table_to_drop"

	// Create namespace
	err := cat.CreateNamespace(context.TODO(), DatabaseIdentifier(dbName), iceberg.Properties{
		"location": getTestTableLocation() + "/" + dbName,
	})
	assert.NoError(err)
	defer cat.DropNamespace(context.TODO(), DatabaseIdentifier(dbName))

	// Create table
	schema := iceberg.NewSchemaWithIdentifiers(0, []int{1},
		iceberg.NestedField{ID: 1, Name: "id", Type: iceberg.PrimitiveTypes.Int64, Required: true},
	)

	tableLocation := getTestTableLocation() + "/" + dbName + "/" + tableName
	_, err = cat.CreateTable(context.TODO(), TableIdentifier(dbName, tableName), schema,
		catalog.WithLocation(tableLocation),
	)
	assert.NoError(err)

	// Verify table exists
	exists, err := cat.CheckTableExists(context.TODO(), TableIdentifier(dbName, tableName))
	assert.NoError(err)
	assert.True(exists)

	// Drop table
	err = cat.DropTable(context.TODO(), TableIdentifier(dbName, tableName))
	assert.NoError(err)

	// Verify table is gone
	exists, err = cat.CheckTableExists(context.TODO(), TableIdentifier(dbName, tableName))
	assert.NoError(err)
	assert.False(exists)
}

func TestHiveIntegrationCheckViewExists(t *testing.T) {
	assert := require.New(t)

	cat := createTestCatalog(t)
	defer cat.Close()

	dbName := fmt.Sprintf("test_db_%d", time.Now().UnixNano())

	err := cat.CreateNamespace(context.TODO(), DatabaseIdentifier(dbName), iceberg.Properties{
		"location": getTestTableLocation() + "/" + dbName,
	})
	assert.NoError(err)
	defer cat.DropNamespace(context.TODO(), DatabaseIdentifier(dbName))

	// Non-existent view returns false
	exists, err := cat.CheckViewExists(context.TODO(), TableIdentifier(dbName, "nonexistent_view"))
	assert.NoError(err)
	assert.False(exists)

	// Create a table and ensure CheckViewExists returns false for that name (it's a table, not a view)
	schema := iceberg.NewSchemaWithIdentifiers(0, []int{1},
		iceberg.NestedField{ID: 1, Name: "id", Type: iceberg.PrimitiveTypes.Int64, Required: true},
	)
	tableLocation := getTestTableLocation() + "/" + dbName + "/some_table"
	_, err = cat.CreateTable(context.TODO(), TableIdentifier(dbName, "some_table"), schema,
		catalog.WithLocation(tableLocation),
	)
	assert.NoError(err)
	defer cat.DropTable(context.TODO(), TableIdentifier(dbName, "some_table"))

	exists, err = cat.CheckViewExists(context.TODO(), TableIdentifier(dbName, "some_table"))
	assert.NoError(err)
	assert.False(exists)
}

func TestHiveIntegrationLoadViewNoSuchView(t *testing.T) {
	assert := require.New(t)

	cat := createTestCatalog(t)
	defer cat.Close()

	dbName := fmt.Sprintf("test_db_%d", time.Now().UnixNano())

	err := cat.CreateNamespace(context.TODO(), DatabaseIdentifier(dbName), iceberg.Properties{
		"location": getTestTableLocation() + "/" + dbName,
	})
	assert.NoError(err)
	defer cat.DropNamespace(context.TODO(), DatabaseIdentifier(dbName))

	_, err = cat.LoadView(context.TODO(), TableIdentifier(dbName, "nonexistent_view"))
	assert.Error(err)
	assert.True(errors.Is(err, catalog.ErrNoSuchView))
}

func TestHiveIntegrationDropViewNoSuchView(t *testing.T) {
	assert := require.New(t)

	cat := createTestCatalog(t)
	defer cat.Close()

	dbName := fmt.Sprintf("test_db_%d", time.Now().UnixNano())

	err := cat.CreateNamespace(context.TODO(), DatabaseIdentifier(dbName), iceberg.Properties{
		"location": getTestTableLocation() + "/" + dbName,
	})
	assert.NoError(err)
	defer cat.DropNamespace(context.TODO(), DatabaseIdentifier(dbName))

	err = cat.DropView(context.TODO(), TableIdentifier(dbName, "nonexistent_view"))
	assert.Error(err)
	assert.True(errors.Is(err, catalog.ErrNoSuchView))
}

func TestHiveIntegrationCreateView(t *testing.T) {
	assert := require.New(t)

	cat := createTestCatalog(t)
	defer cat.Close()

	dbName := fmt.Sprintf("test_db_%d", time.Now().UnixNano())
	viewName := "test_view"

	err := cat.CreateNamespace(context.TODO(), DatabaseIdentifier(dbName), iceberg.Properties{
		"location": getTestTableLocation() + "/" + dbName,
	})
	assert.NoError(err)
	defer cat.DropNamespace(context.TODO(), DatabaseIdentifier(dbName))

	schema := iceberg.NewSchema(1, iceberg.NestedField{ID: 1, Name: "col", Type: iceberg.PrimitiveTypes.Int32, Required: true})
	viewSQL := "SELECT 1 AS col"
	ver, err := view.NewVersionFromSQL(1, 0, viewSQL, table.Identifier{dbName})
	assert.NoError(err)

	viewLocation := getTestTableLocation() + "/" + dbName + "/" + viewName
	v, err := cat.CreateView(context.TODO(), TableIdentifier(dbName, viewName), ver, schema,
		catalog.WithViewLocation(viewLocation),
	)
	assert.NoError(err)
	assert.NotNil(v)
	defer cat.DropView(context.TODO(), TableIdentifier(dbName, viewName))

	exists, err := cat.CheckViewExists(context.TODO(), TableIdentifier(dbName, viewName))
	assert.NoError(err)
	assert.True(exists)

	loaded, err := cat.LoadView(context.TODO(), TableIdentifier(dbName, viewName))
	assert.NoError(err)
	assert.NotNil(loaded)
	assert.True(schema.Equals(loaded.CurrentSchema()))
	assert.Len(loaded.CurrentVersion().Representations, 1)
	assert.Equal("sql", loaded.CurrentVersion().Representations[0].Type)
	assert.Equal(viewSQL, loaded.CurrentVersion().Representations[0].Sql)
}

func TestHiveIntegrationCreateViewThenDrop(t *testing.T) {
	assert := require.New(t)

	cat := createTestCatalog(t)
	defer cat.Close()

	dbName := fmt.Sprintf("test_db_%d", time.Now().UnixNano())
	viewName := "view_to_drop"

	err := cat.CreateNamespace(context.TODO(), DatabaseIdentifier(dbName), iceberg.Properties{
		"location": getTestTableLocation() + "/" + dbName,
	})
	assert.NoError(err)
	defer cat.DropNamespace(context.TODO(), DatabaseIdentifier(dbName))

	schema := iceberg.NewSchema(1, iceberg.NestedField{ID: 1, Name: "id", Type: iceberg.PrimitiveTypes.Int64, Required: true})
	ver, _ := view.NewVersionFromSQL(1, 0, "SELECT 1 AS id", table.Identifier{dbName})

	viewLocation := getTestTableLocation() + "/" + dbName + "/" + viewName
	_, err = cat.CreateView(context.TODO(), TableIdentifier(dbName, viewName), ver, schema,
		catalog.WithViewLocation(viewLocation),
	)
	assert.NoError(err)

	exists, err := cat.CheckViewExists(context.TODO(), TableIdentifier(dbName, viewName))
	assert.NoError(err)
	assert.True(exists)

	err = cat.DropView(context.TODO(), TableIdentifier(dbName, viewName))
	assert.NoError(err)

	exists, err = cat.CheckViewExists(context.TODO(), TableIdentifier(dbName, viewName))
	assert.NoError(err)
	assert.False(exists)

	_, err = cat.LoadView(context.TODO(), TableIdentifier(dbName, viewName))
	assert.Error(err)
	assert.True(errors.Is(err, catalog.ErrNoSuchView))
}

func TestHiveIntegrationCreateView_TableConflict(t *testing.T) {
	assert := require.New(t)

	cat := createTestCatalog(t)
	defer cat.Close()

	dbName := fmt.Sprintf("test_db_%d", time.Now().UnixNano())
	tableName := "t1"

	err := cat.CreateNamespace(context.TODO(), DatabaseIdentifier(dbName), iceberg.Properties{
		"location": getTestTableLocation() + "/" + dbName,
	})
	assert.NoError(err)
	defer cat.DropNamespace(context.TODO(), DatabaseIdentifier(dbName))

	schema := iceberg.NewSchema(1, iceberg.NestedField{ID: 1, Name: "id", Type: iceberg.PrimitiveTypes.Int64, Required: true})
	tableLocation := getTestTableLocation() + "/" + dbName + "/" + tableName
	_, err = cat.CreateTable(context.TODO(), TableIdentifier(dbName, tableName), schema,
		catalog.WithLocation(tableLocation),
	)
	assert.NoError(err)
	defer cat.DropTable(context.TODO(), TableIdentifier(dbName, tableName))

	ver, _ := view.NewVersionFromSQL(1, 0, "SELECT * FROM t1", table.Identifier{dbName})
	viewLocation := getTestTableLocation() + "/" + dbName + "/" + tableName + "_view"
	_, err = cat.CreateView(context.TODO(), TableIdentifier(dbName, tableName), ver, schema,
		catalog.WithViewLocation(viewLocation),
	)
	assert.Error(err)
	assert.True(errors.Is(err, catalog.ErrTableAlreadyExists))
}

func TestHiveIntegrationCreateView_ViewConflict(t *testing.T) {
	assert := require.New(t)

	cat := createTestCatalog(t)
	defer cat.Close()

	dbName := fmt.Sprintf("test_db_%d", time.Now().UnixNano())
	viewName := "v1"

	err := cat.CreateNamespace(context.TODO(), DatabaseIdentifier(dbName), iceberg.Properties{
		"location": getTestTableLocation() + "/" + dbName,
	})
	assert.NoError(err)
	defer cat.DropNamespace(context.TODO(), DatabaseIdentifier(dbName))

	schema := iceberg.NewSchema(1, iceberg.NestedField{ID: 1, Name: "col", Type: iceberg.PrimitiveTypes.Int32, Required: true})
	ver, _ := view.NewVersionFromSQL(1, 0, "SELECT 1 AS col", table.Identifier{dbName})
	viewLocation := getTestTableLocation() + "/" + dbName + "/" + viewName

	_, err = cat.CreateView(context.TODO(), TableIdentifier(dbName, viewName), ver, schema,
		catalog.WithViewLocation(viewLocation),
	)
	assert.NoError(err)
	defer cat.DropView(context.TODO(), TableIdentifier(dbName, viewName))

	_, err = cat.CreateView(context.TODO(), TableIdentifier(dbName, viewName), ver, schema,
		catalog.WithViewLocation(viewLocation+"/second"),
	)
	assert.Error(err)
	assert.True(errors.Is(err, catalog.ErrViewAlreadyExists))
}
