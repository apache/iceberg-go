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

package sql_test

import (
	"context"
	"database/sql"
	"fmt"
	"maps"
	"math/rand/v2"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"

	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/arrow-go/v18/parquet/pqarrow"
	"github.com/apache/iceberg-go"
	"github.com/apache/iceberg-go/catalog"
	"github.com/apache/iceberg-go/catalog/internal"
	sqlcat "github.com/apache/iceberg-go/catalog/sql"
	"github.com/apache/iceberg-go/table"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/suite"
	"github.com/uptrace/bun/driver/sqliteshim"
)

var tableSchemaNested = iceberg.NewSchemaWithIdentifiers(1,
	[]int{1},
	iceberg.NestedField{
		ID: 1, Name: "foo", Type: iceberg.PrimitiveTypes.String, Required: false,
	},
	iceberg.NestedField{
		ID: 2, Name: "bar", Type: iceberg.PrimitiveTypes.Int32, Required: true,
	},
	iceberg.NestedField{
		ID: 3, Name: "baz", Type: iceberg.PrimitiveTypes.Bool, Required: false,
	},
	iceberg.NestedField{
		ID: 4, Name: "qux", Required: true, Type: &iceberg.ListType{
			ElementID: 5, Element: iceberg.PrimitiveTypes.String, ElementRequired: true,
		},
	},
	iceberg.NestedField{
		ID: 6, Name: "quux",
		Type: &iceberg.MapType{
			KeyID:   7,
			KeyType: iceberg.PrimitiveTypes.String,
			ValueID: 8,
			ValueType: &iceberg.MapType{
				KeyID:         9,
				KeyType:       iceberg.PrimitiveTypes.String,
				ValueID:       10,
				ValueType:     iceberg.PrimitiveTypes.Int32,
				ValueRequired: true,
			},
			ValueRequired: true,
		},
		Required: true,
	},
	iceberg.NestedField{
		ID: 11, Name: "location", Type: &iceberg.ListType{
			ElementID: 12, Element: &iceberg.StructType{
				FieldList: []iceberg.NestedField{
					{ID: 13, Name: "latitude", Type: iceberg.PrimitiveTypes.Float32, Required: false},
					{ID: 14, Name: "longitude", Type: iceberg.PrimitiveTypes.Float32, Required: false},
				},
			},
			ElementRequired: true,
		},
		Required: true,
	},
	iceberg.NestedField{
		ID:   15,
		Name: "person",
		Type: &iceberg.StructType{
			FieldList: []iceberg.NestedField{
				{ID: 16, Name: "name", Type: iceberg.PrimitiveTypes.String, Required: false},
				{ID: 17, Name: "age", Type: iceberg.PrimitiveTypes.Int32, Required: true},
			},
		},
		Required: false,
	},
)

func TestCreateSQLCatalogNoDriverDialect(t *testing.T) {
	_, err := catalog.Load(context.Background(), "sql", iceberg.Properties{})
	assert.Error(t, err)

	_, err = catalog.Load(context.Background(), "sql", iceberg.Properties{sqlcat.DriverKey: "sqlite"})
	assert.Error(t, err)
}

func TestInvalidDialect(t *testing.T) {
	_, err := catalog.Load(context.Background(), "sql", iceberg.Properties{
		sqlcat.DriverKey:  sqliteshim.ShimName,
		sqlcat.DialectKey: "foobar",
	})
	assert.Error(t, err)
}

func randomString(n int) string {
	const letters = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ"
	var b strings.Builder
	b.Grow(n)
	for range n {
		b.WriteByte(letters[rand.IntN(len(letters))])
	}

	return b.String()
}

const randomLen = 20

func databaseName() string {
	return strings.ToLower("my-iceberg-db-" + randomString(randomLen))
}

func tableName() string {
	return strings.ToLower("my-iceberg-table-" + randomString(randomLen))
}

func hiearchicalNamespaceName() string {
	prefix := "my-iceberg-ns-"
	tag1 := randomString(randomLen)
	tag2 := randomString(randomLen)

	return strings.Join([]string{prefix + tag1, prefix + tag2}, ".")
}

type SqliteCatalogTestSuite struct {
	suite.Suite

	warehouse string
}

func (s *SqliteCatalogTestSuite) randomTableIdentifier() table.Identifier {
	dbname, tablename := databaseName(), tableName()
	s.Require().NoError(os.MkdirAll(filepath.Join(s.warehouse, dbname+".db", tablename, "metadata"), 0o755))

	return table.Identifier{dbname, tablename}
}

func (s *SqliteCatalogTestSuite) randomHierarchicalIdentifier() table.Identifier {
	hierarchicalNsName, tableName := hiearchicalNamespaceName(), tableName()
	s.Require().NoError(os.MkdirAll(filepath.Join(s.warehouse, hierarchicalNsName+".db", tableName, "metadata"), 0o755))

	return strings.Split(hierarchicalNsName+"."+tableName, ".")
}

func (s *SqliteCatalogTestSuite) SetupTest() {
	var err error
	s.warehouse, err = os.MkdirTemp(os.TempDir(), "test_sql_*")
	s.Require().NoError(err)
}

func (s *SqliteCatalogTestSuite) catalogUri() string {
	return "file://" + filepath.Join(s.warehouse, "sql-catalog.db")
}

func (s *SqliteCatalogTestSuite) confirmNoTables(db *sql.DB) {
	rows, err := db.Query("SELECT name FROM sqlite_master WHERE type='table'")
	s.Require().NoError(err)
	defer rows.Close()

	var tables []string
	for rows.Next() {
		var table string
		s.Require().NoError(rows.Scan(&table))
		tables = append(tables, table)
	}

	s.NotContains(tables, "iceberg_tables")
	s.NotContains(tables, "iceberg_namespace_properties")
}

func (s *SqliteCatalogTestSuite) confirmTablesExist(db *sql.DB) {
	rows, err := db.Query("SELECT name FROM sqlite_master WHERE type='table'")
	s.Require().NoError(err)
	defer rows.Close()

	var tables []string
	for rows.Next() {
		var table string
		s.Require().NoError(rows.Scan(&table))
		tables = append(tables, table)
	}

	s.Contains(tables, "iceberg_tables")
	s.Contains(tables, "iceberg_namespace_properties")
}

func (s *SqliteCatalogTestSuite) loadCatalogForTableCreation() *sqlcat.Catalog {
	cat, err := catalog.Load(context.Background(), "default", iceberg.Properties{
		"uri":                 s.catalogUri(),
		sqlcat.DriverKey:      sqliteshim.ShimName,
		sqlcat.DialectKey:     string(sqlcat.SQLite),
		"type":                "sql",
		"init_catalog_tables": "true",
	})
	s.Require().NoError(err)

	return cat.(*sqlcat.Catalog)
}

func (s *SqliteCatalogTestSuite) TearDownTest() {
	s.Require().NoError(os.RemoveAll(s.warehouse))
}

func (s *SqliteCatalogTestSuite) getDB() *sql.DB {
	sqldb, err := sql.Open(sqliteshim.ShimName, s.catalogUri())
	s.Require().NoError(err)

	return sqldb
}

func (s *SqliteCatalogTestSuite) getCatalogMemory() *sqlcat.Catalog {
	cat, err := catalog.Load(context.Background(), "default", iceberg.Properties{
		"uri":             ":memory:",
		sqlcat.DriverKey:  sqliteshim.ShimName,
		sqlcat.DialectKey: string(sqlcat.SQLite),
		"type":            "sql",
		"warehouse":       "file://" + s.warehouse,
	})
	s.Require().NoError(err)

	return cat.(*sqlcat.Catalog)
}

func (s *SqliteCatalogTestSuite) getCatalogSqlite() *sqlcat.Catalog {
	cat, err := catalog.Load(context.Background(), "default", iceberg.Properties{
		"uri":             s.catalogUri(),
		sqlcat.DriverKey:  sqliteshim.ShimName,
		sqlcat.DialectKey: string(sqlcat.SQLite),
		"type":            "sql",
		"warehouse":       "file://" + s.warehouse,
	})
	s.Require().NoError(err)

	return cat.(*sqlcat.Catalog)
}

func (s *SqliteCatalogTestSuite) TestSqlCatalogType() {
	catalogs := []*sqlcat.Catalog{s.getCatalogMemory(), s.getCatalogSqlite()}
	for _, cat := range catalogs {
		s.Equal(catalog.SQL, cat.CatalogType())
	}
}

func (s *SqliteCatalogTestSuite) TestCreationNoTablesExist() {
	sqldb := s.getDB()
	s.confirmNoTables(sqldb)

	_ = s.loadCatalogForTableCreation()

	s.confirmTablesExist(sqldb)
}

func (s *SqliteCatalogTestSuite) TestCreationOneTableExists() {
	sqldb := s.getDB()
	s.confirmNoTables(sqldb)

	_, err := sqldb.Exec(`CREATE TABLE "iceberg_tables" (
		"catalog_name" VARCHAR NOT NULL, 
		"table_namespace" VARCHAR NOT NULL, 
		"table_name" VARCHAR NOT NULL,
		"iceberg_type" VARCHAR NOT NULL DEFAULT 'TABLE',
		"metadata_location" VARCHAR, 
		"previous_metadata_location" VARCHAR, 
		PRIMARY KEY ("catalog_name", "table_namespace", "table_name"))`)
	s.Require().NoError(err)

	_ = s.loadCatalogForTableCreation()

	s.confirmTablesExist(sqldb)
}

func (s *SqliteCatalogTestSuite) TestCreationAllTablesExist() {
	sqldb := s.getDB()
	s.confirmNoTables(sqldb)

	_, err := sqldb.Exec(`CREATE TABLE "iceberg_tables" (
		"catalog_name" VARCHAR NOT NULL, 
		"table_namespace" VARCHAR NOT NULL, 
		"table_name" VARCHAR NOT NULL,
		"iceberg_type" VARCHAR,
		"metadata_location" VARCHAR, 
		"previous_metadata_location" VARCHAR, 
		PRIMARY KEY ("catalog_name", "table_namespace", "table_name"))`)
	s.Require().NoError(err)

	_, err = sqldb.Exec(`CREATE TABLE "iceberg_namespace_properties" (
		"catalog_name" VARCHAR NOT NULL, 
		"namespace" VARCHAR NOT NULL, 
		"property_key" VARCHAR NOT NULL, 
		"property_value" VARCHAR, 
		PRIMARY KEY ("catalog_name", "namespace", "property_key"))`)
	s.Require().NoError(err)

	_ = s.loadCatalogForTableCreation()

	s.confirmTablesExist(sqldb)
}

func (s *SqliteCatalogTestSuite) TestDropSQLTablesIdempotency() {
	sqldb := s.getDB()
	s.confirmNoTables(sqldb)

	cat := s.loadCatalogForTableCreation()
	s.confirmTablesExist(sqldb)

	s.NoError(cat.DropSQLTables(context.Background()))
	s.confirmNoTables(sqldb)
	s.NoError(cat.DropSQLTables(context.Background()))
}

func (s *SqliteCatalogTestSuite) TestCreateTablesIdempotency() {
	catalogs := []*sqlcat.Catalog{s.getCatalogMemory(), s.getCatalogSqlite()}

	ctx := context.Background()
	for _, cat := range catalogs {
		s.NoError(cat.CreateSQLTables(ctx))
		s.NoError(cat.CreateSQLTables(ctx))
	}
}

func (s *SqliteCatalogTestSuite) TestCreateTableNonExistingNamespace() {
	catalogs := []*sqlcat.Catalog{s.getCatalogMemory(), s.getCatalogSqlite()}

	ctx := context.Background()
	for _, cat := range catalogs {
		_, err := cat.CreateTable(ctx, table.Identifier{"default", "non_existing_namespace"}, tableSchemaNested)
		s.ErrorIs(err, catalog.ErrNoSuchNamespace)
	}
}

func (s *SqliteCatalogTestSuite) TestCreateTableDefaultSortOrder() {
	tests := []struct {
		cat   *sqlcat.Catalog
		tblID table.Identifier
	}{
		{s.getCatalogMemory(), s.randomTableIdentifier()},
		{s.getCatalogSqlite(), s.randomHierarchicalIdentifier()},
	}

	for _, tt := range tests {
		ns := catalog.NamespaceFromIdent(tt.tblID)
		s.Require().NoError(tt.cat.CreateNamespace(context.Background(), ns, nil))
		tbl, err := tt.cat.CreateTable(context.Background(), tt.tblID, tableSchemaNested)
		s.Require().NoError(err)

		s.FileExists(strings.TrimPrefix(tbl.MetadataLocation(), "file://"))

		s.Equal(0, tbl.SortOrder().OrderID)
		s.NoError(tt.cat.DropTable(context.Background(), tt.tblID))
	}
}

func (s *SqliteCatalogTestSuite) TestCreateV1Table() {
	tests := []struct {
		cat   *sqlcat.Catalog
		tblID table.Identifier
	}{
		{s.getCatalogMemory(), s.randomTableIdentifier()},
		{s.getCatalogSqlite(), s.randomHierarchicalIdentifier()},
	}

	for _, tt := range tests {
		ns := catalog.NamespaceFromIdent(tt.tblID)
		s.Require().NoError(tt.cat.CreateNamespace(context.Background(), ns, nil))
		tbl, err := tt.cat.CreateTable(context.Background(), tt.tblID, tableSchemaNested,
			catalog.WithProperties(iceberg.Properties{"format-version": "1"}))
		s.Require().NoError(err)

		s.FileExists(strings.TrimPrefix(tbl.MetadataLocation(), "file://"))
		s.Equal(0, tbl.SortOrder().OrderID)
		s.Equal(1, tbl.Metadata().Version())
		s.True(tbl.Spec().Equals(*iceberg.UnpartitionedSpec))
		s.NoError(tt.cat.DropTable(context.Background(), tt.tblID))
	}
}

func (s *SqliteCatalogTestSuite) TestCreateTableCustomSortOrder() {
	tests := []struct {
		cat   *sqlcat.Catalog
		tblID table.Identifier
	}{
		{s.getCatalogMemory(), s.randomTableIdentifier()},
		{s.getCatalogSqlite(), s.randomHierarchicalIdentifier()},
	}

	for _, tt := range tests {
		ns := catalog.NamespaceFromIdent(tt.tblID)
		s.Require().NoError(tt.cat.CreateNamespace(context.Background(), ns, nil))

		order := table.SortOrder{Fields: []table.SortField{
			{SourceID: 2, Transform: iceberg.IdentityTransform{}, NullOrder: table.NullsFirst},
		}}

		tbl, err := tt.cat.CreateTable(context.Background(), tt.tblID, tableSchemaNested,
			catalog.WithSortOrder(order))
		s.Require().NoError(err)

		s.FileExists(strings.TrimPrefix(tbl.MetadataLocation(), "file://"))
		s.Equal(1, tbl.SortOrder().OrderID)
		s.Len(tbl.SortOrder().Fields, 1)
		s.Equal(table.SortASC, tbl.SortOrder().Fields[0].Direction)
		s.Equal(table.NullsFirst, tbl.SortOrder().Fields[0].NullOrder)
		s.Equal("identity", tbl.SortOrder().Fields[0].Transform.String())
		s.NoError(tt.cat.DropTable(context.Background(), tt.tblID))
	}
}

func (s *SqliteCatalogTestSuite) TestCreateDuplicatedTable() {
	tests := []struct {
		cat   *sqlcat.Catalog
		tblID table.Identifier
	}{
		{s.getCatalogMemory(), s.randomTableIdentifier()},
		{s.getCatalogSqlite(), s.randomHierarchicalIdentifier()},
	}

	for _, tt := range tests {
		ns := catalog.NamespaceFromIdent(tt.tblID)
		s.Require().NoError(tt.cat.CreateNamespace(context.Background(), ns, nil))
		_, err := tt.cat.CreateTable(context.Background(), tt.tblID, tableSchemaNested,
			catalog.WithProperties(iceberg.Properties{"format-version": "1"}))
		s.Require().NoError(err)

		_, err = tt.cat.CreateTable(context.Background(), tt.tblID, tableSchemaNested)
		s.ErrorContains(err, "failed to create table")
	}
}

func (s *SqliteCatalogTestSuite) TestCreateTableWithGivenLocation() {
	tests := []struct {
		cat   *sqlcat.Catalog
		tblID table.Identifier
	}{
		{s.getCatalogMemory(), s.randomTableIdentifier()},
		{s.getCatalogSqlite(), s.randomHierarchicalIdentifier()},
	}

	for _, tt := range tests {
		ns := catalog.NamespaceFromIdent(tt.tblID)
		tblName := catalog.TableNameFromIdent(tt.tblID)

		location := fmt.Sprintf("file://%s/%s.db/%s-given",
			s.warehouse, tt.cat.Name(), tblName)

		s.Require().NoError(tt.cat.CreateNamespace(context.Background(), ns, nil))
		tbl, err := tt.cat.CreateTable(context.Background(), tt.tblID, tableSchemaNested,
			catalog.WithLocation(location+"/"))
		s.Require().NoError(err)

		s.Equal(tt.tblID, tbl.Identifier())
		s.True(strings.HasPrefix(tbl.MetadataLocation(), "file://"+s.warehouse))
		s.FileExists(strings.TrimPrefix(tbl.MetadataLocation(), "file://"))
		s.Equal(location, tbl.Location())
		s.NoError(tt.cat.DropTable(context.Background(), tt.tblID))
	}
}

func (s *SqliteCatalogTestSuite) TestCreateTableWithoutNamespace() {
	catalogs := []*sqlcat.Catalog{s.getCatalogMemory(), s.getCatalogSqlite()}
	tblName := table.Identifier{tableName()}

	for _, cat := range catalogs {
		_, err := cat.CreateTable(context.Background(), tblName, tableSchemaNested)
		s.ErrorIs(err, catalog.ErrNoSuchNamespace)
	}
}

func (s *SqliteCatalogTestSuite) TestListTables() {
	catalogs := []*sqlcat.Catalog{s.getCatalogMemory(), s.getCatalogSqlite()}
	tbl1, tbl2 := s.randomTableIdentifier(), s.randomHierarchicalIdentifier()

	for _, cat := range catalogs {
		ctx := context.Background()
		ns1, ns2 := catalog.NamespaceFromIdent(tbl1), catalog.NamespaceFromIdent(tbl2)
		s.Require().NoError(cat.CreateNamespace(ctx, ns1, nil))
		s.Require().NoError(cat.CreateNamespace(ctx, ns2, nil))

		_, err := cat.CreateTable(ctx, tbl1, tableSchemaNested)
		s.Require().NoError(err)
		_, err = cat.CreateTable(ctx, tbl2, tableSchemaNested)
		s.Require().NoError(err)

		var lastErr error
		tables := make([]table.Identifier, 0)
		iter := cat.ListTables(ctx, ns1)
		for tbl, err := range iter {
			tables = append(tables, tbl)
			if err != nil {
				lastErr = err
			}
		}

		s.Require().NoError(lastErr)
		s.Len(tables, 1)
		s.Equal(tbl1, tables[0])

		tables2 := make([]table.Identifier, 0)
		iter = cat.ListTables(ctx, ns2)
		for tbl, err := range iter {
			tables2 = append(tables2, tbl)
			if err != nil {
				lastErr = err
			}
		}
		s.Require().NoError(lastErr)
		s.Len(tables2, 1)
		s.Equal(tbl2, tables2[0])

		table3 := make([]table.Identifier, 0)
		iter = cat.ListTables(ctx, table.Identifier{"does_not_exist"})
		for tbl, err := range iter {
			table3 = append(table3, tbl)
			if err != nil {
				lastErr = err
			}
		}
		s.ErrorIs(lastErr, catalog.ErrNoSuchNamespace)
	}
}

func (s *SqliteCatalogTestSuite) TestListTablesMissingNamespace() {
	tests := []struct {
		cat       *sqlcat.Catalog
		namespace table.Identifier
	}{
		{s.getCatalogMemory(), table.Identifier{databaseName()}},
		{s.getCatalogSqlite(), strings.Split(hiearchicalNamespaceName(), ".")},
	}

	ctx := context.Background()
	for _, tt := range tests {
		_, err := tt.cat.ListNamespaces(ctx, tt.namespace)
		s.ErrorIs(err, catalog.ErrNoSuchNamespace)
	}
}

func (s *SqliteCatalogTestSuite) TestListNamespaces() {
	catalogs := []*sqlcat.Catalog{s.getCatalogMemory(), s.getCatalogSqlite()}
	tbl1, tbl2 := s.randomTableIdentifier(), s.randomHierarchicalIdentifier()

	for _, cat := range catalogs {
		ctx := context.Background()
		ns1, ns2 := catalog.NamespaceFromIdent(tbl1), catalog.NamespaceFromIdent(tbl2)
		s.Require().NoError(cat.CreateNamespace(ctx, ns1, nil))
		s.Require().NoError(cat.CreateNamespace(ctx, ns2, nil))

		nslist, err := cat.ListNamespaces(ctx, nil)
		s.Require().NoError(err)
		s.Len(nslist, 2)
		s.Contains(nslist, ns1)
		s.Contains(nslist, ns2)

		ns, err := cat.ListNamespaces(ctx, ns1)
		s.Require().NoError(err)
		s.Len(ns, 1)
		s.Equal(ns1, ns[0])

		ns, err = cat.ListNamespaces(ctx, ns2)
		s.Require().NoError(err)
		s.Len(ns, 1)
		s.Equal(ns2, ns[0])

		_, err = cat.ListNamespaces(ctx, table.Identifier{"does_not_exist"})
		s.ErrorIs(err, catalog.ErrNoSuchNamespace)

		ns, err = cat.ListNamespaces(ctx, nil)
		s.Require().NoError(err)
		s.Len(ns, 2)
		s.Equal(ns1, ns[0])
		s.Equal(ns2, ns[1])
	}
}

func (s *SqliteCatalogTestSuite) TestLoadTableFromSelfIdentifier() {
	tests := []struct {
		cat   *sqlcat.Catalog
		tblID table.Identifier
	}{
		{s.getCatalogMemory(), s.randomTableIdentifier()},
		{s.getCatalogSqlite(), s.randomHierarchicalIdentifier()},
	}

	for _, tt := range tests {
		ns := catalog.NamespaceFromIdent(tt.tblID)
		s.Require().NoError(tt.cat.CreateNamespace(context.Background(), ns, nil))
		table, err := tt.cat.CreateTable(context.Background(), tt.tblID, tableSchemaNested)
		s.Require().NoError(err)

		intermediate, err := tt.cat.LoadTable(context.Background(), tt.tblID)
		s.Require().NoError(err)
		s.Equal(intermediate.Identifier(), table.Identifier())

		loaded, err := tt.cat.LoadTable(context.Background(), intermediate.Identifier())
		s.Require().NoError(err)

		s.Equal(table.Identifier(), loaded.Identifier())
		s.Equal(table.MetadataLocation(), loaded.MetadataLocation())
		s.True(table.Metadata().Equals(loaded.Metadata()))
	}
}

func (s *SqliteCatalogTestSuite) TestDropTable() {
	tests := []struct {
		cat   *sqlcat.Catalog
		tblID table.Identifier
	}{
		{s.getCatalogMemory(), s.randomTableIdentifier()},
		{s.getCatalogSqlite(), s.randomHierarchicalIdentifier()},
	}

	for _, tt := range tests {
		ns := catalog.NamespaceFromIdent(tt.tblID)
		s.Require().NoError(tt.cat.CreateNamespace(context.Background(), ns, nil))
		table, err := tt.cat.CreateTable(context.Background(), tt.tblID, tableSchemaNested)
		s.Require().NoError(err)

		s.Equal(tt.tblID, table.Identifier())
		s.NoError(tt.cat.DropTable(context.Background(), tt.tblID))
		_, err = tt.cat.LoadTable(context.Background(), tt.tblID)
		s.ErrorIs(err, catalog.ErrNoSuchTable)

		_, err = tt.cat.LoadTable(context.Background(), table.Identifier())
		s.ErrorIs(err, catalog.ErrNoSuchTable)
	}
}

func (s *SqliteCatalogTestSuite) TestLoadTableNotExists() {
	catalogs := []*sqlcat.Catalog{s.getCatalogMemory(), s.getCatalogSqlite()}

	for _, cat := range catalogs {
		_, err := cat.LoadTable(context.Background(), s.randomTableIdentifier())
		s.ErrorIs(err, catalog.ErrNoSuchTable)
	}
}

func (s *SqliteCatalogTestSuite) TestLoadTableInvalidMetadata() {
	sqldb := s.getDB()
	cat := s.loadCatalogForTableCreation()

	_, err := sqldb.Exec(`INSERT INTO iceberg_tables (catalog_name, table_namespace, table_name)
			VALUES ('default', 'default', 'invalid_metadata')`)
	s.Require().NoError(err)

	_, err = cat.LoadTable(context.Background(), table.Identifier{"default", "invalid_metadata"})
	s.ErrorIs(err, catalog.ErrNoSuchTable)
}

func (s *SqliteCatalogTestSuite) TestDropTableNotExist() {
	catalogs := []*sqlcat.Catalog{s.getCatalogMemory(), s.getCatalogSqlite()}

	for _, cat := range catalogs {
		err := cat.DropTable(context.Background(), s.randomTableIdentifier())
		s.ErrorIs(err, catalog.ErrNoSuchTable)
	}
}

func (s *SqliteCatalogTestSuite) TestRenameTable() {
	tests := []struct {
		cat       *sqlcat.Catalog
		fromTable table.Identifier
		toTable   table.Identifier
	}{
		{s.getCatalogMemory(), s.randomTableIdentifier(), s.randomTableIdentifier()},
		{s.getCatalogSqlite(), s.randomHierarchicalIdentifier(), s.randomHierarchicalIdentifier()},
	}

	ctx := context.Background()
	for _, tt := range tests {
		fromNs := catalog.NamespaceFromIdent(tt.fromTable)
		toNs := catalog.NamespaceFromIdent(tt.toTable)
		s.Require().NoError(tt.cat.CreateNamespace(ctx, fromNs, nil))
		s.Require().NoError(tt.cat.CreateNamespace(ctx, toNs, nil))

		table, err := tt.cat.CreateTable(context.Background(), tt.fromTable, tableSchemaNested)
		s.Require().NoError(err)
		s.Equal(tt.fromTable, table.Identifier())

		renamed, err := tt.cat.RenameTable(ctx, tt.fromTable, tt.toTable)
		s.Require().NoError(err)

		s.Equal(tt.toTable, renamed.Identifier())
		s.Equal(table.MetadataLocation(), renamed.MetadataLocation())

		_, err = tt.cat.LoadTable(ctx, tt.fromTable)
		s.ErrorIs(err, catalog.ErrNoSuchTable)
	}
}

func (s *SqliteCatalogTestSuite) TestRenameTableToExisting() {
	tests := []struct {
		cat       *sqlcat.Catalog
		fromTable table.Identifier
		toTable   table.Identifier
	}{
		{s.getCatalogMemory(), s.randomTableIdentifier(), s.randomTableIdentifier()},
		{s.getCatalogSqlite(), s.randomHierarchicalIdentifier(), s.randomHierarchicalIdentifier()},
	}

	ctx := context.Background()
	for _, tt := range tests {
		fromNs := catalog.NamespaceFromIdent(tt.fromTable)
		toNs := catalog.NamespaceFromIdent(tt.toTable)
		s.Require().NoError(tt.cat.CreateNamespace(ctx, fromNs, nil))
		s.Require().NoError(tt.cat.CreateNamespace(ctx, toNs, nil))

		table, err := tt.cat.CreateTable(ctx, tt.fromTable, tableSchemaNested)
		s.Require().NoError(err)
		s.Equal(tt.fromTable, table.Identifier())

		table2, err := tt.cat.CreateTable(ctx, tt.toTable, tableSchemaNested)
		s.Require().NoError(err)
		s.Equal(tt.toTable, table2.Identifier())

		_, err = tt.cat.RenameTable(ctx, tt.fromTable, tt.toTable)
		s.ErrorIs(err, catalog.ErrTableAlreadyExists)
	}
}

func (s *SqliteCatalogTestSuite) TestRenameMissingTable() {
	tests := []struct {
		cat       *sqlcat.Catalog
		fromTable table.Identifier
		toTable   table.Identifier
	}{
		{s.getCatalogMemory(), s.randomTableIdentifier(), s.randomTableIdentifier()},
		{s.getCatalogSqlite(), s.randomHierarchicalIdentifier(), s.randomHierarchicalIdentifier()},
	}

	ctx := context.Background()
	for _, tt := range tests {
		toNs := catalog.NamespaceFromIdent(tt.toTable)
		s.Require().NoError(tt.cat.CreateNamespace(ctx, toNs, nil))

		_, err := tt.cat.RenameTable(ctx, tt.fromTable, tt.toTable)
		s.ErrorIs(err, catalog.ErrNoSuchTable)
	}
}

func (s *SqliteCatalogTestSuite) TestRenameToMissingNamespace() {
	tests := []struct {
		cat       *sqlcat.Catalog
		fromTable table.Identifier
		toTable   table.Identifier
	}{
		{s.getCatalogMemory(), s.randomTableIdentifier(), s.randomTableIdentifier()},
		{s.getCatalogSqlite(), s.randomHierarchicalIdentifier(), s.randomHierarchicalIdentifier()},
	}

	ctx := context.Background()
	for _, tt := range tests {
		fromNs := catalog.NamespaceFromIdent(tt.fromTable)
		s.Require().NoError(tt.cat.CreateNamespace(ctx, fromNs, nil))

		table, err := tt.cat.CreateTable(ctx, tt.fromTable, tableSchemaNested)
		s.Require().NoError(err)
		s.Equal(tt.fromTable, table.Identifier())

		_, err = tt.cat.RenameTable(ctx, tt.fromTable, tt.toTable)
		s.ErrorIs(err, catalog.ErrNoSuchNamespace)
	}
}

func (s *SqliteCatalogTestSuite) TestCreateDuplicateNamespace() {
	tests := []struct {
		cat       *sqlcat.Catalog
		namespace table.Identifier
	}{
		{s.getCatalogMemory(), table.Identifier{databaseName()}},
		{s.getCatalogSqlite(), strings.Split(hiearchicalNamespaceName(), ".")},
	}

	ctx := context.Background()
	for _, tt := range tests {
		s.Require().NoError(tt.cat.CreateNamespace(ctx, tt.namespace, nil))
		err := tt.cat.CreateNamespace(ctx, tt.namespace, nil)
		s.ErrorIs(err, catalog.ErrNamespaceAlreadyExists)
	}
}

func (s *SqliteCatalogTestSuite) TestCreateNamespaceSharingPrefix() {
	tests := []struct {
		cat       *sqlcat.Catalog
		namespace table.Identifier
	}{
		{s.getCatalogMemory(), table.Identifier{databaseName()}},
		{s.getCatalogSqlite(), strings.Split(hiearchicalNamespaceName(), ".")},
	}

	ctx := context.Background()
	for _, tt := range tests {
		s.Require().NoError(tt.cat.CreateNamespace(ctx, append(tt.namespace, "_1"), nil))
		s.Require().NoError(tt.cat.CreateNamespace(ctx, tt.namespace, nil))
		tt.namespace[len(tt.namespace)-1] = tt.namespace[len(tt.namespace)-1] + "_1"
		s.Require().NoError(tt.cat.CreateNamespace(ctx, tt.namespace, nil))
	}
}

func (s *SqliteCatalogTestSuite) TestCreateaNamespaceWithCommentAndLocation() {
	tests := []struct {
		cat       *sqlcat.Catalog
		namespace table.Identifier
	}{
		{s.getCatalogMemory(), table.Identifier{databaseName()}},
		{s.getCatalogSqlite(), strings.Split(hiearchicalNamespaceName(), ".")},
	}

	ctx := context.Background()
	for _, tt := range tests {
		testLocation := "/test/location"
		testProps := iceberg.Properties{
			"comment":  "this is a test description",
			"location": testLocation,
		}

		s.Require().NoError(tt.cat.CreateNamespace(ctx, tt.namespace, testProps))
		loadedList, err := tt.cat.ListNamespaces(ctx, nil)
		s.Require().NoError(err)
		s.Len(loadedList, 1)
		s.Equal(tt.namespace, loadedList[0])

		props, err := tt.cat.LoadNamespaceProperties(ctx, tt.namespace)
		s.Require().NoError(err)

		s.Equal(testProps["comment"], props["comment"])
		s.Equal(testProps["location"], props["location"])
	}
}

func (s *SqliteCatalogTestSuite) TestDropNamespace() {
	tests := []struct {
		cat   *sqlcat.Catalog
		tblID table.Identifier
	}{
		{s.getCatalogMemory(), s.randomTableIdentifier()},
		{s.getCatalogSqlite(), s.randomHierarchicalIdentifier()},
	}

	ctx := context.Background()
	for _, tt := range tests {
		ns := catalog.NamespaceFromIdent(tt.tblID)
		s.Require().NoError(tt.cat.CreateNamespace(ctx, ns, nil))

		nslist, err := tt.cat.ListNamespaces(ctx, nil)
		s.Require().NoError(err)
		s.Len(nslist, 1)
		s.Equal(ns, nslist[0])

		_, err = tt.cat.CreateTable(ctx, tt.tblID, tableSchemaNested)
		s.Require().NoError(err)

		err = tt.cat.DropNamespace(ctx, ns)
		s.ErrorIs(err, catalog.ErrNamespaceNotEmpty)

		s.Require().NoError(tt.cat.DropTable(ctx, tt.tblID))
		s.Require().NoError(tt.cat.DropNamespace(ctx, ns))

		nslist, err = tt.cat.ListNamespaces(ctx, nil)
		s.Require().NoError(err)
		s.Empty(nslist)
	}
}

func (s *SqliteCatalogTestSuite) TestDropNamespaceNotExist() {
	tests := []struct {
		cat       *sqlcat.Catalog
		namespace table.Identifier
	}{
		{s.getCatalogMemory(), table.Identifier{databaseName()}},
		{s.getCatalogSqlite(), strings.Split(hiearchicalNamespaceName(), ".")},
	}

	ctx := context.Background()
	for _, tt := range tests {
		err := tt.cat.DropNamespace(ctx, tt.namespace)
		s.ErrorIs(err, catalog.ErrNoSuchNamespace)
	}
}

func (s *SqliteCatalogTestSuite) TestLoadEmptyNamespaceProperties() {
	tests := []struct {
		cat       *sqlcat.Catalog
		namespace table.Identifier
	}{
		{s.getCatalogMemory(), table.Identifier{databaseName()}},
		{s.getCatalogSqlite(), strings.Split(hiearchicalNamespaceName(), ".")},
	}

	ctx := context.Background()
	for _, tt := range tests {
		s.Require().NoError(tt.cat.CreateNamespace(ctx, tt.namespace, nil))
		props, err := tt.cat.LoadNamespaceProperties(ctx, tt.namespace)
		s.Require().NoError(err)
		s.Equal(iceberg.Properties{"exists": "true"}, props)
	}
}

func (s *SqliteCatalogTestSuite) TestLoadNamespacePropertiesNonExisting() {
	catalogs := []*sqlcat.Catalog{s.getCatalogMemory(), s.getCatalogSqlite()}

	for _, cat := range catalogs {
		_, err := cat.LoadNamespaceProperties(context.Background(), table.Identifier{"does_not_exist"})
		s.ErrorIs(err, catalog.ErrNoSuchNamespace)
	}
}

func (s *SqliteCatalogTestSuite) TestUpdateNamespaceProperties() {
	tests := []struct {
		cat       *sqlcat.Catalog
		namespace table.Identifier
	}{
		{s.getCatalogMemory(), table.Identifier{databaseName()}},
		{s.getCatalogSqlite(), strings.Split(hiearchicalNamespaceName(), ".")},
	}

	ctx := context.Background()
	for _, tt := range tests {
		warehouseLocation := "/test/location"
		testProps := iceberg.Properties{
			"comment":        "this is a test description",
			"location":       warehouseLocation,
			"test_property1": "1",
			"test_property2": "2",
			"test_property3": "3",
		}

		removals := []string{"test_property1", "test_property2", "test_property3", "should_not_be_removed"}
		updates := iceberg.Properties{
			"test_property4": "4",
			"test_property5": "5",
			"comment":        "updated test description",
		}

		s.Require().NoError(tt.cat.CreateNamespace(ctx, tt.namespace, testProps))
		updateReport, err := tt.cat.UpdateNamespaceProperties(ctx, tt.namespace, removals, updates)
		s.Require().NoError(err)

		for k := range maps.Keys(updates) {
			s.Contains(updateReport.Updated, k)
		}

		for _, k := range removals {
			if k == "should_not_be_removed" {
				s.Contains(updateReport.Missing, k)
			} else {
				s.Contains(updateReport.Removed, k)
			}
		}

		props, err := tt.cat.LoadNamespaceProperties(ctx, tt.namespace)
		s.Require().NoError(err)
		s.Equal(iceberg.Properties{
			"comment":        "updated test description",
			"location":       warehouseLocation,
			"test_property4": "4",
			"test_property5": "5",
		}, props)
	}
}

func (s *SqliteCatalogTestSuite) TestCommitTable() {
	tests := []struct {
		cat   *sqlcat.Catalog
		tblID table.Identifier
	}{
		{s.getCatalogMemory(), s.randomTableIdentifier()},
		{s.getCatalogSqlite(), s.randomHierarchicalIdentifier()},
	}

	arrSchema, err := table.SchemaToArrowSchema(tableSchemaNested, nil, false, false)
	s.Require().NoError(err)

	table, err := array.TableFromJSON(memory.DefaultAllocator, arrSchema,
		[]string{`[
		{
			"foo": "foo_string",
			"bar": 123,
			"baz": true,
			"qux": ["a", "b", "c"],
			"quux": [{"key": "gopher", "value": [
				{"key": "golang", "value": "1337"}]}],
			"location": [{"latitude": 37.7749, "longitude": -122.4194}],
			"person": {"name": "gopher", "age": 10}
		}
	]`})
	s.Require().NoError(err)
	defer table.Release()

	pqfile := filepath.Join(s.warehouse, "test_commit_table_data", "test.parquet")
	s.Require().NoError(os.MkdirAll(filepath.Dir(pqfile), 0o777))
	f, err := os.Create(pqfile)
	s.Require().NoError(err)

	s.Require().NoError(pqarrow.WriteTable(table, f, table.NumRows(),
		nil, pqarrow.DefaultWriterProps()))

	ctx := context.Background()
	for _, tt := range tests {
		ns := catalog.NamespaceFromIdent(tt.tblID)
		s.Require().NoError(tt.cat.CreateNamespace(ctx, ns, nil))

		tbl, err := tt.cat.CreateTable(ctx, tt.tblID, tableSchemaNested)
		s.Require().NoError(err)
		originalMetadataLocation := tbl.MetadataLocation()
		originalLastUpdated := tbl.Metadata().LastUpdatedMillis()

		s.EqualValues(0, internal.ParseMetadataVersion(tbl.MetadataLocation()))
		s.EqualValues(0, tbl.Metadata().CurrentSchema().ID)

		tx := tbl.NewTransaction()
		s.Require().NoError(tx.AddFiles(ctx, []string{pqfile}, nil, false))
		updated, err := tx.Commit(ctx)
		s.Require().NoError(err)

		s.EqualValues(1, internal.ParseMetadataVersion(updated.MetadataLocation()))
		s.Greater(updated.Metadata().LastUpdatedMillis(), originalLastUpdated)
		logs := slices.Collect(updated.Metadata().PreviousFiles())
		s.Len(logs, 1)
		s.Equal(originalMetadataLocation, logs[0].MetadataFile)
		s.Equal(originalLastUpdated, logs[0].TimestampMs)
	}
}

func (s *SqliteCatalogTestSuite) TestCreateView() {
	db := s.getCatalogSqlite()
	s.Require().NoError(db.CreateSQLTables(context.Background()))

	nsName := databaseName()
	viewName := tableName()
	s.Require().NoError(db.CreateNamespace(context.Background(), []string{nsName}, nil))

	viewSQL := "SELECT * FROM test_table"
	schema := iceberg.NewSchema(1, iceberg.NestedField{
		ID: 1, Name: "id", Type: iceberg.PrimitiveTypes.Int32, Required: true,
	})
	s.Require().NoError(db.CreateView(context.Background(), []string{nsName, viewName}, schema, viewSQL, nil))

	exists, err := db.CheckViewExists(context.Background(), []string{nsName, viewName})
	s.Require().NoError(err)
	s.True(exists)
}

func (s *SqliteCatalogTestSuite) TestDropView() {
	db := s.getCatalogSqlite()
	s.Require().NoError(db.CreateSQLTables(context.Background()))

	nsName := databaseName()
	viewName := tableName()
	s.Require().NoError(db.CreateNamespace(context.Background(), []string{nsName}, nil))

	viewSQL := "SELECT * FROM test_table"
	schema := iceberg.NewSchema(1, iceberg.NestedField{
		ID: 1, Name: "id", Type: iceberg.PrimitiveTypes.Int32, Required: true,
	})
	s.Require().NoError(db.CreateView(context.Background(), []string{nsName, viewName}, schema, viewSQL, nil))

	exists, err := db.CheckViewExists(context.Background(), []string{nsName, viewName})
	s.Require().NoError(err)
	s.True(exists)

	s.Require().NoError(db.DropView(context.Background(), []string{nsName, viewName}))

	exists, err = db.CheckViewExists(context.Background(), []string{nsName, viewName})
	s.Require().NoError(err)
	s.False(exists)

	err = db.DropView(context.Background(), []string{nsName, "nonexistent"})
	s.Error(err)
	s.ErrorIs(err, catalog.ErrNoSuchView)
}

func (s *SqliteCatalogTestSuite) TestCheckViewExists() {
	db := s.getCatalogSqlite()
	s.Require().NoError(db.CreateSQLTables(context.Background()))

	nsName := databaseName()
	viewName := tableName()
	s.Require().NoError(db.CreateNamespace(context.Background(), []string{nsName}, nil))

	exists, err := db.CheckViewExists(context.Background(), []string{nsName, viewName})
	s.Require().NoError(err)
	s.False(exists)

	viewSQL := "SELECT * FROM test_table"
	schema := iceberg.NewSchema(1, iceberg.NestedField{
		ID: 1, Name: "id", Type: iceberg.PrimitiveTypes.Int32, Required: true,
	})
	s.Require().NoError(db.CreateView(context.Background(), []string{nsName, viewName}, schema, viewSQL, nil))

	exists, err = db.CheckViewExists(context.Background(), []string{nsName, viewName})
	s.Require().NoError(err)
	s.True(exists)
}

func (s *SqliteCatalogTestSuite) TestListViews() {
	db := s.getCatalogSqlite()
	s.Require().NoError(db.CreateSQLTables(context.Background()))

	nsName := databaseName()
	s.Require().NoError(db.CreateNamespace(context.Background(), []string{nsName}, nil))

	viewNames := []string{tableName(), tableName(), tableName()}
	for _, viewName := range viewNames {
		viewSQL := "SELECT * FROM test_table"
		schema := iceberg.NewSchema(1, iceberg.NestedField{
			ID: 1, Name: "id", Type: iceberg.PrimitiveTypes.Int32, Required: true,
		})
		s.Require().NoError(db.CreateView(context.Background(), []string{nsName, viewName}, schema, viewSQL, nil))
	}

	var foundViews []table.Identifier
	viewsIter := db.ListViews(context.Background(), []string{nsName})
	for view, err := range viewsIter {
		s.Require().NoError(err)
		foundViews = append(foundViews, view)
	}

	s.Require().Len(foundViews, len(viewNames))
	for _, view := range foundViews {
		s.Equal(nsName, view[0])
		s.Contains(viewNames, view[1])
	}

	viewsIter = db.ListViews(context.Background(), []string{"nonexistent"})
	for _, err := range viewsIter {
		s.Error(err)
		s.ErrorIs(err, catalog.ErrNoSuchNamespace)

		break
	}
}

func (s *SqliteCatalogTestSuite) TestLoadView() {
	db := s.getCatalogSqlite()
	s.Require().NoError(db.CreateSQLTables(context.Background()))

	nsName := databaseName()
	viewName := tableName()
	s.Require().NoError(db.CreateNamespace(context.Background(), []string{nsName}, nil))

	viewSQL := "SELECT * FROM test_table"
	schema := iceberg.NewSchema(1, iceberg.NestedField{
		ID: 1, Name: "id", Type: iceberg.PrimitiveTypes.Int32, Required: true,
	})
	props := iceberg.Properties{
		"comment": "Test view",
		"owner":   "test-user",
	}
	s.Require().NoError(db.CreateView(context.Background(), []string{nsName, viewName}, schema, viewSQL, props))

	viewInfo, err := db.LoadView(context.Background(), []string{nsName, viewName})
	s.Require().NoError(err)

	s.Equal(viewName, viewInfo["name"])
	s.Equal(nsName, viewInfo["namespace"])

	_, err = db.LoadView(context.Background(), []string{nsName, "nonexistent"})
	s.Error(err)
	s.ErrorIs(err, catalog.ErrNoSuchView)
}

func TestSqlCatalog(t *testing.T) {
	suite.Run(t, new(SqliteCatalogTestSuite))
}
