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

package catalog_test

import (
	"context"
	"database/sql"
	"os"
	"path/filepath"
	"testing"

	"github.com/apache/iceberg-go"
	"github.com/apache/iceberg-go/catalog"
	"github.com/apache/iceberg-go/table"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/suite"
	"github.com/uptrace/bun/driver/sqliteshim"
)

var (
	tableSchemaNested = iceberg.NewSchemaWithIdentifiers(1,
		[]int{1},
		iceberg.NestedField{
			ID: 1, Name: "foo", Type: iceberg.PrimitiveTypes.String, Required: false},
		iceberg.NestedField{
			ID: 2, Name: "bar", Type: iceberg.PrimitiveTypes.Int32, Required: true},
		iceberg.NestedField{
			ID: 3, Name: "baz", Type: iceberg.PrimitiveTypes.Bool, Required: false},
		iceberg.NestedField{
			ID: 4, Name: "qux", Required: true, Type: &iceberg.ListType{
				ElementID: 5, Element: iceberg.PrimitiveTypes.String, ElementRequired: true}},
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
			Required: true},
		iceberg.NestedField{
			ID: 11, Name: "location", Type: &iceberg.ListType{
				ElementID: 12, Element: &iceberg.StructType{
					FieldList: []iceberg.NestedField{
						{ID: 13, Name: "latitude", Type: iceberg.PrimitiveTypes.Float32, Required: false},
						{ID: 14, Name: "longitude", Type: iceberg.PrimitiveTypes.Float32, Required: false},
					},
				},
				ElementRequired: true},
			Required: true},
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
)

func TestCreateSQLCatalogNoDriverDialect(t *testing.T) {
	_, err := catalog.Load("sql", iceberg.Properties{})
	assert.Error(t, err)

	_, err = catalog.Load("sql", iceberg.Properties{catalog.SqlDriverKey: "sqlite"})
	assert.Error(t, err)
}

func TestInvalidDialect(t *testing.T) {
	_, err := catalog.Load("sql", iceberg.Properties{
		catalog.SqlDriverKey:  sqliteshim.ShimName,
		catalog.SqlDialectKey: "foobar",
	})
	assert.Error(t, err)
}

type SqliteCatalogTestSuite struct {
	suite.Suite

	warehouse string
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

func (s *SqliteCatalogTestSuite) loadCatalogForTableCreation() *catalog.SQLCatalog {
	cat, err := catalog.Load("sql", iceberg.Properties{
		"uri":                 s.catalogUri(),
		catalog.SqlDriverKey:  sqliteshim.ShimName,
		catalog.SqlDialectKey: string(catalog.SQLite),
		"catalog.name":        "default",
		"init_catalog_tables": "true",
	})
	s.Require().NoError(err)

	return cat.(*catalog.SQLCatalog)
}

func (s *SqliteCatalogTestSuite) TearDownTest() {
	s.Require().NoError(os.RemoveAll(s.warehouse))
}

func (s *SqliteCatalogTestSuite) getDB() *sql.DB {
	sqldb, err := sql.Open(sqliteshim.ShimName, s.catalogUri())
	s.Require().NoError(err)

	return sqldb
}

func (s *SqliteCatalogTestSuite) getCatalogMemory() *catalog.SQLCatalog {
	cat, err := catalog.Load("sql", iceberg.Properties{
		"uri":                 ":memory:",
		catalog.SqlDriverKey:  sqliteshim.ShimName,
		catalog.SqlDialectKey: string(catalog.SQLite),
		"catalog.name":        "default",
		"warehouse":           s.warehouse,
	})
	s.Require().NoError(err)

	return cat.(*catalog.SQLCatalog)
}

func (s *SqliteCatalogTestSuite) getCatalogSqlite() *catalog.SQLCatalog {
	cat, err := catalog.Load("sql", iceberg.Properties{
		"uri":                 s.catalogUri(),
		catalog.SqlDriverKey:  sqliteshim.ShimName,
		catalog.SqlDialectKey: string(catalog.SQLite),
		"catalog.name":        "default",
		"warehouse":           s.warehouse,
	})
	s.Require().NoError(err)

	return cat.(*catalog.SQLCatalog)
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

func (s *SqliteCatalogTestSuite) TestCreateTablesIdempotency() {
	catalogs := []*catalog.SQLCatalog{s.getCatalogMemory(), s.getCatalogSqlite()}

	ctx := context.Background()
	for _, cat := range catalogs {
		cat.CreateSQLTables(ctx)
		cat.CreateSQLTables(ctx)
	}
}

func (s *SqliteCatalogTestSuite) TestCreateTableNonExistingNamespace() {
	catalogs := []*catalog.SQLCatalog{s.getCatalogMemory(), s.getCatalogSqlite()}

	ctx := context.Background()
	for _, cat := range catalogs {
		_, err := cat.CreateTable(ctx, table.Identifier{"default", "non_existing_namespace"}, tableSchemaNested)
		s.ErrorIs(err, catalog.ErrNoSuchNamespace)
	}
}

func (s *SqliteCatalogTestSuite) TestCreateTableDefaultSortOrder() {
	tests := []struct {
		cat   *catalog.SQLCatalog
		tblID table.Identifier
	}{
		{s.getCatalogMemory(), table.Identifier{"default", "random_table_identifier"}},
		{s.getCatalogSqlite(), table.Identifier{"default", "random_hierarchical_identifier"}},
	}

	for _, tt := range tests {
		ns := catalog.NamespaceFromIdent(tt.tblID)
		s.Require().NoError(tt.cat.CreateNamespace(context.Background(), ns, nil))
		tbl, err := tt.cat.CreateTable(context.Background(), tt.tblID, tableSchemaNested)
		s.Require().NoError(err)

		s.Equal(0, tbl.SortOrder().OrderID)
		s.NoError(tt.cat.DropTable(context.Background(), tt.tblID))
	}
}

func (s *SqliteCatalogTestSuite) TestCreateV1Table() {
	tests := []struct {
		cat   *catalog.SQLCatalog
		tblID table.Identifier
	}{
		{s.getCatalogMemory(), table.Identifier{"default", "random_table_identifier"}},
		{s.getCatalogSqlite(), table.Identifier{"default", "random_hierarchical_identifier"}},
	}

	for _, tt := range tests {
		ns := catalog.NamespaceFromIdent(tt.tblID)
		s.Require().NoError(tt.cat.CreateNamespace(context.Background(), ns, nil))
		tbl, err := tt.cat.CreateTable(context.Background(), tt.tblID, tableSchemaNested,
			catalog.WithProperties(iceberg.Properties{"format-version": "1"}))
		s.Require().NoError(err)

		s.Equal(0, tbl.SortOrder().OrderID)
		s.Equal(1, tbl.Metadata().Version())
		s.True(tbl.Spec().Equals(*iceberg.UnpartitionedSpec))
		s.NoError(tt.cat.DropTable(context.Background(), tt.tblID))
	}
}

func (s *SqliteCatalogTestSuite) TestCreateTableCustomSortOrder() {
	tests := []struct {
		cat   *catalog.SQLCatalog
		tblID table.Identifier
	}{
		{s.getCatalogMemory(), table.Identifier{"default", "random_table_identifier"}},
		{s.getCatalogSqlite(), table.Identifier{"default", "random_hierarchical_identifier"}},
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
		cat   *catalog.SQLCatalog
		tblID table.Identifier
	}{
		{s.getCatalogMemory(), table.Identifier{"default", "random_table_identifier"}},
		{s.getCatalogSqlite(), table.Identifier{"default", "random_hierarchical_identifier"}},
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

func TestSqlCatalog(t *testing.T) {
	suite.Run(t, new(SqliteCatalogTestSuite))
}
