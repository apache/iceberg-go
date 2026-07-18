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
	"encoding/json"
	"fmt"
	"maps"
	"math/rand/v2"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/arrow-go/v18/parquet/pqarrow"
	"github.com/apache/iceberg-go"
	"github.com/apache/iceberg-go/catalog"
	"github.com/apache/iceberg-go/catalog/internal"
	sqlcat "github.com/apache/iceberg-go/catalog/sql"
	_ "github.com/apache/iceberg-go/io/gocloud"
	"github.com/apache/iceberg-go/table"
	"github.com/google/uuid"
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
		"type":            "sql",
	})
	assert.ErrorContains(t, err, `unsupported sql dialect: "foobar"`)
}

func TestNewCatalogInvalidDialect(t *testing.T) {
	sqldb, err := sql.Open(sqliteshim.ShimName, ":memory:")
	assert.NoError(t, err)
	defer sqldb.Close()

	_, err = sqlcat.NewCatalog("default", sqldb, "foobar", nil)
	assert.ErrorContains(t, err, "unsupported sql dialect")
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

func (s *SqliteCatalogTestSuite) loadCatalogWithV1Migration() *sqlcat.Catalog {
	cat, err := catalog.Load(context.Background(), "default", iceberg.Properties{
		"uri":                   s.catalogUri(),
		sqlcat.DriverKey:        sqliteshim.ShimName,
		sqlcat.DialectKey:       string(sqlcat.SQLite),
		"type":                  "sql",
		"init_catalog_tables":   "true",
		sqlcat.SchemaVersionKey: sqlcat.SchemaVersionV1,
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

func (s *SqliteCatalogTestSuite) TestV0SchemaFromIcebergIsMigrated() {
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

	const (
		catName = "default"
		nsName  = "test_v0_ns"
		tblName = "test_v0_tbl"
		metaLoc = "file:///does/not/matter/00000-v0.metadata.json"
	)
	_, err = sqldb.Exec(
		`INSERT INTO "iceberg_namespace_properties" `+
			`("catalog_name", "namespace", "property_key", "property_value") VALUES (?, ?, ?, ?)`,
		catName, nsName, "exists", "true")
	s.Require().NoError(err)
	_, err = sqldb.Exec(
		`INSERT INTO "iceberg_tables" `+
			`("catalog_name", "table_namespace", "table_name", "metadata_location") VALUES (?, ?, ?, ?)`,
		catName, nsName, tblName, metaLoc)
	s.Require().NoError(err)

	_ = s.loadCatalogWithV1Migration()

	columnPresent := func() bool {
		var present int
		qErr := sqldb.QueryRow(
			`SELECT 1 FROM pragma_table_info('iceberg_tables') WHERE name = 'iceberg_type'`,
		).Scan(&present)
		s.Require().NoError(qErr, "migration must add iceberg_type column")

		return present == 1
	}
	s.True(columnPresent(), "migration must add iceberg_type column")

	rowType := func() sql.NullString {
		var got sql.NullString
		qErr := sqldb.QueryRow(
			`SELECT iceberg_type FROM iceberg_tables WHERE catalog_name = ? AND table_namespace = ? AND table_name = ?`,
			catName, nsName, tblName,
		).Scan(&got)
		s.Require().NoError(qErr)

		return got
	}
	s.False(rowType().Valid, "pre-V0 row should keep iceberg_type IS NULL after migration")

	// Re-opening with the same V1 opt-in must be idempotent: the column stays
	// present and the legacy row's iceberg_type is left NULL (not re-added or stamped).
	_ = s.loadCatalogWithV1Migration()
	s.True(columnPresent(), "column must remain present after a second open")
	s.False(rowType().Valid, "legacy row must remain NULL after a second open")
}

func (s *SqliteCatalogTestSuite) TestV0SchemaNotMigratedWithoutOptIn() {
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

	// Without jdbc.schema-version=V1, opening a legacy V0 catalog must NOT issue
	// the one-way ALTER; the column stays absent.
	_ = s.loadCatalogForTableCreation()

	var present int
	err = sqldb.QueryRow(
		`SELECT 1 FROM pragma_table_info('iceberg_tables') WHERE name = 'iceberg_type'`,
	).Scan(&present)
	s.ErrorIs(err, sql.ErrNoRows, "iceberg_type column must not be added without the V1 opt-in")
}

func (s *SqliteCatalogTestSuite) TestV0SchemaListAndDropAcceptNullIcebergType() {
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

	const (
		catName = "default"
		nsName  = "legacy_ns"
		tblName = "legacy_tbl"
		metaLoc = "file:///legacy/00000-v0.metadata.json"
	)
	_, err = sqldb.Exec(
		`INSERT INTO "iceberg_namespace_properties" `+
			`("catalog_name", "namespace", "property_key", "property_value") VALUES (?, ?, ?, ?)`,
		catName, nsName, "exists", "true")
	s.Require().NoError(err)
	_, err = sqldb.Exec(
		`INSERT INTO "iceberg_tables" `+
			`("catalog_name", "table_namespace", "table_name", "metadata_location") VALUES (?, ?, ?, ?)`,
		catName, nsName, tblName, metaLoc)
	s.Require().NoError(err)

	// Migrate to V1: this adds the iceberg_type column and leaves the legacy row
	// with iceberg_type IS NULL, which the read path must still tolerate.
	cat := s.loadCatalogWithV1Migration()
	ctx := context.Background()

	var listed []table.Identifier
	for ident, err := range cat.ListTables(ctx, table.Identifier{nsName}) {
		s.Require().NoError(err)
		listed = append(listed, ident)
	}
	s.Require().Len(listed, 1, "ListTables must return rows with iceberg_type IS NULL")
	s.Equal(tblName, listed[0][len(listed[0])-1])

	s.Require().NoError(cat.DropTable(ctx, table.Identifier{nsName, tblName}))

	var remaining int
	err = sqldb.QueryRow(
		`SELECT COUNT(*) FROM iceberg_tables WHERE catalog_name = ? AND table_namespace = ? AND table_name = ?`,
		catName, nsName, tblName,
	).Scan(&remaining)
	s.Require().NoError(err)
	s.Equal(0, remaining, "DropTable must delete rows with iceberg_type IS NULL")
}

func (s *SqliteCatalogTestSuite) createLegacyV0Catalog(sqldb *sql.DB) {
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
}

func (s *SqliteCatalogTestSuite) loadCatalogV0NoOptIn() *sqlcat.Catalog {
	cat, err := catalog.Load(context.Background(), "default", iceberg.Properties{
		"uri":                 s.catalogUri(),
		sqlcat.DriverKey:      sqliteshim.ShimName,
		sqlcat.DialectKey:     string(sqlcat.SQLite),
		"type":                "sql",
		"warehouse":           "file://" + s.warehouse,
		"init_catalog_tables": "false",
	})
	s.Require().NoError(err)

	return cat.(*sqlcat.Catalog)
}

func (s *SqliteCatalogTestSuite) loadCatalogV0NoInitWithV1Optin() *sqlcat.Catalog {
	cat, err := catalog.Load(context.Background(), "default", iceberg.Properties{
		"uri":                   s.catalogUri(),
		sqlcat.DriverKey:        sqliteshim.ShimName,
		sqlcat.DialectKey:       string(sqlcat.SQLite),
		"type":                  "sql",
		"warehouse":             "file://" + s.warehouse,
		"init_catalog_tables":   "false",
		sqlcat.SchemaVersionKey: sqlcat.SchemaVersionV1,
	})
	s.Require().NoError(err)

	return cat.(*sqlcat.Catalog)
}

func (s *SqliteCatalogTestSuite) icebergTypeColumnAbsent(sqldb *sql.DB) {
	var present int
	err := sqldb.QueryRow(
		`SELECT 1 FROM pragma_table_info('iceberg_tables') WHERE name = 'iceberg_type'`,
	).Scan(&present)
	s.ErrorIs(err, sql.ErrNoRows, "iceberg_type column must remain absent on a V0 catalog")
}

func (s *SqliteCatalogTestSuite) TestV0SchemaFullLifecycleWithoutOptIn() {
	sqldb := s.getDB()
	s.confirmNoTables(sqldb)
	s.createLegacyV0Catalog(sqldb)

	cat := s.loadCatalogV0NoOptIn()
	ctx := context.Background()

	s.icebergTypeColumnAbsent(sqldb)

	tblID := s.randomTableIdentifier()
	ns := catalog.NamespaceFromIdent(tblID)
	s.Require().NoError(cat.CreateNamespace(ctx, ns, nil))

	created, err := cat.CreateTable(ctx, tblID, tableSchemaNested)
	s.Require().NoError(err, "CreateTable must work on a column-less V0 schema")
	s.Equal(tblID, created.Identifier())

	loaded, err := cat.LoadTable(ctx, tblID)
	s.Require().NoError(err, "LoadTable must work on a column-less V0 schema")
	s.Equal(tblID, loaded.Identifier())

	var listed []table.Identifier
	for ident, iterErr := range cat.ListTables(ctx, ns) {
		s.Require().NoError(iterErr)
		listed = append(listed, ident)
	}
	s.Require().Len(listed, 1, "ListTables must work on a column-less V0 schema")
	s.Equal(tblID, listed[0])

	tx := loaded.NewTransaction()
	s.Require().NoError(tx.SetProperties(iceberg.Properties{"v0.commit.test": "ok"}))
	_, err = tx.Commit(ctx)
	s.Require().NoError(err, "CommitTable must work on a column-less V0 schema")

	toID := s.randomTableIdentifier()
	toNs := catalog.NamespaceFromIdent(toID)
	s.Require().NoError(cat.CreateNamespace(ctx, toNs, nil))

	renamed, err := cat.RenameTable(ctx, tblID, toID)
	s.Require().NoError(err, "RenameTable must work on a column-less V0 schema")
	s.Equal(toID, renamed.Identifier())

	s.Require().NoError(cat.DropTable(ctx, toID), "DropTable must work on a column-less V0 schema")
	_, err = cat.LoadTable(ctx, toID)
	s.ErrorIs(err, catalog.ErrNoSuchTable)

	s.icebergTypeColumnAbsent(sqldb)
}

func (s *SqliteCatalogTestSuite) TestV0SchemaViewOperationsUnsupported() {
	sqldb := s.getDB()
	s.confirmNoTables(sqldb)
	s.createLegacyV0Catalog(sqldb)

	cat := s.loadCatalogV0NoOptIn()
	ctx := context.Background()

	nsName := databaseName()
	s.Require().NoError(cat.CreateNamespace(ctx, []string{nsName}, nil))

	schema := iceberg.NewSchema(1, iceberg.NestedField{
		ID: 1, Name: "id", Type: iceberg.PrimitiveTypes.Int32, Required: true,
	})
	viewID := []string{nsName, tableName()}

	err := cat.CreateView(ctx, viewID, schema, "SELECT 1", nil)
	s.ErrorContains(err, "V0", "CreateView must be rejected on a V0 catalog")

	_, err = cat.LoadView(ctx, viewID)
	s.ErrorContains(err, "V0", "LoadView must be rejected on a V0 catalog")

	err = cat.DropView(ctx, viewID)
	s.ErrorContains(err, "V0", "DropView must be rejected on a V0 catalog")

	exists, err := cat.CheckViewExists(ctx, viewID)
	s.Require().NoError(err, "CheckViewExists must not error on a V0 catalog")
	s.False(exists, "a V0 catalog can hold no views")

	var listed []table.Identifier
	for ident, listErr := range cat.ListViews(ctx, []string{nsName}) {
		s.Require().NoError(listErr, "ListViews must return empty (not error) on a V0 catalog")
		listed = append(listed, ident)
	}
	s.Empty(listed, "a V0 catalog must list no views")

	s.icebergTypeColumnAbsent(sqldb)
}

func (s *SqliteCatalogTestSuite) TestV0InitDisabledDetectsV0AndMigratesWithOptIn() {
	sqldb := s.getDB()
	s.confirmNoTables(sqldb)
	s.createLegacyV0Catalog(sqldb)

	_ = s.loadCatalogV0NoOptIn()
	s.icebergTypeColumnAbsent(sqldb)

	_ = s.loadCatalogV0NoInitWithV1Optin()

	var present int
	err := sqldb.QueryRow(
		`SELECT 1 FROM pragma_table_info('iceberg_tables') WHERE name = 'iceberg_type'`,
	).Scan(&present)
	s.Require().NoError(err, "jdbc.schema-version=V1 must migrate even when init_catalog_tables=false")
	s.Equal(1, present)
}

func (s *SqliteCatalogTestSuite) TestGenuineV0RowSurvivesMigrationForCommitAndRename() {
	sqldb := s.getDB()
	s.confirmNoTables(sqldb)
	s.createLegacyV0Catalog(sqldb)
	ctx := context.Background()

	tblID := s.randomTableIdentifier()
	ns := catalog.NamespaceFromIdent(tblID)

	{
		v0 := s.loadCatalogV0NoOptIn()
		s.Require().NoError(v0.CreateNamespace(ctx, ns, nil))
		_, err := v0.CreateTable(ctx, tblID, tableSchemaNested)
		s.Require().NoError(err, "CreateTable must work on a column-less V0 schema")
	}
	s.icebergTypeColumnAbsent(sqldb)

	nsStr := strings.Join(ns, ".")
	rowType := func() sql.NullString {
		var got sql.NullString
		qErr := sqldb.QueryRow(
			`SELECT iceberg_type FROM iceberg_tables WHERE catalog_name = ? AND table_namespace = ? AND table_name = ?`,
			"default", nsStr, catalog.TableNameFromIdent(tblID),
		).Scan(&got)
		s.Require().NoError(qErr)

		return got
	}

	cat := s.loadCatalogWithV1Migration()
	s.False(rowType().Valid, "the migrated legacy row must keep iceberg_type IS NULL")

	loaded, err := cat.LoadTable(ctx, tblID)
	s.Require().NoError(err, "LoadTable must accept the post-migration NULL row")

	tx := loaded.NewTransaction()
	s.Require().NoError(tx.SetProperties(iceberg.Properties{"v0.migrated.commit": "ok"}))
	_, err = tx.Commit(ctx)
	s.Require().NoError(err, "CommitTable must accept the post-migration NULL row")

	healed := rowType()
	s.True(healed.Valid, "commit must heal the migrated NULL row to TABLE")
	s.Equal(sqlcat.TableType, healed.String)

	_, err = sqldb.Exec(
		`UPDATE iceberg_tables SET iceberg_type = NULL `+
			`WHERE catalog_name = ? AND table_namespace = ? AND table_name = ?`,
		"default", nsStr, catalog.TableNameFromIdent(tblID),
	)
	s.Require().NoError(err)

	toID := s.randomTableIdentifier()
	toNs := catalog.NamespaceFromIdent(toID)
	s.Require().NoError(cat.CreateNamespace(ctx, toNs, nil))

	renamed, err := cat.RenameTable(ctx, tblID, toID)
	s.Require().NoError(err, "RenameTable must accept the post-migration NULL row")
	s.Equal(toID, renamed.Identifier())
}

func (s *SqliteCatalogTestSuite) TestV1TableOpsIgnoreViewRows() {
	sqldb := s.getDB()
	cat := s.getCatalogSqlite()
	ctx := context.Background()

	nsName := databaseName()
	s.Require().NoError(cat.CreateNamespace(ctx, []string{nsName}, nil))

	viewName := tableName()
	_, err := sqldb.Exec(
		`INSERT INTO iceberg_tables `+
			`(catalog_name, table_namespace, table_name, iceberg_type, metadata_location) `+
			`VALUES (?, ?, ?, ?, ?)`,
		cat.Name(), nsName, viewName, sqlcat.ViewType, "file:///view/metadata.json",
	)
	s.Require().NoError(err)

	viewID := table.Identifier{nsName, viewName}

	_, err = cat.LoadTable(ctx, viewID)
	s.ErrorIs(err, catalog.ErrNoSuchTable, "LoadTable must not return a VIEW row")

	err = cat.DropTable(ctx, viewID)
	s.ErrorIs(err, catalog.ErrNoSuchTable, "DropTable must not delete a VIEW row")

	toID := table.Identifier{nsName, tableName()}
	_, err = cat.RenameTable(ctx, viewID, toID)
	s.ErrorIs(err, catalog.ErrNoSuchTable, "RenameTable must not rename a VIEW row")

	var cnt int
	s.Require().NoError(sqldb.QueryRow(
		`SELECT COUNT(*) FROM iceberg_tables `+
			`WHERE catalog_name = ? AND table_namespace = ? AND table_name = ? AND iceberg_type = ?`,
		cat.Name(), nsName, viewName, sqlcat.ViewType,
	).Scan(&cnt))
	s.Equal(1, cnt, "table operations must leave the VIEW row intact")

	for ident, listErr := range cat.ListTables(ctx, []string{nsName}) {
		s.Require().NoError(listErr)
		s.NotEqual(viewName, ident[len(ident)-1], "ListTables must exclude VIEW rows")
	}
}

func (s *SqliteCatalogTestSuite) TestCatalogNameMatchesLoaderArg() {
	ctx := context.Background()

	sharedURI := s.catalogUri()
	props := iceberg.Properties{
		"uri":             sharedURI,
		sqlcat.DriverKey:  sqliteshim.ShimName,
		sqlcat.DialectKey: string(sqlcat.SQLite),
		"type":            "sql",
		"warehouse":       "file://" + s.warehouse,
	}

	const catalogName = "test_catalog"
	cat, err := catalog.Load(ctx, catalogName, props)
	s.Require().NoError(err)
	sqlCat, ok := cat.(*sqlcat.Catalog)
	s.Require().True(ok, "expected *sqlcat.Catalog")
	s.Equal(catalogName, sqlCat.Name(), "SQL catalog must surface the name passed to catalog.Load")

	ns := table.Identifier{"test"}
	s.Require().NoError(sqlCat.CreateNamespace(ctx, ns, iceberg.Properties{"created_by": "iceberg-go"}))

	got, err := sqlCat.ListNamespaces(ctx, table.Identifier{})
	s.Require().NoError(err)
	s.Len(got, 1, "the loader-named catalog should see exactly the namespace it just created")
	s.Contains(got, ns, "iceberg-go must list a namespace it just created under its own catalog name")

	otherCat, err := catalog.Load(ctx, "sql", props)
	s.Require().NoError(err)
	otherSQLCat, ok := otherCat.(*sqlcat.Catalog)
	s.Require().True(ok, "expected *sqlcat.Catalog")
	s.Equal("sql", otherSQLCat.Name())

	otherNS, err := otherSQLCat.ListNamespaces(ctx, table.Identifier{})
	s.Require().NoError(err)
	s.NotContains(otherNS, ns,
		"a catalog loaded under a different name must not see namespaces written under another catalog_name")
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

		s.Equal(0, tbl.SortOrder().OrderID())
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
			catalog.WithProperties(iceberg.Properties{table.PropertyFormatVersion: "1"}))
		s.Require().NoError(err)

		s.FileExists(strings.TrimPrefix(tbl.MetadataLocation(), "file://"))
		s.Equal(0, tbl.SortOrder().OrderID())
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

		order, err := table.NewSortOrder(1, []table.SortField{
			{SourceIDs: []int{2}, Transform: iceberg.IdentityTransform{}, NullOrder: table.NullsFirst, Direction: table.SortASC},
		})
		s.Require().NoError(err)
		tbl, err := tt.cat.CreateTable(context.Background(), tt.tblID, tableSchemaNested,
			catalog.WithSortOrder(order))
		s.Require().NoError(err)

		s.FileExists(strings.TrimPrefix(tbl.MetadataLocation(), "file://"))
		s.Equal(1, tbl.SortOrder().OrderID())
		s.Equal(tbl.SortOrder().Len(), 1)

		for _, f := range tbl.SortOrder().Fields() {
			s.Equal(table.SortASC, f.Direction)
			s.Equal(table.NullsFirst, f.NullOrder)
			s.Equal("identity", f.Transform.String())

			break
		}

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
			catalog.WithProperties(iceberg.Properties{table.PropertyFormatVersion: "1"}))
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

func (s *SqliteCatalogTestSuite) TestTableOperationsRejectEmptyIdentifiers() {
	ctx := context.Background()
	valid := table.Identifier{"db", "table"}

	for _, cat := range []*sqlcat.Catalog{s.getCatalogMemory(), s.getCatalogSqlite()} {
		for _, ident := range []table.Identifier{nil, {}, {"table"}} {
			_, err := cat.CreateTable(ctx, ident, tableSchemaNested)
			s.ErrorIs(err, catalog.ErrNoSuchNamespace)

			_, _, err = cat.CommitTable(ctx, ident, nil, nil)
			s.ErrorIs(err, catalog.ErrNoSuchTable)
			_, err = cat.LoadTable(ctx, ident)
			s.ErrorIs(err, catalog.ErrNoSuchTable)
			s.ErrorIs(cat.DropTable(ctx, ident), catalog.ErrNoSuchTable)
			s.ErrorIs(cat.PurgeTable(ctx, ident), catalog.ErrNoSuchTable)
			_, err = cat.RenameTable(ctx, ident, valid)
			s.ErrorIs(err, catalog.ErrNoSuchTable)
			_, err = cat.RenameTable(ctx, valid, ident)
			s.ErrorIs(err, catalog.ErrNoSuchTable)
			_, err = cat.CheckTableExists(ctx, ident)
			s.ErrorIs(err, catalog.ErrNoSuchTable)
		}
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

func (s *SqliteCatalogTestSuite) TestDottedNamespaceComponentsRemainDistinct() {
	catalogs := []*sqlcat.Catalog{s.getCatalogMemory(), s.getCatalogSqlite()}
	tag := databaseName()

	for _, cat := range catalogs {
		ctx := context.Background()
		dotted := table.Identifier{"company_." + tag, "sales"}
		hierarchical := table.Identifier{"company_", tag, "sales"}
		dottedChild := append(slices.Clone(dotted), "regional")
		hierarchicalChild := append(slices.Clone(hierarchical), "regional")
		s.Require().NoError(cat.CreateNamespace(ctx, dotted, iceberg.Properties{"kind": "dotted"}))
		s.Require().NoError(cat.CreateNamespace(ctx, hierarchical, iceberg.Properties{"kind": "hierarchical"}))
		s.Require().NoError(cat.CreateNamespace(ctx, dottedChild, nil))
		s.Require().NoError(cat.CreateNamespace(ctx, hierarchicalChild, nil))

		namespaces, err := cat.ListNamespaces(ctx, nil)
		s.Require().NoError(err)
		s.Contains(namespaces, dotted)
		s.Contains(namespaces, hierarchical)
		s.Contains(namespaces, dottedChild)
		s.Contains(namespaces, hierarchicalChild)

		dottedNamespaces, err := cat.ListNamespaces(ctx, dotted)
		s.Require().NoError(err)
		s.ElementsMatch([]table.Identifier{dotted, dottedChild}, dottedNamespaces)
		hierarchicalNamespaces, err := cat.ListNamespaces(ctx, hierarchical)
		s.Require().NoError(err)
		s.ElementsMatch([]table.Identifier{hierarchical, hierarchicalChild}, hierarchicalNamespaces)

		dottedProps, err := cat.LoadNamespaceProperties(ctx, dotted)
		s.Require().NoError(err)
		s.Equal("dotted", dottedProps["kind"])
		hierarchicalProps, err := cat.LoadNamespaceProperties(ctx, hierarchical)
		s.Require().NoError(err)
		s.Equal("hierarchical", hierarchicalProps["kind"])

		dottedTable := append(slices.Clone(dotted), "orders")
		hierarchicalTable := append(slices.Clone(hierarchical), "orders")
		dottedCreated, err := cat.CreateTable(ctx, dottedTable, tableSchemaNested)
		s.Require().NoError(err)
		hierarchicalCreated, err := cat.CreateTable(ctx, hierarchicalTable, tableSchemaNested)
		s.Require().NoError(err)
		s.NotEqual(dottedCreated.Location(), hierarchicalCreated.Location())
		s.NotEqual(dottedCreated.MetadataLocation(), hierarchicalCreated.MetadataLocation())

		var dottedTables []table.Identifier
		for ident, listErr := range cat.ListTables(ctx, dotted) {
			s.Require().NoError(listErr)
			dottedTables = append(dottedTables, ident)
		}
		s.Equal([]table.Identifier{dottedTable}, dottedTables)
		var hierarchicalTables []table.Identifier
		for ident, listErr := range cat.ListTables(ctx, hierarchical) {
			s.Require().NoError(listErr)
			hierarchicalTables = append(hierarchicalTables, ident)
		}
		s.Equal([]table.Identifier{hierarchicalTable}, hierarchicalTables)

		renamed := append(slices.Clone(hierarchical), "renamed_orders")
		loaded, err := cat.RenameTable(ctx, dottedTable, renamed)
		s.Require().NoError(err)
		s.Equal(renamed, loaded.Identifier())
		_, err = cat.LoadTable(ctx, dottedTable)
		s.ErrorIs(err, catalog.ErrNoSuchTable)
		_, err = cat.LoadTable(ctx, hierarchicalTable)
		s.Require().NoError(err)

		s.Require().NoError(cat.DropTable(ctx, renamed))
		s.Require().NoError(cat.DropTable(ctx, hierarchicalTable))
		s.Require().NoError(cat.DropNamespace(ctx, dottedChild))
		s.Require().NoError(cat.DropNamespace(ctx, hierarchicalChild))
		s.Require().NoError(cat.DropNamespace(ctx, dotted))
		s.Require().NoError(cat.DropNamespace(ctx, hierarchical))
	}
}

func (s *SqliteCatalogTestSuite) TestDottedNamespaceUsesOwnLocationProperties() {
	catalogs := []*sqlcat.Catalog{s.getCatalogMemory(), s.getCatalogSqlite()}

	for i, cat := range catalogs {
		ctx := context.Background()
		namespace := table.Identifier{"company." + databaseName(), "sales"}
		location := "file://" + filepath.Join(s.warehouse, fmt.Sprintf("dotted-location-%d", i))
		s.Require().NoError(cat.CreateNamespace(ctx, namespace, iceberg.Properties{"location": location}))

		tableIdent := append(slices.Clone(namespace), "orders")
		createdTable, err := cat.CreateTable(ctx, tableIdent, tableSchemaNested)
		s.Require().NoError(err)
		s.Equal(location+"/orders", createdTable.Location())

		viewIdent := append(slices.Clone(namespace), "orders_view")
		s.Require().NoError(cat.CreateView(ctx, viewIdent, tableSchemaNested, "SELECT * FROM orders", nil))
		createdView, err := cat.LoadView(ctx, viewIdent)
		s.Require().NoError(err)
		s.Equal(location+"/orders_view", createdView.Location())

		s.Require().NoError(cat.DropView(ctx, viewIdent))
		s.Require().NoError(cat.DropTable(ctx, tableIdent))
		s.Require().NoError(cat.DropNamespace(ctx, namespace))
	}
}

func (s *SqliteCatalogTestSuite) TestLegacyNamespaceEncodingPrefixRemainsReadable() {
	ctx := context.Background()
	cat := s.getCatalogSqlite()
	db := s.getDB()
	defer db.Close()

	legacyName := "__iceberg_namespace_v1__:legacy_" + databaseName()
	_, err := db.ExecContext(ctx,
		`INSERT INTO "iceberg_namespace_properties" `+
			`("catalog_name", "namespace", "property_key", "property_value") VALUES (?, ?, ?, ?)`,
		cat.Name(), legacyName, "owner", "legacy")
	s.Require().NoError(err)

	namespaces, err := cat.ListNamespaces(ctx, nil)
	s.Require().NoError(err)
	s.Contains(namespaces, table.Identifier{legacyName})

	legacyIdent := table.Identifier{legacyName}
	exists, err := cat.CheckNamespaceExists(ctx, legacyIdent)
	s.Require().NoError(err)
	s.True(exists)
	children, err := cat.ListNamespaces(ctx, legacyIdent)
	s.Require().NoError(err)
	s.Contains(children, legacyIdent)
	props, err := cat.LoadNamespaceProperties(ctx, legacyIdent)
	s.Require().NoError(err)
	s.Equal("legacy", props["owner"])
	_, err = cat.UpdateNamespaceProperties(ctx, legacyIdent, nil, iceberg.Properties{"owner": "updated"})
	s.Require().NoError(err)
	props, err = cat.LoadNamespaceProperties(ctx, legacyIdent)
	s.Require().NoError(err)
	s.Equal("updated", props["owner"])

	tableIdent := append(slices.Clone(legacyIdent), "orders")
	created, err := cat.CreateTable(ctx, tableIdent, tableSchemaNested)
	s.Require().NoError(err)
	loaded, err := cat.LoadTable(ctx, tableIdent)
	s.Require().NoError(err)
	s.Equal(created.MetadataLocation(), loaded.MetadataLocation())

	s.Require().NoError(cat.DropTable(ctx, tableIdent))
	s.Require().NoError(cat.DropNamespace(ctx, legacyIdent))
}

func (s *SqliteCatalogTestSuite) TestListNamespacesEscapesLikeWildcards() {
	catalogs := []*sqlcat.Catalog{s.getCatalogMemory(), s.getCatalogSqlite()}
	tag := databaseName()

	for _, cat := range catalogs {
		ctx := context.Background()
		parent := table.Identifier{"sales%_!" + tag}
		child := append(slices.Clone(parent), "regional")
		// This namespace would match the unescaped SQL pattern for parent.
		decoy := table.Identifier{"salesZZX" + tag, "regional"}

		s.Require().NoError(cat.CreateNamespace(ctx, parent, nil))
		s.Require().NoError(cat.CreateNamespace(ctx, child, nil))
		s.Require().NoError(cat.CreateNamespace(ctx, decoy, nil))

		namespaces, err := cat.ListNamespaces(ctx, parent)
		s.Require().NoError(err)
		s.ElementsMatch([]table.Identifier{parent, child}, namespaces)

		s.Require().NoError(cat.DropNamespace(ctx, child))
		s.Require().NoError(cat.DropNamespace(ctx, parent))
		s.Require().NoError(cat.DropNamespace(ctx, decoy))
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

func (s *SqliteCatalogTestSuite) TestLoadTableRowAbsent() {
	cat := s.loadCatalogForTableCreation()

	_, err := cat.LoadTable(context.Background(), table.Identifier{"default", "does_not_exist"})
	s.ErrorIs(err, catalog.ErrNoSuchTable)
}

func (s *SqliteCatalogTestSuite) TestLoadTableInvalidMetadata() {
	sqldb := s.getDB()
	cat := s.loadCatalogForTableCreation()

	_, err := sqldb.Exec(`INSERT INTO iceberg_tables (catalog_name, table_namespace, table_name)
			VALUES ('default', 'default', 'invalid_metadata')`)
	s.Require().NoError(err)

	var metaLoc sql.NullString
	err = sqldb.QueryRow(
		`SELECT metadata_location FROM iceberg_tables ` +
			`WHERE catalog_name = 'default' AND table_namespace = 'default' AND table_name = 'invalid_metadata'`,
	).Scan(&metaLoc)
	s.Require().NoError(err, "the row must exist for this test to exercise the metadata-location branch")
	s.Require().False(metaLoc.Valid, "metadata_location must be NULL")

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

func (s *SqliteCatalogTestSuite) TestPurgeTable() {
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

		schema := iceberg.NewSchema(1, iceberg.NestedField{
			ID: 1, Name: "foo", Type: iceberg.PrimitiveTypes.String, Required: true,
		})
		tbl, err := tt.cat.CreateTable(context.Background(), tt.tblID, schema)
		s.Require().NoError(err)

		// Append data to create data files and manifest files
		arrowSchema := arrow.NewSchema([]arrow.Field{
			{Name: "foo", Type: arrow.BinaryTypes.String},
		}, nil)

		bldr := array.NewStringBuilder(memory.DefaultAllocator)
		bldr.Append("bar")
		arr := bldr.NewArray()

		rec := array.NewRecordBatch(arrowSchema, []arrow.Array{arr}, 1)
		arrTable := array.NewTableFromRecords(arrowSchema, []arrow.RecordBatch{rec})

		tx := tbl.NewTransaction()
		s.Require().NoError(tx.AppendTable(context.Background(), arrTable, 1024, nil))
		tbl, err = tx.Commit(context.Background())
		s.Require().NoError(err)

		arr.Release()
		bldr.Release()
		rec.Release()
		arrTable.Release()

		metaLoc := strings.TrimPrefix(tbl.MetadataLocation(), "file://")
		tableLoc := strings.TrimPrefix(tbl.Location(), "file://")
		s.FileExists(metaLoc)

		// Create a dummy statistics file at an external path outside the table Location()
		externalStatsPath := filepath.Join(s.warehouse, "external-path", "stats.puffin")
		s.Require().NoError(os.MkdirAll(filepath.Dir(externalStatsPath), 0o755))
		s.Require().NoError(os.WriteFile(externalStatsPath, []byte("dummy puffin data"), 0o644))
		s.FileExists(externalStatsPath)

		// Add this external statistics file to the table metadata JSON
		metaBytes, err := os.ReadFile(metaLoc)
		s.Require().NoError(err)
		var metaMap map[string]any
		s.Require().NoError(json.Unmarshal(metaBytes, &metaMap))
		metaMap["statistics"] = []any{
			map[string]any{
				"snapshot-id":               tbl.Metadata().CurrentSnapshot().SnapshotID,
				"statistics-path":           "file://" + externalStatsPath,
				"file-size-in-bytes":        17,
				"file-footer-size-in-bytes": 10,
				"blob-metadata":             []any{},
			},
		}
		newMetaBytes, err := json.Marshal(metaMap)
		s.Require().NoError(err)
		s.Require().NoError(os.WriteFile(metaLoc, newMetaBytes, 0o644))

		// Assert that the catalog implements PurgeableTable
		purger, ok := any(tt.cat).(catalog.PurgeableTable)
		s.Require().True(ok, "catalog must implement PurgeableTable")

		s.NoError(purger.PurgeTable(context.Background(), tt.tblID))

		// The table catalog entry should be gone
		_, err = tt.cat.LoadTable(context.Background(), tt.tblID)
		s.ErrorIs(err, catalog.ErrNoSuchTable)

		// The external file must also be purged!
		_, statErr := os.Stat(externalStatsPath)
		s.True(os.IsNotExist(statErr), "expected external file to be deleted: %s", externalStatsPath)

		// The physical files should be completely removed.
		// Note: PurgeTableFiles relies on ListableIO and BulkRemovableIO which operate on files, not directories.
		// As a result, empty directories (like data/ and metadata/) may be left behind on local filesystems.
		// This is expected and acceptable since object stores (like S3) do not have true directories.
		walkErr := filepath.Walk(tableLoc, func(path string, info os.FileInfo, err error) error {
			if os.IsNotExist(err) {
				return nil
			}
			s.Require().NoError(err)
			s.True(info.IsDir(), "expected only empty directories to remain, found file: %s", path)

			return nil
		})
		s.Require().NoError(walkErr)
	}
}

func (s *SqliteCatalogTestSuite) TestPurgeTableGCDisabled() {
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

		schema := iceberg.NewSchema(1, iceberg.NestedField{
			ID: 1, Name: "foo", Type: iceberg.PrimitiveTypes.String, Required: true,
		})
		tbl, err := tt.cat.CreateTable(context.Background(), tt.tblID, schema, catalog.WithProperties(iceberg.Properties{"gc.enabled": "false"}))
		s.Require().NoError(err)

		// Append data to create data files and manifest files
		arrowSchema := arrow.NewSchema([]arrow.Field{
			{Name: "foo", Type: arrow.BinaryTypes.String},
		}, nil)

		bldr := array.NewStringBuilder(memory.DefaultAllocator)
		bldr.Append("bar")
		arr := bldr.NewArray()

		rec := array.NewRecordBatch(arrowSchema, []arrow.Array{arr}, 1)
		arrTable := array.NewTableFromRecords(arrowSchema, []arrow.RecordBatch{rec})

		tx := tbl.NewTransaction()
		s.Require().NoError(tx.AppendTable(context.Background(), arrTable, 1024, nil))
		tbl, err = tx.Commit(context.Background())
		s.Require().NoError(err)

		arr.Release()
		bldr.Release()
		rec.Release()
		arrTable.Release()

		metaLoc := strings.TrimPrefix(tbl.MetadataLocation(), "file://")
		s.FileExists(metaLoc)

		purger, ok := any(tt.cat).(catalog.PurgeableTable)
		s.Require().True(ok, "catalog must implement PurgeableTable")

		s.NoError(purger.PurgeTable(context.Background(), tt.tblID))

		// The table catalog entry should be gone
		_, err = tt.cat.LoadTable(context.Background(), tt.tblID)
		s.ErrorIs(err, catalog.ErrNoSuchTable)

		// With gc.enabled=false, metadata files are still deleted, but data files are skipped.
		s.NoFileExists(metaLoc)
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

func (s *SqliteCatalogTestSuite) TestCommitTableWithStatisticsUpdate() {
	ctx := context.Background()
	cat := s.getCatalogSqlite()
	tblID := s.randomTableIdentifier()

	s.Require().NoError(cat.CreateNamespace(ctx, catalog.NamespaceFromIdent(tblID), nil))
	tbl, err := cat.CreateTable(ctx, tblID, tableSchemaNested)
	s.Require().NoError(err)

	_, metadataLocation, err := cat.CommitTable(ctx, tblID, nil, []table.Update{
		table.NewSetStatisticsUpdate(table.StatisticsFile{
			SnapshotID:     1,
			StatisticsPath: "file:///tmp/stats.puffin",
			BlobMetadata:   []table.BlobMetadata{},
		}),
		table.NewSetPartitionStatisticsUpdate(table.PartitionStatisticsFile{
			SnapshotID:     1,
			StatisticsPath: "file:///tmp/partition-stats.parquet",
		}),
	})
	s.Require().NoError(err)

	s.NotEqual(tbl.MetadataLocation(), metadataLocation)
	s.FileExists(strings.TrimPrefix(metadataLocation, "file://"))

	loaded, err := cat.LoadTable(ctx, tblID)
	s.Require().NoError(err)
	s.Len(slices.Collect(loaded.Metadata().Statistics()), 1)
	s.Len(slices.Collect(loaded.Metadata().PartitionStatistics()), 1)
}

func (s *SqliteCatalogTestSuite) TestCommitTableValidatesRequirementsForMissingTable() {
	ctx := context.Background()
	cat := s.getCatalogSqlite()

	snapshotID := int64(1)
	tests := []struct {
		name string
		req  table.Requirement
	}{
		{"table UUID", table.AssertTableUUID(uuid.New())},
		{"current schema ID", table.AssertCurrentSchemaID(0)},
		{"ref snapshot ID", table.AssertRefSnapshotID(table.MainBranch, &snapshotID)},
	}

	for _, tt := range tests {
		s.Run(tt.name, func() {
			tblID := s.randomTableIdentifier()
			s.Require().NoError(cat.CreateNamespace(ctx, catalog.NamespaceFromIdent(tblID), nil))
			location := "file://" + filepath.Join(s.warehouse, "requirement-check", tblID[1])

			_, _, err := cat.CommitTable(ctx, tblID, []table.Requirement{tt.req}, []table.Update{
				table.NewSetLocationUpdate(location),
			})
			s.Require().Error(err)
			s.Contains(err.Error(), "current table metadata does not exist")
		})
	}

	tblID := s.randomTableIdentifier()
	s.Require().NoError(cat.CreateNamespace(ctx, catalog.NamespaceFromIdent(tblID), nil))
	_, _, err := cat.CommitTable(ctx, tblID, []table.Requirement{table.AssertCreate()}, []table.Update{
		table.NewSetLocationUpdate("file://" + filepath.Join(s.warehouse, "create-via-commit", tblID[1])),
	})
	s.Require().NoError(err)
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

func (s *SqliteCatalogTestSuite) TestDropViewWithInvalidMetadataLocation() {
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

	// Manually update the metadata location to a URL with an unsupported scheme
	// This will cause io.LoadFS to fail with "IO for file '...' not implemented"
	sqldb := s.getDB()
	defer sqldb.Close()

	_, err := sqldb.Exec(
		"UPDATE iceberg_tables SET metadata_location = ? WHERE table_namespace = ? AND table_name = ?",
		"unsupported-scheme://bucket/metadata.json",
		nsName,
		viewName,
	)
	s.Require().NoError(err)

	// DropView should return an error when io.LoadFS fails
	err = db.DropView(context.Background(), []string{nsName, viewName})
	s.Error(err, "DropView should return an error when LoadFS fails for invalid metadata location")
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

	s.Equal(1, viewInfo.FormatVersion())
	s.Contains(viewInfo.Properties(), "comment")
	s.Equal("Test view", viewInfo.Properties()["comment"])

	_, err = db.LoadView(context.Background(), []string{nsName, "nonexistent"})
	s.Error(err)
	s.ErrorIs(err, catalog.ErrNoSuchView)
}

func TestSqlCatalog(t *testing.T) {
	suite.Run(t, new(SqliteCatalogTestSuite))
}
