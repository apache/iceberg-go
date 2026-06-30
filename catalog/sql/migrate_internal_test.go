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

package sql

import (
	"context"
	"database/sql"
	"errors"
	"strings"
	"testing"

	"github.com/apache/iceberg-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/uptrace/bun"
	"github.com/uptrace/bun/dialect"
	"github.com/uptrace/bun/driver/sqliteshim"
)

func newInMemoryCatalog(t *testing.T, props iceberg.Properties) (*Catalog, *sql.DB) {
	t.Helper()
	sqldb, err := sql.Open(sqliteshim.ShimName, ":memory:")
	require.NoError(t, err)
	sqldb.SetMaxOpenConns(1)

	return &Catalog{db: bun.NewDB(sqldb, getDialect(SQLite)), name: "test", props: props}, sqldb
}

func createLegacyV0Table(t *testing.T, sqldb *sql.DB) {
	t.Helper()
	_, err := sqldb.Exec(`CREATE TABLE iceberg_tables (
		catalog_name VARCHAR NOT NULL,
		table_namespace VARCHAR NOT NULL,
		table_name VARCHAR NOT NULL,
		metadata_location VARCHAR,
		previous_metadata_location VARCHAR,
		PRIMARY KEY (catalog_name, table_namespace, table_name))`)
	require.NoError(t, err)
}

func icebergTypeColumnPresent(t *testing.T, sqldb *sql.DB) bool {
	t.Helper()
	var present int
	err := sqldb.QueryRow(
		`SELECT 1 FROM pragma_table_info('iceberg_tables') WHERE name = 'iceberg_type'`,
	).Scan(&present)
	if errors.Is(err, sql.ErrNoRows) {
		return false
	}
	require.NoError(t, err)

	return present == 1
}

func TestDetectSchemaVersionV1WhenColumnPresent(t *testing.T) {
	c, sqldb := newInMemoryCatalog(t, nil)
	defer sqldb.Close()

	require.NoError(t, c.CreateSQLTables(context.Background()))
	require.NoError(t, c.detectSchemaVersion(context.Background()))

	assert.Equal(t, schemaV1, c.schemaVersion)
	assert.False(t, c.isV0(), "a schema with iceberg_type must be detected as V1")
}

func TestDetectSchemaVersionV0WhenColumnAbsent(t *testing.T) {
	c, sqldb := newInMemoryCatalog(t, nil)
	defer sqldb.Close()

	createLegacyV0Table(t, sqldb)
	require.NoError(t, c.detectSchemaVersion(context.Background()))

	assert.Equal(t, schemaV0, c.schemaVersion)
	assert.True(t, c.isV0(), "a schema without iceberg_type must be detected as V0")
}

func TestMigrateV0SchemaWithoutOptInStaysV0(t *testing.T) {
	c, sqldb := newInMemoryCatalog(t, nil)
	defer sqldb.Close()

	createLegacyV0Table(t, sqldb)
	require.NoError(t, c.migrateV0Schema(context.Background()))

	assert.True(t, c.isV0(), "no opt-in must leave the catalog on V0")
	assert.False(t, icebergTypeColumnPresent(t, sqldb),
		"no opt-in must not run the one-way ALTER")
}

func TestMigrateV0SchemaWithOptInUpgradesToV1(t *testing.T) {
	c, sqldb := newInMemoryCatalog(t, iceberg.Properties{SchemaVersionKey: SchemaVersionV1})
	defer sqldb.Close()

	createLegacyV0Table(t, sqldb)
	require.NoError(t, c.migrateV0Schema(context.Background()))

	assert.False(t, c.isV0(), "opt-in must upgrade the catalog to V1")
	assert.True(t, icebergTypeColumnPresent(t, sqldb),
		"opt-in must add the iceberg_type column")
}

func TestAddIcebergTypeColumnDDLPerDialect(t *testing.T) {
	cases := []struct {
		name dialect.Name
		want string
	}{
		{dialect.PG, `ALTER TABLE iceberg_tables ADD COLUMN iceberg_type VARCHAR`},
		{dialect.SQLite, `ALTER TABLE iceberg_tables ADD COLUMN iceberg_type VARCHAR`},
		{dialect.MySQL, `ALTER TABLE iceberg_tables ADD COLUMN iceberg_type VARCHAR(255)`},
		{dialect.MSSQL, `ALTER TABLE iceberg_tables ADD iceberg_type VARCHAR(255)`},
		{dialect.Oracle, `ALTER TABLE iceberg_tables ADD (iceberg_type VARCHAR2(255))`},
	}
	for _, tc := range cases {
		t.Run(tc.name.String(), func(t *testing.T) {
			got, err := addIcebergTypeColumnDDL(tc.name)
			require.NoError(t, err)
			assert.Equal(t, tc.want, got)

			switch tc.name {
			case dialect.MSSQL:
				assert.NotContains(t, got, "ADD COLUMN",
					"SQL Server rejects ALTER TABLE ... ADD COLUMN")
			case dialect.Oracle:
				assert.Contains(t, got, "VARCHAR2", "Oracle uses VARCHAR2, not VARCHAR")
				assert.Contains(t, got, "ADD (", "Oracle requires the parenthesised ADD form")
			}
		})
	}
}

func TestAddIcebergTypeColumnDDLUnsupported(t *testing.T) {
	_, err := addIcebergTypeColumnDDL(dialect.Invalid)
	assert.Error(t, err, "default case must surface as an error, not a silent no-op")
}

func TestIcebergTypeColumnExistsQueryPerDialect(t *testing.T) {
	cases := []struct {
		name        dialect.Name
		mustContain []string
	}{
		{dialect.PG, []string{"information_schema.columns", "current_schema()", "iceberg_type"}},
		{dialect.MySQL, []string{"information_schema.columns", "DATABASE()", "iceberg_type"}},
		{dialect.MSSQL, []string{"information_schema.columns", "SCHEMA_NAME()", "iceberg_type"}},
		{dialect.SQLite, []string{"pragma_table_info", "iceberg_tables", "iceberg_type"}},
		{dialect.Oracle, []string{"user_tab_columns", "ICEBERG_TABLES", "ICEBERG_TYPE"}},
	}
	for _, tc := range cases {
		t.Run(tc.name.String(), func(t *testing.T) {
			got, err := icebergTypeColumnExistsQuery(tc.name)
			require.NoError(t, err)
			for _, sub := range tc.mustContain {
				assert.True(t, strings.Contains(got, sub),
					"probe for %s missing %q: %s", tc.name, sub, got)
			}
		})
	}
}

func TestIcebergTypeColumnExistsQueryUnsupported(t *testing.T) {
	_, err := icebergTypeColumnExistsQuery(dialect.Invalid)
	assert.Error(t, err, "default case must surface as an error, not a silent no-op")
}

func TestMigrateV0SchemaProbeErrorFailsFast(t *testing.T) {
	sqldb, err := sql.Open(sqliteshim.ShimName, ":memory:")
	require.NoError(t, err)

	c := &Catalog{db: bun.NewDB(sqldb, getDialect(SQLite)), name: "test"}
	require.NoError(t, sqldb.Close())

	err = c.migrateV0Schema(context.Background())
	require.Error(t, err)
	assert.Contains(t, err.Error(), "column probe failed")
}
