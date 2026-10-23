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

package catalogtest

import (
	"context"
	"testing"

	"github.com/apache/iceberg-go"
	"github.com/apache/iceberg-go/catalog"
	"github.com/apache/iceberg-go/table"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// createTable creates a namespace and a table in it, registering cleanup for
// both, and returns the table's identifier and the created table.
func createTable(t *testing.T, cat catalog.Catalog, opts ...catalog.CreateTableOpt) (table.Identifier, *table.Table) {
	t.Helper()

	ctx := context.Background()
	namespace, ident := newIdentifiers()

	require.NoError(t, cat.CreateNamespace(ctx, namespace, nil))
	t.Cleanup(func() { _ = cat.DropNamespace(ctx, namespace) })

	tbl, err := cat.CreateTable(ctx, ident, Schema, opts...)
	require.NoError(t, err)
	t.Cleanup(func() { _ = cat.DropTable(ctx, ident) })

	return ident, tbl
}

// testCreateTableWithProperties asserts that properties passed at create time
// are visible on a fresh load.
func testCreateTableWithProperties(t *testing.T, cfg Config) {
	ctx := context.Background()
	cat := cfg.NewCatalog(t)

	ident, _ := createTable(t, cat, catalog.WithProperties(iceberg.Properties{"owner": "conformance"}))

	tbl, err := cat.LoadTable(ctx, ident)
	require.NoError(t, err)
	assert.Equal(t, "conformance", tbl.Properties()["owner"])
}

// testUpdateTableProperties asserts that properties set and removed through a
// transaction are visible on a fresh load.
func testUpdateTableProperties(t *testing.T, cfg Config) {
	ctx := context.Background()
	cat := cfg.NewCatalog(t)

	ident, tbl := createTable(t, cat, catalog.WithProperties(iceberg.Properties{"keep": "1", "drop": "1"}))

	txn := tbl.NewTransaction()
	require.NoError(t, txn.SetProperties(iceberg.Properties{"added": "1"}))
	require.NoError(t, txn.RemoveProperties([]string{"drop"}))
	_, err := txn.Commit(ctx)
	require.NoError(t, err)

	loaded, err := cat.LoadTable(ctx, ident)
	require.NoError(t, err)
	props := loaded.Properties()
	assert.Equal(t, "1", props["keep"])
	assert.Equal(t, "1", props["added"])
	assert.NotContains(t, props, "drop")
}

// testUpdateTableSchema asserts that a schema change committed through a
// transaction becomes the current schema on a fresh load, with the next field
// ID assigned to the new column.
func testUpdateTableSchema(t *testing.T, cfg Config) {
	ctx := context.Background()
	cat := cfg.NewCatalog(t)

	ident, tbl := createTable(t, cat)
	originalSchemaID := tbl.Schema().ID

	txn := tbl.NewTransaction()
	require.NoError(t, txn.UpdateSchema(true, false).
		AddColumn([]string{"extra"}, iceberg.PrimitiveTypes.Int64, "", false, nil).
		Commit())
	_, err := txn.Commit(ctx)
	require.NoError(t, err)

	loaded, err := cat.LoadTable(ctx, ident)
	require.NoError(t, err)
	assert.NotEqual(t, originalSchemaID, loaded.Schema().ID, "schema update should produce a new schema ID")

	field, ok := loaded.Schema().FindFieldByName("extra")
	require.True(t, ok, "new column should be in the current schema")
	assert.Equal(t, 3, field.ID, "new column should get the next field ID")
	assert.Equal(t, iceberg.PrimitiveTypes.Int64, field.Type)
}

// testCommitWithStaleRequirement asserts that a commit whose requirement no
// longer holds, because another writer changed the table first, fails with
// table.ErrCommitFailed and leaves the table as the other writer left it.
//
// ErrCommitFailed is what the retry loop in table.Transaction keys on, so a
// catalog that reports a stale requirement any other way never retries.
func testCommitWithStaleRequirement(t *testing.T, cfg Config) {
	ctx := context.Background()
	cat := cfg.NewCatalog(t)

	ident, stale := createTable(t, cat)

	// Another writer evolves the schema, moving the current schema ID.
	txn := stale.NewTransaction()
	require.NoError(t, txn.UpdateSchema(true, false).
		AddColumn([]string{"extra"}, iceberg.PrimitiveTypes.Int64, "", false, nil).
		Commit())
	_, err := txn.Commit(ctx)
	require.NoError(t, err)

	// The stale writer still believes the original schema is current.
	_, _, err = cat.CommitTable(ctx, ident,
		[]table.Requirement{table.AssertCurrentSchemaID(stale.Schema().ID)},
		[]table.Update{table.NewSetPropertiesUpdate(iceberg.Properties{"stale": "1"})})
	assert.ErrorIs(t, err, table.ErrCommitFailed)

	loaded, err := cat.LoadTable(ctx, ident)
	require.NoError(t, err)
	assert.NotContains(t, loaded.Properties(), "stale", "a rejected commit must not apply its updates")
	_, ok := loaded.Schema().FindFieldByName("extra")
	assert.True(t, ok, "the other writer's commit should be intact")
}

// testCommitToDroppedTable asserts that committing through a handle to a table
// that has since been dropped reports that the table does not exist, rather
// than recreating it.
func testCommitToDroppedTable(t *testing.T, cfg Config) {
	ctx := context.Background()
	cat := cfg.NewCatalog(t)

	ident, tbl := createTable(t, cat)
	require.NoError(t, cat.DropTable(ctx, ident))

	txn := tbl.NewTransaction()
	require.NoError(t, txn.SetProperties(iceberg.Properties{"after-drop": "1"}))
	_, err := txn.Commit(ctx)
	assert.ErrorIs(t, err, catalog.ErrNoSuchTable)

	exists, err := cat.CheckTableExists(ctx, ident)
	require.NoError(t, err)
	assert.False(t, exists, "a commit to a dropped table must not recreate it")
}
