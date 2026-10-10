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

// listTables collects every table identifier the catalog reports for namespace.
func listTables(t *testing.T, cat catalog.Catalog, namespace table.Identifier) []table.Identifier {
	t.Helper()

	var out []table.Identifier
	for ident, err := range cat.ListTables(context.Background(), namespace) {
		require.NoError(t, err)
		out = append(out, ident)
	}

	return out
}

// testListTables asserts that ListTables reflects creates and drops.
func testListTables(t *testing.T, cfg Config) {
	ctx := context.Background()
	cat := cfg.NewCatalog(t)
	namespace, ident := newIdentifiers()

	require.NoError(t, cat.CreateNamespace(ctx, namespace, nil))
	t.Cleanup(func() { _ = cat.DropNamespace(ctx, namespace) })

	assert.Empty(t, listTables(t, cat, namespace), "new namespace should have no tables")

	_, err := cat.CreateTable(ctx, ident, Schema)
	require.NoError(t, err)
	t.Cleanup(func() { _ = cat.DropTable(ctx, ident) })

	assert.Equal(t, []table.Identifier{ident}, listTables(t, cat, namespace))

	require.NoError(t, cat.DropTable(ctx, ident))
	assert.Empty(t, listTables(t, cat, namespace), "dropped table should not be listed")
}

// testDropTable asserts that a dropped table is no longer visible.
func testDropTable(t *testing.T, cfg Config) {
	ctx := context.Background()
	cat := cfg.NewCatalog(t)
	namespace, ident := newIdentifiers()

	require.NoError(t, cat.CreateNamespace(ctx, namespace, nil))
	t.Cleanup(func() { _ = cat.DropNamespace(ctx, namespace) })

	_, err := cat.CreateTable(ctx, ident, Schema)
	require.NoError(t, err)

	require.NoError(t, cat.DropTable(ctx, ident))

	exists, err := cat.CheckTableExists(ctx, ident)
	require.NoError(t, err)
	assert.False(t, exists, "table should not exist after drop")

	_, err = cat.LoadTable(ctx, ident)
	assert.ErrorIs(t, err, catalog.ErrNoSuchTable)
}

// testDropMissingTable asserts that dropping a table that was never created
// reports that the table does not exist.
func testDropMissingTable(t *testing.T, cfg Config) {
	ctx := context.Background()
	cat := cfg.NewCatalog(t)
	namespace, ident := newIdentifiers()

	require.NoError(t, cat.CreateNamespace(ctx, namespace, nil))
	t.Cleanup(func() { _ = cat.DropNamespace(ctx, namespace) })

	assert.ErrorIs(t, cat.DropTable(ctx, ident), catalog.ErrNoSuchTable)
}

// testRenameTableNotSupported asserts that a catalog without rename support
// reports it with iceberg.ErrNotImplemented and leaves the source table in
// place, so an unsupported rename is a verified behavior rather than a skip.
func testRenameTableNotSupported(t *testing.T, cfg Config) {
	ctx := context.Background()
	cat := cfg.NewCatalog(t)
	namespace, from := newIdentifiers()
	to := table.Identifier{namespace[0], "renamed"}

	require.NoError(t, cat.CreateNamespace(ctx, namespace, nil))
	t.Cleanup(func() { _ = cat.DropNamespace(ctx, namespace) })

	_, err := cat.CreateTable(ctx, from, Schema)
	require.NoError(t, err)
	t.Cleanup(func() { _ = cat.DropTable(ctx, from) })

	_, err = cat.RenameTable(ctx, from, to)
	assert.ErrorIs(t, err, iceberg.ErrNotImplemented)

	exists, err := cat.CheckTableExists(ctx, from)
	require.NoError(t, err)
	assert.True(t, exists, "source table should still exist after an unsupported rename")
}

// testRenameTable asserts that a renamed table is reachable only under its new
// identifier and keeps its identity, metadata and schema.
func testRenameTable(t *testing.T, cfg Config) {
	ctx := context.Background()
	cat := cfg.NewCatalog(t)
	namespace, from := newIdentifiers()
	to := table.Identifier{namespace[0], "renamed"}

	require.NoError(t, cat.CreateNamespace(ctx, namespace, nil))
	t.Cleanup(func() { _ = cat.DropNamespace(ctx, namespace) })

	created, err := cat.CreateTable(ctx, from, Schema)
	require.NoError(t, err)
	t.Cleanup(func() { _ = cat.DropTable(ctx, from) })

	originalUUID := created.Metadata().TableUUID()
	originalMetadataLocation := created.MetadataLocation()

	_, err = cat.RenameTable(ctx, from, to)
	require.NoError(t, err)
	t.Cleanup(func() { _ = cat.DropTable(ctx, to) })

	exists, err := cat.CheckTableExists(ctx, from)
	require.NoError(t, err)
	assert.False(t, exists, "source table should not exist after rename")

	tbl, err := cat.LoadTable(ctx, to)
	require.NoError(t, err)
	assert.Equal(t, to, tbl.Identifier())
	assert.Equal(t, originalUUID, tbl.Metadata().TableUUID(), "rename must preserve table identity")
	assert.Equal(t, originalMetadataLocation, tbl.MetadataLocation(), "rename should not rewrite table metadata")
	assert.Equal(t, TableSchema.AsStruct(), tbl.Schema().AsStruct(), "rename should keep the schema")
}

// testRenameTableToExisting asserts that renaming onto an existing table is
// rejected and leaves both tables in place.
func testRenameTableToExisting(t *testing.T, cfg Config) {
	ctx := context.Background()
	cat := cfg.NewCatalog(t)
	namespace, from := newIdentifiers()
	to := table.Identifier{namespace[0], "other"}

	require.NoError(t, cat.CreateNamespace(ctx, namespace, nil))
	t.Cleanup(func() { _ = cat.DropNamespace(ctx, namespace) })

	for _, ident := range []table.Identifier{from, to} {
		_, err := cat.CreateTable(ctx, ident, Schema)
		require.NoError(t, err)
		t.Cleanup(func() { _ = cat.DropTable(ctx, ident) })
	}

	_, err := cat.RenameTable(ctx, from, to)
	assert.ErrorIs(t, err, catalog.ErrTableAlreadyExists)

	for _, ident := range []table.Identifier{from, to} {
		exists, err := cat.CheckTableExists(ctx, ident)
		require.NoError(t, err)
		assert.True(t, exists, "%v should still exist after a rejected rename", ident)
	}
}

// testRenameMissingTable asserts that renaming a table that was never created
// reports that the table does not exist.
func testRenameMissingTable(t *testing.T, cfg Config) {
	ctx := context.Background()
	cat := cfg.NewCatalog(t)
	namespace, from := newIdentifiers()
	to := table.Identifier{namespace[0], "renamed"}

	require.NoError(t, cat.CreateNamespace(ctx, namespace, nil))
	t.Cleanup(func() { _ = cat.DropNamespace(ctx, namespace) })

	_, err := cat.RenameTable(ctx, from, to)
	assert.ErrorIs(t, err, catalog.ErrNoSuchTable)
}
