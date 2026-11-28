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
	"errors"
	"testing"

	"github.com/apache/iceberg-go"
	"github.com/apache/iceberg-go/catalog"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// newRegisterableCatalog returns a fresh catalog from cfg and its register
// capability. It skips the test when the catalog cannot register tables, as
// the SQL and Hadoop catalogs cannot, or when it reports register as not
// implemented, as a REST server that does not advertise the endpoint does.
func newRegisterableCatalog(t *testing.T, cfg Config) (catalog.Catalog, catalog.RegisterableCatalog) {
	t.Helper()

	cat := cfg.NewCatalog(t)
	registerable, ok := catalog.As[catalog.RegisterableCatalog](cat)
	if !ok {
		t.Skip("catalog does not implement catalog.RegisterableCatalog")
	}

	// The namespace does not exist, so this registers nothing.
	_, ident := newIdentifiers()
	if _, err := registerable.RegisterTable(context.Background(), ident, "missing.metadata.json"); errors.Is(err, iceberg.ErrNotImplemented) {
		t.Skipf("catalog does not support register: %v", err)
	}

	return cat, registerable
}

// testRegisterTable asserts that a table dropped without purge can be
// registered again from its metadata file and loads as the same table.
func testRegisterTable(t *testing.T, cfg Config) {
	ctx := context.Background()
	cat, registerable := newRegisterableCatalog(t, cfg)
	namespace, ident := newIdentifiers()

	require.NoError(t, cat.CreateNamespace(ctx, namespace, nil))
	t.Cleanup(func() { _ = cat.DropNamespace(ctx, namespace) })

	created, err := cat.CreateTable(ctx, ident, Schema)
	require.NoError(t, err)
	metadataLocation := created.MetadataLocation()
	originalUUID := created.Metadata().TableUUID()

	require.NoError(t, cat.DropTable(ctx, ident))

	_, err = registerable.RegisterTable(ctx, ident, metadataLocation)
	require.NoError(t, err)
	t.Cleanup(func() { _ = cat.DropTable(ctx, ident) })

	exists, err := cat.CheckTableExists(ctx, ident)
	require.NoError(t, err)
	assert.True(t, exists, "table should exist after register")

	tbl, err := cat.LoadTable(ctx, ident)
	require.NoError(t, err)
	assert.Equal(t, ident, tbl.Identifier(), "registered table should report the identifier it was registered under")
	assert.Equal(t, metadataLocation, tbl.MetadataLocation(), "registered table should point at the registered metadata file")
	assert.Equal(t, TableSchema.AsStruct(), tbl.Schema().AsStruct(), "registered table should keep its schema")
	assert.Equal(t, originalUUID, tbl.Metadata().TableUUID(), "registered table should keep its identity")
}

// testRegisterExistingTable asserts that registering over an existing table is
// rejected.
func testRegisterExistingTable(t *testing.T, cfg Config) {
	ctx := context.Background()
	cat, registerable := newRegisterableCatalog(t, cfg)
	namespace, ident := newIdentifiers()

	require.NoError(t, cat.CreateNamespace(ctx, namespace, nil))
	t.Cleanup(func() { _ = cat.DropNamespace(ctx, namespace) })

	created, err := cat.CreateTable(ctx, ident, Schema)
	require.NoError(t, err)
	t.Cleanup(func() { _ = cat.DropTable(ctx, ident) })

	_, err = registerable.RegisterTable(ctx, ident, created.MetadataLocation())
	assert.ErrorIs(t, err, catalog.ErrTableAlreadyExists)
}

// testRegisterTableInMissingNamespace asserts that registering into a
// namespace that does not exist reports the missing namespace.
func testRegisterTableInMissingNamespace(t *testing.T, cfg Config) {
	ctx := context.Background()
	cat, registerable := newRegisterableCatalog(t, cfg)
	namespace, ident := newIdentifiers()
	_, missing := newIdentifiers()

	require.NoError(t, cat.CreateNamespace(ctx, namespace, nil))
	t.Cleanup(func() { _ = cat.DropNamespace(ctx, namespace) })

	created, err := cat.CreateTable(ctx, ident, Schema)
	require.NoError(t, err)
	t.Cleanup(func() { _ = cat.DropTable(ctx, ident) })

	_, err = registerable.RegisterTable(ctx, missing, created.MetadataLocation())
	assert.ErrorIs(t, err, catalog.ErrNoSuchNamespace)
}

// testPurgeTable asserts that a purged table is no longer visible. Whether its
// files are removed is not checked, since the contract makes that best-effort.
func testPurgeTable(t *testing.T, cfg Config) {
	ctx := context.Background()
	cat := cfg.NewCatalog(t)
	namespace, ident := newIdentifiers()

	require.NoError(t, cat.CreateNamespace(ctx, namespace, nil))
	t.Cleanup(func() { _ = cat.DropNamespace(ctx, namespace) })

	_, err := cat.CreateTable(ctx, ident, Schema)
	require.NoError(t, err)

	require.NoError(t, cat.PurgeTable(ctx, ident))

	exists, err := cat.CheckTableExists(ctx, ident)
	require.NoError(t, err)
	assert.False(t, exists, "table should not exist after purge")

	_, err = cat.LoadTable(ctx, ident)
	assert.ErrorIs(t, err, catalog.ErrNoSuchTable)
}

// testPurgeMissingTable asserts that purging a table that was never created
// reports that the table does not exist.
func testPurgeMissingTable(t *testing.T, cfg Config) {
	ctx := context.Background()
	cat := cfg.NewCatalog(t)
	namespace, ident := newIdentifiers()

	require.NoError(t, cat.CreateNamespace(ctx, namespace, nil))
	t.Cleanup(func() { _ = cat.DropNamespace(ctx, namespace) })

	assert.ErrorIs(t, cat.PurgeTable(ctx, ident), catalog.ErrNoSuchTable)
}
