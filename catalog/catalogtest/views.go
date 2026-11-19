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

	"github.com/apache/iceberg-go/catalog"
	"github.com/apache/iceberg-go/table"
	"github.com/apache/iceberg-go/view"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// viewSQL is the SQL the conformance tests create views with.
const viewSQL = "SELECT id, data FROM tbl"

// viewDialect is the dialect of viewSQL.
const viewDialect = "spark"

// newViewVersion returns the version the conformance tests create a view in
// namespace with.
func newViewVersion(t *testing.T, namespace table.Identifier) *view.Version {
	t.Helper()

	version, err := view.NewVersionFromSQL(1, 0,
		view.Representation{SQL: viewSQL, Dialect: viewDialect}, namespace)
	require.NoError(t, err)

	return version
}

// createView creates the conformance view at ident and drops it on cleanup.
// It uses [TableSchema] rather than [Schema] so that catalogs that assign
// fresh field IDs on create and catalogs that keep the given IDs agree.
func createView(t *testing.T, views catalog.ViewCatalog, ident table.Identifier) *view.View {
	t.Helper()
	ctx := context.Background()

	v, err := views.CreateView(ctx, ident, newViewVersion(t, ident[:len(ident)-1]), TableSchema)
	require.NoError(t, err)
	t.Cleanup(func() { _ = views.DropView(ctx, ident) })

	return v
}

// listViews collects every view identifier the catalog reports for namespace.
func listViews(t *testing.T, views catalog.ViewCatalog, namespace table.Identifier) []table.Identifier {
	t.Helper()

	var out []table.Identifier
	for ident, err := range views.ListViews(context.Background(), namespace) {
		require.NoError(t, err)
		out = append(out, ident)
	}

	return out
}

// newViewCatalog returns a fresh catalog from cfg and its view capability.
func newViewCatalog(t *testing.T, cfg Config) (catalog.Catalog, catalog.ViewCatalog) {
	t.Helper()

	cat := cfg.NewCatalog(t)
	views, ok := catalog.As[catalog.ViewCatalog](cat)
	require.True(t, ok, "catalog should implement catalog.ViewCatalog")

	return cat, views
}

// assertConformanceView asserts that v is the conformance view at ident.
func assertConformanceView(t *testing.T, ident table.Identifier, v *view.View) {
	t.Helper()

	assert.Equal(t, ident, v.Identifier(), "view should report the identifier it was created with")
	assert.Equal(t, TableSchema.AsStruct(), v.CurrentSchema().AsStruct(), "view should keep its schema")

	version := v.CurrentVersion()
	require.NotNil(t, version, "view should have a current version")
	require.Len(t, version.Representations, 1)
	assert.Equal(t, viewSQL, version.Representations[0].SQL, "view should keep its SQL")
	assert.Equal(t, viewDialect, version.Representations[0].Dialect, "view should keep its dialect")
}

// testBasicCreateView asserts that a newly created view is visible to the
// catalog and reports the schema and SQL it was created with.
func testBasicCreateView(t *testing.T, cfg Config) {
	ctx := context.Background()
	cat, views := newViewCatalog(t, cfg)
	namespace, ident := newIdentifiers()

	require.NoError(t, cat.CreateNamespace(ctx, namespace, nil))
	t.Cleanup(func() { _ = cat.DropNamespace(ctx, namespace) })

	exists, err := views.CheckViewExists(ctx, ident)
	require.NoError(t, err)
	assert.False(t, exists, "view should not exist before create")

	createView(t, views, ident)

	exists, err = views.CheckViewExists(ctx, ident)
	require.NoError(t, err)
	assert.True(t, exists, "view should exist after create")

	v, err := views.LoadView(ctx, ident)
	require.NoError(t, err)
	assertConformanceView(t, ident, v)
}

// testCreateViewThatAlreadyExists asserts that creating a view over an
// existing one is rejected and leaves the original view intact.
func testCreateViewThatAlreadyExists(t *testing.T, cfg Config) {
	ctx := context.Background()
	cat, views := newViewCatalog(t, cfg)
	namespace, ident := newIdentifiers()

	require.NoError(t, cat.CreateNamespace(ctx, namespace, nil))
	t.Cleanup(func() { _ = cat.DropNamespace(ctx, namespace) })

	createView(t, views, ident)

	other, err := view.NewVersionFromSQL(1, 0,
		view.Representation{SQL: "SELECT some_id FROM other", Dialect: viewDialect}, namespace)
	require.NoError(t, err)

	_, err = views.CreateView(ctx, ident, other, OtherSchema)
	assert.ErrorIs(t, err, catalog.ErrViewAlreadyExists)

	v, err := views.LoadView(ctx, ident)
	require.NoError(t, err)
	assertConformanceView(t, ident, v)
}

// testLoadMissingView asserts that loading a view that was never created
// reports that the view does not exist.
func testLoadMissingView(t *testing.T, cfg Config) {
	ctx := context.Background()
	cat, views := newViewCatalog(t, cfg)
	namespace, ident := newIdentifiers()

	require.NoError(t, cat.CreateNamespace(ctx, namespace, nil))
	t.Cleanup(func() { _ = cat.DropNamespace(ctx, namespace) })

	_, err := views.LoadView(ctx, ident)
	assert.ErrorIs(t, err, catalog.ErrNoSuchView)
}

// testListViews asserts that ListViews reflects creates and drops.
func testListViews(t *testing.T, cfg Config) {
	ctx := context.Background()
	cat, views := newViewCatalog(t, cfg)
	namespace, ident := newIdentifiers()

	require.NoError(t, cat.CreateNamespace(ctx, namespace, nil))
	t.Cleanup(func() { _ = cat.DropNamespace(ctx, namespace) })

	assert.Empty(t, listViews(t, views, namespace), "new namespace should have no views")

	createView(t, views, ident)
	assert.Equal(t, []table.Identifier{ident}, listViews(t, views, namespace))

	require.NoError(t, views.DropView(ctx, ident))
	assert.Empty(t, listViews(t, views, namespace), "dropped view should not be listed")
}

// testDropView asserts that a dropped view is no longer visible.
func testDropView(t *testing.T, cfg Config) {
	ctx := context.Background()
	cat, views := newViewCatalog(t, cfg)
	namespace, ident := newIdentifiers()

	require.NoError(t, cat.CreateNamespace(ctx, namespace, nil))
	t.Cleanup(func() { _ = cat.DropNamespace(ctx, namespace) })

	createView(t, views, ident)
	require.NoError(t, views.DropView(ctx, ident))

	exists, err := views.CheckViewExists(ctx, ident)
	require.NoError(t, err)
	assert.False(t, exists, "view should not exist after drop")

	_, err = views.LoadView(ctx, ident)
	assert.ErrorIs(t, err, catalog.ErrNoSuchView)
}

// testDropMissingView asserts that dropping a view that was never created
// reports that the view does not exist.
func testDropMissingView(t *testing.T, cfg Config) {
	ctx := context.Background()
	cat, views := newViewCatalog(t, cfg)
	namespace, ident := newIdentifiers()

	require.NoError(t, cat.CreateNamespace(ctx, namespace, nil))
	t.Cleanup(func() { _ = cat.DropNamespace(ctx, namespace) })

	assert.ErrorIs(t, views.DropView(ctx, ident), catalog.ErrNoSuchView)
}

// testDropNamespaceWithView asserts that a namespace holding a view cannot be
// dropped and that the view survives the attempt.
func testDropNamespaceWithView(t *testing.T, cfg Config) {
	ctx := context.Background()
	cat, views := newViewCatalog(t, cfg)
	namespace, ident := newIdentifiers()

	require.NoError(t, cat.CreateNamespace(ctx, namespace, nil))
	t.Cleanup(func() { _ = cat.DropNamespace(ctx, namespace) })

	createView(t, views, ident)

	assert.ErrorIs(t, cat.DropNamespace(ctx, namespace), catalog.ErrNamespaceNotEmpty)

	exists, err := views.CheckViewExists(ctx, ident)
	require.NoError(t, err)
	assert.True(t, exists, "view should still exist after a rejected namespace drop")
}
