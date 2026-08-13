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

// This file covers the structural namespace lifecycle: create, exists, list,
// and drop. Namespace *properties* conformance — CreateNamespace with non-nil
// properties, LoadNamespaceProperties, and UpdateNamespaceProperties — is
// deferred to a later slice of #1691, together with the Config capability flags
// (e.g. SupportsNamespaceProperties, SupportsNestedNamespaces) that gate tests
// for backends which do not support those features.

import (
	"context"
	"testing"

	"github.com/DataDog/iceberg-go/catalog"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// testCreateNamespace asserts that a created namespace is reported as existing.
// Listing behavior is covered by testListNamespaces.
func testCreateNamespace(t *testing.T, cfg Config) {
	ctx := context.Background()
	cat := cfg.NewCatalog(t)
	namespace, _ := newIdentifiers()

	exists, err := cat.CheckNamespaceExists(ctx, namespace)
	require.NoError(t, err)
	assert.False(t, exists, "namespace should not exist before create")

	require.NoError(t, cat.CreateNamespace(ctx, namespace, nil))
	t.Cleanup(func() { _ = cat.DropNamespace(ctx, namespace) })

	exists, err = cat.CheckNamespaceExists(ctx, namespace)
	require.NoError(t, err)
	assert.True(t, exists, "namespace should exist after create")
}

// testCreateNamespaceThatAlreadyExists asserts that creating a namespace that
// already exists is rejected and leaves the original namespace intact.
func testCreateNamespaceThatAlreadyExists(t *testing.T, cfg Config) {
	ctx := context.Background()
	cat := cfg.NewCatalog(t)
	namespace, _ := newIdentifiers()

	exists, err := cat.CheckNamespaceExists(ctx, namespace)
	require.NoError(t, err)
	assert.False(t, exists, "namespace should not exist before create")

	require.NoError(t, cat.CreateNamespace(ctx, namespace, nil))
	t.Cleanup(func() { _ = cat.DropNamespace(ctx, namespace) })

	err = cat.CreateNamespace(ctx, namespace, nil)
	assert.ErrorIs(t, err, catalog.ErrNamespaceAlreadyExists)

	exists, err = cat.CheckNamespaceExists(ctx, namespace)
	require.NoError(t, err)
	assert.True(t, exists, "original namespace should still exist after a rejected duplicate create")
}

// testDropNamespace asserts that a dropped namespace no longer exists.
func testDropNamespace(t *testing.T, cfg Config) {
	ctx := context.Background()
	cat := cfg.NewCatalog(t)
	namespace, _ := newIdentifiers()

	require.NoError(t, cat.CreateNamespace(ctx, namespace, nil))
	t.Cleanup(func() { _ = cat.DropNamespace(ctx, namespace) })

	exists, err := cat.CheckNamespaceExists(ctx, namespace)
	require.NoError(t, err)
	assert.True(t, exists, "namespace should exist after create")

	require.NoError(t, cat.DropNamespace(ctx, namespace))

	exists, err = cat.CheckNamespaceExists(ctx, namespace)
	require.NoError(t, err)
	assert.False(t, exists, "namespace should not exist after drop")
}

// testDropMissingNamespace asserts that dropping a namespace that was never
// created reports that the namespace does not exist. This is a deliberate
// Go-idiom divergence from Java's SupportsNamespaces.dropNamespace, which
// returns false (and no error) for a missing namespace; the Go catalog
// interface returns catalog.ErrNoSuchNamespace instead.
func testDropMissingNamespace(t *testing.T, cfg Config) {
	ctx := context.Background()
	cat := cfg.NewCatalog(t)
	namespace, _ := newIdentifiers()

	err := cat.DropNamespace(ctx, namespace)
	assert.ErrorIs(t, err, catalog.ErrNoSuchNamespace)
}

// testDropNamespaceNotEmpty asserts that a namespace containing a table cannot
// be dropped.
func testDropNamespaceNotEmpty(t *testing.T, cfg Config) {
	ctx := context.Background()
	cat := cfg.NewCatalog(t)
	namespace, ident := newIdentifiers()

	require.NoError(t, cat.CreateNamespace(ctx, namespace, nil))
	t.Cleanup(func() {
		_ = cat.DropTable(ctx, ident)
		_ = cat.DropNamespace(ctx, namespace)
	})

	_, err := cat.CreateTable(ctx, ident, Schema)
	require.NoError(t, err)

	err = cat.DropNamespace(ctx, namespace)
	assert.ErrorIs(t, err, catalog.ErrNamespaceNotEmpty)

	// A rejected drop must leave the namespace and its table intact.
	nsExists, err := cat.CheckNamespaceExists(ctx, namespace)
	require.NoError(t, err)
	assert.True(t, nsExists, "namespace should still exist after a rejected non-empty drop")

	tableExists, err := cat.CheckTableExists(ctx, ident)
	require.NoError(t, err)
	assert.True(t, tableExists, "table should still exist after a rejected namespace drop")
}

// testListNamespaces asserts that created namespaces appear in the top-level
// listing and that a dropped namespace disappears from it while the others
// remain.
func testListNamespaces(t *testing.T, cfg Config) {
	ctx := context.Background()
	cat := cfg.NewCatalog(t)
	first, _ := newIdentifiers()
	second, _ := newIdentifiers()

	require.NoError(t, cat.CreateNamespace(ctx, first, nil))
	t.Cleanup(func() { _ = cat.DropNamespace(ctx, first) })
	require.NoError(t, cat.CreateNamespace(ctx, second, nil))
	t.Cleanup(func() { _ = cat.DropNamespace(ctx, second) })

	namespaces, err := cat.ListNamespaces(ctx, nil)
	require.NoError(t, err)
	assert.Contains(t, namespaces, first, "first created namespace should be listed")
	assert.Contains(t, namespaces, second, "second created namespace should be listed")

	require.NoError(t, cat.DropNamespace(ctx, first))

	namespaces, err = cat.ListNamespaces(ctx, nil)
	require.NoError(t, err)
	assert.NotContains(t, namespaces, first, "dropped namespace should no longer be listed")
	assert.Contains(t, namespaces, second, "remaining namespace should still be listed")
}
