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

package view

import (
	"context"
	"net/url"
	"testing"

	"github.com/apache/iceberg-go"
	iceio "github.com/apache/iceberg-go/io"
	"github.com/stretchr/testify/require"
)

func TestCreateViewWithIsolatedIORegistry(t *testing.T) {
	const scheme = "isolated-view-registry"
	filesystems := []*iceio.MemFS{iceio.NewMemFS(), iceio.NewMemFS()}
	registries := []*iceio.Registry{{}, {}}

	for i, registry := range registries {
		fs := filesystems[i]
		require.NoError(t, registry.Register(scheme, func(context.Context, *url.URL, map[string]string) (iceio.IO, error) {
			return fs, nil
		}))
	}

	for i, registry := range registries {
		created, err := CreateViewWithOptions(
			t.Context(), "test", []string{"db", "orders"},
			newTestSchema(0), Representation{SQL: "SELECT 1", Dialect: "spark"},
			[]string{"db"}, scheme+"://bucket/view", nil, WithIORegistry(registry),
		)
		require.NoError(t, err)
		require.NotNil(t, created)

		file, err := filesystems[i].Open(t.Context(), created.MetadataLocation())
		require.NoError(t, err)
		require.NoError(t, file.Close())

		_, err = filesystems[1-i].Open(t.Context(), created.MetadataLocation())
		require.Error(t, err, "the other catalog's filesystem must not see this view")

		_, err = iceio.LoadFS(t.Context(), nil, created.MetadataLocation())
		require.ErrorIs(t, err, iceio.ErrIOSchemeNotFound)
	}
}

func TestCreateViewWithSeparateIOPropertiesAndRegistry(t *testing.T) {
	registry := &iceio.Registry{}
	fs := iceio.NewMemFS()
	var seen map[string]string
	require.NoError(t, registry.Register("isolated-view-properties", func(_ context.Context, _ *url.URL, props map[string]string) (iceio.IO, error) {
		seen = props

		return fs, nil
	}))

	created, err := CreateViewWithIOPropertiesAndOptions(
		t.Context(), "test", []string{"db", "orders"},
		newTestSchema(0), Representation{SQL: "SELECT 1", Dialect: "spark"},
		[]string{"db"}, "isolated-view-properties://bucket/view",
		iceberg.Properties{"storage-only": "token"},
		iceberg.Properties{"persisted": "value"}, WithIORegistry(registry),
	)
	require.NoError(t, err)
	require.Equal(t, "token", seen["storage-only"])
	require.Equal(t, "value", created.Properties()["persisted"])
	require.NotContains(t, created.Properties(), "storage-only")
}

func TestCreateViewWithEmptyRegistryDoesNotFallBackToGlobal(t *testing.T) {
	_, err := CreateViewWithOptions(
		t.Context(), "test", []string{"db", "orders"},
		newTestSchema(0), Representation{SQL: "SELECT 1", Dialect: "spark"},
		[]string{"db"}, "file:///tmp/view-registry-no-fallback", nil, WithIORegistry(&iceio.Registry{}),
	)
	require.ErrorIs(t, err, iceio.ErrIOSchemeNotFound)
}

// TestWriteMetadataUsesIsolatedRegistry directly exercises metadata writes,
// not just the view-creation wrapper. Storage-only settings must reach FileIO
// without becoming persisted view properties.
func TestWriteMetadataUsesIsolatedRegistry(t *testing.T) {
	const scheme = "isolated-view-metadata-write"
	ctx := t.Context()
	registry := &iceio.Registry{}
	fs := iceio.NewMemFS()
	var storageProps map[string]string
	require.NoError(t, registry.Register(scheme, func(_ context.Context, _ *url.URL, props map[string]string) (iceio.IO, error) {
		storageProps = props

		return fs, nil
	}))
	created, err := CreateViewWithOptions(
		ctx, "test", []string{"db", "view"}, newTestSchema(0),
		Representation{SQL: "SELECT 1", Dialect: "spark"},
		[]string{"db"}, scheme+"://bucket/view", iceberg.Properties{"persisted": "yes"},
		WithIORegistry(registry),
	)
	require.NoError(t, err)

	metadataLocation, writeFS, err := WriteMetadataWithOptions(ctx, created.Metadata(),
		iceberg.Properties{"storage-token": "private"}, WithIORegistry(registry))
	require.NoError(t, err)
	require.NotEqual(t, created.MetadataLocation(), metadataLocation)
	require.Same(t, fs, writeFS)
	require.Equal(t, "private", storageProps["storage-token"])
	require.Equal(t, "yes", created.Properties()["persisted"])
	require.NotContains(t, created.Properties(), "storage-token")
	file, err := fs.Open(ctx, metadataLocation)
	require.NoError(t, err)
	require.NoError(t, file.Close())
	_, err = iceio.LoadFS(ctx, nil, metadataLocation)
	require.ErrorIs(t, err, iceio.ErrIOSchemeNotFound)
}

// TestViewMetadataIORegistryIsLive guards the supplied registry pointer
// against being copied or silently replaced by the global registry.
func TestViewMetadataIORegistryIsLive(t *testing.T) {
	const scheme = "isolated-view-registry-live"
	ctx := t.Context()
	registry := &iceio.Registry{}
	ident := []string{"db", "view"}
	representation := Representation{SQL: "SELECT 1", Dialect: "spark"}
	location := scheme + "://bucket/view"

	_, err := CreateViewWithOptions(ctx, "test", ident, newTestSchema(0),
		representation, []string{"db"}, location, nil, WithIORegistry(registry))
	require.ErrorIs(t, err, iceio.ErrIOSchemeNotFound)

	fs := iceio.NewMemFS()
	require.NoError(t, registry.Register(scheme, func(context.Context, *url.URL, map[string]string) (iceio.IO, error) {
		return fs, nil
	}))
	created, err := CreateViewWithOptions(ctx, "test", ident, newTestSchema(0),
		representation, []string{"db"}, location, nil, WithIORegistry(registry))
	require.NoError(t, err)
	file, err := fs.Open(ctx, created.MetadataLocation())
	require.NoError(t, err)
	require.NoError(t, file.Close())

	registry.Unregister(scheme)
	_, _, err = WriteMetadataWithOptions(ctx, created.Metadata(), nil, WithIORegistry(registry))
	require.ErrorIs(t, err, iceio.ErrIOSchemeNotFound,
		"unregistering the private scheme must not fall back to the global registry")
	file, err = fs.Open(ctx, created.MetadataLocation())
	require.NoError(t, err, "failed new metadata writes must not remove an existing view")
	require.NoError(t, file.Close())
}

func TestCreateViewFromVersionPreservesMetadataWithIsolatedRegistry(t *testing.T) {
	const scheme = "isolated-view-version"
	registry := &iceio.Registry{}
	fs := iceio.NewMemFS()
	require.NoError(t, registry.Register(scheme, func(context.Context, *url.URL, map[string]string) (iceio.IO, error) {
		return fs, nil
	}))
	version, err := NewVersion(7, 3, []Representation{
		NewRepresentation(Representation{SQL: "SELECT 1", Dialect: "trino"}),
		NewRepresentation(Representation{SQL: "SELECT 1", Dialect: "spark"}),
	}, []string{"original_ns"},
		WithDefaultViewCatalog("prod"),
		WithVersionSummary(VersionSummary{"engine-version": "467"}),
		WithTimestampMS(1234),
	)
	require.NoError(t, err)
	created, err := CreateViewFromVersionWithOptions(
		t.Context(), []string{"db", "v"}, newTestSchema(0), version,
		scheme+"://bucket/view", iceberg.Properties{"storage-only": "secret"},
		iceberg.Properties{"view-property": "stored"}, WithIORegistry(registry),
	)
	require.NoError(t, err)
	got := created.CurrentVersion()
	require.Equal(t, version.Representations, got.Representations)
	require.Equal(t, "prod", got.DefaultCatalog)
	require.Equal(t, version.DefaultNamespace, got.DefaultNamespace)
	require.Equal(t, int64(1234), got.TimestampMS)
	require.Equal(t, "467", got.Summary["engine-version"])
	require.Equal(t, "stored", created.Properties()["view-property"])
	require.NotContains(t, created.Properties(), "storage-only")
	require.Equal(t, int64(7), version.VersionID)
	f, err := fs.Open(t.Context(), created.MetadataLocation())
	require.NoError(t, err)
	require.NoError(t, f.Close())
	_, err = iceio.LoadFS(t.Context(), nil, created.MetadataLocation())
	require.ErrorIs(t, err, iceio.ErrIOSchemeNotFound)
}
