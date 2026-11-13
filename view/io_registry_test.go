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
		created, err := CreateViewWithIORegistry(
			t.Context(), "test", []string{"db", "orders"},
			newTestSchema(0), Representation{SQL: "SELECT 1", Dialect: "spark"},
			[]string{"db"}, scheme+"://bucket/view", nil, registry,
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

	created, err := CreateViewWithIOPropertiesAndRegistry(
		t.Context(), "test", []string{"db", "orders"},
		newTestSchema(0), Representation{SQL: "SELECT 1", Dialect: "spark"},
		[]string{"db"}, "isolated-view-properties://bucket/view",
		iceberg.Properties{"storage-only": "token"},
		iceberg.Properties{"persisted": "value"}, registry,
	)
	require.NoError(t, err)
	require.Equal(t, "token", seen["storage-only"])
	require.Equal(t, "value", created.Properties()["persisted"])
	require.NotContains(t, created.Properties(), "storage-only")
}

func TestCreateViewWithEmptyRegistryDoesNotFallBackToGlobal(t *testing.T) {
	_, err := CreateViewWithIORegistry(
		t.Context(), "test", []string{"db", "orders"},
		newTestSchema(0), Representation{SQL: "SELECT 1", Dialect: "spark"},
		[]string{"db"}, "file:///tmp/view-registry-no-fallback", nil, &iceio.Registry{},
	)
	require.ErrorIs(t, err, iceio.ErrIOSchemeNotFound)
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
	created, err := CreateViewFromVersionWithRegistry(
		t.Context(), []string{"db", "v"}, newTestSchema(0), version,
		scheme+"://bucket/view", iceberg.Properties{"storage-only": "secret"},
		iceberg.Properties{"view-property": "stored"}, registry,
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
