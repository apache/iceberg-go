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

package rest

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"testing"

	"github.com/apache/iceberg-go"
	iceio "github.com/apache/iceberg-go/io"
	"github.com/stretchr/testify/require"
	"golang.org/x/sync/semaphore"
)

func TestIsolatedIORegistryForVendedCredentials(t *testing.T) {
	registry := &iceio.Registry{}
	fs := iceio.NewMemFS()
	var gotProps map[string]string
	require.NoError(t, registry.Register("isolated", func(_ context.Context, _ *url.URL, props map[string]string) (iceio.IO, error) {
		gotProps = props

		return fs, nil
	}))
	r := &vendedCredentialRefresher{
		mu: semaphore.NewWeighted(1), ioRegistry: registry,
		location: "isolated://bucket/metadata", props: iceberg.Properties{"base": "value", "token": "vended"},
		vended: iceberg.Properties{"token": "vended"},
	}
	loaded, err := r.loadFS(t.Context())
	require.NoError(t, err)
	require.Same(t, fs, loaded)
	require.Equal(t, "value", gotProps["base"])
	require.Equal(t, "vended", gotProps["token"])
	require.NoError(t, r.close())
}

func TestIsolatedIORegistryForPlanCredentials(t *testing.T) {
	registry := &iceio.Registry{}
	fs := iceio.NewMemFS()
	var gotProps map[string]string
	require.NoError(t, registry.Register("isolated", func(_ context.Context, _ *url.URL, props map[string]string) (iceio.IO, error) {
		gotProps = props

		return fs, nil
	}))
	p := planIOFromCredentialsWithRegistry([]StorageCredential{{Prefix: "isolated://bucket/", Config: iceberg.Properties{"token": "vended"}}}, "isolated://bucket/metadata", iceberg.Properties{"base": "value"}, registry)
	require.NotNil(t, p)
	loaded, err := p.Load(t.Context())
	require.NoError(t, err)
	resolver, ok := loaded.(*prefixScopedIO)
	require.True(t, ok)
	actual, err := resolver.filesystemFor("isolated://bucket/data")
	require.NoError(t, err)
	require.Same(t, fs, actual)
	require.Equal(t, "value", gotProps["base"])
	require.Equal(t, "vended", gotProps["token"])
	require.NoError(t, p.Close())
}

func TestCatalogUsesIsolatedIORegistry(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/config", serveEmptyConfig)
	server := httptest.NewServer(mux)
	defer server.Close()

	registry := &iceio.Registry{}
	fs := iceio.NewMemFS()
	require.NoError(t, registry.Register("isolated", func(context.Context, *url.URL, map[string]string) (iceio.IO, error) { return fs, nil }))
	cat, err := NewCatalog(t.Context(), "test", server.URL, WithIORegistry(registry))
	require.NoError(t, err)
	defer cat.Close()
	for _, vended := range []bool{false, true} {
		t.Run(fmt.Sprintf("vended=%t", vended), func(t *testing.T) {
			tbl, err := cat.tableFromResponse(t.Context(), []string{"ns", "tbl"}, nil, "isolated://bucket/metadata", nil, nil, vended, nil, nil)
			require.NoError(t, err)
			loaded, err := tbl.FS(t.Context())
			require.NoError(t, err)
			require.Same(t, fs, loaded)
		})
	}
}

func TestIsolatedIORegistryDoesNotUseGlobalFactories(t *testing.T) {
	registry := &iceio.Registry{}
	_, err := loadRegistryFS(t.Context(), registry, nil, "file:///metadata.json")
	require.ErrorIs(t, err, iceio.ErrIOSchemeNotFound)

	fs, err := loadRegistryFS(t.Context(), nil, nil, "file:///metadata.json")
	require.NoError(t, err)
	require.IsType(t, iceio.LocalFS{}, fs)
}

func TestCatalogsCanUseDifferentFactoriesForSameScheme(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/config", serveEmptyConfig)
	server := httptest.NewServer(mux)
	defer server.Close()

	cases := []struct {
		name string
		fs   *iceio.MemFS
	}{
		{name: "first", fs: iceio.NewMemFS()},
		{name: "second", fs: iceio.NewMemFS()},
	}
	catalogs := make([]*Catalog, len(cases))
	for i, tc := range cases {
		registry := &iceio.Registry{}
		fs := tc.fs
		require.NoError(t, registry.Register("same-scheme", func(context.Context, *url.URL, map[string]string) (iceio.IO, error) {
			return fs, nil
		}))

		cat, err := NewCatalog(t.Context(), tc.name, server.URL, WithIORegistry(registry))
		require.NoError(t, err)
		defer cat.Close()
		catalogs[i] = cat
	}

	// Both catalogs must remain alive while the same URI scheme resolves to
	// different factories, including through vended-credential refreshers.
	for i, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			for _, vended := range []bool{false, true} {
				tbl, err := catalogs[i].tableFromResponse(
					t.Context(), []string{"ns", "tbl"}, nil, "same-scheme://bucket/metadata", nil, nil, vended, nil, nil)
				require.NoError(t, err)
				loaded, err := tbl.FS(t.Context())
				require.NoError(t, err)
				require.Same(t, tc.fs, loaded)
			}
		})
	}
}
