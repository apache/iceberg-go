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

package hadoop

import (
	"context"
	"net/url"
	"sync/atomic"
	"testing"

	"github.com/apache/iceberg-go"
	iceio "github.com/apache/iceberg-go/io"
	"github.com/stretchr/testify/require"
)

func TestHadoopConstructorUsesIsolatedIORegistry(t *testing.T) {
	registry := &iceio.Registry{}
	fs := &stubHadoopCatalogFS{}
	require.NoError(t, registry.Register("hadoop-isolated", func(context.Context, *url.URL, map[string]string) (iceio.IO, error) {
		return fs, nil
	}))

	cat, err := NewCatalogWithOptions(t.Context(), "test", "hadoop-isolated://bucket/warehouse",
		iceberg.Properties{"allow-unsafe-commits": "true"}, WithIORegistry(registry))
	require.NoError(t, err)
	require.Same(t, fs, cat.filesystem)
	require.Same(t, registry, cat.ioRegistry)

	_, err = iceio.LoadFS(t.Context(), nil, "hadoop-isolated://bucket/warehouse")
	require.ErrorIs(t, err, iceio.ErrIOSchemeNotFound)
}

func TestHadoopEmptyRegistryCannotUseGlobalLocalBackend(t *testing.T) {
	_, err := NewCatalogWithOptions(t.Context(), "test", t.TempDir(), nil, WithIORegistry(&iceio.Registry{}))
	require.ErrorIs(t, err, iceio.ErrIOSchemeNotFound)
}

func TestHadoopRegistryUsedForTableCreateAndLoad(t *testing.T) {
	registry := &iceio.Registry{}
	var calls atomic.Int32
	require.NoError(t, registry.Register("", func(context.Context, *url.URL, map[string]string) (iceio.IO, error) {
		calls.Add(1)

		return iceio.LocalFS{}, nil
	}))
	cat, err := NewCatalogWithOptions(t.Context(), "test", t.TempDir(), nil, WithIORegistry(registry))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, cat.Close()) })
	require.NoError(t, cat.CreateNamespace(t.Context(), []string{"db"}, nil))
	schema := iceberg.NewSchema(1, iceberg.NestedField{ID: 1, Name: "id", Type: iceberg.PrimitiveTypes.Int32, Required: true})
	created, err := cat.CreateTable(t.Context(), []string{"db", "tbl"}, schema)
	require.NoError(t, err)
	initial := calls.Load()
	fs, err := created.FS(t.Context())
	require.NoError(t, err)
	require.IsType(t, iceio.LocalFS{}, fs)
	require.Greater(t, calls.Load(), initial)
	initial = calls.Load()
	loaded, err := cat.LoadTable(t.Context(), []string{"db", "tbl"})
	require.NoError(t, err)
	require.Greater(t, calls.Load(), initial)
	require.Equal(t, created.MetadataLocation(), loaded.MetadataLocation())
}
