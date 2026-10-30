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

package sql_test

import (
	"context"
	"database/sql"
	"net/url"
	"path/filepath"
	"testing"

	"github.com/apache/iceberg-go"
	sqlcat "github.com/apache/iceberg-go/catalog/sql"
	iceio "github.com/apache/iceberg-go/io"
	"github.com/apache/iceberg-go/table"
	"github.com/apache/iceberg-go/view"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	"github.com/uptrace/bun/driver/sqliteshim"
)

func TestSQLCatalogIORegistriesAreIsolated(t *testing.T) {
	const scheme = "sql-private-registry"
	type catalogCase struct {
		cat *sqlcat.Catalog
		fs  *iceio.MemFS
	}
	cases := make([]catalogCase, 2)

	for i := range cases {
		fs := iceio.NewMemFS()
		registry := &iceio.Registry{}
		require.NoError(t, registry.Register(scheme, func(context.Context, *url.URL, map[string]string) (iceio.IO, error) {
			return fs, nil
		}))

		db, err := sql.Open(sqliteshim.ShimName, filepath.Join(t.TempDir(), "catalog.db"))
		require.NoError(t, err)
		t.Cleanup(func() { require.NoError(t, db.Close()) })

		cat, err := sqlcat.NewCatalogWithOptions("isolated", db, sqlcat.SQLite,
			iceberg.Properties{"warehouse": scheme + "://bucket/warehouse"},
			sqlcat.WithIORegistry(registry))
		require.NoError(t, err)
		t.Cleanup(func() { require.NoError(t, cat.Close()) })
		cases[i] = catalogCase{cat: cat, fs: fs}
	}

	schema := iceberg.NewSchema(1, iceberg.NestedField{
		ID: 1, Name: "id", Type: iceberg.PrimitiveTypes.Int32, Required: true,
	})
	for i, tc := range cases {
		ident := []string{"db", "table_" + uuid.NewString()}
		require.NoError(t, tc.cat.CreateNamespace(t.Context(), []string{"db"}, nil))

		created, err := tc.cat.CreateTable(t.Context(), ident, schema)
		require.NoError(t, err)
		loaded, err := tc.cat.LoadTable(t.Context(), ident)
		require.NoError(t, err)

		for _, tbl := range []*table.Table{created, loaded} {
			got, err := tbl.FS(t.Context())
			require.NoError(t, err)
			require.Same(t, tc.fs, got)
		}

		metadataFile, err := tc.fs.Open(t.Context(), created.MetadataLocation())
		require.NoError(t, err)
		require.NoError(t, metadataFile.Close())
		_, err = cases[1-i].fs.Open(t.Context(), created.MetadataLocation())
		require.Error(t, err)

		_, err = iceio.LoadFS(t.Context(), nil, created.MetadataLocation())
		require.ErrorIs(t, err, iceio.ErrIOSchemeNotFound)
	}
}

func TestSQLViewUsesCatalogIOPropertiesWithIsolatedRegistry(t *testing.T) {
	const scheme = "sql-view-private-registry"
	registry := &iceio.Registry{}
	fs := iceio.NewMemFS()
	var seen map[string]string
	require.NoError(t, registry.Register(scheme, func(_ context.Context, _ *url.URL, props map[string]string) (iceio.IO, error) {
		seen = props

		return fs, nil
	}))

	db, err := sql.Open(sqliteshim.ShimName, filepath.Join(t.TempDir(), "views.db"))
	require.NoError(t, err)
	defer db.Close()

	cat, err := sqlcat.NewCatalogWithOptions("isolated", db, sqlcat.SQLite,
		iceberg.Properties{
			"warehouse":             scheme + "://bucket/warehouse",
			"storage-secret":        "catalog-only",
			sqlcat.SchemaVersionKey: sqlcat.SchemaVersionV1,
		},
		sqlcat.WithIORegistry(registry),
	)
	require.NoError(t, err)
	defer cat.Close()

	ident := []string{"db", "orders_view"}
	require.NoError(t, cat.CreateNamespace(t.Context(), []string{"db"}, nil))
	schema := iceberg.NewSchema(1, iceberg.NestedField{
		ID: 1, Name: "id", Type: iceberg.PrimitiveTypes.Int32, Required: true,
	})
	err = cat.CreateView(t.Context(), ident, schema, view.Representation{
		SQL: "SELECT 1", Dialect: "spark",
	}, iceberg.Properties{"view-prop": "persisted"})
	require.NoError(t, err)
	require.Equal(t, "catalog-only", seen["storage-secret"])

	loaded, err := cat.LoadView(t.Context(), ident)
	require.NoError(t, err)
	require.Equal(t, "persisted", loaded.Properties()["view-prop"])
	require.NotContains(t, loaded.Properties(), "storage-secret")
	require.NoError(t, cat.DropView(t.Context(), ident))
}

type trackingRegistryFS struct {
	*iceio.MemFS
	removed []string
}

func (fs *trackingRegistryFS) Remove(ctx context.Context, path string) error {
	fs.removed = append(fs.removed, path)

	return fs.MemFS.Remove(ctx, path)
}

func TestSQLViewCollisionCleanupUsesViewIOProperties(t *testing.T) {
	const scheme = "sql-view-cleanup-registry"
	registry := &iceio.Registry{}
	fs := &trackingRegistryFS{MemFS: iceio.NewMemFS()}
	var seen []string
	require.NoError(t, registry.Register(scheme, func(_ context.Context, _ *url.URL, props map[string]string) (iceio.IO, error) {
		seen = append(seen, props["storage-token"])

		return fs, nil
	}))
	db, err := sql.Open(sqliteshim.ShimName, filepath.Join(t.TempDir(), "cleanup.db"))
	require.NoError(t, err)
	defer db.Close()
	cat, err := sqlcat.NewCatalogWithOptions("test", db, sqlcat.SQLite,
		iceberg.Properties{"warehouse": scheme + "://bucket/warehouse", "storage-token": "catalog", sqlcat.SchemaVersionKey: sqlcat.SchemaVersionV1},
		sqlcat.WithIORegistry(registry))
	require.NoError(t, err)
	defer cat.Close()
	require.NoError(t, cat.CreateNamespace(t.Context(), []string{"db"}, nil))
	schema := iceberg.NewSchema(1, iceberg.NestedField{ID: 1, Name: "id", Type: iceberg.PrimitiveTypes.Int32, Required: true})
	ident := []string{"db", "view"}
	representation := view.Representation{SQL: "SELECT 1", Dialect: "spark"}
	require.NoError(t, cat.CreateView(t.Context(), ident, schema, representation, iceberg.Properties{"storage-token": "view"}))
	seen = nil
	err = cat.CreateView(t.Context(), ident, schema, representation, iceberg.Properties{"storage-token": "view"})
	require.Error(t, err)
	require.Equal(t, []string{"view", "view"}, seen)
	require.Len(t, fs.removed, 1)
	_, err = fs.Open(t.Context(), fs.removed[0])
	require.Error(t, err)
}
