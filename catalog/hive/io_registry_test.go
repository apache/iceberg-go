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

package hive

import (
	"context"
	"net/url"
	"os"
	"testing"

	"github.com/apache/iceberg-go"
	"github.com/apache/iceberg-go/catalog"
	iceio "github.com/apache/iceberg-go/io"
	"github.com/apache/iceberg-go/table"
	"github.com/apache/iceberg-go/view"
	"github.com/beltran/gohive/hive_metastore"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

func TestHiveIsolatedIORegistry(t *testing.T) {
	const scheme = "hive-private-registry"
	const metadataLocation = scheme + "://bucket/table/metadata.json"
	registry := &iceio.Registry{}
	fs := iceio.NewMemFS()
	require.NoError(t, registry.Register(scheme, func(context.Context, *url.URL, map[string]string) (iceio.IO, error) { return fs, nil }))
	contents, err := os.ReadFile("../../table/testdata/TableMetadataV2Valid.json")
	require.NoError(t, err)
	require.NoError(t, fs.WriteFile(t.Context(), metadataLocation, contents))
	client := &mockHiveClient{}
	client.On("GetTable", mock.Anything, "db", "tbl").Return(&hive_metastore.Table{DbName: "db", TableName: "tbl", TableType: TableTypeExternalTable, Parameters: map[string]string{TableTypeKey: TableTypeIceberg, MetadataLocationKey: metadataLocation}}, nil).Once()
	cat := NewCatalogWithClientAndOptions(client, iceberg.Properties{"storage-key": "secret"}, WithIORegistry(registry))
	tbl, err := cat.LoadTable(t.Context(), TableIdentifier("db", "tbl"))
	require.NoError(t, err)
	loaded, err := tbl.FS(t.Context())
	require.NoError(t, err)
	require.Same(t, fs, loaded)
	_, err = iceio.LoadFS(t.Context(), nil, metadataLocation)
	require.ErrorIs(t, err, iceio.ErrIOSchemeNotFound)
	client.On("GetDatabase", mock.Anything, "db").Return(&hive_metastore.Database{Name: "db"}, nil).Once()
	client.On("GetTable", mock.Anything, "db", "view").Return(nil, errNoSuchObject).Twice()
	client.On("CreateTable", mock.Anything, mock.Anything).Return(nil).Once()
	version, err := view.NewVersionFromSQL(1, 0, view.Representation{SQL: "SELECT 1", Dialect: "hive"}, table.Identifier{"db"})
	require.NoError(t, err)
	schema := iceberg.NewSchema(1, iceberg.NestedField{ID: 1, Name: "id", Type: iceberg.PrimitiveTypes.Int32, Required: true})
	created, err := cat.CreateView(t.Context(), TableIdentifier("db", "view"), version, schema, catalog.WithViewLocation(scheme+"://bucket/view"))
	require.NoError(t, err)
	file, err := fs.Open(t.Context(), created.MetadataLocation())
	require.NoError(t, err)
	require.NoError(t, file.Close())
	require.NotContains(t, created.Properties(), "storage-key")
	client.AssertExpectations(t)
}
