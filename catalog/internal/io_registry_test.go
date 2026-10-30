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

package internal

import (
	"context"
	"net/url"
	"testing"

	"github.com/apache/iceberg-go"
	iceio "github.com/apache/iceberg-go/io"
	"github.com/apache/iceberg-go/table"
	"github.com/stretchr/testify/require"
)

func TestRegistryIsUsedForStagedAndUpdatedMetadata(t *testing.T) {
	registry := &iceio.Registry{}
	fs := iceio.NewMemFS()
	require.NoError(t, registry.Register("isolated-staging", func(context.Context, *url.URL, map[string]string) (iceio.IO, error) {
		return fs, nil
	}))

	props := iceberg.Properties{"warehouse": "isolated-staging://bucket/warehouse"}
	ident := table.Identifier{"db", "tbl"}
	schema := iceberg.NewSchema(1, iceberg.NestedField{
		ID: 1, Name: "id", Required: true, Type: iceberg.PrimitiveTypes.Int32,
	})
	nsprops := func(context.Context, table.Identifier) (iceberg.Properties, error) { return nil, nil }

	staged, err := CreateStagedTableWithRegistry(t.Context(), props, nsprops, ident, schema, registry)
	require.NoError(t, err)
	loaded, err := staged.FS(t.Context())
	require.NoError(t, err)
	require.Same(t, fs, loaded)
	require.NoError(t, WriteMetadata(t.Context(), staged.Table))

	f, err := fs.Open(t.Context(), staged.MetadataLocation())
	require.NoError(t, err)
	require.NoError(t, f.Close())

	updated, err := UpdateAndStageTableWithRegistry(
		t.Context(), props, staged.Table, ident, nil, nil, nil, registry,
	)
	require.NoError(t, err)
	loaded, err = updated.FS(t.Context())
	require.NoError(t, err)
	require.Same(t, fs, loaded)
	require.NoError(t, WriteMetadata(t.Context(), updated.Table))

	_, err = iceio.LoadFS(t.Context(), props, updated.MetadataLocation())
	require.ErrorIs(t, err, iceio.ErrIOSchemeNotFound)
}

func TestLoadFSWithRegistryDistinguishesNilFromEmpty(t *testing.T) {
	global, err := LoadFSWithRegistry(t.Context(), nil, nil, "file:///metadata.json")
	require.NoError(t, err)
	require.IsType(t, iceio.LocalFS{}, global)

	_, err = LoadFSWithRegistry(t.Context(), &iceio.Registry{}, nil, "file:///metadata.json")
	require.ErrorIs(t, err, iceio.ErrIOSchemeNotFound)

	_, err = LoadFSFuncWithRegistry(&iceio.Registry{}, nil, "file:///metadata.json")(t.Context())
	require.ErrorIs(t, err, iceio.ErrIOSchemeNotFound)
}
