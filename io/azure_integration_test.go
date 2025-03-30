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

//go:build integration

package io_test

import (
	"context"
	"fmt"
	"testing"

	"github.com/apache/iceberg-go"
	"github.com/apache/iceberg-go/catalog"
	sqlcat "github.com/apache/iceberg-go/catalog/sql"
	"github.com/apache/iceberg-go/io"
	"github.com/stretchr/testify/require"
	"github.com/uptrace/bun/driver/sqliteshim"
)

func TestAzuriteWarehouse(t *testing.T) {
	path := "iceberg-test-azure/test-table-azure"
	containerName := "container"
	accountName := "devstoreaccount1"
	accountKey := "Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw=="
	warehouseLocation := fmt.Sprintf("abfs://%s@%s.dfs.core.windows.net", containerName, accountName)
	// endpoint := fmt.Sprintf("http://127.0.0.1:10010/%s", accountName)

	cat, err := catalog.Load(context.Background(), "default", iceberg.Properties{
		"uri":                       ":memory:",
		sqlcat.DriverKey:            sqliteshim.ShimName,
		sqlcat.DialectKey:           string(sqlcat.SQLite),
		"type":                      "sql",
		"warehouse":                 warehouseLocation,
		io.AdlsSharedKeyAccountName: accountName,
		io.AdlsSharedKeyAccountKey:  accountKey,
		io.AdlsEndpoint:             "127.0.0.1:10010",
		io.AdlsProtocol:             "http",
	})
	require.NoError(t, err)

	require.NotNil(t, cat)

	c := cat.(*sqlcat.Catalog)
	ctx := context.Background()
	require.NoError(t, c.CreateNamespace(ctx, catalog.ToIdentifier("iceberg-test-azure"), nil))

	tbl, err := c.CreateTable(ctx,
		catalog.ToIdentifier("iceberg-test-azure", "test-table-azure"),
		iceberg.NewSchema(0, iceberg.NestedField{
			Name: "id", Type: iceberg.PrimitiveTypes.Int32, Required: true, ID: 1,
		}), catalog.WithLocation(fmt.Sprintf("abfs://warehouse/iceberg/%s", path)))
	require.NoError(t, err)
	require.NotNil(t, tbl)
}
