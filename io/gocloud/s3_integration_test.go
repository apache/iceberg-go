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
	"testing"

	"github.com/apache/iceberg-go/internal/recipe"

	"github.com/apache/iceberg-go"
	"github.com/apache/iceberg-go/catalog"
	sqlcat "github.com/apache/iceberg-go/catalog/sql"
	"github.com/apache/iceberg-go/io"
	"github.com/stretchr/testify/require"
	"github.com/uptrace/bun/driver/sqliteshim"
)

func TestMinioWarehouse(t *testing.T) {
	_, err := recipe.Start(t)
	require.NoError(t, err)

	cat, err := catalog.Load(context.Background(), "default", iceberg.Properties{
		"uri":                ":memory:",
		sqlcat.DriverKey:     sqliteshim.ShimName,
		sqlcat.DialectKey:    string(sqlcat.SQLite),
		"type":               "sql",
		"warehouse":          "s3a://warehouse/iceberg/",
		io.S3Region:          "local",
		io.S3AccessKeyID:     "admin",
		io.S3SecretAccessKey: "password",
		// endpoint is passed via AWS_S3_ENDPOINT env var
	})
	require.NoError(t, err)

	require.NotNil(t, cat)

	c := cat.(*sqlcat.Catalog)
	ctx := context.Background()
	require.NoError(t, c.CreateNamespace(ctx, catalog.ToIdentifier("iceberg-test-2"), nil))

	tbl, err := c.CreateTable(ctx,
		catalog.ToIdentifier("iceberg-test-2", "test-table-2"),
		iceberg.NewSchema(0, iceberg.NestedField{
			Name: "id", Type: iceberg.PrimitiveTypes.Int32, Required: true, ID: 1,
		}), catalog.WithLocation("s3a://warehouse/iceberg/iceberg-test-2/test-table-2"))
	require.NoError(t, err)
	require.NotNil(t, tbl)
}

func TestMinioWarehouseNoLocation(t *testing.T) {
	_, err := recipe.Start(t)
	require.NoError(t, err)

	cat, err := catalog.Load(context.Background(), "default", iceberg.Properties{
		"uri":                ":memory:",
		sqlcat.DriverKey:     sqliteshim.ShimName,
		sqlcat.DialectKey:    string(sqlcat.SQLite),
		"type":               "sql",
		"warehouse":          "s3a://warehouse/iceberg/",
		io.S3Region:          "local",
		io.S3AccessKeyID:     "admin",
		io.S3SecretAccessKey: "password",
		// endpoint is passed via AWS_S3_ENDPOINT env var
	})
	require.NoError(t, err)

	require.NotNil(t, cat)

	c := cat.(*sqlcat.Catalog)
	ctx := context.Background()
	require.NoError(t, c.CreateNamespace(ctx, catalog.ToIdentifier("iceberg-test-2"), nil))

	tbl, err := c.CreateTable(ctx,
		catalog.ToIdentifier("iceberg-test-2", "test-table-2"),
		iceberg.NewSchema(0, iceberg.NestedField{
			Name: "id", Type: iceberg.PrimitiveTypes.Int32, Required: true, ID: 1,
		}))
	require.NoError(t, err)
	require.NotNil(t, tbl)
}
