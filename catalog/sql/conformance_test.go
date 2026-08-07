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
	"path/filepath"
	"testing"

	"github.com/DataDog/iceberg-go"
	"github.com/DataDog/iceberg-go/catalog"
	"github.com/DataDog/iceberg-go/catalog/catalogtest"
	sqlcat "github.com/DataDog/iceberg-go/catalog/sql"
	"github.com/stretchr/testify/require"
	"github.com/uptrace/bun/driver/sqliteshim"
)

func TestSqlCatalogConformance(t *testing.T) {
	catalogtest.RunCatalogTests(t, catalogtest.Config{
		NewCatalog: func(t *testing.T) catalog.Catalog {
			warehouse := t.TempDir()

			cat, err := catalog.Load(context.Background(), "default", iceberg.Properties{
				"uri":             "file://" + filepath.Join(warehouse, "sql-catalog.db"),
				sqlcat.DriverKey:  sqliteshim.ShimName,
				sqlcat.DialectKey: string(sqlcat.SQLite),
				"type":            "sql",
				"warehouse":       "file://" + warehouse,
			})
			require.NoError(t, err)
			t.Cleanup(func() { require.NoError(t, cat.(*sqlcat.Catalog).Close()) })

			return cat
		},
	})
}
