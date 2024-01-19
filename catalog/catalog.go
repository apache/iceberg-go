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

package catalog

import (
	"context"
	"errors"

	"github.com/apache/iceberg-go/table"
)

type CatalogType string

const (
	REST     CatalogType = "rest"
	Hive     CatalogType = "hive"
	Glue     CatalogType = "glue"
	DynamoDB CatalogType = "dynamodb"
	SQL      CatalogType = "sql"
)

var (
	// ErrNoSuchTable is returned when a table does not exist in the catalog.
	ErrNoSuchTable = errors.New("table does not exist")
)

// Catalog for iceberg table operations like create, drop, load, list and others.
type Catalog interface {
	GetTable(ctx context.Context, identifier table.Identifier) (CatalogTable, error)
	ListTables(ctx context.Context, identifier table.Identifier) ([]CatalogTable, error)
	LoadTable(ctx context.Context, table CatalogTable) (*table.Table, error)
	CatalogType() CatalogType
}

// CatalogTable is the details of a table in a catalog.
type CatalogTable struct {
	Identifier  table.Identifier // this identifier may vary depending on the catalog implementation
	Location    string           // URL to the table location
	CatalogType CatalogType
}
