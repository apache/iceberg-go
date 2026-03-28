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

package catalog_test

import (
	"context"
	"errors"
	"fmt"
	"iter"

	"github.com/apache/iceberg-go"
	"github.com/apache/iceberg-go/catalog"
	"github.com/apache/iceberg-go/table"
)

// exampleCatalog is a minimal Catalog + TransactionalCatalog stub
// for the runnable example.
type exampleCatalog struct {
	tables map[string]*table.Table
}

func (c *exampleCatalog) CatalogType() catalog.Type { return catalog.REST }

func (c *exampleCatalog) CommitTransaction(_ context.Context, _ []table.TableCommit) error {
	return nil
}

func (c *exampleCatalog) LoadTable(_ context.Context, id table.Identifier) (*table.Table, error) {
	if t, ok := c.tables[id[len(id)-1]]; ok {
		return t, nil
	}

	return nil, errors.New("table not found")
}

func (c *exampleCatalog) ListTables(context.Context, table.Identifier) iter.Seq2[table.Identifier, error] {
	return nil
}

func (c *exampleCatalog) CreateTable(context.Context, table.Identifier, *iceberg.Schema, ...catalog.CreateTableOpt) (*table.Table, error) {
	return nil, nil
}

func (c *exampleCatalog) CommitTable(context.Context, table.Identifier, []table.Requirement, []table.Update) (table.Metadata, string, error) {
	return nil, "", nil
}

func (c *exampleCatalog) DropTable(context.Context, table.Identifier) error { return nil }

func (c *exampleCatalog) RenameTable(context.Context, table.Identifier, table.Identifier) (*table.Table, error) {
	return nil, nil
}

func (c *exampleCatalog) CheckTableExists(context.Context, table.Identifier) (bool, error) {
	return false, nil
}

func (c *exampleCatalog) ListNamespaces(context.Context, table.Identifier) ([]table.Identifier, error) {
	return nil, nil
}

func (c *exampleCatalog) CreateNamespace(context.Context, table.Identifier, iceberg.Properties) error {
	return nil
}

func (c *exampleCatalog) DropNamespace(context.Context, table.Identifier) error { return nil }

func (c *exampleCatalog) CheckNamespaceExists(context.Context, table.Identifier) (bool, error) {
	return false, nil
}

func (c *exampleCatalog) LoadNamespaceProperties(context.Context, table.Identifier) (iceberg.Properties, error) {
	return nil, nil
}

func (c *exampleCatalog) UpdateNamespaceProperties(context.Context, table.Identifier, []string, iceberg.Properties) (catalog.PropertiesUpdateSummary, error) {
	return catalog.PropertiesUpdateSummary{}, nil
}

func makeExampleTable(ns, name string) *table.Table {
	schema := iceberg.NewSchema(0,
		iceberg.NestedField{ID: 1, Name: "id", Type: iceberg.PrimitiveTypes.Int64, Required: true},
	)
	meta, _ := table.NewMetadata(schema, iceberg.UnpartitionedSpec,
		table.UnsortedSortOrder, "s3://bucket/"+name,
		iceberg.Properties{table.PropertyFormatVersion: "2"})

	return table.New(table.Identifier{ns, name}, meta, "", nil, nil)
}

// This example demonstrates atomic multi-table commits using the
// MultiTableTransaction API. Changes to multiple tables are collected
// and committed in a single all-or-nothing request.
func Example_multiTableTransaction() {
	ctx := context.Background()

	tbl1 := makeExampleTable("db", "orders")
	tbl2 := makeExampleTable("db", "inventory")

	cat := &exampleCatalog{
		tables: map[string]*table.Table{
			"orders":    tbl1,
			"inventory": tbl2,
		},
	}

	// Create a multi-table transaction from the catalog.
	mtx, err := catalog.NewMultiTableTransaction(cat)
	if err != nil {
		panic(err)
	}

	// Build changes on each table via individual transactions.
	tx1 := tbl1.NewTransaction()
	_ = tx1.SetProperties(map[string]string{"pipeline": "v2"})
	_ = mtx.AddTransaction(tx1)

	tx2 := tbl2.NewTransaction()
	_ = tx2.SetProperties(map[string]string{"pipeline": "v2"})
	_ = mtx.AddTransaction(tx2)

	// Option A: Commit only — caller reloads tables manually.
	err = mtx.Commit(ctx)
	if err != nil {
		panic(err)
	}

	fmt.Println("committed 2 tables atomically")

	// Option B: CommitAndReload commits and reloads all tables.
	// tables, err := mtx.CommitAndReload(ctx)

	// Output: committed 2 tables atomically
}
