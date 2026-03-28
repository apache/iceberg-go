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
	"fmt"
	"log"

	"github.com/apache/iceberg-go/catalog"
	"github.com/apache/iceberg-go/table"
)

// Example_multiTableTransaction demonstrates how to commit changes to
// multiple tables atomically using the MultiTableTransaction API.
// This requires a catalog that implements TransactionalCatalog (e.g.,
// the REST catalog).
func Example_multiTableTransaction() {
	ctx := context.Background()

	// Load catalog — in practice this comes from catalog.Load().
	var cat catalog.Catalog // e.g., rest.Catalog
	_ = cat

	// Load the tables you want to modify.
	var tbl1, tbl2 *table.Table // e.g., cat.LoadTable(ctx, ident)
	_ = ctx

	// Skip execution — this is a compile-check example only.
	if cat == nil {
		fmt.Println("multi-table commit example")
		return
	}

	// Create a multi-table transaction from the catalog.
	mtx, err := catalog.NewMultiTableTransaction(cat)
	if err != nil {
		log.Fatal(err)
	}

	// Build changes on each table via individual transactions.
	tx1 := tbl1.NewTransaction()
	_ = tx1.SetProperties(map[string]string{"pipeline": "v2"})
	_ = mtx.AddTransaction(tx1)

	tx2 := tbl2.NewTransaction()
	_ = tx2.SetProperties(map[string]string{"pipeline": "v2"})
	_ = mtx.AddTransaction(tx2)

	// Option A: Commit only — caller reloads tables manually.
	if err := mtx.Commit(ctx); err != nil {
		log.Fatal(err)
	}

	// Option B: Commit and reload all tables in one call.
	// tables, err := mtx.CommitAndReload(ctx)

	// Output: multi-table commit example
}
