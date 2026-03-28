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
	"fmt"

	"github.com/apache/iceberg-go/catalog"
)

// This example shows how to use the MultiTableTransaction API to
// commit changes to multiple tables atomically. The catalog must
// implement TransactionalCatalog (e.g., the REST catalog).
//
//	ctx := context.Background()
//	cat, _ := catalog.Load(ctx, "local", props)
//
//	tbl1, _ := cat.LoadTable(ctx, catalog.ToIdentifier("db", "orders"))
//	tbl2, _ := cat.LoadTable(ctx, catalog.ToIdentifier("db", "inventory"))
//
//	mtx, _ := catalog.NewMultiTableTransaction(cat)
//
//	tx1 := tbl1.NewTransaction()
//	tx1.SetProperties(map[string]string{"pipeline": "v2"})
//	mtx.AddTransaction(tx1)
//
//	tx2 := tbl2.NewTransaction()
//	tx2.SetProperties(map[string]string{"pipeline": "v2"})
//	mtx.AddTransaction(tx2)
//
//	// Option A: Commit only — caller reloads tables manually.
//	mtx.Commit(ctx)
//
//	// Option B: Commit and reload all affected tables.
//	tables, _ := mtx.CommitAndReload(ctx)
func Example_multiTableTransaction() {
	// NewMultiTableTransaction requires a Catalog that implements
	// TransactionalCatalog. Passing nil demonstrates the error path.
	_, err := catalog.NewMultiTableTransaction(nil)
	fmt.Println(err)

	// Output: catalog does not support multi-table transactions
}
