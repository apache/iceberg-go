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

	"github.com/apache/iceberg-go"
	"github.com/apache/iceberg-go/catalog"
	"github.com/apache/iceberg-go/table"
)

func (s *SqliteCatalogTestSuite) TestListTablesAndViewsShareNamespaceButNotRows() {
	ctx := context.Background()
	cat := s.getCatalogSqlite()
	ns := table.Identifier{databaseName()}
	s.Require().NoError(cat.CreateNamespace(ctx, ns, nil))

	table1 := table.Identifier{ns[0], "listed_table_1"}
	table2 := table.Identifier{ns[0], "listed_table_2"}
	viewID := table.Identifier{ns[0], "listed_view"}

	_, err := cat.CreateTable(ctx, table1, tableSchemaNested)
	s.Require().NoError(err)
	_, err = cat.CreateTable(ctx, table2, tableSchemaNested)
	s.Require().NoError(err)

	viewSchema := iceberg.NewSchema(1, iceberg.NestedField{
		ID: 1, Name: "id", Type: iceberg.PrimitiveTypes.Int32, Required: true,
	})
	s.Require().NoError(cat.CreateView(ctx, viewID, viewSchema, "SELECT 1", nil))

	// A legacy V0 table migrated to V1 can have a NULL iceberg_type.
	// It must remain visible as a table without appearing in the view list.
	sqldb := s.getDB()
	defer sqldb.Close()
	result, err := sqldb.ExecContext(ctx,
		`UPDATE iceberg_tables SET iceberg_type = NULL `+
			`WHERE catalog_name = ? AND table_namespace = ? AND table_name = ?`,
		cat.Name(), ns[0], table2[1])
	s.Require().NoError(err)
	rows, err := result.RowsAffected()
	s.Require().NoError(err)
	s.Equal(int64(1), rows)

	var tables []table.Identifier
	for identifier, listErr := range cat.ListTables(ctx, ns) {
		s.Require().NoError(listErr)
		tables = append(tables, identifier)
	}
	s.ElementsMatch([]table.Identifier{table1, table2}, tables)

	var views []table.Identifier
	for identifier, listErr := range cat.ListViews(ctx, ns) {
		s.Require().NoError(listErr)
		views = append(views, identifier)
	}
	s.Equal([]table.Identifier{viewID}, views)

	// Listing iterators must honor a consumer stopping after the first item.
	seen := 0
	for _, listErr := range cat.ListTables(ctx, ns) {
		s.Require().NoError(listErr)
		seen++

		break
	}
	s.Equal(1, seen)

	// Missing namespace errors must be delivered once through either iterator.
	missing := table.Identifier{"missing_" + ns[0]}
	tableErrors := 0
	for _, listErr := range cat.ListTables(ctx, missing) {
		s.ErrorIs(listErr, catalog.ErrNoSuchNamespace)
		tableErrors++
	}
	s.Equal(1, tableErrors)

	viewErrors := 0
	for _, listErr := range cat.ListViews(ctx, missing) {
		s.ErrorIs(listErr, catalog.ErrNoSuchNamespace)
		viewErrors++
	}
	s.Equal(1, viewErrors)
}

func (s *SqliteCatalogTestSuite) TestV0ListViewsIgnoresMissingNamespace() {
	sqldb := s.getDB()
	s.createLegacyV0Catalog(sqldb)
	cat := s.loadCatalogV0NoOptIn()

	// V0 has no iceberg_type column or views. Listing must not query a
	// missing namespace just to discover that the view list is empty.
	var seen int
	for _, err := range cat.ListViews(context.Background(), table.Identifier{"missing"}) {
		s.Require().NoError(err)
		seen++
	}
	s.Zero(seen)
}
