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

package catalogtest

import (
	"context"
	"testing"

	"github.com/apache/iceberg-go"
	"github.com/apache/iceberg-go/catalog"
	"github.com/apache/iceberg-go/table"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// sourceName returns the name of the column a partition or sort field reads
// from. Catalogs assign fresh field IDs on create, so fields are matched to
// what was passed by column name rather than by ID.
func sourceName(t *testing.T, schema *iceberg.Schema, sourceIDs []int) string {
	t.Helper()

	require.Len(t, sourceIDs, 1)
	name, ok := schema.FindColumnName(sourceIDs[0])
	require.Truef(t, ok, "source ID %d should be in the schema", sourceIDs[0])

	return name
}

// testCreateTableWithProperties asserts that properties passed at create time
// are visible on a fresh load.
func testCreateTableWithProperties(t *testing.T, cfg Config) {
	ctx := context.Background()
	cat := cfg.NewCatalog(t)

	ident, _ := createTable(t, cat, catalog.WithProperties(iceberg.Properties{"owner": "conformance"}))

	tbl, err := cat.LoadTable(ctx, ident)
	require.NoError(t, err)
	assert.Equal(t, "conformance", tbl.Properties()["owner"])
}

// testCreateTableWithPartitionSpec asserts that the partition spec passed at
// create time is the table's spec on a fresh load, with fresh spec and field
// IDs assigned.
func testCreateTableWithPartitionSpec(t *testing.T, cfg Config) {
	ctx := context.Background()
	cat := cfg.NewCatalog(t)

	spec, err := iceberg.NewPartitionSpecOpts(
		iceberg.WithSpecID(3),
		iceberg.AddPartitionFieldByName("id", "id_bucket", iceberg.BucketTransform{NumBuckets: 16}, Schema, nil),
		iceberg.AddPartitionFieldByName("data", "data", iceberg.IdentityTransform{}, Schema, nil),
	)
	require.NoError(t, err)

	ident, _ := createTable(t, cat, catalog.WithPartitionSpec(&spec))

	tbl, err := cat.LoadTable(ctx, ident)
	require.NoError(t, err)

	loaded := tbl.Spec()
	assert.Equal(t, iceberg.InitialPartitionSpecID, loaded.ID())
	require.Equal(t, spec.NumFields(), loaded.NumFields(), "loaded spec should have the fields it was created with")
	for i := range spec.NumFields() {
		want, got := spec.Field(i), loaded.Field(i)
		assert.Equal(t, iceberg.PartitionDataIDStart+i, got.FieldID)
		assert.Equal(t, want.Name, got.Name)
		assert.Equal(t, want.Transform, got.Transform)
		assert.Equal(t, sourceName(t, Schema, want.SourceIDs), sourceName(t, tbl.Schema(), got.SourceIDs))
	}
}

// testCreateTableWithSortOrder asserts that the sort order passed at create
// time is the table's sort order on a fresh load, with a fresh order ID
// assigned.
func testCreateTableWithSortOrder(t *testing.T, cfg Config) {
	ctx := context.Background()
	cat := cfg.NewCatalog(t)

	order, err := table.NewSortOrder(5, []table.SortField{
		{SourceIDs: []int{4}, Transform: iceberg.IdentityTransform{}, Direction: table.SortASC, NullOrder: table.NullsFirst},
		{SourceIDs: []int{3}, Transform: iceberg.BucketTransform{NumBuckets: 16}, Direction: table.SortDESC, NullOrder: table.NullsLast},
	})
	require.NoError(t, err)

	ident, _ := createTable(t, cat, catalog.WithSortOrder(order))

	tbl, err := cat.LoadTable(ctx, ident)
	require.NoError(t, err)

	loaded := tbl.SortOrder()
	require.False(t, loaded.IsUnsorted(), "loaded table should be sorted")
	assert.Equal(t, table.InitialSortOrderID, loaded.OrderID())
	require.Equal(t, order.Len(), loaded.Len(), "loaded sort order should have the fields it was created with")

	for i, got := range loaded.Fields() {
		want := order.Field(i)
		assert.Equal(t, want.Direction, got.Direction)
		assert.Equal(t, want.NullOrder, got.NullOrder)
		assert.Equal(t, want.Transform, got.Transform)
		assert.Equal(t, sourceName(t, Schema, want.SourceIDs), sourceName(t, tbl.Schema(), got.SourceIDs))
	}
}
