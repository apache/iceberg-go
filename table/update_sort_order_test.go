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

package table_test

import (
	"encoding/json"
	"testing"

	"github.com/apache/iceberg-go"
	"github.com/apache/iceberg-go/table"
	"github.com/stretchr/testify/require"
)

func TestUpdateSortOrderReplaceAndReuse(t *testing.T) {
	txn := testNonPartitionedTable.NewTransaction()

	builder := txn.UpdateSortOrder(true).Asc("id", table.NullsLast).Desc("name", table.NullsFirst)
	preview, err := builder.Apply()
	require.NoError(t, err)
	require.Equal(t, 1, preview.OrderID())
	require.Equal(t, 2, preview.Len())

	unchanged, err := txn.StagedTable()
	require.NoError(t, err)
	require.Equal(t, 0, unchanged.Metadata().DefaultSortOrder(), "Apply must not stage updates")

	updates, reqs, err := builder.BuildUpdates()
	require.NoError(t, err)
	require.Len(t, updates, 2)
	require.Len(t, reqs, 2)
	require.Equal(t, "assert-default-sort-order-id", reqs[0].GetType())
	require.Equal(t, "assert-current-schema-id", reqs[1].GetType())
	require.NoError(t, reqs[0].Validate(unchanged.Metadata()))

	raw, err := json.Marshal(updates)
	require.NoError(t, err)
	require.Contains(t, string(raw), "\"action\":\"add-sort-order\"")
	require.Contains(t, string(raw), "\"action\":\"set-default-sort-order\"")
	require.Contains(t, string(raw), "\"sort-order-id\":-1")

	require.NoError(t, builder.Commit())
	first, err := txn.StagedTable()
	require.NoError(t, err)
	require.Equal(t, 1, first.Metadata().DefaultSortOrder())
	require.Equal(t, 2, first.Metadata().SortOrder().Len())
	require.Len(t, first.Metadata().SortOrders(), 2)

	fields := make([]table.SortField, 0, 2)
	for _, f := range first.Metadata().SortOrder().Fields() {
		fields = append(fields, f)
	}
	require.Equal(t, []int{1}, fields[0].SourceIDs)
	require.Equal(t, table.SortASC, fields[0].Direction)
	require.Equal(t, table.NullsLast, fields[0].NullOrder)
	require.Equal(t, []int{2}, fields[1].SourceIDs)
	require.Equal(t, table.SortDESC, fields[1].Direction)
	require.Equal(t, table.NullsFirst, fields[1].NullOrder)

	// A concurrent change to the default sort order must invalidate the
	// optimistic-concurrency requirement captured from the original table.
	require.Error(t, reqs[0].Validate(first.Metadata()))

	// Reapplying the same sort order must not create a new order or update.
	require.NoError(t, builder.Commit())
	same, err := txn.StagedTable()
	require.NoError(t, err)
	require.Len(t, same.Metadata().SortOrders(), 2)

	// A different definition gets a new ID, retaining previous orders.
	require.NoError(t, txn.UpdateSortOrder(true).Desc("id", table.NullsFirst).Commit())
	second, err := txn.StagedTable()
	require.NoError(t, err)
	require.Equal(t, 2, second.Metadata().DefaultSortOrder())
	require.Len(t, second.Metadata().SortOrders(), 3)

	// Returning to a historical definition only changes the default ID.
	restore := txn.UpdateSortOrder(true).Asc("id", table.NullsLast).Desc("name", table.NullsFirst)
	reused, err := restore.Apply()
	require.NoError(t, err)
	require.Equal(t, 1, reused.OrderID())
	updates, _, err = restore.BuildUpdates()
	require.NoError(t, err)
	require.Len(t, updates, 1)
	require.NoError(t, restore.Commit())
	restored, err := txn.StagedTable()
	require.NoError(t, err)
	require.Equal(t, 1, restored.Metadata().DefaultSortOrder())
	require.Len(t, restored.Metadata().SortOrders(), 3)

	// Undoing sorting reuses order 0 and preserves historical definitions.
	unsort := txn.UpdateSortOrder(true)
	require.NoError(t, unsort.Commit())
	last, err := txn.StagedTable()
	require.NoError(t, err)
	require.Equal(t, 0, last.Metadata().DefaultSortOrder())
	require.True(t, last.Metadata().SortOrder().IsUnsorted())
	require.Len(t, last.Metadata().SortOrders(), 3)

	commits, err := txn.TableCommit()
	require.NoError(t, err)
	require.Len(t, commits.Requirements, 3, "deduplicate requirements across staged replacements")
}

func TestUpdateSortOrderNoopAndUnsortedAddition(t *testing.T) {
	txn := testNonPartitionedTable.NewTransaction()
	empty := txn.UpdateSortOrder(true)
	preview, err := empty.Apply()
	require.NoError(t, err)
	require.Equal(t, 0, preview.OrderID())
	updates, reqs, err := empty.BuildUpdates()
	require.NoError(t, err)
	require.Empty(t, updates)
	require.Empty(t, reqs)
	require.NoError(t, empty.Commit())
	commit, err := txn.TableCommit()
	require.NoError(t, err)
	require.Empty(t, commit.Updates)

	// A table initially created with a sorted order need not list order 0.
	sorted, err := table.NewSortOrder(1, []table.SortField{{
		SourceIDs: []int{1}, Transform: iceberg.IdentityTransform{},
		Direction: table.SortASC, NullOrder: table.NullsFirst,
	}})
	require.NoError(t, err)
	meta, err := table.NewMetadata(testSchema, iceberg.UnpartitionedSpec, sorted, "file:///tmp/sorted", nil)
	require.NoError(t, err)
	require.Equal(t, 1, meta.DefaultSortOrder())

	tbl := table.New([]string{"sorted"}, meta, "", nil, nil)
	t2 := tbl.NewTransaction()
	updates, _, err = t2.UpdateSortOrder(true).BuildUpdates()
	require.NoError(t, err)
	require.Len(t, updates, 2, "unsorted order must be registered before becoming default")
	require.NoError(t, t2.UpdateSortOrder(true).Commit())
	result, err := t2.StagedTable()
	require.NoError(t, err)
	require.Equal(t, 0, result.Metadata().DefaultSortOrder())
	require.Len(t, result.Metadata().SortOrders(), 2)
	require.Equal(t, 1, result.Metadata().SortOrders()[0].OrderID())
	require.Equal(t, 0, result.Metadata().SortOrders()[1].OrderID())
}

func TestUpdateSortOrderTransformsAndNameBinding(t *testing.T) {
	txn := testNonPartitionedTable.NewTransaction()
	builder := txn.UpdateSortOrder(false).
		AddField("TS", iceberg.MonthTransform{}, table.SortDESC, table.NullsLast).
		Asc("NaMe", table.NullsFirst)
	require.NoError(t, builder.Commit())
	staged, err := txn.StagedTable()
	require.NoError(t, err)
	require.Equal(t, 1, staged.Metadata().DefaultSortOrder())

	fields := make([]table.SortField, 0, 2)
	for _, f := range staged.Metadata().SortOrder().Fields() {
		fields = append(fields, f)
	}
	require.Equal(t, []int{3}, fields[0].SourceIDs)
	require.Equal(t, iceberg.MonthTransform{}, fields[0].Transform)
	require.Equal(t, table.SortDESC, fields[0].Direction)
	require.Equal(t, table.NullsLast, fields[0].NullOrder)
	require.Equal(t, []int{2}, fields[1].SourceIDs)
	require.Equal(t, iceberg.IdentityTransform{}, fields[1].Transform)
}

func TestUpdateSortOrderRejectsInvalidFieldsWithoutStaging(t *testing.T) {
	tests := []struct {
		name      string
		build     func(*table.Transaction) *table.UpdateSortOrder
		errorText string
	}{
		{
			name: "case sensitive field missing",
			build: func(t *table.Transaction) *table.UpdateSortOrder {
				return t.UpdateSortOrder(true).Asc("ID", table.NullsFirst)
			},
			errorText: "cannot bind sort field",
		},
		{
			name: "unknown field",
			build: func(t *table.Transaction) *table.UpdateSortOrder {
				return t.UpdateSortOrder(false).Desc("missing", table.NullsLast)
			},
			errorText: "cannot bind sort field",
		},
		{
			name: "non-primitive field",
			build: func(t *table.Transaction) *table.UpdateSortOrder {
				return t.UpdateSortOrder(true).Asc("address", table.NullsLast)
			},
			errorText: "cannot sort by non-primitive",
		},
		{
			name: "incompatible transform",
			build: func(t *table.Transaction) *table.UpdateSortOrder {
				return t.UpdateSortOrder(true).AddField("name", iceberg.YearTransform{}, table.SortASC, table.NullsLast)
			},
			errorText: "invalid source type",
		},
		{
			name: "nil transform",
			build: func(t *table.Transaction) *table.UpdateSortOrder {
				return t.UpdateSortOrder(true).AddField("id", nil, table.SortASC, table.NullsLast)
			},
			errorText: "has no transform",
		},
		{
			name: "invalid direction",
			build: func(t *table.Transaction) *table.UpdateSortOrder {
				return t.UpdateSortOrder(true).AddField("id", iceberg.IdentityTransform{}, "sideways", table.NullsLast)
			},
			errorText: "invalid sort direction",
		},
		{
			name: "invalid null placement",
			build: func(t *table.Transaction) *table.UpdateSortOrder {
				return t.UpdateSortOrder(true).AddField("id", iceberg.IdentityTransform{}, table.SortDESC, "random")
			},
			errorText: "invalid null order",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			txn := testNonPartitionedTable.NewTransaction()
			err := tt.build(txn).Commit()
			require.ErrorContains(t, err, tt.errorText)
			commit, err := txn.TableCommit()
			require.NoError(t, err)
			require.Empty(t, commit.Updates)
			staged, err := txn.StagedTable()
			require.NoError(t, err)
			require.Equal(t, 0, staged.Metadata().DefaultSortOrder())
		})
	}
}
