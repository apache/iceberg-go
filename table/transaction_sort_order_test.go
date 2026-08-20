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
	"testing"

	"github.com/apache/iceberg-go"
	"github.com/apache/iceberg-go/table"
	"github.com/stretchr/testify/require"
)

func sortOrderOf(t *testing.T, fields ...table.SortField) table.SortOrder {
	t.Helper()
	order, err := table.NewSortOrder(1, fields)
	require.NoError(t, err)

	return order
}

func TestReplaceSortOrder(t *testing.T) {
	byID := table.SortField{
		SourceIDs: []int{1},
		Transform: iceberg.IdentityTransform{},
		Direction: table.SortASC,
		NullOrder: table.NullsFirst,
	}
	byName := table.SortField{
		SourceIDs: []int{2},
		Transform: iceberg.IdentityTransform{},
		Direction: table.SortDESC,
		NullOrder: table.NullsLast,
	}

	t.Run("replace unsorted with new order", func(t *testing.T) {
		txn := testNonPartitionedTable.NewTransaction()
		require.NoError(t, txn.ReplaceSortOrder(sortOrderOf(t, byID, byName)))

		staged, err := txn.StagedTable()
		require.NoError(t, err)

		got := staged.SortOrder()
		require.Equal(t, 1, got.OrderID())
		require.Equal(t, 2, got.Len())
		fields := make([]table.SortField, 0, got.Len())
		for _, f := range got.Fields() {
			fields = append(fields, f)
		}
		require.True(t, fields[0].Equals(byID))
		require.True(t, fields[1].Equals(byName))
		require.Len(t, staged.Metadata().SortOrders(), 2)
	})

	t.Run("field-identical replace is a no-op", func(t *testing.T) {
		txn := testNonPartitionedTable.NewTransaction()
		require.NoError(t, txn.ReplaceSortOrder(table.UnsortedSortOrder))

		staged, err := txn.StagedTable()
		require.NoError(t, err)
		require.Equal(t, table.UnsortedSortOrderID, staged.SortOrder().OrderID())
		require.Len(t, staged.Metadata().SortOrders(), 1)
	})

	t.Run("sequential replaces assign fresh ids", func(t *testing.T) {
		txn := testNonPartitionedTable.NewTransaction()
		require.NoError(t, txn.ReplaceSortOrder(sortOrderOf(t, byID)))
		require.NoError(t, txn.ReplaceSortOrder(sortOrderOf(t, byName)))

		staged, err := txn.StagedTable()
		require.NoError(t, err)
		require.Equal(t, 2, staged.SortOrder().OrderID())
		require.Len(t, staged.Metadata().SortOrders(), 3)
	})
}
