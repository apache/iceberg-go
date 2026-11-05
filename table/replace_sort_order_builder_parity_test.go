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

func TestReplaceSortOrderBuilderMatchesDirectReplacement(t *testing.T) {
	byID := table.SortField{
		SourceIDs: []int{1}, Transform: iceberg.IdentityTransform{},
		Direction: table.SortASC, NullOrder: table.NullsFirst,
	}
	byName := table.SortField{
		SourceIDs: []int{2}, Transform: iceberg.IdentityTransform{},
		Direction: table.SortDESC, NullOrder: table.NullsLast,
	}
	byMonth := table.SortField{
		SourceIDs: []int{3}, Transform: iceberg.MonthTransform{},
		Direction: table.SortDESC, NullOrder: table.NullsLast,
	}

	cases := []struct {
		name           string
		prepare        func(*testing.T, *table.Transaction)
		build          func(*table.Transaction) *table.ReplaceSortOrderBuilder
		wantOrderID    int
		wantOrderCount int
		wantUpdates    int
	}{
		{
			name: "new order",
			build: func(txn *table.Transaction) *table.ReplaceSortOrderBuilder {
				return txn.NewReplaceSortOrder(true).Asc("id", table.NullsFirst)
			},
			wantOrderID: 1, wantOrderCount: 2, wantUpdates: 2,
		},
		{
			name: "current order is a no-op",
			prepare: func(t *testing.T, txn *table.Transaction) {
				require.NoError(t, txn.ReplaceSortOrder(sortOrderOf(t, byID)))
			},
			build: func(txn *table.Transaction) *table.ReplaceSortOrderBuilder {
				return txn.NewReplaceSortOrder(true).Asc("id", table.NullsFirst)
			},
			wantOrderID: 1, wantOrderCount: 2, wantUpdates: 2,
		},
		{
			name: "reuse historical order",
			prepare: func(t *testing.T, txn *table.Transaction) {
				require.NoError(t, txn.ReplaceSortOrder(sortOrderOf(t, byID)))
				require.NoError(t, txn.ReplaceSortOrder(sortOrderOf(t, byName)))
			},
			build: func(txn *table.Transaction) *table.ReplaceSortOrderBuilder {
				return txn.NewReplaceSortOrder(true).Asc("id", table.NullsFirst)
			},
			wantOrderID: 1, wantOrderCount: 3, wantUpdates: 5,
		},
		{
			name: "reuse historical transformed order",
			prepare: func(t *testing.T, txn *table.Transaction) {
				require.NoError(t, txn.ReplaceSortOrder(sortOrderOf(t, byMonth)))
				require.NoError(t, txn.ReplaceSortOrder(sortOrderOf(t, byID)))
			},
			build: func(txn *table.Transaction) *table.ReplaceSortOrderBuilder {
				return txn.NewReplaceSortOrder(true).AddField("ts", iceberg.MonthTransform{}, table.SortDESC, table.NullsLast)
			},
			wantOrderID: 1, wantOrderCount: 3, wantUpdates: 5,
		},
		{
			name: "reset to unsorted",
			prepare: func(t *testing.T, txn *table.Transaction) {
				require.NoError(t, txn.ReplaceSortOrder(sortOrderOf(t, byID)))
			},
			build: func(txn *table.Transaction) *table.ReplaceSortOrderBuilder {
				return txn.NewReplaceSortOrder(true)
			},
			wantOrderID: 0, wantOrderCount: 2, wantUpdates: 3,
		},
		{
			name: "bind against staged schema",
			prepare: func(t *testing.T, txn *table.Transaction) {
				require.NoError(t, txn.UpdateSchema(true, false).
					AddColumn([]string{"priority"}, iceberg.PrimitiveTypes.Int64, "", false, nil).Commit())
			},
			build: func(txn *table.Transaction) *table.ReplaceSortOrderBuilder {
				return txn.NewReplaceSortOrder(true).Desc("priority", table.NullsLast)
			},
			wantOrderID: 1, wantOrderCount: 2,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			direct := testNonPartitionedTable.NewTransaction()
			withBuilder := testNonPartitionedTable.NewTransaction()
			if tc.prepare != nil {
				tc.prepare(t, direct)
				tc.prepare(t, withBuilder)
			}

			builder := tc.build(withBuilder)
			preview, err := builder.Apply()
			require.NoError(t, err)
			require.Equal(t, tc.wantOrderID, preview.OrderID())

			require.NoError(t, direct.ReplaceSortOrder(preview))
			require.NoError(t, builder.Commit())

			directCommit, err := direct.TableCommit()
			require.NoError(t, err)
			builderCommit, err := withBuilder.TableCommit()
			require.NoError(t, err)
			require.Equal(t, directCommit.Updates, builderCommit.Updates)
			require.Equal(t, directCommit.Requirements, builderCommit.Requirements)
			if tc.wantUpdates > 0 {
				require.Len(t, builderCommit.Updates, tc.wantUpdates)
			}

			directTable, err := direct.StagedTable()
			require.NoError(t, err)
			builderTable, err := withBuilder.StagedTable()
			require.NoError(t, err)
			require.True(t, directTable.SortOrder().Equals(builderTable.SortOrder()))
			require.Equal(t, tc.wantOrderID, builderTable.SortOrder().OrderID())
			require.Len(t, builderTable.Metadata().SortOrders(), tc.wantOrderCount)
		})
	}
}

func TestReplaceSortOrderBuilderCapturesValueTransform(t *testing.T) {
	transform := iceberg.BucketTransform{NumBuckets: 8}
	txn := testNonPartitionedTable.NewTransaction()
	builder := txn.NewReplaceSortOrder(true).
		AddField("id", transform, table.SortASC, table.NullsLast)
	transform.NumBuckets = 16

	preview, err := builder.Apply()
	require.NoError(t, err)
	require.Equal(t, "bucket[8]", preview.Field(0).Transform.String())

	require.NoError(t, builder.Commit())
	staged, err := txn.StagedTable()
	require.NoError(t, err)
	require.Equal(t, "bucket[8]", staged.SortOrder().Field(0).Transform.String())
}
