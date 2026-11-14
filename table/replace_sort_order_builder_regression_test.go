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

func TestReplaceSortOrderBuilderDistinctFieldDefinitions(t *testing.T) {
	cases := []struct {
		name  string
		build func(*table.Transaction) *table.ReplaceSortOrderBuilder
	}{
		{"id ascending, nulls first", func(txn *table.Transaction) *table.ReplaceSortOrderBuilder {
			return txn.NewReplaceSortOrder(true).Asc("id", table.NullsFirst)
		}},
		{"change only null order", func(txn *table.Transaction) *table.ReplaceSortOrderBuilder {
			return txn.NewReplaceSortOrder(true).Asc("id", table.NullsLast)
		}},
		{"change only direction", func(txn *table.Transaction) *table.ReplaceSortOrderBuilder {
			return txn.NewReplaceSortOrder(true).Desc("id", table.NullsLast)
		}},
		{"two fields", func(txn *table.Transaction) *table.ReplaceSortOrderBuilder {
			return txn.NewReplaceSortOrder(true).Asc("id", table.NullsFirst).Desc("name", table.NullsLast)
		}},
		{"reverse field priority", func(txn *table.Transaction) *table.ReplaceSortOrderBuilder {
			return txn.NewReplaceSortOrder(true).Desc("name", table.NullsLast).Asc("id", table.NullsFirst)
		}},
		{"bucket eight", func(txn *table.Transaction) *table.ReplaceSortOrderBuilder {
			return txn.NewReplaceSortOrder(true).AddField("id", iceberg.BucketTransform{NumBuckets: 8}, table.SortASC, table.NullsFirst)
		}},
		{"bucket sixteen", func(txn *table.Transaction) *table.ReplaceSortOrderBuilder {
			return txn.NewReplaceSortOrder(true).AddField("id", iceberg.BucketTransform{NumBuckets: 16}, table.SortASC, table.NullsFirst)
		}},
		{"year transform", func(txn *table.Transaction) *table.ReplaceSortOrderBuilder {
			return txn.NewReplaceSortOrder(true).AddField("ts", iceberg.YearTransform{}, table.SortASC, table.NullsFirst)
		}},
		{"month transform", func(txn *table.Transaction) *table.ReplaceSortOrderBuilder {
			return txn.NewReplaceSortOrder(true).AddField("ts", iceberg.MonthTransform{}, table.SortASC, table.NullsFirst)
		}},
	}

	txn := testNonPartitionedTable.NewTransaction()
	for i, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			builder := tc.build(txn)
			preview, err := builder.Apply()
			require.NoError(t, err)
			require.Equal(t, i+1, preview.OrderID(), "changing a sort-field property must assign a new ID")
			require.NoError(t, builder.Commit())

			staged, err := txn.StagedTable()
			require.NoError(t, err)
			require.True(t, preview.Equals(staged.SortOrder()))
			require.Len(t, staged.Metadata().SortOrders(), i+2)
		})
	}

	// Even after multiple new orders, reusing an older transformed order
	// must reference its concrete ID instead of the latest add (-1).
	oldOrder := cases[5].build(txn)
	preview, err := oldOrder.Apply()
	require.NoError(t, err)
	require.Equal(t, 6, preview.OrderID())
	require.NoError(t, oldOrder.Commit())

	commit, err := txn.TableCommit()
	require.NoError(t, err)
	last, err := json.Marshal(commit.Updates[len(commit.Updates)-1])
	require.NoError(t, err)
	require.JSONEq(t, `{"action":"set-default-sort-order","sort-order-id":6}`, string(last))

	replay, err := table.MetadataBuilderFromBase(testNonPartitionedTable.Metadata(), "")
	require.NoError(t, err)
	for _, update := range commit.Updates {
		require.NoError(t, update.Apply(replay))
	}
	replayed, err := replay.Build()
	require.NoError(t, err)
	require.Equal(t, 6, replayed.DefaultSortOrder())
	require.Len(t, replayed.SortOrders(), len(cases)+1)
}

func TestReplaceSortOrderBuilderInvalidLaterFieldPreservesStagedOrder(t *testing.T) {
	txn := testNonPartitionedTable.NewTransaction()
	require.NoError(t, txn.NewReplaceSortOrder(true).Asc("id", table.NullsFirst).Commit())
	before, err := txn.TableCommit()
	require.NoError(t, err)
	beforeWire, err := json.Marshal(before.Updates)
	require.NoError(t, err)

	cases := []struct {
		name      string
		build     func(*table.Transaction) *table.ReplaceSortOrderBuilder
		wantError string
	}{
		{"missing second column", func(txn *table.Transaction) *table.ReplaceSortOrderBuilder {
			return txn.NewReplaceSortOrder(true).Asc("name", table.NullsFirst).Asc("missing", table.NullsLast)
		}, "cannot bind sort field"},
		{"invalid second transform", func(txn *table.Transaction) *table.ReplaceSortOrderBuilder {
			return txn.NewReplaceSortOrder(true).Asc("name", table.NullsFirst).
				AddField("name", iceberg.YearTransform{}, table.SortASC, table.NullsLast)
		}, "invalid source type"},
		{"nil second transform", func(txn *table.Transaction) *table.ReplaceSortOrderBuilder {
			return txn.NewReplaceSortOrder(true).Asc("name", table.NullsFirst).
				AddField("id", nil, table.SortASC, table.NullsFirst)
		}, "has no transform"},
		{"invalid second direction", func(txn *table.Transaction) *table.ReplaceSortOrderBuilder {
			return txn.NewReplaceSortOrder(true).Asc("name", table.NullsFirst).
				AddField("id", iceberg.IdentityTransform{}, "sideways", table.NullsFirst)
		}, "invalid sort direction"},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			builder := tc.build(txn)
			_, err := builder.Apply()
			require.ErrorContains(t, err, tc.wantError)
			require.ErrorContains(t, builder.Commit(), tc.wantError)

			after, err := txn.TableCommit()
			require.NoError(t, err)
			afterWire, err := json.Marshal(after.Updates)
			require.NoError(t, err)
			require.JSONEq(t, string(beforeWire), string(afterWire))
			require.Len(t, after.Requirements, len(before.Requirements))

			staged, err := txn.StagedTable()
			require.NoError(t, err)
			require.Equal(t, 1, staged.Metadata().DefaultSortOrder())
			require.Len(t, staged.Metadata().SortOrders(), 2)
		})
	}
}

func TestReplaceSortOrderBuilderNestedCaseCollisionsAreScoped(t *testing.T) {
	schema := iceberg.NewSchema(0,
		iceberg.NestedField{ID: 1, Name: "payload", Type: iceberg.NewStructType(
			iceberg.NestedField{ID: 2, Name: "ID", Type: iceberg.PrimitiveTypes.Int64},
			iceberg.NestedField{ID: 3, Name: "id", Type: iceberg.PrimitiveTypes.Int64},
			iceberg.NestedField{ID: 4, Name: "other", Type: iceberg.PrimitiveTypes.Int64},
		)},
		iceberg.NestedField{ID: 5, Name: "sibling", Type: iceberg.NewStructType(
			iceberg.NestedField{ID: 6, Name: "id", Type: iceberg.PrimitiveTypes.Int64},
		)},
		iceberg.NestedField{ID: 7, Name: "unique", Type: iceberg.PrimitiveTypes.Int64},
	)
	meta, err := table.NewMetadata(schema, iceberg.UnpartitionedSpec, table.UnsortedSortOrder,
		"file:///tmp/nested-sort-collisions", nil)
	require.NoError(t, err)
	tbl := table.New([]string{"nested-sort-collisions"}, meta, "", nil, nil)

	for _, name := range []string{"payload.ID", "payload.id", "PAYLOAD.Id"} {
		t.Run("ambiguous "+name, func(t *testing.T) {
			txn := tbl.NewTransaction()
			builder := txn.NewReplaceSortOrder(false).Asc(name, table.NullsFirst)
			_, err := builder.Apply()
			require.ErrorIs(t, err, iceberg.ErrInvalidSchema)
			require.ErrorContains(t, err, `"payload.ID" and "payload.id"`)
			require.ErrorIs(t, builder.Commit(), iceberg.ErrInvalidSchema)

			commit, err := txn.TableCommit()
			require.NoError(t, err)
			require.Empty(t, commit.Updates)
		})
	}

	for _, tc := range []struct {
		name          string
		caseSensitive bool
		sourceName    string
	}{
		{"payload.ID", true, "payload.ID"},
		{"payload.id", true, "payload.id"},
		{"PAYLOAD.OTHER", false, "payload.other"},
		{"SIBLING.ID", false, "sibling.id"},
		{"UnIqUe", false, "unique"},
	} {
		t.Run("unambiguous "+tc.name, func(t *testing.T) {
			source, found := tbl.Schema().FindFieldByName(tc.sourceName)
			require.True(t, found)

			txn := tbl.NewTransaction()
			builder := txn.NewReplaceSortOrder(tc.caseSensitive).Asc(tc.name, table.NullsLast)
			preview, err := builder.Apply()
			require.NoError(t, err)
			require.Equal(t, source.ID, preview.Field(0).FirstSourceID())
			require.NoError(t, builder.Commit())

			staged, err := txn.StagedTable()
			require.NoError(t, err)
			require.Equal(t, source.ID, staged.SortOrder().Field(0).FirstSourceID())
		})
	}
}
