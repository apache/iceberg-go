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

func TestReplaceSortOrderBuilderReplaceAndReuse(t *testing.T) {
	txn := testNonPartitionedTable.NewTransaction()

	builder := txn.NewReplaceSortOrder(true).Asc("id", table.NullsLast).Desc("name", table.NullsFirst)
	preview, err := builder.Apply()
	require.NoError(t, err)
	require.Equal(t, 1, preview.OrderID())
	require.Equal(t, 2, preview.Len())

	unchanged, err := txn.StagedTable()
	require.NoError(t, err)
	require.Equal(t, 0, unchanged.Metadata().DefaultSortOrder(), "Apply must not stage updates")

	require.NoError(t, builder.Commit())
	firstCommit, err := txn.TableCommit()
	require.NoError(t, err)
	require.Len(t, firstCommit.Updates, 2)
	require.Len(t, firstCommit.Requirements, 3)
	require.Equal(t, "assert-default-sort-order-id", firstCommit.Requirements[0].GetType())
	require.NoError(t, firstCommit.Requirements[0].Validate(unchanged.Metadata()))

	raw, err := json.Marshal(firstCommit.Updates)
	require.NoError(t, err)
	require.Contains(t, string(raw), "\"action\":\"add-sort-order\"")
	require.Contains(t, string(raw), "\"action\":\"set-default-sort-order\"")
	require.Contains(t, string(raw), "\"sort-order-id\":-1")
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
	require.Error(t, firstCommit.Requirements[0].Validate(first.Metadata()))

	// Reapplying the same sort order must not create a new order or update.
	require.NoError(t, builder.Commit())
	same, err := txn.StagedTable()
	require.NoError(t, err)
	require.Len(t, same.Metadata().SortOrders(), 2)

	// A different definition gets a new ID, retaining previous orders.
	require.NoError(t, txn.NewReplaceSortOrder(true).Desc("id", table.NullsFirst).Commit())
	second, err := txn.StagedTable()
	require.NoError(t, err)
	require.Equal(t, 2, second.Metadata().DefaultSortOrder())
	require.Len(t, second.Metadata().SortOrders(), 3)

	// Returning to a historical definition only changes the default ID.
	restore := txn.NewReplaceSortOrder(true).Asc("id", table.NullsLast).Desc("name", table.NullsFirst)
	reused, err := restore.Apply()
	require.NoError(t, err)
	require.Equal(t, 1, reused.OrderID())

	beforeRestore, err := txn.TableCommit()
	require.NoError(t, err)
	require.NoError(t, restore.Commit())
	afterRestore, err := txn.TableCommit()
	require.NoError(t, err)
	require.Len(t, afterRestore.Updates, len(beforeRestore.Updates)+1)
	raw, err = json.Marshal(afterRestore.Updates[len(afterRestore.Updates)-1])
	require.NoError(t, err)
	require.Contains(t, string(raw), "\"sort-order-id\":1")
	restored, err := txn.StagedTable()
	require.NoError(t, err)
	require.Equal(t, 1, restored.Metadata().DefaultSortOrder())
	require.Len(t, restored.Metadata().SortOrders(), 3)

	// Undoing sorting reuses order 0 and preserves historical definitions.
	unsort := txn.NewReplaceSortOrder(true)
	require.NoError(t, unsort.Commit())
	last, err := txn.StagedTable()
	require.NoError(t, err)
	require.Equal(t, 0, last.Metadata().DefaultSortOrder())
	require.True(t, last.Metadata().SortOrder().IsUnsorted())
	require.Len(t, last.Metadata().SortOrders(), 3)

	commits, err := txn.TableCommit()
	require.NoError(t, err)
	require.Len(t, commits.Requirements, 3, "deduplicate requirements across staged replacements")
	require.Equal(t, "assert-default-sort-order-id", commits.Requirements[0].GetType())
	require.Equal(t, "assert-ref-snapshot-id", commits.Requirements[1].GetType())
	require.Equal(t, "assert-table-uuid", commits.Requirements[2].GetType())
}

func TestReplaceSortOrderBuilderNoopAndUnsortedAddition(t *testing.T) {
	txn := testNonPartitionedTable.NewTransaction()
	empty := txn.NewReplaceSortOrder(true)
	preview, err := empty.Apply()
	require.NoError(t, err)
	require.Equal(t, 0, preview.OrderID())
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
	require.NoError(t, t2.NewReplaceSortOrder(true).Commit())
	result, err := t2.StagedTable()
	require.NoError(t, err)
	require.Equal(t, 0, result.Metadata().DefaultSortOrder())
	committed, err := t2.TableCommit()
	require.NoError(t, err)
	require.Len(t, committed.Updates, 2, "unsorted order must be registered before becoming default")
	require.Len(t, result.Metadata().SortOrders(), 2)
	require.Equal(t, 1, result.Metadata().SortOrders()[0].OrderID())
	require.Equal(t, 0, result.Metadata().SortOrders()[1].OrderID())
}

func TestReplaceSortOrderBuilderTransformsAndNameBinding(t *testing.T) {
	txn := testNonPartitionedTable.NewTransaction()
	builder := txn.NewReplaceSortOrder(false).
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

func TestReplaceSortOrderBuilderRejectsInvalidFieldsWithoutStaging(t *testing.T) {
	tests := []struct {
		name      string
		build     func(*table.Transaction) *table.ReplaceSortOrderBuilder
		errorText string
	}{
		{
			name: "case sensitive field missing",
			build: func(t *table.Transaction) *table.ReplaceSortOrderBuilder {
				return t.NewReplaceSortOrder(true).Asc("ID", table.NullsFirst)
			},
			errorText: "cannot bind sort field",
		},
		{
			name: "unknown field",
			build: func(t *table.Transaction) *table.ReplaceSortOrderBuilder {
				return t.NewReplaceSortOrder(false).Desc("missing", table.NullsLast)
			},
			errorText: "cannot bind sort field",
		},
		{
			name: "non-primitive field",
			build: func(t *table.Transaction) *table.ReplaceSortOrderBuilder {
				return t.NewReplaceSortOrder(true).Asc("address", table.NullsLast)
			},
			errorText: "cannot sort by non-primitive",
		},
		{
			name: "incompatible transform",
			build: func(t *table.Transaction) *table.ReplaceSortOrderBuilder {
				return t.NewReplaceSortOrder(true).AddField("name", iceberg.YearTransform{}, table.SortASC, table.NullsLast)
			},
			errorText: "invalid source type",
		},
		{
			name: "nil transform",
			build: func(t *table.Transaction) *table.ReplaceSortOrderBuilder {
				return t.NewReplaceSortOrder(true).AddField("id", nil, table.SortASC, table.NullsLast)
			},
			errorText: "has no transform",
		},
		{
			name: "invalid direction",
			build: func(t *table.Transaction) *table.ReplaceSortOrderBuilder {
				return t.NewReplaceSortOrder(true).AddField("id", iceberg.IdentityTransform{}, "sideways", table.NullsLast)
			},
			errorText: "invalid sort direction",
		},
		{
			name: "invalid null placement",
			build: func(t *table.Transaction) *table.ReplaceSortOrderBuilder {
				return t.NewReplaceSortOrder(true).AddField("id", iceberg.IdentityTransform{}, table.SortDESC, "random")
			},
			errorText: "invalid null order",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			txn := testNonPartitionedTable.NewTransaction()
			builder := tt.build(txn)
			_, err := builder.Apply()
			require.ErrorContains(t, err, tt.errorText)

			err = builder.Commit()
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

func TestReplaceSortOrderBuilderRejectsUnknownTransform(t *testing.T) {
	unknown, err := iceberg.ParseTransform("future-sort-transform")
	require.NoError(t, err)

	txn := testNonPartitionedTable.NewTransaction()
	builder := txn.NewReplaceSortOrder(true).AddField("id", unknown, table.SortASC, table.NullsFirst)
	_, err = builder.Apply()
	require.ErrorIs(t, err, table.ErrInvalidTransform)
	require.ErrorContains(t, err, "unknown transform")
	require.ErrorIs(t, builder.Commit(), table.ErrInvalidTransform)

	commit, err := txn.TableCommit()
	require.NoError(t, err)
	require.Empty(t, commit.Updates)
}

func TestReplaceSortOrderBuilderWithStagedSchema(t *testing.T) {
	txn := testNonPartitionedTable.NewTransaction()
	require.NoError(t, txn.UpdateSchema(true, false).
		AddColumn([]string{"priority"}, iceberg.PrimitiveTypes.Int64, "", false, nil).Commit())
	require.NoError(t, txn.NewReplaceSortOrder(true).Desc("priority", table.NullsLast).Commit())

	staged, err := txn.StagedTable()
	require.NoError(t, err)
	field, found := staged.Metadata().CurrentSchema().FindFieldByName("priority")
	require.True(t, found)
	require.Equal(t, field.ID, staged.Metadata().SortOrder().Field(0).FirstSourceID())

	commit, err := txn.TableCommit()
	require.NoError(t, err)
	require.NotEmpty(t, commit.Updates)
}

func TestReplaceSortOrderBuilderAcceptsNestedSortSource(t *testing.T) {
	txn := testNonPartitionedTable.NewTransaction()
	builder := txn.NewReplaceSortOrder(true).Asc("address.city", table.NullsFirst)

	// The builder accepts nested fields like Java; Go writers currently cannot
	// append data while the default order uses a nested source.
	order, err := builder.Apply()
	require.NoError(t, err)
	require.Equal(t, 6, order.Field(0).FirstSourceID())
	require.NoError(t, builder.Commit())

	staged, err := txn.StagedTable()
	require.NoError(t, err)
	require.Equal(t, 6, staged.SortOrder().Field(0).FirstSourceID())
}

// Case-insensitive lookups reject an ambiguous sort column without affecting
// unrelated columns in the same schema.
func TestReplaceSortOrderBuilderRejectsAmbiguousCaseInsensitiveNames(t *testing.T) {
	schema := iceberg.NewSchema(0,
		iceberg.NestedField{ID: 1, Name: "ID", Type: iceberg.PrimitiveTypes.Int64, Required: true},
		iceberg.NestedField{ID: 2, Name: "id", Type: iceberg.PrimitiveTypes.Int64, Required: true},
		iceberg.NestedField{ID: 3, Name: "unique", Type: iceberg.PrimitiveTypes.Int64},
	)
	meta, err := table.NewMetadata(schema, iceberg.UnpartitionedSpec, table.UnsortedSortOrder,
		"file:///tmp/ambiguous-sort-names", nil)
	require.NoError(t, err)
	tbl := table.New([]string{"ambiguous-sort-names"}, meta, "", nil, nil)

	for _, name := range []string{"id", "ID", "Id"} {
		t.Run("case insensitive "+name, func(t *testing.T) {
			txn := tbl.NewTransaction()
			builder := txn.NewReplaceSortOrder(false).Asc(name, table.NullsFirst)
			_, err := builder.Apply()
			require.ErrorIs(t, err, iceberg.ErrInvalidSchema)
			require.ErrorContains(t, err, "cannot bind sort field")
			require.ErrorContains(t, err, `ambiguous case-insensitive field names "ID" and "id"`)
			require.ErrorIs(t, builder.Commit(), iceberg.ErrInvalidSchema)

			commit, err := txn.TableCommit()
			require.NoError(t, err)
			require.Empty(t, commit.Updates)
		})
	}
	t.Run("case insensitive unrelated name", func(t *testing.T) {
		txn := tbl.NewTransaction()
		builder := txn.NewReplaceSortOrder(false).Asc("UnIqUe", table.NullsFirst)
		order, err := builder.Apply()
		require.NoError(t, err)
		require.Equal(t, 3, order.Field(0).FirstSourceID())
		require.NoError(t, builder.Commit())

		staged, err := txn.StagedTable()
		require.NoError(t, err)
		require.Equal(t, 3, staged.Metadata().SortOrder().Field(0).FirstSourceID())
	})
	for _, tc := range []struct {
		name string
		id   int
	}{
		{name: "ID", id: 1},
		{name: "id", id: 2},
	} {
		t.Run("case sensitive "+tc.name, func(t *testing.T) {
			txn := tbl.NewTransaction()
			require.NoError(t, txn.NewReplaceSortOrder(true).Asc(tc.name, table.NullsFirst).Commit())
			staged, err := txn.StagedTable()
			require.NoError(t, err)
			require.Equal(t, tc.id, staged.Metadata().SortOrder().Field(0).FirstSourceID())
		})
	}
}

func TestReplaceSortOrderBuilderNilTransaction(t *testing.T) {
	builder := table.NewReplaceSortOrderBuilder(nil, true).Asc("id", table.NullsFirst)
	_, err := builder.Apply()
	require.ErrorIs(t, err, table.ErrInvalidMetadata)
	require.ErrorIs(t, builder.Commit(), table.ErrInvalidMetadata)

	var nilBuilder *table.ReplaceSortOrderBuilder
	_, err = nilBuilder.Apply()
	require.ErrorIs(t, err, table.ErrInvalidMetadata)
}
