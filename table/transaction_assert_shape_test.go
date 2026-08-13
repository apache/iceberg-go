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

package table

import (
	"testing"

	"github.com/apache/iceberg-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// countShapeAssertions returns how many default-spec and default-sort-
// order assertions the transaction has accumulated.
func countShapeAssertions(tx *Transaction) (specs, orders int) {
	for _, r := range tx.reqs {
		switch r.(type) {
		case *assertDefaultSpecId:
			specs++
		case *assertDefaultSortOrderId:
			orders++
		}
	}

	return specs, orders
}

// withDefaultSpecChanged returns base with a new identity-partitioned
// spec installed as the default.
func withDefaultSpecChanged(t *testing.T, base Metadata) Metadata {
	t.Helper()

	builder, err := MetadataBuilderFromBase(base, "")
	require.NoError(t, err)
	spec := iceberg.NewPartitionSpecID(base.DefaultPartitionSpec()+1,
		iceberg.PartitionField{
			SourceIDs: []int{1},
			FieldID:   iceberg.PartitionDataIDStart,
			Name:      "id",
			Transform: iceberg.IdentityTransform{},
		})
	require.NoError(t, builder.AddPartitionSpec(&spec, false))
	require.NoError(t, builder.SetDefaultSpecID(-1))
	meta, err := builder.Build()
	require.NoError(t, err)
	require.NotEqual(t, base.DefaultPartitionSpec(), meta.DefaultPartitionSpec())

	return meta
}

// withDefaultSortOrderChanged returns base with a new identity sort
// order installed as the default.
func withDefaultSortOrderChanged(t *testing.T, base Metadata) Metadata {
	t.Helper()

	builder, err := MetadataBuilderFromBase(base, "")
	require.NoError(t, err)
	order, err := NewSortOrder(1, []SortField{
		{SourceIDs: []int{1}, Direction: SortASC, NullOrder: NullsFirst, Transform: iceberg.IdentityTransform{}},
	})
	require.NoError(t, err)
	require.NoError(t, builder.AddSortOrder(&order))
	require.NoError(t, builder.SetDefaultSortOrderID(-1))
	meta, err := builder.Build()
	require.NoError(t, err)
	require.NotEqual(t, base.DefaultSortOrder(), meta.DefaultSortOrder())

	return meta
}

// Why: a transaction that stages no spec or sort-order change of its
// own (e.g. a properties-only commit) commits without any
// shape assertion, so a concurrent spec or sort-order evolution can
// interleave silently. AssertDefaultShape is the explicit fence pinning
// both defaults at their base values, matching the assertions Java
// UpdateRequirements.forUpdateTable registers for shape-changing
// commits.
// Condition: a properties-only transaction with AssertDefaultShape,
// committed against a catalog whose default spec / default sort order
// did or did not move in between.
// Assertion: the commit succeeds when the shape is unmoved and fails
// with ErrCommitFailed — with the properties left uncommitted — when
// either default moved.
func TestTransactionAssertDefaultShape(t *testing.T) {
	head := int64(100)
	retryProps := iceberg.Properties{
		CommitNumRetriesKey:     "2",
		CommitMinRetryWaitMsKey: "1",
		CommitMaxRetryWaitMsKey: "2",
	}

	t.Run("succeeds when the shape is unmoved", func(t *testing.T) {
		base := newConflictTestMetadataWithProps(t, &head, retryProps)
		tbl, cat := newAssertRefTestTable(t, base, base)

		tx := tbl.NewTransaction()
		require.NoError(t, tx.AssertDefaultShape())
		require.NoError(t, tx.SetProperties(iceberg.Properties{"marker": "7"}))

		_, err := tx.Commit(t.Context())
		require.NoError(t, err)
		assert.Equal(t, "7", cat.metadata.Properties()["marker"])
	})

	t.Run("fails when the default spec moved", func(t *testing.T) {
		base := newConflictTestMetadataWithProps(t, &head, retryProps)
		tbl, cat := newAssertRefTestTable(t, base, withDefaultSpecChanged(t, base))

		tx := tbl.NewTransaction()
		require.NoError(t, tx.AssertDefaultShape())
		require.NoError(t, tx.SetProperties(iceberg.Properties{"marker": "7"}))

		_, err := tx.Commit(t.Context())
		require.Error(t, err)
		assert.ErrorIs(t, err, ErrCommitFailed)
		assert.NotContains(t, cat.metadata.Properties(), "marker",
			"a failed shape fence must not apply the properties")
	})

	t.Run("fails when the default sort order moved", func(t *testing.T) {
		base := newConflictTestMetadataWithProps(t, &head, retryProps)
		tbl, cat := newAssertRefTestTable(t, base, withDefaultSortOrderChanged(t, base))

		tx := tbl.NewTransaction()
		require.NoError(t, tx.AssertDefaultShape())
		require.NoError(t, tx.SetProperties(iceberg.Properties{"marker": "7"}))

		_, err := tx.Commit(t.Context())
		require.Error(t, err)
		assert.ErrorIs(t, err, ErrCommitFailed)
		assert.NotContains(t, cat.metadata.Properties(), "marker")
	})

	t.Run("dedupes by type against itself and producer assertions", func(t *testing.T) {
		base := newConflictTestMetadataWithProps(t, &head, retryProps)
		tbl, _ := newAssertRefTestTable(t, base, base)

		tx := tbl.NewTransaction()
		require.NoError(t, tx.AssertDefaultShape())
		require.NoError(t, tx.AssertDefaultShape())
		// ReplaceSortOrder's own default-sort-order assertion pins the
		// same base value and must collapse into the fence.
		order, err := NewSortOrder(1, []SortField{
			{SourceIDs: []int{1}, Direction: SortDESC, NullOrder: NullsLast, Transform: iceberg.IdentityTransform{}},
		})
		require.NoError(t, err)
		require.NoError(t, tx.ReplaceSortOrder(order))

		specs, orders := countShapeAssertions(tx)
		assert.Equal(t, 1, specs)
		assert.Equal(t, 1, orders)
	})
}
