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

func TestReplaceSortOrderBuilderRecomputesPreviewAfterOtherReplacement(t *testing.T) {
	txn := testNonPartitionedTable.NewTransaction()
	byID := txn.NewReplaceSortOrder(true).Asc("id", table.NullsFirst)
	byName := txn.NewReplaceSortOrder(true).Desc("name", table.NullsLast)

	preview, err := byID.Apply()
	require.NoError(t, err)
	require.Equal(t, 1, preview.OrderID())

	// Apply does not reserve an ID. Another builder can stage the first
	// order before the original builder commits.
	require.NoError(t, byName.Commit())
	updatedPreview, err := byID.Apply()
	require.NoError(t, err)
	require.Equal(t, 2, updatedPreview.OrderID())
	require.NoError(t, byID.Commit())

	staged, err := txn.StagedTable()
	require.NoError(t, err)
	require.Equal(t, 2, staged.Metadata().DefaultSortOrder())
	require.Equal(t, 1, staged.SortOrder().Field(0).FirstSourceID())
	require.Len(t, staged.Metadata().SortOrders(), 3)

	commit, err := txn.TableCommit()
	require.NoError(t, err)
	require.Len(t, commit.Updates, 4)
	require.Equal(t, "assert-default-sort-order-id", commit.Requirements[0].GetType())
}

func TestReplaceSortOrderBuilderRebindsAfterStagedRename(t *testing.T) {
	txn := testNonPartitionedTable.NewTransaction()
	original := txn.NewReplaceSortOrder(true).Asc("name", table.NullsFirst)

	preview, err := original.Apply()
	require.NoError(t, err)
	require.Equal(t, 2, preview.Field(0).FirstSourceID())

	require.NoError(t, txn.UpdateSchema(true, false).
		RenameColumn([]string{"name"}, "label").Commit())
	before, err := txn.TableCommit()
	require.NoError(t, err)
	beforeUpdates, err := json.Marshal(before.Updates)
	require.NoError(t, err)

	_, err = original.Apply()
	require.ErrorIs(t, err, iceberg.ErrInvalidSchema)
	require.ErrorIs(t, original.Commit(), iceberg.ErrInvalidSchema)

	after, err := txn.TableCommit()
	require.NoError(t, err)
	afterUpdates, err := json.Marshal(after.Updates)
	require.NoError(t, err)
	require.JSONEq(t, string(beforeUpdates), string(afterUpdates))

	renamed := txn.NewReplaceSortOrder(true).Asc("label", table.NullsFirst)
	preview, err = renamed.Apply()
	require.NoError(t, err)
	require.Equal(t, 2, preview.Field(0).FirstSourceID())
	require.NoError(t, renamed.Commit())

	staged, err := txn.StagedTable()
	require.NoError(t, err)
	require.Equal(t, 2, staged.SortOrder().Field(0).FirstSourceID())
}

func TestReplaceSortOrderBuilderReusesOrderAfterStagedRename(t *testing.T) {
	txn := testNonPartitionedTable.NewTransaction()
	require.NoError(t, txn.NewReplaceSortOrder(true).
		Asc("name", table.NullsLast).Commit())
	require.NoError(t, txn.UpdateSchema(true, false).
		RenameColumn([]string{"name"}, "label").Commit())

	before, err := txn.TableCommit()
	require.NoError(t, err)
	oldUpdateCount := len(before.Updates)

	replacement := txn.NewReplaceSortOrder(true).Asc("label", table.NullsLast)
	preview, err := replacement.Apply()
	require.NoError(t, err)
	require.Equal(t, 1, preview.OrderID())
	require.NoError(t, replacement.Commit())

	after, err := txn.TableCommit()
	require.NoError(t, err)
	require.Len(t, after.Updates, oldUpdateCount)

	staged, err := txn.StagedTable()
	require.NoError(t, err)
	require.Equal(t, 1, staged.SortOrder().OrderID())
	require.Len(t, staged.Metadata().SortOrders(), 2)
}
