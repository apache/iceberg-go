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
	"path/filepath"
	"testing"

	"github.com/apache/iceberg-go"
	"github.com/apache/iceberg-go/table"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func newDynamicOverwriteTable(t *testing.T) (*table.Table, *concurrentTestCatalog) {
	t.Helper()
	location := filepath.ToSlash(t.TempDir())
	schema := iceberg.NewSchema(0, iceberg.NestedField{ID: 1, Name: "id", Type: iceberg.PrimitiveTypes.Int64, Required: true})
	spec := iceberg.NewPartitionSpec(iceberg.PartitionField{SourceIDs: []int{1}, FieldID: 1000, Name: "id", Transform: iceberg.IdentityTransform{}})
	meta, err := table.NewMetadata(schema, &spec, table.UnsortedSortOrder, location, iceberg.Properties{
		table.PropertyFormatVersion:        "2",
		table.CommitNumRetriesKey:          "2",
		table.CommitMinRetryWaitMsKey:      "1",
		table.CommitMaxRetryWaitMsKey:      "2",
		table.CommitTotalRetryTimeoutMsKey: "1000",
	})
	require.NoError(t, err)
	metaLoc := location + "/metadata/v1.metadata.json"
	cat := &concurrentTestCatalog{metadata: meta, location: metaLoc, fsF: branchPlanningFS}

	return table.New(branchPlanningIdent, meta, metaLoc, branchPlanningFS, cat), cat
}

func TestDynamicPartitionOverwriteArrowAndConcurrentCommit(t *testing.T) {
	base, cat := newDynamicOverwriteTable(t)
	tbl := appendRowsOnRef(t, base, table.MainBranch, 1, 2)
	txn := tbl.NewTransaction()
	reader := idRecordReader(t, tbl, 1, 3)
	require.NoError(t, txn.DynamicPartitionOverwrite(t.Context(), reader, nil))
	reader.Release()
	updated, err := txn.Commit(t.Context())
	require.NoError(t, err)
	assert.ElementsMatch(t, []int64{1, 2, 3}, idsOnRef(t, updated, table.MainBranch))

	stale := updated.NewTransaction()
	reader = idRecordReader(t, updated, 1)
	require.NoError(t, stale.DynamicPartitionOverwrite(t.Context(), reader, nil))
	reader.Release()
	updated = appendRowsOnRef(t, updated, table.MainBranch, 1)
	attemptsBefore := cat.attempts.Load()
	_, err = stale.Commit(t.Context())
	require.ErrorIs(t, err, table.ErrCommitFailed)
	assert.Equal(t, attemptsBefore+1, cat.attempts.Load(),
		"the pinned target must fail without retrying against the new main head")
	assert.ElementsMatch(t, []int64{1, 1, 2, 3}, idsOnRef(t, updated, table.MainBranch))
}
