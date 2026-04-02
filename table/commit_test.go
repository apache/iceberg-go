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
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func newCommitTestTable(t *testing.T) *table.Table {
	t.Helper()

	schema := iceberg.NewSchema(0,
		iceberg.NestedField{ID: 1, Name: "id", Type: iceberg.PrimitiveTypes.Int64, Required: true},
		iceberg.NestedField{ID: 2, Name: "data", Type: iceberg.PrimitiveTypes.String, Required: false},
	)

	meta, err := table.NewMetadata(schema, iceberg.UnpartitionedSpec,
		table.UnsortedSortOrder, "s3://bucket/test",
		iceberg.Properties{table.PropertyFormatVersion: "2"})
	require.NoError(t, err)

	return table.New(
		table.Identifier{"db", "test_table"},
		meta, "s3://bucket/test/metadata/v1.metadata.json",
		nil, nil,
	)
}

func TestTableCommitFromEmptyTransaction(t *testing.T) {
	tbl := newCommitTestTable(t)
	tx := tbl.NewTransaction()

	tc, err := tx.TableCommit()
	require.NoError(t, err)

	assert.Equal(t, table.Identifier{"db", "test_table"}, tc.Identifier)
	assert.NotNil(t, tc.Requirements, "Requirements must be non-nil empty slice for JSON serialization")
	assert.NotNil(t, tc.Updates, "Updates must be non-nil empty slice for JSON serialization")
	assert.Empty(t, tc.Requirements)
	assert.Empty(t, tc.Updates)
}

func TestTableCommitAfterCommitFails(t *testing.T) {
	tbl := newCommitTestTable(t)
	tx := tbl.NewTransaction()

	tx.MarkCommitted()

	_, err := tx.TableCommit()
	require.Error(t, err)
	assert.Contains(t, err.Error(), "already been committed")
}

func TestTableCommitIncludesAssertTableUUID(t *testing.T) {
	tbl := newCommitTestTable(t)
	tx := tbl.NewTransaction()

	require.NoError(t, tx.SetProperties(map[string]string{"key": "value"}))

	tc, err := tx.TableCommit()
	require.NoError(t, err)

	assert.NotEmpty(t, tc.Updates)

	var hasUUIDAssert bool
	for _, r := range tc.Requirements {
		if r.GetType() == "assert-table-uuid" {
			hasUUIDAssert = true

			break
		}
	}
	assert.True(t, hasUUIDAssert, "expected AssertTableUUID requirement")
}

func TestTableCommitDoesNotMarkTransactionCommitted(t *testing.T) {
	tbl := newCommitTestTable(t)
	tx := tbl.NewTransaction()

	require.NoError(t, tx.SetProperties(map[string]string{"key": "value"}))

	_, err := tx.TableCommit()
	require.NoError(t, err)

	// Second call should also succeed — not marked as committed
	_, err = tx.TableCommit()
	require.NoError(t, err)
}

func TestTableCommitReturnsCopies(t *testing.T) {
	tbl := newCommitTestTable(t)
	tx := tbl.NewTransaction()

	require.NoError(t, tx.SetProperties(map[string]string{"key": "value"}))

	tc1, err := tx.TableCommit()
	require.NoError(t, err)

	tc2, err := tx.TableCommit()
	require.NoError(t, err)

	// Mutating one should not affect the other
	if len(tc1.Requirements) > 0 {
		tc1.Requirements = tc1.Requirements[:0]
	}
	assert.NotEmpty(t, tc2.Requirements)
}

func newV1CommitTestTable(t *testing.T) *table.Table {
	t.Helper()

	schema := iceberg.NewSchema(0,
		iceberg.NestedField{ID: 1, Name: "id", Type: iceberg.PrimitiveTypes.Int64, Required: true},
	)

	meta, err := table.NewMetadata(schema, iceberg.UnpartitionedSpec,
		table.UnsortedSortOrder, "s3://bucket/test",
		iceberg.Properties{table.PropertyFormatVersion: "1"})
	require.NoError(t, err)

	return table.New(
		table.Identifier{"db", "test_table"},
		meta, "s3://bucket/test/metadata/v1.metadata.json",
		nil, nil,
	)
}

func TestUpgradeFormatVersionV1ToV2(t *testing.T) {
	tbl := newV1CommitTestTable(t)
	assert.Equal(t, 1, tbl.Metadata().Version())

	tx := tbl.NewTransaction()
	require.NoError(t, tx.UpgradeFormatVersion(2))

	tc, err := tx.TableCommit()
	require.NoError(t, err)

	var found bool
	for _, u := range tc.Updates {
		if u.Action() == "upgrade-format-version" {
			found = true

			break
		}
	}
	assert.True(t, found, "expected upgrade-format-version update")
}

func TestUpgradeFormatVersionNoOpWhenSameVersion(t *testing.T) {
	tbl := newCommitTestTable(t) // v2 table
	tx := tbl.NewTransaction()

	require.NoError(t, tx.UpgradeFormatVersion(2))

	tc, err := tx.TableCommit()
	require.NoError(t, err)

	// No updates should be staged — it's a no-op
	assert.Empty(t, tc.Updates)
}

func TestUpgradeFormatVersionDowngradeErrors(t *testing.T) {
	tbl := newCommitTestTable(t) // v2 table
	tx := tbl.NewTransaction()

	err := tx.UpgradeFormatVersion(1)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "downgrading")
}

func TestMarkCommittedPreventsCommit(t *testing.T) {
	tbl := newCommitTestTable(t)
	tx := tbl.NewTransaction()

	tx.MarkCommitted()

	_, err := tx.Commit(t.Context())
	require.Error(t, err)
	assert.Contains(t, err.Error(), "already been committed")
}
