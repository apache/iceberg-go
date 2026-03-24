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

package catalog

import (
	"context"
	"testing"

	"github.com/apache/iceberg-go"
	"github.com/apache/iceberg-go/table"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type stubCatalog struct {
	commits [][]table.TableCommit
}

func (s *stubCatalog) CommitTransaction(_ context.Context, commits []table.TableCommit) error {
	s.commits = append(s.commits, commits)

	return nil
}

func mtxTestTable(t *testing.T, ns, name string) *table.Table {
	t.Helper()

	schema := iceberg.NewSchema(0,
		iceberg.NestedField{ID: 1, Name: "id", Type: iceberg.PrimitiveTypes.Int64, Required: true},
	)

	meta, err := table.NewMetadata(schema, iceberg.UnpartitionedSpec,
		table.UnsortedSortOrder, "s3://bucket/test",
		iceberg.Properties{table.PropertyFormatVersion: "2"})
	require.NoError(t, err)

	return table.New(table.Identifier{ns, name}, meta, "", nil, nil)
}

func TestMultiTableTransaction(t *testing.T) {
	stub := &stubCatalog{}
	mtx := &MultiTableTransaction{cat: stub}

	// Add two tables with changes.
	tx1 := mtxTestTable(t, "db", "t1").NewTransaction()
	require.NoError(t, tx1.SetProperties(map[string]string{"k": "v"}))
	require.NoError(t, mtx.AddTransaction(tx1))

	tx2 := mtxTestTable(t, "db", "t2").NewTransaction()
	require.NoError(t, tx2.SetProperties(map[string]string{"k": "v"}))
	require.NoError(t, mtx.AddTransaction(tx2))

	// Duplicate table is rejected.
	assert.ErrorContains(t, mtx.AddTransaction(mtxTestTable(t, "db", "t1").NewTransaction()), "duplicate table")

	// Already-committed transaction is rejected.
	tx3 := mtxTestTable(t, "db", "t3").NewTransaction()
	tx3.MarkCommitted()
	assert.ErrorContains(t, mtx.AddTransaction(tx3), "already been committed")

	// Commit sends both to the catalog and marks transactions done.
	require.NoError(t, mtx.Commit(context.Background()))
	require.Len(t, stub.commits, 1)
	assert.Len(t, stub.commits[0], 2)

	_, err := tx1.TableCommit()
	assert.ErrorContains(t, err, "already been committed")

	// Double commit and add-after-commit are rejected.
	assert.ErrorContains(t, mtx.Commit(context.Background()), "already been committed")
	assert.ErrorContains(t, mtx.AddTransaction(mtxTestTable(t, "db", "t4").NewTransaction()), "already been committed")
}

func TestMultiTableTransactionEmpty(t *testing.T) {
	mtx := &MultiTableTransaction{cat: &stubCatalog{}}
	assert.ErrorIs(t, mtx.Commit(context.Background()), ErrEmptyCommitList)
}

func TestNewMultiTableTransactionNonTransactional(t *testing.T) {
	// nil doesn't implement TransactionalCatalog
	_, err := NewMultiTableTransaction(nil)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "does not support")
}
