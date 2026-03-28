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
	"errors"
	"iter"
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

// fullStubCatalog implements both Catalog and TransactionalCatalog
// for testing CommitAndReload.
type fullStubCatalog struct {
	commits   [][]table.TableCommit
	tables    map[string]*table.Table
	commitErr error
}

func (s *fullStubCatalog) CommitTransaction(_ context.Context, commits []table.TableCommit) error {
	if s.commitErr != nil {
		return s.commitErr
	}

	s.commits = append(s.commits, commits)

	return nil
}

func (s *fullStubCatalog) LoadTable(_ context.Context, ident table.Identifier) (*table.Table, error) {
	key := ident[len(ident)-1]
	if tbl, ok := s.tables[key]; ok {
		return tbl, nil
	}

	return nil, ErrNoSuchTable
}

func (s *fullStubCatalog) ListTables(context.Context, table.Identifier) iter.Seq2[table.Identifier, error] {
	return nil
}

func (s *fullStubCatalog) CreateTable(context.Context, table.Identifier, *iceberg.Schema, ...CreateTableOpt) (*table.Table, error) {
	return nil, nil
}

func (s *fullStubCatalog) CommitTable(context.Context, table.Identifier, []table.Requirement, []table.Update) (table.Metadata, string, error) {
	return nil, "", nil
}

func (s *fullStubCatalog) DropTable(context.Context, table.Identifier) error { return nil }

func (s *fullStubCatalog) RenameTable(context.Context, table.Identifier, table.Identifier) (*table.Table, error) {
	return nil, nil
}

func (s *fullStubCatalog) CheckTableExists(context.Context, table.Identifier) (bool, error) {
	return false, nil
}

func (s *fullStubCatalog) ListNamespaces(context.Context, table.Identifier) ([]table.Identifier, error) {
	return nil, nil
}

func (s *fullStubCatalog) CreateNamespace(context.Context, table.Identifier, iceberg.Properties) error {
	return nil
}

func (s *fullStubCatalog) DropNamespace(context.Context, table.Identifier) error { return nil }

func (s *fullStubCatalog) CheckNamespaceExists(context.Context, table.Identifier) (bool, error) {
	return false, nil
}

func (s *fullStubCatalog) LoadNamespaceProperties(context.Context, table.Identifier) (iceberg.Properties, error) {
	return nil, nil
}

func (s *fullStubCatalog) UpdateNamespaceProperties(context.Context, table.Identifier, []string, iceberg.Properties) (PropertiesUpdateSummary, error) {
	return PropertiesUpdateSummary{}, nil
}

func (s *fullStubCatalog) CatalogType() Type { return REST }

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

func TestCommitErrorIncludesTableIdentifiers(t *testing.T) {
	stub := &fullStubCatalog{
		commitErr: errors.New("conflict"),
		tables:    map[string]*table.Table{},
	}

	mtx, err := NewMultiTableTransaction(stub)
	require.NoError(t, err)

	tx1 := mtxTestTable(t, "db", "t1").NewTransaction()
	require.NoError(t, tx1.SetProperties(map[string]string{"k": "v"}))
	require.NoError(t, mtx.AddTransaction(tx1))

	tx2 := mtxTestTable(t, "db", "t2").NewTransaction()
	require.NoError(t, tx2.SetProperties(map[string]string{"k": "v"}))
	require.NoError(t, mtx.AddTransaction(tx2))

	err = mtx.Commit(context.Background())
	require.Error(t, err)
	assert.Contains(t, err.Error(), "db.t1")
	assert.Contains(t, err.Error(), "db.t2")
	assert.ErrorContains(t, err, "conflict")
}

func TestCommitAndReload(t *testing.T) {
	tbl1 := mtxTestTable(t, "db", "t1")
	tbl2 := mtxTestTable(t, "db", "t2")

	stub := &fullStubCatalog{
		tables: map[string]*table.Table{
			"t1": tbl1,
			"t2": tbl2,
		},
	}

	mtx, err := NewMultiTableTransaction(stub)
	require.NoError(t, err)

	tx1 := tbl1.NewTransaction()
	require.NoError(t, tx1.SetProperties(map[string]string{"k": "v"}))
	require.NoError(t, mtx.AddTransaction(tx1))

	tx2 := tbl2.NewTransaction()
	require.NoError(t, tx2.SetProperties(map[string]string{"k": "v"}))
	require.NoError(t, mtx.AddTransaction(tx2))

	tables, err := mtx.CommitAndReload(context.Background())
	require.NoError(t, err)
	require.Len(t, tables, 2)
	assert.Equal(t, table.Identifier{"db", "t1"}, tables[0].Identifier())
	assert.Equal(t, table.Identifier{"db", "t2"}, tables[1].Identifier())
}

func TestCommitAndReloadPartialFailure(t *testing.T) {
	tbl1 := mtxTestTable(t, "db", "t1")

	stub := &fullStubCatalog{
		tables: map[string]*table.Table{
			"t1": tbl1,
			// t2 missing — LoadTable will fail
		},
	}

	mtx, err := NewMultiTableTransaction(stub)
	require.NoError(t, err)

	tx1 := tbl1.NewTransaction()
	require.NoError(t, tx1.SetProperties(map[string]string{"k": "v"}))
	require.NoError(t, mtx.AddTransaction(tx1))

	tx2 := mtxTestTable(t, "db", "t2").NewTransaction()
	require.NoError(t, tx2.SetProperties(map[string]string{"k": "v"}))
	require.NoError(t, mtx.AddTransaction(tx2))

	tables, err := mtx.CommitAndReload(context.Background())
	require.Error(t, err)
	assert.ErrorIs(t, err, ErrNoSuchTable)
	assert.Contains(t, err.Error(), "reload table")
	// First table was loaded successfully before the second failed.
	assert.Len(t, tables, 1)
}
