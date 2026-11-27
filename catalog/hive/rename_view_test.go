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

package hive

import (
	"errors"
	"maps"
	"testing"

	"github.com/apache/iceberg-go"
	"github.com/apache/iceberg-go/catalog"
	"github.com/apache/iceberg-go/table"
	"github.com/beltran/gohive/hive_metastore"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

// copyHiveTable returns a copy the code under test can rename in place without
// changing the fixture.
func copyHiveTable(tbl *hive_metastore.Table) *hive_metastore.Table {
	cp := *tbl
	cp.Parameters = maps.Clone(tbl.Parameters)

	return &cp
}

// expectRenameViewSource sets up the namespace check, the source reads before
// and under the lock, and the lock on both names.
func expectRenameViewSource(client *mockHiveClient, source *hive_metastore.Table, toDB string) {
	client.On("GetDatabase", mock.Anything, toDB).
		Return(&hive_metastore.Database{Name: toDB}, nil).Once()
	client.On("GetTable", mock.Anything, "db", "view").Return(copyHiveTable(source), nil).Once()
	client.On("GetTable", mock.Anything, "db", "view").Return(copyHiveTable(source), nil).Once()
	expectImmediateTableLock(client, 1)
}

func TestHiveRenameView(t *testing.T) {
	cat, client, hiveView, created := newHiveViewUpdateTest(t)
	to := table.Identifier{"other", "renamed"}
	expectRenameViewSource(client, hiveView, "other")
	client.On("GetTable", mock.Anything, "other", "renamed").Return(nil, errNoSuchObject).Once()
	renamedHiveView := copyHiveTable(hiveView)
	renamedHiveView.DbName, renamedHiveView.TableName = "other", "renamed"
	client.On("AlterTable", mock.Anything, "db", "view", mock.MatchedBy(func(tbl *hive_metastore.Table) bool {
		return tbl.DbName == "other" && tbl.TableName == "renamed" &&
			tbl.Parameters[MetadataLocationKey] == created.MetadataLocation() &&
			tbl.Parameters["hms-owned"] == "keep" && tbl.Owner == "owner"
	})).Return(nil).Once()
	client.On("GetTable", mock.Anything, "other", "renamed").Return(renamedHiveView, nil).Once()

	renamer, ok := catalog.As[catalog.RenamableViewCatalog](cat)
	require.True(t, ok)
	renamed, err := renamer.RenameView(t.Context(), created.Identifier(), to)
	require.NoError(t, err)
	require.Equal(t, to, renamed.Identifier())
	require.Equal(t, created.MetadataLocation(), renamed.MetadataLocation())
	require.True(t, created.Metadata().Equals(renamed.Metadata()))
	client.AssertExpectations(t)
}

func TestHiveRenameViewDestinationConflicts(t *testing.T) {
	_, _, hiveView, _ := newHiveViewUpdateTest(t)
	otherView := copyHiveTable(hiveView)
	otherView.TableName = "taken"

	tests := []struct {
		name        string
		destination *hive_metastore.Table
		expected    error
	}{
		{"iceberg view", otherView, catalog.ErrViewAlreadyExists},
		{"iceberg table", testIcebergHiveTable1, catalog.ErrTableAlreadyExists},
		{"non-iceberg table", testNonIcebergHiveTable, catalog.ErrTableAlreadyExists},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cat, client, hiveView, created := newHiveViewUpdateTest(t)
			expectRenameViewSource(client, hiveView, "db")
			client.On("GetTable", mock.Anything, "db", "taken").Return(tt.destination, nil).Once()

			_, err := cat.RenameView(t.Context(), created.Identifier(), table.Identifier{"db", "taken"})
			require.ErrorIs(t, err, tt.expected)
			client.AssertNotCalled(t, "AlterTable", mock.Anything, mock.Anything, mock.Anything, mock.Anything)
			client.AssertExpectations(t)
		})
	}
}

// TestHiveRenameViewToItself follows Java's HiveCatalog, which rejects a
// rename onto an existing view even when it is the source.
func TestHiveRenameViewToItself(t *testing.T) {
	cat, client, hiveView, created := newHiveViewUpdateTest(t)
	expectRenameViewSource(client, hiveView, "db")
	client.On("GetTable", mock.Anything, "db", "view").Return(copyHiveTable(hiveView), nil).Once()

	_, err := cat.RenameView(t.Context(), created.Identifier(), created.Identifier())
	require.ErrorIs(t, err, catalog.ErrViewAlreadyExists)
	client.AssertNotCalled(t, "AlterTable", mock.Anything, mock.Anything, mock.Anything, mock.Anything)
	client.AssertExpectations(t)
}

// TestHiveRenameViewDestinationCreatedConcurrently covers a destination that
// appears after the pre-check, from a writer that skips the HMS locks.
func TestHiveRenameViewDestinationCreatedConcurrently(t *testing.T) {
	cat, client, hiveView, created := newHiveViewUpdateTest(t)
	expectRenameViewSource(client, hiveView, "db")
	client.On("GetTable", mock.Anything, "db", "renamed").Return(nil, errNoSuchObject).Once()
	client.On("AlterTable", mock.Anything, "db", "view", mock.Anything).
		Return(errors.New("InvalidOperationException: new table db.renamed already exists")).Once()

	_, err := cat.RenameView(t.Context(), created.Identifier(), table.Identifier{"db", "renamed"})
	require.ErrorIs(t, err, catalog.ErrViewAlreadyExists)
	client.AssertExpectations(t)
}

func TestHiveRenameViewRejectsChangedMetadataAfterLock(t *testing.T) {
	cat, client, hiveView, created := newHiveViewUpdateTest(t)
	changed := copyHiveTable(hiveView)
	changed.Parameters[MetadataLocationKey] = "file:///elsewhere/v2.metadata.json"
	client.On("GetDatabase", mock.Anything, "db").Return(&hive_metastore.Database{Name: "db"}, nil).Once()
	client.On("GetTable", mock.Anything, "db", "view").Return(copyHiveTable(hiveView), nil).Once()
	expectImmediateTableLock(client, 1)
	client.On("GetTable", mock.Anything, "db", "view").Return(changed, nil).Once()

	_, err := cat.RenameView(t.Context(), created.Identifier(), table.Identifier{"db", "renamed"})
	require.ErrorIs(t, err, table.ErrCommitFailed)
	client.AssertNotCalled(t, "AlterTable", mock.Anything, mock.Anything, mock.Anything, mock.Anything)
	client.AssertExpectations(t)
}

func TestHiveRenameViewSourceDroppedAfterLock(t *testing.T) {
	cat, client, hiveView, created := newHiveViewUpdateTest(t)
	client.On("GetDatabase", mock.Anything, "db").Return(&hive_metastore.Database{Name: "db"}, nil).Once()
	client.On("GetTable", mock.Anything, "db", "view").Return(copyHiveTable(hiveView), nil).Once()
	expectImmediateTableLock(client, 1)
	client.On("GetTable", mock.Anything, "db", "view").Return(nil, errNoSuchObject).Once()

	_, err := cat.RenameView(t.Context(), created.Identifier(), table.Identifier{"db", "renamed"})
	require.ErrorIs(t, err, table.ErrCommitFailed)
	require.ErrorIs(t, err, catalog.ErrNoSuchView)
	client.AssertExpectations(t)
}

func TestHiveRenameViewSourceIsNotAView(t *testing.T) {
	sources := []struct {
		name   string
		object *hive_metastore.Table
		err    error
	}{
		{"missing", nil, errNoSuchObject},
		{"iceberg table", testIcebergHiveTable1, nil},
		{"non-iceberg virtual view", &hive_metastore.Table{TableType: TableTypeVirtualView}, nil},
	}
	for _, src := range sources {
		t.Run(src.name, func(t *testing.T) {
			client := &mockHiveClient{}
			client.On("GetDatabase", mock.Anything, "db").Return(&hive_metastore.Database{Name: "db"}, nil).Once()
			client.On("GetTable", mock.Anything, "db", "view").Return(src.object, src.err).Once()
			cat := NewCatalogWithClient(client, iceberg.Properties{})

			_, err := cat.RenameView(t.Context(), table.Identifier{"db", "view"}, table.Identifier{"db", "renamed"})
			require.ErrorIs(t, err, catalog.ErrNoSuchView)
			client.AssertNotCalled(t, "Lock", mock.Anything, mock.Anything)
			client.AssertExpectations(t)
		})
	}
}

func TestHiveRenameViewMissingNamespace(t *testing.T) {
	client := &mockHiveClient{}
	client.On("GetDatabase", mock.Anything, "missing").Return(nil, errNoSuchObject).Once()
	cat := NewCatalogWithClient(client, iceberg.Properties{})

	_, err := cat.RenameView(t.Context(), table.Identifier{"db", "view"}, table.Identifier{"missing", "renamed"})
	require.ErrorIs(t, err, catalog.ErrNoSuchNamespace)
	client.AssertNotCalled(t, "GetTable", mock.Anything, mock.Anything, mock.Anything)
	client.AssertExpectations(t)
}

func TestHiveRenameViewLockFailure(t *testing.T) {
	cat, client, hiveView, created := newHiveViewUpdateTest(t)
	client.On("GetDatabase", mock.Anything, "db").Return(&hive_metastore.Database{Name: "db"}, nil).Once()
	client.On("GetTable", mock.Anything, "db", "view").Return(copyHiveTable(hiveView), nil).Once()
	client.On("Lock", mock.Anything, mock.Anything).Return(nil, errors.New("lock failed")).Once()

	_, err := cat.RenameView(t.Context(), created.Identifier(), table.Identifier{"db", "renamed"})
	require.ErrorIs(t, err, table.ErrCommitFailed)
	client.AssertNotCalled(t, "AlterTable", mock.Anything, mock.Anything, mock.Anything, mock.Anything)
	client.AssertExpectations(t)
}

func TestHiveRenameViewInvalidIdentifiers(t *testing.T) {
	cat := NewCatalogWithClient(&mockHiveClient{}, iceberg.Properties{})
	valid := table.Identifier{"db", "view"}
	for _, ident := range []table.Identifier{nil, {}, {"view"}, {"db", ""}, {"a", "b", "c"}} {
		_, err := cat.RenameView(t.Context(), ident, valid)
		require.Error(t, err)
		_, err = cat.RenameView(t.Context(), valid, ident)
		require.Error(t, err)
	}
}
