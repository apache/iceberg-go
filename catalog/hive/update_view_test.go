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
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/apache/iceberg-go"
	"github.com/apache/iceberg-go/catalog"
	icebergio "github.com/apache/iceberg-go/io"
	"github.com/apache/iceberg-go/table"
	"github.com/apache/iceberg-go/view"
	"github.com/beltran/gohive/hive_metastore"
	"github.com/google/uuid"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

func newHiveViewUpdateTest(t *testing.T) (*Catalog, *mockHiveClient, *hive_metastore.Table, *view.View) {
	t.Helper()
	ident := table.Identifier{"db", "view"}
	version := &view.Version{Representations: []view.Representation{
		view.NewRepresentation(view.Representation{SQL: "SELECT 1", Dialect: "spark"}),
		view.NewRepresentation(view.Representation{SQL: "SELECT 1", Dialect: "hive"}),
	}}
	schema := iceberg.NewSchema(0, iceberg.NestedField{ID: 1, Name: "id", Type: iceberg.PrimitiveTypes.Int32})
	props := iceberg.Properties{"comment": "old", "removed": "remove"}
	location := "file://" + filepath.ToSlash(t.TempDir())
	created, err := view.CreateViewFromVersion(t.Context(), ident, schema, version, location, nil, props)
	require.NoError(t, err)
	hiveView := constructHiveViewTable("db", "view", location, created.MetadataLocation(), schema, "SELECT 1", props)
	hiveView.Parameters["hms-owned"] = "keep"
	hiveView.Owner = "owner"
	client := &mockHiveClient{}

	return NewCatalogWithClient(&viewUpdateTestClient{mockHiveClient: client, renewal: &viewRenewalTestClient{}}, iceberg.Properties{}), client, hiveView, created
}

func TestHiveUpdateViewMetadataAndRollback(t *testing.T) {
	cat, client, old, created := newHiveViewUpdateTest(t)
	expectImmediateTableLock(client, 1)
	get := client.On("GetTable", mock.Anything, "db", "view").Return(old, nil).Once()
	get.NotBefore(client.ExpectedCalls[0])
	var altered *hive_metastore.Table
	client.On("AlterTable", mock.Anything, "db", "view", mock.AnythingOfType("*hive_metastore.Table")).
		Run(func(args mock.Arguments) { altered = args.Get(3).(*hive_metastore.Table) }).Return(nil).Once()
	schema := iceberg.NewSchema(1, iceberg.NestedField{ID: 1, Name: "renamed", Type: iceberg.PrimitiveTypes.Int32})
	next := created.CurrentVersion().Clone()
	next.SchemaID = view.LastAddedID
	next.Representations[0].SQL = "SELECT 2 AS spark"
	next.Representations[1].SQL = "SELECT 2 AS hive"
	updated, err := cat.UpdateView(t.Context(), created.Identifier(), []view.Requirement{view.AssertViewUUID(created.Metadata().ViewUUID())}, []view.Update{
		view.NewAddSchemaUpdate(schema), view.NewAddViewVersionUpdate(next), view.NewSetCurrentVersionUpdate(view.LastAddedID),
		view.NewSetPropertiesUpdate(iceberg.Properties{"comment": "new", "added": "value"}),
		view.NewRemovePropertiesUpdate([]string{"removed"}),
	})
	require.NoError(t, err)
	require.EqualValues(t, 2, updated.CurrentVersion().VersionID)
	require.Len(t, updated.Versions(), 2)
	require.Len(t, updated.Metadata().VersionLog(), 2)
	require.Equal(t, created.Metadata().ViewUUID(), updated.Metadata().ViewUUID())
	require.Equal(t, updated.MetadataLocation(), altered.Parameters[MetadataLocationKey])
	require.Equal(t, created.MetadataLocation(), altered.Parameters[PreviousMetadataLocationKey])
	require.Equal(t, "new", altered.Parameters["comment"])
	require.Equal(t, "keep", altered.Parameters["hms-owned"])
	require.Equal(t, "owner", altered.Owner)
	require.NotContains(t, altered.Parameters, "removed")
	require.Equal(t, "SELECT 2 AS hive", altered.ViewOriginalText)
	require.Equal(t, altered.ViewOriginalText, altered.ViewExpandedText)
	require.Equal(t, schemaToHiveColumns(updated.CurrentSchema()), altered.Sd.Cols)
	require.Equal(t, "old", old.Parameters["comment"])
	require.Equal(t, created.MetadataLocation(), old.Parameters[MetadataLocationKey])

	loaded, err := view.NewFromLocation(t.Context(), created.Identifier(), updated.MetadataLocation(),
		func(context.Context) (icebergio.IO, error) { return icebergio.LocalFS{}, nil })
	require.NoError(t, err)
	require.True(t, updated.Equals(*loaded))

	expectImmediateTableLock(client, 2)
	client.On("GetTable", mock.Anything, "db", "view").Return(altered, nil).Once()
	client.On("AlterTable", mock.Anything, "db", "view", mock.AnythingOfType("*hive_metastore.Table")).Return(nil).Once()
	rolledBack, err := cat.UpdateView(t.Context(), created.Identifier(), nil, []view.Update{view.NewSetCurrentVersionUpdate(1)})
	require.NoError(t, err)
	require.EqualValues(t, 1, rolledBack.CurrentVersion().VersionID)
	require.Len(t, rolledBack.Metadata().VersionLog(), 3)
	client.AssertExpectations(t)
}

func TestHiveUpdateViewValidationAndNoop(t *testing.T) {
	for _, test := range []string{"uuid", "version", "dialect", "no-op"} {
		t.Run(test, func(t *testing.T) {
			cat, client, old, created := newHiveViewUpdateTest(t)
			expectImmediateTableLock(client, 1)
			client.On("GetTable", mock.Anything, "db", "view").Return(old, nil).Once()
			var reqs []view.Requirement
			var updates []view.Update
			switch test {
			case "uuid":
				reqs = []view.Requirement{view.AssertViewUUID(uuid.New())}
			case "version":
				updates = []view.Update{view.NewSetCurrentVersionUpdate(999)}
			case "dialect":
				next := created.CurrentVersion().Clone()
				next.Representations = next.Representations[:1]
				next.Representations[0].SQL = "SELECT 2"
				updates = []view.Update{view.NewAddViewVersionUpdate(next), view.NewSetCurrentVersionUpdate(view.LastAddedID)}
			}
			got, err := cat.UpdateView(t.Context(), created.Identifier(), reqs, updates)
			if test == "no-op" {
				require.NoError(t, err)
				require.Equal(t, created.MetadataLocation(), got.MetadataLocation())
			} else {
				require.Error(t, err)
				if test == "uuid" {
					require.ErrorIs(t, err, table.ErrCommitFailed)
				}
			}
			files, err := os.ReadDir(filepath.Join(strings.TrimPrefix(created.Location(), "file://"), "metadata"))
			require.NoError(t, err)
			require.Len(t, files, 1)
			client.AssertNotCalled(t, "AlterTable", mock.Anything, mock.Anything, mock.Anything, mock.Anything)
			client.AssertExpectations(t)
		})
	}
}

func TestHiveUpdateViewMissingAndTableObjects(t *testing.T) {
	for _, object := range []*hive_metastore.Table{nil, testIcebergHiveTable1, {TableType: TableTypeVirtualView}} {
		cat, client, _, created := newHiveViewUpdateTest(t)
		expectImmediateTableLock(client, 1)
		var getErr error
		if object == nil {
			getErr = errNoSuchObject
		}
		client.On("GetTable", mock.Anything, "db", "view").Return(object, getErr).Once()
		_, err := cat.UpdateView(t.Context(), created.Identifier(), nil, nil)
		require.ErrorIs(t, err, catalog.ErrNoSuchView)
		client.AssertExpectations(t)
	}
}

func TestHiveUpdateViewLockFailure(t *testing.T) {
	client := &mockHiveClient{}
	client.On("Lock", mock.Anything, mock.Anything).Return(nil, errors.New("lock failed")).Once()
	cat := NewCatalogWithClient(client, nil)
	_, err := cat.UpdateView(t.Context(), table.Identifier{"db", "view"}, nil, nil)
	require.ErrorIs(t, err, table.ErrCommitFailed)
	client.AssertNotCalled(t, "GetTable", mock.Anything, mock.Anything, mock.Anything)
	client.AssertExpectations(t)
}

func TestHiveUpdateViewRetainsMetadataAfterUnknownCommit(t *testing.T) {
	cat, client, old, created := newHiveViewUpdateTest(t)
	expectImmediateTableLock(client, 1)
	client.On("GetTable", mock.Anything, "db", "view").Return(old, nil).Once()
	client.On("AlterTable", mock.Anything, "db", "view", mock.Anything).Return(errors.New("response lost")).Once()
	_, err := cat.UpdateView(t.Context(), created.Identifier(), nil,
		[]view.Update{view.NewSetPropertiesUpdate(iceberg.Properties{"comment": "changed"})})
	require.ErrorContains(t, err, "response lost")
	require.NotErrorIs(t, err, table.ErrCommitFailed)
	files, err := os.ReadDir(filepath.Join(strings.TrimPrefix(created.Location(), "file://"), "metadata"))
	require.NoError(t, err)
	require.Len(t, files, 2)
	client.AssertExpectations(t)
}

type viewUpdateTestClient struct {
	*mockHiveClient
	renewal HiveClient
}

func (c *viewUpdateTestClient) NewLockClient(context.Context) (HiveClient, error) {
	return c.renewal, nil
}

type viewRenewalTestClient struct {
	HiveClient
	check func(context.Context, int64) (*hive_metastore.LockResponse, error)
}

func (*viewRenewalTestClient) Close() error { return nil }
func (c *viewRenewalTestClient) CheckLock(ctx context.Context, id int64) (*hive_metastore.LockResponse, error) {
	if c.check != nil {
		return c.check(ctx, id)
	}

	return &hive_metastore.LockResponse{Lockid: id, State: hive_metastore.LockState_ACQUIRED}, nil
}

func TestHiveUpdateViewRejectsLostLeaseBeforeAlter(t *testing.T) {
	cat, client, old, created := newHiveViewUpdateTest(t)
	expectImmediateTableLock(client, 1)
	client.On("GetTable", mock.Anything, "db", "view").Return(old, nil).Once()
	checks := 0
	cat.client.(*viewUpdateTestClient).renewal = &viewRenewalTestClient{check: func(context.Context, int64) (*hive_metastore.LockResponse, error) {
		checks++
		if checks > 1 {
			return nil, errors.New("lock expired while another writer committed")
		}

		return &hive_metastore.LockResponse{State: hive_metastore.LockState_ACQUIRED}, nil
	}}
	_, err := cat.UpdateView(t.Context(), created.Identifier(), nil,
		[]view.Update{view.NewSetPropertiesUpdate(iceberg.Properties{"comment": "changed"})})
	require.ErrorIs(t, err, table.ErrCommitFailed)
	client.AssertNotCalled(t, "AlterTable", mock.Anything, mock.Anything, mock.Anything, mock.Anything)
	files, err := os.ReadDir(filepath.Join(strings.TrimPrefix(created.Location(), "file://"), "metadata"))
	require.NoError(t, err)
	require.Len(t, files, 1)
	client.AssertExpectations(t)
}

func TestHiveUpdateViewPreservesUnknownOutcomeAfterLeaseLoss(t *testing.T) {
	cat, client, old, created := newHiveViewUpdateTest(t)
	expectImmediateTableLock(client, 1)
	client.On("GetTable", mock.Anything, "db", "view").Return(old, nil).Once()
	client.On("AlterTable", mock.Anything, "db", "view", mock.Anything).Return(nil).Once()
	checks := 0
	cat.client.(*viewUpdateTestClient).renewal = &viewRenewalTestClient{check: func(context.Context, int64) (*hive_metastore.LockResponse, error) {
		checks++
		if checks > 2 {
			return nil, errors.New("lock lost during alter")
		}

		return &hive_metastore.LockResponse{State: hive_metastore.LockState_ACQUIRED}, nil
	}}
	_, err := cat.UpdateView(t.Context(), created.Identifier(), nil,
		[]view.Update{view.NewSetPropertiesUpdate(iceberg.Properties{"comment": "changed"})})
	require.ErrorContains(t, err, "may have committed")
	require.NotErrorIs(t, err, table.ErrCommitFailed)
	files, err := os.ReadDir(filepath.Join(strings.TrimPrefix(created.Location(), "file://"), "metadata"))
	require.NoError(t, err)
	require.Len(t, files, 2)
	client.AssertExpectations(t)
}
