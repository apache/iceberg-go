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

package sql

import (
	"context"
	"errors"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/apache/iceberg-go"
	"github.com/apache/iceberg-go/catalog"
	icebergio "github.com/apache/iceberg-go/io"
	"github.com/apache/iceberg-go/table"
	"github.com/apache/iceberg-go/view"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	"github.com/uptrace/bun"
)

func createUpdateTestView(t *testing.T, cat *Catalog) *view.View {
	t.Helper()
	ctx := t.Context()
	ident := table.Identifier{"db", "view"}
	require.NoError(t, cat.CreateNamespace(ctx, ident[:1], nil))
	schema := iceberg.NewSchema(0, iceberg.NestedField{ID: 1, Name: "id", Type: iceberg.PrimitiveTypes.Int32})
	version := &view.Version{Representations: []view.Representation{
		view.NewRepresentation(view.Representation{SQL: "SELECT 1", Dialect: "spark"}),
		view.NewRepresentation(view.Representation{SQL: "SELECT 1", Dialect: "trino"}),
	}}
	created, err := cat.CreateView(ctx, ident, version, schema)
	require.NoError(t, err)

	return created
}

func TestUpdateViewVersionsAndRollback(t *testing.T) {
	cat := newNamespaceRaceCatalog(t)
	created := createUpdateTestView(t, cat)
	version := created.CurrentVersion().Clone()
	version.VersionID = 2
	version.TimestampMS = time.Now().UnixMilli()
	for i := range version.Representations {
		version.Representations[i].SQL = "SELECT 2"
	}
	updated, err := cat.UpdateView(t.Context(), created.Identifier(), []view.Requirement{view.AssertViewUUID(created.Metadata().ViewUUID())}, []view.Update{
		view.NewAddViewVersionUpdate(version),
		view.NewSetCurrentVersionUpdate(view.LastAddedID),
		view.NewSetPropertiesUpdate(iceberg.Properties{"comment": "updated"}),
	})
	require.NoError(t, err)
	require.EqualValues(t, 2, updated.CurrentVersion().VersionID)
	require.Len(t, updated.Versions(), 2)
	require.Len(t, updated.Metadata().VersionLog(), 2)
	require.Equal(t, version.Representations, updated.CurrentVersion().Representations)
	require.Equal(t, created.Metadata().ViewUUID(), updated.Metadata().ViewUUID())
	require.NotEqual(t, created.MetadataLocation(), updated.MetadataLocation())
	require.EqualValues(t, 1, created.CurrentVersion().VersionID)

	loaded, err := cat.LoadView(t.Context(), created.Identifier())
	require.NoError(t, err)
	require.True(t, loaded.Equals(*updated))
	var previous string
	require.NoError(t, cat.db.NewSelect().Model((*sqlIcebergTable)(nil)).Column("previous_metadata_location").
		Where("metadata_location = ?", updated.MetadataLocation()).Scan(t.Context(), &previous))
	require.Equal(t, created.MetadataLocation(), previous)

	rolledBack, err := cat.UpdateView(t.Context(), created.Identifier(), nil, []view.Update{view.NewSetCurrentVersionUpdate(1)})
	require.NoError(t, err)
	require.EqualValues(t, 1, rolledBack.CurrentVersion().VersionID)
	require.Len(t, rolledBack.Metadata().VersionLog(), 3)
	require.Len(t, rolledBack.Versions(), 2)
}

func TestUpdateViewRequirementsAndNoop(t *testing.T) {
	cat := newNamespaceRaceCatalog(t)
	created := createUpdateTestView(t, cat)
	dir := strings.TrimPrefix(created.Location(), "file://")
	countFiles := func() int {
		entries, err := os.ReadDir(filepath.Join(dir, "metadata"))
		require.NoError(t, err)

		return len(entries)
	}
	require.Equal(t, 1, countFiles())
	_, err := cat.UpdateView(t.Context(), created.Identifier(), []view.Requirement{view.AssertViewUUID(uuid.New())}, nil)
	require.ErrorIs(t, err, table.ErrCommitFailed)
	require.Equal(t, 1, countFiles())

	_, err = cat.UpdateView(t.Context(), created.Identifier(), nil, []view.Update{view.NewSetCurrentVersionUpdate(999)})
	require.ErrorContains(t, err, "unknown version")
	require.Equal(t, 1, countFiles())

	same, err := cat.UpdateView(t.Context(), created.Identifier(), nil, nil)
	require.NoError(t, err)
	require.Equal(t, created.MetadataLocation(), same.MetadataLocation())
	require.Equal(t, 1, countFiles())
	_, err = cat.UpdateView(t.Context(), table.Identifier{"db", "missing"}, nil, nil)
	require.ErrorIs(t, err, catalog.ErrNoSuchView)
	_, err = cat.UpdateView(t.Context(), nil, nil, nil)
	require.ErrorIs(t, err, catalog.ErrNoSuchView)

	_, err = cat.CreateTable(t.Context(), table.Identifier{"db", "table"}, created.CurrentSchema())
	require.NoError(t, err)
	_, err = cat.UpdateView(t.Context(), table.Identifier{"db", "table"}, nil, nil)
	require.ErrorIs(t, err, catalog.ErrNoSuchView)
}

func TestUpdateViewDialectValidation(t *testing.T) {
	cat := newNamespaceRaceCatalog(t)
	created := createUpdateTestView(t, cat)
	next := created.CurrentVersion().Clone()
	next.Representations = next.Representations[:1]
	next.Representations[0].SQL = "SELECT 2"
	updates := []view.Update{view.NewAddViewVersionUpdate(next), view.NewSetCurrentVersionUpdate(view.LastAddedID)}
	_, err := cat.UpdateView(t.Context(), created.Identifier(), nil, updates)
	require.ErrorContains(t, err, "dialect")
	loaded, err := cat.LoadView(t.Context(), created.Identifier())
	require.NoError(t, err)
	require.Equal(t, created.MetadataLocation(), loaded.MetadataLocation())

	updates = append([]view.Update{view.NewSetPropertiesUpdate(iceberg.Properties{view.ReplaceDropDialectAllowedKey: "true"})}, updates...)
	updated, err := cat.UpdateView(t.Context(), created.Identifier(), nil, updates)
	require.NoError(t, err)
	require.Len(t, updated.CurrentVersion().Representations, 1)
}

func TestUpdateViewRetentionAndMetadataLocation(t *testing.T) {
	cat := newNamespaceRaceCatalog(t)
	created := createUpdateTestView(t, cat)
	dir := "file://" + filepath.ToSlash(t.TempDir())
	version := created.CurrentVersion().Clone()
	for i := range version.Representations {
		version.Representations[i].SQL = "SELECT 2"
	}
	updated, err := cat.UpdateView(t.Context(), created.Identifier(), nil, []view.Update{
		view.NewSetPropertiesUpdate(iceberg.Properties{view.VersionHistorySizeKey: "1", "write.metadata.path": dir}),
		view.NewAddViewVersionUpdate(version), view.NewSetCurrentVersionUpdate(view.LastAddedID),
	})
	require.NoError(t, err)
	require.True(t, strings.HasPrefix(updated.MetadataLocation(), dir+"/"))
	require.Len(t, updated.Versions(), 1)
	_, err = cat.UpdateView(t.Context(), created.Identifier(), nil, []view.Update{view.NewSetCurrentVersionUpdate(1)})
	require.ErrorContains(t, err, "unknown version")
}

type viewUpdateContextKey struct{}

type viewUpdateHook struct {
	staged  chan struct{}
	release chan struct{}
}

func (h *viewUpdateHook) BeforeQuery(ctx context.Context, event *bun.QueryEvent) context.Context {
	if ctx.Value(viewUpdateContextKey{}) == true && event.Operation() == "UPDATE" && strings.Contains(event.Query, "iceberg_tables") {
		close(h.staged)
		<-h.release
	}

	return ctx
}

func (*viewUpdateHook) AfterQuery(context.Context, *bun.QueryEvent) {}

func TestUpdateViewRejectsCompetingCommit(t *testing.T) {
	cat := newNamespaceRaceCatalog(t)
	created := createUpdateTestView(t, cat)
	hook := &viewUpdateHook{staged: make(chan struct{}), release: make(chan struct{})}
	cat.db = cat.db.WithQueryHook(hook)
	done := make(chan error, 1)
	go func() {
		_, err := cat.UpdateView(context.WithValue(t.Context(), viewUpdateContextKey{}, true), created.Identifier(), nil,
			[]view.Update{view.NewSetPropertiesUpdate(iceberg.Properties{"loser": "true"})})
		done <- err
	}()
	select {
	case <-hook.staged:
	case <-time.After(10 * time.Second):
		close(hook.release)
		t.Fatal("writer did not reach metadata swap")
	}
	winner, err := cat.UpdateView(t.Context(), created.Identifier(), nil,
		[]view.Update{view.NewSetPropertiesUpdate(iceberg.Properties{"winner": "true"})})
	close(hook.release)
	require.NoError(t, err)
	require.ErrorIs(t, <-done, table.ErrCommitFailed)
	loaded, err := cat.LoadView(t.Context(), created.Identifier())
	require.NoError(t, err)
	require.True(t, loaded.Equals(*winner))
	require.NotContains(t, loaded.Properties(), "loser")
	entries, err := os.ReadDir(filepath.Join(strings.TrimPrefix(created.Location(), "file://"), "metadata"))
	require.NoError(t, err)
	require.Len(t, entries, 2)
}

func TestUpdateViewRejectsV0(t *testing.T) {
	cat := newNamespaceRaceCatalog(t)
	cat.schemaVersion = schemaV0
	_, err := cat.UpdateView(t.Context(), table.Identifier{"db", "view"}, nil, nil)
	require.ErrorIs(t, err, errViewsUnsupportedOnV0)
}

type failingViewWriter struct {
	icebergio.FileWriter
	failure   error
	failClose bool
}

func (w *failingViewWriter) Write(p []byte) (int, error) {
	if !w.failClose {
		return 0, w.failure
	}

	return w.FileWriter.Write(p)
}

func (w *failingViewWriter) Close() error {
	err := w.FileWriter.Close()
	if w.failClose {
		return errors.Join(err, w.failure)
	}

	return err
}

type failingViewFS struct {
	icebergio.LocalFS
	failure        error
	failClose      bool
	cleanupContext error
	cancel         context.CancelFunc
}

func (fs *failingViewFS) Create(ctx context.Context, name string) (icebergio.FileWriter, error) {
	writer, err := fs.LocalFS.Create(ctx, name)
	if err != nil {
		return nil, err
	}

	fs.cancel()

	return &failingViewWriter{FileWriter: writer, failure: fs.failure, failClose: fs.failClose}, nil
}

func (fs *failingViewFS) Remove(ctx context.Context, name string) error {
	fs.cleanupContext = ctx.Err()

	return fs.LocalFS.Remove(ctx, name)
}

func TestUpdateViewCleansFailedMetadataWrites(t *testing.T) {
	for _, failClose := range []bool{false, true} {
		t.Run(map[bool]string{false: "write", true: "close"}[failClose], func(t *testing.T) {
			cat := newNamespaceRaceCatalog(t)
			created := createUpdateTestView(t, cat)
			location := filepath.Join(t.TempDir(), "failed.metadata.json")
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			fs := &failingViewFS{failure: io.ErrClosedPipe, failClose: failClose, cancel: cancel}
			err := writeUpdatedViewMetadata(ctx, fs, location, created.Metadata())
			require.ErrorIs(t, err, io.ErrClosedPipe)
			require.NoError(t, fs.cleanupContext)
			_, err = os.Stat(location)
			require.ErrorIs(t, err, os.ErrNotExist)
		})
	}
}

func TestUpdateViewSchemaReplacement(t *testing.T) {
	cat := newNamespaceRaceCatalog(t)
	created := createUpdateTestView(t, cat)
	schema := iceberg.NewSchema(99, iceberg.NestedField{ID: 1, Name: "renamed", Type: iceberg.PrimitiveTypes.Int32})
	version := created.CurrentVersion().Clone()
	version.SchemaID = view.LastAddedID
	updated, err := cat.UpdateView(t.Context(), created.Identifier(), nil, []view.Update{
		view.NewAddSchemaUpdate(schema), view.NewAddViewVersionUpdate(version), view.NewSetCurrentVersionUpdate(view.LastAddedID),
	})
	require.NoError(t, err)
	require.Equal(t, "renamed", updated.CurrentSchema().Field(0).Name)
	require.Equal(t, "id", created.CurrentSchema().Field(0).Name)
	require.Len(t, updated.Schemas(), 2)
	loaded, err := cat.LoadView(t.Context(), created.Identifier())
	require.NoError(t, err)
	require.True(t, updated.Equals(*loaded))
}
