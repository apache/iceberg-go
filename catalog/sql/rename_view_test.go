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
	"database/sql"
	"database/sql/driver"
	"errors"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/apache/iceberg-go"
	"github.com/apache/iceberg-go/catalog"
	"github.com/apache/iceberg-go/table"
	"github.com/apache/iceberg-go/view"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	"github.com/uptrace/bun/driver/sqliteshim"
)

func createRenameTestView(t *testing.T, cat *Catalog, ident table.Identifier) *view.View {
	t.Helper()
	version := &view.Version{Representations: []view.Representation{
		view.NewRepresentation(view.Representation{SQL: "SELECT 1", Dialect: "spark"}),
	}}
	schema := iceberg.NewSchema(0, iceberg.NestedField{ID: 1, Name: "id", Type: iceberg.PrimitiveTypes.Int32})
	created, err := cat.CreateView(t.Context(), ident, version, schema, catalog.WithViewProperties(iceberg.Properties{"comment": "keep"}))
	require.NoError(t, err)

	return created
}

type wrappedRenameCatalog struct{ catalog.Catalog }

func (w wrappedRenameCatalog) Unwrap() catalog.Catalog { return w.Catalog }

func TestRenameViewCapability(t *testing.T) {
	cat := newNamespaceRaceCatalog(t)
	require.NoError(t, cat.CreateNamespace(t.Context(), table.Identifier{"db"}, nil))
	from := table.Identifier{"db", "old"}
	to := table.Identifier{"db", "new"}
	createRenameTestView(t, cat, from)

	wrapped := wrappedRenameCatalog{wrappedRenameCatalog{cat}}
	renamer, ok := catalog.As[catalog.RenamableViewCatalog](wrapped)
	require.True(t, ok)
	renamed, err := renamer.RenameView(t.Context(), from, to)
	require.NoError(t, err)
	require.Equal(t, to, renamed.Identifier())
	_, err = cat.LoadView(t.Context(), from)
	require.ErrorIs(t, err, catalog.ErrNoSuchView)
}

func TestRenameView(t *testing.T) {
	for _, namespace := range []table.Identifier{{"db"}, {"other"}, {"db", "child"}, {"db.child"}} {
		t.Run(strings.Join(namespace, "."), func(t *testing.T) {
			cat := newNamespaceRaceCatalog(t)
			require.NoError(t, cat.CreateNamespace(t.Context(), table.Identifier{"db"}, nil))
			if !slices.Equal(namespace, table.Identifier{"db"}) {
				require.NoError(t, cat.CreateNamespace(t.Context(), namespace, nil))
			}
			from := table.Identifier{"db", "old"}
			to := append(append(table.Identifier{}, namespace...), "new")
			created := createRenameTestView(t, cat, from)
			renamed, err := cat.RenameView(t.Context(), from, to)
			require.NoError(t, err)
			require.Equal(t, to, renamed.Identifier())
			require.Equal(t, created.MetadataLocation(), renamed.MetadataLocation())
			require.True(t, created.Metadata().Equals(renamed.Metadata()))
			_, err = cat.LoadView(t.Context(), from)
			require.ErrorIs(t, err, catalog.ErrNoSuchView)
			exists, err := cat.CheckViewExists(t.Context(), to)
			require.NoError(t, err)
			require.True(t, exists)
			same, err := cat.RenameView(t.Context(), to, to)
			require.NoError(t, err)
			require.True(t, renamed.Equals(*same))
		})
	}
}

func TestRenameViewErrors(t *testing.T) {
	cat := newNamespaceRaceCatalog(t)
	require.NoError(t, cat.CreateNamespace(t.Context(), table.Identifier{"db"}, nil))
	from := table.Identifier{"db", "source"}
	created := createRenameTestView(t, cat, from)
	createRenameTestView(t, cat, table.Identifier{"db", "view"})
	_, err := cat.CreateTable(t.Context(), table.Identifier{"db", "table"}, created.CurrentSchema())
	require.NoError(t, err)
	tests := []struct {
		name     string
		from, to table.Identifier
		expected error
	}{
		{"view conflict", from, table.Identifier{"db", "view"}, catalog.ErrViewAlreadyExists},
		{"table conflict", from, table.Identifier{"db", "table"}, catalog.ErrTableAlreadyExists},
		{"missing source", table.Identifier{"db", "missing"}, table.Identifier{"db", "new"}, catalog.ErrNoSuchView},
		{"table source", table.Identifier{"db", "table"}, table.Identifier{"db", "new"}, catalog.ErrNoSuchView},
		{"missing namespace", from, table.Identifier{"missing", "new"}, catalog.ErrNoSuchNamespace},
		{"missing no-op", table.Identifier{"db", "missing"}, table.Identifier{"db", "missing"}, catalog.ErrNoSuchView},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := cat.RenameView(t.Context(), tt.from, tt.to)
			require.ErrorIs(t, err, tt.expected)
		})
	}
	for _, ident := range []table.Identifier{nil, {}, {"view"}, {"db", ""}} {
		_, err := cat.RenameView(t.Context(), ident, from)
		require.Error(t, err)
		_, err = cat.RenameView(t.Context(), from, ident)
		require.Error(t, err)
	}
	stillThere, err := cat.LoadView(t.Context(), from)
	require.NoError(t, err)
	require.True(t, created.Equals(*stillThere))
	cat.schemaVersion = schemaV0
	_, err = cat.RenameView(t.Context(), from, table.Identifier{"db", "new"})
	require.ErrorIs(t, err, errViewsUnsupportedOnV0)
}

func TestRenameViewCompetingDestinations(t *testing.T) {
	cat := newNamespaceRaceCatalog(t)
	require.NoError(t, cat.CreateNamespace(t.Context(), table.Identifier{"db"}, nil))
	sources := []table.Identifier{{"db", "first"}, {"db", "second"}}
	for _, ident := range sources {
		createRenameTestView(t, cat, ident)
	}
	to := table.Identifier{"db", "target"}
	start := make(chan struct{})
	results := make(chan error, 2)
	var wg sync.WaitGroup
	for _, ident := range sources {
		wg.Go(func() {
			<-start
			_, err := cat.RenameView(context.Background(), ident, to)
			results <- err
		})
	}
	close(start)
	wg.Wait()
	close(results)
	successes, conflicts := 0, 0
	for err := range results {
		if err == nil {
			successes++
		} else {
			require.ErrorIs(t, err, catalog.ErrViewAlreadyExists)
			conflicts++
		}
	}
	require.Equal(t, 1, successes)
	require.Equal(t, 1, conflicts)
}

type renameCommitErrorDriver struct {
	driver.Driver
	fail    atomic.Bool
	renamed atomic.Bool
	err     error
}

func (d *renameCommitErrorDriver) Open(name string) (driver.Conn, error) {
	conn, err := d.Driver.Open(name)
	if err != nil {
		return nil, err
	}

	return &renameCommitErrorConn{Conn: conn, owner: d}, nil
}

type renameCommitErrorConn struct {
	driver.Conn
	owner *renameCommitErrorDriver
}

func (c *renameCommitErrorConn) Prepare(query string) (driver.Stmt, error) {
	if c.owner.fail.Load() && strings.HasPrefix(strings.TrimSpace(query), "UPDATE") && strings.Contains(query, "iceberg_tables") {
		c.owner.renamed.Store(true)
	}

	return c.Conn.Prepare(query)
}

func (c *renameCommitErrorConn) Begin() (driver.Tx, error) {
	return c.BeginTx(context.Background(), driver.TxOptions{})
}

func (c *renameCommitErrorConn) BeginTx(ctx context.Context, opts driver.TxOptions) (driver.Tx, error) {
	beginner, ok := c.Conn.(driver.ConnBeginTx)
	if !ok {
		return nil, errors.New("test driver requires ConnBeginTx")
	}
	tx, err := beginner.BeginTx(ctx, opts)
	if err != nil {
		return nil, err
	}

	return &renameCommitErrorTx{Tx: tx, owner: c.owner}, nil
}

type renameCommitErrorTx struct {
	driver.Tx
	owner *renameCommitErrorDriver
}

func (tx *renameCommitErrorTx) Commit() error {
	if err := tx.Tx.Commit(); err != nil {
		return err
	}
	if tx.owner.renamed.Swap(false) && tx.owner.fail.Swap(false) {
		return tx.owner.err
	}

	return nil
}

func TestRenameViewPreservesUnknownCommitError(t *testing.T) {
	cause := errors.New("commit response lost")
	base, err := sql.Open(sqliteshim.ShimName, ":memory:")
	require.NoError(t, err)
	wrapped := &renameCommitErrorDriver{Driver: base.Driver(), err: cause}
	require.NoError(t, base.Close())
	name := "rename-commit-error-" + uuid.NewString()
	sql.Register(name, wrapped)
	db, err := sql.Open(name, filepath.Join(t.TempDir(), "catalog.db"))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, db.Close()) })
	cat, err := NewCatalog("default", db, SQLite, iceberg.Properties{"warehouse": "file://" + filepath.ToSlash(t.TempDir())})
	require.NoError(t, err)
	require.NoError(t, cat.CreateNamespace(t.Context(), table.Identifier{"db"}, nil))
	from, to := table.Identifier{"db", "source"}, table.Identifier{"db", "target"}
	created := createRenameTestView(t, cat, from)
	wrapped.fail.Store(true)
	_, err = cat.RenameView(t.Context(), from, to)
	require.ErrorIs(t, err, cause)
	require.NotErrorIs(t, err, catalog.ErrViewAlreadyExists)
	loaded, err := cat.LoadView(t.Context(), to)
	require.NoError(t, err)
	require.Equal(t, created.MetadataLocation(), loaded.MetadataLocation())
}
