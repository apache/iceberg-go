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
	dbsql "database/sql"
	"errors"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/apache/iceberg-go"
	"github.com/apache/iceberg-go/catalog"
	_ "github.com/apache/iceberg-go/io/gocloud"
	"github.com/apache/iceberg-go/table"
	mysql "github.com/go-sql-driver/mysql"
	"github.com/stretchr/testify/require"
	"github.com/uptrace/bun"
	"github.com/uptrace/bun/driver/sqliteshim"
)

type namespaceRaceContextKey struct{}

type namespaceRaceOperation string

const (
	dropNamespaceOperation    namespaceRaceOperation = "drop"
	createObjectOperation     namespaceRaceOperation = "create-object"
	updatePropertiesOperation namespaceRaceOperation = "update-properties"
)

type namespaceRaceHook struct {
	dropAtDelete       chan struct{}
	createAtInsert     chan struct{}
	updateAtInsert     chan struct{}
	releaseDrop        chan struct{}
	dropDone           chan struct{}
	releaseUpdate      chan struct{}
	dropAtDeleteOnce   sync.Once
	createAtInsertOnce sync.Once
	updateAtInsertOnce sync.Once
}

func (h *namespaceRaceHook) BeforeQuery(ctx context.Context, event *bun.QueryEvent) context.Context {
	operation, _ := ctx.Value(namespaceRaceContextKey{}).(namespaceRaceOperation)
	query := event.Query

	switch {
	case operation == dropNamespaceOperation && event.Operation() == "DELETE" && strings.Contains(query, "iceberg_namespace_properties"):
		h.dropAtDeleteOnce.Do(func() { close(h.dropAtDelete) })
		<-h.releaseDrop
	case operation == createObjectOperation && event.Operation() == "INSERT" && strings.Contains(query, "iceberg_tables"):
		h.createAtInsertOnce.Do(func() { close(h.createAtInsert) })
		<-h.releaseDrop
		<-h.dropDone
	case operation == updatePropertiesOperation && event.Operation() == "INSERT" && strings.Contains(query, "iceberg_namespace_properties"):
		h.updateAtInsertOnce.Do(func() { close(h.updateAtInsert) })
		<-h.releaseUpdate
	}

	return ctx
}

func (*namespaceRaceHook) AfterQuery(context.Context, *bun.QueryEvent) {}

func newNamespaceRaceCatalog(t *testing.T) *Catalog {
	t.Helper()

	dbPath := filepath.Join(t.TempDir(), "catalog.db")
	db, err := dbsql.Open(sqliteshim.ShimName, dbPath)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, db.Close()) })
	db.SetMaxOpenConns(4)
	_, err = db.Exec("PRAGMA journal_mode=WAL")
	require.NoError(t, err)

	warehouse := filepath.Join(t.TempDir(), "warehouse")
	cat, err := NewCatalog("default", db, SQLite, iceberg.Properties{
		"warehouse": "file://" + filepath.ToSlash(warehouse),
	})
	require.NoError(t, err)

	return cat
}

type sqlStateTestError string

func (e sqlStateTestError) Error() string    { return string(e) }
func (e sqlStateTestError) SQLState() string { return string(e) }

func TestRetrySerializableWriteTx(t *testing.T) {
	t.Run("retries transient errors", func(t *testing.T) {
		attempts := 0
		err := retrySerializableWriteTx(context.Background(), func() error {
			attempts++
			if attempts == 1 {
				return sqlStateTestError("40001")
			}

			return nil
		})
		require.NoError(t, err)
		require.Equal(t, 2, attempts)
	})

	t.Run("does not retry permanent errors", func(t *testing.T) {
		attempts := 0
		permanentErr := errors.New("permanent error")
		err := retrySerializableWriteTx(context.Background(), func() error {
			attempts++

			return permanentErr
		})
		require.ErrorIs(t, err, permanentErr)
		require.Equal(t, 1, attempts)
	})

	t.Run("returns transient error after attempts are exhausted", func(t *testing.T) {
		attempts := 0
		transientErr := sqlStateTestError("40001")
		err := retrySerializableWriteTx(context.Background(), func() error {
			attempts++

			return transientErr
		})
		require.ErrorIs(t, err, transientErr)
		require.Equal(t, serializableWriteMaxAttempts, attempts)
	})

	t.Run("stops when context is canceled", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		attempts := 0
		err := retrySerializableWriteTx(ctx, func() error {
			attempts++

			return sqlStateTestError("40001")
		})
		require.ErrorIs(t, err, context.Canceled)
		require.Equal(t, 1, attempts)
	})
}

func TestRetryableSerializableError(t *testing.T) {
	tests := []struct {
		name string
		err  error
	}{
		{name: "postgres serialization", err: sqlStateTestError("40001")},
		{name: "postgres deadlock", err: sqlStateTestError("40P01")},
		{name: "mysql deadlock", err: &mysql.MySQLError{Number: 1213}},
		{name: "mysql lock timeout", err: &mysql.MySQLError{Number: 1205}},
		{name: "sqlite3 busy", err: errors.New("database is locked")},
		{name: "oracle serialization", err: errors.New("ORA-08177: can't serialize access for this transaction")},
		{name: "oracle deadlock", err: errors.New("ORA-00060: deadlock detected while waiting for resource")},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			require.True(t, isRetryableSerializableError(tt.err))
		})
	}

	require.False(t, isRetryableSerializableError(errors.New("constraint violation")))
}

func TestRetryableSerializableErrorFromSQLiteDriver(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "locked.db")
	first, err := dbsql.Open(sqliteshim.ShimName, dbPath)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, first.Close()) })
	second, err := dbsql.Open(sqliteshim.ShimName, dbPath)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, second.Close()) })

	first.SetMaxOpenConns(1)
	second.SetMaxOpenConns(1)
	_, err = first.Exec("PRAGMA busy_timeout=0")
	require.NoError(t, err)
	_, err = second.Exec("PRAGMA busy_timeout=0")
	require.NoError(t, err)
	_, err = first.Exec("CREATE TABLE locks (id INTEGER PRIMARY KEY)")
	require.NoError(t, err)

	tx, err := first.Begin()
	require.NoError(t, err)
	t.Cleanup(func() { _ = tx.Rollback() })
	_, err = tx.Exec("INSERT INTO locks VALUES (1)")
	require.NoError(t, err)

	_, lockErr := second.Exec("INSERT INTO locks VALUES (2)")
	require.Error(t, lockErr)
	require.True(t, isRetryableSerializableError(lockErr), "unexpected SQLite lock error type %T: %v", lockErr, lockErr)
}

func TestDropNamespaceDoesNotRaceWithObjectCreation(t *testing.T) {
	schema := iceberg.NewSchema(1, iceberg.NestedField{
		ID: 1, Name: "id", Type: iceberg.PrimitiveTypes.Int64, Required: true,
	})
	tests := []struct {
		name   string
		create func(*testing.T, context.Context, *Catalog, table.Identifier) error
		exists func(context.Context, *Catalog, table.Identifier) (bool, error)
	}{
		{
			name: "create table",
			create: func(_ *testing.T, ctx context.Context, cat *Catalog, ident table.Identifier) error {
				_, err := cat.CreateTable(ctx, ident, schema)

				return err
			},
			exists: func(ctx context.Context, cat *Catalog, ident table.Identifier) (bool, error) {
				return cat.CheckTableExists(ctx, ident)
			},
		},
		{
			name: "create view",
			create: func(_ *testing.T, ctx context.Context, cat *Catalog, ident table.Identifier) error {
				return cat.CreateView(ctx, ident, schema, "SELECT 1", nil)
			},
			exists: func(ctx context.Context, cat *Catalog, ident table.Identifier) (bool, error) {
				return cat.CheckViewExists(ctx, ident)
			},
		},
		{
			name: "commit table create",
			create: func(t *testing.T, ctx context.Context, cat *Catalog, ident table.Identifier) error {
				location := "file://" + filepath.ToSlash(filepath.Join(t.TempDir(), "table"))
				_, _, err := cat.CommitTable(ctx, ident, []table.Requirement{table.AssertCreate()}, []table.Update{
					table.NewSetLocationUpdate(location),
				})

				return err
			},
			exists: func(ctx context.Context, cat *Catalog, ident table.Identifier) (bool, error) {
				return cat.CheckTableExists(ctx, ident)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cat := newNamespaceRaceCatalog(t)
			namespace := table.Identifier{"race_namespace"}
			objectID := table.Identifier{"race_namespace", "race_object"}
			require.NoError(t, cat.CreateNamespace(context.Background(), namespace, nil))

			hook := &namespaceRaceHook{
				dropAtDelete:   make(chan struct{}),
				createAtInsert: make(chan struct{}),
				releaseDrop:    make(chan struct{}),
				dropDone:       make(chan struct{}),
			}
			cat.db.AddQueryHook(hook)

			released := false
			defer func() {
				if !released {
					close(hook.releaseDrop)
				}
			}()

			dropCtx := context.WithValue(context.Background(), namespaceRaceContextKey{}, dropNamespaceOperation)
			createCtx := context.WithValue(context.Background(), namespaceRaceContextKey{}, createObjectOperation)
			dropErrCh := make(chan error, 1)
			createErrCh := make(chan error, 1)

			go func() {
				dropErr := cat.DropNamespace(dropCtx, namespace)
				close(hook.dropDone)
				dropErrCh <- dropErr
			}()
			select {
			case <-hook.dropAtDelete:
			case <-time.After(5 * time.Second):
				t.Fatal("drop did not reach namespace deletion")
			}

			go func() { createErrCh <- tt.create(t, createCtx, cat, objectID) }()
			select {
			case <-hook.createAtInsert:
			case <-time.After(5 * time.Second):
				t.Fatal("creator did not reach catalog insertion")
			}

			close(hook.releaseDrop)
			released = true
			dropErr := <-dropErrCh
			createErr := <-createErrCh
			require.False(t, dropErr == nil && createErr == nil, "drop and create must not both report success")

			objectExists, err := tt.exists(context.Background(), cat, objectID)
			require.NoError(t, err)
			namespaceExists, err := cat.CheckNamespaceExists(context.Background(), namespace)
			require.NoError(t, err)

			switch {
			case createErr == nil:
				require.Error(t, dropErr)
				require.True(t, objectExists)
				require.True(t, namespaceExists)
			case dropErr == nil:
				require.False(t, objectExists)
				require.False(t, namespaceExists)
			default:
				require.Equal(t, namespaceExists, objectExists,
					"failed operations must not leave an object without its namespace")
			}
		})
	}
}

func TestDropNamespaceDoesNotRaceWithUpdateNamespaceProperties(t *testing.T) {
	cat := newNamespaceRaceCatalog(t)
	namespace := table.Identifier{"race_namespace"}
	require.NoError(t, cat.CreateNamespace(context.Background(), namespace, iceberg.Properties{"owner": "before"}))

	hook := &namespaceRaceHook{
		updateAtInsert: make(chan struct{}),
		releaseUpdate:  make(chan struct{}),
	}
	cat.db.AddQueryHook(hook)

	released := false
	defer func() {
		if !released {
			close(hook.releaseUpdate)
		}
	}()

	updateCtx := context.WithValue(context.Background(), namespaceRaceContextKey{}, updatePropertiesOperation)
	updateErrCh := make(chan error, 1)
	go func() {
		_, err := cat.UpdateNamespaceProperties(updateCtx, namespace, nil, iceberg.Properties{"owner": "after"})
		updateErrCh <- err
	}()

	select {
	case <-hook.updateAtInsert:
	case <-time.After(5 * time.Second):
		t.Fatal("property update did not reach namespace property insertion")
	}

	dropErrCh := make(chan error, 1)
	go func() { dropErrCh <- cat.DropNamespace(context.Background(), namespace) }()
	select {
	case err := <-dropErrCh:
		require.NoError(t, err)
	case <-time.After(5 * time.Second):
		t.Fatal("namespace drop did not complete while property update was paused")
	}

	close(hook.releaseUpdate)
	released = true

	select {
	case err := <-updateErrCh:
		require.ErrorIs(t, err, catalog.ErrNoSuchNamespace)
	case <-time.After(5 * time.Second):
		t.Fatal("property update did not finish after namespace drop")
	}

	exists, err := cat.CheckNamespaceExists(context.Background(), namespace)
	require.NoError(t, err)
	require.False(t, exists)
}
