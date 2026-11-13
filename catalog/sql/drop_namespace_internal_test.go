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
	"fmt"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/apache/iceberg-go"
	"github.com/apache/iceberg-go/catalog"
	_ "github.com/apache/iceberg-go/io/gocloud"
	"github.com/apache/iceberg-go/table"
	"github.com/apache/iceberg-go/view"
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

// racePause stops one operation, tagged on its context, at the first statement
// match accepts. It closes reached there and, unless release is nil, waits for
// release before the statement runs.
type racePause struct {
	operation namespaceRaceOperation
	match     func(*bun.QueryEvent) bool
	reached   chan struct{}
	release   chan struct{}
	once      sync.Once
}

func newRacePause(operation namespaceRaceOperation, match func(*bun.QueryEvent) bool, blocking bool) *racePause {
	p := &racePause{operation: operation, match: match, reached: make(chan struct{})}
	if blocking {
		p.release = make(chan struct{})
	}

	return p
}

func (p *racePause) await(t *testing.T, what string) {
	t.Helper()
	select {
	case <-p.reached:
	case <-time.After(5 * time.Second):
		t.Fatalf("%s did not reach its pause point", what)
	}
}

func isWriteLock(event *bun.QueryEvent) bool { return event.Query == sqliteWriteLockQuery }

func isNamespaceDelete(event *bun.QueryEvent) bool {
	return event.Operation() == "DELETE" && strings.Contains(event.Query, "iceberg_namespace_properties")
}

func isObjectInsert(event *bun.QueryEvent) bool {
	return event.Operation() == "INSERT" && strings.Contains(event.Query, "iceberg_tables")
}

func isPropertiesInsert(event *bun.QueryEvent) bool {
	return event.Operation() == "INSERT" && strings.Contains(event.Query, "iceberg_namespace_properties")
}

type namespaceRaceHook struct{ pauses []*racePause }

func (h *namespaceRaceHook) BeforeQuery(ctx context.Context, event *bun.QueryEvent) context.Context {
	operation, _ := ctx.Value(namespaceRaceContextKey{}).(namespaceRaceOperation)
	for _, p := range h.pauses {
		if p.operation == operation && p.match(event) {
			p.once.Do(func() {
				close(p.reached)
				if p.release != nil {
					<-p.release
				}
			})
		}
	}

	return ctx
}
func (*namespaceRaceHook) AfterQuery(context.Context, *bun.QueryEvent) {}

func newNamespaceRaceCatalog(t *testing.T) *Catalog {
	t.Helper()

	// The busy timeout makes a writer that finds the lock held wait for it, on
	// both drivers sqliteshim selects, instead of each driver's own default.
	dbPath := filepath.Join(t.TempDir(), "catalog.db")
	db, err := dbsql.Open(sqliteshim.ShimName, dbPath+"?_busy_timeout=5000")
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
	require.False(t, isRetryableSerializableError(fmt.Errorf("create: %w", &sqliteLockWaitError{err: errors.New("database is locked")})),
		"a lock SQLite already waited busy_timeout for must not be retried")
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

// TestDropNamespaceDoesNotRaceWithObjectCreation pins that DropNamespace and an object
// create serialize on SQLite's write lock, in either order, and never leave an object
// without its namespace.
func TestDropNamespaceDoesNotRaceWithObjectCreation(t *testing.T) {
	schema := iceberg.NewSchema(1, iceberg.NestedField{
		ID: 1, Name: "id", Type: iceberg.PrimitiveTypes.Int64, Required: true,
	})
	creates := []struct {
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
				return createTestView(ctx, cat, ident, schema, view.Representation{SQL: "SELECT 1", Dialect: "spark"}, nil)
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

	dropCtx := context.WithValue(context.Background(), namespaceRaceContextKey{}, dropNamespaceOperation)
	createCtx := context.WithValue(context.Background(), namespaceRaceContextKey{}, createObjectOperation)
	namespace := table.Identifier{"race_namespace"}
	objectID := table.Identifier{"race_namespace", "race_object"}

	for _, tt := range creates {
		t.Run(tt.name, func(t *testing.T) {
			t.Run("create waits for drop", func(t *testing.T) {
				cat := newNamespaceRaceCatalog(t)
				require.NoError(t, cat.CreateNamespace(context.Background(), namespace, nil))

				// The drop pauses holding the write lock; the create reaches its own
				// lock statement and waits behind it.
				dropPause := newRacePause(dropNamespaceOperation, isNamespaceDelete, true)
				createPause := newRacePause(createObjectOperation, isWriteLock, false)
				cat.db = cat.db.WithQueryHook(&namespaceRaceHook{pauses: []*racePause{dropPause, createPause}})
				defer func() {
					select {
					case <-dropPause.release:
					default:
						close(dropPause.release)
					}
				}()

				dropErrCh := make(chan error, 1)
				go func() { dropErrCh <- cat.DropNamespace(dropCtx, namespace) }()
				dropPause.await(t, "drop")

				createErrCh := make(chan error, 1)
				go func() { createErrCh <- tt.create(t, createCtx, cat, objectID) }()
				createPause.await(t, "create")

				close(dropPause.release)
				require.NoError(t, <-dropErrCh)
				require.ErrorIs(t, <-createErrCh, catalog.ErrNoSuchNamespace)

				objectExists, err := tt.exists(context.Background(), cat, objectID)
				require.NoError(t, err)
				require.False(t, objectExists)
				namespaceExists, err := cat.CheckNamespaceExists(context.Background(), namespace)
				require.NoError(t, err)
				require.False(t, namespaceExists)
			})

			t.Run("drop waits for create", func(t *testing.T) {
				cat := newNamespaceRaceCatalog(t)
				require.NoError(t, cat.CreateNamespace(context.Background(), namespace, nil))

				// The create pauses at its insert holding the write lock; the drop
				// cannot finish until the create commits, and then finds the object.
				createPause := newRacePause(createObjectOperation, isObjectInsert, true)
				cat.db = cat.db.WithQueryHook(&namespaceRaceHook{pauses: []*racePause{createPause}})
				defer func() {
					select {
					case <-createPause.release:
					default:
						close(createPause.release)
					}
				}()

				createErrCh := make(chan error, 1)
				go func() { createErrCh <- tt.create(t, createCtx, cat, objectID) }()
				createPause.await(t, "create")

				dropErrCh := make(chan error, 1)
				go func() { dropErrCh <- cat.DropNamespace(dropCtx, namespace) }()
				select {
				case err := <-dropErrCh:
					t.Fatalf("drop finished while the create held the write lock: %v", err)
				case <-time.After(100 * time.Millisecond):
				}

				close(createPause.release)
				require.NoError(t, <-createErrCh)
				require.ErrorIs(t, <-dropErrCh, catalog.ErrNamespaceNotEmpty)

				objectExists, err := tt.exists(context.Background(), cat, objectID)
				require.NoError(t, err)
				require.True(t, objectExists)
				namespaceExists, err := cat.CheckNamespaceExists(context.Background(), namespace)
				require.NoError(t, err)
				require.True(t, namespaceExists)
			})
		})
	}
}

// TestDropNamespaceDoesNotRaceWithUpdateNamespaceProperties pins that DropNamespace and
// UpdateNamespaceProperties serialize on SQLite's write lock, in either order, and leave
// no properties behind for a dropped namespace.
func TestDropNamespaceDoesNotRaceWithUpdateNamespaceProperties(t *testing.T) {
	dropCtx := context.WithValue(context.Background(), namespaceRaceContextKey{}, dropNamespaceOperation)
	updateCtx := context.WithValue(context.Background(), namespaceRaceContextKey{}, updatePropertiesOperation)
	namespace := table.Identifier{"race_namespace"}

	propertyRows := func(t *testing.T, cat *Catalog) int64 {
		t.Helper()
		n, err := cat.db.NewSelect().Model((*sqlIcebergNamespaceProps)(nil)).
			Where("catalog_name = ?", cat.name).
			Where("namespace = ?", strings.Join(namespace, ".")).
			Count(context.Background())
		require.NoError(t, err)

		return n
	}
	update := func(cat *Catalog) error {
		_, err := cat.UpdateNamespaceProperties(updateCtx, namespace, nil, iceberg.Properties{"owner": "after"})

		return err
	}

	t.Run("update waits for drop", func(t *testing.T) {
		cat := newNamespaceRaceCatalog(t)
		require.NoError(t, cat.CreateNamespace(context.Background(), namespace, iceberg.Properties{"owner": "before"}))

		updatePause := newRacePause(updatePropertiesOperation, isWriteLock, true)
		cat.db = cat.db.WithQueryHook(&namespaceRaceHook{pauses: []*racePause{updatePause}})
		defer func() {
			select {
			case <-updatePause.release:
			default:
				close(updatePause.release)
			}
		}()

		updateErrCh := make(chan error, 1)
		go func() { updateErrCh <- update(cat) }()
		updatePause.await(t, "property update")

		require.NoError(t, cat.DropNamespace(dropCtx, namespace))

		close(updatePause.release)
		require.ErrorIs(t, <-updateErrCh, catalog.ErrNoSuchNamespace)

		exists, err := cat.CheckNamespaceExists(context.Background(), namespace)
		require.NoError(t, err)
		require.False(t, exists)
		require.Zero(t, propertyRows(t, cat))
	})

	t.Run("drop waits for update", func(t *testing.T) {
		cat := newNamespaceRaceCatalog(t)
		require.NoError(t, cat.CreateNamespace(context.Background(), namespace, iceberg.Properties{"owner": "before"}))

		updatePause := newRacePause(updatePropertiesOperation, isPropertiesInsert, true)
		cat.db = cat.db.WithQueryHook(&namespaceRaceHook{pauses: []*racePause{updatePause}})
		defer func() {
			select {
			case <-updatePause.release:
			default:
				close(updatePause.release)
			}
		}()

		updateErrCh := make(chan error, 1)
		go func() { updateErrCh <- update(cat) }()
		updatePause.await(t, "property update")

		dropErrCh := make(chan error, 1)
		go func() { dropErrCh <- cat.DropNamespace(dropCtx, namespace) }()
		select {
		case err := <-dropErrCh:
			t.Fatalf("drop finished while the property update held the write lock: %v", err)
		case <-time.After(100 * time.Millisecond):
		}

		close(updatePause.release)
		require.NoError(t, <-updateErrCh)
		require.NoError(t, <-dropErrCh)

		exists, err := cat.CheckNamespaceExists(context.Background(), namespace)
		require.NoError(t, err)
		require.False(t, exists)
		require.Zero(t, propertyRows(t, cat))
	})
}

func createTestView(ctx context.Context, cat *Catalog, identifier table.Identifier, schema *iceberg.Schema, representation view.Representation, props iceberg.Properties) error {
	version := &view.Version{Representations: []view.Representation{view.NewRepresentation(representation)}}
	_, err := cat.CreateView(ctx, identifier, version, schema, catalog.WithViewProperties(props))

	return err
}
