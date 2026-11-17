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
	"strconv"
	"testing"
	"time"

	"github.com/apache/iceberg-go"
	"github.com/apache/iceberg-go/catalog"
	"github.com/apache/iceberg-go/table"
	"github.com/stretchr/testify/require"
	"github.com/uptrace/bun"
	"github.com/uptrace/bun/driver/sqliteshim"
)

// newWriteLockCatalog returns a catalog on a single pooled connection with the given
// busy_timeout, so a test can read back that connection's pragma.
func newWriteLockCatalog(t *testing.T, busyTimeout time.Duration) (*Catalog, string) {
	t.Helper()

	dbPath := filepath.Join(t.TempDir(), "catalog.db")
	db, err := dbsql.Open(sqliteshim.ShimName, dbPath+"?_busy_timeout="+strconv.FormatInt(busyTimeout.Milliseconds(), 10))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, db.Close()) })
	db.SetMaxOpenConns(1)
	_, err = db.Exec("PRAGMA journal_mode=WAL")
	require.NoError(t, err)

	cat, err := NewCatalog(t.Context(), "default", db, SQLite, iceberg.Properties{
		"warehouse": "file://" + filepath.ToSlash(filepath.Join(t.TempDir(), "warehouse")),
	})
	require.NoError(t, err)

	return cat, dbPath
}

// holdWriteLock takes the database's write lock from a second connection until the
// test ends.
func holdWriteLock(t *testing.T, dbPath string) {
	t.Helper()

	holder, err := dbsql.Open(sqliteshim.ShimName, dbPath)
	require.NoError(t, err)
	t.Cleanup(func() { _ = holder.Close() })
	_, err = holder.Exec("CREATE TABLE IF NOT EXISTS lock_holder(x)")
	require.NoError(t, err)
	tx, err := holder.Begin()
	require.NoError(t, err)
	t.Cleanup(func() { _ = tx.Rollback() })
	_, err = tx.Exec("INSERT INTO lock_holder VALUES (1)")
	require.NoError(t, err)
}

func TestLockSQLiteForWrite(t *testing.T) {
	const busyTimeout = 3 * time.Second

	tests := []struct {
		name  string
		setup func(*testing.T, *Catalog, string) (context.Context, context.CancelFunc)
		check func(*testing.T, error, time.Duration)
	}{
		{
			name: "deadline caps the busy wait",
			setup: func(t *testing.T, _ *Catalog, dbPath string) (context.Context, context.CancelFunc) {
				holdWriteLock(t, dbPath)

				return context.WithTimeout(context.Background(), 200*time.Millisecond)
			},
			check: func(t *testing.T, err error, elapsed time.Duration) {
				require.ErrorIs(t, err, context.DeadlineExceeded)
				require.Less(t, elapsed, busyTimeout/2, "waited %v", elapsed)
			},
		},
		{
			name: "expired deadline does not wait",
			setup: func(t *testing.T, _ *Catalog, dbPath string) (context.Context, context.CancelFunc) {
				holdWriteLock(t, dbPath)

				return context.WithDeadline(context.Background(), time.Now().Add(-time.Second))
			},
			check: func(t *testing.T, err error, elapsed time.Duration) {
				require.ErrorIs(t, err, context.DeadlineExceeded)
				require.Less(t, elapsed, busyTimeout/2, "waited %v", elapsed)
			},
		},
		{
			name: "non-lock error is not a lock wait",
			setup: func(t *testing.T, cat *Catalog, _ string) (context.Context, context.CancelFunc) {
				_, err := cat.db.Exec("DROP TABLE iceberg_tables")
				require.NoError(t, err)

				return context.WithCancel(context.Background())
			},
			check: func(t *testing.T, err error, _ time.Duration) {
				require.ErrorContains(t, err, "no such table")
				_, isLockWait := errors.AsType[*sqliteLockWaitError](err)
				require.False(t, isLockWait)
				require.False(t, isRetryableSerializableError(err))
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cat, dbPath := newWriteLockCatalog(t, busyTimeout)
			ctx, cancel := tt.setup(t, cat, dbPath)
			defer cancel()

			start := time.Now()
			err := withWriteTx(ctx, cat.db, func(context.Context, bun.Tx) error { return nil })
			tt.check(t, err, time.Since(start))

			// The single pooled connection keeps the busy_timeout it was opened with.
			var restored int64
			require.NoError(t, cat.db.QueryRow("PRAGMA busy_timeout").Scan(&restored))
			require.Equal(t, busyTimeout.Milliseconds(), restored)
		})
	}
}

func TestClassifySQLiteLockError(t *testing.T) {
	var (
		busy        = errors.New("database is locked (5) (SQLITE_BUSY)")
		tableLocked = errors.New("database table is locked (6) (SQLITE_LOCKED)")
		readOnly    = errors.New("attempt to write a readonly database (8)")
	)
	timeout := func(ms int64) func() int64 { return func() int64 { return ms } }
	unread := func() int64 {
		t.Error("busy_timeout must be read only for a lock SQLite may have waited on")

		return 0
	}

	t.Run("no error reads nothing", func(t *testing.T) {
		require.NoError(t, classifySQLiteLockError(t.Context(), nil, false, unread))
	})

	t.Run("busy after a busy_timeout is a lock wait and not retried", func(t *testing.T) {
		err := classifySQLiteLockError(t.Context(), busy, false, timeout(5000))
		_, isLockWait := errors.AsType[*sqliteLockWaitError](err)
		require.True(t, isLockWait)
		require.False(t, isRetryableSerializableError(err))
	})

	t.Run("busy without a busy_timeout is retried", func(t *testing.T) {
		err := classifySQLiteLockError(t.Context(), busy, false, timeout(0))
		require.ErrorIs(t, err, busy)
		require.True(t, isRetryableSerializableError(err))
	})

	// SQLite reports a shared-cache table lock at once, without the busy
	// handler, so no busy_timeout or deadline was waited out.
	t.Run("shared-cache table lock is retried", func(t *testing.T) {
		for _, capped := range []bool{false, true} {
			err := classifySQLiteLockError(t.Context(), tableLocked, capped, unread)
			require.ErrorIs(t, err, tableLocked)
			require.NotErrorIs(t, err, context.DeadlineExceeded)
			require.True(t, isRetryableSerializableError(err))
		}
	})

	t.Run("busy under a capped busy_timeout is the deadline", func(t *testing.T) {
		err := classifySQLiteLockError(t.Context(), busy, true, unread)
		require.ErrorIs(t, err, context.DeadlineExceeded)
	})

	t.Run("read-only database carries on without the lock", func(t *testing.T) {
		require.NoError(t, classifySQLiteLockError(t.Context(), readOnly, false, unread))
	})
}

// A shared-cache connection gets SQLITE_LOCKED for a table another connection of
// the same cache is writing. modernc waits for that lock through unlock_notify,
// so only mattn reports it.
func TestWriteRetriesSharedCacheTableLock(t *testing.T) {
	if sqliteshim.DriverName() != "sqlite3" {
		t.Skipf("driver %s waits for a shared-cache table lock itself", sqliteshim.DriverName())
	}

	dsn := "file:" + filepath.Join(t.TempDir(), "catalog.db") + "?cache=shared&_busy_timeout=5000"
	db, err := dbsql.Open(sqliteshim.ShimName, dsn)
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })
	cat, err := NewCatalog(t.Context(), "default", db, SQLite, iceberg.Properties{
		"warehouse": "file://" + filepath.ToSlash(filepath.Join(t.TempDir(), "warehouse")),
	})
	require.NoError(t, err)
	ns := table.Identifier{"ns"}
	require.NoError(t, cat.CreateNamespace(t.Context(), ns, nil))

	holder, err := dbsql.Open(sqliteshim.ShimName, dsn)
	require.NoError(t, err)
	t.Cleanup(func() { _ = holder.Close() })
	tx, err := holder.Begin()
	require.NoError(t, err)
	_, err = tx.Exec("UPDATE iceberg_tables SET catalog_name = catalog_name")
	require.NoError(t, err)
	released := make(chan error, 1)
	time.AfterFunc(100*time.Millisecond, func() { released <- tx.Commit() })

	// DropNamespace opens with its write transaction, so the lock statement is
	// what meets the table lock.
	require.NoError(t, cat.DropNamespace(t.Context(), ns))
	require.NoError(t, <-released)
}

// On a database that cannot be written, the lock statement fails before the
// transaction reads anything. The reads must still run and return the catalog
// sentinels they returned before the lock statement existed.
func TestReadOnlyDatabaseKeepsCatalogSentinels(t *testing.T) {
	ctx := t.Context()
	dbPath := filepath.Join(t.TempDir(), "catalog.db")
	props := iceberg.Properties{
		"warehouse": "file://" + filepath.ToSlash(filepath.Join(t.TempDir(), "warehouse")),
	}
	ns := table.Identifier{"ns"}
	tbl := table.Identifier{"ns", "tbl"}
	schema := iceberg.NewSchema(1, iceberg.NestedField{
		ID: 1, Name: "id", Type: iceberg.PrimitiveTypes.Int32, Required: true,
	})

	rw, err := dbsql.Open(sqliteshim.ShimName, dbPath)
	require.NoError(t, err)
	writable, err := NewCatalog(ctx, "default", rw, SQLite, props)
	require.NoError(t, err)
	require.NoError(t, writable.CreateNamespace(ctx, ns, nil))
	_, err = writable.CreateTable(ctx, tbl, schema)
	require.NoError(t, err)
	require.NoError(t, rw.Close())

	ro, err := dbsql.Open(sqliteshim.ShimName, "file:"+dbPath+"?mode=ro")
	require.NoError(t, err)
	t.Cleanup(func() { _ = ro.Close() })
	props[initCatalogTablesKey] = "false"
	cat, err := NewCatalog(ctx, "default", ro, SQLite, props)
	require.NoError(t, err)

	require.ErrorIs(t, cat.CreateNamespace(ctx, ns, nil), catalog.ErrNamespaceAlreadyExists)
	_, err = cat.CreateTable(ctx, tbl, schema)
	require.ErrorIs(t, err, catalog.ErrTableAlreadyExists)
	require.ErrorIs(t, cat.DropNamespace(ctx, table.Identifier{"missing"}), catalog.ErrNoSuchNamespace)

	// A real write still fails, with the driver's read-only error.
	err = cat.CreateNamespace(ctx, table.Identifier{"other"}, nil)
	require.ErrorContains(t, err, "readonly database")
	_, isLockWait := errors.AsType[*sqliteLockWaitError](err)
	require.False(t, isLockWait)
}
