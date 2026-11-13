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

	cat, err := NewCatalog("default", db, SQLite, iceberg.Properties{
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
