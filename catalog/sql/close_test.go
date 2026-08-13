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
	"testing"

	"github.com/DataDog/iceberg-go/metrics"
	"github.com/stretchr/testify/require"
	"github.com/uptrace/bun"
	"github.com/uptrace/bun/dialect/sqlitedialect"
)

const (
	closeErrorDriverName   = "iceberg_go_sql_close_error"
	closeErrorReporterName = "iceberg_go_sql_close_reporter"
)

func init() {
	sql.Register(closeErrorDriverName, closeErrorDriver{})
}

type closeErrorDriver struct{}

func (closeErrorDriver) Open(dsn string) (driver.Conn, error) {
	return closeErrorConn{err: errors.New(dsn)}, nil
}

type closeErrorConn struct{ err error }

func (c closeErrorConn) Prepare(string) (driver.Stmt, error) { return nil, driver.ErrSkip }
func (c closeErrorConn) Close() error                        { return c.err }
func (closeErrorConn) Begin() (driver.Tx, error)             { return nil, driver.ErrSkip }

type closeErrorReporter struct{ err error }

func (closeErrorReporter) Report(context.Context, metrics.MetricsReport) {}
func (r closeErrorReporter) Close() error                                { return r.err }

func TestCloseReturnsReporterAndDatabaseErrors(t *testing.T) {
	dbErr := errors.New("database close")
	reporterErr := errors.New("reporter close")

	metrics.Register(closeErrorReporterName, func(map[string]string) (metrics.Reporter, error) {
		return closeErrorReporter{err: reporterErr}, nil
	})
	t.Cleanup(func() {
		metrics.Deregister(closeErrorReporterName)
	})

	db, err := sql.Open(closeErrorDriverName, dbErr.Error())
	require.NoError(t, err)
	// Ping opens a connection so db.Close has a connection to drain.
	require.NoError(t, db.Ping())

	cat := &Catalog{
		db:     bun.NewDB(db, sqlitedialect.New()),
		props:  map[string]string{metrics.ReporterImplKey: closeErrorReporterName},
		ownsDB: true,
	}
	_, err = cat.reporter.Get(cat.props)
	require.NoError(t, err)

	err = cat.Close()
	require.Error(t, err)
	require.ErrorIs(t, err, reporterErr)
	require.ErrorContains(t, err, dbErr.Error())
}
