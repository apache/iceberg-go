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

package table_test

import (
	"context"
	"sync"
	"testing"

	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/DataDog/iceberg-go"
	"github.com/DataDog/iceberg-go/catalog"
	"github.com/DataDog/iceberg-go/catalog/sql"
	"github.com/DataDog/iceberg-go/metrics"
	"github.com/DataDog/iceberg-go/table"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/uptrace/bun/driver/sqliteshim"
)

// commitReportSink is registered once as a named reporter so an in-memory
// catalog can select it via metrics-reporter-impl. Register panics on a
// duplicate name, so registration is guarded by sync.Once.
var (
	commitReportSink         = &metrics.InMemoryReporter{}
	registerCommitSinkOnce   sync.Once
	commitReportReporterName = "test-commit-report-sink"
)

func registerCommitReportSink() {
	registerCommitSinkOnce.Do(func() {
		metrics.Register(commitReportReporterName, func(map[string]string) (metrics.Reporter, error) {
			return commitReportSink, nil
		})
	})
}

func TestCommitEmitsCommitReport(t *testing.T) {
	registerCommitReportSink()
	commitReportSink.Reset()

	ctx := context.Background()
	cat, err := catalog.Load(ctx, "default", iceberg.Properties{
		"uri":                   ":memory:",
		"type":                  "sql",
		sql.DriverKey:           sqliteshim.ShimName,
		sql.DialectKey:          string(sql.SQLite),
		"warehouse":             "file://" + t.TempDir(),
		metrics.ReporterImplKey: commitReportReporterName,
	})
	require.NoError(t, err)

	ident := table.Identifier{"default", "commit_report_tbl"}
	require.NoError(t, cat.CreateNamespace(ctx, catalog.NamespaceFromIdent(ident), nil))

	sc := iceberg.NewSchema(0,
		iceberg.NestedField{ID: 1, Name: "id", Type: iceberg.PrimitiveTypes.Int64, Required: true},
	)
	tbl, err := cat.CreateTable(ctx, ident, sc)
	require.NoError(t, err)

	arrowSchema, err := table.SchemaToArrowSchema(sc, nil, true, false)
	require.NoError(t, err)
	arrTable, err := array.TableFromJSON(memory.DefaultAllocator, arrowSchema,
		[]string{`[{"id": 1}, {"id": 2}, {"id": 3}]`})
	require.NoError(t, err)
	defer arrTable.Release()

	_, err = tbl.AppendTable(ctx, arrTable, arrTable.NumRows(), nil)
	require.NoError(t, err)

	var commits []metrics.CommitReport
	for _, r := range commitReportSink.Reports() {
		if cr, ok := r.(metrics.CommitReport); ok {
			commits = append(commits, cr)
		}
	}
	require.NotEmpty(t, commits, "an append commit must emit a CommitReport")

	cr := commits[len(commits)-1]
	assert.Equal(t, "append", cr.Operation)
	require.NotNil(t, cr.Metrics.Attempts)
	assert.GreaterOrEqual(t, cr.Metrics.Attempts.Value, int64(1))
	require.NotNil(t, cr.Metrics.TotalDuration)
	require.NotNil(t, cr.Metrics.AddedDataFiles)
	assert.Positive(t, cr.Metrics.AddedDataFiles.Value)
	require.NotNil(t, cr.Metrics.AddedRecords)
	assert.Equal(t, int64(3), cr.Metrics.AddedRecords.Value)
}

// TestMetadataOnlyCommitEmitsNoCommitReport pins that a commit which produces no
// snapshot (here a property-only change) emits no CommitReport, even though the
// table already has a snapshot. Reporting the unchanged branch head would
// misattribute the earlier append's metrics to this commit.
func TestMetadataOnlyCommitEmitsNoCommitReport(t *testing.T) {
	registerCommitReportSink()
	commitReportSink.Reset()

	ctx := context.Background()
	cat, err := catalog.Load(ctx, "default", iceberg.Properties{
		"uri":                   ":memory:",
		"type":                  "sql",
		sql.DriverKey:           sqliteshim.ShimName,
		sql.DialectKey:          string(sql.SQLite),
		"warehouse":             "file://" + t.TempDir(),
		metrics.ReporterImplKey: commitReportReporterName,
	})
	require.NoError(t, err)

	ident := table.Identifier{"default", "metadata_only_tbl"}
	require.NoError(t, cat.CreateNamespace(ctx, catalog.NamespaceFromIdent(ident), nil))

	sc := iceberg.NewSchema(0,
		iceberg.NestedField{ID: 1, Name: "id", Type: iceberg.PrimitiveTypes.Int64, Required: true},
	)
	tbl, err := cat.CreateTable(ctx, ident, sc)
	require.NoError(t, err)

	arrowSchema, err := table.SchemaToArrowSchema(sc, nil, true, false)
	require.NoError(t, err)
	arrTable, err := array.TableFromJSON(memory.DefaultAllocator, arrowSchema,
		[]string{`[{"id": 1}]`})
	require.NoError(t, err)
	defer arrTable.Release()

	// Seed a snapshot so the property-only commit below has an existing branch
	// head that must NOT be reported.
	tbl, err = tbl.AppendTable(ctx, arrTable, arrTable.NumRows(), nil)
	require.NoError(t, err)

	// Discard the append's report; only the property-only commit is under test.
	commitReportSink.Reset()

	txn := tbl.NewTransaction()
	require.NoError(t, txn.SetProperties(iceberg.Properties{"free-form": "value"}))
	_, err = txn.Commit(ctx)
	require.NoError(t, err)

	for _, r := range commitReportSink.Reports() {
		_, ok := r.(metrics.CommitReport)
		assert.False(t, ok, "metadata-only commit must not emit a CommitReport")
	}
}
