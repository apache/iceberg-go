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

package table

import (
	"context"
	"path/filepath"
	"testing"

	"github.com/apache/iceberg-go"
	iceio "github.com/apache/iceberg-go/io"
	"github.com/apache/iceberg-go/metrics"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestTableMetricsReporterDefaultsToNop(t *testing.T) {
	tbl := New(nil, nil, "", nil, nil)
	assert.IsType(t, metrics.NopReporter{}, tbl.MetricsReporter())
}

func TestWithMetricsReporter(t *testing.T) {
	rep := &metrics.InMemoryReporter{}
	tbl := New(nil, nil, "", nil, nil, WithMetricsReporter(rep))
	assert.Same(t, rep, tbl.MetricsReporter())

	// A nil reporter option is ignored; the default no-op reporter is kept.
	tblNil := New(nil, nil, "", nil, nil, WithMetricsReporter(nil))
	assert.IsType(t, metrics.NopReporter{}, tblNil.MetricsReporter())
}

func TestScanInheritsReporterFromTable(t *testing.T) {
	rep := &metrics.InMemoryReporter{}
	tbl := New(nil, nil, "", nil, nil, WithMetricsReporter(rep))

	scan := tbl.Scan()
	assert.Same(t, rep, scan.Reporter())
}

func TestScanReporterDefaultsToNop(t *testing.T) {
	tbl := New(nil, nil, "", nil, nil)
	assert.IsType(t, metrics.NopReporter{}, tbl.Scan().Reporter())
}

func TestWithReporterOverridesScan(t *testing.T) {
	tableRep := &metrics.InMemoryReporter{}
	scanRep := &metrics.InMemoryReporter{}
	tbl := New(nil, nil, "", nil, nil, WithMetricsReporter(tableRep))

	// Scan-level WithReporter wins over the table's reporter.
	scan := tbl.Scan(WithReporter(scanRep))
	assert.Same(t, scanRep, scan.Reporter())
	require.NotSame(t, tableRep, scan.Reporter())

	// A nil WithReporter is ignored; the inherited reporter is kept.
	scanNil := tbl.Scan(WithReporter(nil))
	assert.Same(t, tableRep, scanNil.Reporter())
}

// reporterStubCatalog is a minimal catalog whose LoadTable returns a table
// carrying the configured reporter (or the default nop reporter when nil). It
// lets the Refresh tests control exactly which reporter the "fresh" table
// arrives with.
type reporterStubCatalog struct {
	reporter metrics.Reporter
}

func (c *reporterStubCatalog) LoadTable(_ context.Context, ident Identifier) (*Table, error) {
	opts := []Option{}
	if c.reporter != nil {
		opts = append(opts, WithMetricsReporter(c.reporter))
	}

	return New(ident, nil, "",
		func(context.Context) (iceio.IO, error) { return iceio.LocalFS{}, nil }, c, opts...), nil
}

func (c *reporterStubCatalog) CommitTable(context.Context, Identifier, []Requirement, []Update) (Metadata, string, error) {
	return nil, "", nil
}

// TestStagedTableInheritsReporter pins that Transaction.StagedTable() forwards
// the transaction table's reporter. StagedTable rebuilds the table via New(...),
// which would otherwise reset the reporter to the construction-time nop default.
func TestStagedTableInheritsReporter(t *testing.T) {
	schema := iceberg.NewSchema(1, iceberg.NestedField{
		ID: 1, Name: "id", Type: iceberg.PrimitiveTypes.Int64, Required: true,
	})
	meta, err := NewMetadata(schema, iceberg.UnpartitionedSpec, UnsortedSortOrder,
		"mem://default/staged", iceberg.Properties{PropertyFormatVersion: "2"})
	require.NoError(t, err)

	rep := &metrics.InMemoryReporter{}
	tbl := New(Identifier{"default", "staged"}, meta, "",
		func(context.Context) (iceio.IO, error) { return iceio.NewMemFS(), nil }, nil,
		WithMetricsReporter(rep))

	staged, err := tbl.NewTransaction().StagedTable()
	require.NoError(t, err)
	assert.Same(t, rep, staged.MetricsReporter(),
		"StagedTable must forward the transaction table's reporter")
}

// TestRefreshKeepsCallerReporter pins that a reporter injected via
// WithMetricsReporter must survive a Refresh even though the catalog's LoadTable
// hands back the default nop reporter.
func TestRefreshKeepsCallerReporter(t *testing.T) {
	rep := &metrics.InMemoryReporter{}
	cat := &reporterStubCatalog{} // LoadTable yields a nop-reporter table
	tbl := New(Identifier{"db", "reporter-refresh"}, nil, "",
		func(context.Context) (iceio.IO, error) { return iceio.LocalFS{}, nil }, cat,
		WithMetricsReporter(rep))

	require.Same(t, rep, tbl.MetricsReporter())
	require.NoError(t, tbl.Refresh(context.Background()))
	assert.Same(t, rep, tbl.MetricsReporter(),
		"caller-set reporter must not be reverted to the catalog default on refresh")
}

// TestRefreshKeepsExplicitNopOptOut pins the flag-based guard: a caller who
// explicitly opts out with WithMetricsReporter(NopReporter{}) has still "set" a
// reporter, so Refresh must not revert it to a catalog-provided logging reporter.
// A type-based "is it a nop?" guard would silently fail this case.
func TestRefreshKeepsExplicitNopOptOut(t *testing.T) {
	fresh := &metrics.InMemoryReporter{}
	cat := &reporterStubCatalog{reporter: fresh} // LoadTable yields a logging-style reporter
	tbl := New(Identifier{"db", "reporter-refresh"}, nil, "",
		func(context.Context) (iceio.IO, error) { return iceio.LocalFS{}, nil }, cat,
		WithMetricsReporter(metrics.NopReporter{}))

	require.IsType(t, metrics.NopReporter{}, tbl.MetricsReporter())
	require.NoError(t, tbl.Refresh(context.Background()))
	assert.IsType(t, metrics.NopReporter{}, tbl.MetricsReporter(),
		"explicit NopReporter opt-out must not be reverted to the catalog reporter on refresh")
}

// TestRefreshInheritsCatalogReporterWhenUnset pins that when the caller never
// overrode the reporter, Refresh adopts the fresh table's.
func TestRefreshInheritsCatalogReporterWhenUnset(t *testing.T) {
	fresh := &metrics.InMemoryReporter{}
	cat := &reporterStubCatalog{reporter: fresh}
	tbl := New(Identifier{"db", "reporter-refresh"}, nil, "",
		func(context.Context) (iceio.IO, error) { return iceio.LocalFS{}, nil }, cat)

	require.IsType(t, metrics.NopReporter{}, tbl.MetricsReporter())
	require.NoError(t, tbl.Refresh(context.Background()))
	assert.Same(t, fresh, tbl.MetricsReporter(),
		"an unset reporter must inherit the catalog-derived one on refresh")
}

// committingReporterCatalog applies updates on CommitTable and hands every
// LoadTable a table carrying the configured reporter, so a create+Commit+Refresh
// sequence can be exercised end to end.
type committingReporterCatalog struct {
	metadata Metadata
	reporter metrics.Reporter
}

func (c *committingReporterCatalog) LoadTable(_ context.Context, ident Identifier) (*Table, error) {
	opts := []Option{}
	if c.reporter != nil {
		opts = append(opts, WithMetricsReporter(c.reporter))
	}

	return New(ident, c.metadata, "",
		func(context.Context) (iceio.IO, error) { return iceio.LocalFS{}, nil }, c, opts...), nil
}

func (c *committingReporterCatalog) CommitTable(_ context.Context, _ Identifier, _ []Requirement, updates []Update) (Metadata, string, error) {
	meta, err := UpdateTableMetadata(c.metadata, updates, "")
	if err != nil {
		return nil, "", err
	}
	c.metadata = meta

	return meta, "", nil
}

// TestCommitPreservesDefaultedReporterInheritance pins that a table riding the
// catalog default (reporterSet == false) keeps inheriting the catalog reporter
// on a Refresh that follows a Commit. doCommit rebuilds the table via New(...);
// carrying the reporter through WithMetricsReporter(MetricsReporter()) would
// force reporterSet true (that accessor is never nil), freezing the post-commit
// table to its current reporter — this is the create+Commit+Refresh path the
// Refresh-only tests do not cover.
func TestCommitPreservesDefaultedReporterInheritance(t *testing.T) {
	schema := iceberg.NewSchema(0,
		iceberg.NestedField{ID: 1, Name: "id", Type: iceberg.PrimitiveTypes.Int64, Required: true},
	)
	loc := filepath.ToSlash(t.TempDir())
	meta, err := NewMetadata(schema, iceberg.UnpartitionedSpec, UnsortedSortOrder, loc,
		iceberg.Properties{PropertyFormatVersion: "2"})
	require.NoError(t, err)

	fresh := &metrics.InMemoryReporter{}
	cat := &committingReporterCatalog{metadata: meta, reporter: fresh}

	// Created without an explicit reporter: reporterSet == false.
	tbl := New(Identifier{"db", "commit_reporter"}, meta, loc+"/metadata/v1.metadata.json",
		func(context.Context) (iceio.IO, error) { return iceio.LocalFS{}, nil }, cat)
	require.IsType(t, metrics.NopReporter{}, tbl.MetricsReporter())

	txn := tbl.NewTransaction()
	require.NoError(t, txn.SetProperties(iceberg.Properties{"k": "v"}))
	committed, err := txn.Commit(context.Background())
	require.NoError(t, err)

	// No caller ever set a reporter, so the committed table must still inherit
	// the catalog-derived one on refresh.
	require.NoError(t, committed.Refresh(context.Background()))
	assert.Same(t, fresh, committed.MetricsReporter(),
		"a defaulted table must keep inheriting the catalog reporter after a commit")
}
