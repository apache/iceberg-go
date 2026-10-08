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
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// directiveStubCatalog hands every LoadTable a table carrying the configured
// scan-planning directive, so Refresh can be checked to re-hydrate it.
type directiveStubCatalog struct {
	directive string
	present   bool
}

func (c *directiveStubCatalog) LoadTable(_ context.Context, ident Identifier) (*Table, error) {
	return New(ident, nil, "",
		func(context.Context) (iceio.IO, error) { return iceio.LocalFS{}, nil }, c,
		WithScanPlanningDirective(c.directive, c.present)), nil
}

func (c *directiveStubCatalog) CommitTable(context.Context, Identifier, []Requirement, []Update) (Metadata, string, error) {
	return nil, "", nil
}

// TestRefreshRehydratesScanPlanningDirective pins that Refresh adopts the
// directive from the reloaded table, including dropping one the reload no
// longer carries.
func TestRefreshRehydratesScanPlanningDirective(t *testing.T) {
	cat := &directiveStubCatalog{directive: "server", present: true}
	tbl := New(Identifier{"db", "directive_refresh"}, nil, "",
		func(context.Context) (iceio.IO, error) { return iceio.LocalFS{}, nil }, cat)

	got, err := tbl.ScanPlanningDirective()
	require.NoError(t, err)
	require.Equal(t, ScanPlanningDirectiveNone, got)

	require.NoError(t, tbl.Refresh(context.Background()))
	got, err = tbl.ScanPlanningDirective()
	require.NoError(t, err)
	assert.Equal(t, ScanPlanningDirectiveServer, got)

	cat.directive, cat.present = "", false
	require.NoError(t, tbl.Refresh(context.Background()))
	got, err = tbl.ScanPlanningDirective()
	require.NoError(t, err)
	assert.Equal(t, ScanPlanningDirectiveNone, got)
}

// TestCommitPreservesScanPlanningDirective pins the commit path: doCommit
// rebuilds the table via New(...), so the returned table must keep the source
// table's directive, including an unrecognized raw value.
func TestCommitPreservesScanPlanningDirective(t *testing.T) {
	for _, tc := range []struct {
		name    string
		raw     string
		present bool
		want    ScanPlanningDirective
		wantErr bool
	}{
		{name: "absent", want: ScanPlanningDirectiveNone},
		{name: "server", raw: "server", present: true, want: ScanPlanningDirectiveServer},
		{name: "unrecognized", raw: "hybrid", present: true, wantErr: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			schema := iceberg.NewSchema(0,
				iceberg.NestedField{ID: 1, Name: "id", Type: iceberg.PrimitiveTypes.Int64, Required: true},
			)
			loc := filepath.ToSlash(t.TempDir())
			meta, err := NewMetadata(schema, iceberg.UnpartitionedSpec, UnsortedSortOrder, loc,
				iceberg.Properties{PropertyFormatVersion: "2"})
			require.NoError(t, err)

			cat := &committingLabelCatalog{metadata: meta}
			tbl := New(Identifier{"db", "commit_directive"}, meta, loc+"/metadata/v1.metadata.json",
				func(context.Context) (iceio.IO, error) { return iceio.LocalFS{}, nil }, cat,
				WithScanPlanningDirective(tc.raw, tc.present))

			txn := tbl.NewTransaction()
			require.NoError(t, txn.SetProperties(iceberg.Properties{"k": "v"}))
			committed, err := txn.Commit(context.Background())
			require.NoError(t, err)

			got, err := committed.ScanPlanningDirective()
			if tc.wantErr {
				require.ErrorIs(t, err, iceberg.ErrInvalidArgument)
				assert.Contains(t, err.Error(), `"`+tc.raw+`"`)

				return
			}
			require.NoError(t, err)
			assert.Equal(t, tc.want, got)
		})
	}
}

// TestStagedTablePreservesScanPlanningDirective pins that
// Transaction.StagedTable() forwards the transaction table's directive; it
// rebuilds the table via New(...) too.
func TestStagedTablePreservesScanPlanningDirective(t *testing.T) {
	for _, tc := range []struct {
		name    string
		raw     string
		present bool
		want    ScanPlanningDirective
		wantErr bool
	}{
		{name: "absent", want: ScanPlanningDirectiveNone},
		{name: "client", raw: "client", present: true, want: ScanPlanningDirectiveClient},
		{name: "unrecognized", raw: "hybrid", present: true, wantErr: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			schema := iceberg.NewSchema(1, iceberg.NestedField{
				ID: 1, Name: "id", Type: iceberg.PrimitiveTypes.Int64, Required: true,
			})
			meta, err := NewMetadata(schema, iceberg.UnpartitionedSpec, UnsortedSortOrder,
				"mem://default/staged", iceberg.Properties{PropertyFormatVersion: "2"})
			require.NoError(t, err)

			tbl := New(Identifier{"default", "staged"}, meta, "",
				func(context.Context) (iceio.IO, error) { return iceio.NewMemFS(), nil }, nil,
				WithScanPlanningDirective(tc.raw, tc.present))

			staged, err := tbl.NewTransaction().StagedTable()
			require.NoError(t, err)

			got, err := staged.ScanPlanningDirective()
			if tc.wantErr {
				require.ErrorIs(t, err, iceberg.ErrInvalidArgument)
				assert.Contains(t, err.Error(), `"`+tc.raw+`"`)

				return
			}
			require.NoError(t, err)
			assert.Equal(t, tc.want, got)
		})
	}
}
