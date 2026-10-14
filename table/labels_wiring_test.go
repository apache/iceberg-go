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

// labelStubCatalog hands every LoadTable a table carrying the configured labels,
// so Refresh can be checked to re-hydrate them.
type labelStubCatalog struct {
	labels *iceberg.Labels
}

func (c *labelStubCatalog) LoadTable(_ context.Context, ident Identifier) (*Table, error) {
	return New(ident, nil, "",
		func(context.Context) (iceio.IO, error) { return iceio.LocalFS{}, nil }, c,
		WithLabels(c.labels)), nil
}

func (c *labelStubCatalog) CommitTable(context.Context, Identifier, []Requirement, []Update) (Metadata, string, error) {
	return nil, "", nil
}

// committingLabelCatalog applies updates on CommitTable so a transaction Commit
// can be exercised end to end against stubbed metadata.
type committingLabelCatalog struct {
	metadata Metadata
}

func (c *committingLabelCatalog) LoadTable(_ context.Context, ident Identifier) (*Table, error) {
	return New(ident, c.metadata, "",
		func(context.Context) (iceio.IO, error) { return iceio.LocalFS{}, nil }, c), nil
}

func (c *committingLabelCatalog) CommitTable(_ context.Context, _ Identifier, _ []Requirement, updates []Update) (Metadata, string, error) {
	meta, err := UpdateTableMetadata(c.metadata, updates, "")
	if err != nil {
		return nil, "", err
	}
	c.metadata = meta

	return meta, "", nil
}

// TestRefreshRehydratesLabels pins that Refresh adopts the labels from the
// reloaded table.
func TestRefreshRehydratesLabels(t *testing.T) {
	labels := &iceberg.Labels{ObjectLabels: iceberg.Properties{"owner": "analytics"}}
	cat := &labelStubCatalog{labels: labels}
	tbl := New(Identifier{"db", "labels_refresh"}, nil, "",
		func(context.Context) (iceio.IO, error) { return iceio.LocalFS{}, nil }, cat)

	require.Nil(t, tbl.Labels())
	require.NoError(t, tbl.Refresh(context.Background()))
	require.NotNil(t, tbl.Labels())
	assert.Equal(t, iceberg.Properties{"owner": "analytics"}, tbl.Labels().Object())
}

// TestCommitPreservesLabels pins the commit path: doCommit rebuilds the table via
// New(...), so without WithLabels the returned table would carry Labels() == nil
// until a Refresh. The *Table returned by Commit must keep them.
func TestCommitPreservesLabels(t *testing.T) {
	schema := iceberg.NewSchema(0,
		iceberg.NestedField{ID: 1, Name: "id", Type: iceberg.PrimitiveTypes.Int64, Required: true},
	)
	loc := filepath.ToSlash(t.TempDir())
	meta, err := NewMetadata(schema, iceberg.UnpartitionedSpec, UnsortedSortOrder, loc,
		iceberg.Properties{PropertyFormatVersion: "2"})
	require.NoError(t, err)

	labels := &iceberg.Labels{
		ObjectLabels: iceberg.Properties{"owner": "analytics"},
		Fields:       []iceberg.FieldLabel{{FieldID: 1, Labels: iceberg.Properties{"classification": "internal"}}},
	}
	cat := &committingLabelCatalog{metadata: meta}
	tbl := New(Identifier{"db", "commit_labels"}, meta, loc+"/metadata/v1.metadata.json",
		func(context.Context) (iceio.IO, error) { return iceio.LocalFS{}, nil }, cat,
		WithLabels(labels))

	txn := tbl.NewTransaction()
	require.NoError(t, txn.SetProperties(iceberg.Properties{"k": "v"}))
	committed, err := txn.Commit(context.Background())
	require.NoError(t, err)

	require.NotNil(t, committed.Labels(), "commit must carry labels onto the returned table")
	assert.Equal(t, iceberg.Properties{"owner": "analytics"}, committed.Labels().Object())
	assert.Equal(t, iceberg.Properties{"classification": "internal"}, committed.Labels().Field(1))
}

// TestStagedTablePreservesLabels pins that Transaction.StagedTable() forwards the
// transaction table's labels; it rebuilds the table via New(...) too.
func TestStagedTablePreservesLabels(t *testing.T) {
	schema := iceberg.NewSchema(1, iceberg.NestedField{
		ID: 1, Name: "id", Type: iceberg.PrimitiveTypes.Int64, Required: true,
	})
	meta, err := NewMetadata(schema, iceberg.UnpartitionedSpec, UnsortedSortOrder,
		"mem://default/staged", iceberg.Properties{PropertyFormatVersion: "2"})
	require.NoError(t, err)

	labels := &iceberg.Labels{ObjectLabels: iceberg.Properties{"owner": "analytics"}}
	tbl := New(Identifier{"default", "staged"}, meta, "",
		func(context.Context) (iceio.IO, error) { return iceio.NewMemFS(), nil }, nil,
		WithLabels(labels))

	staged, err := tbl.NewTransaction().StagedTable()
	require.NoError(t, err)
	require.NotNil(t, staged.Labels(), "StagedTable must forward the transaction table's labels")
	assert.Equal(t, iceberg.Properties{"owner": "analytics"}, staged.Labels().Object())
}

// TestEqualsIgnoresLabels pins that labels are transient enrichment, excluded
// from Table.Equals.
func TestEqualsIgnoresLabels(t *testing.T) {
	schema := iceberg.NewSchema(0,
		iceberg.NestedField{ID: 1, Name: "id", Type: iceberg.PrimitiveTypes.Int64, Required: true},
	)
	meta, err := NewMetadata(schema, iceberg.UnpartitionedSpec, UnsortedSortOrder, "mem://db/tbl",
		iceberg.Properties{PropertyFormatVersion: "2"})
	require.NoError(t, err)

	fsF := func(context.Context) (iceio.IO, error) { return iceio.NewMemFS(), nil }
	labeled := New(Identifier{"db", "tbl"}, meta, "loc", fsF, nil,
		WithLabels(&iceberg.Labels{ObjectLabels: iceberg.Properties{"owner": "analytics"}}))
	plain := New(Identifier{"db", "tbl"}, meta, "loc", fsF, nil)

	assert.True(t, labeled.Equals(*plain), "labels must not affect Equals")
	assert.True(t, plain.Equals(*labeled))
}
