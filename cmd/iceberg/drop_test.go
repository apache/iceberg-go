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

package main

import (
	"context"
	"errors"
	"iter"
	"testing"

	"github.com/apache/iceberg-go"
	"github.com/apache/iceberg-go/catalog"
	"github.com/apache/iceberg-go/table"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type mockCatalogForDrop struct {
	catalogType    catalog.Type
	dropCalled     bool
	dropIdent      table.Identifier
	dropErr        error
	checkExists    bool
	checkExistsErr error
}

func (m *mockCatalogForDrop) CatalogType() catalog.Type {
	return m.catalogType
}

func (m *mockCatalogForDrop) CreateTable(ctx context.Context, identifier table.Identifier, schema *iceberg.Schema, opts ...catalog.CreateTableOpt) (*table.Table, error) {
	return nil, nil
}

func (m *mockCatalogForDrop) CommitTable(ctx context.Context, identifier table.Identifier, requirements []table.Requirement, updates []table.Update) (table.Metadata, string, error) {
	return nil, "", nil
}

func (m *mockCatalogForDrop) ListTables(ctx context.Context, namespace table.Identifier) iter.Seq2[table.Identifier, error] {
	return nil
}

func (m *mockCatalogForDrop) LoadTable(ctx context.Context, identifier table.Identifier) (*table.Table, error) {
	return nil, nil
}

func (m *mockCatalogForDrop) DropTable(ctx context.Context, identifier table.Identifier) error {
	m.dropCalled = true
	m.dropIdent = identifier

	return m.dropErr
}

func (m *mockCatalogForDrop) RenameTable(ctx context.Context, from, to table.Identifier) (*table.Table, error) {
	return nil, nil
}

func (m *mockCatalogForDrop) CheckTableExists(ctx context.Context, identifier table.Identifier) (bool, error) {
	return m.checkExists, m.checkExistsErr
}

func (m *mockCatalogForDrop) ListNamespaces(ctx context.Context, parent table.Identifier) ([]table.Identifier, error) {
	return nil, nil
}

func (m *mockCatalogForDrop) CreateNamespace(ctx context.Context, namespace table.Identifier, props iceberg.Properties) error {
	return nil
}

func (m *mockCatalogForDrop) DropNamespace(ctx context.Context, namespace table.Identifier) error {
	return nil
}

func (m *mockCatalogForDrop) CheckNamespaceExists(ctx context.Context, namespace table.Identifier) (bool, error) {
	return false, nil
}

func (m *mockCatalogForDrop) LoadNamespaceProperties(ctx context.Context, namespace table.Identifier) (iceberg.Properties, error) {
	return nil, nil
}

func (m *mockCatalogForDrop) UpdateNamespaceProperties(ctx context.Context, namespace table.Identifier, removals []string, updates iceberg.Properties) (catalog.PropertiesUpdateSummary, error) {
	return catalog.PropertiesUpdateSummary{}, nil
}

type mockPurgeableCatalog struct {
	mockCatalogForDrop
	purgeCalled bool
	purgeIdent  table.Identifier
	purgeErr    error
}

func (m *mockPurgeableCatalog) PurgeTable(ctx context.Context, identifier table.Identifier) error {
	m.purgeCalled = true
	m.purgeIdent = identifier

	return m.purgeErr
}

func TestRunDropTable(t *testing.T) {
	cat := &mockCatalogForDrop{
		catalogType: catalog.SQL,
	}

	cmd := &DropCmd{
		Table: &DropTableCmd{
			Identifier: "db.events",
			Purge:      false,
		},
	}

	var errOut errCapture
	runDrop(context.Background(), &errOut, cat, cmd)

	assert.True(t, cat.dropCalled)
	assert.Equal(t, table.Identifier{"db", "events"}, cat.dropIdent)
	assert.NoError(t, errOut.lastErr)
}

func TestRunDropTablePurgeNotSupported(t *testing.T) {
	cat := &mockCatalogForDrop{
		catalogType: catalog.SQL,
	}

	cmd := &DropCmd{
		Table: &DropTableCmd{
			Identifier: "db.events",
			Purge:      true,
		},
	}

	var errOut errCapture
	exitCode := captureExit(func() {
		runDrop(context.Background(), &errOut, cat, cmd)
	})

	assert.Equal(t, 1, exitCode)
	require.Error(t, errOut.lastErr)
	assert.Contains(t, errOut.lastErr.Error(), "does not support purge")
}

func TestRunDropTablePurgeSupported(t *testing.T) {
	cat := &mockPurgeableCatalog{
		mockCatalogForDrop: mockCatalogForDrop{
			catalogType: catalog.SQL,
		},
	}

	cmd := &DropCmd{
		Table: &DropTableCmd{
			Identifier: "db.events",
			Purge:      true,
		},
	}

	var errOut errCapture
	runDrop(context.Background(), &errOut, cat, cmd)

	assert.True(t, cat.purgeCalled)
	assert.Equal(t, table.Identifier{"db", "events"}, cat.purgeIdent)
	assert.NoError(t, errOut.lastErr)
}

func TestRunDropTableError(t *testing.T) {
	expectedErr := errors.New("some sql error")
	cat := &mockCatalogForDrop{
		catalogType: catalog.SQL,
		dropErr:     expectedErr,
	}

	cmd := &DropCmd{
		Table: &DropTableCmd{
			Identifier: "db.events",
			Purge:      false,
		},
	}

	var errOut errCapture
	exitCode := captureExit(func() {
		runDrop(context.Background(), &errOut, cat, cmd)
	})

	assert.Equal(t, 1, exitCode)
	assert.ErrorIs(t, errOut.lastErr, expectedErr)
}

func TestRunDropTablePurgeError(t *testing.T) {
	expectedErr := errors.New("dropped table but failed to purge files: some error")
	cat := &mockPurgeableCatalog{
		mockCatalogForDrop: mockCatalogForDrop{
			catalogType: catalog.SQL,
		},
		purgeErr: expectedErr,
	}

	cmd := &DropCmd{
		Table: &DropTableCmd{
			Identifier: "db.events",
			Purge:      true,
		},
	}

	var errOut errCapture
	exitCode := captureExit(func() {
		runDrop(context.Background(), &errOut, cat, cmd)
	})

	assert.Equal(t, 1, exitCode)
	assert.True(t, cat.purgeCalled)
	assert.ErrorIs(t, errOut.lastErr, expectedErr)
}
