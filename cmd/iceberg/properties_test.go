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
	"iter"
	"testing"

	"github.com/DataDog/iceberg-go"
	"github.com/DataDog/iceberg-go/catalog"
	"github.com/DataDog/iceberg-go/table"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type mockCatalogForProperties struct {
	tbl            *table.Table
	nsProps        iceberg.Properties
	propsGetCalled bool
}

func (m *mockCatalogForProperties) LoadNamespaceProperties(ctx context.Context, namespace table.Identifier) (iceberg.Properties, error) {
	return m.nsProps, nil
}

func (m *mockCatalogForProperties) LoadTable(ctx context.Context, ident table.Identifier) (*table.Table, error) {
	return m.tbl, nil
}

func (m *mockCatalogForProperties) CatalogType() catalog.Type {
	panic("CatalogType must not be called")
}

func (m *mockCatalogForProperties) CreateTable(context.Context, table.Identifier, *iceberg.Schema, ...catalog.CreateTableOpt) (*table.Table, error) {
	panic("CreateTable must not be called")
}

func (m *mockCatalogForProperties) CommitTable(context.Context, table.Identifier, []table.Requirement, []table.Update) (table.Metadata, string, error) {
	panic("CommitTable must not be called")
}

func (m *mockCatalogForProperties) ListTables(context.Context, table.Identifier) iter.Seq2[table.Identifier, error] {
	panic("ListTables must not be called")
}

func (m *mockCatalogForProperties) DropTable(context.Context, table.Identifier) error {
	panic("DropTable must not be called")
}

func (m *mockCatalogForProperties) RenameTable(context.Context, table.Identifier, table.Identifier) (*table.Table, error) {
	panic("RenameTable must not be called")
}

func (m *mockCatalogForProperties) CheckTableExists(context.Context, table.Identifier) (bool, error) {
	panic("CheckTableExists must not be called")
}

func (m *mockCatalogForProperties) ListNamespaces(context.Context, table.Identifier) ([]table.Identifier, error) {
	panic("ListNamespaces must not be called")
}

func (m *mockCatalogForProperties) CreateNamespace(context.Context, table.Identifier, iceberg.Properties) error {
	panic("CreateNamespace must not be called")
}

func (m *mockCatalogForProperties) DropNamespace(context.Context, table.Identifier) error {
	panic("DropNamespace must not be called")
}

func (m *mockCatalogForProperties) CheckNamespaceExists(context.Context, table.Identifier) (bool, error) {
	panic("CheckNamespaceExists must not be called")
}

func (m *mockCatalogForProperties) UpdateNamespaceProperties(context.Context, table.Identifier, []string, iceberg.Properties) (catalog.PropertiesUpdateSummary, error) {
	panic("UpdateNamespaceProperties must not be called")
}

func TestRunPropertiesGetNamespaceNotFound(t *testing.T) {
	cat := &mockCatalogForProperties{
		nsProps: iceberg.Properties{"location": "s3://bucket/db"},
	}

	cmd := &PropertiesCmd{
		Get: &PropsGetCmd{
			Type:       "namespace",
			Identifier: "db",
			PropName:   "nonexisten",
		},
	}

	var errOut errCapture
	exitCode := captureExit(func() {
		runProperties(context.Background(), &errOut, cat, cmd)
	})

	require.Error(t, errOut.lastErr)
	assert.Equal(t, 1, exitCode)
	assert.Equal(t, "could not find property nonexisten on namespace db", errOut.lastErr.Error())
}

func TestRunPropertiesGetTableNotFound(t *testing.T) {
	meta, err := table.ParseMetadataBytes([]byte(`{
              "format-version": 1,
              "table-uuid": "9c12d441-03fe-4693-9a96-a0705ddf69c1",
              "location": "s3://bucket/test/location",
              "last-updated-ms": 1602638573590,
              "last-column-id": 1,
              "schemas": [{"type": "struct", "schema-id": 0, "fields": [{"id": 1, "name": "x", "required": true, "type": "long"}]}],
              "current-schema-id": 0,
              "partition-specs": [{"spec-id": 0, "fields": []}],
              "default-spec-id": 0,
              "last-partition-id": 999,
              "sort-orders": [{"order-id": 0, "fields": []}],
              "default-sort-order-id": 0
      }`))
	require.NoError(t, err)

	tbl := table.New([]string{"db", "events"}, meta, "", nil, nil)

	cat := &mockCatalogForProperties{
		tbl: tbl,
	}

	cmd := &PropertiesCmd{
		Get: &PropsGetCmd{
			Type:       "table",
			Identifier: "db",
			PropName:   "nonexisten",
		},
	}

	var errOut errCapture
	exitCode := captureExit(func() {
		runProperties(context.Background(), &errOut, cat, cmd)
	})

	require.Error(t, errOut.lastErr)
	assert.Equal(t, 1, exitCode)
	assert.Equal(t, "could not find property nonexisten on table db", errOut.lastErr.Error())
}
