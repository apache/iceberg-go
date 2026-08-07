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
	"testing"

	"github.com/DataDog/iceberg-go"
	"github.com/DataDog/iceberg-go/catalog"
	"github.com/DataDog/iceberg-go/table"
	"github.com/stretchr/testify/assert"
)

type mockCatalogForCreate struct {
	mockCatalogForDrop

	createNamespaceCalled bool
	createNamespaceIdent  table.Identifier
	createNamespaceProps  iceberg.Properties
	createNamespaceErr    error
}

func (m *mockCatalogForCreate) CatalogType() catalog.Type {
	return catalog.REST
}

func (m *mockCatalogForCreate) CreateNamespace(_ context.Context, namespace table.Identifier, props iceberg.Properties) error {
	m.createNamespaceCalled = true
	m.createNamespaceIdent = namespace
	m.createNamespaceProps = props

	return m.createNamespaceErr
}

func (m *mockCatalogForCreate) CreateTable(context.Context, table.Identifier, *iceberg.Schema, ...catalog.CreateTableOpt) (*table.Table, error) {
	panic("CreateTable must not be called")
}

func TestRunCreateNamespaceUsesCanonicalLowercaseProps(t *testing.T) {
	cat := &mockCatalogForCreate{}

	cmd := &CreateCmd{
		Namespace: &CreateNamespaceCmd{
			Identifier:  "db",
			Description: "Test Description",
			LocationURI: "s3://test-location",
		},
	}

	var out errCapture

	runCreate(context.Background(), &out, cat, cmd)

	assert.True(t, cat.createNamespaceCalled)
	assert.Equal(t, table.Identifier{"db"}, cat.createNamespaceIdent)
	assert.Equal(t, iceberg.Properties{
		"comment":  "Test Description",
		"location": "s3://test-location",
	}, cat.createNamespaceProps)
	assert.Equal(t, "Namespace db created successfully", out.lastText)
	assert.NoError(t, out.lastErr)
}
