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

	"github.com/apache/iceberg-go"
	"github.com/apache/iceberg-go/catalog"
	"github.com/apache/iceberg-go/table"
	"github.com/stretchr/testify/assert"
)

type mockCatalogForDescribe struct {
	mockCatalogForDrop

	nsErr  error
	tblErr error
}

func (m *mockCatalogForDescribe) LoadNamespaceProperties(ctx context.Context, namespace table.Identifier) (iceberg.Properties, error) {
	return nil, m.nsErr
}

func (m *mockCatalogForDescribe) LoadTable(ctx context.Context, identifier table.Identifier) (*table.Table, error) {
	return nil, m.tblErr
}

func TestDescribeMissingTableReportsNoSuchTable(t *testing.T) {
	const id = "demo.test"

	// Neither namespace "demo.test" nor table "test" exists.
	cat := &mockCatalogForDescribe{
		nsErr:  catalog.ErrNoSuchNamespace,
		tblErr: catalog.ErrNoSuchTable,
	}
	var out errCapture

	exitCode := captureBranchTagExit(func() {
		describe(context.Background(), &out, cat, id, "any")
	})

	assert.Equal(t, 1, exitCode)
	// The table lookup failed, so report a missing table
	assert.ErrorIs(t, out.lastErr, catalog.ErrNoSuchTable)
	assert.NotErrorIs(t, out.lastErr, catalog.ErrNoSuchNamespace)
	assert.ErrorContains(t, out.lastErr, id)
}
