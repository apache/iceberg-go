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

//go:build integration

package rest_test

import (
	"context"
	"os"
	"testing"

	"github.com/apache/iceberg-go"
	"github.com/apache/iceberg-go/catalog"
	"github.com/apache/iceberg-go/catalog/catalogtest"
	"github.com/stretchr/testify/require"
)

// Environment variables that point the conformance suite at an external REST
// catalog server, alongside the Java REST fixture the suite runs against by
// default. Only REST_CATALOG_URI is required; the others are passed to the
// client when set. CI runs this test against Apache Polaris, which
// internal/recipe/setup-polaris.sh starts; see "make integration-external-rest".
const (
	// externalURIEnv is the catalog base URI, including any path prefix the
	// server needs, for example http://localhost:8191/api/catalog for Polaris.
	// The OAuth token endpoint is derived from it.
	externalURIEnv = "REST_CATALOG_URI"
	// externalCredentialEnv is an OAuth client_id:client_secret pair.
	externalCredentialEnv = "REST_CATALOG_CREDENTIAL"
	// externalScopeEnv is the OAuth scope to request. Polaris, for example,
	// rejects the client's default "catalog" scope and needs PRINCIPAL_ROLE:ALL.
	externalScopeEnv = "REST_CATALOG_SCOPE"
	// externalWarehouseEnv selects the warehouse, or the catalog for servers
	// such as Polaris that host several.
	externalWarehouseEnv = "REST_CATALOG_WAREHOUSE"
)

// TestExternalRestCatalogConformance runs the catalog conformance suite against
// an external REST catalog server. It is opt-in: the test is skipped unless
// REST_CATALOG_URI is set.
func TestExternalRestCatalogConformance(t *testing.T) {
	uri := os.Getenv(externalURIEnv)
	if uri == "" {
		t.Skipf("%s is not set; skipping external REST catalog conformance tests", externalURIEnv)
	}

	props := iceberg.Properties{"type": "rest", "uri": uri}
	if credential := os.Getenv(externalCredentialEnv); credential != "" {
		require.Contains(t, credential, ":", "%s must be a client_id:client_secret pair", externalCredentialEnv)
		props["credential"] = credential
	}
	if scope := os.Getenv(externalScopeEnv); scope != "" {
		props["scope"] = scope
	}
	if warehouse := os.Getenv(externalWarehouseEnv); warehouse != "" {
		props["warehouse"] = warehouse
	}

	catalogtest.RunCatalogTests(t, catalogtest.Config{
		SupportsNamespaceProperties: true,
		SupportsRenameTable:         true,
		NewCatalog: func(t *testing.T) catalog.Catalog {
			cat, err := catalog.Load(context.Background(), "external", props)
			require.NoError(t, err)
			t.Cleanup(func() { require.NoError(t, cat.Close()) })

			return cat
		},
	})
}
