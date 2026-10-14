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

// Environment variables that point the conformance suite at an Apache Polaris
// server, a second REST implementation alongside the Java REST fixture.
// POLARIS_URI must include Polaris's catalog prefix, for example
// http://localhost:8191/api/catalog; the OAuth token endpoint is derived from it.
const (
	polarisURIEnv        = "POLARIS_URI"
	polarisCredentialEnv = "POLARIS_CREDENTIAL"
	polarisWarehouseEnv  = "POLARIS_WAREHOUSE"
)

// polarisScope activates all of the principal's roles. Polaris rejects the
// client's default "catalog" scope.
const polarisScope = "PRINCIPAL_ROLE:ALL"

// TestPolarisCatalogConformance runs the catalog conformance suite against an
// Apache Polaris server. It is opt-in: the test is skipped unless POLARIS_URI
// is set. See CONTRIBUTING.md for how to start a local Polaris server.
func TestPolarisCatalogConformance(t *testing.T) {
	uri := os.Getenv(polarisURIEnv)
	if uri == "" {
		t.Skipf("%s is not set; skipping Polaris conformance tests", polarisURIEnv)
	}

	credential := os.Getenv(polarisCredentialEnv)
	require.NotEmpty(t, credential, "%s must be set to a client_id:client_secret pair", polarisCredentialEnv)

	warehouse := os.Getenv(polarisWarehouseEnv)
	require.NotEmpty(t, warehouse, "%s must be set to the name of a Polaris catalog", polarisWarehouseEnv)

	catalogtest.RunCatalogTests(t, catalogtest.Config{
		SupportsNamespaceProperties: true,
		SupportsRenameTable:         true,
		NewCatalog: func(t *testing.T) catalog.Catalog {
			cat, err := catalog.Load(context.Background(), "polaris", iceberg.Properties{
				"type":       "rest",
				"uri":        uri,
				"credential": credential,
				"scope":      polarisScope,
				"warehouse":  warehouse,
			})
			require.NoError(t, err)
			t.Cleanup(func() {
				if closer, ok := cat.(catalog.Closer); ok {
					require.NoError(t, closer.Close())
				}
			})

			return cat
		},
	})
}
