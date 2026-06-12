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

package rest

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestEndpointNegotiation(t *testing.T) {
	// Loading the catalog reads /v1/config and negotiates the endpoint set the
	// fixture advertises. The live server uses the same wire format as our
	// endpoint constants, so the negotiated set must contain the core
	// operations; a format mismatch would surface here as a missing entry
	// rather than as a silent fallback.
	cat, err := NewCatalog(context.Background(), "rest", "http://localhost:8181")
	require.NoError(t, err)

	// Not in the fallback defaults, so its presence means we read the wire list.
	require.Truef(t, cat.endpoints.contains(endpointTableExists),
		"negotiated set must contain advertised %s, not just the fallback defaults", endpointTableExists)
	for _, want := range []endpoint{
		endpointListNamespaces, endpointCreateNamespace,
		endpointListTables, endpointCreateTable, endpointLoadTable,
		endpointDeleteTable, endpointCommitTransaction,
	} {
		assert.Truef(t, cat.endpoints.contains(want), "negotiated set should contain %s", want)
	}
}
