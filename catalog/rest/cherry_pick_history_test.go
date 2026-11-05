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
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package rest_test

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/apache/iceberg-go"
	"github.com/apache/iceberg-go/catalog"
	"github.com/apache/iceberg-go/table"
	"github.com/stretchr/testify/require"
)

// Use the in-memory filesystem so the metadata-only commit stays offline.
// A REST refs-mode load may keep the heads of two branches while omitting
// their common parent (snapshot 10). This ancestry is not safe for a
// duplicate-publication check, even after table or credential refreshes.
const cherryPickRefsMetadata = `{
	"format-version": 1,
	"table-uuid": "b55d9dda-6561-423a-8bfc-787980ce421f",
	"location": "mem://cherry-pick-refs/database/table",
	"last-updated-ms": 1646787054459,
	"last-column-id": 1,
	"schema": {"type": "struct", "schema-id": 0, "fields": [
		{"id": 1, "name": "id", "required": false, "type": "long"}
	]},
	"current-schema-id": 0,
	"schemas": [{"type": "struct", "schema-id": 0, "fields": [
		{"id": 1, "name": "id", "required": false, "type": "long"}
	]}],
	"partition-spec": [],
	"default-spec-id": 0,
	"partition-specs": [{"spec-id": 0, "fields": []}],
	"last-partition-id": 999,
	"default-sort-order-id": 0,
	"sort-orders": [{"order-id": 0, "fields": []}],
	"properties": {},
	"current-snapshot-id": 30,
	"refs": {
		"main": {"snapshot-id": 30, "type": "branch"},
		"source": {"snapshot-id": 20, "type": "branch"}
	},
	"snapshots": [
		{
			"snapshot-id": 20, "parent-snapshot-id": 10,
			"timestamp-ms": 1646787054000,
			"summary": {"operation": "append"},
			"manifest-list": "mem://cherry-pick-refs/database/table/metadata/snap-20.avro",
			"schema-id": 0
		},
		{
			"snapshot-id": 30, "parent-snapshot-id": 10,
			"timestamp-ms": 1646787054459,
			"summary": {"operation": "append"},
			"manifest-list": "mem://cherry-pick-refs/database/table/metadata/snap-30.avro",
			"schema-id": 0
		}
	],
	"snapshot-log": [],
	"metadata-log": []
}`

func TestCherryPickRefsModePersistsAcrossRESTTableLifecycle(t *testing.T) {
	const metadataLoc = "mem://cherry-pick-refs/database/table/metadata/00002.metadata.json"
	respond := func(w http.ResponseWriter) {
		require.NoError(t, json.NewEncoder(w).Encode(map[string]any{
			"metadata-location": metadataLoc,
			"metadata":          json.RawMessage(cherryPickRefsMetadata),
		}))
	}
	cat := newCredsTestCatalog(t, credsCatalogOpts{
		defaults: map[string]string{"snapshot-loading-mode": "refs"},
		loadTable: func(w http.ResponseWriter, req *http.Request) {
			switch req.Method {
			case http.MethodGet:
				require.Equal(t, "refs", req.URL.Query().Get("snapshots"))
			case http.MethodPost:
				// Metadata-only transaction commits use this endpoint.
			default:
				t.Errorf("unexpected table method: %s", req.Method)
				http.Error(w, "unsupported method", http.StatusMethodNotAllowed)

				return
			}
			respond(w)
		},
		creds: func(w http.ResponseWriter, req *http.Request) {
			require.Equal(t, http.MethodGet, req.Method)
			require.NoError(t, json.NewEncoder(w).Encode(storageCredentialsBody(
				"mem://cherry-pick-refs/database/table",
				map[string]string{"s3.access-key-id": "vended-key", "s3.secret-access-key": "vended-secret"},
			)))
		},
	})
	check := func(stage string, tbl *table.Table) {
		t.Helper()
		require.NotNil(t, tbl, stage)
		txn := tbl.NewTransaction()
		err := txn.CherryPickSnapshot(t.Context(), 20, nil)
		require.ErrorIs(t, err, table.ErrInvalidOperation, stage)
		require.ErrorContains(t, err, "snapshot-loading-mode=refs", stage)
	}

	loaded, err := cat.LoadTable(t.Context(), catalog.ToIdentifier("db", "tbl"))
	require.NoError(t, err)
	check("loadTableWithMode", loaded)

	// StagedTable constructs another table over the same partial metadata.
	// It must not make an incomplete-ancestry cherry-pick appear safe.
	staged, err := loaded.NewTransaction().StagedTable()
	require.NoError(t, err)
	check("Transaction.StagedTable", staged.Table)

	require.NoError(t, loaded.Refresh(t.Context()))
	check("Table.Refresh", loaded)

	txn := loaded.NewTransaction()
	require.NoError(t, txn.SetProperties(iceberg.Properties{"refs-mode-check": "yes"}))
	committed, err := txn.Commit(t.Context())
	require.NoError(t, err)
	check("Table.doCommit", committed)

	withCreds, err := cat.RefreshTableCredentials(t.Context(), committed)
	require.NoError(t, err)
	require.NotSame(t, committed, withCreds)
	check("RefreshTableCredentials", withCreds)

	require.NoError(t, withCreds.Refresh(t.Context()))
	check("refresh after credential refresh", withCreds)
}
