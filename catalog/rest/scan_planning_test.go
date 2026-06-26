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

package rest

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/apache/iceberg-go"
	"github.com/apache/iceberg-go/table"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestScanPlanningEndpointConstantsRenderExpectedPaths(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name   string
		ep     endpoint
		params []string
		want   []string
	}{
		{
			name:   "plan table scan",
			ep:     endpointPlanTableScan,
			params: []string{"db", "tbl"},
			want:   []string{"namespaces", "db", "tables", "tbl", "plan"},
		},
		{
			name:   "fetch planning result",
			ep:     endpointFetchPlanResult,
			params: []string{"db", "tbl", "plan-123"},
			want:   []string{"namespaces", "db", "tables", "tbl", "plan", "plan-123"},
		},
		{
			name:   "cancel planning",
			ep:     endpointCancelPlanning,
			params: []string{"db", "tbl", "plan-123"},
			want:   []string{"namespaces", "db", "tables", "tbl", "plan", "plan-123"},
		},
		{
			name:   "fetch scan tasks",
			ep:     endpointFetchScanTasks,
			params: []string{"db", "tbl"},
			want:   []string{"namespaces", "db", "tables", "tbl", "tasks"},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			got, err := tc.ep.reqPath(tc.params...)
			require.NoError(t, err)
			assert.Equal(t, tc.want, got)
		})
	}
}

func TestScanPlanningCapabilities(t *testing.T) {
	t.Parallel()

	t.Run("plan only", func(t *testing.T) {
		t.Parallel()

		cat := &Catalog{endpoints: newEndpointSet([]endpoint{endpointPlanTableScan})}
		assert.True(t, cat.SupportsPlanTableScan())
		assert.False(t, cat.SupportsFullRemoteScanPlanning())
		assert.False(t, cat.SupportsRemoteScanPlanning())
	})

	t.Run("full remote planning", func(t *testing.T) {
		t.Parallel()

		cat := &Catalog{endpoints: newEndpointSet([]endpoint{
			endpointPlanTableScan,
			endpointFetchPlanResult,
			endpointCancelPlanning,
			endpointFetchScanTasks,
		})}
		assert.True(t, cat.SupportsPlanTableScan())
		assert.True(t, cat.SupportsFullRemoteScanPlanning())
		assert.True(t, cat.SupportsRemoteScanPlanning())
	})

	t.Run("default fallback does not advertise scan planning", func(t *testing.T) {
		t.Parallel()

		cat := &Catalog{endpoints: newEndpointSet(defaultEndpoints)}
		assert.False(t, cat.SupportsPlanTableScan())
		assert.False(t, cat.SupportsFullRemoteScanPlanning())
		assert.False(t, cat.SupportsRemoteScanPlanning())
	})
}

func TestPlanTableScanResponseRequiresPlanIDForTrackedStatuses(t *testing.T) {
	t.Parallel()

	for _, status := range []PlanStatus{PlanStatusCompleted, PlanStatusSubmitted} {
		t.Run(string(status), func(t *testing.T) {
			t.Parallel()

			var resp PlanTableScanResponse
			err := json.Unmarshal([]byte(`{"status":"`+string(status)+`"}`), &resp)
			require.ErrorIs(t, err, ErrRESTError)
		})
	}
}

func TestPlanTableScanResponseRejectsCancelled(t *testing.T) {
	t.Parallel()

	var resp PlanTableScanResponse
	err := json.Unmarshal([]byte(`{"status":"cancelled","plan-id":"abc"}`), &resp)
	require.ErrorIs(t, err, ErrRESTError)
}

func TestPlanTableScanResponseAcceptsCompletedWithPlanID(t *testing.T) {
	t.Parallel()

	var resp PlanTableScanResponse
	err := json.Unmarshal([]byte(`{
		"status":"completed",
		"plan-id":"abc",
		"plan-tasks":["next"],
		"file-scan-tasks":[{"data-file":{}}],
		"delete-files":[{"content":"position-deletes"}]
	}`), &resp)
	require.NoError(t, err)

	require.NotNil(t, resp.PlanID)
	assert.Equal(t, "abc", *resp.PlanID)
	assert.Len(t, resp.PlanTasks, 1)
	// TODO(Phase 2): assert decoded task/delete-file content once the
	// scan-task decoder fills RESTFileScanTask and RESTDeleteFile.
	assert.Len(t, resp.FileScanTasks, 1)
	assert.Len(t, resp.DeleteFiles, 1)
}

func TestPlanTableScanResponseRejectsFailedWithoutError(t *testing.T) {
	t.Parallel()

	var resp PlanTableScanResponse
	err := json.Unmarshal([]byte(`{"status":"failed"}`), &resp)
	require.ErrorIs(t, err, ErrRESTError)
}

func TestPlanTableScanResponseAcceptsFailedWithError(t *testing.T) {
	t.Parallel()

	var resp PlanTableScanResponse
	err := json.Unmarshal([]byte(`{"status":"failed","error":{"message":"boom","type":"ServerError","code":500}}`), &resp)
	require.NoError(t, err)
	require.NotNil(t, resp.Error)
	assert.Equal(t, "boom", resp.Error.Message)
}

func TestPlanTableScanResponseRejectsUnknownStatus(t *testing.T) {
	t.Parallel()

	var resp PlanTableScanResponse
	err := json.Unmarshal([]byte(`{"status":"bogus"}`), &resp)
	require.ErrorIs(t, err, ErrRESTError)
}

func TestFetchPlanningResultResponseRejectsMalformedResponses(t *testing.T) {
	t.Parallel()

	t.Run("failed without error", func(t *testing.T) {
		t.Parallel()

		var resp FetchPlanningResultResponse
		err := json.Unmarshal([]byte(`{"status":"failed"}`), &resp)
		require.ErrorIs(t, err, ErrRESTError)
	})

	t.Run("unknown status", func(t *testing.T) {
		t.Parallel()

		var resp FetchPlanningResultResponse
		err := json.Unmarshal([]byte(`{"status":"bogus"}`), &resp)
		require.ErrorIs(t, err, ErrRESTError)
	})
}

func TestPlanTableScanGeneratesIdempotencyKeyAndUsesDefaultAccessDelegation(t *testing.T) {
	t.Parallel()

	cat := newScanPlanningTestCatalog(t, []endpoint{endpointPlanTableScan}, func(mux *http.ServeMux) {
		mux.HandleFunc("/v1/namespaces/db/tables/tbl/plan", func(w http.ResponseWriter, req *http.Request) {
			got := req.Header.Get(headerIdempotencyKey)
			require.NotEmpty(t, got)
			_, err := uuid.Parse(got)
			require.NoError(t, err)
			assert.Equal(t, []string{defaultAccessDelegation}, req.Header.Values(headerIcebergAccessDelegation))

			_, err = w.Write([]byte(`{"status":"completed","plan-id":"plan-1"}`))
			require.NoError(t, err)
		})
	})

	_, err := cat.PlanTableScan(context.Background(), table.Identifier{"db", "tbl"}, PlanTableScanRequest{})
	require.NoError(t, err)
}

func TestPlanTableScanRejectsInvalidIdempotencyKey(t *testing.T) {
	t.Parallel()

	badKey := "not-a-uuid"
	cat := &Catalog{endpoints: newEndpointSet([]endpoint{endpointPlanTableScan})}

	_, err := cat.PlanTableScan(context.Background(), table.Identifier{"db", "tbl"}, PlanTableScanRequest{
		IdempotencyKey: &badKey,
	})
	require.ErrorIs(t, err, iceberg.ErrInvalidArgument)
}

func TestPlanTableScanRequest(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name       string
		response   string
		wantStatus PlanStatus
		wantPlanID *string
		wantError  string
	}{
		{
			name:       "completed",
			response:   `{"status":"completed","plan-id":"plan-1","plan-tasks":["task-1"]}`,
			wantStatus: PlanStatusCompleted,
			wantPlanID: stringPtr("plan-1"),
		},
		{
			name:       "submitted",
			response:   `{"status":"submitted","plan-id":"plan-2"}`,
			wantStatus: PlanStatusSubmitted,
			wantPlanID: stringPtr("plan-2"),
		},
		{
			name:       "failed",
			response:   `{"status":"failed","error":{"message":"boom","type":"ServerError","code":500}}`,
			wantStatus: PlanStatusFailed,
			wantError:  "boom",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			idempotencyKey := "11111111-1111-1111-1111-111111111111"
			accessDelegation := "remote-signing"
			snapshotID := int64(22)
			cat := newScanPlanningTestCatalog(t, []endpoint{endpointPlanTableScan}, func(mux *http.ServeMux) {
				mux.HandleFunc("/v1/namespaces/db/tables/tbl/plan", func(w http.ResponseWriter, req *http.Request) {
					require.Equal(t, http.MethodPost, req.Method)
					assert.Equal(t, idempotencyKey, req.Header.Get(headerIdempotencyKey))
					assert.Equal(t, []string{accessDelegation}, req.Header.Values(headerIcebergAccessDelegation))

					body, err := io.ReadAll(req.Body)
					require.NoError(t, err)
					assert.JSONEq(t, `{
						"snapshot-id": 22,
						"select": ["id", "data"],
						"filter": {"type": "always-true"}
					}`, string(body))

					_, err = w.Write([]byte(tc.response))
					require.NoError(t, err)
				})
			})

			resp, err := cat.PlanTableScan(context.Background(), table.Identifier{"db", "tbl"}, PlanTableScanRequest{
				IdempotencyKey:   &idempotencyKey,
				AccessDelegation: &accessDelegation,
				SnapshotID:       &snapshotID,
				Select:           []string{"id", "data"},
				Filter:           json.RawMessage(`{"type":"always-true"}`),
			})
			require.NoError(t, err)
			assert.Equal(t, tc.wantStatus, resp.Status)
			if tc.wantPlanID != nil {
				require.NotNil(t, resp.PlanID)
				assert.Equal(t, *tc.wantPlanID, *resp.PlanID)
			}
			if tc.wantError != "" {
				require.NotNil(t, resp.Error)
				assert.Equal(t, tc.wantError, resp.Error.Message)
			}
		})
	}
}

func TestFetchPlanningResultRequest(t *testing.T) {
	t.Parallel()

	accessDelegation := "remote-signing"
	cat := newScanPlanningTestCatalog(t, []endpoint{endpointFetchPlanResult}, func(mux *http.ServeMux) {
		mux.HandleFunc("/v1/namespaces/db/tables/tbl/plan/plan-123", func(w http.ResponseWriter, req *http.Request) {
			require.Equal(t, http.MethodGet, req.Method)
			assert.Equal(t, []string{accessDelegation}, req.Header.Values(headerIcebergAccessDelegation))

			_, err := w.Write([]byte(`{"status":"completed","plan-tasks":["task-1"]}`))
			require.NoError(t, err)
		})
	})

	resp, err := cat.FetchPlanningResult(context.Background(), table.Identifier{"db", "tbl"}, "plan-123", FetchPlanningResultOptions{
		AccessDelegation: &accessDelegation,
	})
	require.NoError(t, err)
	assert.Equal(t, PlanStatusCompleted, resp.Status)
	assert.Equal(t, []string{"task-1"}, resp.PlanTasks)
}

func TestFetchPlanningResultUsesDefaultAccessDelegation(t *testing.T) {
	t.Parallel()

	cat := newScanPlanningTestCatalog(t, []endpoint{endpointFetchPlanResult}, func(mux *http.ServeMux) {
		mux.HandleFunc("/v1/namespaces/db/tables/tbl/plan/plan-123", func(w http.ResponseWriter, req *http.Request) {
			require.Equal(t, http.MethodGet, req.Method)
			assert.Equal(t, []string{defaultAccessDelegation}, req.Header.Values(headerIcebergAccessDelegation))

			_, err := w.Write([]byte(`{"status":"submitted"}`))
			require.NoError(t, err)
		})
	})

	resp, err := cat.FetchPlanningResult(context.Background(), table.Identifier{"db", "tbl"}, "plan-123", FetchPlanningResultOptions{})
	require.NoError(t, err)
	assert.Equal(t, PlanStatusSubmitted, resp.Status)
}

func TestFetchPlanningResultMapsNotFoundToPlanExpired(t *testing.T) {
	t.Parallel()

	cat := newScanPlanningTestCatalog(t, []endpoint{endpointFetchPlanResult}, func(mux *http.ServeMux) {
		mux.HandleFunc("/v1/namespaces/db/tables/tbl/plan/expired-plan", func(w http.ResponseWriter, req *http.Request) {
			require.Equal(t, http.MethodGet, req.Method)
			w.WriteHeader(http.StatusNotFound)
		})
	})

	_, err := cat.FetchPlanningResult(context.Background(), table.Identifier{"db", "tbl"}, "expired-plan", FetchPlanningResultOptions{})
	require.ErrorIs(t, err, ErrPlanExpired)
}

func TestCancelPlanningRequest(t *testing.T) {
	t.Parallel()

	cat := newScanPlanningTestCatalog(t, []endpoint{endpointCancelPlanning}, func(mux *http.ServeMux) {
		mux.HandleFunc("/v1/namespaces/db/tables/tbl/plan/plan-123", func(w http.ResponseWriter, req *http.Request) {
			require.Equal(t, http.MethodDelete, req.Method)
			assert.Empty(t, req.Header.Values(headerIdempotencyKey))
			assert.Empty(t, req.Header.Values(headerIcebergAccessDelegation))
			w.WriteHeader(http.StatusNoContent)
		})
	})

	require.NoError(t, cat.CancelPlanning(context.Background(), table.Identifier{"db", "tbl"}, "plan-123"))
}

func TestFetchScanTasksRequest(t *testing.T) {
	t.Parallel()

	idempotencyKey := "22222222-2222-2222-2222-222222222222"
	cat := newScanPlanningTestCatalog(t, []endpoint{endpointFetchScanTasks}, func(mux *http.ServeMux) {
		mux.HandleFunc("/v1/namespaces/db/tables/tbl/tasks", func(w http.ResponseWriter, req *http.Request) {
			require.Equal(t, http.MethodPost, req.Method)
			assert.Equal(t, idempotencyKey, req.Header.Get(headerIdempotencyKey))
			assert.Empty(t, req.Header.Values(headerIcebergAccessDelegation))

			body, err := io.ReadAll(req.Body)
			require.NoError(t, err)
			assert.JSONEq(t, `{"plan-task":"task-1"}`, string(body))

			_, err = w.Write([]byte(`{
				"plan-tasks": ["child-task"],
				"file-scan-tasks": [{}],
				"delete-files": [{}]
			}`))
			require.NoError(t, err)
		})
	})

	resp, err := cat.FetchScanTasks(context.Background(), table.Identifier{"db", "tbl"}, FetchScanTasksRequest{
		IdempotencyKey: &idempotencyKey,
		PlanTask:       "task-1",
	})
	require.NoError(t, err)
	assert.Equal(t, []string{"child-task"}, resp.PlanTasks)
	assert.Len(t, resp.FileScanTasks, 1)
	assert.Len(t, resp.DeleteFiles, 1)
}

func TestScanPlanningEndpointGating(t *testing.T) {
	t.Parallel()

	cat := &Catalog{endpoints: newEndpointSet(defaultEndpoints)}
	ident := table.Identifier{"db", "tbl"}

	_, err := cat.PlanTableScan(context.Background(), ident, PlanTableScanRequest{})
	require.ErrorIs(t, err, ErrEndpointNotSupported)
	assert.NotErrorIs(t, err, ErrRESTError)

	_, err = cat.FetchPlanningResult(context.Background(), ident, "plan-123", FetchPlanningResultOptions{})
	require.ErrorIs(t, err, ErrEndpointNotSupported)
	assert.NotErrorIs(t, err, ErrRESTError)

	err = cat.CancelPlanning(context.Background(), ident, "plan-123")
	require.ErrorIs(t, err, ErrEndpointNotSupported)
	assert.NotErrorIs(t, err, ErrRESTError)

	_, err = cat.FetchScanTasks(context.Background(), ident, FetchScanTasksRequest{PlanTask: "task-1"})
	require.ErrorIs(t, err, ErrEndpointNotSupported)
	assert.NotErrorIs(t, err, ErrRESTError)
}

func newScanPlanningTestCatalog(t *testing.T, endpoints []endpoint, register func(*http.ServeMux)) *Catalog {
	t.Helper()

	mux := http.NewServeMux()
	mux.HandleFunc("/v1/config", func(w http.ResponseWriter, req *http.Request) {
		require.Equal(t, http.MethodGet, req.Method)
		err := json.NewEncoder(w).Encode(map[string]any{
			"defaults":  map[string]any{},
			"overrides": map[string]any{},
			"endpoints": endpointStrings(endpoints),
		})
		require.NoError(t, err)
	})
	if register != nil {
		register(mux)
	}

	srv := httptest.NewServer(mux)
	t.Cleanup(srv.Close)

	cat, err := NewCatalog(context.Background(), "rest", srv.URL)
	require.NoError(t, err)

	return cat
}

func endpointStrings(endpoints []endpoint) []string {
	if endpoints == nil {
		return nil
	}

	out := make([]string, len(endpoints))
	for i, e := range endpoints {
		out[i] = e.String()
	}

	return out
}

func stringPtr(s string) *string { return &s }
