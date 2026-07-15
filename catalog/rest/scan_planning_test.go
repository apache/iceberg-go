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
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"

	"github.com/apache/iceberg-go"
	"github.com/apache/iceberg-go/catalog"
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

func TestScanPlanningEscapesOpaquePlanID(t *testing.T) {
	t.Parallel()

	cat := newScanPlanningTestCatalog(t, []endpoint{endpointFetchPlanResult}, nil)

	// baseURI.JoinPath path.Cleans its segments, so scanPlanningPath must escape
	// an opaque plan-id containing '/', a dot segment, or traversal into a single
	// literal segment; otherwise "a/b" splits and ".." / "../tasks" resolve to a
	// different endpoint. Each must survive JoinPath as the final .../plan/ segment
	// and round-trip.
	for _, planID := range []string{"a/b", "../tasks", "a b", "a%2Fb", ".", "..", "plan-123"} {
		path, err := cat.scanPlanningPath(endpointFetchPlanResult, table.Identifier{"db", "tbl"}, planID)
		require.NoErrorf(t, err, "plan-id %q", planID)

		seg := path[len(path)-1]
		assert.NotContainsf(t, seg, "/", "plan-id %q split into multiple segments: %q", planID, seg)

		decoded, err := url.PathUnescape(seg)
		require.NoErrorf(t, err, "plan-id %q", planID)
		assert.Equalf(t, planID, decoded, "plan-id %q did not round-trip", planID)

		// The escaped segment must survive JoinPath's path.Clean as the last
		// .../plan/ segment (no split, no traversal to a different endpoint).
		escaped := cat.baseURI.JoinPath(path...).EscapedPath()
		assert.Truef(t, strings.HasSuffix(escaped, "/plan/"+seg),
			"plan-id %q was mangled by JoinPath: %s", planID, escaped)
	}
}

func TestScanPlanningCapabilities(t *testing.T) {
	t.Parallel()

	t.Run("plan only", func(t *testing.T) {
		t.Parallel()

		// A plan-only server can plan inline, but a `submitted` reply could not
		// be polled, so it is not full-remote capable. SupportsRemoteScanPlanning
		// is false here too (gated off while PlanFiles is a stub).
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
		// SupportsRemoteScanPlanning stays false while PlanFiles is a stub, even
		// when all four endpoints are advertised, so auto mode falls back to
		// local instead of routing into ErrNotImplemented. Flips on with the
		// PlanFiles phase.
		assert.False(t, cat.SupportsRemoteScanPlanning())
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

func TestPlanTableScanResponseAcceptsFailedWithoutUsableError(t *testing.T) {
	t.Parallel()

	for _, payload := range []string{
		`{"status":"failed"}`,
		`{"status":"failed","error":null}`,
		`{"status":"failed","error":"oops"}`,
	} {
		var resp PlanTableScanResponse
		require.NoError(t, json.Unmarshal([]byte(payload), &resp))
		assert.Equal(t, PlanStatusFailed, resp.Status)
		assert.Nil(t, resp.Error)
	}
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

func TestFetchPlanningResultResponseValidation(t *testing.T) {
	t.Parallel()

	t.Run("failed without usable error is accepted", func(t *testing.T) {
		t.Parallel()

		for _, payload := range []string{
			`{"status":"failed"}`,
			`{"status":"failed","error":null}`,
			`{"status":"failed","error":"oops"}`,
		} {
			var resp FetchPlanningResultResponse
			require.NoError(t, json.Unmarshal([]byte(payload), &resp))
			assert.Equal(t, PlanStatusFailed, resp.Status)
			assert.Nil(t, resp.Error)
		}
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
			parsed, err := uuid.Parse(got)
			require.NoError(t, err)
			// The spec pins the generated key to UUIDv7.
			assert.Equal(t, 7, int(parsed.Version()))
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

	for _, tc := range []struct {
		name string
		key  string
	}{
		{"not a uuid", "not-a-uuid"},
		// Valid UUID, but v1 not v7 — the spec pins the header to UUIDv7.
		{"valid uuid wrong version", "11111111-1111-1111-1111-111111111111"},
		// uuid.Parse accepts these non-canonical encodings of a valid v7 UUID,
		// but the spec's header schema requires the 36-char hyphenated form.
		{"unhyphenated v7", "0190b6c51c3d70008000000000000001"},
		{"urn v7", "urn:uuid:0190b6c5-1c3d-7000-8000-000000000001"},
		{"braced v7", "{0190b6c5-1c3d-7000-8000-000000000001}"},
		// Canonical with the version-7 nibble but a non-RFC-4122 variant (the
		// 4th group's leading nibble is 0 -> reserved/NCS, not 8-b).
		{"v7 non-rfc variant", "0190b6c5-1c3d-7000-0000-000000000001"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			key := tc.key
			cat := &Catalog{endpoints: newEndpointSet([]endpoint{endpointPlanTableScan})}

			_, err := cat.PlanTableScan(context.Background(), table.Identifier{"db", "tbl"}, PlanTableScanRequest{
				IdempotencyKey: &key,
			})
			require.ErrorIs(t, err, iceberg.ErrInvalidArgument)
		})
	}
}

func TestPlanTableScanAcceptsUppercaseCanonicalIdempotencyKey(t *testing.T) {
	t.Parallel()

	// Canonical hyphenated form is required case-insensitively; an uppercase v7
	// key is accepted and forwarded verbatim.
	key := "0190B6C5-1C3D-7000-8000-000000000001"
	cat := newScanPlanningTestCatalog(t, []endpoint{endpointPlanTableScan}, func(mux *http.ServeMux) {
		mux.HandleFunc("/v1/namespaces/db/tables/tbl/plan", func(w http.ResponseWriter, req *http.Request) {
			assert.Equal(t, key, req.Header.Get(headerIdempotencyKey))
			_, err := w.Write([]byte(`{"status":"completed","plan-id":"plan-1"}`))
			require.NoError(t, err)
		})
	})

	_, err := cat.PlanTableScan(context.Background(), table.Identifier{"db", "tbl"}, PlanTableScanRequest{
		IdempotencyKey: &key,
	})
	require.NoError(t, err)
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

			idempotencyKey := "0190b6c5-1c3d-7000-8000-000000000001"
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
			if tc.wantError != "" {
				// A failed plan returns a *PlanFailedError and a zero resp; the
				// detail rides on the error.
				require.ErrorIs(t, err, ErrPlanFailed)
				var pfe *PlanFailedError
				require.ErrorAs(t, err, &pfe)
				require.NotNil(t, pfe.Detail)
				assert.Equal(t, tc.wantError, pfe.Detail.Message)
				assert.Equal(t, PlanTableScanResponse{}, resp)

				return
			}

			require.NoError(t, err)
			assert.Equal(t, tc.wantStatus, resp.Status)
			if tc.wantPlanID != nil {
				require.NotNil(t, resp.PlanID)
				assert.Equal(t, *tc.wantPlanID, *resp.PlanID)
			}
		})
	}
}

func TestPlanTableScanRejectsEmptyBody(t *testing.T) {
	t.Parallel()

	// A 200 with an empty body skips JSON decoding (doPost short-circuits on
	// Content-Length 0), bypassing the response UnmarshalJSON validation. Without
	// a status guard this would return a zero response with a nil PlanID as
	// success; assert it is rejected instead.
	cat := newScanPlanningTestCatalog(t, []endpoint{endpointPlanTableScan}, func(mux *http.ServeMux) {
		mux.HandleFunc("/v1/namespaces/db/tables/tbl/plan", func(w http.ResponseWriter, req *http.Request) {
			require.Equal(t, http.MethodPost, req.Method)
			w.Header().Set("Content-Length", "0")
			w.WriteHeader(http.StatusOK)
		})
	})

	resp, err := cat.PlanTableScan(context.Background(), table.Identifier{"db", "tbl"}, PlanTableScanRequest{})
	require.ErrorIs(t, err, ErrRESTError)
	assert.Equal(t, PlanTableScanResponse{}, resp)
}

func TestFetchScanTasksRejectsEmptyBody(t *testing.T) {
	t.Parallel()

	// FetchScanTasks has no status discriminator, so an empty 200 (doPost
	// short-circuits on Content-Length 0) would otherwise decode to a zero,
	// task-less response and read as a successfully completed empty scan,
	// silently dropping work. requireBody rejects it.
	key := "0190b6c5-1c3d-7000-8000-000000000004"
	cat := newScanPlanningTestCatalog(t, []endpoint{endpointFetchScanTasks}, func(mux *http.ServeMux) {
		mux.HandleFunc("/v1/namespaces/db/tables/tbl/tasks", func(w http.ResponseWriter, req *http.Request) {
			require.Equal(t, http.MethodPost, req.Method)
			w.Header().Set("Content-Length", "0")
			w.WriteHeader(http.StatusOK)
		})
	})

	resp, err := cat.FetchScanTasks(context.Background(), table.Identifier{"db", "tbl"}, FetchScanTasksRequest{
		IdempotencyKey: &key,
		PlanTask:       "task-1",
	})
	require.ErrorIs(t, err, ErrRESTError)
	assert.Equal(t, FetchScanTasksResponse{}, resp)
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

func TestFetchPlanningResultMapsNotFound(t *testing.T) {
	t.Parallel()

	// The GET .../plan/{plan-id} 404 splits on error.type so the poller can tell
	// retry-with-a-new-plan (expired plan-id) from abort (table/namespace gone).
	// A bare or unrecognized 404 is ambiguous and stays ErrRESTError rather than
	// being guessed as an expiry (which would make a poller retry a gone table).
	cases := []struct {
		name    string
		errType string // empty => bare 404 with no body
		wantErr error
		notErr  error
	}{
		{"bare 404", "", ErrRESTError, ErrPlanExpired},
		{"unrecognized type", "SomeFutureException", ErrRESTError, ErrPlanExpired},
		{"no such plan-id", errTypeNoSuchPlanID, ErrPlanExpired, catalog.ErrNoSuchTable},
		{"no such table", errTypeNoSuchTable, catalog.ErrNoSuchTable, ErrPlanExpired},
		{"no such namespace", errTypeNoSuchNamespace, catalog.ErrNoSuchNamespace, ErrPlanExpired},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			cat := newScanPlanningTestCatalog(t, []endpoint{endpointFetchPlanResult}, func(mux *http.ServeMux) {
				mux.HandleFunc("/v1/namespaces/db/tables/tbl/plan/plan-123", func(w http.ResponseWriter, req *http.Request) {
					require.Equal(t, http.MethodGet, req.Method)
					writeRESTNotFound(t, w, tc.errType)
				})
			})

			_, err := cat.FetchPlanningResult(context.Background(), table.Identifier{"db", "tbl"}, "plan-123", FetchPlanningResultOptions{})
			require.ErrorIs(t, err, tc.wantErr)
			if tc.notErr != nil {
				assert.NotErrorIs(t, err, tc.notErr)
			}
		})
	}
}

func TestFetchScanTasksMapsNotFound(t *testing.T) {
	t.Parallel()

	// The POST .../tasks 404 splits on error.type so a fanout caller can tell an
	// expired plan-task handle from the table/namespace having vanished. A bare
	// or unrecognized 404 stays an ambiguous ErrRESTError.
	cases := []struct {
		name    string
		errType string // empty => bare 404 with no body
		wantErr error
		notErr  error
	}{
		{"bare 404", "", ErrRESTError, ErrNoSuchPlanTask},
		{"unrecognized type", "SomeFutureException", ErrRESTError, ErrNoSuchPlanTask},
		{"no such plan-task", errTypeNoSuchPlanTask, ErrNoSuchPlanTask, catalog.ErrNoSuchTable},
		{"no such table", errTypeNoSuchTable, catalog.ErrNoSuchTable, ErrNoSuchPlanTask},
		{"no such namespace", errTypeNoSuchNamespace, catalog.ErrNoSuchNamespace, ErrNoSuchPlanTask},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			idempotencyKey := "0190b6c5-1c3d-7000-8000-000000000003"
			cat := newScanPlanningTestCatalog(t, []endpoint{endpointFetchScanTasks}, func(mux *http.ServeMux) {
				mux.HandleFunc("/v1/namespaces/db/tables/tbl/tasks", func(w http.ResponseWriter, req *http.Request) {
					require.Equal(t, http.MethodPost, req.Method)
					writeRESTNotFound(t, w, tc.errType)
				})
			})

			_, err := cat.FetchScanTasks(context.Background(), table.Identifier{"db", "tbl"}, FetchScanTasksRequest{
				IdempotencyKey: &idempotencyKey,
				PlanTask:       "task-1",
			})
			require.ErrorIs(t, err, tc.wantErr)
			if tc.notErr != nil {
				assert.NotErrorIs(t, err, tc.notErr)
			}
		})
	}
}

func TestPlanTableScanMapsNotFound(t *testing.T) {
	t.Parallel()

	// The POST .../plan 404 carries no plan-id, so a recognized 404 splits only
	// into a gone table vs namespace; a bare or unrecognized 404 stays an
	// ambiguous ErrRESTError rather than being guessed as a missing table.
	cases := []struct {
		name    string
		errType string // empty => bare 404 with no body
		wantErr error
		notErr  error
	}{
		{"bare 404", "", ErrRESTError, catalog.ErrNoSuchTable},
		{"unrecognized type", "SomeFutureException", ErrRESTError, catalog.ErrNoSuchTable},
		{"no such table", errTypeNoSuchTable, catalog.ErrNoSuchTable, catalog.ErrNoSuchNamespace},
		{"no such namespace", errTypeNoSuchNamespace, catalog.ErrNoSuchNamespace, catalog.ErrNoSuchTable},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			cat := newScanPlanningTestCatalog(t, []endpoint{endpointPlanTableScan}, func(mux *http.ServeMux) {
				mux.HandleFunc("/v1/namespaces/db/tables/tbl/plan", func(w http.ResponseWriter, req *http.Request) {
					require.Equal(t, http.MethodPost, req.Method)
					writeRESTNotFound(t, w, tc.errType)
				})
			})

			_, err := cat.PlanTableScan(context.Background(), table.Identifier{"db", "tbl"}, PlanTableScanRequest{})
			require.ErrorIs(t, err, tc.wantErr)
			if tc.notErr != nil {
				assert.NotErrorIs(t, err, tc.notErr)
			}
		})
	}
}

func TestFetchPlanningResultResponseAcceptsCancelled(t *testing.T) {
	t.Parallel()

	// cancelled is a valid poll result (unlike PlanTableScanResponse, which
	// rejects it). Pin it so a refactor routing it into the default error case
	// is caught.
	var resp FetchPlanningResultResponse
	require.NoError(t, json.Unmarshal([]byte(`{"status":"cancelled"}`), &resp))
	assert.Equal(t, PlanStatusCancelled, resp.Status)
}

func TestFetchPlanningResultStatusArms(t *testing.T) {
	t.Parallel()

	t.Run("cancelled returns ErrPlanCancelled", func(t *testing.T) {
		t.Parallel()

		cat := newScanPlanningTestCatalog(t, []endpoint{endpointFetchPlanResult}, func(mux *http.ServeMux) {
			mux.HandleFunc("/v1/namespaces/db/tables/tbl/plan/plan-123", func(w http.ResponseWriter, req *http.Request) {
				_, err := w.Write([]byte(`{"status":"cancelled"}`))
				require.NoError(t, err)
			})
		})

		_, err := cat.FetchPlanningResult(context.Background(), table.Identifier{"db", "tbl"}, "plan-123", FetchPlanningResultOptions{})
		require.ErrorIs(t, err, ErrPlanCancelled)
	})

	t.Run("failed returns PlanFailedError", func(t *testing.T) {
		t.Parallel()

		cat := newScanPlanningTestCatalog(t, []endpoint{endpointFetchPlanResult}, func(mux *http.ServeMux) {
			mux.HandleFunc("/v1/namespaces/db/tables/tbl/plan/plan-123", func(w http.ResponseWriter, req *http.Request) {
				_, err := w.Write([]byte(`{"status":"failed","error":{"message":"boom","type":"ServerError","code":500}}`))
				require.NoError(t, err)
			})
		})

		_, err := cat.FetchPlanningResult(context.Background(), table.Identifier{"db", "tbl"}, "plan-123", FetchPlanningResultOptions{})
		require.ErrorIs(t, err, ErrPlanFailed)
		var pfe *PlanFailedError
		require.ErrorAs(t, err, &pfe)
		require.NotNil(t, pfe.Detail)
		assert.Equal(t, "boom", pfe.Detail.Message)
	})

	t.Run("failed without usable error still returns PlanFailedError", func(t *testing.T) {
		t.Parallel()

		for _, payload := range []string{
			`{"status":"failed"}`,
			`{"status":"failed","error":"oops"}`,
		} {
			cat := newScanPlanningTestCatalog(t, []endpoint{endpointFetchPlanResult}, func(mux *http.ServeMux) {
				mux.HandleFunc("/v1/namespaces/db/tables/tbl/plan/plan-123", func(w http.ResponseWriter, req *http.Request) {
					_, err := w.Write([]byte(payload))
					require.NoError(t, err)
				})
			})

			_, err := cat.FetchPlanningResult(context.Background(), table.Identifier{"db", "tbl"}, "plan-123", FetchPlanningResultOptions{})
			require.ErrorIs(t, err, ErrPlanFailed)
			var pfe *PlanFailedError
			require.ErrorAs(t, err, &pfe)
			assert.Nil(t, pfe.Detail)
		}
	})
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

	idempotencyKey := "0190b6c5-1c3d-7000-8000-000000000002"
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

	ident := table.Identifier{"db", "tbl"}
	cases := []struct {
		name string
		call func(*Catalog) error
	}{
		{"plan", func(c *Catalog) error {
			_, err := c.PlanTableScan(context.Background(), ident, PlanTableScanRequest{})

			return err
		}},
		{"fetch-result", func(c *Catalog) error {
			_, err := c.FetchPlanningResult(context.Background(), ident, "plan-123", FetchPlanningResultOptions{})

			return err
		}},
		{"cancel", func(c *Catalog) error {
			return c.CancelPlanning(context.Background(), ident, "plan-123")
		}},
		{"fetch-tasks", func(c *Catalog) error {
			_, err := c.FetchScanTasks(context.Background(), ident, FetchScanTasksRequest{PlanTask: "task-1"})

			return err
		}},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			cat := &Catalog{endpoints: newEndpointSet(defaultEndpoints)}
			err := tc.call(cat)
			require.ErrorIs(t, err, ErrEndpointNotSupported)
			assert.NotErrorIs(t, err, ErrRESTError)
		})
	}
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

// writeRESTNotFound writes a 404 response. A non-empty errType emits a REST
// ErrorModel body ({"error":{...,"type":errType}}) so the client can split the
// 404 on error.type; an empty errType writes a bare 404 with no body (the
// fallback path).
func writeRESTNotFound(t *testing.T, w http.ResponseWriter, errType string) {
	t.Helper()

	w.WriteHeader(http.StatusNotFound)
	if errType == "" {
		return
	}

	_, err := fmt.Fprintf(w, `{"error":{"message":%q,"type":%q,"code":404}}`, errType, errType)
	require.NoError(t, err)
}

func stringPtr(s string) *string { return &s }
