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
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

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

		// A plan-only server can plan inline even though it cannot handle response
		// shapes that require polling or task expansion.
		cat := &Catalog{endpoints: newEndpointSet([]endpoint{endpointPlanTableScan})}
		assert.True(t, cat.SupportsPlanTableScan())
		assert.False(t, cat.SupportsFullRemoteScanPlanning())
		assert.True(t, cat.SupportsRemoteScanPlanning())
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

	t.Run("execution endpoints without optional cancel", func(t *testing.T) {
		t.Parallel()

		cat := &Catalog{endpoints: newEndpointSet([]endpoint{
			endpointPlanTableScan,
			endpointFetchPlanResult,
			endpointFetchScanTasks,
		})}
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
	require.Len(t, resp.FileScanTasks, 1)
	require.NotNil(t, resp.FileScanTasks[0].DataFile)
	require.Len(t, resp.DeleteFiles, 1)
	assert.Equal(t, "position-deletes", resp.DeleteFiles[0].Content)
}

func TestPlanTableScanResponseRejectsInvalidStatusEnvelope(t *testing.T) {
	t.Parallel()

	for i, payload := range []string{
		`{"status":"submitted","plan-id":"abc","file-scan-tasks":[]}`,
		`{"status":"submitted","plan-id":"abc","file-scan-tasks":null}`,
		`{"status":"submitted","plan-id":"abc","delete-files":[]}`,
		`{"status":"submitted","plan-id":"abc","delete-files":null}`,
		`{"status":"failed","plan-tasks":[]}`,
		`{"status":"failed","plan-tasks":null}`,
		`{"status":"completed","plan-id":"abc","delete-files":[{}]}`,
		`{"status":"completed","plan-id":"abc","plan-tasks":null}`,
		`{"status":"completed","plan-id":"abc","file-scan-tasks":null}`,
		`{"status":"completed","plan-id":"abc","delete-files":null}`,
		`{"status":"failed","plan-id":"abc"}`,
	} {
		t.Run(fmt.Sprintf("payload-%d", i), func(t *testing.T) {
			t.Parallel()

			var resp PlanTableScanResponse
			require.ErrorIs(t, json.Unmarshal([]byte(payload), &resp), ErrRESTError, payload)
		})
	}
}

func TestPlanTableScanResponseAcceptsFailedWithoutUsableError(t *testing.T) {
	t.Parallel()

	for i, payload := range []string{
		`{"status":"failed"}`,
		`{"status":"failed","error":null}`,
		`{"status":"failed","error":"oops"}`,
	} {
		t.Run(fmt.Sprintf("payload-%d", i), func(t *testing.T) {
			t.Parallel()

			var resp PlanTableScanResponse
			require.NoError(t, json.Unmarshal([]byte(payload), &resp))
			assert.Equal(t, PlanStatusFailed, resp.Status)
			assert.Nil(t, resp.Error)
		})
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

func TestPlanTableScanResponseRejectsErrorOnNonFailedStatus(t *testing.T) {
	t.Parallel()

	for i, payload := range []string{
		`{"status":"completed","plan-id":"abc","error":"oops"}`,
		`{"status":"submitted","plan-id":"abc","error":[]}`,
	} {
		t.Run(fmt.Sprintf("payload-%d", i), func(t *testing.T) {
			t.Parallel()

			var resp PlanTableScanResponse
			require.ErrorIs(t, json.Unmarshal([]byte(payload), &resp), ErrRESTError, payload)
		})
	}
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

		for i, payload := range []string{
			`{"status":"failed"}`,
			`{"status":"failed","error":null}`,
			`{"status":"failed","error":"oops"}`,
		} {
			t.Run(fmt.Sprintf("payload-%d", i), func(t *testing.T) {
				t.Parallel()

				var resp FetchPlanningResultResponse
				require.NoError(t, json.Unmarshal([]byte(payload), &resp))
				assert.Equal(t, PlanStatusFailed, resp.Status)
				assert.Nil(t, resp.Error)
			})
		}
	})

	t.Run("unknown status", func(t *testing.T) {
		t.Parallel()

		var resp FetchPlanningResultResponse
		err := json.Unmarshal([]byte(`{"status":"bogus"}`), &resp)
		require.ErrorIs(t, err, ErrRESTError)
	})

	t.Run("rejects task fields before completion", func(t *testing.T) {
		t.Parallel()

		for i, payload := range []string{
			`{"status":"submitted","plan-tasks":[]}`,
			`{"status":"submitted","plan-tasks":null}`,
			`{"status":"submitted","delete-files":[]}`,
			`{"status":"submitted","delete-files":null}`,
			`{"status":"cancelled","file-scan-tasks":[]}`,
			`{"status":"cancelled","file-scan-tasks":null}`,
			`{"status":"completed","delete-files":[{}]}`,
			`{"status":"completed","plan-tasks":null}`,
			`{"status":"completed","file-scan-tasks":null}`,
			`{"status":"completed","delete-files":null}`,
		} {
			t.Run(fmt.Sprintf("payload-%d", i), func(t *testing.T) {
				t.Parallel()

				var resp FetchPlanningResultResponse
				require.ErrorIs(t, json.Unmarshal([]byte(payload), &resp), ErrRESTError, payload)
			})
		}
	})

	t.Run("rejects error on non-failed status", func(t *testing.T) {
		t.Parallel()

		for i, payload := range []string{
			`{"status":"completed","error":"oops"}`,
			`{"status":"submitted","error":[]}`,
			`{"status":"cancelled","error":{}}`,
		} {
			t.Run(fmt.Sprintf("payload-%d", i), func(t *testing.T) {
				t.Parallel()

				var resp FetchPlanningResultResponse
				require.ErrorIs(t, json.Unmarshal([]byte(payload), &resp), ErrRESTError, payload)
			})
		}
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

func TestFetchScanTasksRejectsNullBody(t *testing.T) {
	t.Parallel()

	key := "0190b6c5-1c3d-7000-8000-000000000005"
	cat := newScanPlanningTestCatalog(t, []endpoint{endpointFetchScanTasks}, func(mux *http.ServeMux) {
		mux.HandleFunc("/v1/namespaces/db/tables/tbl/tasks", func(w http.ResponseWriter, req *http.Request) {
			require.Equal(t, http.MethodPost, req.Method)
			_, err := w.Write([]byte("null"))
			require.NoError(t, err)
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

// TestPlanTableScanRequestFromEncodesFilter checks the row filter serializes to
// ExpressionParser JSON on the wire request, and that trivial filters are
// dropped so the server plans without one.
func TestPlanTableScanRequestFromEncodesFilter(t *testing.T) {
	t.Parallel()

	meta := scanTestMetadata{schema: scanFilterSchema()}

	t.Run("predicate", func(t *testing.T) {
		t.Parallel()

		wire, err := planTableScanRequestFrom(table.ScanPlanningRequest{
			Metadata:  meta,
			RowFilter: iceberg.EqualTo(iceberg.Reference("i"), int32(25)),
		})
		require.NoError(t, err)
		assert.JSONEq(t, `{"type":"eq","term":"i","value":25}`, string(wire.Filter))
	})

	// Only serializable once bound: binding to the timestamptz field is what adds
	// the +00:00 offset a bare timestamp literal can't pick on its own.
	t.Run("timestamp predicate", func(t *testing.T) {
		t.Parallel()

		lit, err := iceberg.NewLiteral("2022-08-14T10:00:00").To(iceberg.PrimitiveTypes.Timestamp)
		require.NoError(t, err)

		wire, err := planTableScanRequestFrom(table.ScanPlanningRequest{
			Metadata:  meta,
			RowFilter: iceberg.LiteralPredicate(iceberg.OpEQ, iceberg.Reference("ts"), lit),
		})
		require.NoError(t, err)
		assert.JSONEq(t, `{"type":"eq","term":"ts","value":"2022-08-14T10:00:00+00:00"}`, string(wire.Filter))
	})

	// A filter that can't bind is a loud error, not a dropped or bad filter.
	t.Run("unbindable filter errors", func(t *testing.T) {
		t.Parallel()

		_, err := planTableScanRequestFrom(table.ScanPlanningRequest{
			Metadata:  meta,
			RowFilter: iceberg.EqualTo(iceberg.Reference("nonesuch"), int32(1)),
		})
		require.ErrorIs(t, err, iceberg.ErrInvalidArgument)
	})

	for _, tt := range []struct {
		name   string
		filter iceberg.BooleanExpression
	}{
		{"nil", nil},
		{"always true", iceberg.AlwaysTrue{}},
	} {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			wire, err := planTableScanRequestFrom(table.ScanPlanningRequest{RowFilter: tt.filter})
			require.NoError(t, err)
			assert.Nil(t, wire.Filter)
		})
	}
}

// TestPlanTableScanRequestFromPassesFields checks the non-filter scan fields
// carry through to the wire request unchanged.
func TestPlanTableScanRequestFromPassesFields(t *testing.T) {
	t.Parallel()

	snap := int64(42)
	caseSensitive := false
	wire, err := planTableScanRequestFrom(table.ScanPlanningRequest{
		SnapshotID:     &snap,
		SelectedFields: []string{"a", "b"},
		CaseSensitive:  &caseSensitive,
		StatsFields:    []string{"a"},
	})
	require.NoError(t, err)
	assert.Equal(t, &snap, wire.SnapshotID)
	assert.Equal(t, []string{"a", "b"}, wire.Select)
	assert.Equal(t, &caseSensitive, wire.CaseSensitive)
	assert.Equal(t, []string{"a"}, wire.StatsFields)
	assert.Nil(t, wire.Filter)

	wire, err = planTableScanRequestFrom(table.ScanPlanningRequest{SelectedFields: []string{"*"}})
	require.NoError(t, err)
	assert.Nil(t, wire.Select, "the local wildcard sentinel is not a REST FieldName")
}

// scanFilterSchema is the schema filter-binding tests resolve references against.
func scanFilterSchema() *iceberg.Schema {
	return iceberg.NewSchema(0,
		iceberg.NestedField{ID: 1, Name: "i", Type: iceberg.PrimitiveTypes.Int32},
		iceberg.NestedField{ID: 2, Name: "ts", Type: iceberg.PrimitiveTypes.TimestampTz},
	)
}

// scanTestMetadata is a ScanPlanningMetadata carrying only a schema — all filter
// encoding needs.
type scanTestMetadata struct{ schema *iceberg.Schema }

func (m scanTestMetadata) CurrentSchema() *iceberg.Schema               { return m.schema }
func (m scanTestMetadata) Schemas() []*iceberg.Schema                   { return []*iceberg.Schema{m.schema} }
func (m scanTestMetadata) PartitionSpec() iceberg.PartitionSpec         { return iceberg.PartitionSpec{} }
func (m scanTestMetadata) PartitionSpecByID(int) *iceberg.PartitionSpec { return nil }
func (m scanTestMetadata) CurrentSnapshot() *table.Snapshot             { return nil }
func (m scanTestMetadata) SnapshotByID(int64) *table.Snapshot           { return nil }
func (m scanTestMetadata) Properties() iceberg.Properties               { return nil }

type scanPlanningSchemaMetadata struct {
	*scanTaskDecoderMetadata
	current *iceberg.Schema
	schemas []*iceberg.Schema
}

func (m scanPlanningSchemaMetadata) CurrentSchema() *iceberg.Schema { return m.current }
func (m scanPlanningSchemaMetadata) Schemas() []*iceberg.Schema     { return m.schemas }

// planFilesReq is a minimal planner request naming the test table.
func planFilesReq() table.ScanPlanningRequest {
	return table.ScanPlanningRequest{
		Identifier: table.Identifier{"db", "tbl"},
		Metadata:   scanTestMetadata{schema: scanFilterSchema()},
	}
}

// TestPlanFilesCompletedEmpty covers an inline-completed plan with no tasks: it
// returns cleanly with no tasks and no plan-scoped IO (the scan then uses the
// table's own FileIO).
func TestPlanFilesCompletedEmpty(t *testing.T) {
	t.Parallel()

	cat := newScanPlanningTestCatalog(t, []endpoint{endpointPlanTableScan}, func(mux *http.ServeMux) {
		mux.HandleFunc("/v1/namespaces/db/tables/tbl/plan", func(w http.ResponseWriter, req *http.Request) {
			_, err := w.Write([]byte(`{"status":"completed","plan-id":"plan-1"}`))
			require.NoError(t, err)
		})
	})

	result, err := cat.PlanFiles(context.Background(), planFilesReq())
	require.NoError(t, err)
	assert.Empty(t, result.Tasks)
	assert.Nil(t, result.IO)
}

func TestScanPlanningRemoteSupportsSynchronousPlanOnlyServer(t *testing.T) {
	t.Parallel()

	metadata := newScanTaskDecoderMetadata()
	cat := newScanPlanningTestCatalog(t, []endpoint{endpointPlanTableScan}, func(mux *http.ServeMux) {
		mux.HandleFunc("/v1/namespaces/db/tables/tbl/plan", func(w http.ResponseWriter, req *http.Request) {
			planID := "plan-1"
			require.NoError(t, json.NewEncoder(w).Encode(PlanTableScanResponse{
				Status:    PlanStatusCompleted,
				PlanID:    &planID,
				ScanTasks: validScanTasksWire(),
			}))
		})
	})

	assert.True(t, cat.SupportsRemoteScanPlanning())
	assert.False(t, cat.SupportsFullRemoteScanPlanning())

	req := planFilesReq()
	req.Metadata = metadata
	req.Schema = metadata.schema
	result, err := cat.PlanFiles(context.Background(), req)
	require.NoError(t, err)
	require.Len(t, result.Tasks, 1)
	assert.Equal(t, "s3://bucket/table/data.parquet", result.Tasks[0].File.FilePath())
}

func TestPlanFilesRequiresAdvertisedResponseContinuation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		response string
	}{
		{
			name:     "submitted plan requires result fetch",
			response: `{"status":"submitted","plan-id":"plan-1"}`,
		},
		{
			name:     "plan task requires task fetch",
			response: `{"status":"completed","plan-id":"plan-1","plan-tasks":["task-1"]}`,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			cat := newScanPlanningTestCatalog(t, []endpoint{endpointPlanTableScan}, func(mux *http.ServeMux) {
				mux.HandleFunc("/v1/namespaces/db/tables/tbl/plan", func(w http.ResponseWriter, req *http.Request) {
					_, err := w.Write([]byte(test.response))
					require.NoError(t, err)
				})
			})

			_, err := cat.PlanFiles(context.Background(), planFilesReq())
			require.ErrorIs(t, err, ErrEndpointNotSupported)
		})
	}
}

// TestPlanFilesEncodesFilter checks the row filter reaches the plan request body
// as ExpressionParser JSON.
func TestPlanFilesEncodesFilter(t *testing.T) {
	t.Parallel()

	cat := newScanPlanningTestCatalog(t, []endpoint{endpointPlanTableScan}, func(mux *http.ServeMux) {
		mux.HandleFunc("/v1/namespaces/db/tables/tbl/plan", func(w http.ResponseWriter, req *http.Request) {
			body, err := io.ReadAll(req.Body)
			require.NoError(t, err)

			var got PlanTableScanRequest
			require.NoError(t, json.Unmarshal(body, &got))
			assert.JSONEq(t, `{"type":"eq","term":"i","value":25}`, string(got.Filter))

			_, err = w.Write([]byte(`{"status":"completed","plan-id":"plan-1"}`))
			require.NoError(t, err)
		})
	})

	req := planFilesReq()
	req.RowFilter = iceberg.EqualTo(iceberg.Reference("i"), int32(25))
	_, err := cat.PlanFiles(context.Background(), req)
	require.NoError(t, err)
}

// TestPlanFilesRejectsMalformedScanTasks makes sure remote planning validates
// the task payload instead of returning partially decoded tasks.
func TestPlanFilesRejectsMalformedScanTasks(t *testing.T) {
	t.Parallel()

	cat := newScanPlanningTestCatalog(t, []endpoint{endpointPlanTableScan}, func(mux *http.ServeMux) {
		mux.HandleFunc("/v1/namespaces/db/tables/tbl/plan", func(w http.ResponseWriter, req *http.Request) {
			_, err := w.Write([]byte(`{"status":"completed","plan-id":"plan-1","file-scan-tasks":[{}]}`))
			require.NoError(t, err)
		})
	})

	_, err := cat.PlanFiles(context.Background(), planFilesReq())
	require.ErrorIs(t, err, ErrRESTError)
}

// TestPlanFilesPollsSubmittedPlan covers the async arm: a submitted plan is
// polled to completion via WaitForPlan before returning.
func TestPlanFilesPollsSubmittedPlan(t *testing.T) {
	t.Parallel()

	cat := newScanPlanningTestCatalog(t, []endpoint{endpointPlanTableScan, endpointFetchPlanResult}, func(mux *http.ServeMux) {
		mux.HandleFunc("/v1/namespaces/db/tables/tbl/plan", func(w http.ResponseWriter, req *http.Request) {
			_, err := w.Write([]byte(`{"status":"submitted","plan-id":"plan-9"}`))
			require.NoError(t, err)
		})
		mux.HandleFunc("/v1/namespaces/db/tables/tbl/plan/plan-9", func(w http.ResponseWriter, req *http.Request) {
			require.Equal(t, http.MethodGet, req.Method)
			_, err := w.Write([]byte(`{"status":"completed"}`))
			require.NoError(t, err)
		})
	})

	result, err := cat.PlanFiles(context.Background(), planFilesReq())
	require.NoError(t, err)
	assert.Empty(t, result.Tasks)
}

// TestPlanFilesExpandsPlanTasks checks the fanout: a completed plan carrying a
// plan-task handle drives a fetchScanTasks call, and a handle returned by that
// call is expanded in turn.
func TestPlanFilesExpandsPlanTasks(t *testing.T) {
	t.Parallel()

	var mu sync.Mutex
	var fetched []string
	cat := newScanPlanningTestCatalog(t, []endpoint{endpointPlanTableScan, endpointFetchScanTasks}, func(mux *http.ServeMux) {
		mux.HandleFunc("/v1/namespaces/db/tables/tbl/plan", func(w http.ResponseWriter, req *http.Request) {
			_, err := w.Write([]byte(`{"status":"completed","plan-id":"plan-1","plan-tasks":["h1"]}`))
			require.NoError(t, err)
		})
		mux.HandleFunc("/v1/namespaces/db/tables/tbl/tasks", func(w http.ResponseWriter, req *http.Request) {
			var body FetchScanTasksRequest
			require.NoError(t, json.NewDecoder(req.Body).Decode(&body))
			mu.Lock()
			fetched = append(fetched, body.PlanTask)
			mu.Unlock()
			switch body.PlanTask {
			case "h1":
				_, err := w.Write([]byte(`{"plan-tasks":["h2"]}`))
				require.NoError(t, err)
			default:
				_, err := w.Write([]byte(`{"file-scan-tasks":[]}`))
				require.NoError(t, err)
			}
		})
	})

	result, err := cat.PlanFiles(context.Background(), planFilesReq())
	require.NoError(t, err)
	assert.Empty(t, result.Tasks)
	mu.Lock()
	defer mu.Unlock()
	assert.Equal(t, []string{"h1", "h2"}, fetched)
}

func TestCollectScanTasksFetchesFrontierConcurrentlyInOrder(t *testing.T) {
	t.Parallel()

	h1Started := make(chan struct{})
	h2Started := make(chan struct{})
	releaseH1 := make(chan struct{})
	var releaseOnce sync.Once
	release := func() { releaseOnce.Do(func() { close(releaseH1) }) }

	cat := newScanPlanningTestCatalog(t, []endpoint{endpointFetchScanTasks}, func(mux *http.ServeMux) {
		mux.HandleFunc("/v1/namespaces/db/tables/tbl/tasks", func(w http.ResponseWriter, req *http.Request) {
			var body FetchScanTasksRequest
			if err := json.NewDecoder(req.Body).Decode(&body); err != nil {
				http.Error(w, err.Error(), http.StatusBadRequest)

				return
			}

			switch body.PlanTask {
			case "h1":
				close(h1Started)
				<-releaseH1
				_, _ = w.Write([]byte(`{"file-scan-tasks":[{"data-file":{"file-path":"h1"}}]}`))
			case "h2":
				close(h2Started)
				_, _ = w.Write([]byte(`{"file-scan-tasks":[{"data-file":{"file-path":"h2"}}]}`))
			default:
				http.Error(w, "unexpected plan task", http.StatusBadRequest)
			}
		})
	})
	t.Cleanup(release)

	type outcome struct {
		envelopes []ScanTasks
		err       error
	}
	done := make(chan outcome, 1)
	go func() {
		envelopes, err := cat.collectScanTasksWithConcurrency(t.Context(), table.Identifier{"db", "tbl"}, ScanTasks{
			PlanTasks: []string{"h1", "h2"},
		}, 2)
		done <- outcome{envelopes: envelopes, err: err}
	}()

	for name, started := range map[string]<-chan struct{}{
		"h1": h1Started,
		"h2": h2Started,
	} {
		select {
		case <-started:
		case <-time.After(time.Second):
			t.Fatalf("timed out waiting for %s to start", name)
		}
	}

	release()
	select {
	case result := <-done:
		require.NoError(t, result.err)
		require.Len(t, result.envelopes, 3)
		require.Len(t, result.envelopes[1].FileScanTasks, 1)
		require.Len(t, result.envelopes[2].FileScanTasks, 1)
		require.NotNil(t, result.envelopes[1].FileScanTasks[0].DataFile)
		require.NotNil(t, result.envelopes[2].FileScanTasks[0].DataFile)
		assert.Equal(t, "h1", result.envelopes[1].FileScanTasks[0].DataFile.FilePath)
		assert.Equal(t, "h2", result.envelopes[2].FileScanTasks[0].DataFile.FilePath)
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for frontier fetches")
	}
}

type orderedScanTaskErrorTransport struct {
	planTaskReturned chan struct{}
}

func (t *orderedScanTaskErrorTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	var body FetchScanTasksRequest
	if err := json.NewDecoder(req.Body).Decode(&body); err != nil {
		return nil, err
	}

	var errType string
	switch body.PlanTask {
	case "table":
		select {
		case <-t.planTaskReturned:
		case <-req.Context().Done():
			return nil, req.Context().Err()
		}
		errType = errTypeNoSuchTable
	case "plan-task":
		close(t.planTaskReturned)
		errType = errTypeNoSuchPlanTask
	default:
		return nil, fmt.Errorf("unexpected plan task %q", body.PlanTask)
	}

	data := fmt.Sprintf(`{"error":{"message":%q,"type":%q,"code":404}}`, errType, errType)

	return &http.Response{
		StatusCode:    http.StatusNotFound,
		Header:        http.Header{"Content-Type": {"application/json"}},
		Body:          io.NopCloser(strings.NewReader(data)),
		ContentLength: int64(len(data)),
		Request:       req,
	}, nil
}

func TestCollectScanTasksReturnsFirstErrorInHandleOrder(t *testing.T) {
	t.Parallel()

	cat := newScanPlanningTestCatalog(t, []endpoint{endpointFetchScanTasks}, nil)
	cat.cl = &http.Client{Transport: &orderedScanTaskErrorTransport{
		planTaskReturned: make(chan struct{}),
	}}

	envelopes, err := cat.collectScanTasksWithConcurrency(t.Context(), table.Identifier{"db", "tbl"}, ScanTasks{
		PlanTasks: []string{"table", "plan-task"},
	}, 2)
	require.ErrorIs(t, err, catalog.ErrNoSuchTable)
	assert.Contains(t, err.Error(), `handle "table"`)
	assert.NotErrorIs(t, err, ErrNoSuchPlanTask)
	assert.Nil(t, envelopes)
}

func TestCollectScanTasksBoundsFrontierConcurrency(t *testing.T) {
	t.Parallel()

	const maxConcurrency = 8
	const handleCount = maxConcurrency + 1
	started := make(chan string, handleCount)
	release := make(chan struct{})
	var releaseOnce sync.Once
	finish := func() { releaseOnce.Do(func() { close(release) }) }

	cat := newScanPlanningTestCatalog(t, []endpoint{endpointFetchScanTasks}, func(mux *http.ServeMux) {
		mux.HandleFunc("/v1/namespaces/db/tables/tbl/tasks", func(w http.ResponseWriter, req *http.Request) {
			var body FetchScanTasksRequest
			if err := json.NewDecoder(req.Body).Decode(&body); err != nil {
				http.Error(w, err.Error(), http.StatusBadRequest)

				return
			}

			started <- body.PlanTask
			<-release
			_, _ = w.Write([]byte(`{"file-scan-tasks":[]}`))
		})
	})
	t.Cleanup(finish)

	handles := make([]string, handleCount)
	for i := range handles {
		handles[i] = fmt.Sprintf("h%d", i)
	}
	done := make(chan error, 1)
	go func() {
		_, err := cat.collectScanTasksWithConcurrency(t.Context(), table.Identifier{"db", "tbl"}, ScanTasks{
			PlanTasks: handles,
		}, maxConcurrency)
		done <- err
	}()

	for range maxConcurrency {
		select {
		case <-started:
		case <-time.After(time.Second):
			t.Fatal("timed out waiting for bounded frontier workers")
		}
	}

	select {
	case handle := <-started:
		t.Fatalf("frontier exceeded concurrency limit with %s", handle)
	case <-time.After(50 * time.Millisecond):
	}

	finishOne := func() {
		select {
		case release <- struct{}{}:
		case <-time.After(time.Second):
			t.Fatal("timed out releasing a frontier worker")
		}
	}
	finishOne()
	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("timed out starting the queued frontier handle")
	}
	finish()

	select {
	case err := <-done:
		require.NoError(t, err)
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for bounded frontier fetches")
	}
}

// TestPlanFilesFanoutCycleTerminates guards the seen-set: a server that re-issues
// a handle it already returned must not loop forever.
func TestPlanFilesFanoutCycleTerminates(t *testing.T) {
	t.Parallel()

	var calls atomic.Int32
	cat := newScanPlanningTestCatalog(t, []endpoint{endpointPlanTableScan, endpointFetchScanTasks}, func(mux *http.ServeMux) {
		mux.HandleFunc("/v1/namespaces/db/tables/tbl/plan", func(w http.ResponseWriter, req *http.Request) {
			_, err := w.Write([]byte(`{"status":"completed","plan-id":"plan-1","plan-tasks":["h1"]}`))
			require.NoError(t, err)
		})
		mux.HandleFunc("/v1/namespaces/db/tables/tbl/tasks", func(w http.ResponseWriter, req *http.Request) {
			calls.Add(1)
			_, err := w.Write([]byte(`{"plan-tasks":["h1"]}`))
			require.NoError(t, err)
		})
	})

	result, err := cat.PlanFiles(context.Background(), planFilesReq())
	require.NoError(t, err)
	assert.Empty(t, result.Tasks)
	assert.Equal(t, int32(1), calls.Load())
}

func TestPlanFilesCancelsAfterFanoutFailure(t *testing.T) {
	t.Parallel()

	var cancels atomic.Int32
	cat := newScanPlanningTestCatalog(t, []endpoint{
		endpointPlanTableScan,
		endpointFetchScanTasks,
		endpointCancelPlanning,
	}, func(mux *http.ServeMux) {
		mux.HandleFunc("/v1/namespaces/db/tables/tbl/plan", func(w http.ResponseWriter, req *http.Request) {
			_, err := w.Write([]byte(`{"status":"completed","plan-id":"plan-1","plan-tasks":["h1","h2"]}`))
			require.NoError(t, err)
		})
		mux.HandleFunc("/v1/namespaces/db/tables/tbl/tasks", func(w http.ResponseWriter, req *http.Request) {
			var body FetchScanTasksRequest
			require.NoError(t, json.NewDecoder(req.Body).Decode(&body))
			if body.PlanTask == "h1" {
				_, err := w.Write([]byte(`{"plan-tasks":[]}`))
				require.NoError(t, err)

				return
			}

			w.WriteHeader(http.StatusServiceUnavailable)
		})
		mux.HandleFunc("/v1/namespaces/db/tables/tbl/plan/plan-1", func(w http.ResponseWriter, req *http.Request) {
			require.Equal(t, http.MethodDelete, req.Method)
			cancels.Add(1)
			w.WriteHeader(http.StatusNoContent)
		})
	})

	_, err := cat.PlanFiles(context.Background(), planFilesReq())
	require.ErrorIs(t, err, ErrServiceUnavailable)
	assert.Equal(t, int32(1), cancels.Load())
}

func TestPlanFilesCancelsAfterSuccessfulMaterialization(t *testing.T) {
	t.Parallel()

	var cancels atomic.Int32
	cat := newScanPlanningTestCatalog(t, []endpoint{
		endpointPlanTableScan,
		endpointFetchScanTasks,
		endpointCancelPlanning,
	}, func(mux *http.ServeMux) {
		mux.HandleFunc("/v1/namespaces/db/tables/tbl/plan", func(w http.ResponseWriter, req *http.Request) {
			_, err := w.Write([]byte(`{"status":"completed","plan-id":"plan-1","plan-tasks":["h1"]}`))
			require.NoError(t, err)
		})
		mux.HandleFunc("/v1/namespaces/db/tables/tbl/tasks", func(w http.ResponseWriter, req *http.Request) {
			var body FetchScanTasksRequest
			require.NoError(t, json.NewDecoder(req.Body).Decode(&body))
			require.Equal(t, "h1", body.PlanTask)
			_, err := w.Write([]byte(`{"file-scan-tasks":[]}`))
			require.NoError(t, err)
		})
		mux.HandleFunc("/v1/namespaces/db/tables/tbl/plan/plan-1", func(w http.ResponseWriter, req *http.Request) {
			require.Equal(t, http.MethodDelete, req.Method)
			cancels.Add(1)
			w.WriteHeader(http.StatusNoContent)
		})
	})

	result, err := cat.PlanFiles(context.Background(), planFilesReq())
	require.NoError(t, err)
	assert.Empty(t, result.Tasks)
	assert.Equal(t, int32(1), cancels.Load())
}

func TestPlanFilesDefersCancelWithVendedCredentials(t *testing.T) {
	t.Parallel()

	var cancels atomic.Int32
	cat := newScanPlanningTestCatalog(t, []endpoint{
		endpointPlanTableScan,
		endpointCancelPlanning,
	}, func(mux *http.ServeMux) {
		mux.HandleFunc("/v1/namespaces/db/tables/tbl/plan", func(w http.ResponseWriter, req *http.Request) {
			_, err := w.Write([]byte(`{"status":"completed","plan-id":"plan-1","storage-credentials":[{"prefix":"s3://bucket/","config":{"s3.access-key-id":"vended"}}]}`))
			require.NoError(t, err)
		})
		mux.HandleFunc("/v1/namespaces/db/tables/tbl/plan/plan-1", func(w http.ResponseWriter, req *http.Request) {
			require.Equal(t, http.MethodDelete, req.Method)
			cancels.Add(1)
			w.WriteHeader(http.StatusNoContent)
		})
	})

	result, err := cat.PlanFiles(context.Background(), planFilesReq())
	require.NoError(t, err)
	require.NotNil(t, result.IO)
	assert.Equal(t, int32(0), cancels.Load())

	require.NoError(t, result.IO.Close())
	assert.Equal(t, int32(1), cancels.Load())
	require.NoError(t, result.IO.Close())
	assert.Equal(t, int32(1), cancels.Load())
}

func TestPlanFilesCancelsAfterTaskDecodeFailure(t *testing.T) {
	t.Parallel()

	var cancels atomic.Int32
	cat := newScanPlanningTestCatalog(t, []endpoint{
		endpointPlanTableScan,
		endpointFetchScanTasks,
		endpointCancelPlanning,
	}, func(mux *http.ServeMux) {
		mux.HandleFunc("/v1/namespaces/db/tables/tbl/plan", func(w http.ResponseWriter, req *http.Request) {
			_, err := w.Write([]byte(`{"status":"completed","plan-id":"plan-1","plan-tasks":["h1"]}`))
			require.NoError(t, err)
		})
		mux.HandleFunc("/v1/namespaces/db/tables/tbl/tasks", func(w http.ResponseWriter, req *http.Request) {
			var body FetchScanTasksRequest
			require.NoError(t, json.NewDecoder(req.Body).Decode(&body))
			require.Equal(t, "h1", body.PlanTask)
			_, err := w.Write([]byte(`{"file-scan-tasks":[{}]}`))
			require.NoError(t, err)
		})
		mux.HandleFunc("/v1/namespaces/db/tables/tbl/plan/plan-1", func(w http.ResponseWriter, req *http.Request) {
			require.Equal(t, http.MethodDelete, req.Method)
			cancels.Add(1)
			w.WriteHeader(http.StatusNoContent)
		})
	})

	_, err := cat.PlanFiles(context.Background(), planFilesReq())
	require.ErrorIs(t, err, ErrRESTError)
	assert.Equal(t, int32(1), cancels.Load())
}

func TestPlanFilesCancelsAfterTerminalPollError(t *testing.T) {
	t.Parallel()

	var cancels atomic.Int32
	cat := newScanPlanningTestCatalog(t, []endpoint{
		endpointPlanTableScan,
		endpointFetchPlanResult,
		endpointCancelPlanning,
	}, func(mux *http.ServeMux) {
		mux.HandleFunc("/v1/namespaces/db/tables/tbl/plan", func(w http.ResponseWriter, req *http.Request) {
			_, err := w.Write([]byte(`{"status":"submitted","plan-id":"plan-1"}`))
			require.NoError(t, err)
		})
		mux.HandleFunc("/v1/namespaces/db/tables/tbl/plan/plan-1", func(w http.ResponseWriter, req *http.Request) {
			switch req.Method {
			case http.MethodGet:
				http.Error(w, "unauthorized", http.StatusUnauthorized)
			case http.MethodDelete:
				cancels.Add(1)
				w.WriteHeader(http.StatusNoContent)
			default:
				http.Error(w, "unexpected method", http.StatusMethodNotAllowed)
			}
		})
	})

	_, err := cat.PlanFiles(context.Background(), planFilesReq())
	require.Error(t, err)
	assert.Equal(t, int32(1), cancels.Load())
}

func TestRemoteScanTasksUsesResolvedSchemaForResiduals(t *testing.T) {
	t.Parallel()

	decoderMetadata := newScanTaskDecoderMetadata()
	oldSchema := decoderMetadata.schema
	currentFields := append([]iceberg.NestedField(nil), oldSchema.Fields()...)
	currentFields[1].Name = "new_category"
	currentSchema := iceberg.NewSchema(11, currentFields...)
	metadata := scanPlanningSchemaMetadata{
		scanTaskDecoderMetadata: decoderMetadata,
		current:                 currentSchema,
		schemas:                 []*iceberg.Schema{currentSchema, oldSchema},
	}
	req := table.ScanPlanningRequest{
		Metadata: metadata,
		Schema:   oldSchema,
		RowFilter: iceberg.GreaterThan(
			iceberg.Reference("category"),
			"old",
		),
	}
	wireReq, err := planTableScanRequestFrom(req)
	require.NoError(t, err)

	wire := validScanTasksWire()
	wire.FileScanTasks[0].ResidualFilter = wireReq.Filter
	tasks, err := remoteScanTasks([]ScanTasks{wire}, req)
	require.NoError(t, err)
	require.Len(t, tasks, 1)
	assert.True(t, tasks[0].Residual.Equals(req.RowFilter))
}

// TestPlanFilesDecodesFanoutTaskEnvelopes exercises the Phase 5 boundary from
// REST wire tasks to table.FileScanTask. In particular, each fetchScanTasks
// response has its own delete-file reference namespace; decoding after
// flattening the responses would attach the second task to the first delete.
func TestPlanFilesDecodesFanoutTaskEnvelopes(t *testing.T) {
	t.Parallel()

	first := validScanTasksWire()
	first.FileScanTasks[0].DataFile.FilePath = "s3://bucket/table/first-data.parquet"
	first.DeleteFiles[0].FilePath = "s3://bucket/table/first-delete.parquet"
	first.PlanTasks = []string{"h1"}

	second := validScanTasksWire()
	second.FileScanTasks[0].DataFile.FilePath = "s3://bucket/table/second-data.parquet"
	second.DeleteFiles[0].FilePath = "s3://bucket/table/second-delete.parquet"

	cat := newScanPlanningTestCatalog(t, []endpoint{endpointPlanTableScan, endpointFetchScanTasks}, func(mux *http.ServeMux) {
		mux.HandleFunc("/v1/namespaces/db/tables/tbl/plan", func(w http.ResponseWriter, req *http.Request) {
			response := struct {
				Status PlanStatus `json:"status"`
				PlanID string     `json:"plan-id"`
				ScanTasks
			}{
				Status:    PlanStatusCompleted,
				PlanID:    "plan-1",
				ScanTasks: first,
			}
			require.NoError(t, json.NewEncoder(w).Encode(response))
		})
		mux.HandleFunc("/v1/namespaces/db/tables/tbl/tasks", func(w http.ResponseWriter, req *http.Request) {
			var got FetchScanTasksRequest
			require.NoError(t, json.NewDecoder(req.Body).Decode(&got))
			require.Equal(t, "h1", got.PlanTask)
			require.NoError(t, json.NewEncoder(w).Encode(FetchScanTasksResponse{ScanTasks: second}))
		})
	})

	req := planFilesReq()
	req.Metadata = newScanTaskDecoderMetadata()
	req.RowFilter = iceberg.GreaterThan(iceberg.Reference("id"), int64(10))

	result, err := cat.PlanFiles(context.Background(), req)
	require.NoError(t, err)
	require.Len(t, result.Tasks, 2)
	assert.Equal(t, "s3://bucket/table/first-data.parquet", result.Tasks[0].File.FilePath())
	assert.Equal(t, "s3://bucket/table/first-delete.parquet", result.Tasks[0].DeleteFiles[0].FilePath())
	assert.Equal(t, "s3://bucket/table/second-data.parquet", result.Tasks[1].File.FilePath())
	assert.Equal(t, "s3://bucket/table/second-delete.parquet", result.Tasks[1].DeleteFiles[0].FilePath())
	assert.Same(t, req.RowFilter, result.Tasks[0].Residual)
}

// TestPlanFilesSurfacesVendedCredentials checks a plan that vends storage
// credentials yields a plan-scoped IO that keeps the table's IO props (the
// custom endpoint here) rather than running on the vended creds alone, with the
// vended values winning where they overlap.
func TestPlanFilesSurfacesVendedCredentials(t *testing.T) {
	t.Parallel()

	cat := newScanPlanningTestCatalog(t, []endpoint{endpointPlanTableScan}, func(mux *http.ServeMux) {
		mux.HandleFunc("/v1/namespaces/db/tables/tbl/plan", func(w http.ResponseWriter, req *http.Request) {
			_, err := w.Write([]byte(`{"status":"completed","plan-id":"plan-1","storage-credentials":[{"prefix":"s3://bucket/","config":{"s3.access-key-id":"vended"}}]}`))
			require.NoError(t, err)
		})
	})
	cat.props["s3.endpoint"] = "https://catalog.local"
	cat.props["s3.access-key-id"] = "catalog-static"

	req := planFilesReq()
	req.MetadataLocation = "s3://bucket/db/tbl/metadata/v1.json"
	req.FileIOProperties = iceberg.Properties{
		"s3.endpoint":      "https://table.local",
		"s3.access-key-id": "table-static",
	}

	result, err := cat.PlanFiles(context.Background(), req)
	require.NoError(t, err)
	require.NotNil(t, result.IO)

	wrapped, ok := result.IO.(*planIOWithCleanup)
	require.True(t, ok)
	planIO, ok := wrapped.io.(*planScopedIO)
	require.True(t, ok)
	assert.Equal(t, "https://table.local", planIO.refresher.props["s3.endpoint"])
	assert.Equal(t, "table-static", planIO.refresher.props["s3.access-key-id"])
	require.Len(t, planIO.refresher.credentials, 1)
	assert.Equal(t, "vended", planIO.refresher.credentials[0].Config["s3.access-key-id"])
	require.NoError(t, result.IO.Close())
}

// TestPlanScopedIOExpiredCredentials checks a plan whose creds state an expiry
// fails loudly for a matching location once past it, since plan creds can't be
// renewed.
func TestPlanScopedIOExpiredCredentials(t *testing.T) {
	t.Parallel()

	now := time.Now()
	// file:// keeps LoadFS offline; the scheme is incidental to the expiry logic.
	creds := []StorageCredential{{
		Prefix: "file:///bucket/",
		Config: iceberg.Properties{
			"s3.access-key-id":               "vended",
			"s3.session-token-expires-at-ms": strconv.FormatInt(now.Add(time.Hour).UnixMilli(), 10),
		},
	}}

	planIO := planIOFromCredentials(creds, "file:///bucket/db/tbl/metadata/v1.json", nil)
	require.NotNil(t, planIO)

	p, ok := planIO.(*planScopedIO)
	require.True(t, ok)
	p.refresher.nowFunc = func() time.Time { return now }

	// The first load caches the prefix resolver rather than treating the plan's
	// location-specific credentials as one global credential set.
	fs, err := p.Load(context.Background())
	require.NoError(t, err)
	require.NotNil(t, fs)
	assert.True(t, p.refresher.expiresAt.IsZero())
	prefixIO, ok := fs.(*prefixScopedIO)
	require.True(t, ok)
	_, err = prefixIO.filesystemFor("file:///bucket/data.parquet")
	require.NoError(t, err)

	// Past the expiry, access under the credential's prefix fails even though
	// its filesystem was already cached.
	p.refresher.nowFunc = func() time.Time { return now.Add(2 * time.Hour) }
	_, err = prefixIO.filesystemFor("file:///bucket/data.parquet")
	require.ErrorIs(t, err, ErrVendedCredentialsExpired)
}

// TestPlanScopedIOAlreadyExpiredCredentials checks creds that are already past
// their expiry fail loudly when a matching location is first opened.
func TestPlanScopedIOAlreadyExpiredCredentials(t *testing.T) {
	t.Parallel()

	now := time.Now()
	creds := []StorageCredential{{
		Prefix: "file:///bucket/",
		Config: iceberg.Properties{
			"s3.access-key-id":               "vended",
			"s3.session-token-expires-at-ms": strconv.FormatInt(now.Add(-time.Hour).UnixMilli(), 10),
		},
	}}

	p, ok := planIOFromCredentials(creds, "file:///bucket/db/tbl/metadata/v1.json", nil).(*planScopedIO)
	require.True(t, ok)
	p.refresher.nowFunc = func() time.Time { return now }

	fs, err := p.Load(context.Background())
	require.NoError(t, err)
	_, err = fs.Open("file:///bucket/data.parquet")
	require.ErrorIs(t, err, ErrVendedCredentialsExpired)
}

func TestPlanScopedIOIgnoresExpiredCredentialForUnrelatedPrefix(t *testing.T) {
	t.Parallel()

	now := time.Now()
	creds := []StorageCredential{
		{
			Prefix: "file:///archive/",
			Config: iceberg.Properties{
				keyS3TokenExpiresAtMs: strconv.FormatInt(now.Add(-time.Hour).UnixMilli(), 10),
			},
		},
		{
			Prefix: "file:///current/",
			Config: iceberg.Properties{
				keyS3TokenExpiresAtMs: strconv.FormatInt(now.Add(time.Hour).UnixMilli(), 10),
			},
		},
	}

	p, ok := planIOFromCredentials(creds, "file:///metadata/v1.json", nil).(*planScopedIO)
	require.True(t, ok)
	p.refresher.nowFunc = func() time.Time { return now }

	fs, err := p.Load(context.Background())
	require.NoError(t, err)
	prefixIO, ok := fs.(*prefixScopedIO)
	require.True(t, ok)
	_, err = prefixIO.filesystemFor("file:///current/data.parquet")
	require.NoError(t, err)

	_, err = prefixIO.filesystemFor("file:///archive/data.parquet")
	require.ErrorIs(t, err, ErrVendedCredentialsExpired)
}

// TestPlanScopedIOCredentialsWithoutExpiry checks creds stating no expiry never
// expire: the fallback TTL exists to trigger a re-fetch a plan-scoped IO can't do.
func TestPlanScopedIOCredentialsWithoutExpiry(t *testing.T) {
	t.Parallel()

	creds := []StorageCredential{{Prefix: "file:///bucket/", Config: iceberg.Properties{"s3.access-key-id": "vended"}}}

	p, ok := planIOFromCredentials(creds, "file:///bucket/db/tbl/metadata/v1.json", nil).(*planScopedIO)
	require.True(t, ok)

	_, err := p.Load(context.Background())
	require.NoError(t, err)
	assert.True(t, p.refresher.expiresAt.IsZero())

	p.refresher.nowFunc = func() time.Time { return time.Now().Add(999 * time.Hour) }
	_, err = p.Load(context.Background())
	require.NoError(t, err)
}

// TestPlanFilesPropagatesFailure checks a failed plan surfaces as ErrPlanFailed.
func TestPlanFilesPropagatesFailure(t *testing.T) {
	t.Parallel()

	cat := newScanPlanningTestCatalog(t, []endpoint{endpointPlanTableScan}, func(mux *http.ServeMux) {
		mux.HandleFunc("/v1/namespaces/db/tables/tbl/plan", func(w http.ResponseWriter, req *http.Request) {
			_, err := w.Write([]byte(`{"status":"failed","error":{"message":"boom","type":"ServerError","code":500}}`))
			require.NoError(t, err)
		})
	})

	_, err := cat.PlanFiles(context.Background(), planFilesReq())
	require.ErrorIs(t, err, ErrPlanFailed)
}

func TestCollectScanTasksDeduplicatesAcrossFrontiers(t *testing.T) {
	t.Parallel()

	children := map[string][]string{
		"a": {"c", "d", "a"},
		"b": {"d", "e"},
		"c": {"b"},
	}
	var mu sync.Mutex
	calls := make(map[string]int)
	cat := newScanPlanningTestCatalog(t, []endpoint{endpointFetchScanTasks}, func(mux *http.ServeMux) {
		mux.HandleFunc("/v1/namespaces/db/tables/tbl/tasks", func(w http.ResponseWriter, req *http.Request) {
			var body FetchScanTasksRequest
			if err := json.NewDecoder(req.Body).Decode(&body); err != nil {
				http.Error(w, err.Error(), http.StatusBadRequest)

				return
			}
			mu.Lock()
			calls[body.PlanTask]++
			mu.Unlock()
			response := ScanTasks{
				PlanTasks: children[body.PlanTask],
				FileScanTasks: []RESTFileScanTask{{DataFile: &RESTDataFile{
					RESTContentFile: RESTContentFile{FilePath: body.PlanTask},
				}}},
			}
			_ = json.NewEncoder(w).Encode(response)
		})
	})

	envelopes, err := cat.collectScanTasksWithConcurrency(t.Context(), table.Identifier{"db", "tbl"}, ScanTasks{
		PlanTasks: []string{"a", "a", "b"},
	}, 2)
	require.NoError(t, err)
	require.Len(t, envelopes, 6)
	for i, name := range []string{"a", "b", "c", "d", "e"} {
		require.Len(t, envelopes[i+1].FileScanTasks, 1)
		assert.Equal(t, name, envelopes[i+1].FileScanTasks[0].DataFile.FilePath)
	}
	mu.Lock()
	defer mu.Unlock()
	assert.Equal(t, map[string]int{"a": 1, "b": 1, "c": 1, "d": 1, "e": 1}, calls)
}

func TestCollectScanTasksCancelsSiblingRequestsOnFailure(t *testing.T) {
	t.Parallel()

	started := make(chan struct{})
	cancelled := make(chan struct{})
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	cat := newScanPlanningTestCatalog(t, []endpoint{endpointFetchScanTasks}, func(mux *http.ServeMux) {
		mux.HandleFunc("/v1/namespaces/db/tables/tbl/tasks", func(w http.ResponseWriter, req *http.Request) {
			var body FetchScanTasksRequest
			if err := json.NewDecoder(req.Body).Decode(&body); err != nil {
				http.Error(w, err.Error(), http.StatusBadRequest)

				return
			}
			switch body.PlanTask {
			case "slow":
				close(started)
				<-req.Context().Done()
				close(cancelled)
			case "failed":
				select {
				case <-started:
					w.Header().Set("Content-Type", "application/json")
					w.WriteHeader(http.StatusBadRequest)
					_, _ = w.Write([]byte(`{"error":{"message":"invalid handle","type":"BadRequestException","code":400}}`))
				case <-req.Context().Done():
				}
			default:
				http.Error(w, "unexpected handle", http.StatusBadRequest)
			}
		})
	})

	envelopes, err := cat.collectScanTasksWithConcurrency(ctx, table.Identifier{"db", "tbl"}, ScanTasks{
		PlanTasks: []string{"failed", "slow"},
	}, 2)
	require.ErrorIs(t, err, ErrBadRequest)
	assert.Nil(t, envelopes)
	select {
	case <-cancelled:
	case <-ctx.Done():
		t.Fatal("sibling request was not cancelled after a fetch failure")
	}
}
