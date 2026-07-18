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

package rest_test

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"testing"
	"time"

	icebergcatalog "github.com/apache/iceberg-go/catalog"
	"github.com/apache/iceberg-go/catalog/rest"
	"github.com/apache/iceberg-go/catalog/rest/internal/planfake"
	"github.com/apache/iceberg-go/table"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestPlanFakeCapabilityDiscovery(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name             string
		endpoints        []string
		wantPlan         bool
		wantFullPlanning bool
	}{
		{
			name: "all planning endpoints",
			endpoints: []string{
				planfake.PlanTableScanEndpoint,
				planfake.FetchPlanningResultEndpoint,
				planfake.CancelPlanningEndpoint,
				planfake.FetchScanTasksEndpoint,
			},
			wantPlan:         true,
			wantFullPlanning: true,
		},
		{
			name:             "plan only",
			endpoints:        []string{planfake.PlanTableScanEndpoint},
			wantPlan:         true,
			wantFullPlanning: false,
		},
		{
			name: "missing plan submission",
			endpoints: []string{
				planfake.FetchPlanningResultEndpoint,
				planfake.CancelPlanningEndpoint,
				planfake.FetchScanTasksEndpoint,
			},
			wantPlan:         false,
			wantFullPlanning: false,
		},
		{
			name: "missing result fetch",
			endpoints: []string{
				planfake.PlanTableScanEndpoint,
				planfake.CancelPlanningEndpoint,
				planfake.FetchScanTasksEndpoint,
			},
			wantPlan:         true,
			wantFullPlanning: false,
		},
		{
			name: "missing cancellation",
			endpoints: []string{
				planfake.PlanTableScanEndpoint,
				planfake.FetchPlanningResultEndpoint,
				planfake.FetchScanTasksEndpoint,
			},
			wantPlan:         true,
			wantFullPlanning: false,
		},
		{
			name: "missing task fetch",
			endpoints: []string{
				planfake.PlanTableScanEndpoint,
				planfake.FetchPlanningResultEndpoint,
				planfake.CancelPlanningEndpoint,
			},
			wantPlan:         true,
			wantFullPlanning: false,
		},
		{
			name:             "no planning endpoints",
			endpoints:        []string{"GET /v1/{prefix}/namespaces"},
			wantPlan:         false,
			wantFullPlanning: false,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			srv := planfake.New(t, planfake.Scenario{
				ConfigResponse: planfake.ConfigResponse(test.endpoints...),
			})
			catalog, err := rest.NewCatalog(t.Context(), "rest", srv.URL())
			require.NoError(t, err)

			assert.Equal(t, test.wantPlan, catalog.SupportsPlanTableScan())
			assert.Equal(t, test.wantFullPlanning, catalog.SupportsFullRemoteScanPlanning())

			requests := srv.Requests()
			require.Len(t, requests, 1)
			assert.Equal(t, http.MethodGet, requests[0].Method)
			assert.Equal(t, "/v1/config", requests[0].Path)
		})
	}
}

func TestPlanFakeConfigOverrideRoutesPlanningPrefix(t *testing.T) {
	t.Parallel()

	srv := planfake.New(t, planfake.Scenario{
		ConfigResponse: planFakeJSONResponse(`{
			"defaults": {},
			"overrides": {"prefix": "server-prefix"},
			"endpoints": [
				"POST /v1/{prefix}/namespaces/{namespace}/tables/{table}/plan"
			]
		}`),
		ExpectedTarget: &planfake.ExpectedTarget{
			Prefix: "server-prefix", Namespace: "analytics", Table: "events",
		},
		PlanResponse: planFakeJSONResponse(`{
			"status":"completed",
			"plan-id":"prefix-plan"
		}`),
	})
	catalog, err := rest.NewCatalog(t.Context(), "rest", srv.URL(), rest.WithPrefix("client-prefix"))
	require.NoError(t, err)

	_, err = catalog.PlanTableScan(t.Context(), standardPlanFakeIdentifier(), rest.PlanTableScanRequest{})
	require.NoError(t, err)

	requests := srv.Requests()
	require.Len(t, requests, 2)
	assert.Equal(t, http.MethodGet, requests[0].Method)
	assert.Equal(t, "/v1/config", requests[0].Path)
	assert.Equal(t, http.MethodPost, requests[1].Method)
	assert.Equal(t, "/v1/server-prefix/namespaces/analytics/tables/events/plan", requests[1].Path)
	assert.Equal(t, "server-prefix", requests[1].Prefix)
	assert.Empty(t, srv.ScenarioErrors())
}

func TestPlanFakeSynchronousPlanningCapturesRequest(t *testing.T) {
	t.Parallel()

	srv := planfake.New(t, planfake.Scenario{
		ConfigResponse: fullPlanningConfigResponse(),
		ExpectedTarget: &planfake.ExpectedTarget{
			Prefix: "catalog", Namespace: "org\x1fanalytics", Table: "events",
		},
		PlanResponse: planFakeJSONResponse(canonicalCompletedPlanJSON),
	})
	catalog, err := rest.NewCatalog(t.Context(), "rest", srv.URL(), rest.WithPrefix("catalog"))
	require.NoError(t, err)

	idempotencyKey := "0190b6c5-1c3d-7000-8000-000000000011"
	accessDelegation := "remote-signing"
	snapshotID := int64(123)
	minimumRows := int64(50)
	caseSensitive := false
	useSnapshotSchema := true
	response, err := catalog.PlanTableScan(t.Context(), table.Identifier{"org", "analytics", "events"}, rest.PlanTableScanRequest{
		IdempotencyKey:    &idempotencyKey,
		AccessDelegation:  &accessDelegation,
		SnapshotID:        &snapshotID,
		Select:            []string{"id", "payload"},
		Filter:            json.RawMessage(`{"type":"eq","term":"id","value":34}`),
		MinRowsRequested:  &minimumRows,
		CaseSensitive:     &caseSensitive,
		UseSnapshotSchema: &useSnapshotSchema,
		StatsFields:       []string{"id", "payload"},
	})
	require.NoError(t, err)

	assert.Equal(t, rest.PlanStatusCompleted, response.Status)
	require.NotNil(t, response.PlanID)
	assert.Equal(t, "plan-sync", *response.PlanID)
	assert.Equal(t, []string{"inline-child"}, response.PlanTasks)
	assert.Len(t, response.FileScanTasks, 1)
	assert.Len(t, response.DeleteFiles, 1)
	require.Len(t, response.StorageCredentials, 1)
	assert.Equal(t, "s3://warehouse/table/", response.StorageCredentials[0].Prefix)
	assert.Equal(t, "plan-key", response.StorageCredentials[0].Config["s3.access-key-id"])

	requests := srv.Requests()
	require.Len(t, requests, 2)
	request := requests[1]
	assert.Equal(t, http.MethodPost, request.Method)
	assert.Equal(t, "/v1/catalog/namespaces/org%1Fanalytics/tables/events/plan", request.Path)
	assert.Equal(t, "catalog", request.Prefix)
	assert.Equal(t, "org\x1fanalytics", request.Namespace)
	assert.Equal(t, "events", request.Table)
	assert.Equal(t, idempotencyKey, request.Header.Get("Idempotency-Key"))
	assert.Equal(t, accessDelegation, request.Header.Get("X-Iceberg-Access-Delegation"))
	assert.JSONEq(t, `{
		"snapshot-id": 123,
		"select": ["id", "payload"],
		"filter": {"type": "eq", "term": "id", "value": 34},
		"min-rows-requested": 50,
		"case-sensitive": false,
		"use-snapshot-schema": true,
		"stats-fields": ["id", "payload"]
	}`, string(request.Body))
}

func TestPlanFakeAsynchronousPlanningWaitsThroughRetry(t *testing.T) {
	t.Parallel()

	srv := planfake.New(t, planfake.Scenario{
		ConfigResponse: fullPlanningConfigResponse(),
		ExpectedTarget: standardPlanFakeTarget(),
		PlanResponse:   planFakeJSONResponse(`{"status":"submitted","plan-id":"plan-async"}`),
		PollResponses: map[string]planfake.ResponseSequence{
			"plan-async": {
				Responses: []planfake.Response{
					{
						Status: http.StatusServiceUnavailable,
						Body: json.RawMessage(`{
						"error":{"message":"busy","type":"ServiceUnavailableException","code":503}
					}`),
					},
					planFakeJSONResponse(`{"status":"submitted"}`),
					planFakeJSONResponse(`{
					"status":"completed",
					"plan-tasks":["root-task"],
					"storage-credentials":[{
						"prefix":"s3://warehouse/table/",
						"config":{"token":"async-token"}
						}]
				}`),
				},
			},
		},
	})
	catalog, err := rest.NewCatalog(t.Context(), "rest", srv.URL(), rest.WithPrefix("warehouse"))
	require.NoError(t, err)

	accessDelegation := "remote-signing"
	initial, err := catalog.PlanTableScan(t.Context(), standardPlanFakeIdentifier(), rest.PlanTableScanRequest{
		AccessDelegation: &accessDelegation,
	})
	require.NoError(t, err)
	require.NotNil(t, initial.PlanID)
	assert.Equal(t, rest.PlanStatusSubmitted, initial.Status)

	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	completed, err := catalog.WaitForPlan(ctx, standardPlanFakeIdentifier(), *initial.PlanID, rest.WaitForPlanOptions{
		MinDelay:          time.Millisecond,
		MaxDelay:          2 * time.Millisecond,
		CancelGracePeriod: time.Second,
		MaxRetries:        10,
		AccessDelegation:  &accessDelegation,
	})
	require.NoError(t, err)

	assert.Equal(t, rest.PlanStatusCompleted, completed.Status)
	assert.Equal(t, []string{"root-task"}, completed.PlanTasks)
	require.Len(t, completed.StorageCredentials, 1)
	assert.Equal(t, "async-token", completed.StorageCredentials[0].Config["token"])
	assert.Equal(t, 3, srv.PollCount("plan-async"))
	assert.Zero(t, srv.CancelCount("plan-async"))

	requests := srv.Requests()
	assertPlanFakeUUIDv7Header(t, requests[1])
	pollRequests := requestsForPlanFakeOperation(requests, http.MethodGet, "plan-async", "")
	require.Len(t, pollRequests, 3)
	for _, request := range pollRequests {
		assert.Equal(t, accessDelegation, request.Header.Get("X-Iceberg-Access-Delegation"))
	}
}

func TestPlanFakeSurfacesTerminalPlanningStatuses(t *testing.T) {
	t.Parallel()

	t.Run("failed submission preserves structured detail", func(t *testing.T) {
		t.Parallel()

		srv := planfake.New(t, planfake.Scenario{
			ConfigResponse: fullPlanningConfigResponse(),
			ExpectedTarget: standardPlanFakeTarget(),
			PlanResponse: planFakeJSONResponse(`{
				"status":"failed",
				"error":{
					"message":"manifest read failed",
					"type":"PlanningException",
					"code":500,
					"stack":["planner.go:42"]
				}
			}`),
		})
		catalog, err := rest.NewCatalog(t.Context(), "rest", srv.URL(), rest.WithPrefix("warehouse"))
		require.NoError(t, err)

		_, err = catalog.PlanTableScan(t.Context(), standardPlanFakeIdentifier(), rest.PlanTableScanRequest{})
		require.ErrorIs(t, err, rest.ErrPlanFailed)
		var failed *rest.PlanFailedError
		require.ErrorAs(t, err, &failed)
		require.NotNil(t, failed.Detail)
		assert.Equal(t, "manifest read failed", failed.Detail.Message)
		assert.Equal(t, "PlanningException", failed.Detail.Type)
		assert.Equal(t, 500, failed.Detail.Code)
		assert.Equal(t, []string{"planner.go:42"}, failed.Detail.Stack)
		assert.Empty(t, srv.ScenarioErrors())
	})

	t.Run("cancelled poll returns cancellation sentinel", func(t *testing.T) {
		t.Parallel()

		const planID = "cancelled-plan"
		srv := planfake.New(t, planfake.Scenario{
			ConfigResponse: fullPlanningConfigResponse(),
			ExpectedTarget: standardPlanFakeTarget(),
			PollResponses: map[string]planfake.ResponseSequence{
				planID: {
					Responses: []planfake.Response{
						planFakeJSONResponse(`{"status":"cancelled"}`),
					},
				},
			},
		})
		catalog, err := rest.NewCatalog(t.Context(), "rest", srv.URL(), rest.WithPrefix("warehouse"))
		require.NoError(t, err)

		_, err = catalog.WaitForPlan(t.Context(), standardPlanFakeIdentifier(), planID, rest.WaitForPlanOptions{})
		require.ErrorIs(t, err, rest.ErrPlanCancelled)
		assert.NotErrorIs(t, err, rest.ErrPlanFailed)
		assert.Equal(t, 1, srv.PollCount(planID))
		assert.Zero(t, srv.CancelCount(planID))
		assert.Empty(t, srv.ScenarioErrors())
	})
}

func TestPlanFakePlanSubmissionMapsMissingTarget(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		errorType string
		wantErr   error
		notErr    error
	}{
		{
			name:      "table",
			errorType: "NoSuchTableException",
			wantErr:   icebergcatalog.ErrNoSuchTable,
			notErr:    icebergcatalog.ErrNoSuchNamespace,
		},
		{
			name:      "namespace",
			errorType: "NoSuchNamespaceException",
			wantErr:   icebergcatalog.ErrNoSuchNamespace,
			notErr:    icebergcatalog.ErrNoSuchTable,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			srv := planfake.New(t, planfake.Scenario{
				ConfigResponse: fullPlanningConfigResponse(),
				ExpectedTarget: standardPlanFakeTarget(),
				PlanResponse: planfake.Response{
					Status: http.StatusNotFound,
					Body: json.RawMessage(`{
						"error":{
							"message":"planning target does not exist",
							"type":"` + test.errorType + `",
							"code":404
						}
					}`),
				},
			})
			catalog, err := rest.NewCatalog(t.Context(), "rest", srv.URL(), rest.WithPrefix("warehouse"))
			require.NoError(t, err)

			_, err = catalog.PlanTableScan(t.Context(), standardPlanFakeIdentifier(), rest.PlanTableScanRequest{})
			require.ErrorIs(t, err, test.wantErr)
			assert.NotErrorIs(t, err, test.notErr)
			assert.Empty(t, srv.ScenarioErrors())
		})
	}
}

func TestPlanFakeWaitCancellationCleansUpServerPlan(t *testing.T) {
	t.Parallel()

	srv := planfake.New(t, planfake.Scenario{
		ConfigResponse: fullPlanningConfigResponse(),
		ExpectedTarget: standardPlanFakeTarget(),
		PlanResponse:   planFakeJSONResponse(`{"status":"submitted","plan-id":"plan-cancel"}`),
		PollResponses: map[string]planfake.ResponseSequence{
			"plan-cancel": {
				Responses:  []planfake.Response{planFakeJSONResponse(`{"status":"submitted"}`)},
				RepeatLast: true,
			},
		},
		CancelResponses: map[string]planfake.ResponseSequence{
			"plan-cancel": {
				Responses: []planfake.Response{{Status: http.StatusNoContent}},
			},
		},
	})
	catalog, err := rest.NewCatalog(t.Context(), "rest", srv.URL(), rest.WithPrefix("warehouse"))
	require.NoError(t, err)

	initial, err := catalog.PlanTableScan(t.Context(), standardPlanFakeIdentifier(), rest.PlanTableScanRequest{})
	require.NoError(t, err)
	require.NotNil(t, initial.PlanID)

	waitContext, cancelWait := context.WithCancel(t.Context())
	defer cancelWait()
	waitDone := make(chan error, 1)
	go func() {
		_, waitErr := catalog.WaitForPlan(waitContext, standardPlanFakeIdentifier(), *initial.PlanID, rest.WaitForPlanOptions{
			MinDelay:          time.Hour,
			MaxDelay:          time.Hour,
			CancelGracePeriod: time.Second,
		})
		waitDone <- waitErr
	}()

	observeContext, cancelObserve := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancelObserve()
	require.NoError(t, srv.WaitForPollCount(observeContext, "plan-cancel", 1))
	cancelWait()
	err = waitForPlanFakeResult(t, observeContext, waitDone)
	require.ErrorIs(t, err, context.Canceled)
	assert.GreaterOrEqual(t, srv.PollCount("plan-cancel"), 1)
	assert.Equal(t, 1, srv.CancelCount("plan-cancel"))
	assert.Empty(t, srv.ScenarioErrors())

	requests := requestsForPlanFakeOperation(srv.Requests(), http.MethodDelete, "plan-cancel", "")
	require.Len(t, requests, 1)
	assert.Empty(t, requests[0].Header.Get("Idempotency-Key"))
	assert.Empty(t, requests[0].Header.Get("X-Iceberg-Access-Delegation"))
}

func TestPlanFakeWaitCancellationBoundsStalledCleanup(t *testing.T) {
	t.Parallel()

	stalledCancel := make(chan struct{})
	srv := planfake.New(t, planfake.Scenario{
		ConfigResponse: fullPlanningConfigResponse(),
		ExpectedTarget: standardPlanFakeTarget(),
		PlanResponse:   planFakeJSONResponse(`{"status":"submitted","plan-id":"plan-stalled-cancel"}`),
		PollResponses: map[string]planfake.ResponseSequence{
			"plan-stalled-cancel": {
				Responses:  []planfake.Response{planFakeJSONResponse(`{"status":"submitted"}`)},
				RepeatLast: true,
			},
		},
		CancelResponses: map[string]planfake.ResponseSequence{
			"plan-stalled-cancel": {
				Responses: []planfake.Response{{Status: http.StatusNoContent, Gate: stalledCancel}},
			},
		},
	})
	catalog, err := rest.NewCatalog(t.Context(), "rest", srv.URL(), rest.WithPrefix("warehouse"))
	require.NoError(t, err)

	initial, err := catalog.PlanTableScan(t.Context(), standardPlanFakeIdentifier(), rest.PlanTableScanRequest{})
	require.NoError(t, err)
	require.NotNil(t, initial.PlanID)

	waitContext, cancelWait := context.WithCancel(t.Context())
	defer cancelWait()
	waitDone := make(chan error, 1)
	go func() {
		_, waitErr := catalog.WaitForPlan(waitContext, standardPlanFakeIdentifier(), *initial.PlanID, rest.WaitForPlanOptions{
			MinDelay:          time.Hour,
			MaxDelay:          time.Hour,
			CancelGracePeriod: 100 * time.Millisecond,
		})
		waitDone <- waitErr
	}()

	observeContext, cancelObserve := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancelObserve()
	require.NoError(t, srv.WaitForPollCount(observeContext, "plan-stalled-cancel", 1))
	cancelWait()
	err = waitForPlanFakeResult(t, observeContext, waitDone)
	require.ErrorIs(t, err, context.Canceled)
	assert.GreaterOrEqual(t, srv.PollCount("plan-stalled-cancel"), 1)
	assert.Equal(t, 1, srv.CancelCount("plan-stalled-cancel"))
	assert.Empty(t, srv.ScenarioErrors())
}

func TestPlanFakeSupportsNestedAndReissuedTaskHandles(t *testing.T) {
	t.Parallel()

	srv := planfake.New(t, planfake.Scenario{
		ConfigResponse: fullPlanningConfigResponse(),
		ExpectedTarget: standardPlanFakeTarget(),
		TaskResponses: map[string]planfake.ResponseSequence{
			"root-task": {
				Responses: []planfake.Response{planFakeJSONResponse(`{
					"plan-tasks":["reissued-task","reissued-task"],
					"file-scan-tasks":[` + rootFileScanTaskJSON + `]
				}`)},
			},
			"reissued-task": {
				Responses: []planfake.Response{
					planFakeJSONResponse(`{"plan-tasks":["leaf-a"]}`),
					planFakeJSONResponse(`{"plan-tasks":["leaf-b"]}`),
				},
			},
			"leaf-a": {
				Responses:  []planfake.Response{planFakeJSONResponse(envelopeAScanTasksJSON)},
				RepeatLast: true,
			},
			"leaf-b": {
				Responses:  []planfake.Response{planFakeJSONResponse(envelopeBScanTasksJSON)},
				RepeatLast: true,
			},
			"grandchild": {
				Responses: []planfake.Response{
					planFakeJSONResponse(`{"file-scan-tasks":[` + grandchildFileScanTaskJSON + `]}`),
				},
			},
		},
	})
	catalog, err := rest.NewCatalog(t.Context(), "rest", srv.URL(), rest.WithPrefix("warehouse"))
	require.NoError(t, err)

	queue := []string{"root-task"}
	responsesByHandle := make(map[string][]rest.FetchScanTasksResponse)
	for len(queue) > 0 {
		handle := queue[0]
		queue = queue[1:]

		response, err := catalog.FetchScanTasks(t.Context(), standardPlanFakeIdentifier(), rest.FetchScanTasksRequest{
			PlanTask: handle,
		})
		require.NoError(t, err)
		responsesByHandle[handle] = append(responsesByHandle[handle], response)
		queue = append(queue, response.PlanTasks...)
	}

	assert.Equal(t, 1, srv.TaskCount("root-task"))
	assert.Equal(t, 2, srv.TaskCount("reissued-task"))
	assert.Equal(t, 1, srv.TaskCount("leaf-a"))
	assert.Equal(t, 1, srv.TaskCount("leaf-b"))
	assert.Equal(t, 1, srv.TaskCount("grandchild"))
	require.Len(t, responsesByHandle["reissued-task"], 2)
	assert.Equal(t, []string{"leaf-a"}, responsesByHandle["reissued-task"][0].PlanTasks)
	assert.Equal(t, []string{"leaf-b"}, responsesByHandle["reissued-task"][1].PlanTasks)

	// Both independently fetched envelopes use delete reference 0 and bind it to
	// a different delete path. Keeping these raw envelopes separate is required
	// before the decoder turns them into domain tasks.
	require.Len(t, responsesByHandle["leaf-a"], 1)
	assert.Len(t, responsesByHandle["leaf-a"][0].FileScanTasks, 1)
	assert.Len(t, responsesByHandle["leaf-a"][0].DeleteFiles, 1)
	require.Len(t, responsesByHandle["leaf-b"], 1)
	assert.Len(t, responsesByHandle["leaf-b"][0].FileScanTasks, 1)
	assert.Len(t, responsesByHandle["leaf-b"][0].DeleteFiles, 1)

	// Fetch each leaf again as raw JSON through an independent HTTP client. This
	// keeps the fixture assertion independent from production wire types and binds
	// each handle to its own envelope-local delete reference and child handles.
	leafA := fetchRawPlanTaskFixture(t, srv.URL(), "leaf-a")
	assertEnvelopeLocalFixture(t, leafA, nil,
		"s3://warehouse/table/envelope-a.parquet",
		"s3://warehouse/table/envelope-a-delete.parquet")
	leafB := fetchRawPlanTaskFixture(t, srv.URL(), "leaf-b")
	assertEnvelopeLocalFixture(t, leafB, []string{"grandchild"},
		"s3://warehouse/table/envelope-b.parquet",
		"s3://warehouse/table/envelope-b-delete.parquet")
	assert.Equal(t, 2, srv.TaskCount("leaf-a"))
	assert.Equal(t, 2, srv.TaskCount("leaf-b"))

	for _, request := range srv.Requests() {
		if request.PlanTask == "" {
			continue
		}
		assertPlanFakeUUIDv7Header(t, request)
		assert.Empty(t, request.Header.Get("X-Iceberg-Access-Delegation"))
	}
}

func TestPlanFakeModelsExpiredPlansAndTasks(t *testing.T) {
	t.Parallel()

	srv := planfake.New(t, planfake.Scenario{
		ConfigResponse: fullPlanningConfigResponse(),
		ExpectedTarget: standardPlanFakeTarget(),
		PollResponses: map[string]planfake.ResponseSequence{
			"expired-plan": {
				Responses: []planfake.Response{{
					Status: http.StatusNotFound,
					Body: json.RawMessage(`{
						"error":{"message":"expired","type":"NoSuchPlanIdException","code":404}
					}`),
				}},
			},
		},
		TaskResponses: map[string]planfake.ResponseSequence{
			"expired-task": {
				Responses: []planfake.Response{{
					Status: http.StatusNotFound,
					Body: json.RawMessage(`{
						"error":{"message":"expired","type":"NoSuchPlanTaskException","code":404}
					}`),
				}},
			},
		},
	})
	catalog, err := rest.NewCatalog(t.Context(), "rest", srv.URL(), rest.WithPrefix("warehouse"))
	require.NoError(t, err)

	_, err = catalog.FetchPlanningResult(t.Context(), standardPlanFakeIdentifier(), "expired-plan", rest.FetchPlanningResultOptions{})
	require.ErrorIs(t, err, rest.ErrPlanExpired)

	_, err = catalog.FetchScanTasks(t.Context(), standardPlanFakeIdentifier(), rest.FetchScanTasksRequest{
		PlanTask: "expired-task",
	})
	require.ErrorIs(t, err, rest.ErrNoSuchPlanTask)
}

func TestPlanFakeIndependentWireFixtureContract(t *testing.T) {
	t.Parallel()

	canonical := parseRawPlanningFixture(t, canonicalCompletedPlanJSON)
	assert.Equal(t, "completed", canonical.Status)
	assert.Equal(t, "plan-sync", canonical.PlanID)
	assert.Equal(t, []string{"inline-child"}, canonical.PlanTasks)
	require.Len(t, canonical.FileScanTasks, 1)
	require.Len(t, canonical.DeleteFiles, 1)

	task := canonical.FileScanTasks[0]
	assert.Equal(t, "data", task.DataFile.Content)
	assert.Equal(t, "s3://warehouse/table/data.parquet", task.DataFile.FilePath)
	require.Len(t, task.DataFile.Partition, 3)
	assert.Equal(t, `34`, string(task.DataFile.Partition[0]), "integer partitions must remain typed JSON numbers")
	assert.Equal(t, `"2026-07-17"`, string(task.DataFile.Partition[1]), "date partitions must remain typed JSON strings")
	assert.Equal(t, `"78797A21"`, string(task.DataFile.Partition[2]), "fixed partitions must remain typed JSON strings")
	assert.Equal(t, []int{1, 2}, task.DataFile.ColumnSizes.Keys)
	assert.Equal(t, []int64{800, 1200}, task.DataFile.ColumnSizes.Values)
	assert.Equal(t, []int{8, 9}, task.DataFile.LowerBounds.Keys)
	assert.Equal(t, []string{"01000000", "02000000"}, task.DataFile.LowerBounds.Values,
		"lower bounds must be Java-compatible hexadecimal binary values")
	assert.Equal(t, []int{8, 9}, task.DataFile.UpperBounds.Keys)
	assert.Equal(t, []string{"05000000", "0A000000"}, task.DataFile.UpperBounds.Values,
		"upper bounds must be Java-compatible hexadecimal binary values")
	assert.Equal(t, []int{0}, task.DeleteFileReferences)
	assert.JSONEq(t, `{"type":"eq","term":"id","value":34}`, string(task.ResidualFilter))

	assert.Equal(t, "position-deletes", canonical.DeleteFiles[0].Content)
	assert.Equal(t, "s3://warehouse/table/delete.parquet", canonical.DeleteFiles[0].FilePath)
	require.Len(t, canonical.DeleteFiles[0].Partition, 3)
	assert.Equal(t, `34`, string(canonical.DeleteFiles[0].Partition[0]))
	assert.Equal(t, `"2026-07-17"`, string(canonical.DeleteFiles[0].Partition[1]))
	assert.Equal(t, `"78797A21"`, string(canonical.DeleteFiles[0].Partition[2]))
	require.Len(t, canonical.StorageCredentials, 1)
	assert.Equal(t, "s3://warehouse/table/", canonical.StorageCredentials[0].Prefix)
	assert.Equal(t, map[string]string{"s3.access-key-id": "plan-key"}, canonical.StorageCredentials[0].Config)

	assertEnvelopeLocalFixture(t, parseRawPlanningFixture(t, envelopeAScanTasksJSON), nil,
		"s3://warehouse/table/envelope-a.parquet",
		"s3://warehouse/table/envelope-a-delete.parquet")
	assertEnvelopeLocalFixture(t, parseRawPlanningFixture(t, envelopeBScanTasksJSON), []string{"grandchild"},
		"s3://warehouse/table/envelope-b.parquet",
		"s3://warehouse/table/envelope-b-delete.parquet")
}

func standardPlanFakeTarget() *planfake.ExpectedTarget {
	return &planfake.ExpectedTarget{Prefix: "warehouse", Namespace: "analytics", Table: "events"}
}

func standardPlanFakeIdentifier() table.Identifier {
	return table.Identifier{"analytics", "events"}
}

func waitForPlanFakeResult(t *testing.T, ctx context.Context, result <-chan error) error {
	t.Helper()

	select {
	case err := <-result:
		return err
	case <-ctx.Done():
		t.Fatalf("timed out waiting for WaitForPlan to return: %v", ctx.Err())

		return nil
	}
}

type rawPlanningFixture struct {
	Status             string                        `json:"status"`
	PlanID             string                        `json:"plan-id"`
	PlanTasks          []string                      `json:"plan-tasks"`
	FileScanTasks      []rawFileScanTaskFixture      `json:"file-scan-tasks"`
	DeleteFiles        []rawDeleteFileFixture        `json:"delete-files"`
	StorageCredentials []rawStorageCredentialFixture `json:"storage-credentials"`
}

type rawFileScanTaskFixture struct {
	DataFile             rawDataFileFixture `json:"data-file"`
	DeleteFileReferences []int              `json:"delete-file-references"`
	ResidualFilter       json.RawMessage    `json:"residual-filter"`
}

type rawDataFileFixture struct {
	SpecID      int                `json:"spec-id"`
	Partition   []json.RawMessage  `json:"partition"`
	Content     string             `json:"content"`
	FilePath    string             `json:"file-path"`
	LowerBounds rawValueMapFixture `json:"lower-bounds"`
	UpperBounds rawValueMapFixture `json:"upper-bounds"`
	ColumnSizes rawCountMapFixture `json:"column-sizes"`
}

type rawDeleteFileFixture struct {
	SpecID    int               `json:"spec-id"`
	Partition []json.RawMessage `json:"partition"`
	Content   string            `json:"content"`
	FilePath  string            `json:"file-path"`
}

type rawValueMapFixture struct {
	Keys   []int    `json:"keys"`
	Values []string `json:"values"`
}

type rawCountMapFixture struct {
	Keys   []int   `json:"keys"`
	Values []int64 `json:"values"`
}

type rawStorageCredentialFixture struct {
	Prefix string            `json:"prefix"`
	Config map[string]string `json:"config"`
}

func parseRawPlanningFixture(t *testing.T, body string) rawPlanningFixture {
	t.Helper()

	var fixture rawPlanningFixture
	require.NoError(t, json.Unmarshal([]byte(body), &fixture))

	return fixture
}

func fetchRawPlanTaskFixture(t *testing.T, serverURL, planTask string) rawPlanningFixture {
	t.Helper()

	body, err := json.Marshal(map[string]string{"plan-task": planTask})
	require.NoError(t, err)
	request, err := http.NewRequestWithContext(t.Context(), http.MethodPost,
		serverURL+"/v1/warehouse/namespaces/analytics/tables/events/tasks", bytes.NewReader(body))
	require.NoError(t, err)
	request.Header.Set("Content-Type", "application/json")
	idempotencyKey, err := uuid.NewV7()
	require.NoError(t, err)
	request.Header.Set("Idempotency-Key", idempotencyKey.String())

	response, err := http.DefaultClient.Do(request)
	require.NoError(t, err)
	responseBody, readErr := io.ReadAll(response.Body)
	closeErr := response.Body.Close()
	require.NoError(t, readErr)
	require.NoError(t, closeErr)
	require.Equal(t, http.StatusOK, response.StatusCode)

	return parseRawPlanningFixture(t, string(responseBody))
}

func assertEnvelopeLocalFixture(
	t *testing.T,
	fixture rawPlanningFixture,
	wantPlanTasks []string,
	wantDataPath, wantDeletePath string,
) {
	t.Helper()

	assert.Equal(t, wantPlanTasks, fixture.PlanTasks)
	require.Len(t, fixture.FileScanTasks, 1)
	require.Len(t, fixture.DeleteFiles, 1)
	task := fixture.FileScanTasks[0]
	assert.Equal(t, wantDataPath, task.DataFile.FilePath)
	assert.Equal(t, []int{0}, task.DeleteFileReferences,
		"delete reference 0 must resolve within this response envelope")
	assert.JSONEq(t, `true`, string(task.ResidualFilter))
	assert.NotNil(t, task.DataFile.Partition)
	assert.Empty(t, task.DataFile.Partition)
	assert.Equal(t, wantDeletePath, fixture.DeleteFiles[0].FilePath)
	assert.NotNil(t, fixture.DeleteFiles[0].Partition)
	assert.Empty(t, fixture.DeleteFiles[0].Partition)
}

func fullPlanningConfigResponse() planfake.Response {
	return planfake.ConfigResponse(
		planfake.PlanTableScanEndpoint,
		planfake.FetchPlanningResultEndpoint,
		planfake.CancelPlanningEndpoint,
		planfake.FetchScanTasksEndpoint,
	)
}

func planFakeJSONResponse(body string) planfake.Response {
	return planfake.Response{Status: http.StatusOK, Body: json.RawMessage(body)}
}

func requestsForPlanFakeOperation(
	requests []planfake.Request,
	method, planID, planTask string,
) []planfake.Request {
	matched := make([]planfake.Request, 0)
	for _, request := range requests {
		if request.Method == method && request.PlanID == planID && request.PlanTask == planTask {
			matched = append(matched, request)
		}
	}

	return matched
}

func assertPlanFakeUUIDv7Header(t *testing.T, request planfake.Request) {
	t.Helper()

	key := request.Header.Get("Idempotency-Key")
	require.NotEmpty(t, key)
	parsed, err := uuid.Parse(key)
	require.NoError(t, err)
	assert.Equal(t, uuid.Version(7), parsed.Version())
}

// The scan-task fixtures below are authored as literal REST JSON rather than
// serialized with catalog/rest types. They follow Java's ContentFileParser
// conventions: partition values are typed JSON values, while lower/upper bounds
// are hexadecimal strings containing raw Iceberg binary single-value encodings.
const canonicalCompletedPlanJSON = `{
	"status": "completed",
	"plan-id": "plan-sync",
	"plan-tasks": ["inline-child"],
	"file-scan-tasks": [` + canonicalFileScanTaskJSON + `],
	"delete-files": [` + canonicalPositionDeleteJSON + `],
	"storage-credentials": [{
		"prefix": "s3://warehouse/table/",
		"config": {"s3.access-key-id": "plan-key"}
	}]
}`

const canonicalFileScanTaskJSON = `{
	"data-file": {
		"spec-id": 7,
		"partition": [34, "2026-07-17", "78797A21"],
		"content": "data",
		"file-path": "s3://warehouse/table/data.parquet",
		"file-format": "parquet",
		"file-size-in-bytes": 4096,
		"record-count": 100,
		"column-sizes": {"keys": [1, 2], "values": [800, 1200]},
		"lower-bounds": {"keys": [8, 9], "values": ["01000000", "02000000"]},
		"upper-bounds": {"keys": [8, 9], "values": ["05000000", "0A000000"]}
	},
	"delete-file-references": [0],
	"residual-filter": {"type": "eq", "term": "id", "value": 34}
}`

const canonicalPositionDeleteJSON = `{
	"spec-id": 7,
	"partition": [34, "2026-07-17", "78797A21"],
	"content": "position-deletes",
	"file-path": "s3://warehouse/table/delete.parquet",
	"file-format": "parquet",
	"file-size-in-bytes": 512,
	"record-count": 5
}`

const rootFileScanTaskJSON = `{
	"data-file": {
		"spec-id": 0,
		"partition": [],
		"content": "data",
		"file-path": "s3://warehouse/table/root.parquet",
		"file-format": "parquet",
		"file-size-in-bytes": 100,
		"record-count": 10
	},
	"residual-filter": true
}`

const envelopeAFileScanTaskJSON = `{
	"data-file": {
		"spec-id": 0,
		"partition": [],
		"content": "data",
		"file-path": "s3://warehouse/table/envelope-a.parquet",
		"file-format": "parquet",
		"file-size-in-bytes": 200,
		"record-count": 20
	},
	"delete-file-references": [0],
	"residual-filter": true
}`

const envelopeAPositionDeleteJSON = `{
	"spec-id": 0,
	"partition": [],
	"content": "position-deletes",
	"file-path": "s3://warehouse/table/envelope-a-delete.parquet",
	"file-format": "parquet",
	"file-size-in-bytes": 20,
	"record-count": 2
}`

const envelopeAScanTasksJSON = `{
	"file-scan-tasks": [` + envelopeAFileScanTaskJSON + `],
	"delete-files": [` + envelopeAPositionDeleteJSON + `]
}`

const envelopeBFileScanTaskJSON = `{
	"data-file": {
		"spec-id": 0,
		"partition": [],
		"content": "data",
		"file-path": "s3://warehouse/table/envelope-b.parquet",
		"file-format": "parquet",
		"file-size-in-bytes": 300,
		"record-count": 30
	},
	"delete-file-references": [0],
	"residual-filter": true
}`

const envelopeBPositionDeleteJSON = `{
	"spec-id": 0,
	"partition": [],
	"content": "position-deletes",
	"file-path": "s3://warehouse/table/envelope-b-delete.parquet",
	"file-format": "parquet",
	"file-size-in-bytes": 30,
	"record-count": 3
}`

const envelopeBScanTasksJSON = `{
	"plan-tasks": ["grandchild"],
	"file-scan-tasks": [` + envelopeBFileScanTaskJSON + `],
	"delete-files": [` + envelopeBPositionDeleteJSON + `]
}`

const grandchildFileScanTaskJSON = `{
	"data-file": {
		"spec-id": 0,
		"partition": [],
		"content": "data",
		"file-path": "s3://warehouse/table/grandchild.parquet",
		"file-format": "parquet",
		"file-size-in-bytes": 400,
		"record-count": 40
	},
	"residual-filter": true
}`
