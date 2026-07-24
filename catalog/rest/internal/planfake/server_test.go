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

package planfake

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestServerRoutesAndSequences(t *testing.T) {
	t.Parallel()

	srv := New(t, Scenario{
		ExpectedTarget: &ExpectedTarget{
			Prefix:    "catalog",
			Namespace: "db\x1fanalytics",
			Table:     "events",
		},
		ConfigResponse: ConfigResponse(
			PlanTableScanEndpoint,
			FetchPlanningResultEndpoint,
			CancelPlanningEndpoint,
			FetchScanTasksEndpoint,
		),
		PlanResponse: Response{Status: http.StatusOK, Body: json.RawMessage(`{"status":"submitted","plan-id":"a/b"}`)},
		PollResponses: map[string]ResponseSequence{
			"a/b": {Responses: []Response{
				{Body: json.RawMessage(`{"step":1}`)},
				{Body: json.RawMessage(`{"step":2}`)},
			}},
		},
		TaskResponses: map[string]ResponseSequence{
			"root-task": {Responses: []Response{
				{Body: json.RawMessage(`{"plan-tasks":["child-1"]}`)},
				{Body: json.RawMessage(`{"plan-tasks":["child-2"]}`)},
			}, RepeatLast: true},
		},
		CancelResponses: map[string]ResponseSequence{
			"a/b": {Responses: []Response{{Status: http.StatusNoContent}}},
		},
	})
	client := srv.server.Client()

	response, _ := doPlanFakeRequest(t, client, http.MethodGet, srv.URL()+"/v1/config", "")
	assert.Equal(t, http.StatusOK, response.StatusCode)

	response, _ = doPlanFakeRequest(t, client, http.MethodPost,
		srv.URL()+"/v1/catalog/namespaces/db%1Fanalytics/tables/events/plan", `{"snapshot-id":17}`)
	assert.Equal(t, http.StatusOK, response.StatusCode)

	for _, wantStep := range []int{1, 2} {
		response, body := doPlanFakeRequest(t, client, http.MethodGet,
			srv.URL()+"/v1/catalog/namespaces/db%1Fanalytics/tables/events/plan/a%2Fb", "")
		assert.Equal(t, http.StatusOK, response.StatusCode)
		assert.JSONEq(t, fmt.Sprintf(`{"step":%d}`, wantStep), string(body))
	}
	response, body := doPlanFakeRequest(t, client, http.MethodGet,
		srv.URL()+"/v1/catalog/namespaces/db%1Fanalytics/tables/events/plan/a%2Fb", "")
	assertPlanFakeScenarioError(t, response, body)
	assert.Contains(t, strings.ToLower(string(body)), "exhaust")
	require.NoError(t, srv.AcknowledgeScenarioErrors(
		`poll response sequence for "a/b" exhausted on request 3 after 2 configured responses`))

	for _, wantChild := range []string{"child-1", "child-2", "child-2"} {
		response, body := doPlanFakeRequest(t, client, http.MethodPost,
			srv.URL()+"/v1/catalog/namespaces/db%1Fanalytics/tables/events/tasks", `{"plan-task":"root-task"}`)
		assert.Equal(t, http.StatusOK, response.StatusCode)
		assert.JSONEq(t, fmt.Sprintf(`{"plan-tasks":[%q]}`, wantChild), string(body))
	}

	response, _ = doPlanFakeRequest(t, client, http.MethodDelete,
		srv.URL()+"/v1/catalog/namespaces/db%1Fanalytics/tables/events/plan/a%2Fb", "")
	assert.Equal(t, http.StatusNoContent, response.StatusCode)

	assert.Equal(t, 3, srv.PollCount("a/b"))
	assert.Equal(t, 3, srv.TaskCount("root-task"))
	assert.Equal(t, 1, srv.CancelCount("a/b"))

	requests := srv.Requests()
	require.Len(t, requests, 9)
	assert.Equal(t, "catalog", requests[1].Prefix)
	assert.Equal(t, "db\x1fanalytics", requests[1].Namespace)
	assert.Equal(t, "events", requests[1].Table)
	assert.Equal(t, "a/b", requests[2].PlanID)
	assert.Contains(t, requests[2].Path, "a%2Fb")
	assert.Equal(t, "root-task", requests[5].PlanTask)
}

func TestServerRejectsUnexpectedTarget(t *testing.T) {
	t.Parallel()

	srv := New(t, Scenario{
		ExpectedTarget: &ExpectedTarget{Prefix: "catalog", Namespace: "db", Table: "events"},
		PlanResponse:   Response{Status: http.StatusOK, Body: json.RawMessage(`{"status":"completed"}`)},
	})

	response, body := doPlanFakeRequest(t, srv.server.Client(), http.MethodPost,
		srv.URL()+"/v1/catalog/namespaces/db/tables/wrong-table/plan", `{}`)

	assertPlanFakeScenarioError(t, response, body)
	assert.Contains(t, strings.ToLower(string(body)), "target")
	requests := srv.Requests()
	require.Len(t, requests, 1)
	assert.Equal(t, "wrong-table", requests[0].Table)
	require.NoError(t, srv.AcknowledgeScenarioErrors(
		`planning target mismatch: got prefix "catalog", namespace "db", table "wrong-table"; `+
			`want prefix "catalog", namespace "db", table "events"`))
}

func TestServerRequiresExpectedTarget(t *testing.T) {
	t.Parallel()

	srv := New(t, Scenario{
		PlanResponse: Response{Status: http.StatusOK, Body: json.RawMessage(`{"status":"completed"}`)},
	})

	response, body := doPlanFakeRequest(t, srv.server.Client(), http.MethodPost,
		srv.URL()+"/v1/namespaces/db/tables/events/plan", `{}`)

	assertPlanFakeScenarioError(t, response, body)
	assert.Contains(t, string(body), "expected planning target is not configured")
	require.NoError(t, srv.AcknowledgeScenarioErrors("expected planning target is not configured"))
}

func TestServerRejectsNonCanonicalPaths(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name          string
		path          string
		scenarioError string
	}{
		{
			name:          "trailing config slash",
			path:          "/v1/config/",
			scenarioError: `invalid request path "/v1/config/": path must not have a trailing slash`,
		},
		{
			name:          "multiple leading slashes",
			path:          "//v1/config",
			scenarioError: `invalid request path "//v1/config": path must have exactly one leading slash`,
		},
		{
			name:          "empty prefix segment",
			path:          "/v1//namespaces/db/tables/events/plan",
			scenarioError: `invalid request path "/v1//namespaces/db/tables/events/plan": path must not contain empty segments`,
		},
		{
			name:          "trailing plan slash",
			path:          "/v1/namespaces/db/tables/events/plan/",
			scenarioError: `invalid request path "/v1/namespaces/db/tables/events/plan/": path must not have a trailing slash`,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			srv := New(t, Scenario{
				ExpectedTarget: &ExpectedTarget{Namespace: "db", Table: "events"},
				ConfigResponse: ConfigResponse(),
				PlanResponse:   Response{Status: http.StatusOK, Body: json.RawMessage(`{}`)},
			})
			method := http.MethodGet
			if strings.Contains(test.path, "/plan") {
				method = http.MethodPost
			}

			response, body := doPlanFakeRequest(t, srv.server.Client(), method, srv.URL()+test.path, `{}`)
			assertPlanFakeScenarioError(t, response, body)
			assert.Contains(t, strings.ToLower(string(body)), "invalid request path")
			require.NoError(t, srv.AcknowledgeScenarioErrors(test.scenarioError))
		})
	}
}

func TestServerRejectsRawDotPlanIDsButAcceptsEscapedValues(t *testing.T) {
	t.Parallel()

	srv := New(t, Scenario{
		ExpectedTarget: &ExpectedTarget{Namespace: "db", Table: "events"},
		PollResponses: map[string]ResponseSequence{
			".":  {Responses: []Response{{Status: http.StatusOK, Body: json.RawMessage(`{"plan-id":"."}`)}}},
			"..": {Responses: []Response{{Status: http.StatusOK, Body: json.RawMessage(`{"plan-id":".."}`)}}},
		},
	})
	client := srv.server.Client()
	const planPath = "/v1/namespaces/db/tables/events/plan/"

	for _, planID := range []string{".", ".."} {
		response, body := doPlanFakeRequest(t, client, http.MethodGet, srv.URL()+planPath+planID, "")
		assertPlanFakeScenarioError(t, response, body)
		assert.Contains(t, strings.ToLower(string(body)), "dot")
	}
	require.NoError(t, srv.AcknowledgeScenarioErrors(
		`invalid request path "/v1/namespaces/db/tables/events/plan/.": path must percent-encode dot segments`,
		`invalid request path "/v1/namespaces/db/tables/events/plan/..": path must percent-encode dot segments`,
	))

	for _, test := range []struct {
		escaped string
		planID  string
	}{
		{escaped: "%2E", planID: "."},
		{escaped: "%2E%2E", planID: ".."},
	} {
		response, body := doPlanFakeRequest(t, client, http.MethodGet, srv.URL()+planPath+test.escaped, "")
		assert.Equal(t, http.StatusOK, response.StatusCode)
		assert.JSONEq(t, fmt.Sprintf(`{"plan-id":%q}`, test.planID), string(body))
	}

	assert.Equal(t, 1, srv.PollCount("."))
	assert.Equal(t, 1, srv.PollCount(".."))
}

func TestServerCapturesRawQuery(t *testing.T) {
	t.Parallel()

	srv := New(t, Scenario{ConfigResponse: ConfigResponse()})
	const rawQuery = "warehouse=s3%3A%2F%2Fbucket%2Fwarehouse&token=a+b"
	response, _ := doPlanFakeRequest(t, srv.server.Client(), http.MethodGet,
		srv.URL()+"/v1/config?"+rawQuery, "")
	require.Equal(t, http.StatusOK, response.StatusCode)

	requests := srv.Requests()
	require.Len(t, requests, 1)
	assert.Equal(t, "/v1/config", requests[0].Path)
	assert.Equal(t, rawQuery, requests[0].RawQuery)
}

func TestServerRejectsUnconfiguredTopLevelResponses(t *testing.T) {
	t.Parallel()

	t.Run("config", func(t *testing.T) {
		t.Parallel()

		srv := New(t, Scenario{})
		response, body := doPlanFakeRequest(t, srv.server.Client(), http.MethodGet, srv.URL()+"/v1/config", "")

		assertPlanFakeScenarioError(t, response, body)
		assert.Contains(t, strings.ToLower(string(body)), "config")
		assert.Contains(t, strings.ToLower(string(body)), "configured")
		require.NoError(t, srv.AcknowledgeScenarioErrors(
			"config response is not configured; use an explicit 200 response to model an empty body"))
	})

	t.Run("plan", func(t *testing.T) {
		t.Parallel()

		srv := New(t, Scenario{
			ExpectedTarget: &ExpectedTarget{Namespace: "db", Table: "events"},
		})
		response, body := doPlanFakeRequest(t, srv.server.Client(), http.MethodPost,
			srv.URL()+"/v1/namespaces/db/tables/events/plan", `{}`)

		assertPlanFakeScenarioError(t, response, body)
		assert.Contains(t, strings.ToLower(string(body)), "plan")
		assert.Contains(t, strings.ToLower(string(body)), "configured")
		require.NoError(t, srv.AcknowledgeScenarioErrors(
			"plan response is not configured; use an explicit 200 response to model an empty body"))
	})

	t.Run("explicit empty responses", func(t *testing.T) {
		t.Parallel()

		srv := New(t, Scenario{
			ExpectedTarget: &ExpectedTarget{Namespace: "db", Table: "events"},
			ConfigResponse: Response{Status: http.StatusOK},
			PlanResponse:   Response{Status: http.StatusOK},
		})
		response, body := doPlanFakeRequest(t, srv.server.Client(), http.MethodGet, srv.URL()+"/v1/config", "")
		assert.Equal(t, http.StatusOK, response.StatusCode)
		assert.Empty(t, body)
		response, body = doPlanFakeRequest(t, srv.server.Client(), http.MethodPost,
			srv.URL()+"/v1/namespaces/db/tables/events/plan", `{}`)
		assert.Equal(t, http.StatusOK, response.StatusCode)
		assert.Empty(t, body)
	})
}

func TestServerGatedResponseReleasesOnRequestCancellation(t *testing.T) {
	t.Parallel()

	gate := make(chan struct{})
	srv := New(t, Scenario{
		ExpectedTarget: &ExpectedTarget{Namespace: "db", Table: "events"},
		PollResponses: map[string]ResponseSequence{
			"plan-1": {Responses: []Response{{
				Status: http.StatusOK,
				Body:   json.RawMessage(`{"status":"submitted"}`),
				Gate:   gate,
			}}},
		},
	})
	// Register after New so the gate closes before the server's LIFO cleanup if
	// an assertion fails while a handler is still waiting.
	t.Cleanup(func() { close(gate) })

	requestCtx, cancelRequest := context.WithCancel(t.Context())
	result := make(chan error, 1)
	go func() {
		req, err := http.NewRequestWithContext(requestCtx, http.MethodGet,
			srv.URL()+"/v1/namespaces/db/tables/events/plan/plan-1", nil)
		if err != nil {
			result <- err

			return
		}
		response, err := srv.server.Client().Do(req)
		if response != nil {
			_, _ = io.Copy(io.Discard, response.Body)
			_ = response.Body.Close()
		}
		result <- err
	}()

	waitCtx, cancelWait := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancelWait()
	require.NoError(t, srv.WaitForPollCount(waitCtx, "plan-1", 1))
	cancelRequest()

	select {
	case err := <-result:
		require.ErrorIs(t, err, context.Canceled)
	case <-waitCtx.Done():
		t.Fatalf("gated response did not release after request cancellation: %v", waitCtx.Err())
	}
}

func TestServerGatedResponseAbortsOnServerClose(t *testing.T) {
	t.Parallel()

	gate := make(chan struct{})
	srv := New(t, Scenario{
		ExpectedTarget: &ExpectedTarget{Namespace: "db", Table: "events"},
		PollResponses: map[string]ResponseSequence{
			"plan-1": {Responses: []Response{{
				Status: http.StatusOK,
				Body:   json.RawMessage(`{"status":"submitted"}`),
				Gate:   gate,
			}}},
		},
	})
	// If the behavior regresses, release the handler before New's server cleanup
	// runs so the failure is bounded instead of hanging the test process.
	t.Cleanup(func() { close(gate) })

	type requestResult struct {
		response *http.Response
		err      error
	}
	result := make(chan requestResult, 1)
	go func() {
		response, err := srv.server.Client().Get(
			srv.URL() + "/v1/namespaces/db/tables/events/plan/plan-1")
		result <- requestResult{response: response, err: err}
	}()

	waitCtx, cancelWait := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancelWait()
	require.NoError(t, srv.WaitForPollCount(waitCtx, "plan-1", 1))
	closeDone := make(chan struct{})
	go func() {
		srv.Close()
		close(closeDone)
	}()

	select {
	case got := <-result:
		if got.response != nil {
			_ = got.response.Body.Close()
		}
		assert.Nil(t, got.response, "server shutdown must not turn an interrupted gated response into an implicit 200")
		require.Error(t, got.err, "server shutdown must surface as a transport error")
	case <-waitCtx.Done():
		t.Fatalf("gated response did not abort after server shutdown: %v", waitCtx.Err())
	}

	select {
	case <-closeDone:
	case <-waitCtx.Done():
		t.Fatalf("server shutdown did not finish after aborting the gated response: %v", waitCtx.Err())
	}
}

func TestServerCloseAbortsHandlerBlockedReadingRequestBody(t *testing.T) {
	t.Parallel()

	srv := New(t, Scenario{
		ExpectedTarget: &ExpectedTarget{Namespace: "db", Table: "events"},
		PlanResponse:   Response{Status: http.StatusOK, Body: json.RawMessage(`{"status":"completed"}`)},
	})
	address := strings.TrimPrefix(srv.URL(), "http://")
	dialer := net.Dialer{Timeout: 5 * time.Second}
	connection, err := dialer.DialContext(t.Context(), "tcp", address)
	require.NoError(t, err)
	t.Cleanup(func() { _ = connection.Close() })

	// Receiving 100 Continue proves the handler attempted to read the body. Leave
	// the declared byte unsent so serveHTTP remains blocked inside io.ReadAll.
	_, err = io.WriteString(connection, "POST /v1/namespaces/db/tables/events/plan HTTP/1.1\r\n"+
		"Host: "+address+"\r\n"+
		"Content-Length: 1\r\n"+
		"Expect: 100-continue\r\n\r\n")
	require.NoError(t, err)
	require.NoError(t, connection.SetReadDeadline(time.Now().Add(5*time.Second)))
	reader := bufio.NewReader(connection)
	statusLine, err := reader.ReadString('\n')
	require.NoError(t, err)
	assert.Equal(t, "HTTP/1.1 100 Continue\r\n", statusLine)
	blankLine, err := reader.ReadString('\n')
	require.NoError(t, err)
	assert.Equal(t, "\r\n", blankLine)

	closeDone := make(chan struct{})
	go func() {
		srv.Close()
		close(closeDone)
	}()

	waitCtx, cancelWait := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancelWait()
	select {
	case <-closeDone:
	case <-waitCtx.Done():
		t.Fatalf("server shutdown remained blocked on an unfinished request body: %v", waitCtx.Err())
	}
	assert.Empty(t, srv.ScenarioErrors())
}

func TestServerDefensivelyCopiesScenarioAndHistory(t *testing.T) {
	t.Parallel()

	header := http.Header{"X-Fixture": {"original"}}
	body := json.RawMessage(`{"status":"completed","plan-id":"original"}`)
	scenario := Scenario{
		ExpectedTarget: &ExpectedTarget{Namespace: "db", Table: "events"},
		PlanResponse:   Response{Status: http.StatusOK, Header: header, Body: body},
	}
	srv := New(t, scenario)
	client := srv.server.Client()

	header.Set("X-Fixture", "mutated")
	body[0] = '['
	scenario.PlanResponse.Body = json.RawMessage(`{"status":"completed","plan-id":"replacement"}`)

	response, responseBody := doPlanFakeRequest(t, client, http.MethodPost,
		srv.URL()+"/v1/namespaces/db/tables/events/plan", `{"snapshot-id":17}`)
	assert.Equal(t, "original", response.Header.Get("X-Fixture"))
	assert.JSONEq(t, `{"status":"completed","plan-id":"original"}`, string(responseBody))

	requests := srv.Requests()
	require.Len(t, requests, 1)
	requests[0].Header.Set("X-Changed", "yes")
	requests[0].Body[0] = '['

	requests = srv.Requests()
	assert.Empty(t, requests[0].Header.Get("X-Changed"))
	assert.JSONEq(t, `{"snapshot-id":17}`, string(requests[0].Body))
}

func TestServerHandlesConcurrentPollAndTaskRequests(t *testing.T) {
	t.Parallel()

	const requestCount = 50
	srv := New(t, Scenario{
		ExpectedTarget: &ExpectedTarget{Namespace: "db", Table: "events"},
		PollResponses: map[string]ResponseSequence{
			"plan-1": {
				Responses:  []Response{{Body: json.RawMessage(`{"status":"submitted"}`)}},
				RepeatLast: true,
			},
		},
		TaskResponses: map[string]ResponseSequence{
			"root-task": {
				Responses:  []Response{{Body: json.RawMessage(`{"plan-tasks":[]}`)}},
				RepeatLast: true,
			},
		},
	})
	client := srv.server.Client()

	requestCtx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	errCh := make(chan error, requestCount)
	var wg sync.WaitGroup
	for range requestCount {
		wg.Add(1)
		go func() {
			defer wg.Done()

			for requestIndex, request := range []struct {
				method string
				url    string
				body   string
			}{
				{http.MethodGet, srv.URL() + "/v1/namespaces/db/tables/events/plan/plan-1", ""},
				{http.MethodPost, srv.URL() + "/v1/namespaces/db/tables/events/tasks", `{"plan-task":"root-task"}`},
			} {
				req, err := http.NewRequestWithContext(requestCtx, request.method, request.url, strings.NewReader(request.body))
				if err != nil {
					errCh <- fmt.Errorf("request %d: %w", requestIndex, err)

					return
				}
				response, err := client.Do(req)
				if err != nil {
					errCh <- fmt.Errorf("request %d: %w", requestIndex, err)

					return
				}
				_, readErr := io.Copy(io.Discard, response.Body)
				closeErr := response.Body.Close()
				if readErr != nil {
					errCh <- fmt.Errorf("request %d: reading response: %w", requestIndex, readErr)

					return
				}
				if closeErr != nil {
					errCh <- fmt.Errorf("request %d: closing response: %w", requestIndex, closeErr)

					return
				}
				if response.StatusCode != http.StatusOK {
					errCh <- fmt.Errorf("request %d: unexpected status: %s", requestIndex, response.Status)

					return
				}
			}
			errCh <- nil
		}()
	}
	wg.Wait()
	close(errCh)
	for err := range errCh {
		require.NoError(t, err)
	}

	assert.Equal(t, requestCount, srv.PollCount("plan-1"))
	assert.Equal(t, requestCount, srv.TaskCount("root-task"))
	assert.Len(t, srv.Requests(), requestCount*2)
}

func TestServerReturnsClearScenarioErrors(t *testing.T) {
	t.Parallel()

	srv := New(t, Scenario{
		ExpectedTarget: &ExpectedTarget{Namespace: "db", Table: "events"},
	})
	response, body := doPlanFakeRequest(t, srv.server.Client(), http.MethodPost,
		srv.URL()+"/v1/namespaces/db/tables/events/tasks", `{"plan-task":"missing"}`)

	assertPlanFakeScenarioError(t, response, body)
	assert.Contains(t, string(body), `no task responses configured for \"missing\"`)
	assert.Equal(t, 1, srv.TaskCount("missing"))
	require.Len(t, srv.ScenarioErrors(), 1)
	assert.Contains(t, srv.ScenarioErrors()[0], `no task responses configured for "missing"`)
	require.Error(t, srv.AcknowledgeScenarioErrors("wrong error"))
	assert.Equal(t, []string{`no task responses configured for "missing" (missing response sequence)`},
		srv.ScenarioErrors(), "a mismatched acknowledgement must leave the error pending")
	require.NoError(t, srv.AcknowledgeScenarioErrors(
		`no task responses configured for "missing" (missing response sequence)`))
	assert.Empty(t, srv.ScenarioErrors())
}

func TestConfigResponsePreservesAdvertisedEndpoints(t *testing.T) {
	t.Parallel()

	response := ConfigResponse(PlanTableScanEndpoint, FetchScanTasksEndpoint)
	var body struct {
		Defaults  map[string]string `json:"defaults"`
		Overrides map[string]string `json:"overrides"`
		Endpoints []string          `json:"endpoints"`
	}
	require.NoError(t, json.Unmarshal(response.Body, &body))

	assert.Empty(t, body.Defaults)
	assert.Empty(t, body.Overrides)
	assert.Equal(t, []string{PlanTableScanEndpoint, FetchScanTasksEndpoint}, body.Endpoints)

	var empty struct {
		Endpoints []string `json:"endpoints"`
	}
	require.NoError(t, json.Unmarshal(ConfigResponse().Body, &empty))
	assert.NotNil(t, empty.Endpoints)
	assert.Empty(t, empty.Endpoints)
}

func doPlanFakeRequest(
	t *testing.T,
	client *http.Client,
	method, url, body string,
) (*http.Response, []byte) {
	t.Helper()

	request, err := http.NewRequestWithContext(t.Context(), method, url, strings.NewReader(body))
	require.NoError(t, err)
	response, err := client.Do(request)
	require.NoError(t, err)
	responseBody, err := io.ReadAll(response.Body)
	require.NoError(t, err)
	require.NoError(t, response.Body.Close())

	return response, responseBody
}

func assertPlanFakeScenarioError(t *testing.T, response *http.Response, body []byte) {
	t.Helper()

	assert.GreaterOrEqual(t, response.StatusCode, http.StatusBadRequest)
	assert.Less(t, response.StatusCode, http.StatusInternalServerError)
	assert.NotEqual(t, http.StatusRequestTimeout, response.StatusCode)
	assert.NotEqual(t, http.StatusTooManyRequests, response.StatusCode)
	assert.Contains(t, string(body), "PlanFakeScenarioError")
}
