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

// Package planfake provides a deterministic in-process REST scan-planning
// server for tests. It intentionally uses independent JSON responses instead
// of the production REST wire structs.
package planfake

import (
	"encoding/json"
	"fmt"
	"net/http"
)

// Scan-planning endpoint strings as advertised by GET /v1/config. These are
// literal protocol fixtures, deliberately independent from catalog/rest's
// endpoint constants so a production wire-format regression cannot update
// both sides of a test unnoticed.
const (
	PlanTableScanEndpoint       = "POST /v1/{prefix}/namespaces/{namespace}/tables/{table}/plan"
	FetchPlanningResultEndpoint = "GET /v1/{prefix}/namespaces/{namespace}/tables/{table}/plan/{plan-id}"
	CancelPlanningEndpoint      = "DELETE /v1/{prefix}/namespaces/{namespace}/tables/{table}/plan/{plan-id}"
	FetchScanTasksEndpoint      = "POST /v1/{prefix}/namespaces/{namespace}/tables/{table}/tasks"
)

// Response is one HTTP response in a scenario. A zero Status means 200 OK.
// Header and Body are cloned when the server is created and again before a
// response is written, so callers may safely reuse or mutate their fixtures.
// When Gate is non-nil, the server waits for it to close or receive a value
// before writing the response. Cancelling the request releases the waiter;
// closing the Server aborts the gated connection without manufacturing a
// successful response.
type Response struct {
	Status int
	Header http.Header
	Body   json.RawMessage
	Gate   <-chan struct{}
}

// ResponseSequence is an ordered sequence of responses for one scenario key.
// Each request consumes the next response. Once exhausted, a sequence returns a
// non-retryable scenario error unless RepeatLast is true, in which case its final
// response is returned for every later request.
type ResponseSequence struct {
	Responses  []Response
	RepeatLast bool
}

// ExpectedTarget is the decoded REST planning target accepted by a Scenario.
// Namespace uses the REST namespace path representation (components joined by
// the unit separator), while Prefix and Table are decoded path values.
type ExpectedTarget struct {
	Prefix    string
	Namespace string
	Table     string
}

// Scenario declares the responses returned by a Server. ExpectedTarget is
// required for every planning operation; requests for another target, or a
// planning request made without an expected target, receive a non-retryable
// scenario error.
//
// Poll responses are keyed by decoded plan ID, task responses by the plan-task
// value in the request body, and cancel responses by decoded plan ID. A missing
// key, empty sequence, or exhausted non-repeating sequence returns a clear,
// non-retryable scenario error.
//
// An all-zero ConfigResponse or PlanResponse means that response was not
// configured. Use Response{Status: http.StatusOK} to model an intentional empty
// 200 response. Scenario errors fail the owning test during cleanup unless a
// test explicitly acknowledges their exact values with
// Server.AcknowledgeScenarioErrors.
type Scenario struct {
	ExpectedTarget  *ExpectedTarget
	ConfigResponse  Response
	PlanResponse    Response
	PollResponses   map[string]ResponseSequence
	TaskResponses   map[string]ResponseSequence
	CancelResponses map[string]ResponseSequence
}

// ConfigResponse returns a successful catalog configuration response that
// advertises endpoints exactly as supplied.
func ConfigResponse(endpoints ...string) Response {
	body, err := json.Marshal(struct {
		Defaults  map[string]string `json:"defaults"`
		Overrides map[string]string `json:"overrides"`
		Endpoints []string          `json:"endpoints"`
	}{
		Defaults:  map[string]string{},
		Overrides: map[string]string{},
		Endpoints: append([]string{}, endpoints...),
	})
	if err != nil {
		panic(fmt.Sprintf("planfake: encode config response: %v", err))
	}

	return Response{Status: http.StatusOK, Body: body}
}

func cloneResponse(resp Response) Response {
	resp.Header = resp.Header.Clone()
	resp.Body = append(json.RawMessage(nil), resp.Body...)

	return resp
}

func cloneScenario(scenario Scenario) Scenario {
	cloned := Scenario{
		ConfigResponse: cloneResponse(scenario.ConfigResponse),
		PlanResponse:   cloneResponse(scenario.PlanResponse),
	}
	if scenario.ExpectedTarget != nil {
		target := *scenario.ExpectedTarget
		cloned.ExpectedTarget = &target
	}

	if scenario.PollResponses != nil {
		cloned.PollResponses = make(map[string]ResponseSequence, len(scenario.PollResponses))
		for planID, sequence := range scenario.PollResponses {
			cloned.PollResponses[planID] = cloneResponseSequence(sequence)
		}
	}
	if scenario.TaskResponses != nil {
		cloned.TaskResponses = make(map[string]ResponseSequence, len(scenario.TaskResponses))
		for planTask, sequence := range scenario.TaskResponses {
			cloned.TaskResponses[planTask] = cloneResponseSequence(sequence)
		}
	}
	if scenario.CancelResponses != nil {
		cloned.CancelResponses = make(map[string]ResponseSequence, len(scenario.CancelResponses))
		for planID, sequence := range scenario.CancelResponses {
			cloned.CancelResponses[planID] = cloneResponseSequence(sequence)
		}
	}

	return cloned
}

func cloneResponseSequence(sequence ResponseSequence) ResponseSequence {
	cloned := ResponseSequence{
		Responses:  make([]Response, len(sequence.Responses)),
		RepeatLast: sequence.RepeatLast,
	}
	for i, response := range sequence.Responses {
		cloned.Responses[i] = cloneResponse(response)
	}

	return cloned
}
