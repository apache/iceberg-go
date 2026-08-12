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
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"sync"
	"testing"
)

// Request is one request captured by the fake server. Path is the escaped wire
// path, which preserves opaque plan IDs containing slashes or dot segments.
// RawQuery is captured separately without decoding. Prefix, Namespace, Table,
// PlanID, and PlanTask are decoded routing values when applicable.
type Request struct {
	Method    string
	Path      string
	RawQuery  string
	Header    http.Header
	Body      []byte
	Prefix    string
	Namespace string
	Table     string
	PlanID    string
	PlanTask  string
}

// Server is a concurrency-safe in-process REST scan-planning server.
type Server struct {
	server    *httptest.Server
	closeOnce sync.Once

	mu             sync.Mutex
	scenario       Scenario
	requests       []Request
	scenarioErrors []string
	pollCounts     map[string]int
	taskCounts     map[string]int
	cancelCounts   map[string]int
	pollChanged    chan struct{}
	closed         chan struct{}
}

// New starts a fake server for scenario and registers cleanup with t.
func New(t testing.TB, scenario Scenario) *Server {
	t.Helper()

	s := &Server{
		scenario:     cloneScenario(scenario),
		pollCounts:   make(map[string]int),
		taskCounts:   make(map[string]int),
		cancelCounts: make(map[string]int),
		pollChanged:  make(chan struct{}),
		closed:       make(chan struct{}),
	}
	s.server = httptest.NewServer(http.HandlerFunc(s.serveHTTP))
	t.Cleanup(func() {
		s.Close()
		if scenarioErrors := s.ScenarioErrors(); len(scenarioErrors) != 0 {
			t.Errorf("planfake: unexpected scenario error(s):\n- %s", strings.Join(scenarioErrors, "\n- "))
		}
	})

	return s
}

// URL returns the fake server's base URL.
func (s *Server) URL() string { return s.server.URL }

// Close stops the fake server. It is safe to call more than once.
func (s *Server) Close() {
	s.closeOnce.Do(func() {
		close(s.closed)
		// httptest.Server.Close waits for active handlers and only force-closes
		// idle connections. Close the underlying HTTP server first so a handler
		// blocked reading an unfinished request body cannot hang test cleanup.
		_ = s.server.Config.Close()
		s.server.Close()
	})
}

// Requests returns a defensive copy of the complete request history.
func (s *Server) Requests() []Request {
	s.mu.Lock()
	defer s.mu.Unlock()

	requests := make([]Request, len(s.requests))
	for i, request := range s.requests {
		requests[i] = cloneRequest(request)
	}

	return requests
}

// ScenarioErrors returns a defensive copy of unacknowledged scenario-contract
// errors observed by the server. Any errors left pending fail the owning test
// during cleanup.
func (s *Server) ScenarioErrors() []string {
	s.mu.Lock()
	defer s.mu.Unlock()

	return append([]string(nil), s.scenarioErrors...)
}

// AcknowledgeScenarioErrors marks the currently pending scenario errors as
// expected only when they exactly equal want in order. A mismatch leaves every
// error pending so cleanup still fails, as do any errors observed later.
func (s *Server) AcknowledgeScenarioErrors(want ...string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if len(s.scenarioErrors) != len(want) {
		return fmt.Errorf("scenario errors mismatch: got %q, want %q", s.scenarioErrors, want)
	}
	for i := range want {
		if s.scenarioErrors[i] != want[i] {
			return fmt.Errorf("scenario errors mismatch: got %q, want %q", s.scenarioErrors, want)
		}
	}

	s.scenarioErrors = nil

	return nil
}

// PollCount returns the number of poll requests received for planID.
func (s *Server) PollCount(planID string) int {
	s.mu.Lock()
	defer s.mu.Unlock()

	return s.pollCounts[planID]
}

// WaitForPollCount waits until at least the requested number of poll requests
// have been received for planID. It returns ctx.Err if the context ends first,
// or http.ErrServerClosed if the server closes before the threshold is reached.
func (s *Server) WaitForPollCount(ctx context.Context, planID string, atLeast int) error {
	for {
		s.mu.Lock()
		if s.pollCounts[planID] >= atLeast {
			s.mu.Unlock()

			return nil
		}
		changed := s.pollChanged
		s.mu.Unlock()

		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-s.closed:
			// Prefer success if the threshold and shutdown happened together.
			s.mu.Lock()
			reached := s.pollCounts[planID] >= atLeast
			s.mu.Unlock()
			if reached {
				return nil
			}

			return http.ErrServerClosed
		case <-changed:
		}
	}
}

// TaskCount returns the number of task-fetch requests received for planTask.
func (s *Server) TaskCount(planTask string) int {
	s.mu.Lock()
	defer s.mu.Unlock()

	return s.taskCounts[planTask]
}

// CancelCount returns the number of cancellation requests received for planID.
func (s *Server) CancelCount(planID string) int {
	s.mu.Lock()
	defer s.mu.Unlock()

	return s.cancelCounts[planID]
}

type routeKind uint8

const (
	routeUnknown routeKind = iota
	routeConfig
	routePlan
	routePoll
	routeCancel
	routeTasks
)

type route struct {
	kind      routeKind
	prefix    string
	namespace string
	table     string
	planID    string
}

func (s *Server) serveHTTP(w http.ResponseWriter, req *http.Request) {
	select {
	case <-s.closed:
		panic(http.ErrAbortHandler)
	default:
	}

	body, err := io.ReadAll(req.Body)
	if err != nil {
		select {
		case <-s.closed:
			panic(http.ErrAbortHandler)
		default:
		}

		message := fmt.Sprintf("reading request body: %v", err)
		s.recordScenarioError(message)
		writeResponse(w, errorResponse(http.StatusBadRequest, "PlanFakeRequestError", message))

		return
	}

	route, routeErr := parseRoute(req.Method, req.URL.EscapedPath())
	captured := Request{
		Method:    req.Method,
		Path:      req.URL.EscapedPath(),
		RawQuery:  req.URL.RawQuery,
		Header:    req.Header.Clone(),
		Body:      append([]byte(nil), body...),
		Prefix:    route.prefix,
		Namespace: route.namespace,
		Table:     route.table,
		PlanID:    route.planID,
	}
	if routeErr != nil {
		s.record(captured)
		writeResponse(w, s.scenarioErrorResponse(fmt.Sprintf("invalid request path %q: %v", captured.Path, routeErr)))

		return
	}
	if route.kind == routeTasks {
		var taskRequest struct {
			PlanTask string `json:"plan-task"`
		}
		if err := json.Unmarshal(body, &taskRequest); err != nil {
			s.record(captured)
			message := fmt.Sprintf("decoding plan-task request: %v", err)
			s.recordScenarioError(message)
			writeResponse(w, errorResponse(http.StatusBadRequest, "PlanFakeRequestError", message))

			return
		}
		captured.PlanTask = taskRequest.PlanTask
	}

	response := s.responseFor(route, captured)
	if !s.waitForGate(req.Context(), response.Gate) {
		return
	}
	writeResponse(w, response)
}

func (s *Server) waitForGate(ctx context.Context, gate <-chan struct{}) bool {
	if gate == nil {
		return true
	}
	if ctx.Err() != nil {
		return false
	}
	select {
	case <-s.closed:
		panic(http.ErrAbortHandler)
	default:
	}

	select {
	case <-gate:
		if ctx.Err() != nil {
			return false
		}
		select {
		case <-s.closed:
			panic(http.ErrAbortHandler)
		default:
		}

		return true
	case <-ctx.Done():
		return false
	case <-s.closed:
		// Returning from a handler without writing would make net/http synthesize
		// an empty 200 response. ErrAbortHandler instead closes the connection
		// without logging a stack trace or manufacturing a successful response.
		panic(http.ErrAbortHandler)
	}
}

func (s *Server) record(request Request) {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.requests = append(s.requests, cloneRequest(request))
}

func (s *Server) recordScenarioError(message string) {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.scenarioErrors = append(s.scenarioErrors, message)
}

func (s *Server) scenarioErrorResponse(message string) Response {
	s.mu.Lock()
	defer s.mu.Unlock()

	return s.scenarioErrorResponseLocked(message)
}

// scenarioErrorResponseLocked records and returns a scenario error. The caller
// must hold s.mu.
func (s *Server) scenarioErrorResponseLocked(message string) Response {
	s.scenarioErrors = append(s.scenarioErrors, message)

	return scenarioError(message)
}

func (s *Server) responseFor(route route, request Request) Response {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.requests = append(s.requests, cloneRequest(request))
	if route.kind != routeConfig && route.kind != routeUnknown {
		if targetError := validateTarget(s.scenario.ExpectedTarget, route); targetError != "" {
			return s.scenarioErrorResponseLocked(targetError)
		}
	}

	switch route.kind {
	case routeConfig:
		if isZeroResponse(s.scenario.ConfigResponse) {
			return s.scenarioErrorResponseLocked(
				"config response is not configured; use an explicit 200 response to model an empty body")
		}

		return cloneResponse(s.scenario.ConfigResponse)
	case routePlan:
		if isZeroResponse(s.scenario.PlanResponse) {
			return s.scenarioErrorResponseLocked(
				"plan response is not configured; use an explicit 200 response to model an empty body")
		}

		return cloneResponse(s.scenario.PlanResponse)
	case routePoll:
		response := s.nextResponse("poll", route.planID, s.scenario.PollResponses, s.pollCounts)
		s.signalPollChangedLocked()

		return response
	case routeCancel:
		return s.nextResponse("cancel", route.planID, s.scenario.CancelResponses, s.cancelCounts)
	case routeTasks:
		return s.nextResponse("task", request.PlanTask, s.scenario.TaskResponses, s.taskCounts)
	default:
		return s.scenarioErrorResponseLocked(fmt.Sprintf("unexpected request %s %s", request.Method, request.Path))
	}
}

func (s *Server) nextResponse(
	kind, key string,
	responses map[string]ResponseSequence,
	counts map[string]int,
) Response {
	requestIndex := counts[key]
	counts[key] = requestIndex + 1

	sequence, ok := responses[key]
	if !ok {
		return s.scenarioErrorResponseLocked(
			fmt.Sprintf("no %s responses configured for %q (missing response sequence)", kind, key))
	}
	if len(sequence.Responses) == 0 {
		return s.scenarioErrorResponseLocked(
			fmt.Sprintf("empty %s response sequence configured for %q", kind, key))
	}
	if requestIndex >= len(sequence.Responses) {
		if !sequence.RepeatLast {
			return s.scenarioErrorResponseLocked(fmt.Sprintf(
				"%s response sequence for %q exhausted on request %d after %d configured responses",
				kind, key, requestIndex+1, len(sequence.Responses)))
		}
		requestIndex = len(sequence.Responses) - 1
	}

	return cloneResponse(sequence.Responses[requestIndex])
}

// signalPollChangedLocked wakes all poll-count waiters. The caller must hold
// s.mu so checking a count and subscribing to the next notification is atomic.
func (s *Server) signalPollChangedLocked() {
	close(s.pollChanged)
	s.pollChanged = make(chan struct{})
}

func validateTarget(expected *ExpectedTarget, actual route) string {
	if expected == nil {
		return "expected planning target is not configured"
	}
	if expected.Namespace == "" || expected.Table == "" {
		return "expected planning target must include namespace and table"
	}
	if actual.prefix == expected.Prefix && actual.namespace == expected.Namespace && actual.table == expected.Table {
		return ""
	}

	return fmt.Sprintf(
		"planning target mismatch: got prefix %q, namespace %q, table %q; want prefix %q, namespace %q, table %q",
		actual.prefix, actual.namespace, actual.table, expected.Prefix, expected.Namespace, expected.Table)
}

func isZeroResponse(response Response) bool {
	return response.Status == 0 && len(response.Header) == 0 && len(response.Body) == 0 && response.Gate == nil
}

func parseRoute(method, escapedPath string) (route, error) {
	segments, err := pathSegments(escapedPath)
	if err != nil {
		return route{}, err
	}
	if segments[0] != "v1" {
		return route{}, nil
	}
	if method == http.MethodGet && len(segments) == 2 && segments[1] == "config" {
		return route{kind: routeConfig}, nil
	}

	tailLength := 0
	kind := routeUnknown
	switch {
	case method == http.MethodPost && len(segments) >= 6 && segments[len(segments)-1] == "plan":
		tailLength, kind = 5, routePlan
	case method == http.MethodPost && len(segments) >= 6 && segments[len(segments)-1] == "tasks":
		tailLength, kind = 5, routeTasks
	case method == http.MethodGet && len(segments) >= 7 && segments[len(segments)-2] == "plan":
		tailLength, kind = 6, routePoll
	case method == http.MethodDelete && len(segments) >= 7 && segments[len(segments)-2] == "plan":
		tailLength, kind = 6, routeCancel
	default:
		return route{}, nil
	}

	namespaceIndex := len(segments) - tailLength
	tail := segments[namespaceIndex:]
	if namespaceIndex < 1 || tail[0] != "namespaces" || tail[2] != "tables" {
		return route{}, nil
	}

	matched := route{
		kind:      kind,
		prefix:    strings.Join(segments[1:namespaceIndex], "/"),
		namespace: tail[1],
		table:     tail[3],
	}
	if kind == routePoll || kind == routeCancel {
		matched.planID = tail[5]
	}

	return matched, nil
}

func pathSegments(escapedPath string) ([]string, error) {
	if !strings.HasPrefix(escapedPath, "/") || strings.HasPrefix(escapedPath, "//") {
		return nil, errors.New("path must have exactly one leading slash")
	}
	if strings.HasSuffix(escapedPath, "/") {
		return nil, errors.New("path must not have a trailing slash")
	}

	rawSegments := strings.Split(escapedPath[1:], "/")
	segments := make([]string, len(rawSegments))
	for i, segment := range rawSegments {
		if segment == "" {
			return nil, errors.New("path must not contain empty segments")
		}
		if segment == "." || segment == ".." {
			return nil, errors.New("path must percent-encode dot segments")
		}
		decoded, err := url.PathUnescape(segment)
		if err != nil {
			return nil, err
		}
		segments[i] = decoded
	}

	return segments, nil
}

func cloneRequest(request Request) Request {
	request.Header = request.Header.Clone()
	request.Body = append([]byte(nil), request.Body...)

	return request
}

func writeResponse(w http.ResponseWriter, response Response) {
	for key, values := range response.Header {
		for _, value := range values {
			w.Header().Add(key, value)
		}
	}
	if len(response.Body) != 0 && w.Header().Get("Content-Type") == "" {
		w.Header().Set("Content-Type", "application/json")
	}

	status := response.Status
	if status == 0 {
		status = http.StatusOK
	}
	w.WriteHeader(status)
	if len(response.Body) != 0 {
		_, _ = w.Write(response.Body)
	}
}

func errorResponse(status int, errorType, message string) Response {
	body, err := json.Marshal(map[string]any{
		"error": map[string]any{
			"message": message,
			"type":    errorType,
			"code":    status,
		},
	})
	if err != nil {
		panic(fmt.Sprintf("planfake: encode error response: %v", err))
	}

	return Response{Status: status, Body: body}
}

func scenarioError(message string) Response {
	return errorResponse(http.StatusUnprocessableEntity, "PlanFakeScenarioError", message)
}
