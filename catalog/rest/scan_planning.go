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

// This file is a PROPOSED public API surface for REST server-side scan
// planning (apache/iceberg-go#1178). The method bodies are intentionally
// unimplemented stubs (returning ErrNotImplemented) so the REST surface can be
// reviewed as Go — with one exception: PlanTableScanResponse.UnmarshalJSON
// validates the status/plan-id union (with tests), added per review.
// Endpoint capability discovery (Endpoint, SupportsEndpoint) lands separately
// in the Phase 0 PR and is intentionally not redeclared here.

package rest

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/apache/iceberg-go"
	"github.com/apache/iceberg-go/table"
)

// Compile-time proof that the REST catalog satisfies the table planner seam.
var _ table.ScanPlanner = (*Catalog)(nil)

// ErrPlanExpired is returned when polling a plan-id the server no longer knows
// about (HTTP 404 while polling), distinct from a table-not-found 404.
var ErrPlanExpired = fmt.Errorf("%w: scan plan expired", ErrRESTError)

// --- Capability gating (Open Question 2) ------------------------------------
//
// A single capability check is too coarse: requiring all four endpoints falls
// back to local against sync-only servers, while requiring only the plan
// endpoint false-positives, because planTableScan can return `submitted` or
// `plan-tasks` that need the poll/fetch endpoints. The split below lets `auto`
// use a sync-only server while reserving the async/fanout path for servers
// that advertise everything.

// SupportsPlanTableScan reports whether the server advertised the synchronous
// plan endpoint.
func (r *Catalog) SupportsPlanTableScan() bool {
	return false
}

// SupportsFullRemoteScanPlanning reports whether the server advertised all four
// scan-planning endpoints (plan, fetch-result, cancel, fetch-tasks).
func (r *Catalog) SupportsFullRemoteScanPlanning() bool {
	return false
}

// --- table.ScanPlanner implementation ---------------------------------------

// SupportsRemoteScanPlanning reports whether this catalog can complete a remote
// plan end-to-end; backed by the split capability checks above.
func (r *Catalog) SupportsRemoteScanPlanning() bool {
	return false
}

// PlanFiles plans a scan server-side and returns tasks (and, optionally, a
// plan-scoped FileIO) for the table to read.
func (r *Catalog) PlanFiles(ctx context.Context, req table.ScanPlanningRequest) (table.ScanPlanningResult, error) {
	return table.ScanPlanningResult{}, fmt.Errorf("%w: REST scan planning", iceberg.ErrNotImplemented)
}

// --- Low-level client methods -----------------------------------------------

// PlanTableScan submits a scan plan. The result is either completed inline,
// submitted (returns a plan-id to poll), or failed.
func (r *Catalog) PlanTableScan(ctx context.Context, ident table.Identifier, req PlanTableScanRequest) (PlanTableScanResponse, error) {
	return PlanTableScanResponse{}, fmt.Errorf("%w: plan table scan", iceberg.ErrNotImplemented)
}

// FetchPlanningResult polls a previously submitted plan. opts.AccessDelegation
// is sent as the X-Iceberg-Access-Delegation header so an async poll can still
// receive plan-scoped storage credentials: the spec defines data-access on this
// endpoint, and the completed-async result is where those credentials are vended.
func (r *Catalog) FetchPlanningResult(ctx context.Context, ident table.Identifier, planID string, opts FetchPlanningResultOptions) (FetchPlanningResultResponse, error) {
	return FetchPlanningResultResponse{}, fmt.Errorf("%w: fetch planning result", iceberg.ErrNotImplemented)
}

// CancelPlanning cancels a server-side plan. Callers should cancel on context
// cancellation using a detached context with a short timeout.
func (r *Catalog) CancelPlanning(ctx context.Context, ident table.Identifier, planID string) error {
	return fmt.Errorf("%w: cancel planning", iceberg.ErrNotImplemented)
}

// FetchScanTasks fetches the scan tasks for a plan-task handle returned by a
// completed plan.
func (r *Catalog) FetchScanTasks(ctx context.Context, ident table.Identifier, req FetchScanTasksRequest) (FetchScanTasksResponse, error) {
	return FetchScanTasksResponse{}, fmt.Errorf("%w: fetch scan tasks", iceberg.ErrNotImplemented)
}

// WaitForPlan polls a submitted plan to completion using jittered backoff,
// cancelling the server-side plan if the context is cancelled. The total wait is
// bounded by the context deadline; it returns an error if the deadline passes
// while still submitted, or if the plan is cancelled, failed, or expired.
func (r *Catalog) WaitForPlan(ctx context.Context, ident table.Identifier, planID string, opts WaitForPlanOptions) (CompletedPlanningResult, error) {
	return CompletedPlanningResult{}, fmt.Errorf("%w: wait for plan", iceberg.ErrNotImplemented)
}

// --- Wire types (sketch) ----------------------------------------------------
//
// Content-file, delete-file, and residual decoding lands with the scan-task
// decoder PR; these sketch the request/response envelopes so the client
// surface compiles and reads.

// PlanStatus is the status of a server-side plan.
type PlanStatus string

const (
	PlanStatusCompleted PlanStatus = "completed"
	PlanStatusSubmitted PlanStatus = "submitted"
	// PlanStatusCancelled is valid when polling a submitted plan, but invalid
	// as a planTableScan response. PlanTableScan and WaitForPlan should treat a
	// cancelled initial planning response as an error.
	PlanStatusCancelled PlanStatus = "cancelled"
	PlanStatusFailed    PlanStatus = "failed"
)

// PlanningError is the REST ErrorModel payload carried by the error arm of a
// failed planning result. It mirrors the package's internal error wire shape
// but is a dedicated exported struct so it renders cleanly in godoc and does
// not leak an unexported type or its unexported fields into the public API.
type PlanningError struct {
	Message string   `json:"message"`
	Type    string   `json:"type"`
	Code    int      `json:"code"`
	Stack   []string `json:"stack,omitempty"`
}

// RESTFileScanTask is the REST FileScanTask wire payload. The REST prefix
// avoids confusion with table.FileScanTask, the decoded domain type. The
// scan-task decoder PR fills in the content-file and residual fields; the named
// type is committed here so ScanTasks does not expose json.RawMessage.
type RESTFileScanTask struct{}

// RESTDeleteFile is the REST DeleteFile wire payload. The scan-task decoder PR
// fills in the position/equality delete-file variants.
type RESTDeleteFile struct{}

// ScanTasks carries the task payload shared by completed planning responses and
// fetchScanTasks responses. Task/delete payload decoding lands with the
// scan-task decoder PR.
type ScanTasks struct {
	PlanTasks     []string           `json:"plan-tasks,omitempty"`
	FileScanTasks []RESTFileScanTask `json:"file-scan-tasks,omitempty"`
	DeleteFiles   []RESTDeleteFile   `json:"delete-files,omitempty"`
}

// CompletedPlanningResult is the completed arm of the planning-result union.
// planTableScan carries the plan-id on PlanTableScanResponse; fetchPlanningResult
// omits it.
type CompletedPlanningResult struct {
	Status PlanStatus `json:"status"`
	ScanTasks
	StorageCredentials []StorageCredential `json:"storage-credentials,omitempty"`
}

// PlanTableScanRequest is the POST .../plan request body. Filter is the
// ExpressionParser-format JSON produced by iceberg.MarshalExpressionJSON.
//
// Point-in-time only for now: the spec's incremental start-snapshot-id /
// end-snapshot-id fields are deliberately omitted and land with the incremental
// phase, together with the matching fields on table.ScanPlanningRequest, so the
// wire type and the seam stay in agreement.
type PlanTableScanRequest struct {
	// IdempotencyKey is sent as the Idempotency-Key header, not in the JSON body.
	// If set, it must be a UUID string; nil lets the implementation choose a
	// safe retry key.
	IdempotencyKey *string `json:"-"`
	// AccessDelegation is sent as the X-Iceberg-Access-Delegation header, not
	// in the JSON body. Nil uses the catalog default.
	AccessDelegation *string `json:"-"`

	SnapshotID        *int64          `json:"snapshot-id,omitempty"`
	Select            []string        `json:"select,omitempty"`
	Filter            json.RawMessage `json:"filter,omitempty"`
	MinRowsRequested  *int64          `json:"min-rows-requested,omitempty"`
	CaseSensitive     *bool           `json:"case-sensitive,omitempty"`
	UseSnapshotSchema *bool           `json:"use-snapshot-schema,omitempty"`
	StatsFields       []string        `json:"stats-fields,omitempty"`
}

// PlanTableScanResponse is the POST .../plan response. The spec models this as
// a `status`-discriminated union; the flat struct carries every arm's fields
// with omitempty so none are discarded. Task/delete payloads are filled in by
// the scan-task decoder PR. Per the spec, plan-id is required for both completed
// (CompletedPlanningWithIDResult) and submitted (AsyncPlanningResult) responses
// here; the wire decoder must validate PlanID != nil at unmarshal rather than
// rely on the omitempty pointer. A cancelled status is invalid for this endpoint
// and must be treated as an error. A failed status decodes successfully when it
// carries Error; callers must branch on Status before dereferencing PlanID.
type PlanTableScanResponse struct {
	Status PlanStatus     `json:"status"`
	PlanID *string        `json:"plan-id,omitempty"`
	Error  *PlanningError `json:"error,omitempty"`
	ScanTasks
	StorageCredentials []StorageCredential `json:"storage-credentials,omitempty"`
}

func (r *PlanTableScanResponse) UnmarshalJSON(data []byte) error {
	type planTableScanResponse PlanTableScanResponse
	var resp planTableScanResponse
	if err := json.Unmarshal(data, &resp); err != nil {
		return err
	}

	switch resp.Status {
	case PlanStatusCompleted, PlanStatusSubmitted:
		if resp.PlanID == nil {
			return fmt.Errorf("%w: planTableScan response with status %q missing plan-id", ErrRESTError, resp.Status)
		}
	case PlanStatusFailed:
		if resp.Error == nil {
			return fmt.Errorf("%w: planTableScan failed response missing error", ErrRESTError)
		}
	case PlanStatusCancelled:
		return fmt.Errorf("%w: planTableScan response has invalid status %q", ErrRESTError, resp.Status)
	default:
		return fmt.Errorf("%w: planTableScan response has unknown status %q", ErrRESTError, resp.Status)
	}

	*r = PlanTableScanResponse(resp)

	return nil
}

// FetchPlanningResultResponse is the GET .../plan/{plan-id} poll response. Same
// `status`-discriminated union (completed / submitted / cancelled / failed).
type FetchPlanningResultResponse struct {
	Status PlanStatus     `json:"status"`
	Error  *PlanningError `json:"error,omitempty"`
	ScanTasks
	StorageCredentials []StorageCredential `json:"storage-credentials,omitempty"`
}

// FetchScanTasksRequest is the POST .../tasks request body.
type FetchScanTasksRequest struct {
	// IdempotencyKey is sent as the Idempotency-Key header, not in the JSON body.
	// If set, it must be a UUID string; nil lets the implementation choose a
	// safe retry key.
	IdempotencyKey *string `json:"-"`

	PlanTask string `json:"plan-task"`
}

// FetchScanTasksResponse is the POST .../tasks response. May itself return more
// plan-tasks for further fanout. Task/delete payloads decoded by the
// scan-task decoder PR.
type FetchScanTasksResponse struct {
	ScanTasks
}

// DefaultWaitForPlanOptions is the conservative polling backoff used when
// callers pass the zero-value WaitForPlanOptions.
var DefaultWaitForPlanOptions = WaitForPlanOptions{
	MinDelay: 100 * time.Millisecond,
	MaxDelay: 5 * time.Second,
}

// WaitForPlanOptions tunes the polling backoff. The total wait is bounded by the
// caller's context deadline (context.WithTimeout). There is deliberately no
// Timeout field to avoid duplicating the context and the zero-value footgun.
// A zero MinDelay/MaxDelay uses DefaultWaitForPlanOptions.
type WaitForPlanOptions struct {
	MinDelay time.Duration
	MaxDelay time.Duration
	// AccessDelegation is sent as the X-Iceberg-Access-Delegation header on each
	// poll; nil uses the catalog default. Needed for async plans so the
	// completed poll can return vended storage credentials.
	AccessDelegation *string
}

// FetchPlanningResultOptions carries per-call headers for fetchPlanningResult.
type FetchPlanningResultOptions struct {
	// AccessDelegation is sent as the X-Iceberg-Access-Delegation header; nil
	// uses the catalog default. The spec defines data-access on this endpoint,
	// so an async poll needs it to receive vended storage credentials.
	AccessDelegation *string
}
