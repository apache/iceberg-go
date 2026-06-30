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

// This file contains the REST server-side scan planning client surface for
// apache/iceberg-go#1178. Low-level client methods and endpoint capability
// checks are implemented here; higher-level orchestration, scanner delegation,
// expression wrappers, and scan-task content decoding land in follow-up phases.

package rest

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"time"

	"github.com/apache/iceberg-go"
	"github.com/apache/iceberg-go/catalog"
	"github.com/apache/iceberg-go/table"
	"github.com/google/uuid"
)

// Compile-time proof that the REST catalog satisfies the table planner seam.
var _ table.ScanPlanner = (*Catalog)(nil)

// ErrPlanExpired is returned when polling a plan that the server no longer
// knows about (any HTTP 404 from fetchPlanningResult). This low-level method
// maps every 404 to ErrPlanExpired; splitting it from a table/namespace-gone
// 404 on the response error.type is deferred to the polling layer that needs
// the distinction.
var ErrPlanExpired = fmt.Errorf("%w: scan plan expired", ErrRESTError)

// ErrPlanFailed is returned by PlanTableScan and FetchPlanningResult when the
// server reports a failed plan. The returned error is a *PlanFailedError, so the
// structured PlanningError detail is reachable via errors.As.
var ErrPlanFailed = fmt.Errorf("%w: scan plan failed", ErrRESTError)

// ErrPlanCancelled is returned by FetchPlanningResult when polling a plan that
// was cancelled (by this client or another). Like a failed plan it is terminal,
// so it surfaces as an error rather than a (resp, nil) the if-err idiom skips.
var ErrPlanCancelled = fmt.Errorf("%w: scan plan cancelled", ErrRESTError)

// PlanFailedError carries the server's structured PlanningError detail for a
// failed plan. It satisfies errors.Is(err, ErrPlanFailed) so callers can branch
// on the failure with the if-err idiom while still reaching the detail via
// errors.As.
type PlanFailedError struct {
	Detail *PlanningError
}

func (e *PlanFailedError) Error() string {
	if e.Detail != nil && e.Detail.Message != "" {
		return ErrPlanFailed.Error() + ": " + e.Detail.Message
	}

	return ErrPlanFailed.Error()
}

func (e *PlanFailedError) Unwrap() error { return ErrPlanFailed }

// headerIdempotencyKey is the per-request retry key on the plan and tasks
// POSTs. The access-delegation header name lives in rest.go because it is also
// a session default.
const headerIdempotencyKey = "Idempotency-Key"

// --- Capability gating -------------------------------------------------------
//
// Capability is split into two predicates. SupportsPlanTableScan is the narrow
// "server can plan inline" check (plan endpoint only). SupportsRemoteScanPlanning
// is the end-to-end check used by table.Scan's auto mode to decide whether to
// route a scan to the server: it requires all four endpoints, because a plan can
// come back `submitted` or with `plan-tasks` that need the poll/cancel/fetch
// endpoints to finish — and auto mode has no second chance to fall back to local
// once it commits to remote. A plan-only server therefore must not advertise as
// end-to-end capable, or an auto-mode scan that gets a `submitted` reply would
// fail instead of planning locally.

// SupportsPlanTableScan reports whether the server advertised the synchronous
// plan endpoint.
func (r *Catalog) SupportsPlanTableScan() bool {
	return r.endpoints.contains(endpointPlanTableScan)
}

// SupportsFullRemoteScanPlanning reports whether the server advertised all four
// scan-planning endpoints (plan, fetch-result, cancel, fetch-tasks), i.e. it can
// drive the async/fanout path, not just sync inline planning.
func (r *Catalog) SupportsFullRemoteScanPlanning() bool {
	return r.SupportsPlanTableScan() &&
		r.endpoints.contains(endpointFetchPlanResult) &&
		r.endpoints.contains(endpointCancelPlanning) &&
		r.endpoints.contains(endpointFetchScanTasks)
}

// --- table.ScanPlanner implementation ---------------------------------------

// SupportsRemoteScanPlanning reports whether this catalog can complete a remote
// plan end-to-end, including a `submitted` reply that must be polled. It requires
// all four endpoints so table.Scan's auto mode only routes to the server when it
// can finish without a fallback. Callers wanting the narrow "can plan inline"
// signal should use SupportsPlanTableScan.
func (r *Catalog) SupportsRemoteScanPlanning() bool {
	return r.SupportsFullRemoteScanPlanning()
}

// PlanFiles plans a scan server-side and returns tasks (and, optionally, a
// plan-scoped FileIO) for the table to read.
func (r *Catalog) PlanFiles(ctx context.Context, req table.ScanPlanningRequest) (table.ScanPlanningResult, error) {
	return table.ScanPlanningResult{}, fmt.Errorf("%w: REST scan planning", iceberg.ErrNotImplemented)
}

// --- Low-level client methods -----------------------------------------------

// PlanTableScan submits a scan plan. A completed or submitted plan returns
// (resp, nil); callers branch on resp.Status for the plan-id (completed) vs
// poll (submitted) distinction. A failed plan returns a zero response and a
// non-nil *PlanFailedError (errors.Is(err, ErrPlanFailed)) so the if-err idiom
// does not mistake a failure for success; the server detail rides on the error.
func (r *Catalog) PlanTableScan(ctx context.Context, ident table.Identifier, req PlanTableScanRequest) (PlanTableScanResponse, error) {
	if err := r.endpoints.check(endpointPlanTableScan); err != nil {
		return PlanTableScanResponse{}, err
	}

	path, err := r.scanPlanningPath(endpointPlanTableScan, ident)
	if err != nil {
		return PlanTableScanResponse{}, err
	}

	headers, err := scanPlanningHeaders(req.IdempotencyKey, req.AccessDelegation, true)
	if err != nil {
		return PlanTableScanResponse{}, err
	}

	resp, err := doPost[PlanTableScanRequest, PlanTableScanResponse](
		ctx, r.baseURI, path, req, r.cl,
		map[int]error{http.StatusNotFound: catalog.ErrNoSuchTable}, withHeaders(headers))
	if err != nil {
		return PlanTableScanResponse{}, err
	}
	if resp.Status == PlanStatusFailed {
		return PlanTableScanResponse{}, &PlanFailedError{Detail: resp.Error}
	}

	return resp, nil
}

// FetchPlanningResult polls a previously submitted plan. opts.AccessDelegation
// is sent as the X-Iceberg-Access-Delegation header so an async poll can still
// receive plan-scoped storage credentials: the spec defines data-access on this
// endpoint, and the completed-async result is where those credentials are vended.
//
// completed and submitted return (resp, nil) and callers branch on resp.Status
// (done vs poll again). The terminal failure states surface as errors so an
// if-err poll loop cannot mistake them for an empty scan: failed returns a
// *PlanFailedError (errors.Is(err, ErrPlanFailed)) and cancelled returns
// ErrPlanCancelled. A 404 maps to ErrPlanExpired (the plan-id the server forgot).
func (r *Catalog) FetchPlanningResult(ctx context.Context, ident table.Identifier, planID string, opts FetchPlanningResultOptions) (FetchPlanningResultResponse, error) {
	if err := r.endpoints.check(endpointFetchPlanResult); err != nil {
		return FetchPlanningResultResponse{}, err
	}

	path, err := r.scanPlanningPath(endpointFetchPlanResult, ident, planID)
	if err != nil {
		return FetchPlanningResultResponse{}, err
	}

	headers, err := scanPlanningHeaders(nil, opts.AccessDelegation, false)
	if err != nil {
		return FetchPlanningResultResponse{}, err
	}

	resp, err := doGet[FetchPlanningResultResponse](
		ctx, r.baseURI, path, r.cl,
		map[int]error{http.StatusNotFound: ErrPlanExpired}, withHeaders(headers))
	if err != nil {
		return FetchPlanningResultResponse{}, err
	}
	switch resp.Status {
	case PlanStatusFailed:
		return FetchPlanningResultResponse{}, &PlanFailedError{Detail: resp.Error}
	case PlanStatusCancelled:
		return FetchPlanningResultResponse{}, ErrPlanCancelled
	}

	return resp, nil
}

// CancelPlanning cancels a server-side plan. Callers should cancel on context
// cancellation using a detached context with a short timeout. The spec supports
// idempotency and access-delegation headers on cancel; this low-level method
// deliberately defers those until a cancel options type is added, and suppresses
// the session-default access-delegation header (cancel vends no credentials). A
// 404 (already-expired or unknown plan) is not special-cased: cancel is
// best-effort, so the generic REST error is acceptable.
func (r *Catalog) CancelPlanning(ctx context.Context, ident table.Identifier, planID string) error {
	if err := r.endpoints.check(endpointCancelPlanning); err != nil {
		return err
	}

	path, err := r.scanPlanningPath(endpointCancelPlanning, ident, planID)
	if err != nil {
		return err
	}

	_, err = doDelete[struct{}](
		ctx, r.baseURI, path, r.cl, nil,
		withSuppressedHeaders(headerIcebergAccessDelegation))

	return err
}

// FetchScanTasks fetches the scan tasks for a plan-task handle returned by a
// completed plan.
func (r *Catalog) FetchScanTasks(ctx context.Context, ident table.Identifier, req FetchScanTasksRequest) (FetchScanTasksResponse, error) {
	if err := r.endpoints.check(endpointFetchScanTasks); err != nil {
		return FetchScanTasksResponse{}, err
	}

	path, err := r.scanPlanningPath(endpointFetchScanTasks, ident)
	if err != nil {
		return FetchScanTasksResponse{}, err
	}

	headers, err := scanPlanningHeaders(req.IdempotencyKey, nil, true)
	if err != nil {
		return FetchScanTasksResponse{}, err
	}

	return doPost[FetchScanTasksRequest, FetchScanTasksResponse](
		ctx, r.baseURI, path, req, r.cl,
		map[int]error{http.StatusNotFound: catalog.ErrNoSuchTable},
		withHeaders(headers), withSuppressedHeaders(headerIcebergAccessDelegation))
}

// WaitForPlan polls a submitted plan to completion using jittered backoff,
// cancelling the server-side plan if the context is cancelled. The total wait is
// bounded by the context deadline; it returns an error if the deadline passes
// while still submitted, or if the plan is cancelled, failed, or expired.
func (r *Catalog) WaitForPlan(ctx context.Context, ident table.Identifier, planID string, opts WaitForPlanOptions) (CompletedPlanningResult, error) {
	return CompletedPlanningResult{}, fmt.Errorf("%w: wait for plan", iceberg.ErrNotImplemented)
}

func (r *Catalog) scanPlanningPath(ep endpoint, ident table.Identifier, extra ...string) ([]string, error) {
	ns, tbl, err := r.splitIdentForPath(ident)
	if err != nil {
		return nil, err
	}

	return ep.reqPath(append([]string{ns, tbl}, extra...)...)
}

func scanPlanningHeaders(idempotencyKey, accessDelegation *string, includeIdempotency bool) (map[string]string, error) {
	headers := make(map[string]string, 2)
	if includeIdempotency {
		key, err := idempotencyHeaderValue(idempotencyKey)
		if err != nil {
			return nil, err
		}
		headers[headerIdempotencyKey] = key
	}
	if accessDelegation != nil {
		headers[headerIcebergAccessDelegation] = *accessDelegation
	}
	if len(headers) == 0 {
		return nil, nil
	}

	return headers, nil
}

func idempotencyHeaderValue(idempotencyKey *string) (string, error) {
	if idempotencyKey == nil {
		// Plan and task POSTs always send an idempotency key. The spec pins it
		// to a UUIDv7 string (RFC 9562) so a server can key its dedup window off
		// the embedded timestamp, matching Java's UUIDUtil.generateUuidV7(). A
		// v4 key would be treated as undefined by such servers. There is no
		// transport retry here, so nil means a fresh key for this call.
		key, err := uuid.NewV7()
		if err != nil {
			return "", fmt.Errorf("generating idempotency key: %w", err)
		}

		return key.String(), nil
	}

	parsed, err := uuid.Parse(*idempotencyKey)
	if err != nil {
		return "", fmt.Errorf("%w: invalid idempotency key %q", iceberg.ErrInvalidArgument, *idempotencyKey)
	}
	// The spec pins the header to UUIDv7, so reject other versions rather than
	// forward a key a timestamp-keyed server would treat as undefined.
	if parsed.Version() != 7 {
		return "", fmt.Errorf("%w: idempotency key %q must be a UUIDv7, got v%d",
			iceberg.ErrInvalidArgument, *idempotencyKey, int(parsed.Version()))
	}

	return *idempotencyKey, nil
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
	// If set, it must be a UUIDv7 string (RFC 9562); nil lets the implementation
	// generate a safe UUIDv7 retry key.
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

func (r *FetchPlanningResultResponse) UnmarshalJSON(data []byte) error {
	type fetchPlanningResultResponse FetchPlanningResultResponse
	var resp fetchPlanningResultResponse
	if err := json.Unmarshal(data, &resp); err != nil {
		return err
	}

	switch resp.Status {
	case PlanStatusCompleted, PlanStatusSubmitted, PlanStatusCancelled:
	case PlanStatusFailed:
		if resp.Error == nil {
			return fmt.Errorf("%w: fetchPlanningResult failed response missing error", ErrRESTError)
		}
	default:
		return fmt.Errorf("%w: fetchPlanningResult response has unknown status %q", ErrRESTError, resp.Status)
	}

	*r = FetchPlanningResultResponse(resp)

	return nil
}

// FetchScanTasksRequest is the POST .../tasks request body.
type FetchScanTasksRequest struct {
	// IdempotencyKey is sent as the Idempotency-Key header, not in the JSON body.
	// If set, it must be a UUIDv7 string (RFC 9562); nil lets the implementation
	// generate a safe UUIDv7 retry key.
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
