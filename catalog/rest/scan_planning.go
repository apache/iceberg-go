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
	"net/url"
	"strings"
	"time"

	"github.com/apache/iceberg-go"
	"github.com/apache/iceberg-go/catalog"
	"github.com/apache/iceberg-go/table"
	"github.com/google/uuid"
)

// Compile-time proof that the REST catalog satisfies the table planner seam.
var _ table.ScanPlanner = (*Catalog)(nil)

// ErrPlanExpired is returned when polling a plan that the server no longer
// knows about: a fetchPlanningResult 404 whose error.type is exactly
// NoSuchPlanIdException. It is distinct from a table/namespace-gone 404
// (catalog.ErrNoSuchTable / catalog.ErrNoSuchNamespace) so the polling layer can
// tell retry-with-a-new-plan from abort. A bare or unrecognized 404 stays an
// ambiguous ErrRESTError rather than being guessed as an expiry.
var ErrPlanExpired = fmt.Errorf("%w: scan plan expired", ErrRESTError)

// ErrNoSuchPlanTask is returned when fetchScanTasks is called with a plan-task
// handle the server no longer knows about: a 404 whose error.type is
// NoSuchPlanTaskException. It is distinct from a table/namespace-gone 404 so a
// caller fanning out over plan-task handles can tell an expired handle from the
// table having vanished.
var ErrNoSuchPlanTask = fmt.Errorf("%w: scan plan task not found", ErrRESTError)

// ErrPlanFailed is returned by PlanTableScan and FetchPlanningResult when the
// server reports a failed plan. The returned error is a *PlanFailedError, so the
// structured PlanningError detail is reachable via errors.As.
var ErrPlanFailed = fmt.Errorf("%w: scan plan failed", ErrRESTError)

// ErrPlanCancelled is returned by FetchPlanningResult when polling a plan that
// was cancelled (by this client or another). Like a failed plan it is terminal,
// so it surfaces as an error rather than a (resp, nil) the if-err idiom skips.
var ErrPlanCancelled = fmt.Errorf("%w: scan plan cancelled", ErrRESTError)

// REST error.type values the scan-planning 404 responses carry. The spec models
// a 404 on these endpoints as one of several distinct not-found conditions;
// each maps to its own sentinel so the polling/fanout layer can tell a
// retry-able expiry from a table/namespace-gone abort. Only these recognized
// types get a specific sentinel; a bare or unrecognized 404 provides no basis to
// choose one over another and stays an ambiguous ErrRESTError (the status-code
// fallback). The strings are the ErrorModel `type` values from the REST OpenAPI
// spec (the exception class simple names Java's RESTCatalog raises).
const (
	errTypeNoSuchPlanID    = "NoSuchPlanIdException"
	errTypeNoSuchPlanTask  = "NoSuchPlanTaskException"
	errTypeNoSuchTable     = "NoSuchTableException"
	errTypeNoSuchNamespace = "NoSuchNamespaceException"
)

// planTableScanErrorTypes splits a POST .../plan 404 by error.type: the plan
// endpoint takes an identifier but no plan-id, so a recognized 404 is a missing
// table or namespace. An unrecognized 404 stays ErrRESTError.
var planTableScanErrorTypes = map[string]error{
	errTypeNoSuchTable:     catalog.ErrNoSuchTable,
	errTypeNoSuchNamespace: catalog.ErrNoSuchNamespace,
}

// fetchPlanningResultErrorTypes splits a GET .../plan/{plan-id} 404: a forgotten
// plan-id is retry-with-a-new-plan (ErrPlanExpired), while a gone table or
// namespace is an abort. An unrecognized 404 stays ErrRESTError.
var fetchPlanningResultErrorTypes = map[string]error{
	errTypeNoSuchPlanID:    ErrPlanExpired,
	errTypeNoSuchTable:     catalog.ErrNoSuchTable,
	errTypeNoSuchNamespace: catalog.ErrNoSuchNamespace,
}

// fetchScanTasksErrorTypes splits a POST .../tasks 404: an expired plan-task
// handle (ErrNoSuchPlanTask) is distinct from a gone table or namespace. An
// unrecognized 404 stays ErrRESTError.
var fetchScanTasksErrorTypes = map[string]error{
	errTypeNoSuchPlanTask:  ErrNoSuchPlanTask,
	errTypeNoSuchTable:     catalog.ErrNoSuchTable,
	errTypeNoSuchNamespace: catalog.ErrNoSuchNamespace,
}

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

// headerIdempotencyKey is the per-request idempotency key on the plan and tasks
// POSTs. The access-delegation header name lives in rest.go because it is also
// a session default.
const headerIdempotencyKey = "Idempotency-Key"

// --- Capability gating -------------------------------------------------------
//
// Capability is split into two predicates. SupportsPlanTableScan is the narrow
// "server can plan inline" check (plan endpoint only). SupportsFullRemoteScanPlanning
// is the endpoint-level "server advertises all four endpoints" check: an
// end-to-end plan can come back `submitted` or with `plan-tasks` that need the
// poll/cancel/fetch endpoints to finish, and auto mode has no second chance to
// fall back to local once it commits to remote, so a plan-only server must not
// count as end-to-end capable.
//
// SupportsRemoteScanPlanning is the table.ScanPlanner-facing predicate that
// table.Scan's auto mode routes on. It is deliberately gated to false while
// PlanFiles is an unimplemented stub: routing on endpoint capability alone would
// send an auto-mode scan into PlanFiles and surface ErrNotImplemented instead of
// falling back to local planning. It flips on with the PlanFiles phase.

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
// plan end-to-end. table.Scan's auto mode routes on it, calling PlanFiles when it
// is true, so it must stay false until PlanFiles is implemented — otherwise an
// auto-mode scan against a server advertising all four endpoints would fail with
// ErrNotImplemented instead of falling back to local planning.
//
// TODO(#1178): return SupportsFullRemoteScanPlanning() once PlanFiles is wired
// end-to-end. Until then, callers probing endpoint capability should use
// SupportsFullRemoteScanPlanning / SupportsPlanTableScan directly.
func (r *Catalog) SupportsRemoteScanPlanning() bool {
	return false
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
// Any other status — including the empty status of a 200 with no body, which
// bypasses the response UnmarshalJSON validation — returns an ErrRESTError so a
// malformed response cannot masquerade as an empty completed plan.
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
		map[int]error{http.StatusNotFound: ErrRESTError},
		withHeaders(headers), withErrorTypeOverride(planTableScanErrorTypes))
	if err != nil {
		return PlanTableScanResponse{}, err
	}
	switch resp.Status {
	case PlanStatusCompleted, PlanStatusSubmitted:
		return resp, nil
	case PlanStatusFailed:
		return PlanTableScanResponse{}, &PlanFailedError{Detail: resp.Error}
	default:
		return PlanTableScanResponse{}, fmt.Errorf(
			"%w: planTableScan response has invalid status %q", ErrRESTError, resp.Status)
	}
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
// ErrPlanCancelled. A 404 is split by the response error.type: a forgotten
// plan-id (NoSuchPlanIdException) is ErrPlanExpired, while a gone table or
// namespace is catalog.ErrNoSuchTable / catalog.ErrNoSuchNamespace, so the poller
// can tell retry-with-a-new-plan from abort. A bare or unrecognized 404 stays an
// ambiguous ErrRESTError rather than being guessed as an expiry.
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
		map[int]error{http.StatusNotFound: ErrRESTError},
		withHeaders(headers), withErrorTypeOverride(fetchPlanningResultErrorTypes))
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
// completed plan. A 404 is split by the response error.type: an expired
// plan-task handle (NoSuchPlanTaskException) is ErrNoSuchPlanTask, while a gone
// table or namespace is catalog.ErrNoSuchTable / catalog.ErrNoSuchNamespace, so
// a caller fanning out over handles can tell a handle expiry from the table
// vanishing. A bare or unrecognized 404 stays an ambiguous ErrRESTError. An
// empty 200 body is rejected (requireBody) so a truncated response is not read
// as a successfully completed empty task set.
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
		map[int]error{http.StatusNotFound: ErrRESTError},
		withHeaders(headers), withSuppressedHeaders(headerIcebergAccessDelegation),
		withErrorTypeOverride(fetchScanTasksErrorTypes), requireBody())
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

	// extra carries opaque, server-issued path segments (the plan-id). The spec
	// puts no character restriction on them, and baseURI.JoinPath path.Cleans its
	// arguments — so an unescaped id containing '/' would split into segments and
	// a '..'/'.' segment would resolve to a different endpoint (e.g. ".." on the
	// plan path lands on .../tasks). Escape each as a single literal path segment.
	params := make([]string, 0, 2+len(extra))
	params = append(params, ns, tbl)
	for _, seg := range extra {
		params = append(params, escapeOpaquePathSegment(seg))
	}

	return ep.reqPath(params...)
}

// escapeOpaquePathSegment percent-encodes an opaque string so it survives
// url.URL.JoinPath as a single literal path segment. url.PathEscape handles '/',
// '%', spaces, etc., but leaves a pure-dot segment ("." or "..") unescaped, which
// JoinPath's path.Clean would then resolve as a dot-segment; encode those dots so
// the segment is preserved verbatim.
func escapeOpaquePathSegment(s string) string {
	switch escaped := url.PathEscape(s); escaped {
	case ".":
		return "%2E"
	case "..":
		return "%2E%2E"
	default:
		return escaped
	}
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
	// uuid.Parse is lenient (its docs say it should not be used for validation):
	// it accepts the URN, brace, and un-hyphenated 32-char forms. The spec's
	// header schema pins this to the 36-char canonical hyphenated form, so reject
	// anything whose canonical rendering differs (case-insensitively) from the
	// input rather than forward a nonconforming encoding.
	if !strings.EqualFold(parsed.String(), *idempotencyKey) {
		return "", fmt.Errorf("%w: idempotency key %q must be a canonical hyphenated UUID",
			iceberg.ErrInvalidArgument, *idempotencyKey)
	}
	// The spec pins the header to UUIDv7, so reject other versions rather than
	// forward a key a timestamp-keyed server would treat as undefined. RFC 9562
	// UUIDv7 also uses the RFC 4122 variant, so require that too: a canonical
	// string can carry the version-7 nibble while setting non-RFC variant bits.
	if parsed.Version() != 7 || parsed.Variant() != uuid.RFC4122 {
		return "", fmt.Errorf("%w: idempotency key %q must be an RFC 4122 UUIDv7 (got v%d, variant %v)",
			iceberg.ErrInvalidArgument, *idempotencyKey, int(parsed.Version()), parsed.Variant())
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
	// If set, it must be a UUIDv7 string (RFC 9562). If nil, a fresh UUIDv7 is
	// generated per call and not returned, so passing nil and retrying on a
	// transient error sends a different key each attempt and defeats server-side
	// dedup: pass an explicit key to get idempotency across retries.
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
	// If set, it must be a UUIDv7 string (RFC 9562). If nil, a fresh UUIDv7 is
	// generated per call and not returned, so passing nil and retrying on a
	// transient error sends a different key each attempt and defeats server-side
	// dedup: pass an explicit key to get idempotency across retries.
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
