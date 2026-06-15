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
// planning (apache/iceberg-go#1178). The bodies are intentionally
// unimplemented; the file exists so the REST surface can be reviewed as Go.
// Endpoint capability discovery (Endpoint, SupportsEndpoint) lands separately
// in the Phase 0 PR and is intentionally not redeclared here.

package rest

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

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
func (c *Catalog) SupportsPlanTableScan() bool {
	panic("unimplemented: proposed API for #1178")
}

// SupportsFullRemoteScanPlanning reports whether the server advertised all four
// scan-planning endpoints (plan, fetch-result, cancel, fetch-tasks).
func (c *Catalog) SupportsFullRemoteScanPlanning() bool {
	panic("unimplemented: proposed API for #1178")
}

// --- table.ScanPlanner implementation ---------------------------------------

// SupportsRemoteScanPlanning reports whether this catalog can complete a remote
// plan end-to-end; backed by the split capability checks above.
func (c *Catalog) SupportsRemoteScanPlanning() bool {
	panic("unimplemented: proposed API for #1178")
}

// PlanFiles plans a scan server-side and returns tasks (and, optionally, a
// plan-scoped FileIO) for the table to read.
func (c *Catalog) PlanFiles(ctx context.Context, req table.ScanPlanningRequest) (table.ScanPlanningResult, error) {
	panic("unimplemented: proposed API for #1178")
}

// --- Low-level client methods -----------------------------------------------

// PlanTableScan submits a scan plan. The result is either completed inline,
// submitted (returns a plan-id to poll), or failed.
func (c *Catalog) PlanTableScan(ctx context.Context, ident table.Identifier, req PlanTableScanRequest) (PlanTableScanResponse, error) {
	panic("unimplemented: proposed API for #1178")
}

// FetchPlanningResult polls a previously submitted plan.
func (c *Catalog) FetchPlanningResult(ctx context.Context, ident table.Identifier, planID string) (FetchPlanningResultResponse, error) {
	panic("unimplemented: proposed API for #1178")
}

// CancelPlanning cancels a server-side plan. Callers should cancel on context
// cancellation using a detached context with a short timeout.
func (c *Catalog) CancelPlanning(ctx context.Context, ident table.Identifier, planID string) error {
	panic("unimplemented: proposed API for #1178")
}

// FetchScanTasks fetches the scan tasks for a plan-task handle returned by a
// completed plan.
func (c *Catalog) FetchScanTasks(ctx context.Context, ident table.Identifier, req FetchScanTasksRequest) (FetchScanTasksResponse, error) {
	panic("unimplemented: proposed API for #1178")
}

// WaitForPlan submits and polls a plan to completion using jittered backoff,
// cancelling the server-side plan if the context is cancelled.
func (c *Catalog) WaitForPlan(ctx context.Context, ident table.Identifier, planID string, opts WaitForPlanOptions) (FetchPlanningResultResponse, error) {
	panic("unimplemented: proposed API for #1178")
}

// --- Wire types (sketch) ----------------------------------------------------
//
// Field-complete request/response decoding (content-file JSON, residuals,
// storage credentials) lands with the scan-task decoder PR; these sketch the
// request/response envelopes so the client surface compiles and reads.

// PlanStatus is the status of a server-side plan.
type PlanStatus string

const (
	PlanStatusCompleted PlanStatus = "completed"
	PlanStatusSubmitted PlanStatus = "submitted"
	PlanStatusCancelled PlanStatus = "cancelled"
	PlanStatusFailed    PlanStatus = "failed"
)

// PlanTableScanRequest is the POST .../plan request body. Filter is the
// ExpressionParser-format JSON produced by iceberg.MarshalExpressionJSON.
type PlanTableScanRequest struct {
	SnapshotID        *int64          `json:"snapshot-id,omitempty"`
	StartSnapshotID   *int64          `json:"start-snapshot-id,omitempty"`
	EndSnapshotID     *int64          `json:"end-snapshot-id,omitempty"`
	Select            []string        `json:"select,omitempty"`
	Filter            json.RawMessage `json:"filter,omitempty"`
	CaseSensitive     *bool           `json:"case-sensitive,omitempty"`
	UseSnapshotSchema *bool           `json:"use-snapshot-schema,omitempty"`
}

// PlanTableScanResponse is the POST .../plan response envelope.
type PlanTableScanResponse struct {
	PlanStatus PlanStatus `json:"plan-status"`
	PlanID     *string    `json:"plan-id,omitempty"`
	// file-scan-tasks, delete-files, plan-tasks, storage-credentials decoded
	// by the scan-task decoder PR.
}

// FetchPlanningResultResponse is the GET .../plan/{plan-id} response envelope.
type FetchPlanningResultResponse struct {
	PlanStatus PlanStatus `json:"plan-status"`
}

// FetchScanTasksRequest is the POST .../tasks request body.
type FetchScanTasksRequest struct {
	PlanTask string `json:"plan-task"`
}

// FetchScanTasksResponse is the POST .../tasks response envelope.
type FetchScanTasksResponse struct{}

// WaitForPlanOptions tunes the polling loop. Defaults should be conservative.
type WaitForPlanOptions struct {
	MinDelay time.Duration
	MaxDelay time.Duration
	Timeout  time.Duration
}
