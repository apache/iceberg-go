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
// unimplemented; the file exists so the seam can be reviewed as Go rather
// than prose. Nothing here changes existing behavior.

package table

import (
	"context"

	"github.com/apache/iceberg-go"
)

// ScanPlanningMode selects how (*Scan).PlanFiles plans a scan. Local planning
// remains the default; remote planning is opt-in via WithScanPlanningMode.
type ScanPlanningMode string

const (
	// ScanPlanningLocal always plans locally by reading manifests through the
	// table's FileIO. This is the default and current behavior.
	ScanPlanningLocal ScanPlanningMode = "local"
	// ScanPlanningRemote requires a planner that advertises remote capability
	// and fails loudly if remote planning is unavailable.
	ScanPlanningRemote ScanPlanningMode = "remote"
	// ScanPlanningAuto uses remote planning when available and allowed by the
	// table config, otherwise falls back to local.
	ScanPlanningAuto ScanPlanningMode = "auto"
)

// WithScanPlanningMode sets the scan-planning mode for a scan. The default is
// ScanPlanningLocal unless the REST table config requires server planning.
func WithScanPlanningMode(mode ScanPlanningMode) ScanOption {
	panic("unimplemented: proposed API for #1178")
}

// ScanPlanningRequest is the input a Scan hands to a ScanPlanner. It carries
// the resolved scan state a planner needs without depending on catalog/rest.
//
// Open question (epic OQ4): when the table has evolved, UseSnapshotSchema must
// pin which schema binds a returned residual and the partition decode — the
// snapshot's schema (via schema-id), kept separate from each file's partition
// spec-id. Incremental scans (start/end snapshot) are deferred to a later
// phase; point-in-time SnapshotID lands first.
type ScanPlanningRequest struct {
	Identifier        Identifier
	Metadata          Metadata
	MetadataLocation  string
	SnapshotID        *int64
	SelectedFields    []string
	RowFilter         iceberg.BooleanExpression
	CaseSensitive     bool
	UseSnapshotSchema bool
}

// ScanPlanningResult is what a ScanPlanner returns.
//
// Open question (OQ1): how plan-scoped FileIO reaches ReadTasks across the
// PlanFiles -> ReadTasks boundary is unsettled. IO here is one provisional
// carrier; a live FileIO should not live on FileScanTask (it has a transport
// codec). Alternatives: a richer planned-result object, an internal plan
// context on Scan, or a serializable credential handle on FileScanTask.
type ScanPlanningResult struct {
	Tasks []FileScanTask
	IO    FSysF // PROVISIONAL carrier — see OQ1
}

// ScanPlanner plans scans for a table. rest.Catalog implements it; non-REST
// catalogs leave it nil and planning stays local.
//
// SupportsRemoteScanPlanning reports whether the planner can complete a remote
// plan end-to-end for the requested scan.
//
// Note: FileScanTask is proposed to gain a `Residual iceberg.BooleanExpression`
// field so remote tasks can carry the server's residual filter. That field is
// not added here because it would trip the codec/file_scan_task.go drift guard;
// it lands with the scan-task decoder PR.
type ScanPlanner interface {
	SupportsRemoteScanPlanning() bool
	PlanFiles(context.Context, ScanPlanningRequest) (ScanPlanningResult, error)
}
