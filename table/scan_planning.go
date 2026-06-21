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
// planning (apache/iceberg-go#1178). It defines the table-side seam — the
// option, request/result types, and the ScanPlanner interface (implemented by
// catalog/rest) — so it can be reviewed as Go rather than prose.
// WithScanPlanningMode records the requested mode on the Scan, but nothing reads
// it until scanner delegation lands, so nothing here changes existing behavior.

package table

import (
	"context"

	"github.com/apache/iceberg-go"
	icebergio "github.com/apache/iceberg-go/io"
)

// ScanPlanningMode is the user-facing scan option: three values
// (local/remote/auto) selecting how (*Scan).PlanFiles plans a scan. Local
// planning remains the default; remote is opt-in via WithScanPlanningMode.
//
// This is deliberately distinct from the REST table-config key
// `scan-planning-mode` (values `client`/`server`), which is a server directive
// resolved separately (OQ4): a `client` table forces local planning, a `server`
// table forces remote planning, and explicit conflicting scan options fail
// fast. There is intentionally no fourth `server` value here; the directive
// lives in the table config, not the user option.
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
	return func(scan *Scan) { scan.planningMode = mode }
}

// ScanPlanningRequest is the input a Scan hands to a ScanPlanner. It carries
// the resolved scan state a planner needs without depending on catalog/rest.
//
// Open question (epic OQ4): when the table has evolved, UseSnapshotSchema must
// pin which schema binds a returned residual and the partition decode: the
// snapshot's schema (via schema-id), kept separate from each file's partition
// spec-id. Incremental scans (start/end snapshot) are deferred to a later
// phase; point-in-time SnapshotID lands first.
type ScanPlanningRequest struct {
	Identifier Identifier
	// Metadata is the full table metadata. This likely over-specifies the
	// contract: a planner needs only schema(s), partition specs, and snapshot
	// resolution; narrowing to a smaller interface is an open refinement.
	Metadata         Metadata
	MetadataLocation string
	SnapshotID       *int64
	SelectedFields   []string
	RowFilter        iceberg.BooleanExpression
	MinRowsRequested *int64
	StatsFields      []string
	// CaseSensitive must carry the Scan's value (which defaults to true), not
	// Go's false zero value, or the wire request would flip the spec default.
	// Nil means use the scan default.
	CaseSensitive *bool
	// UseSnapshotSchema is a pointer to distinguish the spec default from an
	// explicit false when the scanner-delegation phase binds it to table config.
	UseSnapshotSchema *bool
}

// PlanIO lazily loads the FileIO that should be used to read a planned scan.
// Nil means the scan should keep using the table's normal FileIO. Remote
// planners may return a PlanIO backed by plan-scoped storage credentials.
type PlanIO interface {
	Load(context.Context) (icebergio.IO, error)
}

// ScanPlanningResult is what a ScanPlanner returns.
type ScanPlanningResult struct {
	Tasks []FileScanTask
	IO    PlanIO
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

// Scan integration, added here as inert fields and completed in the
// scanner-delegation phase:
//
//	type Scan struct {
//		// ...existing fields...
//		planningMode ScanPlanningMode // set by WithScanPlanningMode; default ScanPlanningLocal
//		planner      ScanPlanner      // non-nil only when the catalog supplies one
//	}
//
// table.New sets Table.planner when the supplied CatalogIO also implements
// ScanPlanner; Table.Scan copies that planner into Scan. This chooses the
// Catalog -> Table -> Scan wiring now, while keeping (*Scan).PlanFiles on the
// existing local path until delegation lands.
//
// (*Scan).PlanFiles resolves planningMode and, for remote/auto with a capable
// planner, delegates to planner.PlanFiles; otherwise it runs the existing local
// path unchanged. The compile-time `var _ table.ScanPlanner = (*Catalog)(nil)`
// in catalog/rest proves the REST catalog satisfies the planner interface.
