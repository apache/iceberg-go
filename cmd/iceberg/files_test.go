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

package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"os"
	"strings"
	"testing"

	"github.com/apache/iceberg-go"
	"github.com/apache/iceberg-go/catalog"
	iceio "github.com/apache/iceberg-go/io"
	"github.com/apache/iceberg-go/table"
	"github.com/pterm/pterm"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const filesTestMetadata = `{
    "format-version": 2,
    "table-uuid": "c8e7f6d4-03fe-4693-9a96-a0705ddf69c2",
    "location": "mem://default/table",
    "last-sequence-number": 1,
    "last-updated-ms": 1602638573590,
    "last-column-id": 1,
    "current-schema-id": 0,
    "schemas": [
        {"type": "struct", "schema-id": 0, "fields": [{"id": 1, "name": "id", "required": true, "type": "int"}]}
    ],
    "default-spec-id": 0,
    "partition-specs": [{"spec-id": 0, "fields": []}],
    "last-partition-id": 1000,
    "default-sort-order-id": 0,
    "sort-orders": [{"order-id": 0, "fields": []}],
    "properties": {},
    "current-snapshot-id": 100,
    "snapshots": [
        {
            "snapshot-id": 100,
            "timestamp-ms": 1602638573590,
            "sequence-number": 1,
            "schema-id": 0,
            "summary": {"operation": "append"}
        }
    ],
    "snapshot-log": [],
    "refs": {}
}`

// filesPlanner is a remote scan planner that returns fixed tasks. It also
// serves as the table's CatalogIO, which is how table.New finds a planner.
type filesPlanner struct {
	tasks []table.FileScanTask
	calls int
}

func (p *filesPlanner) SupportsRemoteScanPlanning() bool { return true }

func (p *filesPlanner) PlanFiles(context.Context, table.ScanPlanningRequest) (table.ScanPlanningResult, error) {
	p.calls++

	return table.ScanPlanningResult{Tasks: p.tasks}, nil
}

func (p *filesPlanner) LoadTable(context.Context, table.Identifier) (*table.Table, error) {
	return nil, errors.New("unexpected LoadTable call")
}

func (p *filesPlanner) CommitTable(context.Context, table.Identifier, []table.Requirement, []table.Update) (table.Metadata, string, error) {
	return nil, "", errors.New("unexpected CommitTable call")
}

type filesCatalog struct {
	catalog.Catalog
	tbl *table.Table
}

func (c *filesCatalog) LoadTable(context.Context, table.Identifier) (*table.Table, error) {
	return c.tbl, nil
}

type filesCapture struct {
	textOutput
	lastErr error
}

func (f *filesCapture) Error(err error) {
	f.lastErr = err
}

type filesTableOptions struct {
	directive  string
	noSnapshot bool
	// readManifests lets the table load its FileIO. Otherwise loading it fails
	// the test, which is how the server-directive cases prove files never
	// walked the manifests. The FileIO is still returned, so a regression
	// shows up as assertion failures rather than textOutput.Files exiting.
	readManifests bool
}

func newFilesTable(t *testing.T, planner *filesPlanner, opts filesTableOptions) *table.Table {
	t.Helper()

	metaJSON := filesTestMetadata
	if opts.noSnapshot {
		metaJSON = strings.Replace(metaJSON, `"current-snapshot-id": 100,`, "", 1)
	}
	meta, err := table.ParseMetadataBytes([]byte(metaJSON))
	require.NoError(t, err)

	fsF := func(ctx context.Context) (iceio.IO, error) {
		if !opts.readManifests {
			t.Error("files read the table's manifests locally")
		}

		return iceio.LoadFS(ctx, nil, "mem://default/table")
	}

	return table.New([]string{"db", "events"}, meta, "", fsF, planner,
		table.WithScanPlanningDirective(opts.directive, opts.directive != ""))
}

func filesTestDataFile(t *testing.T, content iceberg.ManifestEntryContent, path string) iceberg.DataFile {
	t.Helper()

	builder, err := iceberg.NewDataFileBuilder(*iceberg.UnpartitionedSpec, content, path,
		iceberg.ParquetFile, nil, nil, nil, 10, 100)
	require.NoError(t, err)

	return builder.Build()
}

func filesTestTasks(t *testing.T) []table.FileScanTask {
	t.Helper()

	first := filesTestDataFile(t, iceberg.EntryContentData, "mem://default/table/data/a.parquet")
	posDelete := filesTestDataFile(t, iceberg.EntryContentPosDeletes, "mem://default/table/data/a-deletes.parquet")
	second := filesTestDataFile(t, iceberg.EntryContentData, "mem://default/table/data/b.parquet")

	return []table.FileScanTask{
		{File: first, DeleteFiles: []iceberg.DataFile{posDelete}, Start: 0, Length: 50},
		{File: first, DeleteFiles: []iceberg.DataFile{posDelete}, Start: 50, Length: 50},
		{File: second, Length: 100},
	}
}

func runFilesCapture(t *testing.T, tbl *table.Table, cmd *FilesCmd) (out string, capture *filesCapture, exitCode int) {
	t.Helper()

	var buf bytes.Buffer
	pterm.SetDefaultOutput(&buf)
	pterm.DisableColor()
	t.Cleanup(func() { pterm.SetDefaultOutput(os.Stdout) })

	capture = &filesCapture{}
	exitCode = captureBranchTagExit(func() {
		runFiles(context.Background(), capture, &filesCatalog{tbl: tbl}, cmd)
	})

	return buf.String(), capture, exitCode
}

func TestRunFilesListsScanPlan(t *testing.T) {
	tests := []struct {
		name      string
		directive string
		mode      string
	}{
		{name: "server directive", directive: "server"},
		{name: "server directive with remote mode", directive: "server", mode: "remote"},
		{name: "remote mode", mode: "remote"},
		{name: "auto mode", mode: "auto"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			planner := &filesPlanner{tasks: filesTestTasks(t)}
			tbl := newFilesTable(t, planner, filesTableOptions{directive: tt.directive})

			out, capture, exitCode := runFilesCapture(t, tbl, &FilesCmd{TableID: "db.events", ScanPlanningMode: tt.mode})

			require.NoError(t, capture.lastErr)
			assert.Equal(t, 0, exitCode)
			assert.Equal(t, 1, planner.calls)
			assert.Contains(t, out, "Planned files: db.events")
			assert.Contains(t, out, "Snapshot 100, schema 0")
			assert.Equal(t, 1, strings.Count(out, "Datafile: mem://default/table/data/a.parquet"),
				"a file split into several tasks is listed once")
			assert.Contains(t, out, "Deletefile: mem://default/table/data/a-deletes.parquet")
			assert.Contains(t, out, "Datafile: mem://default/table/data/b.parquet")
		})
	}
}

func TestRunFilesWalksManifestsLocally(t *testing.T) {
	tests := []struct {
		name      string
		directive string
		mode      string
	}{
		{name: "no directive"},
		{name: "client directive", directive: "client"},
		{name: "local mode", mode: "local"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			planner := &filesPlanner{}
			tbl := newFilesTable(t, planner, filesTableOptions{directive: tt.directive, readManifests: true})

			out, capture, exitCode := runFilesCapture(t, tbl, &FilesCmd{TableID: "db.events", ScanPlanningMode: tt.mode})

			require.NoError(t, capture.lastErr)
			assert.Equal(t, 0, exitCode)
			assert.Zero(t, planner.calls)
			assert.Contains(t, out, "Snapshots: db.events")
			assert.Contains(t, out, "Snapshot 100, schema 0")
		})
	}
}

func TestRunFilesRejects(t *testing.T) {
	tests := []struct {
		name      string
		directive string
		cmd       FilesCmd
		wantErr   string
	}{
		{
			name:      "history under server directive",
			directive: "server",
			cmd:       FilesCmd{History: true},
			wantErr:   "--history reads every snapshot's manifests, which the catalog forbids (scan-planning-mode=server)",
		},
		{
			name:    "history with remote mode",
			cmd:     FilesCmd{History: true, ScanPlanningMode: "remote"},
			wantErr: "cannot be combined with --scan-planning-mode remote",
		},
		{
			name:      "local mode under server directive",
			directive: "server",
			cmd:       FilesCmd{ScanPlanningMode: "local"},
			wantErr:   "the catalog requires server-side scan planning (scan-planning-mode=server)",
		},
		{
			name:      "remote mode under client directive",
			directive: "client",
			cmd:       FilesCmd{ScanPlanningMode: "remote"},
			wantErr:   "the catalog requires client-side scan planning",
		},
		{
			name:      "unrecognized directive",
			directive: "sometimes",
			wantErr:   `unrecognized scan-planning-mode "sometimes"`,
		},
		{
			name:    "unknown mode",
			cmd:     FilesCmd{ScanPlanningMode: "server"},
			wantErr: `invalid --scan-planning-mode "server"`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			planner := &filesPlanner{tasks: filesTestTasks(t)}
			tbl := newFilesTable(t, planner, filesTableOptions{directive: tt.directive})
			tt.cmd.TableID = "db.events"

			_, capture, exitCode := runFilesCapture(t, tbl, &tt.cmd)

			require.Error(t, capture.lastErr)
			assert.Contains(t, capture.lastErr.Error(), tt.wantErr)
			assert.Equal(t, 1, exitCode)
			assert.Zero(t, planner.calls)
		})
	}
}

func TestRunFilesSkipsPlanningWithoutSnapshot(t *testing.T) {
	planner := &filesPlanner{tasks: filesTestTasks(t)}
	tbl := newFilesTable(t, planner, filesTableOptions{directive: "server", noSnapshot: true})

	out, capture, exitCode := runFilesCapture(t, tbl, &FilesCmd{TableID: "db.events"})

	require.NoError(t, capture.lastErr)
	assert.Equal(t, 0, exitCode)
	assert.Zero(t, planner.calls)
	assert.Contains(t, out, "Planned files: db.events")
	assert.NotContains(t, out, "Datafile:")
}

func TestJSONOutputPlannedFiles(t *testing.T) {
	oldStdout := os.Stdout
	r, w, err := os.Pipe()
	require.NoError(t, err)
	os.Stdout = w
	defer func() {
		os.Stdout = oldStdout
	}()

	tbl := newFilesTable(t, &filesPlanner{}, filesTableOptions{})

	jsonOutput{}.PlannedFiles(tbl, filesTestTasks(t))

	w.Close()
	var buf bytes.Buffer
	_, _ = buf.ReadFrom(r)

	var got struct {
		Snapshot struct {
			SnapshotID int64 `json:"snapshot-id"`
		} `json:"snapshot"`
		Files []plannedFile `json:"files"`
	}
	require.NoError(t, json.Unmarshal(buf.Bytes(), &got))
	assert.Equal(t, int64(100), got.Snapshot.SnapshotID)
	assert.Equal(t, []plannedFile{
		{
			Path:        "mem://default/table/data/a.parquet",
			DeleteFiles: []string{"mem://default/table/data/a-deletes.parquet"},
		},
		{Path: "mem://default/table/data/b.parquet"},
	}, got.Files)
}
