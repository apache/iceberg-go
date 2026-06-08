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
	"iter"
	"os"
	"os/exec"
	"testing"
	"time"

	"github.com/apache/iceberg-go"
	"github.com/apache/iceberg-go/catalog"
	"github.com/apache/iceberg-go/table"
	"github.com/pterm/pterm"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestSpecURL(t *testing.T) {
	assert.Contains(t, specURL(1), "version-1")
	assert.Contains(t, specURL(2), "version-2")
	assert.Contains(t, specURL(3), "version-3")
	assert.Equal(t, "https://iceberg.apache.org/spec/", specURL(99))
}

func TestTextOutputUpgradeResultDryRun(t *testing.T) {
	var buf bytes.Buffer
	pterm.SetDefaultOutput(&buf)
	pterm.DisableColor()
	t.Cleanup(func() { pterm.SetDefaultOutput(os.Stdout); pterm.EnableColor() })

	result := UpgradeResult{
		DryRun:          true,
		Table:           "db.events",
		PreviousVersion: 1,
		TargetVersion:   2,
		SpecURL:         specURL(2),
	}

	buf.Reset()
	textOutput{}.UpgradeResult(result)

	output := buf.String()
	assert.Contains(t, output, "[DRY RUN]")
	assert.Contains(t, output, "format version 1 to 2")
	assert.Contains(t, output, "version-2")
}

func TestTextOutputUpgradeResultCommitted(t *testing.T) {
	var buf bytes.Buffer
	pterm.SetDefaultOutput(&buf)
	pterm.DisableColor()
	t.Cleanup(func() { pterm.SetDefaultOutput(os.Stdout); pterm.EnableColor() })

	result := UpgradeResult{
		DryRun:          false,
		Table:           "db.events",
		PreviousVersion: 1,
		TargetVersion:   2,
		SpecURL:         specURL(2),
	}

	buf.Reset()
	textOutput{}.UpgradeResult(result)

	output := buf.String()
	assert.Contains(t, output, "Upgraded db.events")
	assert.Contains(t, output, "format version 1 to 2")
}

func TestJSONOutputUpgradeResult(t *testing.T) {
	r, w, err := os.Pipe()
	require.NoError(t, err)
	oldStdout := os.Stdout
	os.Stdout = w
	t.Cleanup(func() { w.Close(); os.Stdout = oldStdout })

	result := UpgradeResult{
		DryRun:          true,
		Table:           "db.events",
		PreviousVersion: 1,
		TargetVersion:   2,
		SpecURL:         "https://iceberg.apache.org/spec/#version-2-row-level-deletes",
	}

	jsonOutput{}.UpgradeResult(result)

	w.Close()
	var buf bytes.Buffer
	_, _ = buf.ReadFrom(r)

	output := buf.String()
	assert.Contains(t, output, `"dry_run":true`)
	assert.Contains(t, output, `"previous_version":1`)
	assert.Contains(t, output, `"target_version":2`)
}

func TestTextOutputRollbackResult(t *testing.T) {
	var buf bytes.Buffer
	pterm.SetDefaultOutput(&buf)
	pterm.DisableColor()
	t.Cleanup(func() { pterm.SetDefaultOutput(os.Stdout); pterm.EnableColor() })

	prevID := int64(100)
	result := RollbackResult{
		Table:                  "db.events",
		PreviousSnapshotID:     &prevID,
		RolledBackToSnapshotID: 50,
	}

	buf.Reset()
	textOutput{}.RollbackResult(result)

	output := buf.String()
	assert.Contains(t, output, "Rolled back db.events to snapshot 50")
	assert.Contains(t, output, "previous: 100")
}

func TestTextOutputRollbackResultNoPrevious(t *testing.T) {
	var buf bytes.Buffer
	pterm.SetDefaultOutput(&buf)
	pterm.DisableColor()
	t.Cleanup(func() { pterm.SetDefaultOutput(os.Stdout); pterm.EnableColor() })

	result := RollbackResult{
		Table:                  "db.events",
		PreviousSnapshotID:     nil,
		RolledBackToSnapshotID: 50,
	}

	buf.Reset()
	textOutput{}.RollbackResult(result)

	output := buf.String()
	assert.Contains(t, output, "previous: none")
}

func TestJSONOutputRollbackResult(t *testing.T) {
	r, w, err := os.Pipe()
	require.NoError(t, err)
	oldStdout := os.Stdout
	os.Stdout = w
	t.Cleanup(func() { w.Close(); os.Stdout = oldStdout })

	prevID := int64(100)
	result := RollbackResult{
		Table:                  "db.events",
		PreviousSnapshotID:     &prevID,
		RolledBackToSnapshotID: 50,
	}

	jsonOutput{}.RollbackResult(result)

	w.Close()
	var buf bytes.Buffer
	_, _ = buf.ReadFrom(r)

	output := buf.String()
	assert.Contains(t, output, `"table":"db.events"`)
	assert.Contains(t, output, `"previous_snapshot_id":100`)
	assert.Contains(t, output, `"rolled_back_to_snapshot_id":50`)
}

const upgradeRollbackTestMetadata = `{
    "format-version": 1,
    "table-uuid": "9c12d441-03fe-4693-9a96-a0705ddf69c1",
    "location": "s3://bucket/test/location",
    "last-updated-ms": 1602638573590,
    "last-column-id": 3,
    "schemas": [
        {
            "type": "struct",
            "schema-id": 0,
            "fields": [
                {"id": 1, "name": "x", "required": true, "type": "long"}
            ]
        }
    ],
    "current-schema-id": 0,
    "partition-specs": [{"spec-id": 0, "fields": []}],
    "default-spec-id": 0,
    "last-partition-id": 999,
    "sort-orders": [{"order-id": 0, "fields": []}],
    "default-sort-order-id": 0,
    "current-snapshot-id": 200,
    "snapshots": [
        {
            "snapshot-id": 100,
            "timestamp-ms": 1515100955770,
            "summary": {"operation": "append"},
            "manifest-list": "s3://a/b/1.avro"
        },
        {
            "snapshot-id": 200,
            "parent-snapshot-id": 100,
            "timestamp-ms": 1555100955770,
            "summary": {"operation": "append"},
            "manifest-list": "s3://a/b/2.avro"
        }
    ],
    "snapshot-log": [
        {"snapshot-id": 100, "timestamp-ms": 1515100955770},
        {"snapshot-id": 200, "timestamp-ms": 1555100955770}
    ]
}`

func TestResolveRollbackSnapshotIDExplicit(t *testing.T) {
	tbl := upgradeRollbackTestTable(t)

	got, err := resolveRollbackSnapshotID(tbl, &RollbackCmd{
		TableID:    "db.events",
		SnapshotID: int64Ptr(100),
	})

	require.NoError(t, err)
	assert.Equal(t, int64(100), got)
}

func TestResolveRollbackSnapshotIDTimestamp(t *testing.T) {
	tbl := upgradeRollbackTestTable(t)

	tests := []struct {
		name      string
		timestamp int64
		want      int64
	}{
		{
			name:      "exact first snapshot",
			timestamp: 1515100955770,
			want:      100,
		},
		{
			name:      "between snapshots",
			timestamp: 1555100955769,
			want:      100,
		},
		{
			name:      "exact second snapshot",
			timestamp: 1555100955770,
			want:      200,
		},
		{
			name:      "after latest snapshot",
			timestamp: 1555100955771,
			want:      200,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := resolveRollbackSnapshotID(tbl, &RollbackCmd{
				TableID:   "db.events",
				Timestamp: time.UnixMilli(tt.timestamp).UTC().Format(time.RFC3339Nano),
			})

			require.NoError(t, err)
			assert.Equal(t, tt.want, got)
		})
	}
}

// unsortedSnapshotsMetadata stores its snapshots array in descending timestamp
// order while the snapshot-log stays chronological. Timestamp resolution must
// follow the log, so a naive backward scan of the snapshots array (which would
// return 100 here) is provably wrong.
const unsortedSnapshotsMetadata = `{
    "format-version": 1,
    "table-uuid": "9c12d441-03fe-4693-9a96-a0705ddf69c1",
    "location": "s3://bucket/test/location",
    "last-updated-ms": 1602638573590,
    "last-column-id": 3,
    "schemas": [
        {"type": "struct", "schema-id": 0, "fields": [{"id": 1, "name": "x", "required": true, "type": "long"}]}
    ],
    "current-schema-id": 0,
    "partition-specs": [{"spec-id": 0, "fields": []}],
    "default-spec-id": 0,
    "last-partition-id": 999,
    "sort-orders": [{"order-id": 0, "fields": []}],
    "default-sort-order-id": 0,
    "current-snapshot-id": 200,
    "snapshots": [
        {"snapshot-id": 200, "parent-snapshot-id": 100, "timestamp-ms": 1555100955770, "summary": {"operation": "append"}, "manifest-list": "s3://a/b/2.avro"},
        {"snapshot-id": 100, "timestamp-ms": 1515100955770, "summary": {"operation": "append"}, "manifest-list": "s3://a/b/1.avro"}
    ],
    "snapshot-log": [
        {"snapshot-id": 100, "timestamp-ms": 1515100955770},
        {"snapshot-id": 200, "timestamp-ms": 1555100955770}
    ]
}`

func TestResolveRollbackSnapshotIDTimestampIgnoresSnapshotArrayOrder(t *testing.T) {
	meta, err := table.ParseMetadataBytes([]byte(unsortedSnapshotsMetadata))
	require.NoError(t, err)
	tbl := table.New([]string{"db", "events"}, meta, "", nil, nil)

	// A timestamp after the latest snapshot must resolve to the most recent one
	// (200) per the chronological log, not the trailing element of the snapshots
	// array (100).
	got, err := resolveRollbackSnapshotID(tbl, &RollbackCmd{
		TableID:   "db.events",
		Timestamp: time.UnixMilli(1565100955770).UTC().Format(time.RFC3339Nano),
	})

	require.NoError(t, err)
	assert.Equal(t, int64(200), got)
}

func TestValidateRollbackSelector(t *testing.T) {
	tests := []struct {
		name    string
		cmd     RollbackCmd
		wantErr string
	}{
		{
			name:    "snapshot id only",
			cmd:     RollbackCmd{SnapshotID: int64Ptr(100)},
			wantErr: "",
		},
		{
			name:    "timestamp only",
			cmd:     RollbackCmd{Timestamp: "2026-01-15T03:00:00Z"},
			wantErr: "",
		},
		{
			name:    "both selectors",
			cmd:     RollbackCmd{SnapshotID: int64Ptr(100), Timestamp: "2026-01-15T03:00:00Z"},
			wantErr: "mutually exclusive",
		},
		{
			name:    "no selector",
			cmd:     RollbackCmd{},
			wantErr: "exactly one",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := validateRollbackSelector(&tt.cmd)
			if tt.wantErr == "" {
				require.NoError(t, err)

				return
			}

			require.Error(t, err)
			assert.Contains(t, err.Error(), tt.wantErr)
		})
	}
}

// TestResolveRollbackSnapshotIDErrors covers resolution failures on already
// validated single-selector input (validateRollbackSelector is the caller's job).
func TestResolveRollbackSnapshotIDErrors(t *testing.T) {
	tbl := upgradeRollbackTestTable(t)

	tests := []struct {
		name    string
		cmd     RollbackCmd
		wantErr string
	}{
		{
			name:    "invalid timestamp",
			cmd:     RollbackCmd{TableID: "db.events", Timestamp: "not-a-time"},
			wantErr: "invalid --timestamp",
		},
		{
			name:    "timestamp before first snapshot",
			cmd:     RollbackCmd{TableID: "db.events", Timestamp: time.UnixMilli(1515100955769).UTC().Format(time.RFC3339Nano)},
			wantErr: "no snapshot found",
		},
		{
			name:    "missing snapshot id",
			cmd:     RollbackCmd{TableID: "db.events", SnapshotID: int64Ptr(999)},
			wantErr: "snapshot 999 not found",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := resolveRollbackSnapshotID(tbl, &tt.cmd)

			require.Error(t, err)
			assert.Contains(t, err.Error(), tt.wantErr)
		})
	}
}

type panicCatalog struct {
	tbl *table.Table
}

func (p *panicCatalog) CatalogType() catalog.Type { return "panic" }
func (p *panicCatalog) CreateTable(context.Context, table.Identifier, *iceberg.Schema, ...catalog.CreateTableOpt) (*table.Table, error) {
	panic("CreateTable must not be called")
}

func (p *panicCatalog) CommitTable(context.Context, table.Identifier, []table.Requirement, []table.Update) (table.Metadata, string, error) {
	panic("CommitTable must not be called")
}

func (p *panicCatalog) ListTables(context.Context, table.Identifier) iter.Seq2[table.Identifier, error] {
	panic("ListTables must not be called")
}

func (p *panicCatalog) LoadTable(_ context.Context, _ table.Identifier) (*table.Table, error) {
	return p.tbl, nil
}

func (p *panicCatalog) DropTable(context.Context, table.Identifier) error {
	panic("DropTable must not be called")
}

func (p *panicCatalog) RenameTable(context.Context, table.Identifier, table.Identifier) (*table.Table, error) {
	panic("RenameTable must not be called")
}

func (p *panicCatalog) CheckTableExists(context.Context, table.Identifier) (bool, error) {
	panic("CheckTableExists must not be called")
}

func (p *panicCatalog) ListNamespaces(context.Context, table.Identifier) ([]table.Identifier, error) {
	panic("ListNamespaces must not be called")
}

func (p *panicCatalog) CreateNamespace(context.Context, table.Identifier, iceberg.Properties) error {
	panic("CreateNamespace must not be called")
}

func (p *panicCatalog) DropNamespace(context.Context, table.Identifier) error {
	panic("DropNamespace must not be called")
}

func (p *panicCatalog) CheckNamespaceExists(context.Context, table.Identifier) (bool, error) {
	panic("CheckNamespaceExists must not be called")
}

func (p *panicCatalog) LoadNamespaceProperties(context.Context, table.Identifier) (iceberg.Properties, error) {
	panic("LoadNamespaceProperties must not be called")
}

func (p *panicCatalog) UpdateNamespaceProperties(context.Context, table.Identifier, []string, iceberg.Properties) (catalog.PropertiesUpdateSummary, error) {
	panic("UpdateNamespaceProperties must not be called")
}

func TestRunUpgradeDryRunDoesNotCommit(t *testing.T) {
	meta, err := table.ParseMetadataBytes([]byte(upgradeRollbackTestMetadata))
	require.NoError(t, err)

	tbl := table.New([]string{"db", "events"}, meta, "", nil, nil)
	cat := &panicCatalog{tbl: tbl}

	var buf bytes.Buffer
	pterm.SetDefaultOutput(&buf)
	pterm.DisableColor()
	t.Cleanup(func() { pterm.SetDefaultOutput(os.Stdout); pterm.EnableColor() })

	runUpgrade(context.Background(), textOutput{}, cat, &UpgradeCmd{
		TableID:       "db.events",
		FormatVersion: 2,
		DryRun:        true,
	})

	output := buf.String()
	assert.Contains(t, output, "[DRY RUN]")
	assert.Contains(t, output, "format version 1 to 2")
}

func TestRunUpgradeDryRunAlreadyAtVersion(t *testing.T) {
	meta, err := table.ParseMetadataBytes([]byte(upgradeRollbackTestMetadata))
	require.NoError(t, err)

	tbl := table.New([]string{"db", "events"}, meta, "", nil, nil)
	cat := &panicCatalog{tbl: tbl}

	var buf bytes.Buffer
	pterm.SetDefaultOutput(&buf)
	pterm.DisableColor()
	t.Cleanup(func() { pterm.SetDefaultOutput(os.Stdout); pterm.EnableColor() })

	runUpgrade(context.Background(), textOutput{}, cat, &UpgradeCmd{
		TableID:       "db.events",
		FormatVersion: 1,
		DryRun:        true,
	})

	output := buf.String()
	assert.Contains(t, output, "[DRY RUN]")
	assert.Contains(t, output, "format version 1 to 1")
}

func TestConfirmActionNonTTYWithoutYes(t *testing.T) {
	err := confirmAction("do something?", false)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "stdin is not a terminal")
}

func TestRunRollbackRejectsNonAncestor(t *testing.T) {
	const metaWithBranch = `{
    "format-version": 2,
    "table-uuid": "9c12d441-03fe-4693-9a96-a0705ddf69c1",
    "location": "s3://bucket/test/location",
    "last-sequence-number": 2,
    "last-updated-ms": 1602638573590,
    "last-column-id": 1,
    "schemas": [{"type": "struct", "schema-id": 0, "fields": [{"id": 1, "name": "x", "required": true, "type": "long"}]}],
    "current-schema-id": 0,
    "partition-specs": [{"spec-id": 0, "fields": []}],
    "default-spec-id": 0,
    "last-partition-id": 999,
    "sort-orders": [{"order-id": 0, "fields": []}],
    "default-sort-order-id": 0,
    "current-snapshot-id": 200,
    "snapshots": [
        {"snapshot-id": 100, "timestamp-ms": 1515100955770, "sequence-number": 0, "summary": {"operation": "append"}, "manifest-list": "s3://a/b/1.avro"},
        {"snapshot-id": 200, "parent-snapshot-id": 100, "timestamp-ms": 1555100955770, "sequence-number": 1, "summary": {"operation": "append"}, "manifest-list": "s3://a/b/2.avro"},
        {"snapshot-id": 300, "timestamp-ms": 1565100955770, "sequence-number": 2, "summary": {"operation": "append"}, "manifest-list": "s3://a/b/3.avro"}
    ],
    "snapshot-log": [
        {"snapshot-id": 100, "timestamp-ms": 1515100955770},
        {"snapshot-id": 200, "timestamp-ms": 1555100955770}
    ],
    "refs": {"main": {"snapshot-id": 200, "type": "branch"}}
}`

	meta, err := table.ParseMetadataBytes([]byte(metaWithBranch))
	require.NoError(t, err)

	tbl := table.New([]string{"db", "events"}, meta, "", nil, nil)
	cat := &panicCatalog{tbl: tbl}

	var errOut errCapture
	exitCode := captureExit(func() {
		runRollback(context.Background(), &errOut, cat, &RollbackCmd{
			TableID:    "db.events",
			SnapshotID: int64Ptr(300),
			Yes:        true,
		})
	})

	assert.Equal(t, 1, exitCode)
	assert.Contains(t, errOut.lastErr.Error(), "not an ancestor")
}

func TestRunRollbackRejectsInvalidSelectorsBeforeLoad(t *testing.T) {
	tests := []struct {
		name    string
		cmd     RollbackCmd
		wantErr string
	}{
		{
			name:    "no selector",
			cmd:     RollbackCmd{TableID: "db.events"},
			wantErr: "exactly one",
		},
		{
			name: "snapshot id and timestamp",
			cmd: RollbackCmd{
				TableID:    "db.events",
				SnapshotID: int64Ptr(100),
				Timestamp:  time.UnixMilli(1515100955770).UTC().Format(time.RFC3339),
			},
			wantErr: "mutually exclusive",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var errOut errCapture
			exitCode := captureExit(func() {
				runRollback(context.Background(), &errOut, nil, &tt.cmd)
			})

			assert.Equal(t, 1, exitCode)
			require.Error(t, errOut.lastErr)
			assert.Contains(t, errOut.lastErr.Error(), tt.wantErr)
		})
	}
}

func TestResolveRollbackSnapshotIDTimestampIgnoresOffLineageSnapshot(t *testing.T) {
	const metaWithBranch = `{
    "format-version": 2,
    "table-uuid": "9c12d441-03fe-4693-9a96-a0705ddf69c1",
    "location": "s3://bucket/test/location",
    "last-sequence-number": 2,
    "last-updated-ms": 1602638573590,
    "last-column-id": 1,
    "schemas": [{"type": "struct", "schema-id": 0, "fields": [{"id": 1, "name": "x", "required": true, "type": "long"}]}],
    "current-schema-id": 0,
    "partition-specs": [{"spec-id": 0, "fields": []}],
    "default-spec-id": 0,
    "last-partition-id": 999,
    "sort-orders": [{"order-id": 0, "fields": []}],
    "default-sort-order-id": 0,
    "current-snapshot-id": 200,
    "snapshots": [
        {"snapshot-id": 100, "timestamp-ms": 1515100955770, "sequence-number": 0, "summary": {"operation": "append"}, "manifest-list": "s3://a/b/1.avro"},
        {"snapshot-id": 200, "parent-snapshot-id": 100, "timestamp-ms": 1555100955770, "sequence-number": 1, "summary": {"operation": "append"}, "manifest-list": "s3://a/b/2.avro"},
        {"snapshot-id": 300, "timestamp-ms": 1565100955770, "sequence-number": 2, "summary": {"operation": "append"}, "manifest-list": "s3://a/b/3.avro"}
    ],
    "snapshot-log": [
        {"snapshot-id": 100, "timestamp-ms": 1515100955770},
        {"snapshot-id": 200, "timestamp-ms": 1555100955770}
    ],
    "refs": {"main": {"snapshot-id": 200, "type": "branch"}}
}`

	meta, err := table.ParseMetadataBytes([]byte(metaWithBranch))
	require.NoError(t, err)

	tbl := table.New([]string{"db", "events"}, meta, "", nil, nil)

	// Snapshot 300 exists in the snapshots array with the latest timestamp but is
	// absent from the snapshot-log: it is off the main lineage. Timestamp
	// resolution follows the log, so a timestamp equal to 300's must still resolve
	// to the latest in-lineage snapshot (200), never the off-lineage 300.
	got, err := resolveRollbackSnapshotID(tbl, &RollbackCmd{
		TableID:   "db.events",
		Timestamp: time.UnixMilli(1565100955770).UTC().Format(time.RFC3339Nano),
	})

	require.NoError(t, err)
	assert.Equal(t, int64(200), got)
}

// TestRunRollbackByTimestampUsesResolvedSnapshotID drives the full runRollback
// path with a --timestamp selector and asserts the snapshot resolved from the
// log (200) — not the current snapshot or the raw flag — is what flows into the
// rollback decision. The fixture's main lineage diverges (100 -> 200 -> back to
// 100), so a timestamp that resolves to 200 is no longer an ancestor of the
// current snapshot (100); runRollback must surface 200 in that error, proving
// the resolved ID is threaded through. The actual commit is exercised by the
// table package, since it requires a real filesystem to rebuild manifest lists.
func TestRunRollbackByTimestampUsesResolvedSnapshotID(t *testing.T) {
	const divergentLogMetadata = `{
    "format-version": 2,
    "table-uuid": "9c12d441-03fe-4693-9a96-a0705ddf69c1",
    "location": "s3://bucket/test/location",
    "last-sequence-number": 2,
    "last-updated-ms": 1602638573590,
    "last-column-id": 1,
    "schemas": [{"type": "struct", "schema-id": 0, "fields": [{"id": 1, "name": "x", "required": true, "type": "long"}]}],
    "current-schema-id": 0,
    "partition-specs": [{"spec-id": 0, "fields": []}],
    "default-spec-id": 0,
    "last-partition-id": 999,
    "sort-orders": [{"order-id": 0, "fields": []}],
    "default-sort-order-id": 0,
    "current-snapshot-id": 100,
    "snapshots": [
        {"snapshot-id": 100, "timestamp-ms": 1515100955770, "sequence-number": 0, "summary": {"operation": "append"}, "manifest-list": "s3://a/b/1.avro"},
        {"snapshot-id": 200, "parent-snapshot-id": 100, "timestamp-ms": 1555100955770, "sequence-number": 1, "summary": {"operation": "append"}, "manifest-list": "s3://a/b/2.avro"}
    ],
    "snapshot-log": [
        {"snapshot-id": 100, "timestamp-ms": 1515100955770},
        {"snapshot-id": 200, "timestamp-ms": 1555100955770},
        {"snapshot-id": 100, "timestamp-ms": 1565100955770}
    ],
    "refs": {"main": {"snapshot-id": 100, "type": "branch"}}
}`

	meta, err := table.ParseMetadataBytes([]byte(divergentLogMetadata))
	require.NoError(t, err)

	tbl := table.New([]string{"db", "events"}, meta, "", nil, nil)
	cat := &panicCatalog{tbl: tbl}

	var errOut errCapture
	exitCode := captureExit(func() {
		runRollback(context.Background(), &errOut, cat, &RollbackCmd{
			TableID:   "db.events",
			Timestamp: time.UnixMilli(1555100955770).UTC().Format(time.RFC3339Nano),
			Yes:       true,
		})
	})

	assert.Equal(t, 1, exitCode)
	require.Error(t, errOut.lastErr)
	assert.Contains(t, errOut.lastErr.Error(), "snapshot 200 is not an ancestor of current snapshot 100")
}

func upgradeRollbackTestTable(t *testing.T) *table.Table {
	t.Helper()

	meta, err := table.ParseMetadataBytes([]byte(upgradeRollbackTestMetadata))
	require.NoError(t, err)

	return table.New([]string{"db", "events"}, meta, "", nil, nil)
}

func int64Ptr(v int64) *int64 {
	return &v
}

type errCapture struct {
	textOutput
	lastErr error
}

func (e *errCapture) Error(err error) {
	e.lastErr = err
}

func captureExit(f func()) (exitCode int) {
	origExit := osExit
	defer func() { osExit = origExit }()

	osExit = func(code int) {
		exitCode = code
		panic(exitSentinel{})
	}

	defer func() {
		if r := recover(); r != nil {
			if _, ok := r.(exitSentinel); !ok {
				panic(r)
			}
		}
	}()

	f()

	return 0
}

type exitSentinel struct{}

// TestMain is the entry point for every test in this package. When the binary
// is re-exec'd with icebergCLISubprocessEnv set, it runs the real CLI main()
// instead of the test suite, letting tests assert end-to-end behavior such as
// argument-validation ordering relative to catalog initialization. In the normal
// (non-subprocess) case it just runs the tests.
func TestMain(m *testing.M) {
	if os.Getenv(icebergCLISubprocessEnv) == "1" {
		main()

		return
	}

	os.Exit(m.Run())
}

const icebergCLISubprocessEnv = "ICEBERG_CLI_TEST_SUBPROCESS"

// TestRollbackSelectorValidatedBeforeCatalogInit drives the real main() entry
// point. An invalid selector must fail with the validation message and never
// reach initCatalog (which would surface a catalog connection error instead).
func TestRollbackSelectorValidatedBeforeCatalogInit(t *testing.T) {
	if testing.Short() {
		t.Skip("spawns a subprocess")
	}

	tests := []struct {
		name    string
		args    []string
		wantErr string
	}{
		{
			name:    "no selector",
			args:    []string{"rollback", "db.events"},
			wantErr: "exactly one of --snapshot-id or --timestamp is required",
		},
		{
			name:    "both selectors",
			args:    []string{"rollback", "db.events", "--snapshot-id", "100", "--timestamp", "2026-01-15T03:00:00Z"},
			wantErr: "--snapshot-id and --timestamp are mutually exclusive",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cmd := exec.Command(os.Args[0], tt.args...)
			cmd.Env = append(os.Environ(), icebergCLISubprocessEnv+"=1")

			out, err := cmd.CombinedOutput()

			var exitErr *exec.ExitError
			require.ErrorAs(t, err, &exitErr, "expected non-zero exit; output: %s", out)
			assert.Equal(t, 1, exitErr.ExitCode())
			// If validation did not run before initCatalog, the subprocess would
			// fail with a catalog connection error and never print this message.
			assert.Contains(t, string(out), tt.wantErr)
		})
	}
}
