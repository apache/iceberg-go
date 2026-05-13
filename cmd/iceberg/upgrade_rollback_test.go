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
	"testing"

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
			SnapshotID: 300,
			Yes:        true,
		})
	})

	assert.Equal(t, 1, exitCode)
	assert.Contains(t, errOut.lastErr.Error(), "not an ancestor")
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
