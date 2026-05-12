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
	"os"
	"testing"

	"github.com/pterm/pterm"
	"github.com/stretchr/testify/assert"
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
	oldStdout := os.Stdout
	r, w, _ := os.Pipe()
	os.Stdout = w
	defer func() { os.Stdout = oldStdout }()

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
	oldStdout := os.Stdout
	r, w, _ := os.Pipe()
	os.Stdout = w
	defer func() { os.Stdout = oldStdout }()

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
