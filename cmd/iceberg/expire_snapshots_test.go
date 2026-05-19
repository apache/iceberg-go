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

	"github.com/apache/iceberg-go/table"
	"github.com/pterm/pterm"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestDiffSnapshots(t *testing.T) {
	before := []table.Snapshot{
		{SnapshotID: 1, TimestampMs: 1000000000000, Summary: &table.Summary{Operation: table.OpAppend}},
		{SnapshotID: 2, TimestampMs: 2000000000000, Summary: &table.Summary{Operation: table.OpOverwrite}},
		{SnapshotID: 3, TimestampMs: 3000000000000, Summary: &table.Summary{Operation: table.OpAppend}},
	}
	after := []table.Snapshot{
		{SnapshotID: 3, TimestampMs: 3000000000000, Summary: &table.Summary{Operation: table.OpAppend}},
	}

	expired := diffSnapshots(before, after)

	require.Len(t, expired, 2)
	assert.Equal(t, int64(1), expired[0].SnapshotID)
	assert.Equal(t, "append", expired[0].Operation)
	assert.Equal(t, int64(2), expired[1].SnapshotID)
	assert.Equal(t, "overwrite", expired[1].Operation)
}

func TestDiffSnapshotsNoneExpired(t *testing.T) {
	snaps := []table.Snapshot{
		{SnapshotID: 1, TimestampMs: 1000000000000, Summary: &table.Summary{Operation: table.OpAppend}},
	}

	expired := diffSnapshots(snaps, snaps)
	assert.Empty(t, expired)
}

func TestTextOutputExpireSnapshotsResultDryRun(t *testing.T) {
	var buf bytes.Buffer
	pterm.SetDefaultOutput(&buf)
	pterm.DisableColor()

	result := ExpireSnapshotsResult{
		DryRun:               true,
		Table:                "db.events",
		ExpiredSnapshotCount: 2,
		ExpiredSnapshots: []SnapshotEntry{
			{SnapshotID: 100, Timestamp: "2024-01-01T00:00:00Z", Operation: "append"},
			{SnapshotID: 200, Timestamp: "2024-01-02T00:00:00Z", Operation: "overwrite"},
		},
	}

	buf.Reset()
	textOutput{}.ExpireSnapshotsResult(result)

	output := buf.String()
	assert.Contains(t, output, "[DRY RUN]")
	assert.Contains(t, output, "2 snapshots would be expired")
	assert.Contains(t, output, "100")
	assert.Contains(t, output, "200")
}

func TestTextOutputExpireSnapshotsResultCommitted(t *testing.T) {
	var buf bytes.Buffer
	pterm.SetDefaultOutput(&buf)
	pterm.DisableColor()

	result := ExpireSnapshotsResult{
		DryRun:               false,
		Table:                "db.events",
		ExpiredSnapshotCount: 1,
		ExpiredSnapshots: []SnapshotEntry{
			{SnapshotID: 100, Timestamp: "2024-01-01T00:00:00Z", Operation: "append"},
		},
	}

	buf.Reset()
	textOutput{}.ExpireSnapshotsResult(result)

	output := buf.String()
	assert.Contains(t, output, "Expired 1 snapshots from db.events.")
	assert.Contains(t, output, "100")
}

func TestTextOutputExpireSnapshotsResultEmpty(t *testing.T) {
	var buf bytes.Buffer
	pterm.SetDefaultOutput(&buf)
	pterm.DisableColor()

	result := ExpireSnapshotsResult{
		DryRun:               false,
		Table:                "db.events",
		ExpiredSnapshotCount: 0,
		ExpiredSnapshots:     nil,
	}

	buf.Reset()
	textOutput{}.ExpireSnapshotsResult(result)

	assert.Contains(t, buf.String(), "No snapshots to expire.")
}

func TestJSONOutputExpireSnapshotsResult(t *testing.T) {
	oldStdout := os.Stdout
	r, w, _ := os.Pipe()
	os.Stdout = w
	defer func() { os.Stdout = oldStdout }()

	result := ExpireSnapshotsResult{
		DryRun:               true,
		Table:                "db.events",
		ExpiredSnapshotCount: 1,
		ExpiredSnapshots: []SnapshotEntry{
			{SnapshotID: 100, Timestamp: "2024-01-01T00:00:00Z", Operation: "append"},
		},
	}

	jsonOutput{}.ExpireSnapshotsResult(result)

	w.Close()
	var buf bytes.Buffer
	_, _ = buf.ReadFrom(r)

	output := buf.String()
	assert.Contains(t, output, `"dry_run":true`)
	assert.Contains(t, output, `"table":"db.events"`)
	assert.Contains(t, output, `"expired_snapshot_count":1`)
	assert.Contains(t, output, `"snapshot_id":100`)
}
