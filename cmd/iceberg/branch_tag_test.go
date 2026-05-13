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

const branchTagTestMetadata = `{
    "format-version": 2,
    "table-uuid": "9c12d441-03fe-4693-9a96-a0705ddf69c1",
    "location": "s3://bucket/test/location",
    "last-sequence-number": 1,
    "last-updated-ms": 1602638573590,
    "last-column-id": 1,
    "current-schema-id": 0,
    "schemas": [
        {"type": "struct", "schema-id": 0, "fields": [{"id": 1, "name": "x", "required": true, "type": "long"}]}
    ],
    "default-spec-id": 0,
    "partition-specs": [{"spec-id": 0, "fields": []}],
    "last-partition-id": 0,
    "default-sort-order-id": 0,
    "sort-orders": [{"order-id": 0, "fields": []}],
    "properties": {},
    "current-snapshot-id": 5000,
    "snapshots": [
        {"snapshot-id": 5000, "timestamp-ms": 1615100955770, "sequence-number": 1, "summary": {"operation": "append"}, "manifest-list": "s3://a/b/1.avro", "schema-id": 0}
    ],
    "snapshot-log": [],
    "metadata-log": [],
    "refs": {"main": {"snapshot-id": 5000, "type": "branch"}}
}`

func TestResolveSnapshotIDExplicit(t *testing.T) {
	meta, err := table.ParseMetadataBytes([]byte(branchTagTestMetadata))
	require.NoError(t, err)

	tbl := table.New([]string{"db", "tbl"}, meta, "", nil, nil)
	explicit := int64(5000)

	result := resolveSnapshotID(textOutput{}, tbl, &explicit)
	assert.Equal(t, int64(5000), result)
}

func TestResolveSnapshotIDCurrent(t *testing.T) {
	meta, err := table.ParseMetadataBytes([]byte(branchTagTestMetadata))
	require.NoError(t, err)

	tbl := table.New([]string{"db", "tbl"}, meta, "", nil, nil)

	result := resolveSnapshotID(textOutput{}, tbl, nil)
	assert.Equal(t, int64(5000), result)
}

func TestTextOutputRefCreated(t *testing.T) {
	var buf bytes.Buffer
	pterm.SetDefaultOutput(&buf)
	pterm.DisableColor()
	t.Cleanup(func() {
		pterm.SetDefaultOutput(os.Stderr)
		pterm.EnableColor()
	})

	result := RefCreatedResult{
		Table:      "db.events",
		RefName:    "feature-branch",
		RefType:    "branch",
		SnapshotID: 5000,
	}

	buf.Reset()
	textOutput{}.RefCreated(result)

	output := buf.String()
	assert.Contains(t, output, "Created branch")
	assert.Contains(t, output, "feature-branch")
	assert.Contains(t, output, "db.events")
	assert.Contains(t, output, "5000")
}

func TestTextOutputRefCreatedTag(t *testing.T) {
	var buf bytes.Buffer
	pterm.SetDefaultOutput(&buf)
	pterm.DisableColor()
	t.Cleanup(func() {
		pterm.SetDefaultOutput(os.Stderr)
		pterm.EnableColor()
	})

	result := RefCreatedResult{
		Table:      "db.events",
		RefName:    "v1.0",
		RefType:    "tag",
		SnapshotID: 3000,
	}

	buf.Reset()
	textOutput{}.RefCreated(result)

	output := buf.String()
	assert.Contains(t, output, "Created tag")
	assert.Contains(t, output, "v1.0")
	assert.Contains(t, output, "3000")
}

func TestJSONOutputRefCreated(t *testing.T) {
	r, w, err := os.Pipe()
	require.NoError(t, err)

	oldStdout := os.Stdout
	os.Stdout = w
	t.Cleanup(func() {
		w.Close()
		os.Stdout = oldStdout
	})

	result := RefCreatedResult{
		Table:      "db.events",
		RefName:    "feature-branch",
		RefType:    "branch",
		SnapshotID: 5000,
	}

	jsonOutput{}.RefCreated(result)

	w.Close()

	var buf bytes.Buffer
	_, _ = buf.ReadFrom(r)

	output := buf.String()
	assert.Contains(t, output, `"table":"db.events"`)
	assert.Contains(t, output, `"ref_name":"feature-branch"`)
	assert.Contains(t, output, `"ref_type":"branch"`)
	assert.Contains(t, output, `"snapshot_id":5000`)
}

func TestJSONOutputRefCreatedWithRetention(t *testing.T) {
	r, w, err := os.Pipe()
	require.NoError(t, err)

	oldStdout := os.Stdout
	os.Stdout = w
	t.Cleanup(func() {
		w.Close()
		os.Stdout = oldStdout
	})

	maxRefAge := int64(604800000)
	maxSnapshotAge := int64(86400000)
	minSnapshots := 5

	result := RefCreatedResult{
		Table:              "db.events",
		RefName:            "feature-branch",
		RefType:            "branch",
		SnapshotID:         5000,
		MaxRefAgeMs:        &maxRefAge,
		MaxSnapshotAgeMs:   &maxSnapshotAge,
		MinSnapshotsToKeep: &minSnapshots,
	}

	jsonOutput{}.RefCreated(result)

	w.Close()

	var buf bytes.Buffer
	_, _ = buf.ReadFrom(r)

	output := buf.String()
	assert.Contains(t, output, `"max_ref_age_ms":604800000`)
	assert.Contains(t, output, `"max_snapshot_age_ms":86400000`)
	assert.Contains(t, output, `"min_snapshots_to_keep":5`)
}
