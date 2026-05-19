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

const snapshotsTestMetadata = `{
    "format-version": 2,
    "table-uuid": "9c12d441-03fe-4693-9a96-a0705ddf69c1",
    "location": "s3://bucket/test/location",
    "last-sequence-number": 2,
    "last-updated-ms": 1602638573590,
    "last-column-id": 3,
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
    "current-snapshot-id": 2000000000000000002,
    "snapshots": [
        {
            "snapshot-id": 2000000000000000001,
            "timestamp-ms": 1615100955770,
            "sequence-number": 1,
            "summary": {"operation": "append", "added-data-files": "3", "deleted-data-files": "0"},
            "manifest-list": "s3://a/b/1.avro",
            "schema-id": 0
        },
        {
            "snapshot-id": 2000000000000000002,
            "parent-snapshot-id": 2000000000000000001,
            "timestamp-ms": 1625100955770,
            "sequence-number": 2,
            "summary": {"operation": "overwrite", "added-data-files": "1", "deleted-data-files": "2"},
            "manifest-list": "s3://a/b/2.avro",
            "schema-id": 0
        }
    ],
    "snapshot-log": [],
    "metadata-log": [],
    "refs": {
        "main": {"snapshot-id": 2000000000000000002, "type": "branch"},
        "v1.0": {"snapshot-id": 2000000000000000001, "type": "tag", "max-ref-age-ms": 86400000}
    }
}`

func TestBuildSnapshotEntries(t *testing.T) {
	meta, err := table.ParseMetadataBytes([]byte(snapshotsTestMetadata))
	require.NoError(t, err)

	tbl := table.New([]string{"db", "events"}, meta, "", nil, nil)
	entries := buildSnapshotEntries(tbl)

	require.Len(t, entries, 2)

	assert.Equal(t, int64(2000000000000000002), entries[0].SnapshotID)
	assert.Equal(t, "overwrite", entries[0].Operation)
	assert.Equal(t, "1", entries[0].AddedDataFiles)
	assert.Equal(t, "2", entries[0].DeletedDataFiles)
	require.NotNil(t, entries[0].ParentSnapshotID)
	assert.Equal(t, int64(2000000000000000001), *entries[0].ParentSnapshotID)

	assert.Equal(t, int64(2000000000000000001), entries[1].SnapshotID)
	assert.Equal(t, "append", entries[1].Operation)
	assert.Equal(t, "3", entries[1].AddedDataFiles)
	assert.Nil(t, entries[1].ParentSnapshotID)
}

func TestBuildSnapshotEntriesNilSummary(t *testing.T) {
	const metadata = `{
        "format-version": 2,
        "table-uuid": "9c12d441-03fe-4693-9a96-a0705ddf69c1",
        "location": "s3://bucket/test/location",
        "last-sequence-number": 1,
        "last-updated-ms": 1602638573590,
        "last-column-id": 1,
        "current-schema-id": 0,
        "schemas": [{"type": "struct", "schema-id": 0, "fields": [{"id": 1, "name": "x", "required": true, "type": "long"}]}],
        "default-spec-id": 0,
        "partition-specs": [{"spec-id": 0, "fields": []}],
        "last-partition-id": 0,
        "default-sort-order-id": 0,
        "sort-orders": [{"order-id": 0, "fields": []}],
        "properties": {},
        "current-snapshot-id": 1001,
        "snapshots": [{"snapshot-id": 1001, "timestamp-ms": 1615100955770, "sequence-number": 1, "manifest-list": "s3://a/b/1.avro", "schema-id": 0}],
        "snapshot-log": [],
        "metadata-log": [],
        "refs": {"main": {"snapshot-id": 1001, "type": "branch"}}
    }`

	meta, err := table.ParseMetadataBytes([]byte(metadata))
	require.NoError(t, err)

	tbl := table.New([]string{"db", "tbl"}, meta, "", nil, nil)
	entries := buildSnapshotEntries(tbl)

	require.Len(t, entries, 1)
	assert.Equal(t, "", entries[0].Operation)
	assert.Equal(t, "-", entries[0].AddedDataFiles)
	assert.Equal(t, "-", entries[0].DeletedDataFiles)
}

func TestBuildRefEntries(t *testing.T) {
	meta, err := table.ParseMetadataBytes([]byte(snapshotsTestMetadata))
	require.NoError(t, err)

	tbl := table.New([]string{"db", "events"}, meta, "", nil, nil)

	all := buildRefEntries(tbl, "")
	assert.Len(t, all, 2)

	branches := buildRefEntries(tbl, "branch")
	assert.Len(t, branches, 1)
	assert.Equal(t, "main", branches[0].Name)

	tags := buildRefEntries(tbl, "tag")
	assert.Len(t, tags, 1)
	assert.Equal(t, "v1.0", tags[0].Name)
	require.NotNil(t, tags[0].MaxRefAgeMs)
	assert.Equal(t, int64(86400000), *tags[0].MaxRefAgeMs)
}

func TestTextOutputSnapshots(t *testing.T) {
	var buf bytes.Buffer
	pterm.SetDefaultOutput(&buf)
	pterm.DisableColor()

	meta, err := table.ParseMetadataBytes([]byte(snapshotsTestMetadata))
	require.NoError(t, err)

	tbl := table.New([]string{"db", "events"}, meta, "", nil, nil)
	buf.Reset()

	textOutput{}.Snapshots(tbl)

	output := buf.String()
	assert.Contains(t, output, "SNAPSHOT ID")
	assert.Contains(t, output, "TIMESTAMP")
	assert.Contains(t, output, "PARENT")
	assert.Contains(t, output, "OP")
	assert.Contains(t, output, "2000000000000000002")
	assert.Contains(t, output, "overwrite")
}

func TestJSONOutputSnapshots(t *testing.T) {
	oldStdout := os.Stdout
	r, w, _ := os.Pipe()
	os.Stdout = w
	defer func() { os.Stdout = oldStdout }()

	meta, err := table.ParseMetadataBytes([]byte(snapshotsTestMetadata))
	require.NoError(t, err)

	tbl := table.New([]string{"db", "events"}, meta, "", nil, nil)

	jsonOutput{}.Snapshots(tbl)

	w.Close()
	var buf bytes.Buffer
	_, _ = buf.ReadFrom(r)

	output := buf.String()
	assert.Contains(t, output, `"table":"db.events"`)
	assert.Contains(t, output, `"snapshot_id":2000000000000000002`)
	assert.Contains(t, output, `"operation":"overwrite"`)
}

func TestTextOutputRefs(t *testing.T) {
	var buf bytes.Buffer
	pterm.SetDefaultOutput(&buf)
	pterm.DisableColor()

	meta, err := table.ParseMetadataBytes([]byte(snapshotsTestMetadata))
	require.NoError(t, err)

	tbl := table.New([]string{"db", "events"}, meta, "", nil, nil)
	buf.Reset()

	textOutput{}.Refs(tbl, "")

	output := buf.String()
	assert.Contains(t, output, "NAME")
	assert.Contains(t, output, "TYPE")
	assert.Contains(t, output, "SNAPSHOT ID")
	assert.Contains(t, output, "main")
	assert.Contains(t, output, "v1.0")
	assert.Contains(t, output, "24h")
}

func TestJSONOutputRefs(t *testing.T) {
	oldStdout := os.Stdout
	r, w, _ := os.Pipe()
	os.Stdout = w
	defer func() { os.Stdout = oldStdout }()

	meta, err := table.ParseMetadataBytes([]byte(snapshotsTestMetadata))
	require.NoError(t, err)

	tbl := table.New([]string{"db", "events"}, meta, "", nil, nil)

	jsonOutput{}.Refs(tbl, "")

	w.Close()
	var buf bytes.Buffer
	_, _ = buf.ReadFrom(r)

	output := buf.String()
	assert.Contains(t, output, `"table":"db.events"`)
	assert.Contains(t, output, `"refs":[`)
}
