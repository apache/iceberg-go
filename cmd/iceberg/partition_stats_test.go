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

const partitionStatsTestMetadata = `{
    "format-version": 2,
    "table-uuid": "9c12d441-03fe-4693-9a96-a0705ddf69c1",
    "location": "s3://bucket/test/location",
    "last-sequence-number": 2,
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
    "current-snapshot-id": 2000,
    "snapshots": [
        {"snapshot-id": 1000, "timestamp-ms": 1615100955770, "sequence-number": 1, "summary": {"operation": "append"}, "manifest-list": "s3://a/b/1.avro", "schema-id": 0},
        {"snapshot-id": 2000, "parent-snapshot-id": 1000, "timestamp-ms": 1625100955770, "sequence-number": 2, "summary": {"operation": "append"}, "manifest-list": "s3://a/b/2.avro", "schema-id": 0}
    ],
    "snapshot-log": [],
    "metadata-log": [],
    "refs": {"main": {"snapshot-id": 2000, "type": "branch"}},
    "partition-statistics": [
        {"snapshot-id": 1000, "statistics-path": "s3://bucket/stats/1000.bin", "file-size-in-bytes": 1024},
        {"snapshot-id": 2000, "statistics-path": "s3://bucket/stats/2000.bin", "file-size-in-bytes": 2048}
    ]
}`

func TestBuildPartitionStatsEntriesAll(t *testing.T) {
	meta, err := table.ParseMetadataBytes([]byte(partitionStatsTestMetadata))
	require.NoError(t, err)

	tbl := table.New([]string{"db", "events"}, meta, "", nil, nil)
	entries := buildPartitionStatsEntries(tbl, nil, true)

	assert.Len(t, entries, 2)
}

func TestBuildPartitionStatsEntriesCurrentOnly(t *testing.T) {
	meta, err := table.ParseMetadataBytes([]byte(partitionStatsTestMetadata))
	require.NoError(t, err)

	tbl := table.New([]string{"db", "events"}, meta, "", nil, nil)
	entries := buildPartitionStatsEntries(tbl, nil, false)

	require.Len(t, entries, 1)
	assert.Equal(t, int64(2000), entries[0].SnapshotID)
	assert.Equal(t, "s3://bucket/stats/2000.bin", entries[0].Path)
	assert.Equal(t, int64(2048), entries[0].SizeBytes)
}

func TestBuildPartitionStatsEntriesBySnapshotID(t *testing.T) {
	meta, err := table.ParseMetadataBytes([]byte(partitionStatsTestMetadata))
	require.NoError(t, err)

	tbl := table.New([]string{"db", "events"}, meta, "", nil, nil)
	sid := int64(1000)
	entries := buildPartitionStatsEntries(tbl, &sid, false)

	require.Len(t, entries, 1)
	assert.Equal(t, int64(1000), entries[0].SnapshotID)
}

func TestTextOutputPartitionStatsEmpty(t *testing.T) {
	var buf bytes.Buffer
	pterm.SetDefaultOutput(&buf)
	pterm.DisableColor()

	const emptyMeta = `{
        "format-version": 2,
        "table-uuid": "9c12d441-03fe-4693-9a96-a0705ddf69c1",
        "location": "s3://bucket/test/location",
        "last-sequence-number": 0,
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
        "current-snapshot-id": -1,
        "snapshots": [],
        "snapshot-log": [],
        "metadata-log": [],
        "refs": {}
    }`

	meta, err := table.ParseMetadataBytes([]byte(emptyMeta))
	require.NoError(t, err)

	tbl := table.New([]string{"db", "empty"}, meta, "", nil, nil)
	buf.Reset()

	textOutput{}.PartitionStats(tbl, nil, false)

	assert.Contains(t, buf.String(), "No partition statistics files found.")
}

func TestTextOutputPartitionStats(t *testing.T) {
	var buf bytes.Buffer
	pterm.SetDefaultOutput(&buf)
	pterm.DisableColor()

	meta, err := table.ParseMetadataBytes([]byte(partitionStatsTestMetadata))
	require.NoError(t, err)

	tbl := table.New([]string{"db", "events"}, meta, "", nil, nil)
	buf.Reset()

	textOutput{}.PartitionStats(tbl, nil, true)

	output := buf.String()
	assert.Contains(t, output, "SNAPSHOT ID")
	assert.Contains(t, output, "PATH")
	assert.Contains(t, output, "SIZE")
	assert.Contains(t, output, "1000")
	assert.Contains(t, output, "2000")
}

func TestJSONOutputPartitionStats(t *testing.T) {
	oldStdout := os.Stdout
	r, w, _ := os.Pipe()
	os.Stdout = w
	defer func() { os.Stdout = oldStdout }()

	meta, err := table.ParseMetadataBytes([]byte(partitionStatsTestMetadata))
	require.NoError(t, err)

	tbl := table.New([]string{"db", "events"}, meta, "", nil, nil)

	jsonOutput{}.PartitionStats(tbl, nil, true)

	w.Close()
	var buf bytes.Buffer
	_, _ = buf.ReadFrom(r)

	output := buf.String()
	assert.Contains(t, output, `"table":"db.events"`)
	assert.Contains(t, output, `"partition_statistics_files":[`)
	assert.Contains(t, output, `"snapshot_id":1000`)
}
