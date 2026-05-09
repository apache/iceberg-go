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

const infoTestMetadata = `{
    "format-version": 2,
    "table-uuid": "9c12d441-03fe-4693-9a96-a0705ddf69c1",
    "location": "s3://bucket/test/location",
    "last-sequence-number": 34,
    "last-updated-ms": 1602638573590,
    "last-column-id": 3,
    "current-schema-id": 1,
    "schemas": [
        {"type": "struct", "schema-id": 0, "fields": [{"id": 1, "name": "x", "required": true, "type": "long"}]},
        {
            "type": "struct",
            "schema-id": 1,
            "identifier-field-ids": [1, 2],
            "fields": [
                {"id": 1, "name": "x", "required": true, "type": "long"},
                {"id": 2, "name": "y", "required": true, "type": "long", "doc": "comment"},
                {"id": 3, "name": "z", "required": true, "type": "long"}
            ]
        }
    ],
    "default-spec-id": 0,
    "partition-specs": [{"spec-id": 0, "fields": [{"name": "x", "transform": "identity", "source-id": 1, "field-id": 1000}]}],
    "last-partition-id": 1000,
    "default-sort-order-id": 3,
    "sort-orders": [
        {
            "order-id": 3,
            "fields": [
                {"transform": "identity", "source-id": 2, "direction": "asc", "null-order": "nulls-first"},
                {"transform": "bucket[4]", "source-id": 3, "direction": "desc", "null-order": "nulls-last"}
            ]
        }
    ],
    "properties": {"read.split.target.size": "134217728"},
    "current-snapshot-id": 3055729675574597004,
    "snapshots": [
        {
            "snapshot-id": 3051729675574597004,
            "timestamp-ms": 1515100955770,
            "sequence-number": 0,
            "summary": {"operation": "append"},
            "manifest-list": "s3://a/b/1.avro",
            "schema-id": 1
        },
        {
            "snapshot-id": 3055729675574597004,
            "parent-snapshot-id": 3051729675574597004,
            "timestamp-ms": 1555100955770,
            "sequence-number": 1,
            "summary": {"operation": "append"},
            "manifest-list": "s3://a/b/2.avro",
            "schema-id": 1
        }
    ],
    "snapshot-log": [
        {"snapshot-id": 3051729675574597004, "timestamp-ms": 1515100955770},
        {"snapshot-id": 3055729675574597004, "timestamp-ms": 1555100955770}
    ],
    "metadata-log": [{"metadata-file": "s3://bucket/.../v1.json", "timestamp-ms": 1515100}],
    "refs": {"test": {"snapshot-id": 3051729675574597004, "type": "tag", "max-ref-age-ms": 10000000}}
}`

const infoTestMetadataUnsorted = `{
    "format-version": 2,
    "table-uuid": "9c12d441-03fe-4693-9a96-a0705ddf69c1",
    "location": "s3://bucket/test/location",
    "last-sequence-number": 0,
    "last-updated-ms": 1602638573590,
    "last-column-id": 3,
    "current-schema-id": 0,
    "schemas": [
        {"type": "struct", "schema-id": 0, "fields": [{"id": 1, "name": "x", "required": true, "type": "long"}]}
    ],
    "default-spec-id": 0,
    "partition-specs": [{"spec-id": 0, "fields": []}],
    "last-partition-id": 1000,
    "default-sort-order-id": 0,
    "sort-orders": [{"order-id": 0, "fields": []}],
    "properties": {},
    "current-snapshot-id": -1,
    "snapshots": [],
    "snapshot-log": [],
    "metadata-log": [],
    "refs": {}
}`

func TestBuildTableInfo(t *testing.T) {
	meta, err := table.ParseMetadataBytes([]byte(infoTestMetadata))
	require.NoError(t, err)

	tbl := table.New([]string{"db", "events"}, meta, "", nil, nil)
	info := buildTableInfo(tbl)

	assert.Equal(t, "db.events", info.Table)
	assert.Equal(t, "9c12d441-03fe-4693-9a96-a0705ddf69c1", info.UUID)
	assert.Equal(t, 2, info.FormatVersion)
	assert.Equal(t, "s3://bucket/test/location", info.Location)
	assert.Equal(t, "2020-10-14T01:22:53Z", info.LastUpdated)
	require.NotNil(t, info.CurrentSnapshotID)
	assert.Equal(t, int64(3055729675574597004), *info.CurrentSnapshotID)
	assert.Equal(t, 1, info.SchemaID)
	assert.Equal(t, 3, info.SchemaFieldCount)
	assert.Contains(t, info.PartitionSpec, "identity")
	assert.NotEqual(t, "unsorted", info.SortOrder)
	assert.Equal(t, 2, info.SnapshotCount)
	assert.Equal(t, 1, info.Refs.Branches)
	assert.Equal(t, 1, info.Refs.Tags)
	assert.Equal(t, 1, info.PropertyCount)
}

func TestBuildTableInfoUnsorted(t *testing.T) {
	meta, err := table.ParseMetadataBytes([]byte(infoTestMetadataUnsorted))
	require.NoError(t, err)

	tbl := table.New([]string{"db", "empty"}, meta, "", nil, nil)
	info := buildTableInfo(tbl)

	assert.Equal(t, "db.empty", info.Table)
	assert.Nil(t, info.CurrentSnapshotID)
	assert.Equal(t, "unsorted", info.SortOrder)
	assert.Equal(t, 0, info.SnapshotCount)
	assert.Equal(t, 0, info.PropertyCount)
}

func TestTableIDString(t *testing.T) {
	meta, err := table.ParseMetadataBytes([]byte(infoTestMetadataUnsorted))
	require.NoError(t, err)

	tbl := table.New([]string{"catalog", "db", "tbl"}, meta, "", nil, nil)
	assert.Equal(t, "catalog.db.tbl", tableIDString(tbl))
}

func TestTextOutputInfo(t *testing.T) {
	var buf bytes.Buffer
	pterm.SetDefaultOutput(&buf)
	pterm.DisableColor()

	meta, err := table.ParseMetadataBytes([]byte(infoTestMetadata))
	require.NoError(t, err)

	tbl := table.New([]string{"db", "events"}, meta, "", nil, nil)
	buf.Reset()

	textOutput{}.Info(tbl)

	output := buf.String()
	assert.Contains(t, output, "Table:")
	assert.Contains(t, output, "db.events")
	assert.Contains(t, output, "UUID:")
	assert.Contains(t, output, "9c12d441-03fe-4693-9a96-a0705ddf69c1")
	assert.Contains(t, output, "Format version:")
	assert.Contains(t, output, "Location:")
	assert.Contains(t, output, "Current snapshot:")
	assert.Contains(t, output, "3055729675574597004")
	assert.Contains(t, output, "Schema ID:")
	assert.Contains(t, output, "1 (3 fields)")
	assert.Contains(t, output, "1 branch, 1 tag")
}

func TestJSONOutputInfo(t *testing.T) {
	oldStdout := os.Stdout
	r, w, _ := os.Pipe()
	os.Stdout = w
	defer func() {
		os.Stdout = oldStdout
	}()

	meta, err := table.ParseMetadataBytes([]byte(infoTestMetadata))
	require.NoError(t, err)

	tbl := table.New([]string{"db", "events"}, meta, "", nil, nil)

	jsonOutput{}.Info(tbl)

	w.Close()
	var buf bytes.Buffer
	_, _ = buf.ReadFrom(r)

	output := buf.String()
	assert.Contains(t, output, `"table":"db.events"`)
	assert.Contains(t, output, `"uuid":"9c12d441-03fe-4693-9a96-a0705ddf69c1"`)
	assert.Contains(t, output, `"format_version":2`)
	assert.Contains(t, output, `"current_snapshot_id":3055729675574597004`)
	assert.Contains(t, output, `"snapshot_count":2`)
}
