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

func TestBuildCleanOrphanFilesResultDryRun(t *testing.T) {
	const metadata = `{
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

	meta, err := table.ParseMetadataBytes([]byte(metadata))
	require.NoError(t, err)

	tbl := table.New([]string{"db", "orphans"}, meta, "", nil, nil)

	orphanResult := table.OrphanCleanupResult{
		OrphanFileLocations: []string{"s3://bucket/data/file1.parquet", "s3://bucket/data/file2.parquet"},
		TotalSizeBytes:      4096,
	}

	result := buildCleanOrphanFilesResult(tbl, orphanResult, true)

	assert.True(t, result.DryRun)
	assert.Equal(t, "db.orphans", result.Table)
	assert.Equal(t, 2, result.OrphanFileCount)
	assert.Equal(t, int64(4096), result.TotalSizeBytes)
	require.Len(t, result.OrphanFiles, 2)
	assert.Equal(t, "s3://bucket/data/file1.parquet", result.OrphanFiles[0].Path)
}

func TestBuildCleanOrphanFilesResultDeleted(t *testing.T) {
	const metadata = `{
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

	meta, err := table.ParseMetadataBytes([]byte(metadata))
	require.NoError(t, err)

	tbl := table.New([]string{"db", "orphans"}, meta, "", nil, nil)

	orphanResult := table.OrphanCleanupResult{
		OrphanFileLocations: []string{"s3://bucket/data/file1.parquet"},
		DeletedFiles:        []string{"s3://bucket/data/file1.parquet"},
		TotalSizeBytes:      2048,
	}

	result := buildCleanOrphanFilesResult(tbl, orphanResult, false)

	assert.False(t, result.DryRun)
	assert.Equal(t, 1, result.OrphanFileCount)
	assert.Equal(t, "s3://bucket/data/file1.parquet", result.OrphanFiles[0].Path)
}

func TestTextOutputCleanOrphanFilesResultEmpty(t *testing.T) {
	var buf bytes.Buffer
	pterm.SetDefaultOutput(&buf)
	pterm.DisableColor()

	result := CleanOrphanFilesResult{
		DryRun:          false,
		Table:           "db.tbl",
		OrphanFileCount: 0,
		TotalSizeBytes:  0,
		OrphanFiles:     nil,
	}

	buf.Reset()
	textOutput{}.CleanOrphanFilesResult(result)

	assert.Contains(t, buf.String(), "No orphan files found.")
}

func TestTextOutputCleanOrphanFilesResultDryRun(t *testing.T) {
	var buf bytes.Buffer
	pterm.SetDefaultOutput(&buf)
	pterm.DisableColor()

	result := CleanOrphanFilesResult{
		DryRun:          true,
		Table:           "db.tbl",
		OrphanFileCount: 2,
		TotalSizeBytes:  1048576,
		OrphanFiles: []OrphanFileEntry{
			{Path: "s3://bucket/data/a.parquet"},
			{Path: "s3://bucket/data/b.parquet"},
		},
	}

	buf.Reset()
	textOutput{}.CleanOrphanFilesResult(result)

	output := buf.String()
	assert.Contains(t, output, "[DRY RUN]")
	assert.Contains(t, output, "2 orphan files found")
	assert.Contains(t, output, "1.0 MB")
	assert.Contains(t, output, "s3://bucket/data/a.parquet")
}

func TestJSONOutputCleanOrphanFilesResult(t *testing.T) {
	oldStdout := os.Stdout
	r, w, _ := os.Pipe()
	os.Stdout = w
	defer func() { os.Stdout = oldStdout }()

	result := CleanOrphanFilesResult{
		DryRun:          true,
		Table:           "db.tbl",
		OrphanFileCount: 1,
		TotalSizeBytes:  512,
		OrphanFiles: []OrphanFileEntry{
			{Path: "s3://bucket/data/orphan.parquet"},
		},
	}

	jsonOutput{}.CleanOrphanFilesResult(result)

	w.Close()
	var buf bytes.Buffer
	_, _ = buf.ReadFrom(r)

	output := buf.String()
	assert.Contains(t, output, `"dry_run":true`)
	assert.Contains(t, output, `"table":"db.tbl"`)
	assert.Contains(t, output, `"orphan_file_count":1`)
	assert.Contains(t, output, `"total_size_bytes":512`)
	assert.Contains(t, output, `"path":"s3://bucket/data/orphan.parquet"`)
}
