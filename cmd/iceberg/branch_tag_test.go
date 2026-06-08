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
	"os"
	"strconv"
	"testing"

	"github.com/apache/iceberg-go/catalog"
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
    "refs": {
        "main": {"snapshot-id": 5000, "type": "branch"},
        "feature-branch": {"snapshot-id": 5000, "type": "branch"},
        "v1.0": {"snapshot-id": 5000, "type": "tag"}
    }
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

func TestTextOutputRefDeleted(t *testing.T) {
	var buf bytes.Buffer
	pterm.SetDefaultOutput(&buf)
	pterm.DisableColor()
	t.Cleanup(func() {
		pterm.SetDefaultOutput(os.Stderr)
		pterm.EnableColor()
	})

	result := RefDeletedResult{
		Table:      "db.events",
		RefName:    "feature-branch",
		RefType:    "branch",
		SnapshotID: 5000,
	}

	textOutput{}.RefDeleted(result)

	output := buf.String()
	assert.Contains(t, output, "Deleted branch")
	assert.Contains(t, output, "feature-branch")
	assert.Contains(t, output, "db.events")
	assert.Contains(t, output, "5000")
}

func TestJSONOutputRefDeleted(t *testing.T) {
	r, w, err := os.Pipe()
	require.NoError(t, err)

	oldStdout := os.Stdout
	os.Stdout = w
	t.Cleanup(func() {
		w.Close()
		os.Stdout = oldStdout
	})

	result := RefDeletedResult{
		Table:      "db.events",
		RefName:    "feature-branch",
		RefType:    "branch",
		SnapshotID: 5000,
	}

	jsonOutput{}.RefDeleted(result)

	w.Close()

	var buf bytes.Buffer
	_, _ = buf.ReadFrom(r)

	output := buf.String()
	assert.Contains(t, output, `"table":"db.events"`)
	assert.Contains(t, output, `"ref_name":"feature-branch"`)
	assert.Contains(t, output, `"ref_type":"branch"`)
	assert.Contains(t, output, `"snapshot_id":5000`)
}

func TestRunBranchDeleteCommitsRemoveRef(t *testing.T) {
	tbl := branchTagTestTable(t)
	cat := &branchTagCommitCatalog{tbl: tbl}
	out := &refDeleteCapture{}

	runBranchDelete(context.Background(), out, cat, &BranchDeleteCmd{
		TableID:    "db.events",
		BranchName: "feature-branch",
		Yes:        true,
	})

	require.NoError(t, out.lastErr)
	require.NotNil(t, out.deleted)
	assert.Equal(t, RefDeletedResult{
		Table:      "db.events",
		RefName:    "feature-branch",
		RefType:    string(table.BranchRef),
		SnapshotID: 5000,
	}, *out.deleted)
	assertDeleteRequirements(t, cat.requirements, "feature-branch", 5000)
	require.Len(t, cat.updates, 1)
	assertRemoveSnapshotRefUpdate(t, cat.updates[0], "feature-branch")
	_, found := findSnapshotRef(cat.committed, "feature-branch")
	assert.False(t, found)
}

func TestRunTagDeleteCommitsRemoveRef(t *testing.T) {
	tbl := branchTagTestTable(t)
	cat := &branchTagCommitCatalog{tbl: tbl}
	out := &refDeleteCapture{}

	runTagDelete(context.Background(), out, cat, &TagDeleteCmd{
		TableID: "db.events",
		TagName: "v1.0",
		Yes:     true,
	})

	require.NoError(t, out.lastErr)
	require.NotNil(t, out.deleted)
	assert.Equal(t, RefDeletedResult{
		Table:      "db.events",
		RefName:    "v1.0",
		RefType:    string(table.TagRef),
		SnapshotID: 5000,
	}, *out.deleted)
	assertDeleteRequirements(t, cat.requirements, "v1.0", 5000)
	require.Len(t, cat.updates, 1)
	assertRemoveSnapshotRefUpdate(t, cat.updates[0], "v1.0")
	_, found := findSnapshotRef(cat.committed, "v1.0")
	assert.False(t, found)
}

func TestRunBranchDeleteRejectsMain(t *testing.T) {
	out := &refDeleteCapture{}

	exitCode := captureBranchTagExit(func() {
		runBranchDelete(context.Background(), out, nil, &BranchDeleteCmd{
			TableID:    "db.events",
			BranchName: "main",
			Yes:        true,
		})
	})

	assert.Equal(t, 1, exitCode)
	require.Error(t, out.lastErr)
	assert.Contains(t, out.lastErr.Error(), "cannot delete")
	assert.Contains(t, out.lastErr.Error(), "main")
}

func TestRunBranchDeleteRejectsMissingRef(t *testing.T) {
	tbl := branchTagTestTable(t)
	cat := &branchTagCommitCatalog{tbl: tbl}
	out := &refDeleteCapture{}

	exitCode := captureBranchTagExit(func() {
		runBranchDelete(context.Background(), out, cat, &BranchDeleteCmd{
			TableID:    "db.events",
			BranchName: "missing",
			Yes:        true,
		})
	})

	assert.Equal(t, 1, exitCode)
	require.Error(t, out.lastErr)
	assert.Contains(t, out.lastErr.Error(), `branch "missing" does not exist`)
	assert.Empty(t, cat.updates)
}

func TestRunTagDeleteRejectsMissingRef(t *testing.T) {
	tbl := branchTagTestTable(t)
	cat := &branchTagCommitCatalog{tbl: tbl}
	out := &refDeleteCapture{}

	exitCode := captureBranchTagExit(func() {
		runTagDelete(context.Background(), out, cat, &TagDeleteCmd{
			TableID: "db.events",
			TagName: "missing",
			Yes:     true,
		})
	})

	assert.Equal(t, 1, exitCode)
	require.Error(t, out.lastErr)
	assert.Contains(t, out.lastErr.Error(), `tag "missing" does not exist`)
	assert.Empty(t, cat.updates)
}

func TestRunBranchDeleteRejectsTagRef(t *testing.T) {
	tbl := branchTagTestTable(t)
	cat := &branchTagCommitCatalog{tbl: tbl}
	out := &refDeleteCapture{}

	exitCode := captureBranchTagExit(func() {
		runBranchDelete(context.Background(), out, cat, &BranchDeleteCmd{
			TableID:    "db.events",
			BranchName: "v1.0",
			Yes:        true,
		})
	})

	assert.Equal(t, 1, exitCode)
	require.Error(t, out.lastErr)
	assert.Contains(t, out.lastErr.Error(), `ref "v1.0" is a tag, not a branch`)
	assert.Empty(t, cat.updates)
}

func TestRunTagDeleteRejectsBranchRef(t *testing.T) {
	tbl := branchTagTestTable(t)
	cat := &branchTagCommitCatalog{tbl: tbl}
	out := &refDeleteCapture{}

	exitCode := captureBranchTagExit(func() {
		runTagDelete(context.Background(), out, cat, &TagDeleteCmd{
			TableID: "db.events",
			TagName: "feature-branch",
			Yes:     true,
		})
	})

	assert.Equal(t, 1, exitCode)
	require.Error(t, out.lastErr)
	assert.Contains(t, out.lastErr.Error(), `ref "feature-branch" is a branch, not a tag`)
	assert.Empty(t, cat.updates)
}

func branchTagTestTable(t *testing.T) *table.Table {
	t.Helper()

	meta, err := table.ParseMetadataBytes([]byte(branchTagTestMetadata))
	require.NoError(t, err)

	return table.New([]string{"db", "events"}, meta, "", nil, nil)
}

type branchTagCommitCatalog struct {
	catalog.Catalog
	requirements []table.Requirement
	updates      []table.Update
	committed    table.Metadata
	tbl          *table.Table
}

func (c *branchTagCommitCatalog) LoadTable(_ context.Context, _ table.Identifier) (*table.Table, error) {
	return c.tbl, nil
}

func (c *branchTagCommitCatalog) CommitTable(
	_ context.Context,
	_ table.Identifier,
	requirements []table.Requirement,
	updates []table.Update,
) (table.Metadata, string, error) {
	c.requirements = requirements
	c.updates = updates

	base := c.tbl.Metadata()
	for _, req := range requirements {
		if err := req.Validate(base); err != nil {
			return nil, "", err
		}
	}

	builder, err := table.MetadataBuilderFromBase(base, c.tbl.MetadataLocation())
	if err != nil {
		return nil, "", err
	}

	for _, update := range updates {
		if err := update.Apply(builder); err != nil {
			return nil, "", err
		}
	}

	meta, err := builder.Build()
	if err != nil {
		return nil, "", err
	}

	c.committed = meta
	c.tbl = table.New(c.tbl.Identifier(), meta, c.tbl.MetadataLocation(), nil, nil)

	return meta, "", nil
}

func assertDeleteRequirements(t *testing.T, requirements []table.Requirement, refName string, snapshotID int64) {
	t.Helper()

	require.Len(t, requirements, 2)
	expected, err := json.Marshal(table.AssertRefSnapshotID(refName, &snapshotID))
	require.NoError(t, err)

	var found bool
	for _, requirement := range requirements {
		actual, err := json.Marshal(requirement)
		require.NoError(t, err)
		if bytes.Equal(actual, expected) {
			found = true

			break
		}
	}

	assert.True(t, found, "expected AssertRefSnapshotID(%q, %d)", refName, snapshotID)
}

func assertRemoveSnapshotRefUpdate(t *testing.T, update table.Update, refName string) {
	t.Helper()

	actual, err := json.Marshal(update)
	require.NoError(t, err)
	assert.JSONEq(t, `{"action":"remove-snapshot-ref","ref-name":`+strconv.Quote(refName)+`}`, string(actual))
}

type refDeleteCapture struct {
	textOutput
	deleted *RefDeletedResult
	lastErr error
}

func (r *refDeleteCapture) RefDeleted(result RefDeletedResult) {
	r.deleted = &result
}

func (r *refDeleteCapture) Error(err error) {
	r.lastErr = err
}

func captureBranchTagExit(f func()) (exitCode int) {
	origExit := osExit
	defer func() { osExit = origExit }()

	osExit = func(code int) {
		exitCode = code
		panic(branchTagExitSentinel{})
	}

	defer func() {
		if r := recover(); r != nil {
			if _, ok := r.(branchTagExitSentinel); !ok {
				panic(r)
			}
		}
	}()

	f()

	return 0
}

type branchTagExitSentinel struct{}
