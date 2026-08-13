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

package table_test

import (
	"context"
	"path/filepath"
	"testing"

	"github.com/DataDog/iceberg-go"
	iceio "github.com/DataDog/iceberg-go/io"
	"github.com/DataDog/iceberg-go/table"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Why: Java's newFastAppend().commit() with zero files is legal and
// produces an empty snapshot that moves the branch head. Writers use
// such commits for metadata-only bookkeeping (e.g. recording ingestion
// progress in snapshot or table properties) that must still serialize
// through the producer's AssertRefSnapshotID requirement, which only
// engages when a snapshot moves the head. AddFiles with an empty path
// list must keep committing an empty append rather than degrading to a
// silent no-op.
// Condition: AddFiles with no paths on an empty table, then again on
// the resulting table after real data was added.
// Assertion: each commit creates a new "append" snapshot chained to its
// parent, the branch head moves, and existing data files stay reachable.
func TestAddFilesEmptyCommitsEmptySnapshot(t *testing.T) {
	location := filepath.ToSlash(t.TempDir())
	schema := iceberg.NewSchema(0,
		iceberg.NestedField{ID: 1, Name: "id", Type: iceberg.PrimitiveTypes.Int64, Required: true},
		iceberg.NestedField{ID: 2, Name: "data", Type: iceberg.PrimitiveTypes.String, Required: false},
	)
	meta, err := table.NewMetadata(schema, iceberg.UnpartitionedSpec,
		table.UnsortedSortOrder, location, iceberg.Properties{table.PropertyFormatVersion: "3"})
	require.NoError(t, err)

	fsF := func(context.Context) (iceio.IO, error) { return iceio.LocalFS{}, nil }
	cat := &concurrentTestCatalog{
		metadata: meta,
		location: location + "/metadata/v1.metadata.json",
		fsF:      fsF,
	}
	tbl := table.New(table.Identifier{"db", "empty_append"},
		meta, location+"/metadata/v1.metadata.json", fsF, cat)

	// Empty append on an empty table creates the branch head.
	tx := tbl.NewTransaction()
	require.NoError(t, tx.AddFiles(t.Context(), nil, nil, false))
	tbl, err = tx.Commit(t.Context())
	require.NoError(t, err)

	first := tbl.CurrentSnapshot()
	require.NotNil(t, first, "an empty append must still commit a snapshot")
	assert.Equal(t, table.OpAppend, first.Summary.Operation)
	assert.Equal(t, "0", first.Summary.Properties["total-data-files"])
	assert.Nil(t, first.ParentSnapshotID)

	// Add real data, then another empty append: the head must move
	// again and the data must remain reachable from the new snapshot.
	arrowSc, err := table.SchemaToArrowSchema(tbl.Schema(), nil, false, false)
	require.NoError(t, err)
	dataPath := location + "/data/data-001.parquet"
	writeParquetFile(t, dataPath, arrowSc, `[{"id": 1, "data": "alpha"}]`)

	txData := tbl.NewTransaction()
	require.NoError(t, txData.AddFiles(t.Context(), []string{dataPath}, nil, false))
	tbl, err = txData.Commit(t.Context())
	require.NoError(t, err)
	withData := tbl.CurrentSnapshot()
	require.NotNil(t, withData)

	txEmpty := tbl.NewTransaction()
	require.NoError(t, txEmpty.AddFiles(t.Context(), nil, nil, false))
	tbl, err = txEmpty.Commit(t.Context())
	require.NoError(t, err)

	head := tbl.CurrentSnapshot()
	require.NotNil(t, head)
	assert.NotEqual(t, withData.SnapshotID, head.SnapshotID, "the branch head must move")
	require.NotNil(t, head.ParentSnapshotID)
	assert.Equal(t, withData.SnapshotID, *head.ParentSnapshotID)
	assert.Equal(t, "1", head.Summary.Properties["total-data-files"],
		"existing data files must stay reachable from the empty snapshot")
}
