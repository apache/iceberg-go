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

package table

import (
	"context"
	"path/filepath"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/iceberg-go"
	iceio "github.com/apache/iceberg-go/io"
	"github.com/stretchr/testify/require"
)

// buildV3TableWithRows builds a v3 table backed by a counting catalog and
// appends a single data file containing rowsJSON.
func buildV3TableWithRows(t *testing.T, rowsJSON string) *Table {
	t.Helper()

	location := filepath.ToSlash(t.TempDir())
	schema := iceberg.NewSchema(0,
		iceberg.NestedField{ID: 1, Name: "id", Type: iceberg.PrimitiveTypes.Int64, Required: true},
		iceberg.NestedField{ID: 2, Name: "data", Type: iceberg.PrimitiveTypes.String, Required: false},
	)
	meta, err := NewMetadata(schema, iceberg.UnpartitionedSpec, UnsortedSortOrder, location,
		iceberg.Properties{PropertyFormatVersion: "3"})
	require.NoError(t, err)

	cat := &countingCatalog{metadata: meta}
	tbl := New(Identifier{"db", "vdfe"}, meta, location+"/metadata/v1.metadata.json",
		func(context.Context) (iceio.IO, error) { return iceio.LocalFS{}, nil }, cat)

	arrowSchema := arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int64, Nullable: false},
		{Name: "data", Type: arrow.BinaryTypes.String, Nullable: true},
	}, nil)
	data, err := array.TableFromJSON(memory.DefaultAllocator, arrowSchema, []string{rowsJSON})
	require.NoError(t, err)
	defer data.Release()

	tbl, err = tbl.Append(context.Background(), array.NewTableReader(data, -1), nil)
	require.NoError(t, err)

	return tbl
}

// TestValidateDataFilesExist_TreatsConcurrentlyRemovedFileAsMissing pins the
// status-aware existence check shared by the RowDelta and merge-on-read delete
// conflict validators: a data file that a concurrent commit rewrote away leaves
// a DELETED tombstone on the head, which must NOT satisfy existence (the rows
// moved to a new file, orphaning any position delete that referenced it). Only
// a live (ADDED/EXISTING) entry counts as present.
func TestValidateDataFilesExist_TreatsConcurrentlyRemovedFileAsMissing(t *testing.T) {
	ctx := context.Background()
	tbl := buildV3TableWithRows(t, `[{"id":1,"data":"a"},{"id":2,"data":"b"}]`)

	tasks, err := tbl.Scan().PlanFiles(ctx)
	require.NoError(t, err)
	require.Len(t, tasks, 1)
	dataFile := tasks[0].File

	fs := iceio.LocalFS{}

	// Control: while the file is live on the head, it satisfies existence.
	ccLive := &conflictContext{current: tbl.metadata, branch: MainBranch, fs: fs}
	require.NoError(t, validateDataFilesExist(ccLive, []string{dataFile.FilePath()}))

	// A compaction rewrites the file away, leaving a DELETED tombstone on the
	// head's data manifests.
	rtx := tbl.NewTransaction()
	require.NoError(t, rtx.NewRewrite(nil).DeleteFile(dataFile).Commit(ctx))
	tbl2, err := rtx.Commit(ctx)
	require.NoError(t, err)

	// The tombstone must not be mistaken for a live file: existence fails.
	ccRemoved := &conflictContext{current: tbl2.metadata, branch: MainBranch, fs: fs}
	err = validateDataFilesExist(ccRemoved, []string{dataFile.FilePath()})
	require.Error(t, err)
	require.ErrorIs(t, err, ErrDataFilesMissing)
}
