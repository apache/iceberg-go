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
	"slices"
	"testing"

	"github.com/apache/iceberg-go"
	iceio "github.com/apache/iceberg-go/io"
	"github.com/apache/iceberg-go/table"
	"github.com/stretchr/testify/require"
)

// A transaction builds its metadata several times before it commits (for a
// copy-on-write delete: while classifying files, once per rewritten file, and again
// for StagedTable). However many builds there are, the staged table must carry a
// single previous-metadata entry.
func TestCopyOnWriteDeleteStagesSinglePreviousMetadataLogEntry(t *testing.T) {
	ctx := context.Background()
	location := filepath.ToSlash(t.TempDir())
	schema := iceberg.NewSchema(0,
		iceberg.NestedField{ID: 1, Name: "id", Type: iceberg.PrimitiveTypes.Int64, Required: true},
		iceberg.NestedField{ID: 2, Name: "data", Type: iceberg.PrimitiveTypes.String, Required: false},
	)
	meta, err := table.NewMetadata(schema, iceberg.UnpartitionedSpec, table.UnsortedSortOrder, location,
		iceberg.Properties{
			table.PropertyFormatVersion: "2",
			table.WriteDeleteModeKey:    table.WriteModeCopyOnWrite,
		})
	require.NoError(t, err)

	metaLoc := location + "/metadata/v1.metadata.json"
	fsF := func(context.Context) (iceio.IO, error) { return iceio.LocalFS{}, nil }
	cat := &concurrentTestCatalog{metadata: meta, location: metaLoc, fsF: fsF}
	tbl := table.New(table.Identifier{"db", "cow_metadata_log"}, meta, metaLoc, fsF, cat)
	tbl = appendTenRows(t, appendTenRows(t, tbl))

	tasks, err := tbl.Scan().PlanFiles(ctx)
	require.NoError(t, err)
	require.Len(t, tasks, 2, "setup: the delete must rewrite more than one file")

	txn := tbl.NewTransaction()
	require.NoError(t, txn.Delete(ctx, iceberg.EqualTo(iceberg.Reference("id"), int64(2)), nil))

	staged, err := txn.StagedTable()
	require.NoError(t, err)
	require.Len(t, slices.Collect(staged.Metadata().PreviousFiles()), 1,
		"each Build must not add another previous-metadata entry")
}
