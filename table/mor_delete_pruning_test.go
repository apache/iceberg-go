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

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/iceberg-go"
	iceio "github.com/apache/iceberg-go/io"
	"github.com/apache/iceberg-go/table"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestMergeOnReadDeleteAcrossPrunedRowGroups guards the row-group pruning hazard
// in generated position deletes: a merge-on-read DELETE whose filter prunes a
// leading row group must still delete the right physical rows. enrichRecords-
// WithPosDeleteFields stamps each surviving row's position from a rowPositionCursor
// seeded with the surviving row groups, so a pruned group's rows are still counted
// and the survivors keep their original file positions.
//
// The file has two row groups (ids 1..5, then 6..10). Deleting id == 7 matches
// only the second group, so the first is pruned by stats; a dense position
// count would target physical position 1 (id=2) instead of 6 (id=7).
func TestMergeOnReadDeleteAcrossPrunedRowGroups(t *testing.T) {
	ctx := context.Background()
	tbl := newMergeOnReadTestTable(t)

	arrowSchema := arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int64, Nullable: false},
		{Name: "data", Type: arrow.BinaryTypes.String, Nullable: true},
	}, nil)
	data, err := array.TableFromJSON(memory.DefaultAllocator, arrowSchema, []string{
		`[{"id":1,"data":"a"},{"id":2,"data":"b"},{"id":3,"data":"c"},{"id":4,"data":"d"},` +
			`{"id":5,"data":"e"},{"id":6,"data":"f"},{"id":7,"data":"g"},{"id":8,"data":"h"},` +
			`{"id":9,"data":"i"},{"id":10,"data":"j"}]`,
	})
	require.NoError(t, err)
	defer data.Release()

	tbl, err = tbl.Append(ctx, array.NewTableReader(data, -1), nil)
	require.NoError(t, err)
	require.Equal(t, []int64{1, 2, 3, 4, 5, 6, 7, 8, 9, 10}, idsInTable(t, tbl))

	tbl, err = tbl.Delete(ctx, iceberg.EqualTo(iceberg.Reference("id"), int64(7)), nil)
	require.NoError(t, err)

	assert.Equal(t, []int64{1, 2, 3, 4, 5, 6, 8, 9, 10}, idsInTable(t, tbl),
		"only id=7 must be deleted; a pruned leading row group must not shift positions")
}

// newMergeOnReadTestTable builds a v2 table that deletes via merge-on-read and
// caps row groups at 5 rows so a 10-row append spans two of them.
func newMergeOnReadTestTable(t *testing.T) *table.Table {
	t.Helper()

	location := filepath.ToSlash(t.TempDir())
	schema := iceberg.NewSchema(0,
		iceberg.NestedField{ID: 1, Name: "id", Type: iceberg.PrimitiveTypes.Int64, Required: true},
		iceberg.NestedField{ID: 2, Name: "data", Type: iceberg.PrimitiveTypes.String, Required: false},
	)
	meta, err := table.NewMetadata(schema, iceberg.UnpartitionedSpec, table.UnsortedSortOrder, location,
		iceberg.Properties{
			table.PropertyFormatVersion:   "2",
			table.WriteDeleteModeKey:      table.WriteModeMergeOnRead,
			table.ParquetRowGroupLimitKey: "5",
		})
	require.NoError(t, err)

	metaLoc := location + "/metadata/v1.metadata.json"
	fsF := func(context.Context) (iceio.IO, error) { return iceio.LocalFS{}, nil }
	cat := &concurrentTestCatalog{metadata: meta, location: metaLoc, fsF: fsF}

	return table.New(table.Identifier{"db", "mor_delete_test"}, meta, metaLoc, fsF, cat)
}

// idsInTable scans the table and returns the surviving id values, sorted.
func idsInTable(t *testing.T, tbl *table.Table) []int64 {
	t.Helper()

	_, itr, err := tbl.Scan().ToArrowRecords(context.Background())
	require.NoError(t, err)

	var ids []int64
	for rec, err := range itr {
		require.NoError(t, err)
		idIdx := rec.Schema().FieldIndices("id")
		require.NotEmpty(t, idIdx)
		col := rec.Column(idIdx[0]).(*array.Int64)
		for i := range int(rec.NumRows()) {
			ids = append(ids, col.Value(i))
		}
		rec.Release()
	}
	slices.Sort(ids)

	return ids
}
