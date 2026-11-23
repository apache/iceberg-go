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

	"github.com/apache/iceberg-go"
	iceio "github.com/apache/iceberg-go/io"
	"github.com/apache/iceberg-go/table"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// newPromotablePartitionTable builds a table identity-partitioned on a
// promotable column "p", so manifests written before and after promoting p
// decode its partition values with different Go widths.
func newPromotablePartitionTable(t *testing.T, formatVersion string, partType iceberg.Type) *table.Table {
	t.Helper()

	location := filepath.ToSlash(t.TempDir())
	schema := iceberg.NewSchema(0,
		iceberg.NestedField{ID: 1, Name: "id", Type: iceberg.PrimitiveTypes.Int64, Required: true},
		iceberg.NestedField{ID: 2, Name: "p", Type: partType, Required: false},
	)
	spec := iceberg.NewPartitionSpec(
		iceberg.PartitionField{SourceIDs: []int{2}, FieldID: 1000, Name: "p", Transform: iceberg.IdentityTransform{}},
	)
	meta, err := table.NewMetadata(schema, &spec, table.UnsortedSortOrder, location,
		iceberg.Properties{
			table.PropertyFormatVersion: formatVersion,
			table.WriteDeleteModeKey:    table.WriteModeMergeOnRead,
		})
	require.NoError(t, err)

	metaLoc := location + "/metadata/v1.metadata.json"
	fsF := func(context.Context) (iceio.IO, error) { return iceio.LocalFS{}, nil }
	cat := &concurrentTestCatalog{metadata: meta, location: metaLoc, fsF: fsF}

	return table.New(table.Identifier{"db", "partition_promotion"}, meta, metaLoc, fsF, cat)
}

func promotePartitionSource(t *testing.T, tbl *table.Table, target iceberg.Type) *table.Table {
	t.Helper()

	tx := tbl.NewTransaction()
	require.NoError(t, tx.UpdateSchema(true, false).
		UpdateColumn([]string{"p"}, table.ColumnUpdate{
			FieldType: iceberg.Optional[iceberg.Type]{Valid: true, Val: target},
		}).Commit())
	tbl, err := tx.Commit(t.Context())
	require.NoError(t, err)

	return tbl
}

// TestMergeOnReadDeleteAfterPartitionSourcePromotion covers #2605: a v2 MoR
// delete on a file written before the promotion records the partition with
// the new width, and the scan must still pair it with the old data file.
func TestMergeOnReadDeleteAfterPartitionSourcePromotion(t *testing.T) {
	tests := []struct {
		name     string
		from, to iceberg.Type
		rows     string
	}{
		{
			name: "int to long",
			from: iceberg.PrimitiveTypes.Int32, to: iceberg.PrimitiveTypes.Int64,
			rows: `[{"id": 1, "p": 7}, {"id": 2, "p": 7}, {"id": 3, "p": 8}]`,
		},
		{
			name: "float to double",
			from: iceberg.PrimitiveTypes.Float32, to: iceberg.PrimitiveTypes.Float64,
			rows: `[{"id": 1, "p": 1.5}, {"id": 2, "p": 1.5}, {"id": 3, "p": 4.5}]`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tbl := newPromotablePartitionTable(t, "2", tt.from)
			tbl = appendRows(t, tbl, tt.rows)
			tbl = promotePartitionSource(t, tbl, tt.to)

			tbl, err := tbl.Delete(t.Context(), iceberg.EqualTo(iceberg.Reference("id"), int64(1)), nil)
			require.NoError(t, err)

			assert.Equal(t, []int64{2, 3}, idsInTable(t, tbl))
		})
	}
}

// TestEqualityDeletesAfterFloatPartitionSourcePromotion covers #2610: an
// equality delete written in a promoted double partition must apply to data
// files whose manifests still decode the partition as float. A delete whose
// partition differs after widening must not.
func TestEqualityDeletesAfterFloatPartitionSourcePromotion(t *testing.T) {
	for _, formatVersion := range []string{"2", "3"} {
		t.Run("v"+formatVersion, func(t *testing.T) {
			tbl := newPromotablePartitionTable(t, formatVersion, iceberg.PrimitiveTypes.Float32)
			tbl = appendRows(t, tbl, `[{"id": 1, "p": 1.5}, {"id": 2, "p": 1.5}, {"id": 3, "p": 4.5}]`)
			tbl = promotePartitionSource(t, tbl, iceberg.PrimitiveTypes.Float64)

			// id 3 lives in partition 4.5, so the delete in partition 4.6
			// must not remove it.
			tbl = commitPartitionedEqualityDeletes(t, tbl, `[{"id": 1, "p": 1.5}, {"id": 3, "p": 4.6}]`)

			assert.Equal(t, []int64{2, 3}, idsInTable(t, tbl))
		})
	}
}

// TestCompactionKeepsEqualityDeleteAfterFloatPartitionSourcePromotion keeps a delete that still applies to a pre-promotion file.
func TestCompactionKeepsEqualityDeleteAfterFloatPartitionSourcePromotion(t *testing.T) {
	for _, formatVersion := range []string{"2", "3"} {
		t.Run("v"+formatVersion, func(t *testing.T) {
			tbl := newPromotablePartitionTable(t, formatVersion, iceberg.PrimitiveTypes.Float32)
			tbl = appendRows(t, tbl, `[{"id": 1, "p": 1.1}, {"id": 2, "p": 1.1}]`)
			tbl = promotePartitionSource(t, tbl, iceberg.PrimitiveTypes.Float64)
			tbl = appendRows(t, tbl, `[{"id": 3, "p": 4.5}]`)
			tbl = appendRows(t, tbl, `[{"id": 4, "p": 4.5}]`)

			tbl = commitPartitionedEqualityDeletes(t, tbl, `[{"id": 1, "p": 1.100000023841858}]`)
			require.Equal(t, []int64{2, 3, 4}, idsInTable(t, tbl))

			tasks, err := tbl.Scan().PlanFiles(t.Context())
			require.NoError(t, err)
			var group table.CompactionTaskGroup
			for _, task := range tasks {
				if p, ok := task.File.Partition()[1000].(float64); ok && p == 4.5 {
					group.Tasks = append(group.Tasks, task)
					group.TotalSizeBytes += task.File.FileSizeBytes()
				}
			}
			require.Len(t, group.Tasks, 2)

			_, tbl = runRewriteWithCleanup(t, tbl, []table.CompactionTaskGroup{group}, false)
			assert.Equal(t, []int64{2, 3, 4}, idsInTable(t, tbl))
		})
	}
}

// commitPartitionedEqualityDeletes commits equality deletes on id, with each row's partition taken from its p value.
func commitPartitionedEqualityDeletes(t *testing.T, tbl *table.Table, recordsJSON string) *table.Table {
	t.Helper()

	arrowSc, err := table.SchemaToArrowSchema(tbl.Schema(), nil, true, false)
	require.NoError(t, err)
	records, release := makeEqDeleteRecords(t, arrowSc, recordsJSON)
	defer release()

	tx := tbl.NewTransaction()
	require.NoError(t, tx.CommitEqualityDeletes(t.Context(), []int{1}, records, nil))
	out, err := tx.Commit(t.Context())
	require.NoError(t, err)

	return out
}
