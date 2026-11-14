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
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/apache/iceberg-go"
	iceio "github.com/apache/iceberg-go/io"
	"github.com/apache/iceberg-go/table"
	"github.com/stretchr/testify/require"
)

func newMissingEqualityProjectionTestTable(t *testing.T, version int) (*table.Table, iceberg.DataFile, iceberg.DataFile) {
	t.Helper()

	ctx := t.Context()
	tbl := newChangelogMORTable(t, version)
	oldSchema := tbl.Metadata().CurrentSchema()
	dataPath := tbl.Location() + "/data/data-before-category.parquet"
	dataArrowSchema, err := table.SchemaToArrowSchema(oldSchema, nil, false, false)
	require.NoError(t, err)
	writeParquetFile(t, dataPath, dataArrowSchema, `[{"id":1,"data":"a"},{"id":2,"data":"b"}]`)
	dataFile := changelogDataFile(t, dataPath, 2)

	tx := tbl.NewTransaction()
	require.NoError(t, tx.AddDataFilesByPath(ctx, []string{dataPath}, nil, false))
	tbl, err = tx.Commit(ctx)
	require.NoError(t, err)

	tx = tbl.NewTransaction()
	require.NoError(t, tx.UpdateSchema(true, false).
		AddColumn([]string{"category"}, iceberg.PrimitiveTypes.String, "", false, nil).
		Commit())
	tbl, err = tx.Commit(ctx)
	require.NoError(t, err)

	deletePath := tbl.Location() + "/data/eq-del-null-category.parquet"
	deleteSchema := iceberg.NewSchema(0,
		iceberg.NestedField{ID: 3, Name: "category", Type: iceberg.PrimitiveTypes.String},
	)
	deleteArrowSchema, err := table.SchemaToArrowSchema(deleteSchema, nil, true, false)
	require.NoError(t, err)
	writeParquetFile(t, deletePath, deleteArrowSchema, `[{"category":null}]`)
	deleteInfo, err := os.Stat(deletePath)
	require.NoError(t, err)
	deleteBuilder, err := iceberg.NewDataFileBuilder(
		*iceberg.UnpartitionedSpec, iceberg.EntryContentEqDeletes,
		deletePath, iceberg.ParquetFile, nil, nil, nil, 1, deleteInfo.Size())
	require.NoError(t, err)
	deleteBuilder.EqualityFieldIDs([]int{3})

	return tbl, dataFile, deleteBuilder.Build()
}

func TestChangelogMissingEqualityFieldWithPositionDeletes(t *testing.T) {
	ctx := t.Context()
	tbl, dataFile, equalityFile := newMissingEqualityProjectionTestTable(t, 2)
	posPath := tbl.Location() + "/data/pos-delete.parquet"
	writeParquetFile(t, posPath, table.PositionalDeleteArrowSchema, fmt.Sprintf(`[{"file_path":%q,"pos":0}]`, dataFile.FilePath()))
	posInfo, err := os.Stat(posPath)
	require.NoError(t, err)
	posBuilder, err := iceberg.NewDataFileBuilder(*iceberg.UnpartitionedSpec, iceberg.EntryContentPosDeletes,
		posPath, iceberg.ParquetFile, nil, nil, nil, 1, posInfo.Size())
	require.NoError(t, err)

	deletedRows, err := table.NewDeletedRowsScanTask(
		dataFile, []iceberg.DataFile{equalityFile, posBuilder.Build()}, nil, 1, 44)
	require.NoError(t, err)
	_, records, err := tbl.NewIncrementalChangelogScan(
		table.WithSelectedFields("id", "data")).Read(ctx, []table.ChangelogScanTask{deletedRows})
	require.NoError(t, err)

	got := collectChangelogRows(t, records)
	require.Equal(t, []int64{1, 2}, changelogIDs(got, table.ChangelogOpDelete))
}

func TestChangelogMissingEqualityFieldWithDeletionVector(t *testing.T) {
	ctx := t.Context()
	tbl, dataFile, equalityFile := newMissingEqualityProjectionTestTable(t, 3)
	addedDV := writeDV(t, tbl.Location(), "added-dv.puffin", dataFile.FilePath(), []int64{0})

	deletedRows, err := table.NewDeletedRowsScanTask(
		dataFile, []iceberg.DataFile{equalityFile, addedDV}, nil, 1, 44)
	require.NoError(t, err)
	_, records, err := tbl.NewIncrementalChangelogScan(
		table.WithSelectedFields("id", "data")).Read(ctx, []table.ChangelogScanTask{deletedRows})
	require.NoError(t, err)

	got := collectChangelogRows(t, records)
	require.Equal(t, []int64{1, 2}, changelogIDs(got, table.ChangelogOpDelete))
}

func TestConvertMissingEqualityFieldToDeletionVector(t *testing.T) {
	ctx := t.Context()
	tbl, _, equalityFile := newMissingEqualityProjectionTestTable(t, 3)
	tx := tbl.NewTransaction()
	require.NoError(t, tx.NewRowDelta(nil).AddDeletes(equalityFile).Commit(ctx))
	tbl, err := tx.Commit(ctx)
	require.NoError(t, err)
	result, err := tbl.ConvertEqualityDeletesToDeletionVectors(ctx)
	require.NoError(t, err)
	require.Equal(t, 1, result.AddedDeletionVectors)
	assertRowCount(t, result.Table, 0)
}

func TestScanOnlyMissingEqualityField(t *testing.T) {
	ctx := t.Context()
	tbl, _, equalityFile := newMissingEqualityProjectionTestTable(t, 3)
	tx := tbl.NewTransaction()
	require.NoError(t, tx.NewRowDelta(nil).AddDeletes(equalityFile).Commit(ctx))
	tbl, err := tx.Commit(ctx)
	require.NoError(t, err)
	_, records, err := tbl.Scan(table.WithSelectedFields("category")).ToArrowRecords(ctx)
	require.NoError(t, err)
	var count int64
	for rec, err := range records {
		require.NoError(t, err)
		count += rec.NumRows()
		rec.Release()
	}
	require.Zero(t, count)
}

func TestMissingEqualityProjectionNestedPhysicalCarriers(t *testing.T) {
	cases := []struct {
		name string
		typ  iceberg.Type
		data string
	}{
		{"struct", iceberg.NewStructType(iceberg.NestedField{ID: 2, Name: "existing", Type: iceberg.PrimitiveTypes.String}), `[{"payload":null},{"payload":{"existing":"x"}},{"payload":{}}]`},
		{"list", iceberg.NewListType(2, iceberg.PrimitiveTypes.String, false), `[{"payload":null},{"payload":[]},{"payload":["x",null]}]`},
		{"map", iceberg.NewMapType(2, iceberg.PrimitiveTypes.String, 3, iceberg.PrimitiveTypes.String, false), `[{"payload":null},{"payload":[]},{"payload":[{"key":"a","value":"x"}]}]`},
	}
	for _, tc := range cases {
		for _, fileCount := range []int{1, 2} {
			t.Run(fmt.Sprintf("%s/files=%d", tc.name, fileCount), func(t *testing.T) {
				ctx := t.Context()
				loc := filepath.ToSlash(t.TempDir())
				sc := iceberg.NewSchema(0, iceberg.NestedField{ID: 1, Name: "payload", Type: tc.typ})
				meta, err := table.NewMetadata(sc, iceberg.UnpartitionedSpec, table.UnsortedSortOrder, loc, iceberg.Properties{table.PropertyFormatVersion: "3"})
				require.NoError(t, err)
				tbl := table.New(table.Identifier{"db", "nested_carrier"}, meta, loc+"/metadata/v1.metadata.json", func(context.Context) (iceio.IO, error) { return iceio.LocalFS{}, nil }, &rowDeltaCatalog{metadata: meta})
				arrSc, err := table.SchemaToArrowSchema(sc, nil, false, false)
				require.NoError(t, err)
				var paths []string
				for i := range fileCount {
					path := fmt.Sprintf("%s/data/old-%d.parquet", loc, i)
					writeParquetFile(t, path, arrSc, tc.data)
					paths = append(paths, path)
				}
				tx := tbl.NewTransaction()
				require.NoError(t, tx.AddDataFilesByPath(ctx, paths, nil, false))
				tbl, err = tx.Commit(ctx)
				require.NoError(t, err)
				tx = tbl.NewTransaction()
				require.NoError(t, tx.UpdateSchema(true, false).AddColumn([]string{"category"}, iceberg.PrimitiveTypes.String, "", false, iceberg.StringLiteral("default")).Commit())
				tbl, err = tx.Commit(ctx)
				require.NoError(t, err)
				_, records, err := tbl.Scan(table.WithSelectedFields("category")).ToArrowRecords(ctx)
				require.NoError(t, err)
				var count int64
				for rec, err := range records {
					require.NoError(t, err)
					require.EqualValues(t, 1, rec.NumCols())
					require.Zero(t, rec.Column(0).NullN())
					count += rec.NumRows()
					rec.Release()
				}
				require.EqualValues(t, 3*fileCount, count)
				cat, found := tbl.Schema().FindFieldByName("category")
				require.True(t, found)
				tbl = appendEqualityDelete(t, tbl, []int{cat.ID}, `[{"category":"default"}]`)
				result, err := tbl.ConvertEqualityDeletesToDeletionVectors(ctx)
				require.NoError(t, err)
				require.Equal(t, fileCount, result.AddedDeletionVectors)
				assertRowCount(t, result.Table, 0)
			})
		}
	}
}
