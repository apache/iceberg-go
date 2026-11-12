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
	"strings"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/arrow-go/v18/parquet"
	"github.com/apache/arrow-go/v18/parquet/pqarrow"
	"github.com/apache/iceberg-go"
	iceio "github.com/apache/iceberg-go/io"
	"github.com/stretchr/testify/require"
)

func TestEnsureNameMappingPreservesExistingProperty(t *testing.T) {
	for _, tc := range []struct {
		name    string
		value   string
		present bool
	}{
		{name: "missing"},
		{name: "valid", value: `[{"field-id":1,"names":["old","new"]}]`, present: true},
		{name: "empty array", value: `[]`, present: true},
		{name: "invalid JSON", value: `{not valid json`, present: true},
		{name: "null", value: `null`, present: true},
		{name: "empty string", value: ``, present: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			txn, _ := createTestTransactionWithMemIO(t, *iceberg.UnpartitionedSpec)
			if tc.present {
				require.NoError(t, txn.SetProperties(iceberg.Properties{DefaultNameMappingKey: tc.value}))
			}
			require.NoError(t, txn.ensureNameMapping())
			meta, err := txn.txnMeta()
			require.NoError(t, err)
			if tc.present {
				require.Equal(t, tc.value, meta.props[DefaultNameMappingKey])
			} else {
				require.JSONEq(t, `[{"field-id":1,"names":["id"]}]`, meta.props[DefaultNameMappingKey])
			}
		})
	}
}

func TestScanRejectsUnparseableNameMapping(t *testing.T) {
	for _, tc := range []struct {
		name      string
		value     string
		present   bool
		wantError bool
	}{
		{name: "missing"},
		{name: "valid", value: `[{"field-id":1,"names":["id"]}]`, present: true},
		{name: "empty array", value: `[]`, present: true},
		{name: "invalid JSON", value: `{not valid json`, present: true, wantError: true},
		{name: "null", value: `null`, present: true, wantError: true},
		{name: "empty string", value: ``, present: true, wantError: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			dir := filepath.ToSlash(t.TempDir())
			tbl, tasks := newReorderScanFixture(t, dir, 1, 4, iceio.LocalFS{})
			txn := tbl.NewTransaction()
			if tc.present {
				require.NoError(t, txn.SetProperties(iceberg.Properties{DefaultNameMappingKey: tc.value}))
			}
			scan, err := txn.Scan()
			require.NoError(t, err)
			_, records, err := scan.ReadTasks(t.Context(), tasks)
			if tc.wantError {
				require.ErrorIs(t, err, ErrInvalidMetadata)
				require.ErrorContains(t, err, DefaultNameMappingKey)

				return
			}
			require.NoError(t, err)
			var count int64
			for record, err := range records {
				require.NoError(t, err)
				count += record.NumRows()
				record.Release()
			}
			require.EqualValues(t, 4, count)
		})
	}
}

func TestScanWithZeroLimitSkipsNameMapping(t *testing.T) {
	dir := filepath.ToSlash(t.TempDir())
	tbl, tasks := newReorderScanFixture(t, dir, 1, 4, iceio.LocalFS{})
	txn := tbl.NewTransaction()
	require.NoError(t, txn.SetProperties(iceberg.Properties{DefaultNameMappingKey: "{"}))
	scan, err := txn.Scan(WithLimit(0))
	require.NoError(t, err)
	_, records, err := scan.ReadTasks(t.Context(), tasks)
	require.NoError(t, err)
	for record, err := range records {
		require.NoError(t, err)
		record.Release()
		t.Fatal("zero-limit scan returned a record")
	}
}

func TestScanWithoutTasksSkipsNameMapping(t *testing.T) {
	dir := filepath.ToSlash(t.TempDir())
	tbl, _ := newReorderScanFixture(t, dir, 1, 4, iceio.LocalFS{})
	txn := tbl.NewTransaction()
	require.NoError(t, txn.SetProperties(iceberg.Properties{DefaultNameMappingKey: "{"}))
	scan, err := txn.Scan()
	require.NoError(t, err)
	_, records, err := scan.ReadTasks(t.Context(), nil)
	require.NoError(t, err)
	for record, err := range records {
		require.NoError(t, err)
		record.Release()
		t.Fatal("empty scan returned a record")
	}
}

func TestInspectPositionDeletesRejectsUnparseableNameMapping(t *testing.T) {
	for _, mapping := range []string{"{not valid json", "null", ""} {
		t.Run(mapping, func(t *testing.T) {
			dir := filepath.ToSlash(t.TempDir())
			fs := iceio.LocalFS{}
			tbl, _ := newReorderScanFixture(t, dir, 1, 4, fs)
			txn := tbl.NewTransaction()
			require.NoError(t, txn.SetProperties(iceberg.Properties{DefaultNameMappingKey: mapping}))
			builder, err := txn.txnMeta()
			require.NoError(t, err)
			meta, err := builder.Build()
			require.NoError(t, err)
			tbl = New(tbl.Identifier(), meta, tbl.MetadataLocation(),
				func(context.Context) (iceio.IO, error) { return fs, nil }, nil)
			_, err = tbl.Inspect().PositionDeletes(t.Context())
			require.ErrorIs(t, err, ErrInvalidMetadata)
			require.ErrorContains(t, err, DefaultNameMappingKey)
		})
	}
}

func TestImportWithoutFieldIDsRespectsNameMapping(t *testing.T) {
	for _, replace := range []bool{false, true} {
		name := "add"
		if replace {
			name = "replace"
		}
		t.Run(name, func(t *testing.T) {
			for _, tc := range []struct {
				name      string
				mapping   string
				present   bool
				wantError bool
			}{
				{name: "missing"},
				{name: "valid", mapping: `[{"field-id":1,"names":["id"]},{"field-id":2,"names":["data"]}]`, present: true},
				{name: "null", mapping: "null", present: true, wantError: true},
				{name: "invalid JSON", mapping: "{", present: true, wantError: true},
			} {
				t.Run(tc.name, func(t *testing.T) {
					dir := filepath.ToSlash(t.TempDir())
					fs := iceio.LocalFS{}
					tbl, files := newReorderScanFixture(t, dir, 1, 4, fs)
					txn := tbl.NewTransaction()
					require.NoError(t, txn.AddDataFiles(t.Context(), []iceberg.DataFile{files[0].File}, nil, WithoutAutoNameMapping()))
					if tc.present {
						require.NoError(t, txn.SetProperties(iceberg.Properties{DefaultNameMappingKey: tc.mapping}))
					}
					before, err := txn.txnMeta()
					require.NoError(t, err)
					beforeSnapshot := before.currentSnapshot().SnapshotID

					schema := arrow.NewSchema([]arrow.Field{
						{Name: "id", Type: arrow.PrimitiveTypes.Int64, Nullable: false},
						{Name: "data", Type: arrow.BinaryTypes.String, Nullable: false},
					}, nil)
					record, _, err := array.RecordFromJSON(memory.DefaultAllocator, schema, strings.NewReader(`[{"id":1,"data":"value"}]`))
					require.NoError(t, err)
					defer record.Release()
					data := array.NewTableFromRecords(schema, []arrow.RecordBatch{record})
					defer data.Release()
					path := dir + "/data/no-ids.parquet"
					out, err := fs.Create(t.Context(), path)
					require.NoError(t, err)
					defer out.Close()
					require.NoError(t, pqarrow.WriteTable(data, out, 1, parquet.NewWriterProperties(), pqarrow.DefaultWriterProps()))

					if replace {
						err = txn.ReplaceDataFilesByPath(t.Context(), []string{files[0].File.FilePath()}, []string{path}, nil)
					} else {
						err = txn.AddDataFilesByPath(t.Context(), []string{path}, nil, false)
					}
					if tc.wantError {
						require.ErrorIs(t, err, ErrInvalidMetadata)
					} else {
						require.NoError(t, err)
					}
					meta, err := txn.txnMeta()
					require.NoError(t, err)
					if tc.present {
						require.Equal(t, tc.mapping, meta.props[DefaultNameMappingKey])
					}
					if tc.wantError {
						require.Equal(t, beforeSnapshot, meta.currentSnapshot().SnapshotID)

						return
					}
					scan, err := txn.Scan()
					require.NoError(t, err)
					_, records, err := scan.ToArrowRecords(t.Context())
					require.NoError(t, err)
					var count int64
					for batch, err := range records {
						require.NoError(t, err)
						count += batch.NumRows()
						batch.Release()
					}
					wantCount := int64(5)
					if replace {
						wantCount = 1
					}
					require.Equal(t, wantCount, count)
				})
			}
		})
	}
}

func TestFileWritesPreserveExistingNameMappingProperty(t *testing.T) {
	const invalidMapping = `{not valid json`
	for _, tc := range []struct {
		name      string
		wantError bool
		run       func(*testing.T, *Transaction, []FileScanTask) error
	}{
		{"add data files", false, func(t *testing.T, txn *Transaction, files []FileScanTask) error {
			return txn.AddDataFiles(t.Context(), []iceberg.DataFile{files[1].File}, nil)
		}},
		{"replace data files", false, func(t *testing.T, txn *Transaction, files []FileScanTask) error {
			return txn.ReplaceDataFiles(t.Context(), []iceberg.DataFile{files[0].File}, []iceberg.DataFile{files[1].File}, nil)
		}},
		{"add by path", true, func(t *testing.T, txn *Transaction, files []FileScanTask) error {
			return txn.AddDataFilesByPath(t.Context(), []string{files[1].File.FilePath()}, nil, false)
		}},
		{"replace by path", true, func(t *testing.T, txn *Transaction, files []FileScanTask) error {
			return txn.ReplaceDataFilesByPath(t.Context(), []string{files[0].File.FilePath()}, []string{files[1].File.FilePath()}, nil)
		}},
		{"empty add by path", false, func(t *testing.T, txn *Transaction, _ []FileScanTask) error {
			return txn.AddDataFilesByPath(t.Context(), nil, nil, false)
		}},
		{"remove by path", false, func(t *testing.T, txn *Transaction, files []FileScanTask) error {
			return txn.ReplaceDataFilesByPath(t.Context(), []string{files[0].File.FilePath()}, nil, nil)
		}},
		{"copy on write delete", false, func(t *testing.T, txn *Transaction, _ []FileScanTask) error {
			return txn.Delete(t.Context(), iceberg.AlwaysTrue{}, nil)
		}},
		{"merge on read delete", false, func(t *testing.T, txn *Transaction, _ []FileScanTask) error {
			if err := txn.SetProperties(iceberg.Properties{WriteDeleteModeKey: WriteModeMergeOnRead}); err != nil {
				return err
			}

			return txn.Delete(t.Context(), iceberg.AlwaysTrue{}, nil)
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			dir := filepath.ToSlash(t.TempDir())
			tbl, files := newReorderScanFixture(t, dir, 2, 4, iceio.LocalFS{})
			txn := tbl.NewTransaction()
			require.NoError(t, txn.AddDataFiles(t.Context(), []iceberg.DataFile{files[0].File}, nil, WithoutAutoNameMapping()))
			require.NoError(t, txn.SetProperties(iceberg.Properties{DefaultNameMappingKey: invalidMapping}))
			before, err := txn.txnMeta()
			require.NoError(t, err)
			beforeSnapshot := before.currentSnapshot().SnapshotID
			err = tc.run(t, txn, files)
			if tc.wantError {
				require.ErrorIs(t, err, ErrInvalidMetadata)
			} else {
				require.NoError(t, err)
			}
			meta, err := txn.txnMeta()
			require.NoError(t, err)
			require.Equal(t, invalidMapping, meta.props[DefaultNameMappingKey])
			if tc.wantError {
				require.Equal(t, beforeSnapshot, meta.currentSnapshot().SnapshotID)
			}
		})
	}
}
