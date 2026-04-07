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
	"iter"
	"path/filepath"
	"strconv"
	"strings"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/arrow-go/v18/parquet"
	"github.com/apache/arrow-go/v18/parquet/pqarrow"
	"github.com/apache/iceberg-go"
	iceio "github.com/apache/iceberg-go/io"
	"github.com/apache/iceberg-go/table"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func newRowDeltaTestTable(t *testing.T, formatVersion int) *table.Table {
	t.Helper()

	schema := iceberg.NewSchema(0,
		iceberg.NestedField{ID: 1, Name: "id", Type: iceberg.PrimitiveTypes.Int64, Required: true},
		iceberg.NestedField{ID: 2, Name: "data", Type: iceberg.PrimitiveTypes.String, Required: false},
	)

	meta, err := table.NewMetadata(schema, iceberg.UnpartitionedSpec,
		table.UnsortedSortOrder, "s3://bucket/test",
		iceberg.Properties{table.PropertyFormatVersion: formatVersionStr(formatVersion)})
	require.NoError(t, err)

	return table.New(
		table.Identifier{"db", "test_table"},
		meta, "s3://bucket/test/metadata/v1.metadata.json",
		nil, nil,
	)
}

func formatVersionStr(v int) string {
	return string(rune('0' + v))
}

func buildDataFile(t *testing.T, path string) iceberg.DataFile {
	t.Helper()

	b, err := iceberg.NewDataFileBuilder(
		*iceberg.UnpartitionedSpec, iceberg.EntryContentData,
		path, iceberg.ParquetFile, nil, nil, nil, 10, 1024)
	require.NoError(t, err)

	return b.Build()
}

func buildPosDeleteFile(t *testing.T, path string) iceberg.DataFile {
	t.Helper()

	b, err := iceberg.NewDataFileBuilder(
		*iceberg.UnpartitionedSpec, iceberg.EntryContentPosDeletes,
		path, iceberg.ParquetFile, nil, nil, nil, 5, 512)
	require.NoError(t, err)

	return b.Build()
}

func buildEqDeleteFile(t *testing.T, path string, fieldIDs []int) iceberg.DataFile {
	t.Helper()

	b, err := iceberg.NewDataFileBuilder(
		*iceberg.UnpartitionedSpec, iceberg.EntryContentEqDeletes,
		path, iceberg.ParquetFile, nil, nil, nil, 3, 256)
	require.NoError(t, err)
	b.EqualityFieldIDs(fieldIDs)

	return b.Build()
}

func TestRowDeltaOperationDataOnly(t *testing.T) {
	tbl := newRowDeltaTestTable(t, 2)
	rd := tbl.NewTransaction().NewRowDelta(nil)
	rd.AddRows(buildDataFile(t, "s3://bucket/data/file1.parquet"))

	assert.Equal(t, table.OpAppend, rd.Operation())
}

func TestRowDeltaOperationDeleteOnly(t *testing.T) {
	tbl := newRowDeltaTestTable(t, 2)
	rd := tbl.NewTransaction().NewRowDelta(nil)
	rd.AddDeletes(buildPosDeleteFile(t, "s3://bucket/data/del1.parquet"))

	assert.Equal(t, table.OpDelete, rd.Operation())
}

func TestRowDeltaOperationBoth(t *testing.T) {
	tbl := newRowDeltaTestTable(t, 2)
	rd := tbl.NewTransaction().NewRowDelta(nil)
	rd.AddRows(buildDataFile(t, "s3://bucket/data/file1.parquet"))
	rd.AddDeletes(buildPosDeleteFile(t, "s3://bucket/data/del1.parquet"))

	assert.Equal(t, table.OpOverwrite, rd.Operation())
}

func TestRowDeltaCommitEmpty(t *testing.T) {
	tbl := newRowDeltaTestTable(t, 2)
	rd := tbl.NewTransaction().NewRowDelta(nil)

	err := rd.Commit(t.Context())
	require.Error(t, err)
	assert.Contains(t, err.Error(), "at least one data file or delete file")
}

func TestRowDeltaRejectsDataFileInDeletes(t *testing.T) {
	tbl := newRowDeltaTestTable(t, 2)
	rd := tbl.NewTransaction().NewRowDelta(nil)
	rd.AddDeletes(buildDataFile(t, "s3://bucket/data/file1.parquet"))

	err := rd.Commit(t.Context())
	require.Error(t, err)
	assert.Contains(t, err.Error(), "expected delete file")
}

func TestRowDeltaRejectsDeleteFileInRows(t *testing.T) {
	tbl := newRowDeltaTestTable(t, 2)
	rd := tbl.NewTransaction().NewRowDelta(nil)
	rd.AddRows(buildPosDeleteFile(t, "s3://bucket/data/del1.parquet"))

	err := rd.Commit(t.Context())
	require.Error(t, err)
	assert.Contains(t, err.Error(), "expected data file")
}

func TestRowDeltaAcceptsEqualityDeleteFiles(t *testing.T) {
	tbl := newRowDeltaTestTable(t, 2)
	rd := tbl.NewTransaction().NewRowDelta(nil)
	rd.AddDeletes(buildEqDeleteFile(t, "s3://bucket/data/eq-del1.parquet", []int{1}))

	assert.Equal(t, table.OpDelete, rd.Operation())
}

func TestRowDeltaChaining(t *testing.T) {
	tbl := newRowDeltaTestTable(t, 2)
	rd := tbl.NewTransaction().NewRowDelta(nil).
		AddRows(buildDataFile(t, "s3://bucket/data/file1.parquet")).
		AddDeletes(buildEqDeleteFile(t, "s3://bucket/data/eq-del1.parquet", []int{1}))

	assert.Equal(t, table.OpOverwrite, rd.Operation())
}

func TestRowDeltaRejectsDeleteFilesOnV1Table(t *testing.T) {
	tbl := newRowDeltaTestTable(t, 1)
	rd := tbl.NewTransaction().NewRowDelta(nil)
	rd.AddDeletes(buildPosDeleteFile(t, "s3://bucket/data/del1.parquet"))

	err := rd.Commit(t.Context())
	require.Error(t, err)
	assert.Contains(t, err.Error(), "format version >= 2")
}

func TestRowDeltaAllowsDataOnlyOnV1Table(t *testing.T) {
	tbl := newRowDeltaTestTable(t, 1)
	rd := tbl.NewTransaction().NewRowDelta(nil)
	rd.AddRows(buildDataFile(t, "s3://bucket/data/file1.parquet"))

	// Operation selection should work — data-only on v1 is fine.
	assert.Equal(t, table.OpAppend, rd.Operation())
}

func TestRowDeltaRejectsEqDeleteWithoutFieldIDs(t *testing.T) {
	tbl := newRowDeltaTestTable(t, 2)

	// Build an equality delete file without setting EqualityFieldIDs
	b, err := iceberg.NewDataFileBuilder(
		*iceberg.UnpartitionedSpec, iceberg.EntryContentEqDeletes,
		"s3://bucket/data/eq-del.parquet", iceberg.ParquetFile, nil, nil, nil, 3, 256)
	require.NoError(t, err)
	df := b.Build()

	rd := tbl.NewTransaction().NewRowDelta(nil)
	rd.AddDeletes(df)

	err = rd.Commit(t.Context())
	require.Error(t, err)
	assert.Contains(t, err.Error(), "non-empty EqualityFieldIDs")
}

func TestRowDeltaRejectsEqDeleteWithInvalidFieldID(t *testing.T) {
	tbl := newRowDeltaTestTable(t, 2)
	rd := tbl.NewTransaction().NewRowDelta(nil)
	// Field ID 999 does not exist in the schema (which has fields 1 and 2)
	rd.AddDeletes(buildEqDeleteFile(t, "s3://bucket/data/eq-del.parquet", []int{999}))

	err := rd.Commit(t.Context())
	require.Error(t, err)
	assert.Contains(t, err.Error(), "not found in table schema")
}

// rowDeltaCatalog simulates catalog behavior for RowDelta commit tests.
type rowDeltaCatalog struct {
	metadata table.Metadata
}

func (m *rowDeltaCatalog) LoadTable(ctx context.Context, ident table.Identifier) (*table.Table, error) {
	return nil, nil
}

func (m *rowDeltaCatalog) CommitTable(ctx context.Context, ident table.Identifier, reqs []table.Requirement, updates []table.Update) (table.Metadata, string, error) {
	meta, err := table.UpdateTableMetadata(m.metadata, updates, "")
	if err != nil {
		return nil, "", err
	}

	m.metadata = meta

	return meta, "", nil
}

func newRowDeltaCommitTestTable(t *testing.T) *table.Table {
	t.Helper()

	location := filepath.ToSlash(t.TempDir())

	schema := iceberg.NewSchema(0,
		iceberg.NestedField{ID: 1, Name: "id", Type: iceberg.PrimitiveTypes.Int64, Required: true},
		iceberg.NestedField{ID: 2, Name: "data", Type: iceberg.PrimitiveTypes.String, Required: false},
	)

	meta, err := table.NewMetadata(schema, iceberg.UnpartitionedSpec,
		table.UnsortedSortOrder, location,
		iceberg.Properties{table.PropertyFormatVersion: "2"})
	require.NoError(t, err)

	return table.New(
		table.Identifier{"db", "row_delta_test"},
		meta, location+"/metadata/v1.metadata.json",
		func(ctx context.Context) (iceio.IO, error) {
			return iceio.LocalFS{}, nil
		},
		&rowDeltaCatalog{meta},
	)
}

func TestRowDeltaCommitDataAndDeletes(t *testing.T) {
	tbl := newRowDeltaCommitTestTable(t)

	tx := tbl.NewTransaction()
	rd := tx.NewRowDelta(iceberg.Properties{"custom-prop": "test"})
	rd.AddRows(buildDataFile(t, "s3://bucket/data/insert.parquet"))
	rd.AddDeletes(buildPosDeleteFile(t, "s3://bucket/data/pos-del.parquet"))

	require.NoError(t, rd.Commit(t.Context()))

	result, err := tx.Commit(t.Context())
	require.NoError(t, err)

	snap := result.CurrentSnapshot()
	require.NotNil(t, snap)

	assert.Equal(t, table.OpOverwrite, snap.Summary.Operation)
	assert.Equal(t, "1", snap.Summary.Properties["added-data-files"])
	assert.Equal(t, "1", snap.Summary.Properties["added-delete-files"])
	assert.Equal(t, "10", snap.Summary.Properties["added-records"])
}

func TestRowDeltaCommitDataOnly(t *testing.T) {
	tbl := newRowDeltaCommitTestTable(t)

	tx := tbl.NewTransaction()
	rd := tx.NewRowDelta(nil)
	rd.AddRows(
		buildDataFile(t, "s3://bucket/data/file1.parquet"),
		buildDataFile(t, "s3://bucket/data/file2.parquet"),
	)

	require.NoError(t, rd.Commit(t.Context()))

	result, err := tx.Commit(t.Context())
	require.NoError(t, err)

	snap := result.CurrentSnapshot()
	require.NotNil(t, snap)

	assert.Equal(t, table.OpAppend, snap.Summary.Operation)
	assert.Equal(t, "2", snap.Summary.Properties["added-data-files"])
	assert.Equal(t, "20", snap.Summary.Properties["added-records"])
}

func TestRowDeltaCommitDeletesOnly(t *testing.T) {
	tbl := newRowDeltaCommitTestTable(t)

	tx := tbl.NewTransaction()
	rd := tx.NewRowDelta(nil)
	rd.AddDeletes(buildPosDeleteFile(t, "s3://bucket/data/pos-del.parquet"))

	require.NoError(t, rd.Commit(t.Context()))

	result, err := tx.Commit(t.Context())
	require.NoError(t, err)

	snap := result.CurrentSnapshot()
	require.NotNil(t, snap)

	assert.Equal(t, table.OpDelete, snap.Summary.Operation)
	assert.Equal(t, "1", snap.Summary.Properties["added-position-delete-files"])
	assert.Equal(t, "1", snap.Summary.Properties["added-delete-files"])
}

func TestRowDeltaCommitWithEqualityDeletes(t *testing.T) {
	tbl := newRowDeltaCommitTestTable(t)

	tx := tbl.NewTransaction()
	rd := tx.NewRowDelta(nil)
	rd.AddRows(buildDataFile(t, "s3://bucket/data/insert.parquet"))
	rd.AddDeletes(buildEqDeleteFile(t, "s3://bucket/data/eq-del.parquet", []int{1}))

	require.NoError(t, rd.Commit(t.Context()))

	result, err := tx.Commit(t.Context())
	require.NoError(t, err)

	snap := result.CurrentSnapshot()
	require.NotNil(t, snap)

	assert.Equal(t, table.OpOverwrite, snap.Summary.Operation)
	assert.Equal(t, "1", snap.Summary.Properties["added-data-files"])
	assert.Equal(t, "1", snap.Summary.Properties["added-equality-delete-files"])
}

func TestRowDeltaManifestContents(t *testing.T) {
	tbl := newRowDeltaCommitTestTable(t)

	tx := tbl.NewTransaction()
	rd := tx.NewRowDelta(nil)
	rd.AddRows(buildDataFile(t, "s3://bucket/data/insert.parquet"))
	rd.AddDeletes(buildPosDeleteFile(t, "s3://bucket/data/pos-del.parquet"))

	require.NoError(t, rd.Commit(t.Context()))

	result, err := tx.Commit(t.Context())
	require.NoError(t, err)

	snap := result.CurrentSnapshot()
	require.NotNil(t, snap)

	fs := iceio.LocalFS{}
	manifests, err := snap.Manifests(fs)
	require.NoError(t, err)

	// Should have separate data and delete manifests
	var dataManifests, deleteManifests int
	for _, m := range manifests {
		switch m.ManifestContent() {
		case iceberg.ManifestContentData:
			dataManifests++
		case iceberg.ManifestContentDeletes:
			deleteManifests++
		}
	}

	assert.Equal(t, 1, dataManifests, "expected 1 data manifest")
	assert.Equal(t, 1, deleteManifests, "expected 1 delete manifest")

	// Verify manifest entries have correct content types
	for _, m := range manifests {
		entries, err := m.FetchEntries(fs, true)
		require.NoError(t, err)

		for _, e := range entries {
			if m.ManifestContent() == iceberg.ManifestContentData {
				assert.Equal(t, iceberg.EntryContentData, e.DataFile().ContentType())
			} else {
				assert.Equal(t, iceberg.EntryContentPosDeletes, e.DataFile().ContentType())
			}
		}
	}
}

func TestRowDeltaMultipleCommitsOnSameTransaction(t *testing.T) {
	tbl := newRowDeltaCommitTestTable(t)

	tx := tbl.NewTransaction()

	// First RowDelta: append data
	rd1 := tx.NewRowDelta(nil)
	rd1.AddRows(buildDataFile(t, "s3://bucket/data/batch1.parquet"))
	require.NoError(t, rd1.Commit(t.Context()))

	// Second RowDelta: append + delete
	rd2 := tx.NewRowDelta(nil)
	rd2.AddRows(buildDataFile(t, "s3://bucket/data/batch2.parquet"))
	rd2.AddDeletes(buildPosDeleteFile(t, "s3://bucket/data/del2.parquet"))
	require.NoError(t, rd2.Commit(t.Context()))

	result, err := tx.Commit(t.Context())
	require.NoError(t, err)

	snap := result.CurrentSnapshot()
	require.NotNil(t, snap)

	// The last RowDelta's operation should be reflected
	assert.Equal(t, table.OpOverwrite, snap.Summary.Operation)
	assert.Equal(t, strconv.Itoa(2), snap.Summary.Properties["total-data-files"])
}

// writeParquetFile writes Arrow records to a Parquet file on local disk.
func writeParquetFile(t testing.TB, path string, sc *arrow.Schema, jsonData string) {
	t.Helper()

	rec, _, err := array.RecordFromJSON(memory.DefaultAllocator, sc, strings.NewReader(jsonData))
	require.NoError(t, err)
	defer rec.Release()

	fs := iceio.LocalFS{}
	fw, err := fs.Create(path)
	require.NoError(t, err)

	tbl := array.NewTableFromRecords(sc, []arrow.RecordBatch{rec})
	defer tbl.Release()

	require.NoError(t, pqarrow.WriteTable(tbl, fw, rec.NumRows(),
		parquet.NewWriterProperties(parquet.WithStats(true)),
		pqarrow.DefaultWriterProps()))
}

func TestRowDeltaIntegrationPosDeleteRoundTrip(t *testing.T) {
	location := filepath.ToSlash(t.TempDir())

	// Schema: id (int64), data (string)
	iceSchema := iceberg.NewSchema(0,
		iceberg.NestedField{ID: 1, Name: "id", Type: iceberg.PrimitiveTypes.Int64, Required: true},
		iceberg.NestedField{ID: 2, Name: "data", Type: iceberg.PrimitiveTypes.String, Required: false},
	)

	meta, err := table.NewMetadata(iceSchema, iceberg.UnpartitionedSpec,
		table.UnsortedSortOrder, location,
		iceberg.Properties{table.PropertyFormatVersion: "2"})
	require.NoError(t, err)

	cat := &rowDeltaCatalog{metadata: meta}
	tbl := table.New(
		table.Identifier{"db", "pos_del_roundtrip"},
		meta, location+"/metadata/v1.metadata.json",
		func(ctx context.Context) (iceio.IO, error) {
			return iceio.LocalFS{}, nil
		},
		cat,
	)

	// Step 1: Append 5 rows via normal append
	arrowSc, err := table.SchemaToArrowSchema(iceSchema, nil, false, false)
	require.NoError(t, err)

	dataPath := location + "/data/data-001.parquet"
	writeParquetFile(t, dataPath, arrowSc, `[
		{"id": 1, "data": "alpha"},
		{"id": 2, "data": "beta"},
		{"id": 3, "data": "gamma"},
		{"id": 4, "data": "delta"},
		{"id": 5, "data": "epsilon"}
	]`)

	tx := tbl.NewTransaction()
	err = tx.AddFiles(t.Context(), []string{dataPath}, nil, false)
	require.NoError(t, err)

	tbl, err = tx.Commit(t.Context())
	require.NoError(t, err)

	// Verify: 5 rows scannable
	assertRowCount(t, tbl, 5)

	// Step 2: Commit a position delete via RowDelta that removes rows 1 and 3
	// (0-indexed: positions 1 and 3 → "beta" and "delta")
	posDelArrowSc := table.PositionalDeleteArrowSchema
	posDelPath := location + "/data/pos-del-001.parquet"
	writeParquetFile(t, posDelPath, posDelArrowSc, `[
		{"file_path": "`+dataPath+`", "pos": 1},
		{"file_path": "`+dataPath+`", "pos": 3}
	]`)

	posDelBuilder, err := iceberg.NewDataFileBuilder(
		*iceberg.UnpartitionedSpec, iceberg.EntryContentPosDeletes,
		posDelPath, iceberg.ParquetFile, nil, nil, nil, 2, 256)
	require.NoError(t, err)
	posDelFile := posDelBuilder.Build()

	tx2 := tbl.NewTransaction()
	rd := tx2.NewRowDelta(nil)
	rd.AddDeletes(posDelFile)
	require.NoError(t, rd.Commit(t.Context()))

	tbl, err = tx2.Commit(t.Context())
	require.NoError(t, err)

	// Step 3: Scan and verify rows 1 and 3 (beta, delta) are deleted
	// Remaining: alpha (0), gamma (2), epsilon (4)
	assertRowCount(t, tbl, 3)

	// Verify the actual values
	_, itr, err := tbl.Scan(table.WithSelectedFields("id", "data")).ToArrowRecords(t.Context())
	require.NoError(t, err)

	var ids []int64
	for rec, err := range itr {
		require.NoError(t, err)
		col := rec.Column(0).(*array.Int64)
		for i := 0; i < col.Len(); i++ {
			ids = append(ids, col.Value(i))
		}
		rec.Release()
	}

	assert.Equal(t, []int64{1, 3, 5}, ids, "expected rows at positions 0,2,4 (beta/delta deleted)")
}

func assertRowCount(t *testing.T, tbl *table.Table, expected int64) {
	t.Helper()

	_, itr, err := tbl.Scan().ToArrowRecords(t.Context())
	require.NoError(t, err)

	var total int64
	next, stop := iter.Pull2(itr)
	defer stop()

	for {
		rec, err, valid := next()
		if !valid {
			break
		}

		require.NoError(t, err)
		total += rec.NumRows()
		rec.Release()
	}

	assert.Equal(t, expected, total, "unexpected row count")
}
