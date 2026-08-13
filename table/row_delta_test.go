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
	"github.com/DataDog/iceberg-go"
	iceio "github.com/DataDog/iceberg-go/io"
	"github.com/DataDog/iceberg-go/table"
	"github.com/DataDog/iceberg-go/table/dv"
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

func newRowDeltaFloatingPointTestTable(t *testing.T) *table.Table {
	t.Helper()

	schema := iceberg.NewSchema(0,
		iceberg.NestedField{ID: 1, Name: "id", Type: iceberg.PrimitiveTypes.Int64, Required: true},
		iceberg.NestedField{ID: 2, Name: "score", Type: iceberg.PrimitiveTypes.Float32, Required: false},
		iceberg.NestedField{ID: 3, Name: "ratio", Type: iceberg.PrimitiveTypes.Float64, Required: false},
	)

	meta, err := table.NewMetadata(schema, iceberg.UnpartitionedSpec,
		table.UnsortedSortOrder, "s3://bucket/test",
		iceberg.Properties{table.PropertyFormatVersion: "2"})
	require.NoError(t, err)

	return table.New(
		table.Identifier{"db", "floating_key_test"},
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

func TestRowDeltaRejectsEqDeleteWithFloatAndDoubleFieldIDs(t *testing.T) {
	tbl := newRowDeltaFloatingPointTestTable(t)

	tests := []struct {
		name      string
		fieldID   int
		fieldName string
		fieldType string
	}{
		{name: "float", fieldID: 2, fieldName: "score", fieldType: "float"},
		{name: "double", fieldID: 3, fieldName: "ratio", fieldType: "double"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			rd := tbl.NewTransaction().NewRowDelta(nil)
			rd.AddDeletes(buildEqDeleteFile(t, "s3://bucket/data/eq-del.parquet", []int{tt.fieldID}))

			err := rd.Commit(t.Context())
			require.ErrorIs(t, err, iceberg.ErrInvalidSchema)
			assert.ErrorContains(t, err, "eq-del.parquet")
			assert.ErrorContains(t, err, tt.fieldName)
			assert.ErrorContains(t, err, tt.fieldType)
		})
	}
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

	return newRowDeltaCommitTestTableVersion(t, 2)
}

func newRowDeltaCommitTestTableVersion(t *testing.T, formatVersion int) *table.Table {
	t.Helper()

	location := filepath.ToSlash(t.TempDir())

	schema := iceberg.NewSchema(0,
		iceberg.NestedField{ID: 1, Name: "id", Type: iceberg.PrimitiveTypes.Int64, Required: true},
		iceberg.NestedField{ID: 2, Name: "data", Type: iceberg.PrimitiveTypes.String, Required: false},
	)

	meta, err := table.NewMetadata(schema, iceberg.UnpartitionedSpec,
		table.UnsortedSortOrder, location,
		iceberg.Properties{table.PropertyFormatVersion: formatVersionStr(formatVersion)})
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

func TestNewRowDeltaCopiesSnapshotProperties(t *testing.T) {
	tbl := newRowDeltaCommitTestTable(t)

	snapshotProps := iceberg.Properties{"custom-prop": "initial"}
	tx := tbl.NewTransaction()
	rd := tx.NewRowDelta(snapshotProps)
	snapshotProps["custom-prop"] = "changed"
	snapshotProps["new-prop"] = "added"

	rd.AddRows(buildDataFile(t, "s3://bucket/data/insert.parquet"))
	require.NoError(t, rd.Commit(t.Context()))

	result, err := tx.Commit(t.Context())
	require.NoError(t, err)
	snap := result.CurrentSnapshot()
	require.NotNil(t, snap)

	assert.Equal(t, "initial", snap.Summary.Properties["custom-prop"])
	assert.NotContains(t, snap.Summary.Properties, "new-prop")
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
		for e, err := range m.Entries(fs, true) {
			require.NoError(t, err)
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

	writeParquetFileWithProperties(t, path, sc, jsonData, 0, nil)
}

func writeParquetFileWithProperties(
	t testing.TB,
	path string,
	sc *arrow.Schema,
	jsonData string,
	rowGroupSize int64,
	writerProps *parquet.WriterProperties,
) {
	t.Helper()

	rec, _, err := array.RecordFromJSON(memory.DefaultAllocator, sc, strings.NewReader(jsonData))
	require.NoError(t, err)
	defer rec.Release()

	fs := iceio.LocalFS{}
	fw, err := fs.Create(path)
	require.NoError(t, err)
	defer fw.Close()

	tbl := array.NewTableFromRecords(sc, []arrow.RecordBatch{rec})
	defer tbl.Release()

	if rowGroupSize <= 0 {
		rowGroupSize = rec.NumRows()
	}
	if writerProps == nil {
		writerProps = parquet.NewWriterProperties(parquet.WithStats(true))
	}

	require.NoError(t, pqarrow.WriteTable(tbl, fw, rowGroupSize, writerProps, pqarrow.DefaultWriterProps()))
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
	writeParquetFileWithProperties(t, posDelPath, posDelArrowSc, `[
		{"file_path": "`+dataPath+`", "pos": 1},
		{"file_path": "`+dataPath+`", "pos": 3}
	]`, 1, parquet.NewWriterProperties(
		parquet.WithStats(true),
		parquet.WithDictionaryDefault(false),
	))

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
	var data []string
	for rec, err := range itr {
		require.NoError(t, err)
		idCol := rec.Column(0).(*array.Int64)
		dataCol := rec.Column(1).(*array.String)
		for i := 0; i < idCol.Len(); i++ {
			ids = append(ids, idCol.Value(i))
			data = append(data, dataCol.Value(i))
		}
		rec.Release()
	}

	assert.Equal(t, []int64{1, 3, 5}, ids, "expected rows at positions 0,2,4 (beta/delta deleted)")
	assert.Equal(t, []string{"alpha", "gamma", "epsilon"}, data)
}

// buildDVFile builds a deletion-vector manifest entry (puffin pos-delete
// with a referenced data file) without writing any bytes; for tests that
// only exercise metadata validation.
func buildDVFile(t *testing.T, path, refDataFile string) iceberg.DataFile {
	t.Helper()

	b, err := iceberg.NewDataFileBuilder(
		*iceberg.UnpartitionedSpec, iceberg.EntryContentPosDeletes,
		path, iceberg.PuffinFile, nil, nil, nil, 2, 128)
	require.NoError(t, err)

	return b.ReferencedDataFile(refDataFile).
		ContentOffset(4).
		ContentSizeInBytes(64).
		Build()
}

// writeDV writes a single-blob deletion vector puffin file for dataPath
// and returns its manifest entry.
func writeDV(t *testing.T, location, name, dataPath string, positions []int64) iceberg.DataFile {
	t.Helper()

	w := dv.NewDVWriter(iceio.LocalFS{}, func(int32) *iceberg.PartitionSpec {
		return iceberg.UnpartitionedSpec
	})
	require.NoError(t, w.Add(dataPath, positions, 0, nil))
	files, err := w.Flush(t.Context(), location+"/data/"+name)
	require.NoError(t, err)
	require.Len(t, files, 1)

	return files[0]
}

// snapshotDeleteEntryFiles walks snap's delete manifests and returns
// the DataFiles of live (non-DELETED) and removed (DELETED) entries.
func snapshotDeleteEntryFiles(t *testing.T, snap *table.Snapshot, fs iceio.IO) (live, removed []iceberg.DataFile) {
	t.Helper()

	manifests, err := snap.Manifests(fs)
	require.NoError(t, err)

	for _, m := range manifests {
		if m.ManifestContent() != iceberg.ManifestContentDeletes {
			continue
		}
		for e, err := range m.Entries(fs, false) {
			require.NoError(t, err)
			if e.Status() == iceberg.EntryStatusDELETED {
				removed = append(removed, e.DataFile())
			} else {
				live = append(live, e.DataFile())
			}
		}
	}

	return live, removed
}

// snapshotDVEntries walks snap's delete manifests and returns the paths
// of live (non-DELETED) and removed (DELETED) entries.
func snapshotDVEntries(t *testing.T, snap *table.Snapshot, fs iceio.IO) (live, removed []string) {
	t.Helper()

	liveFiles, removedFiles := snapshotDeleteEntryFiles(t, snap, fs)
	for _, df := range liveFiles {
		live = append(live, df.FilePath())
	}
	for _, df := range removedFiles {
		removed = append(removed, df.FilePath())
	}

	return live, removed
}

// refOf returns df's referenced data file, or "" when unset.
func refOf(df iceberg.DataFile) string {
	if r := df.ReferencedDataFile(); r != nil {
		return *r
	}

	return ""
}

// pathRefPairs maps entries to [path, referenced data file] pairs for
// order-insensitive assertions on shared-Puffin manifests.
func pathRefPairs(files []iceberg.DataFile) [][2]string {
	pairs := make([][2]string, 0, len(files))
	for _, df := range files {
		pairs = append(pairs, [2]string{df.FilePath(), refOf(df)})
	}

	return pairs
}

// newTableWithLiveDV builds a v3 table containing one data file with two
// rows and a live deletion vector hiding position 0, committed across
// two snapshots. Returns the table, its location, the data file path,
// and the live DV's manifest entry.
func newTableWithLiveDV(t *testing.T) (*table.Table, string, string, iceberg.DataFile) {
	t.Helper()

	tbl := newRowDeltaCommitTestTableVersion(t, 3)
	location := tbl.Location()

	arrowSc, err := table.SchemaToArrowSchema(tbl.Schema(), nil, false, false)
	require.NoError(t, err)

	dataPath := location + "/data/data-001.parquet"
	writeParquetFile(t, dataPath, arrowSc, `[
		{"id": 1, "data": "alpha"},
		{"id": 2, "data": "beta"}
	]`)

	tx := tbl.NewTransaction()
	require.NoError(t, tx.AddFiles(t.Context(), []string{dataPath}, nil, false))
	tbl, err = tx.Commit(t.Context())
	require.NoError(t, err)

	dv1 := writeDV(t, location, "dv-001.puffin", dataPath, []int64{0})
	tx2 := tbl.NewTransaction()
	require.NoError(t, tx2.NewRowDelta(nil).AddDeletes(dv1).Commit(t.Context()))
	tbl, err = tx2.Commit(t.Context())
	require.NoError(t, err)

	return tbl, location, dataPath, dv1
}

// newTableWithSharedPuffinDVs builds a v3 table with two 2-row data
// files whose deletion vectors (each hiding position 0) share a single
// multi-blob Puffin file: two live manifest entries with the same
// FilePath and distinct referenced data files. Returns the table, its
// location, the two data paths, and the two DV entries.
func newTableWithSharedPuffinDVs(t *testing.T) (*table.Table, string, string, string, iceberg.DataFile, iceberg.DataFile) {
	t.Helper()

	tbl := newRowDeltaCommitTestTableVersion(t, 3)
	location := tbl.Location()

	arrowSc, err := table.SchemaToArrowSchema(tbl.Schema(), nil, false, false)
	require.NoError(t, err)

	pathA := location + "/data/data-a.parquet"
	pathB := location + "/data/data-b.parquet"
	writeParquetFile(t, pathA, arrowSc, `[{"id": 1, "data": "alpha"}, {"id": 2, "data": "beta"}]`)
	writeParquetFile(t, pathB, arrowSc, `[{"id": 3, "data": "gamma"}, {"id": 4, "data": "delta"}]`)

	tx := tbl.NewTransaction()
	require.NoError(t, tx.AddFiles(t.Context(), []string{pathA, pathB}, nil, false))
	tbl, err = tx.Commit(t.Context())
	require.NoError(t, err)

	w := dv.NewDVWriter(iceio.LocalFS{}, func(int32) *iceberg.PartitionSpec {
		return iceberg.UnpartitionedSpec
	})
	require.NoError(t, w.Add(pathA, []int64{0}, 0, nil))
	require.NoError(t, w.Add(pathB, []int64{0}, 0, nil))
	files, err := w.Flush(t.Context(), location+"/data/dv-shared.puffin")
	require.NoError(t, err)
	require.Len(t, files, 2)

	var dvA, dvB iceberg.DataFile
	for _, f := range files {
		switch refOf(f) {
		case pathA:
			dvA = f
		case pathB:
			dvB = f
		}
	}
	require.NotNil(t, dvA)
	require.NotNil(t, dvB)
	require.Equal(t, dvA.FilePath(), dvB.FilePath(), "both DVs must share one Puffin path")

	tx2 := tbl.NewTransaction()
	require.NoError(t, tx2.NewRowDelta(nil).AddDeletes(dvA, dvB).Commit(t.Context()))
	tbl, err = tx2.Commit(t.Context())
	require.NoError(t, err)

	return tbl, location, pathA, pathB, dvA, dvB
}

// scanSingleDVTask plans a scan of tbl and returns its single task's
// deletion-vector file paths.
func scanSingleDVTask(t *testing.T, tbl *table.Table) []string {
	t.Helper()

	tasks, err := tbl.Scan().PlanFiles(t.Context())
	require.NoError(t, err)
	require.Len(t, tasks, 1)

	dvPaths := make([]string, 0, len(tasks[0].DeletionVectorFiles))
	for _, df := range tasks[0].DeletionVectorFiles {
		dvPaths = append(dvPaths, df.FilePath())
	}

	return dvPaths
}

// Why: the v3 spec allows one live DV per data file and requires a writer
// replacing a DV to add the superseding DV and remove the old one in the
// SAME snapshot; two-snapshot workarounds briefly resurrect rows.
// Condition: a v3 table with 5 rows and a live DV hiding position 1; a
// RowDelta adds a superseding DV for positions {1,3} and removes the old
// DV, then the transaction commits.
// Assertion: exactly one snapshot is produced with operation delete, the
// delete manifests carry one live DV (the replacement) and the superseded
// DV with status DELETED, the scanner plans exactly the replacement DV,
// whose contents hide positions 1 and 3, and the materialized scan
// returns only the three surviving rows.
func TestRowDeltaDVSupersessionSingleSnapshot(t *testing.T) {
	tbl := newRowDeltaCommitTestTableVersion(t, 3)
	location := tbl.Location()

	arrowSc, err := table.SchemaToArrowSchema(tbl.Schema(), nil, false, false)
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
	require.NoError(t, tx.AddFiles(t.Context(), []string{dataPath}, nil, false))
	tbl, err = tx.Commit(t.Context())
	require.NoError(t, err)
	assertRowCount(t, tbl, 5)

	// First DV: hide position 1 ("beta").
	dv1 := writeDV(t, location, "dv-001.puffin", dataPath, []int64{1})
	tx2 := tbl.NewTransaction()
	require.NoError(t, tx2.NewRowDelta(nil).AddDeletes(dv1).Commit(t.Context()))
	tbl, err = tx2.Commit(t.Context())
	require.NoError(t, err)
	assert.Equal(t, []string{dv1.FilePath()}, scanSingleDVTask(t, tbl))
	assertRowCount(t, tbl, 4)

	snapsBefore := len(tbl.Metadata().Snapshots())

	// Superseding DV: previous position 1 plus new position 3 ("delta").
	dv2 := writeDV(t, location, "dv-002.puffin", dataPath, []int64{1, 3})
	tx3 := tbl.NewTransaction()
	require.NoError(t, tx3.NewRowDelta(nil).AddDeletes(dv2).RemoveDeletes(dv1).Commit(t.Context()))
	tbl, err = tx3.Commit(t.Context())
	require.NoError(t, err)

	require.Len(t, tbl.Metadata().Snapshots(), snapsBefore+1,
		"supersession must produce a single snapshot")

	snap := tbl.CurrentSnapshot()
	require.NotNil(t, snap)
	assert.Equal(t, table.OpDelete, snap.Summary.Operation)
	assert.Equal(t, "1", snap.Summary.Properties["added-delete-files"])
	assert.Equal(t, "1", snap.Summary.Properties["removed-delete-files"])

	fs := iceio.LocalFS{}
	liveDVs, removedDVs := snapshotDVEntries(t, snap, fs)
	assert.Equal(t, []string{dv2.FilePath()}, liveDVs,
		"the data file must carry exactly one live DV: the replacement")
	assert.Equal(t, []string{dv1.FilePath()}, removedDVs,
		"the superseded DV must be recorded as DELETED in the same snapshot")

	// Row visibility: the scanner plans exactly the replacement DV, its
	// bitmap hides positions 1 and 3, and the materialized scan returns
	// the three surviving rows — ids 1/3/5.
	assert.Equal(t, []string{dv2.FilePath()}, scanSingleDVTask(t, tbl))

	bm, err := dv.ReadDV(fs, dv2)
	require.NoError(t, err)
	var hidden []uint64
	for pos := range bm.Positions() {
		hidden = append(hidden, pos)
	}
	assert.Equal(t, []uint64{1, 3}, hidden)

	assertRowCount(t, tbl, 3)
}

// Why: removing a DV without publishing a superseding DV for the same
// data file would resurrect the rows the removed DV was hiding.
// Condition: a RowDelta removes a live DV while adding either no DV at
// all or a DV that references a different data file.
// Assertion: Commit fails with a replacement-required error before any
// snapshot work happens.
func TestRowDeltaRemoveDeletesRequiresReplacement(t *testing.T) {
	t.Run("no added deletes", func(t *testing.T) {
		tbl, _, _, dv1 := newTableWithLiveDV(t)
		rd := tbl.NewTransaction().NewRowDelta(nil).
			AddRows(buildDataFile(t, "s3://bucket/data/new.parquet")).
			RemoveDeletes(dv1)

		err := rd.Commit(t.Context())
		require.Error(t, err)
		assert.Contains(t, err.Error(), "no replacement deletion vector")
	})

	t.Run("replacement references a different data file", func(t *testing.T) {
		tbl, location, _, dv1 := newTableWithLiveDV(t)
		rd := tbl.NewTransaction().NewRowDelta(nil).
			AddDeletes(buildDVFile(t, location+"/data/dv-002.puffin", "s3://bucket/data/other.parquet")).
			RemoveDeletes(dv1)

		err := rd.Commit(t.Context())
		require.Error(t, err)
		assert.Contains(t, err.Error(), "no replacement deletion vector")
	})

	t.Run("removal-only delta", func(t *testing.T) {
		tbl, _, _, dv1 := newTableWithLiveDV(t)
		rd := tbl.NewTransaction().NewRowDelta(nil).RemoveDeletes(dv1)

		err := rd.Commit(t.Context())
		require.Error(t, err)
		assert.Contains(t, err.Error(), "no replacement deletion vector",
			"a removal-only delta must fail with the replacement-required error, not the empty-delta error")
	})
}

// Why: committing a replacement DV while the data file's current live
// DV stays live would put two live DVs on one data file, which the v3
// spec forbids and scan planning rejects; the same holds for two added
// replacements targeting one data file in a single delta.
// Condition: a supersession delta that (a) adds a replacement DV for a
// data file whose live DV is not in RemoveDeletes, or (b) adds two
// replacement DVs referencing the same data file.
// Assertion: Commit fails identifying the surviving live DV (a) or the
// duplicated reference (b).
func TestRowDeltaRemoveDeletesRejectsSurvivingLiveDV(t *testing.T) {
	t.Run("replacement added while live DV not removed", func(t *testing.T) {
		tbl, location, pathA, pathB, dvA, _ := newTableWithSharedPuffinDVs(t)

		// Supersede A's DV, but also add a replacement for B without
		// removing B's live DV.
		dvA2 := writeDV(t, location, "dv-a2.puffin", pathA, []int64{0, 1})
		dvB2 := writeDV(t, location, "dv-b2.puffin", pathB, []int64{0, 1})
		rd := tbl.NewTransaction().NewRowDelta(nil).
			AddDeletes(dvA2, dvB2).
			RemoveDeletes(dvA)

		err := rd.Commit(t.Context())
		require.Error(t, err)
		assert.Contains(t, err.Error(), "is not removed by this row delta")
		assert.Contains(t, err.Error(), pathB)
	})

	t.Run("two replacements for one data file", func(t *testing.T) {
		tbl, location, dataPath, dv1 := newTableWithLiveDV(t)

		rep1 := writeDV(t, location, "dv-002.puffin", dataPath, []int64{0, 1})
		rep2 := buildDVFile(t, location+"/data/dv-003.puffin", dataPath)
		rd := tbl.NewTransaction().NewRowDelta(nil).
			AddDeletes(rep1, rep2).
			RemoveDeletes(dv1)

		err := rd.Commit(t.Context())
		require.Error(t, err)
		assert.Contains(t, err.Error(), "multiple added deletion vectors reference data file "+dataPath)
	})
}

// Why: a table that already violates the one-live-DV-per-data-file
// invariant must fail a supersession commit loudly — the producer keys
// DV removals by referenced data file, so proceeding would either
// tombstone several entries while counting one removal (same path) or
// leave the sibling duplicate live next to the replacement (different
// paths).
// Condition: v3 tables corrupted through the unvalidated plain
// AddDeletes path so one data file carries two live DV entries, first
// as duplicate entries at one Puffin path, then at two distinct paths;
// a RowDelta then supersedes one of them.
// Assertion: Commit fails naming the duplicate entries rather than
// committing.
func TestRowDeltaRemoveDeletesCorruptDuplicateLiveDVs(t *testing.T) {
	t.Run("duplicate entries at one path", func(t *testing.T) {
		tbl, location, dataPath, dv1 := newTableWithLiveDV(t)

		// Corrupt the table: a second snapshot re-adds the same DV
		// entry (same path, same referenced data file).
		tx := tbl.NewTransaction()
		require.NoError(t, tx.NewRowDelta(nil).AddDeletes(dv1).Commit(t.Context()))
		tbl, err := tx.Commit(t.Context())
		require.NoError(t, err)

		replacement := writeDV(t, location, "dv-002.puffin", dataPath, []int64{0, 1})
		rd := tbl.NewTransaction().NewRowDelta(nil).
			AddDeletes(replacement).
			RemoveDeletes(dv1)

		err = rd.Commit(t.Context())
		require.Error(t, err)
		assert.Contains(t, err.Error(), "duplicate live deletion vectors")
	})

	t.Run("duplicate entries at two paths", func(t *testing.T) {
		tbl, location, dataPath, dv1 := newTableWithLiveDV(t)

		// Corrupt the table: a second live DV for the same data file
		// at a different Puffin path.
		dv1b := writeDV(t, location, "dv-001b.puffin", dataPath, []int64{0})
		tx := tbl.NewTransaction()
		require.NoError(t, tx.NewRowDelta(nil).AddDeletes(dv1b).Commit(t.Context()))
		tbl, err := tx.Commit(t.Context())
		require.NoError(t, err)

		replacement := writeDV(t, location, "dv-002.puffin", dataPath, []int64{0, 1})
		rd := tbl.NewTransaction().NewRowDelta(nil).
			AddDeletes(replacement).
			RemoveDeletes(dv1)

		err = rd.Commit(t.Context())
		require.Error(t, err)
		assert.Contains(t, err.Error(), "is not removed by this row delta",
			"the sibling duplicate at the other path must be flagged as surviving")
		assert.Contains(t, err.Error(), dv1b.FilePath())
	})
}

// Why: RemoveDeletes resolves removals against the snapshot the writer
// built on, so a commit carrying them must fail on a CAS conflict
// instead of refresh-and-replaying — a replay would inherit the peer's
// replacement DV from the fresh base while the stale removal replays
// as a no-op, stranding two live DVs on one data file.
// Condition: two writers stage supersessions of the same live DV from
// the same view via RowDelta.RemoveDeletes; the peer commits first;
// the stale writer commits with retries enabled.
// Assertion: the stale commit fails wrapping ErrCommitFailed after
// exactly one CommitTable attempt, and the committed table carries
// exactly one live DV — the peer's.
func TestRowDeltaRemoveDeletesFailsInsteadOfReplaying(t *testing.T) {
	ctx := t.Context()
	tbl, cat := newNoReplayV3Table(t)
	tbl = appendTenRows(t, tbl)
	location := tbl.Location()

	tasks, err := tbl.Scan().PlanFiles(ctx)
	require.NoError(t, err)
	require.Len(t, tasks, 1)
	dataPath := tasks[0].File.FilePath()

	// Baseline DV hides position 0.
	dv1 := writeDV(t, location, "dv-001.puffin", dataPath, []int64{0})
	tx := tbl.NewTransaction()
	require.NoError(t, tx.NewRowDelta(nil).AddDeletes(dv1).Commit(ctx))
	tbl, err = tx.Commit(ctx)
	require.NoError(t, err)

	// Stage the stale writer's supersession first, from the current view.
	dvStale := writeDV(t, location, "dv-stale.puffin", dataPath, []int64{0, 2})
	staleTxn := tbl.NewTransaction()
	require.NoError(t, staleTxn.NewRowDelta(nil).AddDeletes(dvStale).RemoveDeletes(dv1).Commit(ctx))

	// Peer supersedes dv1 from the same view and wins the race.
	dvPeer := writeDV(t, location, "dv-peer.puffin", dataPath, []int64{0, 1})
	peerTxn := tbl.NewTransaction()
	require.NoError(t, peerTxn.NewRowDelta(nil).AddDeletes(dvPeer).RemoveDeletes(dv1).Commit(ctx))
	_, err = peerTxn.Commit(ctx)
	require.NoError(t, err)

	attemptsBefore := cat.attempts.Load()
	_, err = staleTxn.Commit(ctx)
	require.Error(t, err)
	assert.ErrorIs(t, err, table.ErrCommitFailed)
	assert.Equal(t, attemptsBefore+1, cat.attempts.Load(),
		"a RowDelta carrying DV removals must fail on the first CAS conflict, not refresh-and-replay")

	// The table is uncorrupted: exactly one live DV — the peer's.
	committed, err := cat.LoadTable(ctx, table.Identifier{"db", "no_replay_removals"})
	require.NoError(t, err)
	assert.Equal(t, []string{dvPeer.FilePath() + " -> " + dataPath},
		deleteEntriesReferencing(t, committed, map[string]struct{}{dataPath: {}}),
		"the data file must carry exactly one live DV: the peer's")
}

// Why: deletion vectors exist only in format v3; a v2 table cannot
// carry the entries RemoveDeletes targets, and the error should say so
// instead of failing resolution with a confusing lookup error.
// Condition: a v2 table commits a RowDelta with a removal.
// Assertion: Commit fails with a format-version error.
func TestRowDeltaRemoveDeletesRequiresV3(t *testing.T) {
	tbl := newRowDeltaCommitTestTableVersion(t, 2)
	rd := tbl.NewTransaction().NewRowDelta(nil).
		AddDeletes(buildPosDeleteFile(t, "s3://bucket/data/pos-del.parquet")).
		RemoveDeletes(buildDVFile(t, "s3://bucket/data/dv-001.puffin", "s3://bucket/data/data-001.parquet"))

	err := rd.Commit(t.Context())
	require.Error(t, err)
	assert.Contains(t, err.Error(), "format version >= 3")
}

// Why: only DV supersession is safe to express through a RowDelta;
// expunging plain position or equality delete files needs the rewrite
// path's data-file validation. The check must classify the LIVE entry,
// not the caller's copy, so a stale copy cannot smuggle a non-DV
// removal through.
// Condition: the table's live delete entry is a plain Parquet
// pos-delete file; a RowDelta removes it (caller copy claims it is a
// DV) while adding a valid DV.
// Assertion: Commit fails identifying the live entry as not a DV.
func TestRowDeltaRemoveDeletesRejectsNonDV(t *testing.T) {
	tbl := newRowDeltaCommitTestTableVersion(t, 3)
	location := tbl.Location()

	posDelPath := location + "/data/pos-del-001.parquet"
	tx := tbl.NewTransaction()
	require.NoError(t, tx.NewRowDelta(nil).
		AddDeletes(buildPosDeleteFile(t, posDelPath)).Commit(t.Context()))
	tbl, err := tx.Commit(t.Context())
	require.NoError(t, err)

	// The caller's copy carries DV-shaped metadata for the live plain
	// pos-delete's path; classification must follow the live entry.
	disguised := buildDVFile(t, posDelPath, "s3://bucket/data/data-001.parquet")
	rd := tbl.NewTransaction().NewRowDelta(nil).
		AddDeletes(buildDVFile(t, location+"/data/dv-001.puffin", "s3://bucket/data/data-001.parquet")).
		RemoveDeletes(disguised)

	err = rd.Commit(t.Context())
	require.Error(t, err)
	assert.Contains(t, err.Error(), "only deletion vectors can be removed")
}

// Why: replacement pairing must be validated against the referenced
// data file recorded by the LIVE manifest entry; trusting the caller's
// copy lets a stale or forged DataFile with a real path but a wrong
// referenced_data_file remove an unrelated live DV.
// Condition: removed files whose paths resolve to live DVs but whose
// caller-side referenced_data_file metadata is wrong.
// Assertion: a stale copy still pairs correctly per the live entry and
// the commit succeeds; a forged reference cannot satisfy pairing with a
// replacement for a different data file, so the unrelated DV survives.
func TestRowDeltaRemoveDeletesStaleReference(t *testing.T) {
	t.Run("stale metadata pairs against the live entry", func(t *testing.T) {
		tbl, location, dataPath, dv1 := newTableWithLiveDV(t)

		// Same path as the live DV, wrong referenced data file.
		stale := buildDVFile(t, dv1.FilePath(), "s3://bucket/data/bogus.parquet")
		replacement := writeDV(t, location, "dv-002.puffin", dataPath, []int64{0, 1})

		tx := tbl.NewTransaction()
		require.NoError(t, tx.NewRowDelta(nil).
			AddDeletes(replacement).RemoveDeletes(stale).Commit(t.Context()))
		tbl, err := tx.Commit(t.Context())
		require.NoError(t, err)

		live, removed := snapshotDVEntries(t, tbl.CurrentSnapshot(), iceio.LocalFS{})
		assert.Equal(t, []string{replacement.FilePath()}, live,
			"the live DV pairs with the replacement per the live entry's reference")
		assert.Equal(t, []string{dv1.FilePath()}, removed)
	})

	t.Run("forged reference cannot remove an unrelated DV", func(t *testing.T) {
		tbl := newRowDeltaCommitTestTableVersion(t, 3)
		location := tbl.Location()

		arrowSc, err := table.SchemaToArrowSchema(tbl.Schema(), nil, false, false)
		require.NoError(t, err)

		pathA := location + "/data/data-a.parquet"
		pathB := location + "/data/data-b.parquet"
		writeParquetFile(t, pathA, arrowSc, `[{"id": 1, "data": "alpha"}, {"id": 2, "data": "beta"}]`)
		writeParquetFile(t, pathB, arrowSc, `[{"id": 3, "data": "gamma"}, {"id": 4, "data": "delta"}]`)

		tx := tbl.NewTransaction()
		require.NoError(t, tx.AddFiles(t.Context(), []string{pathA, pathB}, nil, false))
		tbl, err = tx.Commit(t.Context())
		require.NoError(t, err)

		dvA := writeDV(t, location, "dv-a.puffin", pathA, []int64{0})
		dvB := writeDV(t, location, "dv-b.puffin", pathB, []int64{0})
		tx2 := tbl.NewTransaction()
		require.NoError(t, tx2.NewRowDelta(nil).AddDeletes(dvA, dvB).Commit(t.Context()))
		tbl, err = tx2.Commit(t.Context())
		require.NoError(t, err)

		// Forged copy: dvB's real path, but claims to reference data
		// file A — for which the delta does add a replacement. Pairing
		// against the caller's copy would remove dvB and strand data
		// file B's deletes.
		forged := buildDVFile(t, dvB.FilePath(), pathA)
		replacementA := writeDV(t, location, "dv-a2.puffin", pathA, []int64{0, 1})

		rd := tbl.NewTransaction().NewRowDelta(nil).
			AddDeletes(replacementA).RemoveDeletes(forged)

		err = rd.Commit(t.Context())
		require.Error(t, err)
		assert.Contains(t, err.Error(), "no replacement deletion vector for data file "+pathB,
			"pairing must use the live entry's referenced data file")

		// The unrelated DV is untouched: still live on the branch head.
		live, removed := snapshotDVEntries(t, tbl.CurrentSnapshot(), iceio.LocalFS{})
		assert.ElementsMatch(t, []string{dvA.FilePath(), dvB.FilePath()}, live)
		assert.Empty(t, removed)
	})
}

// Why: a multi-blob Puffin file legally carries DVs for several data
// files under one path, one manifest entry each; removal identity must
// be (file path, referenced data file), or superseding one blob would
// silently drop its siblings' entries and resurrect their deleted rows.
// Condition: a v3 table where data files A and B carry DVs in one
// shared Puffin file, then RowDeltas that supersede one blob, both
// blobs, or misidentify a blob.
// Assertion: superseding A's DV alone produces a single snapshot where
// A's old entry is DELETED while B's entry at the SAME path stays live
// and B's rows stay hidden; both blobs can be superseded in one delta;
// removals that cannot identify exactly one live entry fail.
func TestRowDeltaRemoveDeletesSharedPuffin(t *testing.T) {
	fs := iceio.LocalFS{}

	t.Run("superseding one blob leaves siblings live", func(t *testing.T) {
		tbl, location, pathA, pathB, dvA, dvB := newTableWithSharedPuffinDVs(t)
		snapsBefore := len(tbl.Metadata().Snapshots())

		dvA2 := writeDV(t, location, "dv-a2.puffin", pathA, []int64{0, 1})
		tx := tbl.NewTransaction()
		require.NoError(t, tx.NewRowDelta(nil).AddDeletes(dvA2).RemoveDeletes(dvA).Commit(t.Context()))
		tbl, err := tx.Commit(t.Context())
		require.NoError(t, err)
		require.Len(t, tbl.Metadata().Snapshots(), snapsBefore+1,
			"supersession must produce a single snapshot")

		live, removed := snapshotDeleteEntryFiles(t, tbl.CurrentSnapshot(), fs)
		assert.ElementsMatch(t, [][2]string{
			{dvA2.FilePath(), pathA},
			{dvB.FilePath(), pathB},
		}, pathRefPairs(live),
			"B's entry at the shared Puffin path must survive A's supersession")
		assert.Equal(t, [][2]string{{dvA.FilePath(), pathA}}, pathRefPairs(removed),
			"only A's entry may be recorded as DELETED")

		// B's rows stay hidden: the scanner still plans B's blob of the
		// shared Puffin, and its bitmap still hides position 0.
		tasks, err := tbl.Scan().PlanFiles(t.Context())
		require.NoError(t, err)
		require.Len(t, tasks, 2)
		for _, task := range tasks {
			require.Len(t, task.DeletionVectorFiles, 1)
			planned := task.DeletionVectorFiles[0]
			switch task.File.FilePath() {
			case pathA:
				assert.Equal(t, dvA2.FilePath(), planned.FilePath())
			case pathB:
				assert.Equal(t, dvB.FilePath(), planned.FilePath())
				assert.Equal(t, pathB, refOf(planned))
				bm, err := dv.ReadDV(fs, planned)
				require.NoError(t, err)
				assert.True(t, bm.Contains(0), "B's deleted row must stay hidden")
				assert.EqualValues(t, 1, bm.Cardinality())
			default:
				t.Fatalf("unexpected data file in plan: %s", task.File.FilePath())
			}
		}

		// A hides {0,1} of its 2 rows, B hides {0} of its 2 rows.
		assertRowCount(t, tbl, 1)
	})

	t.Run("all blobs can be superseded in one delta", func(t *testing.T) {
		tbl, location, pathA, pathB, dvA, dvB := newTableWithSharedPuffinDVs(t)

		dvA2 := writeDV(t, location, "dv-a2.puffin", pathA, []int64{0, 1})
		dvB2 := writeDV(t, location, "dv-b2.puffin", pathB, []int64{0, 1})
		tx := tbl.NewTransaction()
		require.NoError(t, tx.NewRowDelta(nil).
			AddDeletes(dvA2, dvB2).RemoveDeletes(dvA, dvB).Commit(t.Context()))
		tbl, err := tx.Commit(t.Context())
		require.NoError(t, err)

		live, removed := snapshotDeleteEntryFiles(t, tbl.CurrentSnapshot(), fs)
		assert.ElementsMatch(t, [][2]string{
			{dvA2.FilePath(), pathA},
			{dvB2.FilePath(), pathB},
		}, pathRefPairs(live))
		assert.ElementsMatch(t, [][2]string{
			{dvA.FilePath(), pathA},
			{dvB.FilePath(), pathB},
		}, pathRefPairs(removed),
			"two removals at the shared path with distinct references are legal")

		assertRowCount(t, tbl, 0)
	})

	t.Run("removal without a referenced data file is ambiguous", func(t *testing.T) {
		tbl, location, pathA, _, dvA, _ := newTableWithSharedPuffinDVs(t)

		dvA2 := writeDV(t, location, "dv-a2.puffin", pathA, []int64{0, 1})
		rd := tbl.NewTransaction().NewRowDelta(nil).
			AddDeletes(dvA2).
			RemoveDeletes(buildPosDeleteFile(t, dvA.FilePath()))

		err := rd.Commit(t.Context())
		require.Error(t, err)
		assert.Contains(t, err.Error(), "ambiguous removal")
	})

	t.Run("reference matching no live entry at the path", func(t *testing.T) {
		tbl, location, pathA, _, dvA, _ := newTableWithSharedPuffinDVs(t)

		dvA2 := writeDV(t, location, "dv-a2.puffin", pathA, []int64{0, 1})
		rd := tbl.NewTransaction().NewRowDelta(nil).
			AddDeletes(dvA2).
			RemoveDeletes(buildDVFile(t, dvA.FilePath(), "s3://bucket/data/nonexistent.parquet"))

		err := rd.Commit(t.Context())
		require.Error(t, err)
		assert.Contains(t, err.Error(), "no live delete entry references data file")
	})

	t.Run("duplicate removals of one entry are rejected", func(t *testing.T) {
		tbl, location, pathA, _, dvA, _ := newTableWithSharedPuffinDVs(t)

		dvA2 := writeDV(t, location, "dv-a2.puffin", pathA, []int64{0, 1})
		rd := tbl.NewTransaction().NewRowDelta(nil).
			AddDeletes(dvA2).
			RemoveDeletes(dvA, buildDVFile(t, dvA.FilePath(), pathA))

		err := rd.Commit(t.Context())
		require.Error(t, err)
		assert.Contains(t, err.Error(), "removed delete files must be unique")
	})
}

// Why: a removal the producer cannot match against a live delete entry
// would be silently dropped, leaving two live DVs on one data file.
// Condition: remove a DV that no snapshot references, on an empty table
// and on a table whose snapshot has no delete files.
// Assertion: Commit fails naming the missing file (or the missing
// snapshot).
func TestRowDeltaRemoveDeletesUnknownFile(t *testing.T) {
	dataPath := "s3://bucket/data/data-001.parquet"
	oldDV := buildDVFile(t, "s3://bucket/data/dv-001.puffin", dataPath)
	newDV := buildDVFile(t, "s3://bucket/data/dv-002.puffin", dataPath)

	t.Run("no current snapshot", func(t *testing.T) {
		tbl := newRowDeltaCommitTestTableVersion(t, 3)
		rd := tbl.NewTransaction().NewRowDelta(nil).
			AddDeletes(newDV).
			RemoveDeletes(oldDV)

		err := rd.Commit(t.Context())
		require.Error(t, err)
		assert.Contains(t, err.Error(), "without an existing snapshot")
	})

	t.Run("not referenced by current snapshot", func(t *testing.T) {
		tbl := newRowDeltaCommitTestTableVersion(t, 3)
		location := tbl.Location()

		arrowSc, err := table.SchemaToArrowSchema(tbl.Schema(), nil, false, false)
		require.NoError(t, err)
		realDataPath := location + "/data/data-001.parquet"
		writeParquetFile(t, realDataPath, arrowSc, `[{"id": 1, "data": "alpha"}]`)

		tx := tbl.NewTransaction()
		require.NoError(t, tx.AddFiles(t.Context(), []string{realDataPath}, nil, false))
		tbl, err = tx.Commit(t.Context())
		require.NoError(t, err)

		rd := tbl.NewTransaction().NewRowDelta(nil).
			AddDeletes(newDV).
			RemoveDeletes(oldDV)

		err = rd.Commit(t.Context())
		require.Error(t, err)
		assert.Contains(t, err.Error(), "do not belong to the table")
	})
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
