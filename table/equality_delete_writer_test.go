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
	"strings"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/arrow-go/v18/parquet/file"
	"github.com/apache/arrow-go/v18/parquet/pqarrow"
	"github.com/apache/iceberg-go"
	iceio "github.com/apache/iceberg-go/io"
	"github.com/apache/iceberg-go/table"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func newEqDeleteTestTable(t *testing.T, formatVersion string) *table.Table {
	t.Helper()

	location := filepath.ToSlash(t.TempDir())

	iceSchema := iceberg.NewSchema(0,
		iceberg.NestedField{ID: 1, Name: "id", Type: iceberg.PrimitiveTypes.Int64, Required: true},
		iceberg.NestedField{ID: 2, Name: "data", Type: iceberg.PrimitiveTypes.String, Required: false},
	)

	meta, err := table.NewMetadata(iceSchema, iceberg.UnpartitionedSpec,
		table.UnsortedSortOrder, location,
		iceberg.Properties{table.PropertyFormatVersion: formatVersion})
	require.NoError(t, err)

	return table.New(
		table.Identifier{"db", "eq_del_test"},
		meta, location+"/metadata/v1.metadata.json",
		func(ctx context.Context) (iceio.IO, error) {
			return iceio.LocalFS{}, nil
		},
		&rowDeltaCatalog{metadata: meta},
	)
}

func makeEqDeleteRecords(t *testing.T, arrowSc *arrow.Schema, jsonData string) (func(yield func(arrow.RecordBatch, error) bool), func()) {
	t.Helper()

	rec, _, err := array.RecordFromJSON(memory.DefaultAllocator, arrowSc, strings.NewReader(jsonData))
	require.NoError(t, err)

	records := func(yield func(arrow.RecordBatch, error) bool) {
		yield(rec, nil)
	}

	return records, rec.Release
}

func TestWriteEqualityDeleteFiles(t *testing.T) {
	tbl := newEqDeleteTestTable(t, "2")

	// Build Arrow schema with just the delete key column
	delArrowSc, err := table.SchemaToArrowSchema(
		iceberg.NewSchema(0, iceberg.NestedField{ID: 1, Name: "id", Type: iceberg.PrimitiveTypes.Int64, Required: true}),
		nil, true, false)
	require.NoError(t, err)

	records, release := makeEqDeleteRecords(t, delArrowSc, `[{"id": 2}, {"id": 4}]`)
	defer release()

	tx := tbl.NewTransaction()
	deleteFiles, err := tx.WriteEqualityDeletes(t.Context(), []int{1}, records)
	require.NoError(t, err)
	require.Len(t, deleteFiles, 1)

	df := deleteFiles[0]
	assert.Equal(t, iceberg.EntryContentEqDeletes, df.ContentType())
	assert.Equal(t, []int{1}, df.EqualityFieldIDs())
	assert.EqualValues(t, 2, df.Count())

	// Verify the file was actually written to disk
	fs := iceio.LocalFS{}
	f, err := fs.Open(df.FilePath())
	require.NoError(t, err)
	require.NoError(t, f.Close())
}

func TestWriteEqualityDeleteFilesParquetContent(t *testing.T) {
	tbl := newEqDeleteTestTable(t, "2")

	delArrowSc, err := table.SchemaToArrowSchema(
		iceberg.NewSchema(0, iceberg.NestedField{ID: 1, Name: "id", Type: iceberg.PrimitiveTypes.Int64, Required: true}),
		nil, true, false)
	require.NoError(t, err)

	records, release := makeEqDeleteRecords(t, delArrowSc, `[{"id": 10}, {"id": 20}, {"id": 30}]`)
	defer release()

	tx := tbl.NewTransaction()
	deleteFiles, err := tx.WriteEqualityDeletes(t.Context(), []int{1}, records)
	require.NoError(t, err)
	require.Len(t, deleteFiles, 1)

	// Read the Parquet file back and verify its content
	rdr, err := file.OpenParquetFile(deleteFiles[0].FilePath(), false)
	require.NoError(t, err)
	defer rdr.Close()

	arrowRdr, err := pqarrow.NewFileReader(rdr, pqarrow.ArrowReadProperties{}, memory.DefaultAllocator)
	require.NoError(t, err)

	tblReader, err := arrowRdr.ReadTable(t.Context())
	require.NoError(t, err)
	defer tblReader.Release()

	// Should have exactly 1 column (the delete key) and 3 rows
	assert.Equal(t, int64(1), tblReader.NumCols())
	assert.Equal(t, int64(3), tblReader.NumRows())

	col := tblReader.Column(0)
	assert.Equal(t, "id", col.Name())

	// Verify actual values by collecting all chunks
	chunked := col.Data()
	var vals []int64
	for i := range chunked.Chunks() {
		arr := chunked.Chunks()[i].(*array.Int64)
		for j := 0; j < arr.Len(); j++ {
			vals = append(vals, arr.Value(j))
		}
	}
	assert.Equal(t, []int64{10, 20, 30}, vals)
}

func TestWriteEqualityDeleteFilesMultiColumnKey(t *testing.T) {
	location := filepath.ToSlash(t.TempDir())

	iceSchema := iceberg.NewSchema(0,
		iceberg.NestedField{ID: 1, Name: "id", Type: iceberg.PrimitiveTypes.Int64, Required: true},
		iceberg.NestedField{ID: 2, Name: "name", Type: iceberg.PrimitiveTypes.String, Required: true},
		iceberg.NestedField{ID: 3, Name: "value", Type: iceberg.PrimitiveTypes.Float64, Required: false},
	)

	meta, err := table.NewMetadata(iceSchema, iceberg.UnpartitionedSpec,
		table.UnsortedSortOrder, location,
		iceberg.Properties{table.PropertyFormatVersion: "2"})
	require.NoError(t, err)

	tbl := table.New(
		table.Identifier{"db", "multi_key_test"},
		meta, location+"/metadata/v1.metadata.json",
		func(ctx context.Context) (iceio.IO, error) {
			return iceio.LocalFS{}, nil
		},
		&rowDeltaCatalog{metadata: meta},
	)

	delArrowSc, err := table.SchemaToArrowSchema(
		iceberg.NewSchema(0,
			iceberg.NestedField{ID: 1, Name: "id", Type: iceberg.PrimitiveTypes.Int64, Required: true},
			iceberg.NestedField{ID: 2, Name: "name", Type: iceberg.PrimitiveTypes.String, Required: true},
		), nil, true, false)
	require.NoError(t, err)

	records, release := makeEqDeleteRecords(t, delArrowSc,
		`[{"id": 1, "name": "alice"}, {"id": 2, "name": "bob"}]`)
	defer release()

	tx := tbl.NewTransaction()
	deleteFiles, err := tx.WriteEqualityDeletes(t.Context(), []int{1, 2}, records)
	require.NoError(t, err)
	require.Len(t, deleteFiles, 1)

	df := deleteFiles[0]
	assert.Equal(t, iceberg.EntryContentEqDeletes, df.ContentType())
	assert.Equal(t, []int{1, 2}, df.EqualityFieldIDs())
	assert.EqualValues(t, 2, df.Count())
}

func TestWriteEqualityDeleteFilesRejectsV1Table(t *testing.T) {
	tbl := newEqDeleteTestTable(t, "1")

	records := func(yield func(arrow.RecordBatch, error) bool) {}

	tx := tbl.NewTransaction()
	_, err := tx.WriteEqualityDeletes(t.Context(), []int{1}, records)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "format version >= 2")
}

func TestWriteEqualityDeleteFilesRejectsEmptyFieldIDs(t *testing.T) {
	tbl := newEqDeleteTestTable(t, "2")

	records := func(yield func(arrow.RecordBatch, error) bool) {}

	tx := tbl.NewTransaction()
	_, err := tx.WriteEqualityDeletes(t.Context(), nil, records)
	require.ErrorIs(t, err, table.ErrEmptyEqualityFieldIDs)
}

func TestWriteEqualityDeleteFilesRejectsInvalidFieldID(t *testing.T) {
	tbl := newEqDeleteTestTable(t, "2")

	records := func(yield func(arrow.RecordBatch, error) bool) {}

	tx := tbl.NewTransaction()
	_, err := tx.WriteEqualityDeletes(t.Context(), []int{999}, records)
	require.ErrorIs(t, err, iceberg.ErrInvalidSchema)
}

func TestWriteEqualityDeleteFilesRejectsPartitionedTable(t *testing.T) {
	location := filepath.ToSlash(t.TempDir())

	iceSchema := iceberg.NewSchema(0,
		iceberg.NestedField{ID: 1, Name: "id", Type: iceberg.PrimitiveTypes.Int64, Required: true},
		iceberg.NestedField{ID: 2, Name: "category", Type: iceberg.PrimitiveTypes.String, Required: true},
	)

	partSpec := iceberg.NewPartitionSpec(
		iceberg.PartitionField{SourceID: 2, FieldID: 1000, Name: "category", Transform: iceberg.IdentityTransform{}},
	)

	meta, err := table.NewMetadata(iceSchema, &partSpec,
		table.UnsortedSortOrder, location,
		iceberg.Properties{table.PropertyFormatVersion: "2"})
	require.NoError(t, err)

	tbl := table.New(
		table.Identifier{"db", "partitioned_test"},
		meta, location+"/metadata/v1.metadata.json",
		func(ctx context.Context) (iceio.IO, error) {
			return iceio.LocalFS{}, nil
		},
		&rowDeltaCatalog{metadata: meta},
	)

	delArrowSc, err := table.SchemaToArrowSchema(
		iceberg.NewSchema(0, iceberg.NestedField{ID: 1, Name: "id", Type: iceberg.PrimitiveTypes.Int64, Required: true}),
		nil, true, false)
	require.NoError(t, err)

	records, release := makeEqDeleteRecords(t, delArrowSc, `[{"id": 1}]`)
	defer release()

	tx := tbl.NewTransaction()
	_, err = tx.WriteEqualityDeletes(t.Context(), []int{1}, records)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "partitioned tables is not yet supported")
}

func TestWriteEqualityDeleteFilesCommitViaRowDelta(t *testing.T) {
	tbl := newEqDeleteTestTable(t, "2")

	// Step 1: Append data
	arrowSc, err := table.SchemaToArrowSchema(tbl.Metadata().CurrentSchema(), nil, false, false)
	require.NoError(t, err)

	dataPath := tbl.Location() + "/data/data-001.parquet"
	writeParquetFile(t, dataPath, arrowSc, `[
		{"id": 1, "data": "alpha"},
		{"id": 2, "data": "beta"},
		{"id": 3, "data": "gamma"}
	]`)

	tx := tbl.NewTransaction()
	require.NoError(t, tx.AddFiles(t.Context(), []string{dataPath}, nil, false))
	tbl, err = tx.Commit(t.Context())
	require.NoError(t, err)
	assertRowCount(t, tbl, 3)

	// Step 2: Write equality delete files and commit via RowDelta
	delArrowSc, err := table.SchemaToArrowSchema(
		iceberg.NewSchema(0, iceberg.NestedField{ID: 1, Name: "id", Type: iceberg.PrimitiveTypes.Int64, Required: true}),
		nil, true, false)
	require.NoError(t, err)

	records, release := makeEqDeleteRecords(t, delArrowSc, `[{"id": 2}]`)
	defer release()

	tx2 := tbl.NewTransaction()
	deleteFiles, err := tx2.WriteEqualityDeletes(t.Context(), []int{1}, records)
	require.NoError(t, err)
	require.Len(t, deleteFiles, 1)

	rd := tx2.NewRowDelta(nil)
	rd.AddDeletes(deleteFiles...)
	require.NoError(t, rd.Commit(t.Context()))

	tbl, err = tx2.Commit(t.Context())
	require.NoError(t, err)

	snap := tbl.CurrentSnapshot()
	require.NotNil(t, snap)
	assert.Equal(t, table.OpDelete, snap.Summary.Operation)
	assert.Equal(t, "1", snap.Summary.Properties["added-equality-delete-files"])
}
