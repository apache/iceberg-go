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
	"encoding/json"
	"path/filepath"
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
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func newEqDeleteReadTestTable(t *testing.T) *table.Table {
	t.Helper()

	iceSchema := iceberg.NewSchema(0,
		iceberg.NestedField{ID: 1, Name: "id", Type: iceberg.PrimitiveTypes.Int64, Required: true},
		iceberg.NestedField{ID: 2, Name: "data", Type: iceberg.PrimitiveTypes.String, Required: false},
	)

	return newEqDeleteReadTestTableWithSchema(t, iceSchema, nil)
}

func newEqDeleteReadTestTableWithSchema(t *testing.T, iceSchema *iceberg.Schema, properties iceberg.Properties) *table.Table {
	t.Helper()

	location := filepath.ToSlash(t.TempDir())
	if properties == nil {
		properties = iceberg.Properties{}
	}
	properties[table.PropertyFormatVersion] = "2"

	meta, err := table.NewMetadata(iceSchema, iceberg.UnpartitionedSpec,
		table.UnsortedSortOrder, location,
		properties)
	require.NoError(t, err)

	return table.New(
		table.Identifier{"db", "eq_del_read_test"},
		meta, location+"/metadata/v1.metadata.json",
		func(ctx context.Context) (iceio.IO, error) {
			return iceio.LocalFS{}, nil
		},
		&rowDeltaCatalog{metadata: meta},
	)
}

func nameMappingProperties(t *testing.T, mapping iceberg.NameMapping) iceberg.Properties {
	t.Helper()

	mappingJSON, err := json.Marshal(mapping)
	require.NoError(t, err)

	return iceberg.Properties{table.DefaultNameMappingKey: string(mappingJSON)}
}

func TestEqualityDeleteReadRoundTrip(t *testing.T) {
	tbl := newEqDeleteReadTestTable(t)

	arrowSc, err := table.SchemaToArrowSchema(tbl.Metadata().CurrentSchema(), nil, false, false)
	require.NoError(t, err)

	// Step 1: Append 5 rows.
	dataPath := tbl.Location() + "/data/data-001.parquet"
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

	// Step 2: Write equality delete file that removes id=2 and id=4.
	eqDelPath := tbl.Location() + "/data/eq-del-001.parquet"
	delArrowSc, err := table.SchemaToArrowSchema(
		iceberg.NewSchema(0, iceberg.NestedField{ID: 1, Name: "id", Type: iceberg.PrimitiveTypes.Int64, Required: true}),
		nil, true, false)
	require.NoError(t, err)

	writeParquetFile(t, eqDelPath, delArrowSc, `[{"id": 2}, {"id": 4}]`)

	eqDelBuilder, err := iceberg.NewDataFileBuilder(
		*iceberg.UnpartitionedSpec, iceberg.EntryContentEqDeletes,
		eqDelPath, iceberg.ParquetFile, nil, nil, nil, 2, 256)
	require.NoError(t, err)
	eqDelBuilder.EqualityFieldIDs([]int{1})
	eqDelFile := eqDelBuilder.Build()

	tx2 := tbl.NewTransaction()
	rd := tx2.NewRowDelta(nil)
	rd.AddDeletes(eqDelFile)
	require.NoError(t, rd.Commit(t.Context()))
	tbl, err = tx2.Commit(t.Context())
	require.NoError(t, err)

	// Step 3: Scan and verify rows id=2 and id=4 are deleted.
	assertRowCount(t, tbl, 3)

	_, itr, err := tbl.Scan(table.WithSelectedFields("id")).ToArrowRecords(t.Context())
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

	assert.Equal(t, []int64{1, 3, 5}, ids, "expected rows with id=2 and id=4 deleted")
}

func TestEqualityDeleteReadResolvesRenamedDataColumnByFieldID(t *testing.T) {
	tbl := newEqDeleteReadTestTable(t)

	dataSchema, err := table.SchemaToArrowSchema(tbl.Metadata().CurrentSchema(), nil, true, false)
	require.NoError(t, err)
	dataFields := dataSchema.Fields()
	dataFields[0].Name = "legacy_id"
	dataMetadata := dataSchema.Metadata()
	dataSchema = arrow.NewSchema(dataFields, &dataMetadata)

	dataPath := tbl.Location() + "/data/data-renamed.parquet"
	writeParquetFile(t, dataPath, dataSchema, `[
		{"legacy_id": 1, "data": "one"},
		{"legacy_id": 2, "data": "two"},
		{"legacy_id": 3, "data": "three"}
	]`)

	tx := tbl.NewTransaction()
	require.NoError(t, tx.AddFiles(t.Context(), []string{dataPath}, nil, false))
	tbl, err = tx.Commit(t.Context())
	require.NoError(t, err)

	deleteSchema, err := table.SchemaToArrowSchema(
		iceberg.NewSchema(0, iceberg.NestedField{ID: 1, Name: "id", Type: iceberg.PrimitiveTypes.Int64, Required: true}),
		nil, true, false)
	require.NoError(t, err)
	deletePath := tbl.Location() + "/data/delete-renamed.parquet"
	writeParquetFile(t, deletePath, deleteSchema, `[{"id": 2}]`)

	deleteBuilder, err := iceberg.NewDataFileBuilder(
		*iceberg.UnpartitionedSpec, iceberg.EntryContentEqDeletes,
		deletePath, iceberg.ParquetFile, nil, nil, nil, 1, 128)
	require.NoError(t, err)
	deleteBuilder.EqualityFieldIDs([]int{1})

	tx = tbl.NewTransaction()
	rowDelta := tx.NewRowDelta(nil)
	rowDelta.AddDeletes(deleteBuilder.Build())
	require.NoError(t, rowDelta.Commit(t.Context()))
	tbl, err = tx.Commit(t.Context())
	require.NoError(t, err)

	_, records, err := tbl.Scan(table.WithSelectedFields("id")).ToArrowRecords(t.Context())
	require.NoError(t, err)

	var ids []int64
	for record, err := range records {
		require.NoError(t, err)
		column := record.Column(0).(*array.Int64)
		for i := 0; i < column.Len(); i++ {
			ids = append(ids, column.Value(i))
		}
		record.Release()
	}

	assert.Equal(t, []int64{1, 3}, ids)
}

func TestEqualityDeleteReadResolvesRenamedDataColumnByNameMapping(t *testing.T) {
	iceSchema := iceberg.NewSchema(0,
		iceberg.NestedField{ID: 1, Name: "id", Type: iceberg.PrimitiveTypes.Int64, Required: true},
		iceberg.NestedField{ID: 2, Name: "data", Type: iceberg.PrimitiveTypes.String},
	)
	mapping := iceSchema.NameMapping()
	mapping[0].Names = append(mapping[0].Names, "legacy_id")
	tbl := newEqDeleteReadTestTableWithSchema(t, iceSchema, nameMappingProperties(t, mapping))

	dataSchema := arrow.NewSchema([]arrow.Field{
		{Name: "legacy_id", Type: arrow.PrimitiveTypes.Int64, Nullable: false},
		{Name: "data", Type: arrow.BinaryTypes.String, Nullable: true},
	}, nil)
	dataPath := tbl.Location() + "/data/data-name-mapped.parquet"
	writeParquetFile(t, dataPath, dataSchema, `[
		{"legacy_id": 1, "data": "one"},
		{"legacy_id": 2, "data": "two"},
		{"legacy_id": 3, "data": "three"}
	]`)
	dataBuilder, err := iceberg.NewDataFileBuilder(
		*iceberg.UnpartitionedSpec, iceberg.EntryContentData,
		dataPath, iceberg.ParquetFile, nil, nil, nil, 3, mustFileSize(t, dataPath))
	require.NoError(t, err)

	tx := tbl.NewTransaction()
	require.NoError(t, tx.AddDataFiles(t.Context(), []iceberg.DataFile{dataBuilder.Build()}, nil))
	tbl, err = tx.Commit(t.Context())
	require.NoError(t, err)

	deleteSchema, err := table.SchemaToArrowSchema(
		iceberg.NewSchema(0, iceberg.NestedField{ID: 1, Name: "id", Type: iceberg.PrimitiveTypes.Int64, Required: true}),
		nil, true, false)
	require.NoError(t, err)
	deletePath := tbl.Location() + "/data/delete-name-mapped.parquet"
	writeParquetFile(t, deletePath, deleteSchema, `[{"id": 2}]`)

	deleteBuilder, err := iceberg.NewDataFileBuilder(
		*iceberg.UnpartitionedSpec, iceberg.EntryContentEqDeletes,
		deletePath, iceberg.ParquetFile, nil, nil, nil, 1, 128)
	require.NoError(t, err)
	deleteBuilder.EqualityFieldIDs([]int{1})

	tx = tbl.NewTransaction()
	rowDelta := tx.NewRowDelta(nil)
	rowDelta.AddDeletes(deleteBuilder.Build())
	require.NoError(t, rowDelta.Commit(t.Context()))
	tbl, err = tx.Commit(t.Context())
	require.NoError(t, err)

	_, records, err := tbl.Scan(table.WithSelectedFields("id")).ToArrowRecords(t.Context())
	require.NoError(t, err)

	var ids []int64
	for record, err := range records {
		require.NoError(t, err)
		column := record.Column(0).(*array.Int64)
		for i := 0; i < column.Len(); i++ {
			ids = append(ids, column.Value(i))
		}
		record.Release()
	}

	assert.Equal(t, []int64{1, 3}, ids)
}

func TestEqualityDeleteReadResolvesLiteralDottedColumnName(t *testing.T) {
	iceSchema := iceberg.NewSchema(0,
		iceberg.NestedField{ID: 1, Name: "user.id", Type: iceberg.PrimitiveTypes.Int64, Required: true},
		iceberg.NestedField{ID: 2, Name: "data", Type: iceberg.PrimitiveTypes.String},
	)
	tbl := newEqDeleteReadTestTableWithSchema(t, iceSchema, nameMappingProperties(t, iceSchema.NameMapping()))

	dataSchema := arrow.NewSchema([]arrow.Field{
		{Name: "user.id", Type: arrow.PrimitiveTypes.Int64, Nullable: false},
		{Name: "data", Type: arrow.BinaryTypes.String, Nullable: true},
	}, nil)
	dataPath := tbl.Location() + "/data/data-dotted-name.parquet"
	writeParquetFile(t, dataPath, dataSchema, `[
		{"user.id": 1, "data": "one"},
		{"user.id": 2, "data": "two"},
		{"user.id": 3, "data": "three"}
	]`)

	tx := tbl.NewTransaction()
	require.NoError(t, tx.AddFiles(t.Context(), []string{dataPath}, nil, false))
	tbl, err := tx.Commit(t.Context())
	require.NoError(t, err)

	deleteSchema, err := table.SchemaToArrowSchema(
		iceberg.NewSchema(0, iceberg.NestedField{ID: 1, Name: "user.id", Type: iceberg.PrimitiveTypes.Int64, Required: true}),
		nil, true, false)
	require.NoError(t, err)
	deletePath := tbl.Location() + "/data/delete-dotted-name.parquet"
	writeParquetFile(t, deletePath, deleteSchema, `[{"user.id": 2}]`)

	deleteBuilder, err := iceberg.NewDataFileBuilder(
		*iceberg.UnpartitionedSpec, iceberg.EntryContentEqDeletes,
		deletePath, iceberg.ParquetFile, nil, nil, nil, 1, 128)
	require.NoError(t, err)
	deleteBuilder.EqualityFieldIDs([]int{1})

	tx = tbl.NewTransaction()
	rowDelta := tx.NewRowDelta(nil)
	rowDelta.AddDeletes(deleteBuilder.Build())
	require.NoError(t, rowDelta.Commit(t.Context()))
	tbl, err = tx.Commit(t.Context())
	require.NoError(t, err)

	_, records, err := tbl.Scan().ToArrowRecords(t.Context())
	require.NoError(t, err)

	var ids []int64
	for record, err := range records {
		require.NoError(t, err)
		column := record.Column(0).(*array.Int64)
		for i := 0; i < column.Len(); i++ {
			ids = append(ids, column.Value(i))
		}
		record.Release()
	}

	assert.Equal(t, []int64{1, 3}, ids)
}

func TestEqualityDeleteReadRejectsAmbiguousColumns(t *testing.T) {
	tbl := newEqDeleteReadTestTable(t)
	arrowSc, err := table.SchemaToArrowSchema(tbl.Metadata().CurrentSchema(), nil, false, false)
	require.NoError(t, err)
	dataPath := tbl.Location() + "/data/data.parquet"
	writeParquetFile(t, dataPath, arrowSc, `[{"id": 1, "data": "one"}]`)
	tx := tbl.NewTransaction()
	require.NoError(t, tx.AddFiles(t.Context(), []string{dataPath}, nil, false))
	tbl, err = tx.Commit(t.Context())
	require.NoError(t, err)

	deleteSchema, err := table.SchemaToArrowSchema(
		iceberg.NewSchema(0, iceberg.NestedField{ID: 1, Name: "id", Type: iceberg.PrimitiveTypes.Int64, Required: true}),
		nil, true, false)
	require.NoError(t, err)
	duplicateSchema := arrow.NewSchema([]arrow.Field{deleteSchema.Field(0), deleteSchema.Field(0)}, nil)
	deletePath := tbl.Location() + "/data/delete.parquet"
	builder := array.NewInt64Builder(memory.DefaultAllocator)
	builder.Append(1)
	first := builder.NewArray()
	builder.Append(2)
	second := builder.NewArray()
	builder.Release()
	batch := array.NewRecordBatch(duplicateSchema, []arrow.Array{first, second}, 1)
	first.Release()
	second.Release()
	deleteTable := array.NewTableFromRecords(duplicateSchema, []arrow.RecordBatch{batch})
	batch.Release()
	file, err := iceio.LocalFS{}.Create(deletePath)
	require.NoError(t, err)
	defer file.Close()
	require.NoError(t, pqarrow.WriteTable(deleteTable, file, 1,
		parquet.NewWriterProperties(parquet.WithStats(true)), pqarrow.DefaultWriterProps()))
	deleteTable.Release()
	deleteBuilder, err := iceberg.NewDataFileBuilder(
		*iceberg.UnpartitionedSpec, iceberg.EntryContentEqDeletes,
		deletePath, iceberg.ParquetFile, nil, nil, nil, 1, 128)
	require.NoError(t, err)
	deleteBuilder.EqualityFieldIDs([]int{1})
	tx = tbl.NewTransaction()
	rd := tx.NewRowDelta(nil)
	rd.AddDeletes(deleteBuilder.Build())
	require.NoError(t, rd.Commit(t.Context()))
	tbl, err = tx.Commit(t.Context())
	require.NoError(t, err)

	_, _, err = tbl.Scan().ToArrowRecords(t.Context())
	require.ErrorIs(t, err, table.ErrAmbiguousEqualityColumn)
}

func TestEqualityDeleteMatchingAcrossPartitionSpecEvolution(t *testing.T) {
	ctx := t.Context()
	tbl := newEqDeleteReadTestTable(t)

	// Keep spec 0 available for the global delete, then write data under a
	// partitioned spec and rename that partition field to create a second spec
	// with the same field ID and identical-looking partition tuple.
	tx := tbl.NewTransaction()
	require.NoError(t, table.NewUpdateSpec(tx, false).
		AddField("data", iceberg.IdentityTransform{}, "data_partition").Commit())
	var err error
	tbl, err = tx.Commit(ctx)
	require.NoError(t, err)
	dataSpec := tbl.Metadata().PartitionSpec()
	require.Equal(t, 1, dataSpec.ID())

	arrowSc, err := table.SchemaToArrowSchema(tbl.Metadata().CurrentSchema(), nil, false, false)
	require.NoError(t, err)
	rec, _, err := array.RecordFromJSON(memory.DefaultAllocator, arrowSc, strings.NewReader(`[
		{"id": 1, "data": "books"},
		{"id": 2, "data": "books"}
	]`))
	require.NoError(t, err)
	defer rec.Release()
	data := array.NewTableFromRecords(arrowSc, []arrow.RecordBatch{rec})
	defer data.Release()

	tbl, err = tbl.AppendTable(ctx, data, rec.NumRows(), nil)
	require.NoError(t, err)

	tx = tbl.NewTransaction()
	require.NoError(t, table.NewUpdateSpec(tx, false).
		RenameField("data_partition", "renamed_data_partition").Commit())
	tbl, err = tx.Commit(ctx)
	require.NoError(t, err)
	deleteSpec := tbl.Metadata().PartitionSpec()
	require.Equal(t, 2, deleteSpec.ID())

	delSchema := iceberg.NewSchema(0,
		iceberg.NestedField{ID: 1, Name: "id", Type: iceberg.PrimitiveTypes.Int64, Required: true},
		iceberg.NestedField{ID: 2, Name: "data", Type: iceberg.PrimitiveTypes.String, Required: false},
	)
	delArrowSc, err := table.SchemaToArrowSchema(delSchema, nil, true, false)
	require.NoError(t, err)
	partitionedRecords, release := makeEqDeleteRecords(t, delArrowSc,
		`[{"id": 1, "data": "books"}]`)
	defer release()

	tx = tbl.NewTransaction()
	partitionedDeletes, err := tx.WriteEqualityDeletes(ctx, []int{1}, partitionedRecords)
	require.NoError(t, err)
	require.Len(t, partitionedDeletes, 1)
	require.EqualValues(t, 2, partitionedDeletes[0].SpecID())
	require.NoError(t, tx.NewRowDelta(nil).AddDeletes(partitionedDeletes...).Commit(ctx))
	tbl, err = tx.Commit(ctx)
	require.NoError(t, err)

	globalDeletePath := tbl.Location() + "/data/global-eq-delete.parquet"
	globalArrowSc, err := table.SchemaToArrowSchema(
		iceberg.NewSchema(0,
			iceberg.NestedField{ID: 1, Name: "id", Type: iceberg.PrimitiveTypes.Int64, Required: true},
		), nil, true, false)
	require.NoError(t, err)
	writeParquetFile(t, globalDeletePath, globalArrowSc, `[{"id": 2}]`)
	globalBuilder, err := iceberg.NewDataFileBuilder(
		*iceberg.UnpartitionedSpec, iceberg.EntryContentEqDeletes,
		globalDeletePath, iceberg.ParquetFile, nil, nil, nil, 1, 128)
	require.NoError(t, err)
	globalBuilder.EqualityFieldIDs([]int{1})

	tx = tbl.NewTransaction()
	require.NoError(t, tx.NewRowDelta(nil).AddDeletes(globalBuilder.Build()).Commit(ctx))
	tbl, err = tx.Commit(ctx)
	require.NoError(t, err)

	tasks, err := tbl.Scan().PlanFiles(ctx)
	require.NoError(t, err)
	require.Len(t, tasks, 1)
	require.EqualValues(t, 1, tasks[0].File.SpecID())
	require.Equal(t, partitionedDeletes[0].Partition(), tasks[0].File.Partition(),
		"the data and partitioned delete must have identical tuples under different specs")
	require.Len(t, tasks[0].EqualityDeleteFiles, 1)
	assert.Equal(t, globalDeletePath, tasks[0].EqualityDeleteFiles[0].FilePath())

	_, itr, err := tbl.Scan(table.WithSelectedFields("id")).ToArrowRecords(ctx)
	require.NoError(t, err)
	var ids []int64
	for record, err := range itr {
		require.NoError(t, err)
		col := record.Column(0).(*array.Int64)
		for i := 0; i < col.Len(); i++ {
			ids = append(ids, col.Value(i))
		}
		record.Release()
	}
	assert.Equal(t, []int64{1}, ids)
}

func TestEqualityDeleteDoesNotAffectSameSnapshot(t *testing.T) {
	tbl := newEqDeleteReadTestTable(t)

	arrowSc, err := table.SchemaToArrowSchema(tbl.Metadata().CurrentSchema(), nil, false, false)
	require.NoError(t, err)

	// Append data file and equality delete in the SAME snapshot via RowDelta.
	// The equality delete should NOT affect the data file in the same commit
	// (sequence number rule: delete must be strictly greater).
	records := func(yield func(arrow.RecordBatch, error) bool) {
		record, _, err := array.RecordFromJSON(
			memory.DefaultAllocator,
			arrowSc,
			strings.NewReader(`[
				{"id": 1, "data": "alpha"},
				{"id": 2, "data": "beta"}
			]`),
		)
		require.NoError(t, err)
		yield(record, nil)
	}

	var dataFiles []iceberg.DataFile
	for dataFile, err := range table.WriteRecords(t.Context(), tbl, arrowSc, records) {
		require.NoError(t, err)
		dataFiles = append(dataFiles, dataFile)
	}
	require.Len(t, dataFiles, 1)

	eqDelPath := tbl.Location() + "/data/eq-del-001.parquet"
	delArrowSc, err := table.SchemaToArrowSchema(
		iceberg.NewSchema(0, iceberg.NestedField{ID: 1, Name: "id", Type: iceberg.PrimitiveTypes.Int64, Required: true}),
		nil, true, false)
	require.NoError(t, err)

	writeParquetFile(t, eqDelPath, delArrowSc, `[{"id": 2}]`)

	// Build equality delete file
	eqDelBuilder, err := iceberg.NewDataFileBuilder(
		*iceberg.UnpartitionedSpec, iceberg.EntryContentEqDeletes,
		eqDelPath, iceberg.ParquetFile, nil, nil, nil, 1, 128)
	require.NoError(t, err)
	eqDelBuilder.EqualityFieldIDs([]int{1})

	// Commit the data and delete in the same RowDelta so they receive the same
	// sequence number.
	tx := tbl.NewTransaction()
	rd := tx.NewRowDelta(nil)
	rd.AddRows(dataFiles...)
	rd.AddDeletes(eqDelBuilder.Build())
	require.NoError(t, rd.Commit(t.Context()))

	tbl, err = tx.Commit(t.Context())
	require.NoError(t, err)

	// Both rows should be visible because the equality delete in the same
	// snapshot must not affect co-committed data files.
	_, itr, err := tbl.Scan(table.WithSelectedFields("id")).ToArrowRecords(t.Context())
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

	assert.ElementsMatch(t, []int64{1, 2}, ids)
}

func TestEqualityDeleteMultiColumnKey(t *testing.T) {
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
		table.Identifier{"db", "multi_key_read"},
		meta, location+"/metadata/v1.metadata.json",
		func(ctx context.Context) (iceio.IO, error) {
			return iceio.LocalFS{}, nil
		},
		&rowDeltaCatalog{metadata: meta},
	)

	arrowSc, err := table.SchemaToArrowSchema(iceSchema, nil, false, false)
	require.NoError(t, err)

	// Append data.
	dataPath := location + "/data/data-001.parquet"
	writeParquetFile(t, dataPath, arrowSc, `[
		{"id": 1, "name": "alice", "value": 10.0},
		{"id": 2, "name": "bob", "value": 20.0},
		{"id": 1, "name": "charlie", "value": 30.0}
	]`)

	tx := tbl.NewTransaction()
	require.NoError(t, tx.AddFiles(t.Context(), []string{dataPath}, nil, false))
	tbl, err = tx.Commit(t.Context())
	require.NoError(t, err)

	// Delete by composite key (id=1, name="alice"). Should only remove
	// the first row, not the third (id=1, name="charlie").
	delArrowSc, err := table.SchemaToArrowSchema(
		iceberg.NewSchema(0,
			iceberg.NestedField{ID: 1, Name: "id", Type: iceberg.PrimitiveTypes.Int64, Required: true},
			iceberg.NestedField{ID: 2, Name: "name", Type: iceberg.PrimitiveTypes.String, Required: true},
		), nil, true, false)
	require.NoError(t, err)

	eqDelPath := location + "/data/eq-del-001.parquet"
	writeParquetFile(t, eqDelPath, delArrowSc, `[{"id": 1, "name": "alice"}]`)

	eqDelBuilder, err := iceberg.NewDataFileBuilder(
		*iceberg.UnpartitionedSpec, iceberg.EntryContentEqDeletes,
		eqDelPath, iceberg.ParquetFile, nil, nil, nil, 1, 128)
	require.NoError(t, err)
	eqDelBuilder.EqualityFieldIDs([]int{1, 2})

	tx2 := tbl.NewTransaction()
	rd := tx2.NewRowDelta(nil)
	rd.AddDeletes(eqDelBuilder.Build())
	require.NoError(t, rd.Commit(t.Context()))
	tbl, err = tx2.Commit(t.Context())
	require.NoError(t, err)

	assertRowCount(t, tbl, 2)

	_, itr, err := tbl.Scan(table.WithSelectedFields("id", "name")).ToArrowRecords(t.Context())
	require.NoError(t, err)

	type row struct {
		id   int64
		name string
	}

	var rows []row
	for rec, err := range itr {
		require.NoError(t, err)
		idCol := rec.Column(0).(*array.Int64)
		nameCol := rec.Column(1).(*array.String)
		for i := 0; i < int(rec.NumRows()); i++ {
			rows = append(rows, row{id: idCol.Value(i), name: nameCol.Value(i)})
		}
		rec.Release()
	}

	assert.Equal(t, []row{
		{id: 2, name: "bob"},
		{id: 1, name: "charlie"},
	}, rows)
}

func TestEqualityDeleteNullableFastPathKeys(t *testing.T) {
	tests := []struct {
		name           string
		keyType        iceberg.Type
		dataJSON       string
		nullDeleteJSON string
		zeroDeleteJSON string
	}{
		{
			name:    "int",
			keyType: iceberg.PrimitiveTypes.Int32,
			dataJSON: `[
				{"row_id": 1, "key": null},
				{"row_id": 2, "key": 0},
				{"row_id": 3, "key": 7}
			]`,
			nullDeleteJSON: `[{"key": null}]`,
			zeroDeleteJSON: `[{"key": 0}]`,
		},
		{
			name:    "long",
			keyType: iceberg.PrimitiveTypes.Int64,
			dataJSON: `[
				{"row_id": 1, "key": null},
				{"row_id": 2, "key": 0},
				{"row_id": 3, "key": 7}
			]`,
			nullDeleteJSON: `[{"key": null}]`,
			zeroDeleteJSON: `[{"key": 0}]`,
		},
		{
			name:    "date",
			keyType: iceberg.PrimitiveTypes.Date,
			dataJSON: `[
				{"row_id": 1, "key": null},
				{"row_id": 2, "key": "1970-01-01"},
				{"row_id": 3, "key": "2024-01-02"}
			]`,
			nullDeleteJSON: `[{"key": null}]`,
			zeroDeleteJSON: `[{"key": "1970-01-01"}]`,
		},
		{
			name:    "timestamp",
			keyType: iceberg.PrimitiveTypes.Timestamp,
			dataJSON: `[
				{"row_id": 1, "key": null},
				{"row_id": 2, "key": "1970-01-01T00:00:00.000000Z"},
				{"row_id": 3, "key": "2024-01-02T03:04:05.000000Z"}
			]`,
			nullDeleteJSON: `[{"key": null}]`,
			zeroDeleteJSON: `[{"key": "1970-01-01T00:00:00.000000Z"}]`,
		},
	}

	for _, tt := range tests {
		for _, del := range []struct {
			name       string
			deleteJSON string
			wantRows   []int64
		}{
			{name: "delete-null", deleteJSON: tt.nullDeleteJSON, wantRows: []int64{2, 3}},
			{name: "delete-zero", deleteJSON: tt.zeroDeleteJSON, wantRows: []int64{1, 3}},
		} {
			t.Run(tt.name+"/"+del.name, func(t *testing.T) {
				tbl := newNullableEqDeleteReadTestTable(t, tt.keyType)
				arrowSc, err := table.SchemaToArrowSchema(tbl.Metadata().CurrentSchema(), nil, false, false)
				require.NoError(t, err)

				dataPath := tbl.Location() + "/data/data-001.parquet"
				writeParquetFile(t, dataPath, arrowSc, tt.dataJSON)

				tx := tbl.NewTransaction()
				require.NoError(t, tx.AddFiles(t.Context(), []string{dataPath}, nil, false))
				tbl, err = tx.Commit(t.Context())
				require.NoError(t, err)

				delArrowSc, err := table.SchemaToArrowSchema(
					iceberg.NewSchema(0, iceberg.NestedField{
						ID: 2, Name: "key", Type: tt.keyType, Required: false,
					}), nil, true, false)
				require.NoError(t, err)

				eqDelPath := tbl.Location() + "/data/eq-del-null.parquet"
				writeParquetFile(t, eqDelPath, delArrowSc, del.deleteJSON)

				eqDelBuilder, err := iceberg.NewDataFileBuilder(
					*iceberg.UnpartitionedSpec, iceberg.EntryContentEqDeletes,
					eqDelPath, iceberg.ParquetFile, nil, nil, nil, 1, 128)
				require.NoError(t, err)
				eqDelBuilder.EqualityFieldIDs([]int{2})

				tx2 := tbl.NewTransaction()
				rd := tx2.NewRowDelta(nil)
				rd.AddDeletes(eqDelBuilder.Build())
				require.NoError(t, rd.Commit(t.Context()))
				tbl, err = tx2.Commit(t.Context())
				require.NoError(t, err)

				assert.Equal(t, del.wantRows, collectRowIDs(t, tbl))
			})
		}
	}
}

func TestEqualityDeleteNullableCompositeFastPathKey(t *testing.T) {
	for _, tt := range []struct {
		name       string
		dataJSON   string
		deleteJSON string
		wantRows   []int64
	}{
		{
			name: "partially null key",
			dataJSON: `[
				{"row_id": 1, "id": 7, "deleted_at": null},
				{"row_id": 2, "id": 7, "deleted_at": "1970-01-01T00:00:00.000000Z"},
				{"row_id": 3, "id": 8, "deleted_at": null},
				{"row_id": 4, "id": null, "deleted_at": null}
			]`,
			deleteJSON: `[{"id": 7, "deleted_at": null}]`,
			wantRows:   []int64{2, 3, 4},
		},
		{
			name: "all-null key",
			dataJSON: `[
				{"row_id": 1, "id": null, "deleted_at": null},
				{"row_id": 2, "id": null, "deleted_at": "1970-01-01T00:00:00.000000Z"},
				{"row_id": 3, "id": 7, "deleted_at": null}
			]`,
			deleteJSON: `[{"id": null, "deleted_at": null}]`,
			wantRows:   []int64{2, 3},
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			location := filepath.ToSlash(t.TempDir())

			iceSchema := iceberg.NewSchema(0,
				iceberg.NestedField{ID: 1, Name: "row_id", Type: iceberg.PrimitiveTypes.Int64, Required: true},
				iceberg.NestedField{ID: 2, Name: "id", Type: iceberg.PrimitiveTypes.Int32, Required: false},
				iceberg.NestedField{ID: 3, Name: "deleted_at", Type: iceberg.PrimitiveTypes.Timestamp, Required: false},
			)

			meta, err := table.NewMetadata(iceSchema, iceberg.UnpartitionedSpec,
				table.UnsortedSortOrder, location,
				iceberg.Properties{table.PropertyFormatVersion: "2"})
			require.NoError(t, err)

			tbl := table.New(
				table.Identifier{"db", "eq_del_nullable_composite"},
				meta, location+"/metadata/v1.metadata.json",
				func(ctx context.Context) (iceio.IO, error) {
					return iceio.LocalFS{}, nil
				},
				&rowDeltaCatalog{metadata: meta},
			)

			arrowSc, err := table.SchemaToArrowSchema(iceSchema, nil, false, false)
			require.NoError(t, err)

			dataPath := location + "/data/data-001.parquet"
			writeParquetFile(t, dataPath, arrowSc, tt.dataJSON)

			tx := tbl.NewTransaction()
			require.NoError(t, tx.AddFiles(t.Context(), []string{dataPath}, nil, false))
			tbl, err = tx.Commit(t.Context())
			require.NoError(t, err)

			delArrowSc, err := table.SchemaToArrowSchema(
				iceberg.NewSchema(0,
					iceberg.NestedField{ID: 2, Name: "id", Type: iceberg.PrimitiveTypes.Int32, Required: false},
					iceberg.NestedField{ID: 3, Name: "deleted_at", Type: iceberg.PrimitiveTypes.Timestamp, Required: false},
				), nil, true, false)
			require.NoError(t, err)

			eqDelPath := location + "/data/eq-del-composite.parquet"
			writeParquetFile(t, eqDelPath, delArrowSc, tt.deleteJSON)

			eqDelBuilder, err := iceberg.NewDataFileBuilder(
				*iceberg.UnpartitionedSpec, iceberg.EntryContentEqDeletes,
				eqDelPath, iceberg.ParquetFile, nil, nil, nil, 1, 128)
			require.NoError(t, err)
			eqDelBuilder.EqualityFieldIDs([]int{2, 3})

			tx2 := tbl.NewTransaction()
			rd := tx2.NewRowDelta(nil)
			rd.AddDeletes(eqDelBuilder.Build())
			require.NoError(t, rd.Commit(t.Context()))
			tbl, err = tx2.Commit(t.Context())
			require.NoError(t, err)

			assert.Equal(t, tt.wantRows, collectRowIDs(t, tbl))
		})
	}
}

func newNullableEqDeleteReadTestTable(t *testing.T, keyType iceberg.Type) *table.Table {
	t.Helper()

	location := filepath.ToSlash(t.TempDir())

	iceSchema := iceberg.NewSchema(0,
		iceberg.NestedField{ID: 1, Name: "row_id", Type: iceberg.PrimitiveTypes.Int64, Required: true},
		iceberg.NestedField{ID: 2, Name: "key", Type: keyType, Required: false},
	)

	meta, err := table.NewMetadata(iceSchema, iceberg.UnpartitionedSpec,
		table.UnsortedSortOrder, location,
		iceberg.Properties{table.PropertyFormatVersion: "2"})
	require.NoError(t, err)

	return table.New(
		table.Identifier{"db", "eq_del_nullable_" + keyType.String()},
		meta, location+"/metadata/v1.metadata.json",
		func(ctx context.Context) (iceio.IO, error) {
			return iceio.LocalFS{}, nil
		},
		&rowDeltaCatalog{metadata: meta},
	)
}

func collectRowIDs(t *testing.T, tbl *table.Table) []int64 {
	t.Helper()

	_, itr, err := tbl.Scan().ToArrowRecords(t.Context())
	require.NoError(t, err)

	var ids []int64
	for rec, err := range itr {
		require.NoError(t, err)

		indices := rec.Schema().FieldIndices("row_id")
		require.NotEmpty(t, indices)

		col, ok := rec.Column(indices[0]).(*array.Int64)
		require.True(t, ok, "row_id column should be *array.Int64, got %T", rec.Column(indices[0]))
		for i := range col.Len() {
			ids = append(ids, col.Value(i))
		}

		rec.Release()
	}

	return ids
}
