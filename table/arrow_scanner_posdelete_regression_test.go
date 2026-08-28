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
	"bytes"
	"context"
	"strconv"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/compute"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/arrow-go/v18/parquet"
	"github.com/apache/arrow-go/v18/parquet/file"
	"github.com/apache/arrow-go/v18/parquet/pqarrow"
	"github.com/apache/iceberg-go"
	iceio "github.com/apache/iceberg-go/io"
	"github.com/apache/iceberg-go/table/internal"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestPositionDeleteColumnIndices(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name              string
		fields            []arrow.Field
		wantFilePathIndex int
		wantPosIndex      int
		wantErr           string
	}{
		{name: "valid", fields: []arrow.Field{{Name: "file_path", Type: arrow.BinaryTypes.String}, {Name: "pos", Type: arrow.PrimitiveTypes.Int64}}, wantPosIndex: 1},
		{name: "valid with row", fields: []arrow.Field{{Name: "file_path", Type: arrow.BinaryTypes.String}, {Name: "pos", Type: arrow.PrimitiveTypes.Int64}, {Name: "row", Type: arrow.BinaryTypes.String}}, wantPosIndex: 1},
		{name: "valid reversed", fields: []arrow.Field{{Name: "pos", Type: arrow.PrimitiveTypes.Int64}, {Name: "file_path", Type: arrow.BinaryTypes.String}}, wantFilePathIndex: 1},
		{name: "missing file_path", fields: []arrow.Field{{Name: "pos", Type: arrow.PrimitiveTypes.Int64}}, wantErr: `exactly one "file_path" column, found 0`},
		{name: "missing pos", fields: []arrow.Field{{Name: "file_path", Type: arrow.BinaryTypes.String}}, wantErr: `exactly one "pos" column, found 0`},
		{name: "missing both", fields: nil, wantErr: `exactly one "file_path" column, found 0`},
		{name: "duplicate pos", fields: []arrow.Field{{Name: "file_path", Type: arrow.BinaryTypes.String}, {Name: "pos", Type: arrow.PrimitiveTypes.Int64}, {Name: "pos", Type: arrow.PrimitiveTypes.Int64}}, wantErr: `exactly one "pos" column, found 2`},
		{name: "duplicate file_path", fields: []arrow.Field{{Name: "file_path", Type: arrow.BinaryTypes.String}, {Name: "file_path", Type: arrow.BinaryTypes.String}, {Name: "pos", Type: arrow.PrimitiveTypes.Int64}}, wantErr: `exactly one "file_path" column, found 2`},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			filePathIndex, posIndex, err := positionDeleteColumnIndices(arrow.NewSchema(test.fields, nil))
			if test.wantErr != "" {
				require.ErrorIs(t, err, iceberg.ErrInvalidSchema)
				require.ErrorContains(t, err, test.wantErr)

				return
			}

			require.NoError(t, err)
			assert.Equal(t, test.wantFilePathIndex, filePathIndex)
			assert.Equal(t, test.wantPosIndex, posIndex)
		})
	}
}

func TestReadDeletesRejectsMissingFilePath(t *testing.T) {
	mem := memory.NewCheckedAllocator(memory.DefaultAllocator)
	ctx := compute.WithAllocator(t.Context(), mem)
	defer mem.AssertSize(t, 0)

	deleteSchema := arrow.NewSchema([]arrow.Field{{Name: "pos", Type: arrow.PrimitiveTypes.Int64}}, nil)
	deletePath := "mem://bucket/deletes/missing-file-path.parquet"
	rec := mustLoadRecordBatchFromJSON(deleteSchema, `[{"pos": 1}]`)
	defer rec.Release()
	tbl := array.NewTableFromRecords(deleteSchema, []arrow.RecordBatch{rec})
	defer tbl.Release()

	memFS := iceio.NewMemFS()
	fw, err := memFS.Create(deletePath)
	require.NoError(t, err)
	require.NoError(t, pqarrow.WriteTable(tbl, fw, rec.NumRows(),
		parquet.NewWriterProperties(parquet.WithStats(true)),
		pqarrow.DefaultWriterProps()))
	require.NoError(t, fw.Close())

	deletes, err := readDeletes(ctx, memFS, newPosDeleteFile(t, deletePath, 1, 128))
	require.ErrorIs(t, err, iceberg.ErrInvalidSchema)
	assert.Nil(t, deletes)
	assert.Contains(t, err.Error(), `exactly one "file_path" column, found 0`)
}

func TestReadDeletesForPathsFiltersUnneededRows(t *testing.T) {
	mem := memory.NewCheckedAllocator(memory.DefaultAllocator)
	ctx := compute.WithAllocator(t.Context(), mem)
	defer mem.AssertSize(t, 0)

	deletePath := "mem://bucket/deletes/filtered.parquet"
	dataPath := "mem://bucket/data/needed.parquet"
	otherPath := "mem://bucket/data/unneeded.parquet"
	memFS := iceio.NewMemFS()
	writePosDeleteParquetToMemFS(t, memFS, deletePath, `[
		{"file_path": "`+dataPath+`", "pos": 10},
		{"file_path": "`+otherPath+`", "pos": 20},
		{"file_path": "`+dataPath+`", "pos": 30}
	]`)

	deletes, err := readDeletesForPaths(ctx, memFS, newPosDeleteFile(t, deletePath, 3, 128), map[string]struct{}{dataPath: {}})
	require.NoError(t, err)
	defer releasePosDeletes(deletes)

	assert.Equal(t, []int64{10, 30}, int64Values(deletes[dataPath]))
	assert.NotContains(t, deletes, otherPath)
}

func TestReadDeletesForPathsHandlesDictionaryFilePath(t *testing.T) {
	mem := memory.NewCheckedAllocator(memory.DefaultAllocator)
	ctx := internal.WithTableProperties(
		compute.WithAllocator(t.Context(), mem),
		iceberg.Properties{internal.ParquetBatchSizeKey: "2"},
	)
	defer mem.AssertSize(t, 0)

	deletePath := "mem://bucket/deletes/dictionary-filtered.parquet"
	dataPath := "mem://bucket/data/needed.parquet"
	otherPath := "mem://bucket/data/unneeded.parquet"
	memFS := iceio.NewMemFS()
	writePosDeleteParquetToMemFSWithSchema(t, memFS, deletePath, PositionalDeleteArrowSchema, `[
		{"file_path": "`+dataPath+`", "pos": 10},
		{"file_path": "`+otherPath+`", "pos": 20},
		{"file_path": "`+dataPath+`", "pos": 30},
		{"file_path": "`+dataPath+`", "pos": 40}
	]`)

	deletes, err := readDeletesForPaths(ctx, memFS, newPosDeleteFile(t, deletePath, 4, 128), map[string]struct{}{dataPath: {}})
	require.NoError(t, err)
	defer releasePosDeletes(deletes)

	assert.Equal(t, []int64{10, 30, 40}, int64Values(deletes[dataPath]))
	assert.NotContains(t, deletes, otherPath)
}

func TestReadDeletesForPathsHandlesReversedPhysicalSchema(t *testing.T) {
	mem := memory.NewCheckedAllocator(memory.DefaultAllocator)
	ctx := compute.WithAllocator(t.Context(), mem)
	defer mem.AssertSize(t, 0)

	deleteSchema := arrow.NewSchema([]arrow.Field{
		{Name: "pos", Type: arrow.PrimitiveTypes.Int64, Nullable: false},
		{Name: "file_path", Type: arrow.BinaryTypes.String, Nullable: false},
	}, nil)
	deletePath := "mem://bucket/deletes/reversed-filtered.parquet"
	dataPath := "mem://bucket/data/needed.parquet"
	otherPath := "mem://bucket/data/unneeded.parquet"
	memFS := iceio.NewMemFS()
	writePosDeleteParquetToMemFSWithSchema(t, memFS, deletePath, deleteSchema, `[
		{"pos": 10, "file_path": "`+dataPath+`"},
		{"pos": 20, "file_path": "`+otherPath+`"},
		{"pos": 30, "file_path": "`+dataPath+`"}
	]`)

	deletes, err := readDeletesForPaths(ctx, memFS, newPosDeleteFile(t, deletePath, 3, 128), map[string]struct{}{dataPath: {}})
	require.NoError(t, err)
	defer releasePosDeletes(deletes)

	assert.Equal(t, []int64{10, 30}, int64Values(deletes[dataPath]))
	assert.NotContains(t, deletes, otherPath)
}

func TestReadAllDeleteFilesUsesTaskDataFilePaths(t *testing.T) {
	mem := memory.NewCheckedAllocator(memory.DefaultAllocator)
	ctx := compute.WithAllocator(t.Context(), mem)
	defer mem.AssertSize(t, 0)

	deletePath := "mem://bucket/deletes/task-target.parquet"
	dataPath := "mem://bucket/data/needed.parquet"
	otherPath := "mem://bucket/data/unneeded.parquet"
	memFS := iceio.NewMemFS()
	writePosDeleteParquetToMemFS(t, memFS, deletePath, `[
		{"file_path": "`+dataPath+`", "pos": 10},
		{"file_path": "`+otherPath+`", "pos": 20}
	]`)

	dataBuilder, err := iceberg.NewDataFileBuilder(
		*iceberg.UnpartitionedSpec, iceberg.EntryContentData,
		dataPath, iceberg.ParquetFile, nil, nil, nil, 1, 128)
	require.NoError(t, err)

	deletes, err := readAllDeleteFiles(ctx, memFS, []FileScanTask{{
		File: dataBuilder.Build(),
		DeleteFiles: []iceberg.DataFile{
			newPosDeleteFile(t, deletePath, 2, 128),
		},
	}}, 1)
	require.NoError(t, err)
	defer releasePerFilePosDeletes(deletes)

	assert.Equal(t, []int64{10}, int64Values(deletes[dataPath][0]))
	assert.NotContains(t, deletes, otherPath)
}

func TestPositionDeleteRowGroupTesterUsesFilePathStats(t *testing.T) {
	dataPath := "mem://bucket/data/needed.parquet"
	otherPath := "mem://bucket/data/unneeded.parquet"
	rec := mustLoadRecordBatchFromJSON(PositionalDeleteArrowSchema, `[
		{"file_path": "`+otherPath+`", "pos": 10},
		{"file_path": "`+otherPath+`", "pos": 20},
		{"file_path": "`+dataPath+`", "pos": 30},
		{"file_path": "`+dataPath+`", "pos": 40}
	]`)
	defer rec.Release()

	var buf bytes.Buffer
	writer, err := pqarrow.NewFileWriter(
		PositionalDeleteArrowSchema, &buf,
		parquet.NewWriterProperties(
			parquet.WithStats(true),
			parquet.WithMaxRowGroupLength(2),
		),
		pqarrow.DefaultWriterProps(),
	)
	require.NoError(t, err)
	require.NoError(t, writer.Write(rec))
	require.NoError(t, writer.Close())

	reader, err := file.NewParquetReader(bytes.NewReader(buf.Bytes()))
	require.NoError(t, err)
	defer reader.Close()
	assert.Equal(t, 2, reader.NumRowGroups())

	tester, err := newPositionDeleteRowGroupTester(PositionalDeleteArrowSchema, map[string]struct{}{dataPath: {}})
	require.NoError(t, err)
	require.NotNil(t, tester)

	use, err := tester.StatsFn(reader.MetaData().RowGroup(0), []int{0, 1})
	require.NoError(t, err)
	assert.False(t, use)

	use, err = tester.StatsFn(reader.MetaData().RowGroup(1), []int{0, 1})
	require.NoError(t, err)
	assert.True(t, use)
}

func TestReadDeletesProjectsColumnsAndAccumulatesBatches(t *testing.T) {
	mem := memory.NewCheckedAllocator(memory.DefaultAllocator)
	ctx := compute.WithAllocator(t.Context(), mem)
	ctx = internal.WithTableProperties(ctx, iceberg.Properties{ParquetBatchSizeKey: "2"})
	defer mem.AssertSize(t, 0)

	deleteSchema := arrow.NewSchema([]arrow.Field{
		{Name: "file_path", Type: arrow.BinaryTypes.String, Nullable: false},
		{Name: "pos", Type: arrow.PrimitiveTypes.Int64, Nullable: false},
		{Name: "unused", Type: arrow.BinaryTypes.String, Nullable: false},
	}, nil)
	deletePath := "mem://bucket/deletes/projected.parquet"
	dataPath := "mem://bucket/data/data.parquet"
	rec := mustLoadRecordBatchFromJSON(deleteSchema, `[
		{"file_path": "`+dataPath+`", "pos": 10, "unused": "unused-0"},
		{"file_path": "other/data.parquet", "pos": 20, "unused": "unused-1"},
		{"file_path": "`+dataPath+`", "pos": 30, "unused": "unused-2"},
		{"file_path": "third/data.parquet", "pos": 40, "unused": "unused-3"},
		{"file_path": "other/data.parquet", "pos": 50, "unused": "unused-4"}
	]`)
	defer rec.Release()
	tbl := array.NewTableFromRecords(deleteSchema, []arrow.RecordBatch{rec})
	defer tbl.Release()

	memFS := iceio.NewMemFS()
	fw, err := memFS.Create(deletePath)
	require.NoError(t, err)
	require.NoError(t, pqarrow.WriteTable(tbl, fw, rec.NumRows(),
		parquet.NewWriterProperties(parquet.WithStats(true)),
		pqarrow.DefaultWriterProps()))
	require.NoError(t, fw.Close())

	dataFile := newPosDeleteFile(t, deletePath, rec.NumRows(), 128)
	src, err := internal.GetFile(ctx, memFS, dataFile, true)
	require.NoError(t, err)
	rdr, err := src.GetReader(ctx)
	require.NoError(t, err)
	defer func() { require.NoError(t, rdr.Close()) }()

	projected, err := rdr.GetRecords(ctx, []int{0, 1}, nil)
	require.NoError(t, err)
	assert.Equal(t, 2, projected.Schema().NumFields())
	assert.Equal(t, "file_path", projected.Schema().Field(0).Name)
	assert.Equal(t, "pos", projected.Schema().Field(1).Name)
	var batchCount int
	for projected.Next() {
		batchCount++
		assert.Equal(t, int64(2), projected.RecordBatch().NumCols())
	}
	require.NoError(t, projected.Err())
	projected.Release()
	assert.Equal(t, 3, batchCount)

	deletes, err := readDeletes(ctx, memFS, dataFile)
	require.NoError(t, err)
	defer releasePosDeletes(deletes)

	assert.Equal(t, []int64{10, 30}, int64Values(deletes[dataPath]))
	assert.Equal(t, []int64{20, 50}, int64Values(deletes["other/data.parquet"]))
	assert.Equal(t, []int64{40}, int64Values(deletes["third/data.parquet"]))
}

func TestReadDeletesHandlesDictionaryEncodedFilePath(t *testing.T) {
	mem := memory.NewCheckedAllocator(memory.DefaultAllocator)
	ctx := compute.WithAllocator(t.Context(), mem)
	ctx = internal.WithTableProperties(ctx, iceberg.Properties{ParquetBatchSizeKey: "2"})
	defer mem.AssertSize(t, 0)

	deleteSchema := arrow.NewSchema([]arrow.Field{
		{Name: "file_path", Type: arrow.BinaryTypes.String, Nullable: false},
		{Name: "pos", Type: arrow.PrimitiveTypes.Int64, Nullable: false},
	}, nil)
	deletePath := "mem://bucket/deletes/dictionary-file-path.parquet"
	dataPath := "mem://bucket/data/data.parquet"
	memFS := iceio.NewMemFS()
	writePosDeleteParquetToMemFSWithSchema(t, memFS, deletePath, deleteSchema, `[
		{"file_path": "`+dataPath+`", "pos": 10},
		{"file_path": "other/data.parquet", "pos": 20},
		{"file_path": "`+dataPath+`", "pos": 30},
		{"file_path": "`+dataPath+`", "pos": 40}
	]`)

	dataFile := newPosDeleteFile(t, deletePath, 4, 128)
	src, err := internal.GetFile(ctx, memFS, dataFile, true)
	require.NoError(t, err)
	rdr, err := src.GetReader(ctx)
	require.NoError(t, err)
	defer func() { require.NoError(t, rdr.Close()) }()

	records, err := rdr.GetRecords(ctx, []int{0, 1}, nil)
	require.NoError(t, err)
	defer records.Release()
	var dictionaryBatches int
	for records.Next() {
		if _, ok := records.RecordBatch().Column(0).(*array.Dictionary); ok {
			dictionaryBatches++
		}
	}
	require.NoError(t, records.Err())
	assert.Greater(t, dictionaryBatches, 0)

	deletes, err := readDeletes(ctx, memFS, dataFile)
	require.NoError(t, err)
	defer releasePosDeletes(deletes)

	assert.Equal(t, []int64{10, 30, 40}, int64Values(deletes[dataPath]))
	assert.Equal(t, []int64{20}, int64Values(deletes["other/data.parquet"]))
}

func TestReadDeletesHandlesReversedPhysicalSchema(t *testing.T) {
	mem := memory.NewCheckedAllocator(memory.DefaultAllocator)
	ctx := compute.WithAllocator(t.Context(), mem)
	ctx = internal.WithTableProperties(ctx, iceberg.Properties{ParquetBatchSizeKey: "2"})
	defer mem.AssertSize(t, 0)

	deleteSchema := arrow.NewSchema([]arrow.Field{
		{Name: "pos", Type: arrow.PrimitiveTypes.Int64, Nullable: false},
		{Name: "file_path", Type: arrow.BinaryTypes.String, Nullable: false},
	}, nil)
	deletePath := "mem://bucket/deletes/reversed-schema.parquet"
	dataPath := "mem://bucket/data/data.parquet"
	memFS := iceio.NewMemFS()
	writePosDeleteParquetToMemFSWithSchema(t, memFS, deletePath, deleteSchema, `[
		{"pos": 10, "file_path": "`+dataPath+`"},
		{"pos": 20, "file_path": "other/data.parquet"},
		{"pos": 30, "file_path": "`+dataPath+`"}
	]`)

	deletes, err := readDeletes(ctx, memFS, newPosDeleteFile(t, deletePath, 3, 128))
	require.NoError(t, err)
	defer releasePosDeletes(deletes)

	assert.Equal(t, []int64{10, 30}, int64Values(deletes[dataPath]))
	assert.Equal(t, []int64{20}, int64Values(deletes["other/data.parquet"]))
}

func TestPosDeleteAccumulatorFinishAfterReleasePanics(t *testing.T) {
	acc := newPosDeleteAccumulator(t.Context(), nil)
	acc.release()

	assert.PanicsWithValue(t, "position delete accumulator is already finished or released", func() {
		acc.finish()
	})
}

func TestPositionDeleteRowGroupTesterValidatesPhysicalFieldIDs(t *testing.T) {
	t.Parallel()

	filePathField, _ := iceberg.PositionalDeleteSchema.FindFieldByName("file_path")
	posField, _ := iceberg.PositionalDeleteSchema.FindFieldByName("pos")
	dataPath := "mem://bucket/data/needed.parquet"

	tests := []struct {
		name       string
		schema     *arrow.Schema
		wantTester bool
		wantErr    string
	}{
		{
			name:       "canonical IDs",
			schema:     positionDeleteSchemaWithFieldIDs(filePathField.ID, posField.ID),
			wantTester: true,
		},
		{
			name:   "IDs absent",
			schema: positionDeleteSchemaWithoutFieldIDs(),
		},
		{
			name:    "swapped IDs",
			schema:  positionDeleteSchemaWithFieldIDs(posField.ID, filePathField.ID),
			wantErr: `position delete column "file_path" has field ID`,
		},
		{
			name:    "duplicate IDs",
			schema:  positionDeleteSchemaWithFieldIDs(filePathField.ID, filePathField.ID),
			wantErr: "is not unique",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tester, err := newPositionDeleteRowGroupTester(tt.schema, map[string]struct{}{dataPath: {}})
			if tt.wantErr != "" {
				require.ErrorIs(t, err, iceberg.ErrInvalidSchema)
				require.ErrorContains(t, err, tt.wantErr)

				return
			}

			require.NoError(t, err)
			if tt.wantTester {
				require.NotNil(t, tester)
				assert.NotEmpty(t, tester.BloomPreds)
			} else {
				assert.Nil(t, tester)
			}
		})
	}
}

func TestReadDeletesForPathsRejectsSwappedPhysicalFieldIDs(t *testing.T) {
	mem := memory.NewCheckedAllocator(memory.DefaultAllocator)
	ctx := compute.WithAllocator(t.Context(), mem)
	defer mem.AssertSize(t, 0)

	filePathField, _ := iceberg.PositionalDeleteSchema.FindFieldByName("file_path")
	posField, _ := iceberg.PositionalDeleteSchema.FindFieldByName("pos")
	deletePath := "mem://bucket/deletes/swapped-ids.parquet"
	dataPath := "mem://bucket/data/needed.parquet"
	memFS := iceio.NewMemFS()
	writePosDeleteParquetToMemFSWithSchema(t, memFS, deletePath,
		positionDeleteSchemaWithFieldIDs(posField.ID, filePathField.ID),
		`[{"file_path": "`+dataPath+`", "pos": 0}]`)

	deletes, err := readDeletesForPaths(ctx, memFS, newPosDeleteFile(t, deletePath, 1, 128),
		map[string]struct{}{dataPath: {}})
	require.ErrorIs(t, err, iceberg.ErrInvalidSchema)
	assert.Nil(t, deletes)
}

func TestReadDeletesForPathsRejectsDuplicatePhysicalFieldIDs(t *testing.T) {
	mem := memory.NewCheckedAllocator(memory.DefaultAllocator)
	ctx := compute.WithAllocator(t.Context(), mem)
	defer mem.AssertSize(t, 0)

	filePathField, _ := iceberg.PositionalDeleteSchema.FindFieldByName("file_path")
	deletePath := "mem://bucket/deletes/duplicate-ids.parquet"
	dataPath := "mem://bucket/data/needed.parquet"
	memFS := iceio.NewMemFS()
	writePosDeleteParquetToMemFSWithSchema(t, memFS, deletePath,
		positionDeleteSchemaWithFieldIDs(filePathField.ID, filePathField.ID),
		`[{"file_path": "`+dataPath+`", "pos": 0}]`)

	deletes, err := readDeletesForPaths(ctx, memFS, newPosDeleteFile(t, deletePath, 1, 128),
		map[string]struct{}{dataPath: {}})
	require.ErrorIs(t, err, iceberg.ErrInvalidSchema)
	assert.Nil(t, deletes)
}

func positionDeleteSchemaWithFieldIDs(filePathID, posID int) *arrow.Schema {
	return arrow.NewSchema([]arrow.Field{
		{
			Name:     "file_path",
			Type:     arrow.BinaryTypes.String,
			Nullable: false,
			Metadata: arrow.MetadataFrom(map[string]string{ArrowParquetFieldIDKey: strconv.Itoa(filePathID)}),
		},
		{
			Name:     "pos",
			Type:     arrow.PrimitiveTypes.Int64,
			Nullable: false,
			Metadata: arrow.MetadataFrom(map[string]string{ArrowParquetFieldIDKey: strconv.Itoa(posID)}),
		},
	}, nil)
}

func positionDeleteSchemaWithoutFieldIDs() *arrow.Schema {
	return arrow.NewSchema([]arrow.Field{
		{Name: "file_path", Type: arrow.BinaryTypes.String, Nullable: false},
		{Name: "pos", Type: arrow.PrimitiveTypes.Int64, Nullable: false},
	}, nil)
}

func TestGroupPosDeletesByFilePathSupportsStringLayouts(t *testing.T) {
	for _, tc := range []struct {
		name                  string
		filePathCol           func(memory.Allocator) (*arrow.Chunked, func())
		checkComputeAllocator bool
	}{
		{
			name: "string chunks",
			filePathCol: func(mem memory.Allocator) (*arrow.Chunked, func()) {
				chunkA := stringArray(mem, "file-a.parquet", "file-b.parquet")
				chunkB := stringArray(mem, "file-a.parquet", "file-c.parquet")
				chunked := arrow.NewChunked(arrow.BinaryTypes.String, []arrow.Array{chunkA, chunkB})

				return chunked, func() {
					chunked.Release()
					chunkA.Release()
					chunkB.Release()
				}
			},
		},
		{
			name: "large string chunks",
			filePathCol: func(mem memory.Allocator) (*arrow.Chunked, func()) {
				chunkA := largeStringArray(mem, "file-a.parquet", "file-b.parquet")
				chunkB := largeStringArray(mem, "file-a.parquet", "file-c.parquet")
				chunked := arrow.NewChunked(arrow.BinaryTypes.LargeString, []arrow.Array{chunkA, chunkB})

				return chunked, func() {
					chunked.Release()
					chunkA.Release()
					chunkB.Release()
				}
			},
		},
		{
			name:                  "dictionary chunks",
			checkComputeAllocator: true,
			filePathCol: func(mem memory.Allocator) (*arrow.Chunked, func()) {
				dict := stringArray(mem, "file-a.parquet", "file-b.parquet", "file-c.parquet")
				idxA := int32Array(mem, 0, 1)
				idxB := int32Array(mem, 0, 2)
				dictType := &arrow.DictionaryType{
					IndexType: arrow.PrimitiveTypes.Int32,
					ValueType: arrow.BinaryTypes.String,
				}
				chunkA := array.NewDictionaryArray(dictType, idxA, dict)
				chunkB := array.NewDictionaryArray(dictType, idxB, dict)
				chunked := arrow.NewChunked(dictType, []arrow.Array{chunkA, chunkB})

				return chunked, func() {
					chunked.Release()
					chunkA.Release()
					chunkB.Release()
					dict.Release()
					idxA.Release()
					idxB.Release()
				}
			},
		},
		{
			name:                  "large string dictionary chunks",
			checkComputeAllocator: true,
			filePathCol: func(mem memory.Allocator) (*arrow.Chunked, func()) {
				dict := largeStringArray(mem, "file-a.parquet", "file-b.parquet", "file-c.parquet")
				idxA := int32Array(mem, 0, 1)
				idxB := int32Array(mem, 0, 2)
				dictType := &arrow.DictionaryType{
					IndexType: arrow.PrimitiveTypes.Int32,
					ValueType: arrow.BinaryTypes.LargeString,
				}
				chunkA := array.NewDictionaryArray(dictType, idxA, dict)
				chunkB := array.NewDictionaryArray(dictType, idxB, dict)
				chunked := arrow.NewChunked(dictType, []arrow.Array{chunkA, chunkB})

				return chunked, func() {
					chunked.Release()
					chunkA.Release()
					chunkB.Release()
					dict.Release()
					idxA.Release()
					idxB.Release()
				}
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			mem := memory.DefaultAllocator
			ctx := t.Context()
			if tc.checkComputeAllocator {
				checked := memory.NewCheckedAllocator(memory.DefaultAllocator)
				ctx = compute.WithAllocator(ctx, checked)
				defer checked.AssertSize(t, 0)
			}

			filePathCol, releaseFilePathCol := tc.filePathCol(mem)
			defer releaseFilePathCol()
			posA := int64Array(mem, 1, 2)
			defer posA.Release()
			posB := int64Array(mem, 3, 4)
			defer posB.Release()
			posCol := arrow.NewChunked(arrow.PrimitiveTypes.Int64, []arrow.Array{posA, posB})
			defer posCol.Release()

			got, err := groupPosDeletesByFilePath(ctx, filePathCol, posCol)
			require.NoError(t, err)
			defer releasePosDeletes(got)

			assert.Equal(t, []int64{1, 3}, int64Values(got["file-a.parquet"]))
			assert.Equal(t, []int64{2}, int64Values(got["file-b.parquet"]))
			assert.Equal(t, []int64{4}, int64Values(got["file-c.parquet"]))
		})
	}
}

func TestGroupPosDeletesByFilePathPreservesRepeatedPositions(t *testing.T) {
	mem := memory.NewCheckedAllocator(memory.DefaultAllocator)
	defer mem.AssertSize(t, 0)
	ctx := compute.WithAllocator(t.Context(), mem)

	filePathArr := stringArray(mem,
		"file-a.parquet", "file-b.parquet", "file-a.parquet", "file-c.parquet", "file-b.parquet")
	defer filePathArr.Release()
	filePathCol := arrow.NewChunked(arrow.BinaryTypes.String, []arrow.Array{filePathArr})
	defer filePathCol.Release()
	posArr := int64Array(mem, 7, 8, 7, 9, 8)
	defer posArr.Release()
	posCol := arrow.NewChunked(arrow.PrimitiveTypes.Int64, []arrow.Array{posArr})
	defer posCol.Release()

	got, err := groupPosDeletesByFilePath(ctx, filePathCol, posCol)
	require.NoError(t, err)
	defer releasePosDeletes(got)

	assert.Equal(t, []int64{7, 7}, int64Values(got["file-a.parquet"]))
	assert.Equal(t, []int64{8, 8}, int64Values(got["file-b.parquet"]))
	assert.Equal(t, []int64{9}, int64Values(got["file-c.parquet"]))
}

func TestGroupPosDeletesByFilePathHandlesDifferentChunkBoundaries(t *testing.T) {
	mem := memory.NewCheckedAllocator(memory.DefaultAllocator)
	defer mem.AssertSize(t, 0)
	ctx := compute.WithAllocator(t.Context(), mem)

	filePathA := stringArray(mem, "file-a.parquet", "file-b.parquet", "file-a.parquet")
	defer filePathA.Release()
	filePathB := stringArray(mem, "file-c.parquet")
	defer filePathB.Release()
	filePathCol := arrow.NewChunked(arrow.BinaryTypes.String, []arrow.Array{filePathA, filePathB})
	defer filePathCol.Release()

	posA := int64Array(mem, 1, 2)
	defer posA.Release()
	posB := int64Array(mem, 3, 4)
	defer posB.Release()
	posCol := arrow.NewChunked(arrow.PrimitiveTypes.Int64, []arrow.Array{posA, posB})
	defer posCol.Release()

	got, err := groupPosDeletesByFilePath(ctx, filePathCol, posCol)
	require.NoError(t, err)
	defer releasePosDeletes(got)

	assert.Equal(t, []int64{1, 3}, int64Values(got["file-a.parquet"]))
	assert.Equal(t, []int64{2}, int64Values(got["file-b.parquet"]))
	assert.Equal(t, []int64{4}, int64Values(got["file-c.parquet"]))
}

func TestGroupPosDeletesByFilePathSkipsEmptyPositionChunks(t *testing.T) {
	mem := memory.NewCheckedAllocator(memory.DefaultAllocator)
	defer mem.AssertSize(t, 0)
	ctx := compute.WithAllocator(t.Context(), mem)

	filePathArr := stringArray(mem, "file-a.parquet", "file-b.parquet", "file-a.parquet")
	defer filePathArr.Release()
	filePathCol := arrow.NewChunked(arrow.BinaryTypes.String, []arrow.Array{filePathArr})
	defer filePathCol.Release()

	empty := int64Array(mem)
	defer empty.Release()
	posA := int64Array(mem, 10)
	defer posA.Release()
	posB := int64Array(mem, 20, 30)
	defer posB.Release()
	posCol := arrow.NewChunked(arrow.PrimitiveTypes.Int64, []arrow.Array{empty, posA, posB})
	defer posCol.Release()

	got, err := groupPosDeletesByFilePath(ctx, filePathCol, posCol)
	require.NoError(t, err)
	defer releasePosDeletes(got)

	assert.Equal(t, []int64{10, 30}, int64Values(got["file-a.parquet"]))
	assert.Equal(t, []int64{20}, int64Values(got["file-b.parquet"]))
}

func TestGroupPosDeletesByFilePathRejectsMismatchedLengths(t *testing.T) {
	mem := memory.NewCheckedAllocator(memory.DefaultAllocator)
	defer mem.AssertSize(t, 0)
	ctx := compute.WithAllocator(t.Context(), mem)

	filePathArr := stringArray(mem, "file-a.parquet", "file-b.parquet")
	defer filePathArr.Release()
	filePathCol := arrow.NewChunked(arrow.BinaryTypes.String, []arrow.Array{filePathArr})
	defer filePathCol.Release()
	posArr := int64Array(mem, 1)
	defer posArr.Release()
	posCol := arrow.NewChunked(arrow.PrimitiveTypes.Int64, []arrow.Array{posArr})
	defer posCol.Release()

	_, err := groupPosDeletesByFilePath(ctx, filePathCol, posCol)
	require.ErrorIs(t, err, iceberg.ErrInvalidSchema)
	assert.Contains(t, err.Error(), "file_path and pos columns have different lengths: 2 and 1")
}

func TestGroupPosDeletesByFilePathRejectsNegativePositions(t *testing.T) {
	mem := memory.NewCheckedAllocator(memory.DefaultAllocator)
	defer mem.AssertSize(t, 0)
	ctx := compute.WithAllocator(t.Context(), mem)

	filePathArr := stringArray(mem, "file-a.parquet")
	defer filePathArr.Release()
	filePathCol := arrow.NewChunked(arrow.BinaryTypes.String, []arrow.Array{filePathArr})
	defer filePathCol.Release()
	posArr := int64Array(mem, -1)
	defer posArr.Release()
	posCol := arrow.NewChunked(arrow.PrimitiveTypes.Int64, []arrow.Array{posArr})
	defer posCol.Release()

	_, err := groupPosDeletesByFilePath(ctx, filePathCol, posCol)
	require.ErrorIs(t, err, iceberg.ErrInvalidSchema)
	assert.Contains(t, err.Error(), "negative pos -1")
}

func TestGroupPosDeletesByFilePathOwnsResults(t *testing.T) {
	mem := memory.NewCheckedAllocator(memory.DefaultAllocator)
	defer mem.AssertSize(t, 0)
	ctx := compute.WithAllocator(t.Context(), mem)

	filePathArr := stringArray(mem, "file-a.parquet", "file-b.parquet")
	filePathCol := arrow.NewChunked(arrow.BinaryTypes.String, []arrow.Array{filePathArr})
	posArr := int64Array(mem, 3, 5)
	posCol := arrow.NewChunked(arrow.PrimitiveTypes.Int64, []arrow.Array{posArr})
	releaseInputs := func() {
		if filePathCol != nil {
			filePathCol.Release()
			filePathCol = nil
		}
		if filePathArr != nil {
			filePathArr.Release()
			filePathArr = nil
		}
		if posCol != nil {
			posCol.Release()
			posCol = nil
		}
		if posArr != nil {
			posArr.Release()
			posArr = nil
		}
	}
	defer releaseInputs()

	got, err := groupPosDeletesByFilePath(ctx, filePathCol, posCol)
	require.NoError(t, err)
	defer releasePosDeletes(got)
	releaseInputs()

	assert.Equal(t, []int64{3}, int64Values(got["file-a.parquet"]))
	assert.Equal(t, []int64{5}, int64Values(got["file-b.parquet"]))
}

func TestGroupPosDeletesByFilePathHandlesEmptyInput(t *testing.T) {
	mem := memory.NewCheckedAllocator(memory.DefaultAllocator)
	defer mem.AssertSize(t, 0)
	ctx := compute.WithAllocator(t.Context(), mem)

	filePathArr := stringArray(mem)
	defer filePathArr.Release()
	filePathCol := arrow.NewChunked(arrow.BinaryTypes.String, []arrow.Array{filePathArr})
	defer filePathCol.Release()
	posArr := int64Array(mem)
	defer posArr.Release()
	posCol := arrow.NewChunked(arrow.PrimitiveTypes.Int64, []arrow.Array{posArr})
	defer posCol.Release()

	got, err := groupPosDeletesByFilePath(ctx, filePathCol, posCol)
	require.NoError(t, err)
	assert.NotNil(t, got)
	assert.Empty(t, got)
}

func TestGroupPosDeletesByFilePathRejectsUnsupportedFilePathLayout(t *testing.T) {
	for _, tc := range []struct {
		name        string
		filePathCol func(memory.Allocator) (*arrow.Chunked, func())
		want        string
	}{
		{
			name: "unsupported primitive",
			filePathCol: func(mem memory.Allocator) (*arrow.Chunked, func()) {
				arr := int32Array(mem, 1)
				chunked := arrow.NewChunked(arrow.PrimitiveTypes.Int32, []arrow.Array{arr})

				return chunked, func() {
					chunked.Release()
					arr.Release()
				}
			},
			want: "unsupported file_path column type",
		},
		{
			name: "null string",
			filePathCol: func(mem memory.Allocator) (*arrow.Chunked, func()) {
				arr := nullableStringArray(mem, "file-a.parquet", "")
				chunked := arrow.NewChunked(arrow.BinaryTypes.String, []arrow.Array{arr})

				return chunked, func() {
					chunked.Release()
					arr.Release()
				}
			},
			want: "null file_path",
		},
		{
			name: "null dictionary value",
			filePathCol: func(mem memory.Allocator) (*arrow.Chunked, func()) {
				dict := nullableStringArray(mem, "", "file-a.parquet")
				idx := int32Array(mem, 0)
				dictType := &arrow.DictionaryType{
					IndexType: arrow.PrimitiveTypes.Int32,
					ValueType: arrow.BinaryTypes.String,
				}
				arr := array.NewDictionaryArray(dictType, idx, dict)
				chunked := arrow.NewChunked(dictType, []arrow.Array{arr})

				return chunked, func() {
					chunked.Release()
					arr.Release()
					dict.Release()
					idx.Release()
				}
			},
			want: "null file_path dictionary value",
		},
		{
			name: "non-string dictionary",
			filePathCol: func(mem memory.Allocator) (*arrow.Chunked, func()) {
				dict := int32Array(mem, 1)
				idx := int32Array(mem, 0)
				dictType := &arrow.DictionaryType{
					IndexType: arrow.PrimitiveTypes.Int32,
					ValueType: arrow.PrimitiveTypes.Int32,
				}
				arr := array.NewDictionaryArray(dictType, idx, dict)
				chunked := arrow.NewChunked(dictType, []arrow.Array{arr})

				return chunked, func() {
					chunked.Release()
					arr.Release()
					dict.Release()
					idx.Release()
				}
			},
			want: "file_path column is not string",
		},
		{
			name: "string view",
			filePathCol: func(mem memory.Allocator) (*arrow.Chunked, func()) {
				arr := stringViewArray(mem, "file-a.parquet")
				chunked := arrow.NewChunked(arrow.BinaryTypes.StringView, []arrow.Array{arr})

				return chunked, func() {
					chunked.Release()
					arr.Release()
				}
			},
			want: "unsupported file_path column type",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			mem := memory.NewCheckedAllocator(memory.DefaultAllocator)
			defer mem.AssertSize(t, 0)

			ctx := t.Context()
			filePathCol, releaseFilePathCol := tc.filePathCol(mem)
			defer releaseFilePathCol()
			posArr := int64Array(mem, 1)
			defer posArr.Release()
			posCol := arrow.NewChunked(arrow.PrimitiveTypes.Int64, []arrow.Array{posArr})
			defer posCol.Release()

			_, err := groupPosDeletesByFilePath(ctx, filePathCol, posCol)
			require.ErrorIs(t, err, iceberg.ErrInvalidSchema)
			assert.Contains(t, err.Error(), tc.want)
		})
	}
}

func TestGroupPosDeletesByFilePathHonorsCancellation(t *testing.T) {
	mem := memory.NewCheckedAllocator(memory.DefaultAllocator)
	defer mem.AssertSize(t, 0)

	ctx := compute.WithAllocator(t.Context(), mem)
	ctx, cancel := context.WithCancel(ctx)
	cancel()

	filePathArr := stringArray(mem, "file-a.parquet")
	defer filePathArr.Release()
	filePathCol := arrow.NewChunked(arrow.BinaryTypes.String, []arrow.Array{filePathArr})
	defer filePathCol.Release()
	posArr := int64Array(mem, 1)
	defer posArr.Release()
	posCol := arrow.NewChunked(arrow.PrimitiveTypes.Int64, []arrow.Array{posArr})
	defer posCol.Release()

	_, err := groupPosDeletesByFilePath(ctx, filePathCol, posCol)
	require.ErrorIs(t, err, context.Canceled)
}

type cancelAfterErrContext struct {
	context.Context
	remaining int
}

func (c *cancelAfterErrContext) Err() error {
	if c.remaining == 0 {
		return context.Canceled
	}
	c.remaining--

	return nil
}

func TestGroupPosDeletesByFilePathReleasesBuildersAfterCancellation(t *testing.T) {
	mem := memory.NewCheckedAllocator(memory.DefaultAllocator)
	defer mem.AssertSize(t, 0)

	baseCtx := compute.WithAllocator(t.Context(), mem)
	ctx := &cancelAfterErrContext{Context: baseCtx, remaining: 3}
	const numRows = positionalDeleteCancellationCheckInterval + 1

	filePaths := make([]string, numRows)
	positions := make([]int64, numRows)
	for i := range numRows {
		filePaths[i] = "file-a.parquet"
		positions[i] = int64(i)
	}
	filePathArr := stringArray(mem, filePaths...)
	defer filePathArr.Release()
	filePathCol := arrow.NewChunked(arrow.BinaryTypes.String, []arrow.Array{filePathArr})
	defer filePathCol.Release()
	posArr := int64Array(mem, positions...)
	defer posArr.Release()
	posCol := arrow.NewChunked(arrow.PrimitiveTypes.Int64, []arrow.Array{posArr})
	defer posCol.Release()

	_, err := groupPosDeletesByFilePath(ctx, filePathCol, posCol)
	require.ErrorIs(t, err, context.Canceled)
}

func TestGroupPosDeletesByFilePathReleasesBuildersAfterError(t *testing.T) {
	mem := memory.NewCheckedAllocator(memory.DefaultAllocator)
	defer mem.AssertSize(t, 0)
	ctx := compute.WithAllocator(t.Context(), mem)

	dict := nullableStringArray(mem, "file-a.parquet", "")
	defer dict.Release()
	dictType := &arrow.DictionaryType{
		IndexType: arrow.PrimitiveTypes.Int32,
		ValueType: arrow.BinaryTypes.String,
	}
	idxA := int32Array(mem, 0)
	defer idxA.Release()
	filePathA := array.NewDictionaryArray(dictType, idxA, dict)
	defer filePathA.Release()
	idxB := int32Array(mem, 1)
	defer idxB.Release()
	filePathB := array.NewDictionaryArray(dictType, idxB, dict)
	defer filePathB.Release()
	filePathCol := arrow.NewChunked(dictType, []arrow.Array{filePathA, filePathB})
	defer filePathCol.Release()

	posA := int64Array(mem, 1)
	defer posA.Release()
	posB := int64Array(mem, 2)
	defer posB.Release()
	posCol := arrow.NewChunked(arrow.PrimitiveTypes.Int64, []arrow.Array{posA, posB})
	defer posCol.Release()

	_, err := groupPosDeletesByFilePath(ctx, filePathCol, posCol)
	require.ErrorIs(t, err, iceberg.ErrInvalidSchema)
	assert.Contains(t, err.Error(), "null file_path dictionary value")
}

func TestCollectPosDeletePositionsRejectsUnsupportedPosType(t *testing.T) {
	mem := memory.NewCheckedAllocator(memory.DefaultAllocator)
	defer mem.AssertSize(t, 0)

	posArr := int32Array(mem, 1, 2)
	defer posArr.Release()

	posCol := arrow.NewChunked(arrow.PrimitiveTypes.Int32, []arrow.Array{posArr})
	defer posCol.Release()

	_, err := collectPosDeletePositions(positionDeletes{posCol})
	require.ErrorIs(t, err, iceberg.ErrInvalidSchema)
	assert.Contains(t, err.Error(), "unsupported pos column type")
}

func TestCollectPosDeletePositionsRejectsNegativePositions(t *testing.T) {
	mem := memory.NewCheckedAllocator(memory.DefaultAllocator)
	defer mem.AssertSize(t, 0)

	posArr := int64Array(mem, -1)
	defer posArr.Release()

	posCol := arrow.NewChunked(arrow.PrimitiveTypes.Int64, []arrow.Array{posArr})
	defer posCol.Release()

	_, err := collectPosDeletePositions(positionDeletes{posCol})
	require.ErrorIs(t, err, iceberg.ErrInvalidSchema)
	assert.Contains(t, err.Error(), "negative pos -1")
}

func TestCollectPosDeletePositionsRejectsNullPositions(t *testing.T) {
	mem := memory.NewCheckedAllocator(memory.DefaultAllocator)
	defer mem.AssertSize(t, 0)

	bldr := array.NewInt64Builder(mem)
	bldr.AppendNull()
	posArr := bldr.NewInt64Array()
	bldr.Release()
	defer posArr.Release()

	posCol := arrow.NewChunked(arrow.PrimitiveTypes.Int64, []arrow.Array{posArr})
	defer posCol.Release()

	_, err := collectPosDeletePositions(positionDeletes{posCol})
	require.ErrorIs(t, err, iceberg.ErrInvalidSchema)
	assert.Contains(t, err.Error(), "null pos in position delete file")
}

func TestCollectPosDeletePositionsDeduplicatesAcrossChunks(t *testing.T) {
	mem := memory.NewCheckedAllocator(memory.DefaultAllocator)
	defer mem.AssertSize(t, 0)

	first := int64Array(mem, 1, 2, 2)
	defer first.Release()
	second := int64Array(mem, 3, 4)
	defer second.Release()
	third := int64Array(mem, 4, 5)
	defer third.Release()

	firstColumn := arrow.NewChunked(arrow.PrimitiveTypes.Int64, []arrow.Array{first, second})
	defer firstColumn.Release()
	secondColumn := arrow.NewChunked(arrow.PrimitiveTypes.Int64, []arrow.Array{third})
	defer secondColumn.Release()

	got, err := collectPosDeletePositions(positionDeletes{firstColumn, secondColumn})
	require.NoError(t, err)
	assert.Equal(t, set[int64]{1: {}, 2: {}, 3: {}, 4: {}, 5: {}}, got)
}

func TestCollectPosDeletePositionsRejectsNilChunk(t *testing.T) {
	_, err := collectPosDeletePositions(positionDeletes{nil})
	require.ErrorIs(t, err, iceberg.ErrInvalidSchema)
	assert.Contains(t, err.Error(), "nil pos column chunk")
}

func TestReadDeletesRejectsNullPos(t *testing.T) {
	mem := memory.NewCheckedAllocator(memory.DefaultAllocator)
	ctx := compute.WithAllocator(t.Context(), mem)
	defer mem.AssertSize(t, 0)

	fields := PositionalDeleteArrowSchema.Fields()
	fields[1].Nullable = true
	deleteSchema := arrow.NewSchema(fields, nil)
	deletePath := "mem://bucket/deletes/null-pos.parquet"
	dataPath := "mem://bucket/data/data.parquet"

	rec := mustLoadRecordBatchFromJSON(deleteSchema, `[
		{"file_path": "`+dataPath+`", "pos": null}
	]`)
	defer rec.Release()
	tbl := array.NewTableFromRecords(deleteSchema, []arrow.RecordBatch{rec})
	defer tbl.Release()

	memFS := iceio.NewMemFS()
	fw, err := memFS.Create(deletePath)
	require.NoError(t, err)
	require.NoError(t, pqarrow.WriteTable(tbl, fw, rec.NumRows(),
		parquet.NewWriterProperties(parquet.WithStats(true)),
		pqarrow.DefaultWriterProps()))
	require.NoError(t, fw.Close())

	deletes, err := readDeletes(ctx, memFS, newPosDeleteFile(t, deletePath, 1, 128))
	require.ErrorIs(t, err, iceberg.ErrInvalidSchema)
	assert.Nil(t, deletes)
	assert.Contains(t, err.Error(), "null pos in position delete file")
}

// TestProcessPositionalDeletesAcrossBatches is the regression net for the
// positional-delete index bug: processPositionalDeletes applies deletes one Arrow
// batch at a time, but the surviving-row indices must index into the *current
// batch*, not into the whole file. combinePositionalDeletes therefore has to
// rebase the global file positions [start, end) to batch-local coordinates.
//
// Before the rebase, the second and later batches of a single data file passed
// indices >= the batch length into compute.Take and the scan failed with
// "index error: N out of bounds" (N being a multiple of the parquet batch size,
// e.g. 131072). This test feeds two consecutive batches with a delete located in
// the *second* batch — exactly the case the old code got wrong.
func TestProcessPositionalDeletesAcrossBatches(t *testing.T) {
	schema := arrow.NewSchema([]arrow.Field{
		{Name: "val", Type: arrow.PrimitiveTypes.Int64, Nullable: false},
	}, nil)

	// Two batches: global positions 0,1,2 then 3,4. Delete global position 3
	// (the first row of the SECOND batch). The survivors are 0,1,2 and 4.
	batches := []arrow.RecordBatch{
		mustLoadRecordBatchFromJSON(schema, `[{"val": 10}, {"val": 11}, {"val": 12}]`),
		mustLoadRecordBatchFromJSON(schema, `[{"val": 13}, {"val": 14}]`),
	}
	expected := []string{
		`[{"val":10},{"val":11},{"val":12}]`,
		`[{"val":14}]`,
	}

	mem := memory.NewCheckedAllocator(memory.DefaultAllocator)
	ctx := compute.WithAllocator(t.Context(), mem)
	defer mem.AssertSize(t, 0)

	// processPositionalDeletes owns (releases) each input batch it is handed.
	deletes := set[int64]{3: {}}
	processFn := processPositionalDeletes(ctx, deletes, (&rowPositionSource{}).cursor())

	for i, b := range batches {
		out, err := processFn(b)
		require.NoErrorf(t, err, "batch %d must not return an out-of-bounds error", i)

		gotJSON, err := out.MarshalJSON()
		require.NoError(t, err)
		assert.JSONEq(t, expected[i], string(gotJSON))

		out.Release()
	}
}

func TestProcessPositionalDeletesNoOpRetainsBatch(t *testing.T) {
	mem := memory.NewCheckedAllocator(memory.DefaultAllocator)
	defer mem.AssertSize(t, 0)

	ctx := compute.WithAllocator(t.Context(), mem)
	batch := checkedInt64RecordBatch(mem, 0, 1, 2)

	process := processPositionalDeletes(ctx, set[int64]{99: {}}, (&rowPositionSource{}).cursor())
	out, err := process(batch)
	require.NoError(t, err)
	assert.Same(t, batch, out)
	out.Release()
}

func TestProcessPositionalDeletes(t *testing.T) {
	for _, tc := range []struct {
		name      string
		deletes   set[int64]
		spans     []internal.RowGroupSpan
		batches   [][]int64
		expecteds [][]int64
	}{
		{
			name:      "zero deletes",
			deletes:   set[int64]{},
			batches:   [][]int64{{0, 1, 2}},
			expecteds: [][]int64{{0, 1, 2}},
		},
		{
			name:      "first deletion",
			deletes:   set[int64]{0: {}},
			batches:   [][]int64{{0, 1, 2, 3}},
			expecteds: [][]int64{{1, 2, 3}},
		},
		{
			name:      "middle deletion",
			deletes:   set[int64]{2: {}},
			batches:   [][]int64{{0, 1, 2, 3}},
			expecteds: [][]int64{{0, 1, 3}},
		},
		{
			name:      "last deletion",
			deletes:   set[int64]{3: {}},
			batches:   [][]int64{{0, 1, 2, 3}},
			expecteds: [][]int64{{0, 1, 2}},
		},
		{
			name:      "all but last row deleted",
			deletes:   set[int64]{0: {}, 1: {}, 2: {}},
			batches:   [][]int64{{0, 1, 2, 3}},
			expecteds: [][]int64{{3}},
		},
		{
			name:      "batch boundary deletion",
			deletes:   set[int64]{4: {}},
			batches:   [][]int64{{0, 1, 2, 3}, {4, 5, 6}},
			expecteds: [][]int64{{0, 1, 2, 3}, {5, 6}},
		},
		{
			name:      "all rows deleted",
			deletes:   set[int64]{0: {}, 1: {}, 2: {}},
			batches:   [][]int64{{0, 1}, {2}},
			expecteds: [][]int64{{}, {}},
		},
		{
			name:    "pruned row group positions",
			deletes: set[int64]{4: {}},
			spans: []internal.RowGroupSpan{
				{FirstRowPos: 0, NumRows: 2},
				{FirstRowPos: 4, NumRows: 2},
			},
			batches:   [][]int64{{0, 1}, {4, 5}},
			expecteds: [][]int64{{0, 1}, {5}},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			mem := memory.NewCheckedAllocator(memory.DefaultAllocator)
			defer mem.AssertSize(t, 0)
			ctx := compute.WithAllocator(t.Context(), mem)
			process := processPositionalDeletes(ctx, tc.deletes, (&rowPositionSource{spans: tc.spans}).cursor())

			for i, values := range tc.batches {
				batch := checkedInt64RecordBatch(mem, values...)
				out, err := process(batch)
				require.NoErrorf(t, err, "batch %d", i)
				got := out.Column(0).(*array.Int64).Int64Values()
				if len(tc.expecteds[i]) == 0 {
					assert.Empty(t, got)
				} else {
					assert.Equal(t, tc.expecteds[i], got)
				}
				out.Release()
			}
		})
	}
}

func checkedInt64RecordBatch(mem memory.Allocator, values ...int64) arrow.RecordBatch {
	bldr := array.NewInt64Builder(mem)
	defer bldr.Release()
	bldr.AppendValues(values, nil)

	col := bldr.NewArray()
	defer col.Release()

	schema := arrow.NewSchema([]arrow.Field{{
		Name: "value", Type: arrow.PrimitiveTypes.Int64, Nullable: false,
	}}, nil)

	return array.NewRecordBatch(schema, []arrow.Array{col}, int64(len(values)))
}

func stringArray(mem memory.Allocator, values ...string) *array.String {
	bldr := array.NewStringBuilder(mem)
	defer bldr.Release()
	bldr.AppendValues(values, nil)

	return bldr.NewStringArray()
}

func largeStringArray(mem memory.Allocator, values ...string) *array.LargeString {
	bldr := array.NewLargeStringBuilder(mem)
	defer bldr.Release()
	bldr.AppendValues(values, nil)

	return bldr.NewLargeStringArray()
}

func stringViewArray(mem memory.Allocator, values ...string) *array.StringView {
	bldr := array.NewStringViewBuilder(mem)
	defer bldr.Release()
	bldr.AppendValues(values, nil)

	return bldr.NewStringViewArray()
}

func nullableStringArray(mem memory.Allocator, values ...string) *array.String {
	bldr := array.NewStringBuilder(mem)
	defer bldr.Release()
	for _, v := range values {
		if v == "" {
			bldr.AppendNull()
		} else {
			bldr.Append(v)
		}
	}

	return bldr.NewStringArray()
}

func int32Array(mem memory.Allocator, values ...int32) *array.Int32 {
	bldr := array.NewInt32Builder(mem)
	defer bldr.Release()
	bldr.AppendValues(values, nil)

	return bldr.NewInt32Array()
}

func int64Array(mem memory.Allocator, values ...int64) *array.Int64 {
	bldr := array.NewInt64Builder(mem)
	defer bldr.Release()
	bldr.AppendValues(values, nil)

	return bldr.NewInt64Array()
}

func int64Values(chunked *arrow.Chunked) []int64 {
	var out []int64
	for _, chunk := range chunked.Chunks() {
		out = append(out, chunk.(*array.Int64).Int64Values()...)
	}

	return out
}

func TestReadDeletesProjectsLeafColumnsAroundNestedRow(t *testing.T) {
	rowField := arrow.Field{Name: "row", Type: arrow.StructOf(
		arrow.Field{Name: "id", Type: arrow.PrimitiveTypes.Int64},
		arrow.Field{Name: "name", Type: arrow.BinaryTypes.String},
	)}
	filePath := PositionalDeleteArrowSchema.Field(0)
	pos := PositionalDeleteArrowSchema.Field(1)
	for _, tc := range []struct {
		name   string
		fields []arrow.Field
	}{
		{"row first", []arrow.Field{rowField, filePath, pos}},
		{"row between delete columns", []arrow.Field{filePath, rowField, pos}},
		{"row last", []arrow.Field{filePath, pos, rowField}},
		{"reversed delete columns", []arrow.Field{pos, rowField, filePath}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			mem := memory.NewCheckedAllocator(memory.DefaultAllocator)
			ctx := compute.WithAllocator(t.Context(), mem)
			defer mem.AssertSize(t, 0)
			deletePath := "mem://bucket/deletes/nested-row.parquet"
			dataPath := "mem://bucket/data/needed.parquet"
			memFS := iceio.NewMemFS()
			writePosDeleteParquetToMemFSWithSchema(t, memFS, deletePath, arrow.NewSchema(tc.fields, nil), `[
                {"file_path": "`+dataPath+`", "pos": 1, "row": {"id": 10, "name": "a"}},
                {"file_path": "other.parquet", "pos": 2, "row": {"id": 20, "name": "b"}},
                {"file_path": "`+dataPath+`", "pos": 3, "row": {"id": 30, "name": "c"}}
            ]`)
			deletes, err := readDeletes(ctx, memFS, newPosDeleteFile(t, deletePath, 3, 128))
			require.NoError(t, err)
			defer releasePosDeletes(deletes)
			assert.Equal(t, []int64{1, 3}, int64Values(deletes[dataPath]))
		})
	}
}
