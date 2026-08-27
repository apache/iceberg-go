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
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/compute"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/arrow-go/v18/parquet"
	"github.com/apache/arrow-go/v18/parquet/pqarrow"
	"github.com/apache/iceberg-go"
	iceinternal "github.com/apache/iceberg-go/internal"
	iceio "github.com/apache/iceberg-go/io"
	"github.com/apache/iceberg-go/table/internal"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestMakeColEncoderMatchesGenericForNullFastPathTypes(t *testing.T) {
	mem := memory.NewCheckedAllocator(memory.DefaultAllocator)
	defer mem.AssertSize(t, 0)

	tests := []struct {
		name  string
		build func(memory.Allocator, encoderArrayShape) arrow.Array
	}{
		{name: "int8", build: buildInt8Array},
		{name: "int16", build: buildInt16Array},
		{name: "int32", build: buildInt32Array},
		{name: "int64", build: buildInt64Array},
		{name: "float32", build: buildFloat32Array},
		{name: "float64", build: buildFloat64Array},
		{name: "date32", build: buildDate32Array},
		{name: "date64", build: buildDate64Array},
		{name: "time32", build: buildTime32Array},
		{name: "time64", build: buildTime64Array},
		{name: "timestamp", build: buildTimestampArray},
	}

	for _, tt := range tests {
		for _, shape := range encoderArrayShapes {
			t.Run(tt.name+"/"+shape.name, func(t *testing.T) {
				arr := tt.build(mem, shape.shape)
				defer arr.Release()

				encoder := makeColEncoder(arr)

				var fast, generic bytes.Buffer
				for row := range arr.Len() {
					fast.Reset()
					generic.Reset()

					encoder(&fast, row)
					encodeArrowValue(&generic, arr, row)

					assert.Equal(t, generic.Bytes(), fast.Bytes(), "row %d", row)
				}
			})
		}
	}
}

func TestMakeArrowFieldEncoderHonorsNullStructParents(t *testing.T) {
	mem := memory.NewCheckedAllocator(memory.DefaultAllocator)
	defer mem.AssertSize(t, 0)

	childBuilder := array.NewInt64Builder(mem)
	childBuilder.Append(123)
	childBuilder.Append(456)
	child := childBuilder.NewArray()
	childBuilder.Release()
	defer child.Release()
	structArray, err := array.NewStructArrayWithNulls(
		[]arrow.Array{child}, []string{"id"}, memory.NewBufferBytes([]byte{0x02}), 1, 0)
	require.NoError(t, err)
	structType := structArray.DataType().(*arrow.StructType)

	schema := arrow.NewSchema([]arrow.Field{{Name: "person", Type: structType}}, nil)
	record := array.NewRecordBatch(schema, []arrow.Array{structArray}, 2)
	structArray.Release()
	defer record.Release()

	assert.False(t, record.Column(0).(*array.Struct).Field(0).IsNull(0), "child storage must remain valid under the null parent")

	encoder, err := makeArrowFieldEncoder(record, arrowFieldRef{path: []int{0, 0}}, 1, "person.id", "")
	require.NoError(t, err)

	var got bytes.Buffer
	encoder(&got, 0)
	assert.Equal(t, []byte{0}, got.Bytes())

	got.Reset()
	encoder(&got, 1)
	var want bytes.Buffer
	encodeArrowValue(&want, record.Column(0).(*array.Struct).Field(0), 1)
	assert.Equal(t, want.Bytes(), got.Bytes())
}

type encoderArrayShape int

const (
	encoderArrayNullsAtEnds encoderArrayShape = iota
	encoderArrayAllNull
	encoderArrayAllValid
)

var encoderArrayShapes = []struct {
	name  string
	shape encoderArrayShape
}{
	{name: "nulls-at-ends", shape: encoderArrayNullsAtEnds},
	{name: "all-null", shape: encoderArrayAllNull},
	{name: "all-valid", shape: encoderArrayAllValid},
}

type testArrayBuilder[T any] interface {
	Append(T)
	AppendNull()
	AppendNulls(int)
	NewArray() arrow.Array
	Release()
}

func buildFastPathArray[T any](builder testArrayBuilder[T], shape encoderArrayShape, values ...T) arrow.Array {
	defer builder.Release()

	switch shape {
	case encoderArrayNullsAtEnds:
		builder.AppendNull()
		builder.Append(values[0])
		builder.Append(values[1])
		builder.AppendNull()
	case encoderArrayAllNull:
		builder.AppendNulls(4)
	case encoderArrayAllValid:
		for _, v := range values {
			builder.Append(v)
		}
	}

	return builder.NewArray()
}

func buildInt8Array(mem memory.Allocator, shape encoderArrayShape) arrow.Array {
	return buildFastPathArray(array.NewInt8Builder(mem), shape, int8(7), int8(-3), int8(0), int8(4))
}

func buildInt16Array(mem memory.Allocator, shape encoderArrayShape) arrow.Array {
	return buildFastPathArray(array.NewInt16Builder(mem), shape, int16(7), int16(-3), int16(0), int16(4))
}

func buildInt32Array(mem memory.Allocator, shape encoderArrayShape) arrow.Array {
	return buildFastPathArray(array.NewInt32Builder(mem), shape, int32(7), int32(-3), int32(0), int32(4))
}

func buildInt64Array(mem memory.Allocator, shape encoderArrayShape) arrow.Array {
	return buildFastPathArray(array.NewInt64Builder(mem), shape, int64(7), int64(-3), int64(0), int64(4))
}

func buildFloat32Array(mem memory.Allocator, shape encoderArrayShape) arrow.Array {
	return buildFastPathArray(array.NewFloat32Builder(mem), shape, float32(7.5), float32(-3.25), float32(0), float32(4))
}

func buildFloat64Array(mem memory.Allocator, shape encoderArrayShape) arrow.Array {
	return buildFastPathArray(array.NewFloat64Builder(mem), shape, 7.5, -3.25, 0, 4)
}

func buildDate32Array(mem memory.Allocator, shape encoderArrayShape) arrow.Array {
	return buildFastPathArray(array.NewDate32Builder(mem), shape, arrow.Date32(7), arrow.Date32(-3), arrow.Date32(0), arrow.Date32(4))
}

func buildDate64Array(mem memory.Allocator, shape encoderArrayShape) arrow.Array {
	return buildFastPathArray(array.NewDate64Builder(mem), shape, arrow.Date64(7), arrow.Date64(-3), arrow.Date64(0), arrow.Date64(4))
}

func buildTime32Array(mem memory.Allocator, shape encoderArrayShape) arrow.Array {
	return buildFastPathArray(
		array.NewTime32Builder(mem, arrow.FixedWidthTypes.Time32s.(*arrow.Time32Type)),
		shape, arrow.Time32(7), arrow.Time32(-3), arrow.Time32(0), arrow.Time32(4))
}

func buildTime64Array(mem memory.Allocator, shape encoderArrayShape) arrow.Array {
	return buildFastPathArray(
		array.NewTime64Builder(mem, arrow.FixedWidthTypes.Time64us.(*arrow.Time64Type)),
		shape, arrow.Time64(7), arrow.Time64(-3), arrow.Time64(0), arrow.Time64(4))
}

func buildTimestampArray(mem memory.Allocator, shape encoderArrayShape) arrow.Array {
	return buildFastPathArray(
		array.NewTimestampBuilder(mem, arrow.FixedWidthTypes.Timestamp_us.(*arrow.TimestampType)),
		shape, arrow.Timestamp(7), arrow.Timestamp(-3), arrow.Timestamp(0), arrow.Timestamp(4))
}

func TestReadAllEqualityDeleteFilesRejectsEmptyEqualityFieldIDs(t *testing.T) {
	schema := iceberg.NewSchema(0,
		iceberg.NestedField{ID: 1, Name: "id", Type: iceberg.PrimitiveTypes.Int64, Required: true},
	)

	builder, err := iceberg.NewDataFileBuilder(
		*iceberg.UnpartitionedSpec, iceberg.EntryContentEqDeletes,
		"mem://default/table/delete/empty-equality-fields.parquet",
		iceberg.ParquetFile, nil, nil, nil, 1, 128,
	)
	require.NoError(t, err)
	deleteFile := builder.EqualityFieldIDs(nil).Build()

	_, err = readAllEqualityDeleteFiles(
		t.Context(),
		iceio.NewMemFS(),
		schema,
		nil,
		[]FileScanTask{{EqualityDeleteFiles: []iceberg.DataFile{deleteFile}}},
		1,
	)
	require.ErrorIs(t, err, ErrEmptyEqualityFieldIDs)
	require.ErrorContains(t, err, "empty-equality-fields.parquet")
}

func TestReadEqualityDeleteFileMatchesMaterializedRead(t *testing.T) {
	t.Parallel()

	tableSchema := iceberg.NewSchema(0,
		iceberg.NestedField{ID: 1, Name: "id", Type: iceberg.PrimitiveTypes.Int64},
		iceberg.NestedField{ID: 2, Name: "name", Type: iceberg.PrimitiveTypes.String},
		iceberg.NestedField{ID: 3, Name: "ignored", Type: iceberg.PrimitiveTypes.String},
	)
	arrowSchema, err := SchemaToArrowSchema(tableSchema, nil, true, false)
	require.NoError(t, err)

	path := filepath.Join(t.TempDir(), "equality-delete.parquet")
	rec, _, err := array.RecordFromJSON(memory.DefaultAllocator, arrowSchema, strings.NewReader(`[
		{"id": 7, "name": "alice", "ignored": "a very large value that is not part of the equality key"},
		{"id": null, "name": "bob", "ignored": "another value that should not be read"},
		{"id": 7, "name": null, "ignored": "yet another value that should not be read"}
	]`))
	require.NoError(t, err)
	defer rec.Release()

	tbl := array.NewTableFromRecords(arrowSchema, []arrow.RecordBatch{rec})
	defer tbl.Release()

	f, err := (iceio.LocalFS{}).Create(path)
	require.NoError(t, err)
	require.NoError(t, pqarrow.WriteTable(tbl, f, 2,
		parquet.NewWriterProperties(parquet.WithStats(true)), pqarrow.DefaultWriterProps()))

	info, err := os.Stat(path)
	require.NoError(t, err)
	builder, err := iceberg.NewDataFileBuilder(
		*iceberg.UnpartitionedSpec, iceberg.EntryContentEqDeletes,
		path, iceberg.ParquetFile, nil, nil, nil, 3, info.Size())
	require.NoError(t, err)
	builder.EqualityFieldIDs([]int{1, 2})
	dataFile := builder.Build()

	mem := memory.NewCheckedAllocator(memory.DefaultAllocator)
	ctx := compute.WithAllocator(t.Context(), mem)
	defer mem.AssertSize(t, 0)

	gotKeys, gotNames, err := readEqualityDeleteFile(
		ctx, iceio.LocalFS{}, tableSchema, nil, dataFile, []int{1, 2})
	require.NoError(t, err)

	wantKeys, wantNames, err := readEqualityDeleteFileMaterialized(
		ctx, iceio.LocalFS{}, tableSchema, nil, dataFile, []int{1, 2})
	require.NoError(t, err)

	assert.Equal(t, wantNames, gotNames)
	assert.Equal(t, wantKeys, gotKeys)

	canceledCtx, cancel := context.WithCancel(ctx)
	cancel()
	_, _, err = readEqualityDeleteFile(
		canceledCtx, iceio.LocalFS{}, tableSchema, nil, dataFile, []int{1, 2})
	require.ErrorIs(t, err, context.Canceled)
}

func TestReadEqualityDeleteFilePreservesEmbeddedFieldIDsWhenNamesAreReused(t *testing.T) {
	t.Parallel()

	oldSchema := iceberg.NewSchema(0,
		iceberg.NestedField{ID: 1, Name: "old_name", Type: iceberg.PrimitiveTypes.Int64},
	)
	tableSchema := iceberg.NewSchema(0,
		iceberg.NestedField{ID: 1, Name: "new_name", Type: iceberg.PrimitiveTypes.Int64},
		iceberg.NestedField{ID: 2, Name: "old_name", Type: iceberg.PrimitiveTypes.Int64},
	)
	arrowSchema, err := SchemaToArrowSchema(oldSchema, nil, true, false)
	require.NoError(t, err)

	path := filepath.Join(t.TempDir(), "equality-delete-rename-reuse.parquet")
	rec, _, err := array.RecordFromJSON(memory.DefaultAllocator, arrowSchema, strings.NewReader(`[
		{"old_name": 7}
	]`))
	require.NoError(t, err)
	defer rec.Release()

	tbl := array.NewTableFromRecords(arrowSchema, []arrow.RecordBatch{rec})
	defer tbl.Release()

	f, err := (iceio.LocalFS{}).Create(path)
	require.NoError(t, err)
	require.NoError(t, pqarrow.WriteTable(tbl, f, 1,
		parquet.NewWriterProperties(parquet.WithStats(true)), pqarrow.DefaultWriterProps()))

	info, err := os.Stat(path)
	require.NoError(t, err)
	builder, err := iceberg.NewDataFileBuilder(
		*iceberg.UnpartitionedSpec, iceberg.EntryContentEqDeletes,
		path, iceberg.ParquetFile, nil, nil, nil, 1, info.Size())
	require.NoError(t, err)
	builder.EqualityFieldIDs([]int{1})
	dataFile := builder.Build()

	mem := memory.NewCheckedAllocator(memory.DefaultAllocator)
	ctx := compute.WithAllocator(t.Context(), mem)
	defer mem.AssertSize(t, 0)

	wantKeys, wantNames, err := readEqualityDeleteFileMaterialized(
		ctx, iceio.LocalFS{}, tableSchema, tableSchema.NameMapping(), dataFile, []int{1})
	require.NoError(t, err)

	gotKeys, gotNames, err := readEqualityDeleteFile(
		ctx, iceio.LocalFS{}, tableSchema, tableSchema.NameMapping(), dataFile, []int{1})
	require.NoError(t, err)

	assert.Equal(t, wantNames, gotNames)
	assert.Equal(t, wantKeys, gotKeys)
}

func readEqualityDeleteFileMaterialized(
	ctx context.Context,
	fs iceio.IO,
	tableSchema *iceberg.Schema,
	nameMapping iceberg.NameMapping,
	dataFile iceberg.DataFile,
	fieldIDs []int,
) (set[string], []string, error) {
	src, err := internal.GetFile(ctx, fs, dataFile, true)
	if err != nil {
		return nil, nil, err
	}

	rdr, err := src.GetReader(ctx)
	if err != nil {
		return nil, nil, err
	}
	defer iceinternal.CheckedClose(rdr, &err)

	tbl, err := rdr.ReadTable(ctx)
	if err != nil {
		return nil, nil, err
	}
	defer tbl.Release()

	if nameMapping == nil {
		nameMapping = tableSchema.NameMapping()
	}
	hasFieldIDs, err := VisitArrowSchema(tbl.Schema(), hasIDs{})
	if err != nil {
		return nil, nil, err
	}

	var fileSchema *iceberg.Schema
	if !hasFieldIDs {
		fileSchema, err = ArrowSchemaToIcebergWithOptions(tbl.Schema(), ArrowToIcebergOptions{
			NameMapping: nameMapping,
			TableSchema: tableSchema,
		})
		if err != nil {
			return nil, nil, err
		}
	}

	var fieldRefsByID arrowFieldRefsByID
	if hasFieldIDs {
		fieldRefsByID = indexArrowFieldsByMetadata(tbl.Schema())
	} else {
		fieldRefsByID = indexArrowFields(fileSchema)
	}

	colNames := make([]string, len(fieldIDs))
	fieldRefs := make([]arrowFieldRef, len(fieldIDs))
	for i, fieldID := range fieldIDs {
		name, ok := tableSchema.FindColumnName(fieldID)
		if !ok {
			return nil, nil, fmt.Errorf("equality delete field ID %d not found in table schema for %s", fieldID, dataFile.FilePath())
		}

		ref, err := resolveArrowField(fieldRefsByID, fieldID, name, dataFile.FilePath())
		if err != nil {
			return nil, nil, err
		}
		colNames[i] = name
		fieldRefs[i] = ref
	}

	keys := make(set[string])
	var keyBuf bytes.Buffer
	tr := array.NewTableReader(tbl, tbl.NumRows())
	defer tr.Release()
	for tr.Next() {
		rec := tr.RecordBatch()
		encoders := make([]colEncoder, len(fieldRefs))
		for i, ref := range fieldRefs {
			encoders[i], err = makeArrowFieldEncoder(rec, ref, fieldIDs[i], colNames[i], dataFile.FilePath())
			if err != nil {
				return nil, nil, err
			}
		}

		for row := range int(rec.NumRows()) {
			keyBuf.Reset()
			for _, enc := range encoders {
				enc(&keyBuf, row)
			}
			keys[keyBuf.String()] = struct{}{}
		}
	}

	return keys, colNames, nil
}

func TestProcessEqualityDeletesUsesStructuralFieldPaths(t *testing.T) {
	builder := array.NewInt64Builder(memory.DefaultAllocator)
	builder.Append(1)
	first := builder.NewArray()
	builder.Append(2)
	second := builder.NewArray()
	builder.Release()
	var key bytes.Buffer
	encodeArrowValue(&key, first, 0)

	schema := arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int64},
		{Name: "id", Type: arrow.PrimitiveTypes.Int64},
	}, nil)
	record := array.NewRecordBatch(schema, []arrow.Array{first, second}, 1)
	first.Release()
	second.Release()

	fileSchema := iceberg.NewSchema(0,
		iceberg.NestedField{ID: 1, Name: "left", Type: iceberg.PrimitiveTypes.Int64},
		iceberg.NestedField{ID: 2, Name: "right", Type: iceberg.PrimitiveTypes.Int64},
	)
	process, err := processEqualityDeletesColumnarForFile(context.Background(), []*equalityDeleteSet{{
		keys:     set[string]{key.String(): {}},
		fieldIDs: []int{1},
		colNames: []string{"id"},
	}}, fileSchema, "data.parquet")
	require.NoError(t, err)

	result, err := process(record)
	require.NoError(t, err)
	assert.Zero(t, result.NumRows())
	result.Release()
}

func TestProcessEqualityDeletesRejectsMismatchedFieldMetadata(t *testing.T) {
	process, err := processEqualityDeletesColumnarForFile(context.Background(), []*equalityDeleteSet{{
		keys:     make(set[string]),
		fieldIDs: []int{1},
		colNames: []string{"id", "other"},
	}}, iceberg.NewSchema(0), "data.parquet")

	require.ErrorIs(t, err, iceberg.ErrInvalidArgument)
	assert.Nil(t, process)
}

func TestResolveArrowFieldUsesFieldIDBeforeName(t *testing.T) {
	schema := iceberg.NewSchema(0,
		iceberg.NestedField{ID: 1, Name: "id", Type: iceberg.PrimitiveTypes.Int64},
		iceberg.NestedField{ID: 2, Name: "renamed", Type: iceberg.PrimitiveTypes.Int64},
		iceberg.NestedField{ID: 4, Name: "record", Type: &iceberg.StructType{FieldList: []iceberg.NestedField{
			{ID: 3, Name: "value", Type: iceberg.PrimitiveTypes.Int64},
		}}},
	)

	refs := indexArrowFields(schema)
	ref, err := resolveArrowField(refs, 2, "id", "data.parquet")
	require.NoError(t, err)
	assert.Equal(t, []int{1}, ref.path)

	ref, err = resolveArrowField(refs, 3, "record.value", "data.parquet")
	require.NoError(t, err)
	assert.Equal(t, []int{2, 0}, ref.path)
}

func TestResolveArrowFieldRejectsAmbiguousOrMismatchedIDs(t *testing.T) {
	tests := []struct {
		name      string
		schema    *iceberg.Schema
		fieldID   int
		fieldName string
		wantPath  []int
		wantErr   error
	}{
		{
			name: "renamed field uses matching ID",
			schema: iceberg.NewSchema(0, iceberg.NestedField{
				ID: 7, Name: "renamed", Type: iceberg.PrimitiveTypes.Int64,
			}),
			fieldID: 7, fieldName: "old_name", wantPath: []int{0},
		},
		{
			name: "missing ID is rejected even if a name matches",
			schema: iceberg.NewSchema(0, iceberg.NestedField{
				ID: 2, Name: "id", Type: iceberg.PrimitiveTypes.Int64,
			}),
			fieldID: 1, fieldName: "id", wantErr: errors.New("not found"),
		},
		{
			name: "literal dotted name is distinct from nested path",
			schema: iceberg.NewSchema(0,
				iceberg.NestedField{ID: 1, Name: "user.id", Type: iceberg.PrimitiveTypes.Int64},
				iceberg.NestedField{ID: 2, Name: "user", Type: &iceberg.StructType{FieldList: []iceberg.NestedField{
					{ID: 3, Name: "id", Type: iceberg.PrimitiveTypes.Int64},
				}}},
			),
			fieldID: 1, fieldName: "user.id", wantPath: []int{0},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			ref, err := resolveArrowField(indexArrowFields(test.schema), test.fieldID, test.fieldName, "data.parquet")
			if test.wantErr != nil {
				require.Error(t, err)
				if test.wantErr == ErrAmbiguousEqualityColumn {
					assert.ErrorIs(t, err, test.wantErr)
				}
				assert.Contains(t, err.Error(), test.wantErr.Error())
				assert.Contains(t, err.Error(), "data.parquet")

				return
			}

			require.NoError(t, err)
			assert.Equal(t, test.wantPath, ref.path)
		})
	}
}

func TestResolveArrowFieldRejectsDuplicateMetadataIDs(t *testing.T) {
	schema := arrow.NewSchema([]arrow.Field{
		{Name: "left", Type: arrow.PrimitiveTypes.Int64, Metadata: arrow.MetadataFrom(map[string]string{ArrowParquetFieldIDKey: "1"})},
		{Name: "right", Type: arrow.PrimitiveTypes.Int64, Metadata: arrow.MetadataFrom(map[string]string{ArrowParquetFieldIDKey: "1"})},
	}, nil)

	_, err := resolveArrowField(indexArrowFieldsByMetadata(schema), 1, "id", "data.parquet")
	require.ErrorIs(t, err, ErrAmbiguousEqualityColumn)
	require.ErrorContains(t, err, "data.parquet")
}

func TestBuildEqualityDeleteSetsPerTaskSharesIdenticalSets(t *testing.T) {
	deleteA := newEqualityDeleteSetAssemblyTestFile(t, "delete-a.parquet", []int{1})
	deleteB := newEqualityDeleteSetAssemblyTestFile(t, "delete-b.parquet", []int{1})
	deleteC := newEqualityDeleteSetAssemblyTestFile(t, "delete-c.parquet", []int{1})

	perFile := map[string]*equalityDeleteFileSet{
		deleteA.FilePath(): newEqualityDeleteFileSet(0, &equalityDeleteSet{
			keys:     set[string]{"a": {}},
			fieldIDs: []int{1},
			colNames: []string{"id"},
		}),
		deleteB.FilePath(): newEqualityDeleteFileSet(1, &equalityDeleteSet{
			keys:     set[string]{"b": {}},
			fieldIDs: []int{1},
			colNames: []string{"id"},
		}),
		deleteC.FilePath(): newEqualityDeleteFileSet(2, &equalityDeleteSet{
			keys:     set[string]{"c": {}},
			fieldIDs: []int{1},
			colNames: []string{"id"},
		}),
	}

	tasks := []FileScanTask{
		{EqualityDeleteFiles: []iceberg.DataFile{deleteA}},
		{EqualityDeleteFiles: []iceberg.DataFile{deleteA}},
		{EqualityDeleteFiles: []iceberg.DataFile{deleteA, deleteB}},
		{EqualityDeleteFiles: []iceberg.DataFile{deleteB, deleteA}},
		{EqualityDeleteFiles: []iceberg.DataFile{deleteA, deleteB, deleteA}},
		{EqualityDeleteFiles: []iceberg.DataFile{deleteA, deleteC}},
	}

	perTask := buildEqualityDeleteSetsPerTask(tasks, perFile)
	require.Len(t, perTask, len(tasks))

	assert.Same(t, perFile[deleteA.FilePath()].equalityDeleteSet, perTask[0][0])
	assert.Same(t, perTask[0][0], perTask[1][0])
	assert.Same(t, perTask[2][0], perTask[3][0])
	assert.Same(t, perTask[2][0], perTask[4][0])
	assert.NotSame(t, perTask[2][0], perTask[5][0])
	assert.Equal(t, set[string]{"a": {}, "b": {}}, perTask[2][0].keys)
	assert.Equal(t, set[string]{"a": {}, "c": {}}, perTask[5][0].keys)
}

func TestBuildEqualityDeleteSetsPerTaskKeepsFieldGroupsSeparate(t *testing.T) {
	deleteID := newEqualityDeleteSetAssemblyTestFile(t, "delete-id.parquet", []int{1})
	deleteCategory := newEqualityDeleteSetAssemblyTestFile(t, "delete-category.parquet", []int{2})

	perFile := map[string]*equalityDeleteFileSet{
		deleteID.FilePath(): newEqualityDeleteFileSet(0, &equalityDeleteSet{
			keys:     set[string]{"id": {}},
			fieldIDs: []int{1},
			colNames: []string{"id"},
		}),
		deleteCategory.FilePath(): newEqualityDeleteFileSet(1, &equalityDeleteSet{
			keys:     set[string]{"category": {}},
			fieldIDs: []int{2},
			colNames: []string{"category"},
		}),
	}

	perTask := buildEqualityDeleteSetsPerTask([]FileScanTask{{
		EqualityDeleteFiles: []iceberg.DataFile{deleteID, deleteCategory},
	}}, perFile)
	require.Len(t, perTask[0], 2)

	setsByFieldID := make(map[int]*equalityDeleteSet)
	for _, deleteSet := range perTask[0] {
		setsByFieldID[deleteSet.fieldIDs[0]] = deleteSet
	}
	assert.Same(t, perFile[deleteID.FilePath()].equalityDeleteSet, setsByFieldID[1])
	assert.Same(t, perFile[deleteCategory.FilePath()].equalityDeleteSet, setsByFieldID[2])
}

func newEqualityDeleteSetAssemblyTestFile(
	t *testing.T,
	path string,
	fieldIDs []int,
) iceberg.DataFile {
	t.Helper()

	builder, err := iceberg.NewDataFileBuilder(
		*iceberg.UnpartitionedSpec,
		iceberg.EntryContentEqDeletes,
		path,
		iceberg.ParquetFile,
		nil,
		nil,
		nil,
		1,
		128,
	)
	require.NoError(t, err)

	return builder.EqualityFieldIDs(fieldIDs).Build()
}
