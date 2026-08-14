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

package internal

import (
	"fmt"
	"slices"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/geoarrow/geoarrow-go"
	"github.com/stretchr/testify/require"
)

func TestNormalizeWKBArray(t *testing.T) {
	typeDef := geoarrow.NewWKBType(geoarrow.WKBWithBinaryStorage())
	builder := array.NewBinaryBuilder(memory.DefaultAllocator, arrow.BinaryTypes.Binary)
	builder.Append(newWKBBuilder(wkbPoint|ewkbFlagSRID).u32(4326).f64(1, 2).bytes())
	builder.Append(newWKBBuilder(wkbPointZ).f64(3, 4, 5).bytes())
	builder.AppendNull()
	storage := builder.NewArray()
	builder.Release()
	ext := array.NewExtensionArrayWithStorage(typeDef, storage).(array.ExtensionArray)
	storage.Release()
	defer ext.Release()

	normalized, changed, err := normalizeWKBArray(ext, memory.DefaultAllocator)
	require.NoError(t, err)
	require.True(t, changed)
	defer normalized.Release()

	arr := normalized.(*geoarrow.WKBArray)
	require.Equal(t, newWKBBuilder(wkbPoint).f64(1, 2).bytes(), []byte(arr.Value(0)))
	require.Equal(t, newWKBBuilder(wkbPointZ).f64(3, 4, 5).bytes(), []byte(arr.Value(1)))
	require.True(t, arr.IsNull(2))
}

func TestNormalizeWKBArrayUsesProvidedAllocator(t *testing.T) {
	mem := memory.NewCheckedAllocator(memory.DefaultAllocator)
	defer mem.AssertSize(t, 0)

	typeDef := geoarrow.NewWKBType(geoarrow.WKBWithBinaryStorage())
	builder := array.NewBinaryBuilder(mem, arrow.BinaryTypes.Binary)
	builder.Append(newWKBBuilder(wkbPoint|ewkbFlagSRID).u32(4326).f64(1, 2).bytes())
	storage := builder.NewArray()
	builder.Release()
	ext := array.NewExtensionArrayWithStorage(typeDef, storage).(array.ExtensionArray)
	storage.Release()
	defer ext.Release()

	normalized, changed, err := normalizeWKBArray(ext, mem)
	require.NoError(t, err)
	require.True(t, changed)
	require.NotZero(t, mem.CurrentAlloc())
	normalized.Release()
}

func TestNormalizeNestedArray(t *testing.T) {
	mem := memory.DefaultAllocator
	typeDef := geoarrow.NewWKBType(geoarrow.WKBWithBinaryStorage())
	ewkb := newWKBBuilder(wkbPoint|ewkbFlagSRID).u32(4326).f64(1, 2).bytes()
	iso := newWKBBuilder(wkbPoint).f64(3, 4).bytes()

	geo := newGeoWKBArray(t, mem, typeDef, ewkb, iso)
	structArray, err := array.NewStructArrayWithFields(
		[]arrow.Array{geo},
		[]arrow.Field{{Name: "geom", Type: typeDef, Nullable: true}},
	)
	require.NoError(t, err)
	geo.Release()

	listArray := newGeoListArray(t, mem, typeDef, false, ewkb, iso)
	largeListArray := newGeoListArray(t, mem, typeDef, true, ewkb, iso)
	fixedListArray := newGeoFixedSizeListArray(t, mem, typeDef, ewkb, iso)
	mapArray := newGeoMapArray(t, mem, typeDef, ewkb, iso)
	deepArray, err := array.NewStructArrayWithFields(
		[]arrow.Array{listArray},
		[]arrow.Field{{Name: "nested", Type: listArray.DataType(), Nullable: true}},
	)
	require.NoError(t, err)

	tests := []struct {
		name string
		arr  arrow.Array
	}{
		{name: "struct", arr: structArray},
		{name: "list", arr: listArray},
		{name: "large list", arr: largeListArray},
		{name: "fixed size list", arr: fixedListArray},
		{name: "map", arr: mapArray},
		{name: "deep nesting", arr: deepArray},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			normalized, changed, err := normalizeNestedArray(tt.arr, mem)
			require.NoError(t, err)
			require.True(t, changed)
			defer normalized.Release()
			assertNoEWKB(t, normalized)
		})
	}

	structArray.Release()
	listArray.Release()
	largeListArray.Release()
	fixedListArray.Release()
	mapArray.Release()
	deepArray.Release()
}

func TestNormalizeNestedArrayPreservesSliceAndNulls(t *testing.T) {
	mem := memory.NewCheckedAllocator(memory.DefaultAllocator)
	defer mem.AssertSize(t, 0)
	typeDef := geoarrow.NewWKBType(geoarrow.WKBWithBinaryStorage())
	ewkb := newWKBBuilder(wkbPoint|ewkbFlagSRID).u32(4326).f64(1, 2).bytes()
	iso := newWKBBuilder(wkbPoint).f64(3, 4).bytes()

	builder := array.NewListBuilder(mem, arrow.BinaryTypes.Binary)
	values := builder.ValueBuilder().(*array.BinaryBuilder)
	builder.Append(true)
	values.Append(iso)
	builder.AppendNull()
	builder.Append(true)
	values.Append(ewkb)
	raw := builder.NewArray()
	builder.Release()
	geo := newGeoWKBArray(t, mem, typeDef, iso, ewkb)
	data := array.NewData(arrow.ListOf(typeDef), raw.Len(), raw.Data().Buffers(),
		[]arrow.ArrayData{geo.Data()}, raw.Data().NullN(), raw.Data().Offset())
	base := array.NewListData(data)
	data.Release()
	raw.Release()
	geo.Release()
	defer base.Release()

	sliced := array.NewSlice(base, 1, 3)
	defer sliced.Release()
	normalized, changed, err := normalizeNestedArray(sliced, mem)
	require.NoError(t, err)
	require.True(t, changed)
	defer normalized.Release()

	require.Zero(t, normalized.Data().Offset())
	require.True(t, normalized.IsNull(0))
	require.True(t, normalized.IsValid(1))
	storage := normalized.(*array.List).ListValues().(array.ExtensionArray).Storage().(wkbStorage)
	require.Equal(t, newWKBBuilder(wkbPoint).f64(1, 2).bytes(), storage.Value(0))
	assertNoEWKB(t, normalized)
}

func TestNormalizeNestedArrayIgnoresValuesOutsideListSlice(t *testing.T) {
	mem := memory.NewCheckedAllocator(memory.DefaultAllocator)
	defer mem.AssertSize(t, 0)
	typeDef := geoarrow.NewWKBType(geoarrow.WKBWithBinaryStorage())
	malformedEWKB := newWKBBuilder(wkbPoint | ewkbFlagSRID).u32(4326).bytes()
	iso := newWKBBuilder(wkbPoint).f64(3, 4).bytes()

	builder := array.NewListBuilder(mem, arrow.BinaryTypes.Binary)
	values := builder.ValueBuilder().(*array.BinaryBuilder)
	builder.Append(true)
	values.Append(malformedEWKB)
	builder.Append(true)
	values.Append(iso)
	raw := builder.NewArray()
	builder.Release()
	geo := newGeoWKBArray(t, mem, typeDef, malformedEWKB, iso)
	data := array.NewData(arrow.ListOf(typeDef), raw.Len(), raw.Data().Buffers(),
		[]arrow.ArrayData{geo.Data()}, raw.Data().NullN(), raw.Data().Offset())
	base := array.NewListData(data)
	data.Release()
	raw.Release()
	geo.Release()
	defer base.Release()

	sliced := array.NewSlice(base, 1, 2)
	defer sliced.Release()
	normalized, changed, err := normalizeNestedArray(sliced, mem)
	require.NoError(t, err)
	require.False(t, changed)
	require.Nil(t, normalized)
}

func TestNormalizeNestedArrayPreservesStructSlice(t *testing.T) {
	mem := memory.NewCheckedAllocator(memory.DefaultAllocator)
	defer mem.AssertSize(t, 0)
	typeDef := geoarrow.NewWKBType(geoarrow.WKBWithBinaryStorage())
	ewkb := newWKBBuilder(wkbPoint|ewkbFlagSRID).u32(4326).f64(1, 2).bytes()
	iso := newWKBBuilder(wkbPoint).f64(3, 4).bytes()
	geo := newGeoWKBArray(t, mem, typeDef, iso, ewkb, iso)
	valuesBuilder := array.NewInt32Builder(mem)
	valuesBuilder.AppendValues([]int32{1, 2, 3}, nil)
	values := valuesBuilder.NewArray()
	valuesBuilder.Release()

	input, err := array.NewStructArrayWithFields(
		[]arrow.Array{geo, values},
		[]arrow.Field{
			{Name: "geom", Type: typeDef, Nullable: true},
			{Name: "value", Type: arrow.PrimitiveTypes.Int32, Nullable: false},
		},
	)
	require.NoError(t, err)
	geo.Release()
	values.Release()
	defer input.Release()

	sliced := array.NewSlice(input, 1, 3)
	defer sliced.Release()
	normalized, changed, err := normalizeNestedArray(sliced, mem)
	require.NoError(t, err)
	require.True(t, changed)
	defer normalized.Release()

	result := normalized.(*array.Struct)
	require.Equal(t, 2, result.Len())
	require.Zero(t, result.Data().Offset())
	require.True(t, result.IsValid(0))
	require.True(t, result.IsValid(1))
	storage := result.Field(0).(array.ExtensionArray).Storage().(wkbStorage)
	require.Equal(t, newWKBBuilder(wkbPoint).f64(1, 2).bytes(), storage.Value(0))
	require.Equal(t, iso, storage.Value(1))
	assertNoEWKB(t, result)
}

func TestNormalizeNestedArrayIgnoresStructNullChild(t *testing.T) {
	mem := memory.NewCheckedAllocator(memory.DefaultAllocator)
	defer mem.AssertSize(t, 0)
	typeDef := geoarrow.NewWKBType(geoarrow.WKBWithBinaryStorage())
	malformedEWKB := newWKBBuilder(wkbPoint | ewkbFlagSRID).u32(4326).bytes()
	ewkb := newWKBBuilder(wkbPoint|ewkbFlagSRID).u32(4326).f64(1, 2).bytes()
	geo := newGeoWKBArray(t, mem, typeDef, malformedEWKB, ewkb)
	validity := memory.NewBufferBytes([]byte{0b00000010})

	input, err := array.NewStructArrayWithFieldsAndNulls(
		[]arrow.Array{geo},
		[]arrow.Field{{Name: "geom", Type: typeDef, Nullable: true}},
		validity, 1, 0,
	)
	require.NoError(t, err)
	validity.Release()
	geo.Release()
	defer input.Release()

	normalized, changed, err := normalizeNestedArray(input, mem)
	require.NoError(t, err)
	require.True(t, changed)
	defer normalized.Release()

	result := normalized.(*array.Struct)
	require.True(t, result.IsNull(0))
	require.True(t, result.IsValid(1))
	storage := result.Field(0).(array.ExtensionArray).Storage().(wkbStorage)
	require.Equal(t, malformedEWKB, storage.Value(0))
	require.Equal(t, newWKBBuilder(wkbPoint).f64(1, 2).bytes(), storage.Value(1))
}

func TestNormalizeNestedArrayIgnoresNullListChild(t *testing.T) {
	mem := memory.NewCheckedAllocator(memory.DefaultAllocator)
	defer mem.AssertSize(t, 0)
	typeDef := geoarrow.NewWKBType(geoarrow.WKBWithBinaryStorage())
	malformedEWKB := newWKBBuilder(wkbPoint | ewkbFlagSRID).u32(4326).bytes()
	ewkb := newWKBBuilder(wkbPoint|ewkbFlagSRID).u32(4326).f64(1, 2).bytes()
	geo := newGeoWKBArray(t, mem, typeDef, malformedEWKB, ewkb)
	validity := memory.NewBufferBytes([]byte{0b00000010})
	offsets := memory.NewBufferBytes(arrow.Int32Traits.CastToBytes([]int32{0, 1, 2}))
	data := array.NewData(arrow.ListOf(typeDef), 2,
		[]*memory.Buffer{validity, offsets}, []arrow.ArrayData{geo.Data()}, 1, 0)
	base := array.NewListData(data)
	data.Release()
	validity.Release()
	offsets.Release()
	geo.Release()
	defer base.Release()

	normalized, changed, err := normalizeNestedArray(base, mem)
	require.NoError(t, err)
	require.True(t, changed)
	defer normalized.Release()

	result := normalized.(*array.List)
	require.Zero(t, result.Data().Offset())
	require.True(t, result.IsNull(0))
	require.True(t, result.IsValid(1))
	start, end := result.ValueOffsets(0)
	require.Equal(t, int64(0), start)
	require.Equal(t, int64(1), end)
	start, end = result.ValueOffsets(1)
	require.Equal(t, int64(1), start)
	require.Equal(t, int64(2), end)
	storage := result.ListValues().(array.ExtensionArray).Storage().(wkbStorage)
	require.Equal(t, malformedEWKB, storage.Value(0))
	require.Equal(t, newWKBBuilder(wkbPoint).f64(1, 2).bytes(), storage.Value(1))
}

func TestNormalizeNestedArrayIgnoresAncestorNullChild(t *testing.T) {
	mem := memory.NewCheckedAllocator(memory.DefaultAllocator)
	defer mem.AssertSize(t, 0)
	typeDef := geoarrow.NewWKBType(geoarrow.WKBWithBinaryStorage())
	malformedEWKB := newWKBBuilder(wkbPoint | ewkbFlagSRID).u32(4326).bytes()
	ewkb := newWKBBuilder(wkbPoint|ewkbFlagSRID).u32(4326).f64(1, 2).bytes()
	geo := newGeoWKBArray(t, mem, typeDef, malformedEWKB, ewkb)
	offsets := memory.NewBufferBytes(arrow.Int32Traits.CastToBytes([]int32{0, 1, 2}))
	listData := array.NewData(arrow.ListOf(typeDef), 2,
		[]*memory.Buffer{nil, offsets}, []arrow.ArrayData{geo.Data()}, 0, 0)
	list := array.NewListData(listData)
	listData.Release()
	offsets.Release()
	geo.Release()

	validity := memory.NewBufferBytes([]byte{0b00000010})
	input, err := array.NewStructArrayWithFieldsAndNulls(
		[]arrow.Array{list},
		[]arrow.Field{{Name: "items", Type: list.DataType(), Nullable: true}},
		validity, 1, 0,
	)
	require.NoError(t, err)
	validity.Release()
	list.Release()
	defer input.Release()

	normalized, changed, err := normalizeNestedArray(input, mem)
	require.NoError(t, err)
	require.True(t, changed)
	defer normalized.Release()

	result := normalized.(*array.Struct)
	require.True(t, result.IsNull(0))
	require.True(t, result.IsValid(1))
	values := result.Field(0).(*array.List).ListValues().(array.ExtensionArray).Storage().(wkbStorage)
	require.Equal(t, malformedEWKB, values.Value(0))
	require.Equal(t, newWKBBuilder(wkbPoint).f64(1, 2).bytes(), values.Value(1))
}

func TestNormalizeNestedArrayReusesUnchangedChildren(t *testing.T) {
	mem := memory.DefaultAllocator
	typeDef := geoarrow.NewWKBType(geoarrow.WKBWithBinaryStorage())
	ewkb := newWKBBuilder(wkbPoint|ewkbFlagSRID).u32(4326).f64(1, 2).bytes()
	geo := newGeoWKBArray(t, mem, typeDef, ewkb)
	intsBuilder := array.NewInt32Builder(mem)
	intsBuilder.AppendValues([]int32{1}, nil)
	ints := intsBuilder.NewArray()
	intsBuilder.Release()

	input, err := array.NewStructArrayWithFields(
		[]arrow.Array{geo, ints},
		[]arrow.Field{
			{Name: "geom", Type: typeDef, Nullable: true},
			{Name: "value", Type: arrow.PrimitiveTypes.Int32, Nullable: false},
		},
	)
	require.NoError(t, err)
	geo.Release()
	ints.Release()
	defer input.Release()

	normalized, changed, err := normalizeNestedArray(input, mem)
	require.NoError(t, err)
	require.True(t, changed)
	defer normalized.Release()

	result := normalized.(*array.Struct)
	require.Same(t, input.Field(1).Data(), result.Field(1).Data())
	assertNoEWKB(t, result)
}

func TestNormalizeGeoBatchNormalizesNestedEWKB(t *testing.T) {
	mem := memory.DefaultAllocator
	typeDef := geoarrow.NewWKBType(geoarrow.WKBWithBinaryStorage())
	ewkb := newWKBBuilder(wkbPoint|ewkbFlagSRID).u32(4326).f64(1, 2).bytes()
	list := newGeoListArray(t, mem, typeDef, false, ewkb)
	defer list.Release()

	schema := arrow.NewSchema([]arrow.Field{{Name: "nested", Type: list.DataType(), Nullable: true}}, nil)
	batch := array.NewRecordBatch(schema, []arrow.Array{list}, int64(list.Len()))
	defer batch.Release()

	writer := &ParquetFileWriter{mem: mem, geoNormalizeCols: []int{0}}
	normalized, err := writer.normalizeGeoBatch(batch)
	require.NoError(t, err)
	require.NotSame(t, batch, normalized)
	defer normalized.Release()
	assertNoEWKB(t, normalized.Column(0))
}

func TestCollectGeoNormalizationColumns(t *testing.T) {
	typeDef := geoarrow.NewWKBType(geoarrow.WKBWithBinaryStorage())
	schema := arrow.NewSchema([]arrow.Field{
		{Name: "plain", Type: arrow.PrimitiveTypes.Int32},
		{Name: "struct", Type: arrow.StructOf(arrow.Field{Name: "geom", Type: typeDef})},
		{Name: "list", Type: arrow.ListOf(typeDef)},
		{Name: "map", Type: arrow.MapOf(arrow.BinaryTypes.String, typeDef)},
	}, nil)

	require.Equal(t, []int{1, 2, 3}, collectGeoNormalizationColumns(schema))
}

func TestNormalizeGeoBatchSkipsNonGeoColumns(t *testing.T) {
	mem := memory.DefaultAllocator
	builder := array.NewInt32Builder(mem)
	builder.Append(1)
	values := builder.NewArray()
	builder.Release()
	defer values.Release()

	schema := arrow.NewSchema([]arrow.Field{{Name: "value", Type: arrow.PrimitiveTypes.Int32}}, nil)
	batch := array.NewRecordBatch(schema, []arrow.Array{values}, 1)
	defer batch.Release()

	writer := &ParquetFileWriter{mem: mem}
	normalized, err := writer.normalizeGeoBatch(batch)
	require.NoError(t, err)
	require.Same(t, batch, normalized)
}

func newGeoWKBArray(t *testing.T, mem memory.Allocator, typeDef *geoarrow.WKBType, values ...[]byte) arrow.Array {
	builder := array.NewBinaryBuilder(mem, arrow.BinaryTypes.Binary)
	for _, value := range values {
		if value == nil {
			builder.AppendNull()
		} else {
			builder.Append(value)
		}
	}
	storage := builder.NewArray()
	builder.Release()
	ext := array.NewExtensionArrayWithStorage(typeDef, storage)
	storage.Release()

	return ext
}

func newGeoListArray(t *testing.T, mem memory.Allocator, typeDef *geoarrow.WKBType, large bool, values ...[]byte) arrow.Array {
	if large {
		builder := array.NewLargeListBuilder(mem, arrow.BinaryTypes.Binary)
		items := builder.ValueBuilder().(*array.BinaryBuilder)
		for _, value := range values {
			builder.Append(true)
			items.Append(value)
		}
		raw := builder.NewArray()
		builder.Release()

		return replaceListValues(t, raw, typeDef, values, true)
	}

	builder := array.NewListBuilder(mem, arrow.BinaryTypes.Binary)
	items := builder.ValueBuilder().(*array.BinaryBuilder)
	for _, value := range values {
		builder.Append(true)
		items.Append(value)
	}
	raw := builder.NewArray()
	builder.Release()

	return replaceListValues(t, raw, typeDef, values, false)
}

func newGeoFixedSizeListArray(t *testing.T, mem memory.Allocator, typeDef *geoarrow.WKBType, values ...[]byte) arrow.Array {
	builder := array.NewFixedSizeListBuilder(mem, 1, arrow.BinaryTypes.Binary)
	items := builder.ValueBuilder().(*array.BinaryBuilder)
	for _, value := range values {
		builder.Append(true)
		items.Append(value)
	}
	raw := builder.NewArray()
	builder.Release()

	return replaceListValues(t, raw, typeDef, values, false)
}

func newGeoMapArray(t *testing.T, mem memory.Allocator, typeDef *geoarrow.WKBType, values ...[]byte) arrow.Array {
	builder := array.NewMapBuilder(mem, arrow.BinaryTypes.String, arrow.BinaryTypes.Binary, false)
	keys := builder.KeyBuilder().(*array.StringBuilder)
	items := builder.ItemBuilder().(*array.BinaryBuilder)
	for idx, value := range values {
		builder.Append(true)
		keys.Append(fmt.Sprintf("key-%d", idx))
		items.Append(value)
	}
	raw := builder.NewMapArray()
	builder.Release()
	geo := newGeoWKBArray(t, mem, typeDef, values...)
	entryData := raw.Data().Children()[0]
	entryChildren := slices.Clone(entryData.Children())
	entryChildren[1] = geo.Data()
	newEntryData := array.NewData(entryData.DataType(), entryData.Len(), entryData.Buffers(),
		entryChildren, entryData.NullN(), entryData.Offset())
	mapData := array.NewData(raw.DataType(), raw.Len(), raw.Data().Buffers(),
		[]arrow.ArrayData{newEntryData}, raw.Data().NullN(), raw.Data().Offset())
	result := array.NewMapData(mapData)
	mapData.Release()
	newEntryData.Release()
	geo.Release()
	raw.Release()

	return result
}

func replaceListValues(t *testing.T, raw arrow.Array, typeDef *geoarrow.WKBType, values [][]byte, large bool) arrow.Array {
	t.Helper()
	mem := memory.DefaultAllocator
	geo := newGeoWKBArray(t, mem, typeDef, values...)
	var listType arrow.DataType
	var newArray func(arrow.ArrayData) arrow.Array
	if large {
		listType = arrow.LargeListOf(typeDef)
		newArray = func(data arrow.ArrayData) arrow.Array { return array.NewLargeListData(data) }
	} else if _, ok := raw.(*array.FixedSizeList); ok {
		listType = arrow.FixedSizeListOf(1, typeDef)
		newArray = func(data arrow.ArrayData) arrow.Array { return array.NewFixedSizeListData(data) }
	} else {
		listType = arrow.ListOf(typeDef)
		newArray = func(data arrow.ArrayData) arrow.Array { return array.NewListData(data) }
	}
	data := array.NewData(listType, raw.Len(), raw.Data().Buffers(),
		[]arrow.ArrayData{geo.Data()}, raw.Data().NullN(), raw.Data().Offset())
	result := newArray(data)
	data.Release()
	geo.Release()
	raw.Release()

	return result
}

func assertNoEWKB(t *testing.T, arr arrow.Array) {
	t.Helper()
	switch nested := arr.(type) {
	case array.ExtensionArray:
		storage := nested.Storage().(wkbStorage)
		for idx := range storage.Len() {
			if storage.IsNull(idx) {
				continue
			}
			require.False(t, isEWKB(storage.Value(idx)))
		}
	case *array.Struct:
		for idx := range nested.NumField() {
			assertNoEWKB(t, nested.Field(idx))
		}
	case *array.Map:
		assertNoEWKB(t, nested.ListValues())
	case *array.List:
		assertNoEWKB(t, nested.ListValues())
	case *array.LargeList:
		assertNoEWKB(t, nested.ListValues())
	case *array.FixedSizeList:
		assertNoEWKB(t, nested.ListValues())
	}
}
