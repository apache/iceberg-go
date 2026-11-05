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
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package table

import (
	"bytes"
	"fmt"
	"strings"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/compute"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/iceberg-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func appendEqualityBytesTestValue(t testing.TB, builder array.Builder, value string, valid bool) {
	t.Helper()
	if !valid {
		builder.AppendNull()

		return
	}
	switch b := builder.(type) {
	case *array.StringBuilder:
		b.Append(value)
	case *array.LargeStringBuilder:
		b.Append(value)
	case *array.BinaryBuilder:
		b.Append([]byte(value))
	default:
		t.Fatalf("unexpected Arrow builder %T", builder)
	}
}

func TestProcessEqualityDeletesSingleBytesMatchesGeneric(t *testing.T) {
	values := []string{"", "keep", "embedded\x00null", "東京", "unused", "ignored", "last", "embedded\x00null"}
	valid := []bool{true, true, true, true, false, false, true, true}
	for _, shape := range []struct {
		name    string
		typ     arrow.DataType
		iceType iceberg.Type
	}{
		{name: "string", typ: arrow.BinaryTypes.String, iceType: iceberg.PrimitiveTypes.String},
		{name: "large_string", typ: arrow.BinaryTypes.LargeString, iceType: iceberg.PrimitiveTypes.String},
		{name: "binary", typ: arrow.BinaryTypes.Binary, iceType: iceberg.PrimitiveTypes.Binary},
		{name: "large_binary", typ: arrow.BinaryTypes.LargeBinary, iceType: iceberg.PrimitiveTypes.Binary},
	} {
		for _, offset := range []int{0, 3, 9} {
			for _, tc := range []struct {
				name       string
				deleteRows []int
			}{
				{name: "no_match"},
				{name: "empty_value", deleteRows: []int{0}},
				{name: "embedded_and_unicode", deleteRows: []int{2, 3}},
				{name: "null_and_duplicates", deleteRows: []int{0, 2, 3, 5}},
			} {
				t.Run(fmt.Sprintf("%s/offset=%d/%s", shape.name, offset, tc.name), func(t *testing.T) {
					mem := memory.NewCheckedAllocator(memory.DefaultAllocator)
					defer mem.AssertSize(t, 0)
					ctx := compute.WithAllocator(t.Context(), mem)
					schema := arrow.NewSchema([]arrow.Field{{Name: "value", Type: shape.typ, Nullable: true}}, nil)
					builder := array.NewRecordBuilder(mem, schema)
					for range offset {
						appendEqualityBytesTestValue(t, builder.Field(0), "padding", true)
					}
					for i, value := range values {
						appendEqualityBytesTestValue(t, builder.Field(0), value, valid[i])
					}
					full := builder.NewRecordBatch()
					builder.Release()
					sliced := array.NewSlice(full.Column(0), int64(offset), int64(offset+len(values)))
					full.Release()
					record := array.NewRecordBatch(schema, []arrow.Array{sliced}, int64(len(values)))
					sliced.Release()
					defer record.Release()

					keys := make(set[string], len(tc.deleteRows))
					for _, row := range tc.deleteRows {
						var encoded bytes.Buffer
						encodeArrowValue(&encoded, record.Column(0), row)
						keys[encoded.String()] = struct{}{}
					}
					fastSet := &equalityDeleteSet{keys: keys, fieldIDs: []int{1}, colNames: []string{"value"}}
					genericSet := &equalityDeleteSet{keys: keys, fieldIDs: []int{1}, colNames: []string{"value"}}
					genericSet.singleBytesOnce.Do(func() {})
					fileSchema := iceberg.NewSchema(0, iceberg.NestedField{
						ID: 1, Name: "value", Type: shape.iceType,
					})

					generic, err := processEqualityDeletesColumnarForFile(ctx,
						[]*equalityDeleteSet{genericSet}, fileSchema, "data.parquet")
					require.NoError(t, err)
					fast, err := processEqualityDeletesColumnarForFile(ctx,
						[]*equalityDeleteSet{fastSet}, fileSchema, "data.parquet")
					require.NoError(t, err)
					require.NotNil(t, fastSet.singleBytes, "the raw-value index must be exercised")
					require.Nil(t, genericSet.singleBytes)

					record.Retain()
					want, err := generic(record)
					require.NoError(t, err)
					defer want.Release()
					record.Retain()
					got, err := fast(record)
					require.NoError(t, err)
					defer got.Release()

					assert.True(t, array.RecordEqual(want, got), "generic: %s; fast: %s", want, got)
					if len(tc.deleteRows) == 0 {
						assert.Same(t, record, got)
					}
				})
			}
		}
	}
}

func TestProcessEqualityDeletesSingleBytesInvalidKeysFallBack(t *testing.T) {
	for _, key := range []string{"", "\x01\x00", "\x02\x00\x00\x00\x00", "\x01\x00\x00\x00\x04bad"} {
		t.Run(fmt.Sprintf("%x", key), func(t *testing.T) {
			deleteSet := &equalityDeleteSet{
				keys:     set[string]{key: {}},
				fieldIDs: []int{1},
				colNames: []string{"value"},
			}
			fileSchema := iceberg.NewSchema(0, iceberg.NestedField{
				ID: 1, Name: "value", Type: iceberg.PrimitiveTypes.String,
			})
			_, err := processEqualityDeletesColumnarForFile(t.Context(),
				[]*equalityDeleteSet{deleteSet}, fileSchema, "data.parquet")
			require.NoError(t, err)
			assert.Nil(t, deleteSet.singleBytes)
		})
	}
}

func TestProcessEqualityDeletesNestedStringDoesNotBuildUnusedIndex(t *testing.T) {
	deleteSet := &equalityDeleteSet{
		keys:     set[string]{"\x01\x00\x00\x00\x01x": {}},
		fieldIDs: []int{1},
		colNames: []string{"person.name"},
	}
	fileSchema := iceberg.NewSchema(0, iceberg.NestedField{
		ID: 2, Name: "person",
		Type: &iceberg.StructType{FieldList: []iceberg.NestedField{{
			ID: 1, Name: "name", Type: iceberg.PrimitiveTypes.String,
		}}},
	})
	_, err := processEqualityDeletesColumnarForFile(t.Context(),
		[]*equalityDeleteSet{deleteSet}, fileSchema, "data.parquet")
	require.NoError(t, err)
	assert.Nil(t, deleteSet.singleBytes)
}

func TestProcessEqualityDeletesSingleBytesDictionary(t *testing.T) {
	tests := []struct {
		name       string
		dictionary []string
		indices    string
		offset     int
		key        string
		wantFast   bool
		wantIDs    []int64
	}{
		{
			name: "no_match", dictionary: []string{"apple", "bob", "cherry"}, indices: "[0, 1, 2, 0, 1, 2]",
			key: "\x01\x00\x00\x00\x03zzz", wantFast: true, wantIDs: []int64{1, 2, 3, 4, 5, 6},
		},
		{
			name: "null_index_without_null_entry", dictionary: []string{"apple", "bob"}, indices: "[0, null, 1, null, 0, 1]",
			key: "\x00", wantFast: true, wantIDs: []int64{1, 3, 5, 6},
		},
		{
			name: "empty_dictionary_null_key", dictionary: []string{}, indices: "[null, null, null]",
			key: "\x00", wantFast: true, wantIDs: nil,
		},
		{
			name: "empty_dictionary_without_null_key", dictionary: []string{}, indices: "[null, null, null]",
			key: "\x01\x00\x00\x00\x05apple", wantFast: true, wantIDs: []int64{1, 2, 3},
		},
		{
			name: "sliced", dictionary: []string{"apple", "bob", "cherry"}, indices: "[0, 1, 2, 0, 1, 2]", offset: 2,
			key: "\x01\x00\x00\x00\x05apple", wantFast: true, wantIDs: []int64{3, 5, 6},
		},
		{
			name: "dictionary_larger_than_batch", dictionary: []string{"apple", "bob", "cherry", "date"}, indices: "[0, 3]",
			key: "\x01\x00\x00\x00\x05apple", wantFast: false, wantIDs: []int64{2},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			mem := memory.NewCheckedAllocator(memory.DefaultAllocator)
			defer mem.AssertSize(t, 0)
			ctx := compute.WithAllocator(t.Context(), mem)

			valueBuilder := array.NewStringBuilder(mem)
			valueBuilder.AppendValues(tt.dictionary, nil)
			dictionary := valueBuilder.NewArray()
			valueBuilder.Release()
			defer dictionary.Release()
			indices, _, err := array.FromJSON(mem, arrow.PrimitiveTypes.Int16, strings.NewReader(tt.indices))
			require.NoError(t, err)
			defer indices.Release()
			dictType := &arrow.DictionaryType{IndexType: arrow.PrimitiveTypes.Int16, ValueType: arrow.BinaryTypes.String}
			values := array.NewDictionaryArray(dictType, indices, dictionary)
			defer values.Release()
			idBuilder := array.NewInt64Builder(mem)
			for id := range values.Len() {
				idBuilder.Append(int64(id + 1))
			}
			ids := idBuilder.NewArray()
			idBuilder.Release()
			defer ids.Release()

			schema := arrow.NewSchema([]arrow.Field{
				{Name: "id", Type: arrow.PrimitiveTypes.Int64},
				{Name: "value", Type: dictType, Nullable: true},
			}, nil)
			full := array.NewRecordBatch(schema, []arrow.Array{ids, values}, int64(values.Len()))
			defer full.Release()
			record := full.NewSlice(int64(tt.offset), full.NumRows())
			defer record.Release()

			deleteSet := &equalityDeleteSet{keys: set[string]{tt.key: {}}, fieldIDs: []int{2}, colNames: []string{"value"}}
			fileSchema := iceberg.NewSchema(0,
				iceberg.NestedField{ID: 1, Name: "id", Type: iceberg.PrimitiveTypes.Int64, Required: true},
				iceberg.NestedField{ID: 2, Name: "value", Type: iceberg.PrimitiveTypes.String},
			)
			process, err := processEqualityDeletesColumnarForFile(ctx,
				[]*equalityDeleteSet{deleteSet}, fileSchema, "data.parquet")
			require.NoError(t, err)

			record.Retain()
			got, err := process(record)
			require.NoError(t, err)
			defer got.Release()
			assert.Equal(t, tt.wantIDs, got.Column(0).(*array.Int64).Int64Values())
			if len(tt.wantIDs) == int(record.NumRows()) {
				assert.Same(t, record, got)
			}

			var mask equalityDeleteMask
			defer mask.release()
			assert.Equal(t, tt.wantFast, mask.applySingleBytes(record.Column(1), deleteSet.singleBytes, mem))
		})
	}
}

func TestProcessEqualityDeletesSingleBytesDictionaryAcrossBatches(t *testing.T) {
	const apple, null = "\x01\x00\x00\x00\x05apple", "\x00"
	type batch struct {
		dictionary string
		indices    string
		offset     int
		wantIDs    []int64
	}
	tests := []struct {
		name         string
		dictionaries map[string]string
		// dictionaryOffset slices this many leading values off every dictionary.
		dictionaryOffset int
		sets             [][]string
		batches          []batch
	}{
		{
			name:         "different_dictionary_same_length",
			dictionaries: map[string]string{"a": `["apple", "bob", "cherry"]`, "b": `["bob", "apple", "cherry"]`},
			sets:         [][]string{{apple}},
			batches: []batch{
				{dictionary: "a", indices: "[0, 1]", wantIDs: []int64{2}},
				{dictionary: "b", indices: "[0, 1]", wantIDs: []int64{1}},
			},
		},
		{
			name:         "shorter_dictionary",
			dictionaries: map[string]string{"a": `["apple", "bob", "cherry", "date"]`, "b": `["bob", "cherry", "apple"]`},
			sets:         [][]string{{apple}},
			batches: []batch{
				{dictionary: "a", indices: "[0, 1]", wantIDs: []int64{2}},
				{dictionary: "b", indices: "[0, 2]", wantIDs: []int64{1}},
			},
		},
		{
			name:         "null_index_and_null_entry_with_null_key",
			dictionaries: map[string]string{"a": `["apple", null, "cherry", "date", "egg"]`},
			sets:         [][]string{{null}},
			batches:      []batch{{dictionary: "a", indices: "[0, null, 1, 2]", wantIDs: []int64{1, 4}}},
		},
		{
			name:         "null_index_and_null_entry_without_null_key",
			dictionaries: map[string]string{"a": `["apple", null, "cherry", "date", "egg"]`},
			sets:         [][]string{{apple}},
			batches:      []batch{{dictionary: "a", indices: "[0, null, 1, 2]", wantIDs: []int64{2, 3, 4}}},
		},
		{
			name:         "sliced_over_shared_dictionary",
			dictionaries: map[string]string{"a": `["apple", "bob", "cherry", "date", "egg"]`},
			sets:         [][]string{{apple}},
			batches: []batch{
				{dictionary: "a", indices: "[1, 2, 0, 0, 3, 1]", offset: 3, wantIDs: []int64{5, 6}},
				{dictionary: "a", indices: "[4, 0, 2]", offset: 1, wantIDs: []int64{3}},
			},
		},
		{
			name:             "sliced_dictionary_values",
			dictionaries:     map[string]string{"a": `["zzz", "apple", "bob", "cherry", "date"]`},
			dictionaryOffset: 1,
			sets:             [][]string{{apple}},
			batches:          []batch{{dictionary: "a", indices: "[0, 1]", wantIDs: []int64{2}}},
		},
		{
			// The second set's row 1 is already deleted by the first set, so
			// the second set never looks up apple.
			name:         "delete_sets_share_dictionary",
			dictionaries: map[string]string{"a": `["apple", "bob", "cherry", "date"]`},
			sets:         [][]string{{apple}, {"\x01\x00\x00\x00\x03bob"}},
			batches: []batch{
				{dictionary: "a", indices: "[0, 1]", wantIDs: nil},
				{dictionary: "a", indices: "[1, 0, 2]", wantIDs: []int64{3}},
			},
		},
		{
			// The second batch is long enough for the per-batch entry bitmap,
			// so the third batch must not reuse results from the first.
			name:         "entry_bitmap_between_batches",
			dictionaries: map[string]string{"a": `["apple", "bob", "cherry"]`, "b": `["cherry", "bob", "apple"]`},
			sets:         [][]string{{apple}},
			batches: []batch{
				{dictionary: "a", indices: "[0, 1]", wantIDs: []int64{2}},
				{dictionary: "b", indices: "[0, 1, 2, 0]", wantIDs: []int64{1, 2, 4}},
				{dictionary: "b", indices: "[0, 2]", wantIDs: []int64{1}},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			mem := memory.NewCheckedAllocator(memory.DefaultAllocator)
			defer mem.AssertSize(t, 0)
			ctx := compute.WithAllocator(t.Context(), mem)

			dictionaries := make(map[string]arrow.Array, len(tt.dictionaries))
			for name, values := range tt.dictionaries {
				full, _, err := array.FromJSON(mem, arrow.BinaryTypes.String, strings.NewReader(values))
				require.NoError(t, err)
				dictionary := array.NewSlice(full, int64(tt.dictionaryOffset), int64(full.Len()))
				full.Release()
				defer dictionary.Release()
				dictionaries[name] = dictionary
			}
			deleteSets := make([]*equalityDeleteSet, len(tt.sets))
			for i, setKeys := range tt.sets {
				keys := make(set[string], len(setKeys))
				for _, key := range setKeys {
					keys[key] = struct{}{}
				}
				deleteSets[i] = &equalityDeleteSet{keys: keys, fieldIDs: []int{2}, colNames: []string{"value"}}
			}
			fileSchema := iceberg.NewSchema(0,
				iceberg.NestedField{ID: 1, Name: "id", Type: iceberg.PrimitiveTypes.Int64, Required: true},
				iceberg.NestedField{ID: 2, Name: "value", Type: iceberg.PrimitiveTypes.String},
			)
			process, err := processEqualityDeletesColumnarForFile(ctx, deleteSets, fileSchema, "data.parquet")
			require.NoError(t, err)

			dictType := &arrow.DictionaryType{IndexType: arrow.PrimitiveTypes.Int16, ValueType: arrow.BinaryTypes.String}
			schema := arrow.NewSchema([]arrow.Field{
				{Name: "id", Type: arrow.PrimitiveTypes.Int64},
				{Name: "value", Type: dictType, Nullable: true},
			}, nil)
			for i, b := range tt.batches {
				indices, _, err := array.FromJSON(mem, arrow.PrimitiveTypes.Int16, strings.NewReader(b.indices))
				require.NoError(t, err)
				values := array.NewDictionaryArray(dictType, indices, dictionaries[b.dictionary])
				indices.Release()
				idBuilder := array.NewInt64Builder(mem)
				for id := range values.Len() {
					idBuilder.Append(int64(id + 1))
				}
				ids := idBuilder.NewArray()
				idBuilder.Release()
				full := array.NewRecordBatch(schema, []arrow.Array{ids, values}, int64(values.Len()))
				ids.Release()
				values.Release()
				record := full.NewSlice(int64(b.offset), full.NumRows())
				full.Release()

				got, err := process(record)
				require.NoError(t, err)
				assert.Equal(t, b.wantIDs, got.Column(0).(*array.Int64).Int64Values(), "batch %d", i)
				got.Release()
			}
		})
	}
}

func TestDictionaryEntryMemoKeepsResultsForSameDictionary(t *testing.T) {
	mem := memory.NewCheckedAllocator(memory.DefaultAllocator)
	defer mem.AssertSize(t, 0)

	deleteSet := &singleBytesEqualityDeleteSet{keys: set[string]{"apple": {}}}
	dictType := &arrow.DictionaryType{IndexType: arrow.PrimitiveTypes.Int16, ValueType: arrow.BinaryTypes.String}
	dictionary, _, err := array.FromJSON(mem, arrow.BinaryTypes.String, strings.NewReader(`["apple", "bob", "cherry"]`))
	require.NoError(t, err)
	defer dictionary.Release()

	var memo dictionaryEntryMemo
	apply := func(indices string) {
		t.Helper()
		idx, _, err := array.FromJSON(mem, arrow.PrimitiveTypes.Int16, strings.NewReader(indices))
		require.NoError(t, err)
		column := array.NewDictionaryArray(dictType, idx, dictionary)
		idx.Release()
		defer column.Release()
		var mask equalityDeleteMask
		defer mask.release()
		require.True(t, memo.apply(&mask, column, deleteSet, mem))
	}

	apply("[0]")
	assert.Equal(t, []uint8{dictionaryEntryDeleted, dictionaryEntryUnknown, dictionaryEntryUnknown}, memo.results)
	apply("[1]")
	assert.Equal(t, []uint8{dictionaryEntryDeleted, dictionaryEntryKept, dictionaryEntryUnknown}, memo.results)
}

func buildBenchDeleteSetSingleString(numDeletes int, noMatch bool) *equalityDeleteSet {
	keys := make(set[string], numDeletes)
	var buf bytes.Buffer
	for i := range numDeletes {
		value := fmt.Sprintf("user-%08d", i*3)
		if noMatch {
			value = fmt.Sprintf("missing-%08d", i*3)
		}
		buf.Reset()
		buf.WriteByte(1)
		bufPutUint32(&buf, uint32(len(value)))
		buf.WriteString(value)
		keys[buf.String()] = struct{}{}
	}

	return &equalityDeleteSet{
		keys:     keys,
		fieldIDs: []int{2},
		colNames: []string{"name"},
	}
}

func benchSingleStringEqualityDeletes(b *testing.B, buildRec func(memory.Allocator, int) arrow.RecordBatch) {
	b.Helper()

	for _, noMatch := range []bool{false, true} {
		name := "matches"
		if noMatch {
			name = "no_match"
		}
		b.Run(name, func(b *testing.B) {
			for _, fast := range []bool{false, true} {
				mode := "generic"
				if fast {
					mode = "fast"
				}
				b.Run(mode, func(b *testing.B) {
					benchEqDeletesForFile(b, buildRec, func(n int) *equalityDeleteSet {
						deleteSet := buildBenchDeleteSetSingleString(n, noMatch)
						if !fast {
							deleteSet.singleBytesOnce.Do(func() {})
						}

						return deleteSet
					}, benchStringFileSchema())
				})
			}
		})
	}
}

func BenchmarkProcessEqualityDeletesSingleString(b *testing.B) {
	benchSingleStringEqualityDeletes(b, buildBenchRecordString)
}

// 1,000 distinct names keep the dictionary no larger than the smallest
// benchmark batch, so the dictionary path always applies.
func buildBenchRecordStringDictionary(mem memory.Allocator, numRows int) arrow.RecordBatch {
	dictType := &arrow.DictionaryType{IndexType: arrow.PrimitiveTypes.Int32, ValueType: arrow.BinaryTypes.String}
	schema := arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int64},
		{Name: "name", Type: dictType},
	}, nil)

	bldr := array.NewRecordBuilder(mem, schema)
	defer bldr.Release()

	idBldr := bldr.Field(0).(*array.Int64Builder)
	nameBldr := bldr.Field(1).(*array.BinaryDictionaryBuilder)

	for i := range numRows {
		idBldr.Append(int64(i))
		if err := nameBldr.AppendString(fmt.Sprintf("user-%08d", i%1000)); err != nil {
			panic(err)
		}
	}

	return bldr.NewRecordBatch()
}

func BenchmarkProcessEqualityDeletesSingleStringDictionary(b *testing.B) {
	benchSingleStringEqualityDeletes(b, buildBenchRecordStringDictionary)
}

// buildBenchStringDictionaryBatches gives every batch the same dictionary
// object when shared is true, as Arrow's Parquet reader does for an unchanged
// dictionary. Otherwise each batch gets its own copy of the same values.
func buildBenchStringDictionaryBatches(mem memory.Allocator, numEntries, numRows, numBatches int, shared bool) []arrow.RecordBatch {
	newDictionary := func() arrow.Array {
		bldr := array.NewStringBuilder(mem)
		defer bldr.Release()
		for entry := range numEntries {
			bldr.Append(fmt.Sprintf("user-%08d", entry))
		}

		return bldr.NewArray()
	}
	dictType := &arrow.DictionaryType{IndexType: arrow.PrimitiveTypes.Int32, ValueType: arrow.BinaryTypes.String}
	schema := arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int64},
		{Name: "name", Type: dictType},
	}, nil)
	dictionary := newDictionary()
	defer dictionary.Release()

	batches := make([]arrow.RecordBatch, numBatches)
	for i := range batches {
		idBldr := array.NewInt64Builder(mem)
		indexBldr := array.NewInt32Builder(mem)
		for row := range numRows {
			idBldr.Append(int64(i*numRows + row))
			indexBldr.Append(int32((row*7919 + i*numRows) % numEntries))
		}
		ids, indices := idBldr.NewArray(), indexBldr.NewArray()
		idBldr.Release()
		indexBldr.Release()
		batchDictionary := dictionary
		if !shared {
			batchDictionary = newDictionary()
		}
		names := array.NewDictionaryArray(dictType, indices, batchDictionary)
		if !shared {
			batchDictionary.Release()
		}
		batches[i] = array.NewRecordBatch(schema, []arrow.Array{ids, names}, int64(numRows))
		ids.Release()
		indices.Release()
		names.Release()
	}

	return batches
}

// BenchmarkProcessEqualityDeletesSingleStringDictionaryBatches filters one
// file's row group per op, with a dictionary larger than each batch.
func BenchmarkProcessEqualityDeletesSingleStringDictionaryBatches(b *testing.B) {
	const numEntries, numBatches = 200_000, 8
	for _, shared := range []bool{false, true} {
		dictionaries := "fresh_dictionaries"
		if shared {
			dictionaries = "shared_dictionary"
		}
		for _, numRows := range []int{8192, 131_072} {
			for _, numDeletes := range []int{10, 10_000} {
				b.Run(fmt.Sprintf("%s/rows=%d/deletes=%d", dictionaries, numRows, numDeletes), func(b *testing.B) {
					mem := memory.NewGoAllocator()
					ctx := compute.WithAllocator(b.Context(), mem)
					batches := buildBenchStringDictionaryBatches(mem, numEntries, numRows, numBatches, shared)
					defer func() {
						for _, batch := range batches {
							batch.Release()
						}
					}()
					deleteSet := buildBenchDeleteSetSingleString(numDeletes, false)

					b.ReportAllocs()
					for b.Loop() {
						process, err := processEqualityDeletesColumnarForFile(ctx,
							[]*equalityDeleteSet{deleteSet}, benchStringFileSchema(), "bench.parquet")
						if err != nil {
							b.Fatal(err)
						}
						for _, batch := range batches {
							batch.Retain()
							got, err := process(batch)
							if err != nil {
								b.Fatal(err)
							}
							got.Release()
						}
					}
				})
			}
		}
	}
}
