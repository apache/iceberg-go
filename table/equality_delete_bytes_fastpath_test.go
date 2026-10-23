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

func BenchmarkProcessEqualityDeletesSingleString(b *testing.B) {
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
					benchEqDeletesForFile(b, buildBenchRecordString, func(n int) *equalityDeleteSet {
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
