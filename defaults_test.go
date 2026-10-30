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

package iceberg_test

import (
	"encoding/json"
	"testing"

	iceberg "github.com/apache/iceberg-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestCloneDefaultValue(t *testing.T) {
	original := map[string]any{
		"binary":  []byte{1, 2},
		"literal": iceberg.BinaryLiteral{3, 4},
		"nested": []any{
			map[string]any{"fixed": iceberg.FixedLiteral{5, 6}},
		},
	}

	cloned := iceberg.CloneDefaultValue(original).(map[string]any)
	cloned["binary"].([]byte)[0] = 9
	cloned["literal"].(iceberg.BinaryLiteral)[0] = 9
	nested, ok := cloned["nested"].([]any)
	require.True(t, ok)
	nestedMap := nested[0].(map[string]any)
	nestedMap["fixed"].(iceberg.FixedLiteral)[0] = 9
	cloned["added"] = true
	nestedMap["added"] = true
	nested[0] = "changed"

	assert.Equal(t, []byte{1, 2}, original["binary"])
	assert.Equal(t, iceberg.BinaryLiteral{3, 4}, original["literal"])
	assert.Equal(t, iceberg.FixedLiteral{5, 6}, original["nested"].([]any)[0].(map[string]any)["fixed"])
	assert.NotContains(t, original, "added")
	assert.NotContains(t, original["nested"].([]any)[0], "added")
}

func TestNewSchemaStoresByteDefaultsAsHex(t *testing.T) {
	tests := []struct {
		name  string
		typ   iceberg.Type
		value any
	}{
		{"binary bytes", iceberg.PrimitiveTypes.Binary, []byte{0x0a, 0xff}},
		{"binary literal", iceberg.PrimitiveTypes.Binary, iceberg.BinaryLiteral{0x0a, 0xff}},
		{"fixed bytes", iceberg.MustFixedTypeOf(2), []byte{0x0a, 0xff}},
		{"fixed literal", iceberg.MustFixedTypeOf(2), iceberg.FixedLiteral{0x0a, 0xff}},
		{"hex string", iceberg.PrimitiveTypes.Binary, "0aff"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			sc := iceberg.NewSchema(0, iceberg.NestedField{
				ID: 1, Name: "b", Type: tt.typ, InitialDefault: tt.value, WriteDefault: tt.value,
			})

			field := sc.Field(0)
			assert.Equal(t, "0aff", field.InitialDefault)
			assert.Equal(t, "0aff", field.WriteDefault)
		})
	}
}

func TestNewSchemaLeavesOtherDefaultsAlone(t *testing.T) {
	sc := iceberg.NewSchema(0,
		iceberg.NestedField{ID: 1, Name: "s", Type: iceberg.PrimitiveTypes.String, InitialDefault: "0aff"},
		iceberg.NestedField{ID: 2, Name: "i", Type: iceberg.PrimitiveTypes.Int32, InitialDefault: int32(7)},
		iceberg.NestedField{ID: 3, Name: "n", Type: iceberg.PrimitiveTypes.Binary},
	)

	assert.Equal(t, "0aff", sc.Field(0).InitialDefault)
	assert.Equal(t, int32(7), sc.Field(1).InitialDefault)
	assert.Nil(t, sc.Field(2).InitialDefault)
}

// A schema built in process must equal the same schema read back from
// metadata, where binary and fixed defaults are hex strings.
func TestNewSchemaByteDefaultsSurviveJSONRoundTrip(t *testing.T) {
	sc := iceberg.NewSchema(0,
		iceberg.NestedField{ID: 1, Name: "b", Type: iceberg.PrimitiveTypes.Binary, InitialDefault: []byte{1, 2}},
		iceberg.NestedField{ID: 2, Name: "f", Type: iceberg.MustFixedTypeOf(2), WriteDefault: iceberg.FixedLiteral{3, 4}},
	)

	data, err := json.Marshal(sc)
	require.NoError(t, err)

	var decoded iceberg.Schema
	require.NoError(t, json.Unmarshal(data, &decoded))
	assert.True(t, sc.Equals(&decoded), "built: %v, decoded: %v", sc.Fields(), decoded.Fields())
}

func TestNewSchemaByteDefaultIsIsolatedFromCaller(t *testing.T) {
	value := []byte{1, 2}
	sc := iceberg.NewSchema(0, iceberg.NestedField{
		ID: 1, Name: "b", Type: iceberg.PrimitiveTypes.Binary, InitialDefault: value,
	})

	value[0] = 9
	assert.Equal(t, "0102", sc.Field(0).InitialDefault)
}

func TestNewSchemaNormalizesNestedByteDefaultsWithoutModifyingCaller(t *testing.T) {
	inner := &iceberg.StructType{FieldList: []iceberg.NestedField{
		{ID: 5, Name: "payload", Type: iceberg.PrimitiveTypes.Binary, InitialDefault: []byte{1, 2}},
		{ID: 6, Name: "label", Type: iceberg.PrimitiveTypes.String, InitialDefault: "x"},
	}}
	list := &iceberg.ListType{ElementID: 4, Element: inner, ElementRequired: true}
	mapType := &iceberg.MapType{
		KeyID: 2, KeyType: iceberg.PrimitiveTypes.String,
		ValueID: 3, ValueType: list, ValueRequired: true,
	}
	fields := []iceberg.NestedField{
		{ID: 1, Name: "m", Type: mapType},
		{ID: 7, Name: "top", Type: iceberg.PrimitiveTypes.Binary, WriteDefault: []byte{3}},
	}

	sc := iceberg.NewSchema(0, fields...)

	payload, ok := sc.FindFieldByID(5)
	require.True(t, ok)
	assert.Equal(t, "0102", payload.InitialDefault)
	label, ok := sc.FindFieldByID(6)
	require.True(t, ok)
	assert.Equal(t, "x", label.InitialDefault)
	top, ok := sc.FindFieldByID(7)
	require.True(t, ok)
	assert.Equal(t, "03", top.WriteDefault)

	assert.Equal(t, []byte{1, 2}, inner.FieldList[0].InitialDefault)
	assert.Same(t, list, mapType.ValueType)
	assert.Same(t, inner, list.Element)
	assert.Equal(t, []byte{3}, fields[1].WriteDefault)
}
