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

package iceberg

// These tests write through the unexported fields of the nested types to
// check that schema and partition getters hand out copies.

import (
	"fmt"
	"runtime"
	"testing"

	"github.com/apache/iceberg-go/internal"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestSchemaAsStructClonesTopLevelFieldList(t *testing.T) {
	schema := NewSchema(0,
		NestedField{ID: 1, Name: "foo", Type: PrimitiveTypes.String, Required: true},
	)

	structType := schema.AsStruct()
	structType.fields[0].Name = "hijacked"

	assert.Equal(t, "foo", schema.Field(0).Name)
}

func TestSchemaAsStructClonesNestedTypes(t *testing.T) {
	schema := NewSchema(0,
		NestedField{
			ID:   1,
			Name: "person",
			Type: NewStructType(
				NestedField{ID: 2, Name: "name", Type: PrimitiveTypes.String},
			),
			Required: true,
		},
	)

	structType := schema.AsStruct()
	personType, ok := structType.Field(0).Type.(*StructType)
	require.True(t, ok)
	personType.fields[0].Name = "full_name"

	clonedPersonType, ok := schema.Field(0).Type.(*StructType)
	require.True(t, ok)
	assert.Equal(t, "name", clonedPersonType.Field(0).Name)
}

func TestSchemaAsStructClonesNestedListTypes(t *testing.T) {
	schema := NewSchema(0,
		NestedField{
			ID:   1,
			Name: "people",
			Type: NewListType(2, NewStructType(
				NestedField{ID: 3, Name: "name", Type: PrimitiveTypes.String},
			), true),
			Required: true,
		},
	)

	structType := schema.AsStruct()
	listType, ok := structType.Field(0).Type.(*ListType)
	require.True(t, ok)

	elementType, ok := listType.Element().(*StructType)
	require.True(t, ok)
	elementType.fields[0].Name = "full_name"

	clonedListType, ok := schema.Field(0).Type.(*ListType)
	require.True(t, ok)

	clonedElementType, ok := clonedListType.Element().(*StructType)
	require.True(t, ok)
	assert.Equal(t, "name", clonedElementType.Field(0).Name)
}

func TestSchemaAsStructClonesNestedMapTypes(t *testing.T) {
	schema := NewSchema(0,
		NestedField{
			ID:   1,
			Name: "people_by_id",
			Type: NewMapType(2, PrimitiveTypes.String, 3, NewStructType(
				NestedField{ID: 4, Name: "name", Type: PrimitiveTypes.String},
			), true),
			Required: true,
		},
	)

	structType := schema.AsStruct()
	mapType, ok := structType.Field(0).Type.(*MapType)
	require.True(t, ok)

	valueType, ok := mapType.ValueType().(*StructType)
	require.True(t, ok)
	valueType.fields[0].Name = "full_name"

	clonedMapType, ok := schema.Field(0).Type.(*MapType)
	require.True(t, ok)

	clonedValueType, ok := clonedMapType.ValueType().(*StructType)
	require.True(t, ok)
	assert.Equal(t, "name", clonedValueType.Field(0).Name)
}

func TestSchemaFieldGettersReturnDefensiveCopies(t *testing.T) {
	schema := NewSchema(0,
		NestedField{
			ID:   1,
			Name: "person",
			Type: NewStructType(
				NestedField{ID: 2, Name: "name", Type: PrimitiveTypes.String},
			),
		},
		NestedField{
			ID:             3,
			Name:           "payload",
			Type:           PrimitiveTypes.Binary,
			InitialDefault: BinaryLiteral{1, 2},
			WriteDefault:   map[string]any{"nested": []any{[]byte{3, 4}}},
		},
	)

	assertIndependent := func(t *testing.T, personField, payloadField NestedField) {
		t.Helper()
		person, ok := personField.Type.(*StructType)
		require.True(t, ok)
		person.fields[0].Name = "hijacked"
		payloadField.WriteDefault.(map[string]any)["nested"].([]any)[0].([]byte)[0] = 9

		actual, ok := schema.FindFieldByID(2)
		require.True(t, ok)
		assert.Equal(t, "name", actual.Name)
		payload, ok := schema.FindFieldByID(3)
		require.True(t, ok)
		assert.Equal(t, "0102", payload.InitialDefault)
		assert.Equal(t, map[string]any{"nested": []any{[]byte{3, 4}}}, payload.WriteDefault)
	}

	t.Run("Field", func(t *testing.T) {
		assertIndependent(t, schema.Field(0), schema.Field(1))
	})
	t.Run("Fields", func(t *testing.T) {
		fields := schema.Fields()
		assertIndependent(t, fields[0], fields[1])
	})
	t.Run("FindFieldByID", func(t *testing.T) {
		person, ok := schema.FindFieldByID(1)
		require.True(t, ok)
		payload, ok := schema.FindFieldByID(3)
		require.True(t, ok)
		assertIndependent(t, person, payload)
	})
	t.Run("FindFieldByName", func(t *testing.T) {
		person, ok := schema.FindFieldByName("person")
		require.True(t, ok)
		payload, ok := schema.FindFieldByName("payload")
		require.True(t, ok)
		assertIndependent(t, person, payload)
	})
	t.Run("FindFieldByNameCaseInsensitive", func(t *testing.T) {
		person, ok := schema.FindFieldByNameCaseInsensitive("PERSON")
		require.True(t, ok)
		payload, ok := schema.FindFieldByNameCaseInsensitive("PAYLOAD")
		require.True(t, ok)
		assertIndependent(t, person, payload)
	})
	t.Run("FindTypeByID", func(t *testing.T) {
		typ, ok := schema.FindTypeByID(1)
		require.True(t, ok)
		person, ok := typ.(*StructType)
		require.True(t, ok)
		person.fields[0].Name = "hijacked"

		actual, ok := schema.FindFieldByID(2)
		require.True(t, ok)
		assert.Equal(t, "name", actual.Name)
	})
	t.Run("FlatFields", func(t *testing.T) {
		fields, err := schema.FlatFields()
		require.NoError(t, err)
		byID := make(map[int]NestedField)
		for field := range fields {
			byID[field.ID] = field
		}
		assertIndependent(t, byID[1], byID[3])
	})
}

func TestSchemaFieldRefLookupDoesNotAllocate(t *testing.T) {
	children := make([]NestedField, 50)
	for i := range children {
		children[i] = NestedField{
			ID: i + 2, Name: fmt.Sprintf("child_%d", i), Type: PrimitiveTypes.String,
		}
	}
	schema := NewSchema(0, NestedField{
		ID: 1, Name: "parent", Type: NewStructType(children...),
	})
	_, ok := schema.FindFieldByIDRef(1, internal.SchemaRef{})
	require.True(t, ok)

	var field NestedField
	assert.Zero(t, testing.AllocsPerRun(100, func() {
		field, ok = schema.FindFieldByIDRef(1, internal.SchemaRef{})
		runtime.KeepAlive(field)
	}))
	require.True(t, ok)
	assert.Equal(t, 1, field.ID)

	var fields []NestedField
	assert.Zero(t, testing.AllocsPerRun(100, func() {
		fields = schema.FieldsRef(internal.SchemaRef{})
		runtime.KeepAlive(fields)
	}))
	assert.Len(t, fields, 1)

	parent := field.Type.(*StructType)
	parent.fields[0].Name = "shared_child"
	shared, ok := schema.FindFieldByIDRef(1, internal.SchemaRef{})
	require.True(t, ok)
	sharedParent := shared.Type.(*StructType)
	assert.Equal(t, "shared_child", sharedParent.Field(0).Name)
}

func TestPartitionTypeReturnsIndependentFields(t *testing.T) {
	schema := NewSchema(0, NestedField{ID: 1, Name: "value", Type: StringType{}})
	spec := NewPartitionSpec(PartitionField{
		SourceIDs: []int{1}, FieldID: 1000, Name: "value", Transform: IdentityTransform{},
	})
	first := spec.PartitionType(schema)
	second := spec.PartitionType(schema)
	first.fields[0] = NestedField{ID: 2000, Name: "changed", Type: Int64Type{}, Required: true}

	want := []NestedField{{ID: 1000, Name: "value", Type: StringType{}}}
	require.Equal(t, want, second.Fields())
	require.Equal(t, want, spec.PartitionType(schema).Fields())
	require.Equal(t, "value", spec.Field(0).Name)
	require.Equal(t, 1000, spec.Field(0).FieldID)
}
