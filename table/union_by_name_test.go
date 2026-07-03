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
	"testing"

	"github.com/apache/iceberg-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func unionTxn(t *testing.T, schema *iceberg.Schema) *Transaction {
	t.Helper()
	meta, err := NewMetadata(schema, nil, UnsortedSortOrder, "", nil)
	require.NoError(t, err)

	return New([]string{"tbl"}, meta, "", nil, nil).NewTransaction()
}

func TestUnionByNameAddTopLevelColumns(t *testing.T) {
	current := iceberg.NewSchema(1,
		iceberg.NestedField{ID: 1, Name: "id", Type: iceberg.PrimitiveTypes.Int32, Required: true},
	)
	incoming := iceberg.NewSchema(1,
		iceberg.NestedField{ID: 1, Name: "id", Type: iceberg.PrimitiveTypes.Int32, Required: true},
		iceberg.NestedField{ID: 2, Name: "name", Type: iceberg.PrimitiveTypes.String, Required: false},
		iceberg.NestedField{ID: 3, Name: "age", Type: iceberg.PrimitiveTypes.Int32, Required: false},
	)

	applied, err := NewUpdateSchema(unionTxn(t, current), true, false).UnionByNameWith(incoming).Apply()
	require.NoError(t, err)

	name, ok := applied.FindFieldByName("name")
	require.True(t, ok)
	assert.Equal(t, iceberg.PrimitiveTypes.String, name.Type)
	assert.False(t, name.Required)

	age, ok := applied.FindFieldByName("age")
	require.True(t, ok)
	assert.Equal(t, iceberg.PrimitiveTypes.Int32, age.Type)
}

func TestUnionByNameAddedColumnsAreOptional(t *testing.T) {
	current := iceberg.NewSchema(1,
		iceberg.NestedField{ID: 1, Name: "id", Type: iceberg.PrimitiveTypes.Int32, Required: true},
	)
	// The incoming field is required, but union always adds columns as optional.
	incoming := iceberg.NewSchema(1,
		iceberg.NestedField{ID: 1, Name: "id", Type: iceberg.PrimitiveTypes.Int32, Required: true},
		iceberg.NestedField{ID: 2, Name: "data", Type: iceberg.PrimitiveTypes.String, Required: true},
	)

	applied, err := NewUpdateSchema(unionTxn(t, current), true, false).UnionByNameWith(incoming).Apply()
	require.NoError(t, err)

	data, ok := applied.FindFieldByName("data")
	require.True(t, ok)
	assert.False(t, data.Required)
}

func TestUnionByNameAddNestedStructField(t *testing.T) {
	current := iceberg.NewSchema(1,
		iceberg.NestedField{ID: 1, Name: "id", Type: iceberg.PrimitiveTypes.Int32, Required: true},
		iceberg.NestedField{ID: 2, Name: "address", Type: &iceberg.StructType{
			FieldList: []iceberg.NestedField{
				{ID: 3, Name: "city", Type: iceberg.PrimitiveTypes.String, Required: false},
			},
		}, Required: false},
	)
	incoming := iceberg.NewSchema(1,
		iceberg.NestedField{ID: 1, Name: "id", Type: iceberg.PrimitiveTypes.Int32, Required: true},
		iceberg.NestedField{ID: 2, Name: "address", Type: &iceberg.StructType{
			FieldList: []iceberg.NestedField{
				{ID: 3, Name: "city", Type: iceberg.PrimitiveTypes.String, Required: false},
				{ID: 4, Name: "zip", Type: iceberg.PrimitiveTypes.String, Required: false},
			},
		}, Required: false},
	)

	applied, err := NewUpdateSchema(unionTxn(t, current), true, false).UnionByNameWith(incoming).Apply()
	require.NoError(t, err)

	zip, ok := applied.FindFieldByName("address.zip")
	require.True(t, ok)
	assert.Equal(t, iceberg.PrimitiveTypes.String, zip.Type)

	// existing nested field is retained
	_, ok = applied.FindFieldByName("address.city")
	assert.True(t, ok)
}

func TestUnionByNamePromoteType(t *testing.T) {
	current := iceberg.NewSchema(1,
		iceberg.NestedField{ID: 1, Name: "amount", Type: iceberg.PrimitiveTypes.Int32, Required: true},
	)
	incoming := iceberg.NewSchema(1,
		iceberg.NestedField{ID: 1, Name: "amount", Type: iceberg.PrimitiveTypes.Int64, Required: true},
	)

	applied, err := NewUpdateSchema(unionTxn(t, current), true, false).UnionByNameWith(incoming).Apply()
	require.NoError(t, err)

	amount, ok := applied.FindFieldByName("amount")
	require.True(t, ok)
	assert.Equal(t, iceberg.PrimitiveTypes.Int64, amount.Type)
}

func TestUnionByNameIgnoreNarrowingType(t *testing.T) {
	current := iceberg.NewSchema(1,
		iceberg.NestedField{ID: 1, Name: "amount", Type: iceberg.PrimitiveTypes.Int64, Required: true},
	)
	incoming := iceberg.NewSchema(1,
		iceberg.NestedField{ID: 1, Name: "amount", Type: iceberg.PrimitiveTypes.Int32, Required: true},
	)

	applied, err := NewUpdateSchema(unionTxn(t, current), true, false).UnionByNameWith(incoming).Apply()
	require.NoError(t, err)

	amount, ok := applied.FindFieldByName("amount")
	require.True(t, ok)
	assert.Equal(t, iceberg.PrimitiveTypes.Int64, amount.Type)
}

func TestUnionByNamePromoteListElement(t *testing.T) {
	current := iceberg.NewSchema(1,
		iceberg.NestedField{ID: 1, Name: "vals", Type: &iceberg.ListType{
			ElementID: 2, Element: iceberg.PrimitiveTypes.Int32, ElementRequired: false,
		}, Required: false},
	)
	incoming := iceberg.NewSchema(1,
		iceberg.NestedField{ID: 1, Name: "vals", Type: &iceberg.ListType{
			ElementID: 2, Element: iceberg.PrimitiveTypes.Int64, ElementRequired: false,
		}, Required: false},
	)

	applied, err := NewUpdateSchema(unionTxn(t, current), true, false).UnionByNameWith(incoming).Apply()
	require.NoError(t, err)

	elem, ok := applied.FindFieldByName("vals.element")
	require.True(t, ok)
	assert.Equal(t, iceberg.PrimitiveTypes.Int64, elem.Type)
}

func TestUnionByNameMakeColumnOptional(t *testing.T) {
	current := iceberg.NewSchema(1,
		iceberg.NestedField{ID: 1, Name: "id", Type: iceberg.PrimitiveTypes.Int32, Required: true},
	)
	incoming := iceberg.NewSchema(1,
		iceberg.NestedField{ID: 1, Name: "id", Type: iceberg.PrimitiveTypes.Int32, Required: false},
	)

	applied, err := NewUpdateSchema(unionTxn(t, current), true, false).UnionByNameWith(incoming).Apply()
	require.NoError(t, err)

	id, ok := applied.FindFieldByName("id")
	require.True(t, ok)
	assert.False(t, id.Required)
}

func TestUnionByNameUpdateDoc(t *testing.T) {
	current := iceberg.NewSchema(1,
		iceberg.NestedField{ID: 1, Name: "id", Type: iceberg.PrimitiveTypes.Int32, Required: true},
	)
	incoming := iceberg.NewSchema(1,
		iceberg.NestedField{ID: 1, Name: "id", Type: iceberg.PrimitiveTypes.Int32, Required: true, Doc: "identifier"},
	)

	applied, err := NewUpdateSchema(unionTxn(t, current), true, false).UnionByNameWith(incoming).Apply()
	require.NoError(t, err)

	id, ok := applied.FindFieldByName("id")
	require.True(t, ok)
	assert.Equal(t, "identifier", id.Doc)
}

func TestUnionByNameUpdateWriteDefaultOnly(t *testing.T) {
	current := iceberg.NewSchema(1,
		iceberg.NestedField{ID: 1, Name: "id", Type: iceberg.PrimitiveTypes.Int32, Required: true},
	)
	incoming := iceberg.NewSchema(1,
		iceberg.NestedField{
			ID: 1, Name: "id", Type: iceberg.PrimitiveTypes.Int32, Required: true,
			InitialDefault: int32(34), WriteDefault: int32(35),
		},
	)

	applied, err := NewUpdateSchema(unionTxn(t, current), true, false).UnionByNameWith(incoming).Apply()
	require.NoError(t, err)

	id, ok := applied.FindFieldByName("id")
	require.True(t, ok)
	// write default is applied but the initial default of an existing column is not modified
	assert.Equal(t, int32(35), id.WriteDefault)
	assert.Nil(t, id.InitialDefault)
}

func TestUnionByNameCaseSensitivity(t *testing.T) {
	current := iceberg.NewSchema(1,
		iceberg.NestedField{ID: 1, Name: "id", Type: iceberg.PrimitiveTypes.Int32, Required: true},
	)
	incoming := iceberg.NewSchema(1,
		iceberg.NestedField{ID: 1, Name: "ID", Type: iceberg.PrimitiveTypes.Int32, Required: false},
	)

	t.Run("case sensitive adds a new column", func(t *testing.T) {
		applied, err := NewUpdateSchema(unionTxn(t, current), true, false).UnionByNameWith(incoming).Apply()
		require.NoError(t, err)

		_, ok := applied.FindFieldByName("id")
		assert.True(t, ok)
		_, ok = applied.FindFieldByName("ID")
		assert.True(t, ok)
	})

	t.Run("case insensitive matches the existing column", func(t *testing.T) {
		applied, err := NewUpdateSchema(unionTxn(t, current), false, false).UnionByNameWith(incoming).Apply()
		require.NoError(t, err)

		assert.Len(t, applied.Fields(), 1)
		id, ok := applied.FindFieldByName("id")
		require.True(t, ok)
		assert.False(t, id.Required)
	})
}

func TestUnionByNameIdenticalSchemaIsNoop(t *testing.T) {
	current := iceberg.NewSchema(1,
		iceberg.NestedField{ID: 1, Name: "id", Type: iceberg.PrimitiveTypes.Int32, Required: true},
		iceberg.NestedField{ID: 2, Name: "name", Type: iceberg.PrimitiveTypes.String, Required: false},
	)

	applied, err := NewUpdateSchema(unionTxn(t, current), true, false).UnionByNameWith(current).Apply()
	require.NoError(t, err)
	assert.Equal(t, current.AsStruct(), applied.AsStruct())
}

func TestUnionByNameInvalidTypeChange(t *testing.T) {
	current := iceberg.NewSchema(1,
		iceberg.NestedField{ID: 1, Name: "col", Type: &iceberg.ListType{
			ElementID: 2, Element: iceberg.PrimitiveTypes.String, ElementRequired: false,
		}, Required: false},
	)
	incoming := iceberg.NewSchema(1,
		iceberg.NestedField{ID: 1, Name: "col", Type: iceberg.PrimitiveTypes.String, Required: false},
	)

	_, err := NewUpdateSchema(unionTxn(t, current), true, false).UnionByNameWith(incoming).Apply()
	assert.Error(t, err)
}

func TestUnionByNameNilSchema(t *testing.T) {
	current := iceberg.NewSchema(1,
		iceberg.NestedField{ID: 1, Name: "id", Type: iceberg.PrimitiveTypes.Int32, Required: true},
	)

	_, err := NewUpdateSchema(unionTxn(t, current), true, false).UnionByNameWith(nil).Apply()
	assert.Error(t, err)
}
