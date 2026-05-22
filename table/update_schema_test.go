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
)

var originalSchema = iceberg.NewSchema(1,
	iceberg.NestedField{ID: 1, Name: "id", Type: iceberg.PrimitiveTypes.Int32, Required: true, Doc: ""},
	iceberg.NestedField{ID: 2, Name: "name", Type: iceberg.PrimitiveTypes.String, Required: false, Doc: ""},
	iceberg.NestedField{ID: 3, Name: "age", Type: iceberg.PrimitiveTypes.Int32, Required: false, Doc: ""},
	iceberg.NestedField{ID: 4, Name: "address", Type: &iceberg.StructType{
		FieldList: []iceberg.NestedField{
			{ID: 5, Name: "city", Type: iceberg.PrimitiveTypes.String, Required: false, Doc: ""},
			{ID: 6, Name: "zip", Type: iceberg.PrimitiveTypes.String, Required: false, Doc: ""},
		},
	}, Required: false, Doc: ""},
	iceberg.NestedField{ID: 7, Name: "tags", Type: &iceberg.ListType{
		ElementID:       8,
		Element:         iceberg.PrimitiveTypes.String,
		ElementRequired: false,
	}, Required: false, Doc: ""},
	iceberg.NestedField{ID: 9, Name: "properties", Type: &iceberg.MapType{
		KeyID:         10,
		KeyType:       iceberg.PrimitiveTypes.String,
		ValueID:       11,
		ValueType:     iceberg.PrimitiveTypes.String,
		ValueRequired: false,
	}, Required: false, Doc: ""},
)

var testMetadata, _ = NewMetadata(originalSchema, nil, UnsortedSortOrder, "", nil)

func TestAddColumn(t *testing.T) {
	t.Run("test update schema with add primitive type on top level", func(t *testing.T) {
		table := New([]string{"id"}, testMetadata, "", nil, nil)
		txn := table.NewTransaction()

		newSchema, err := NewUpdateSchema(txn, true, true).AddColumn([]string{"gender"}, iceberg.PrimitiveTypes.String, "", false, iceberg.StringLiteral("male")).Apply()
		assert.NoError(t, err)
		assert.NotNil(t, newSchema)

		assert.Equal(t, []iceberg.NestedField{
			{ID: 1, Name: "id", Type: iceberg.PrimitiveTypes.Int32, Required: true, Doc: ""},
			{ID: 2, Name: "name", Type: iceberg.PrimitiveTypes.String, Required: false, Doc: ""},
			{ID: 3, Name: "age", Type: iceberg.PrimitiveTypes.Int32, Required: false, Doc: ""},
			{ID: 4, Name: "address", Type: &iceberg.StructType{
				FieldList: []iceberg.NestedField{
					{ID: 5, Name: "city", Type: iceberg.PrimitiveTypes.String, Required: false, Doc: ""},
					{ID: 6, Name: "zip", Type: iceberg.PrimitiveTypes.String, Required: false, Doc: ""},
				},
			}, Required: false, Doc: ""},
			{ID: 7, Name: "tags", Type: &iceberg.ListType{
				ElementID:       8,
				Element:         iceberg.PrimitiveTypes.String,
				ElementRequired: false,
			}, Required: false, Doc: ""},
			{ID: 9, Name: "properties", Type: &iceberg.MapType{
				KeyID:         10,
				KeyType:       iceberg.PrimitiveTypes.String,
				ValueID:       11,
				ValueType:     iceberg.PrimitiveTypes.String,
				ValueRequired: false,
			}, Required: false, Doc: ""},
			{ID: 12, Name: "gender", Type: iceberg.PrimitiveTypes.String, Required: false, Doc: ""},
		}, newSchema.Fields())
	})

	t.Run("test update schema with add list of primitive type on top level", func(t *testing.T) {
		table := New([]string{"id"}, testMetadata, "", nil, nil)
		txn := table.NewTransaction()

		newSchema, err := NewUpdateSchema(txn, true, true).AddColumn([]string{"files"}, &iceberg.ListType{
			Element:         iceberg.PrimitiveTypes.String,
			ElementRequired: false,
		}, "", false, nil).Apply()
		assert.NoError(t, err)
		assert.NotNil(t, newSchema)

		assert.Equal(t, []iceberg.NestedField{
			{ID: 1, Name: "id", Type: iceberg.PrimitiveTypes.Int32, Required: true, Doc: ""},
			{ID: 2, Name: "name", Type: iceberg.PrimitiveTypes.String, Required: false, Doc: ""},
			{ID: 3, Name: "age", Type: iceberg.PrimitiveTypes.Int32, Required: false, Doc: ""},
			{ID: 4, Name: "address", Type: &iceberg.StructType{
				FieldList: []iceberg.NestedField{
					{ID: 5, Name: "city", Type: iceberg.PrimitiveTypes.String, Required: false, Doc: ""},
					{ID: 6, Name: "zip", Type: iceberg.PrimitiveTypes.String, Required: false, Doc: ""},
				},
			}, Required: false, Doc: ""},
			{ID: 7, Name: "tags", Type: &iceberg.ListType{
				ElementID:       8,
				Element:         iceberg.PrimitiveTypes.String,
				ElementRequired: false,
			}, Required: false, Doc: ""},
			{ID: 9, Name: "properties", Type: &iceberg.MapType{
				KeyID:         10,
				KeyType:       iceberg.PrimitiveTypes.String,
				ValueID:       11,
				ValueType:     iceberg.PrimitiveTypes.String,
				ValueRequired: false,
			}, Required: false, Doc: ""},
			{ID: 12, Name: "files", Type: &iceberg.ListType{
				ElementID:       13,
				Element:         iceberg.PrimitiveTypes.String,
				ElementRequired: false,
			}, Required: false, Doc: ""},
		}, newSchema.Fields())
	})

	t.Run("test update schema with add map of primitive type on top level", func(t *testing.T) {
		table := New([]string{"id"}, testMetadata, "", nil, nil)
		txn := table.NewTransaction()

		newSchema, err := NewUpdateSchema(txn, true, true).AddColumn([]string{"files"}, &iceberg.MapType{
			KeyType:       iceberg.PrimitiveTypes.String,
			ValueType:     iceberg.PrimitiveTypes.String,
			ValueRequired: false,
		}, "", false, nil).Apply()
		assert.NoError(t, err)
		assert.NotNil(t, newSchema)

		assert.Equal(t, []iceberg.NestedField{
			{ID: 1, Name: "id", Type: iceberg.PrimitiveTypes.Int32, Required: true, Doc: ""},
			{ID: 2, Name: "name", Type: iceberg.PrimitiveTypes.String, Required: false, Doc: ""},
			{ID: 3, Name: "age", Type: iceberg.PrimitiveTypes.Int32, Required: false, Doc: ""},
			{ID: 4, Name: "address", Type: &iceberg.StructType{
				FieldList: []iceberg.NestedField{
					{ID: 5, Name: "city", Type: iceberg.PrimitiveTypes.String, Required: false, Doc: ""},
					{ID: 6, Name: "zip", Type: iceberg.PrimitiveTypes.String, Required: false, Doc: ""},
				},
			}, Required: false, Doc: ""},
			{ID: 7, Name: "tags", Type: &iceberg.ListType{
				ElementID:       8,
				Element:         iceberg.PrimitiveTypes.String,
				ElementRequired: false,
			}, Required: false, Doc: ""},
			{ID: 9, Name: "properties", Type: &iceberg.MapType{
				KeyID:         10,
				KeyType:       iceberg.PrimitiveTypes.String,
				ValueID:       11,
				ValueType:     iceberg.PrimitiveTypes.String,
				ValueRequired: false,
			}, Required: false, Doc: ""},
			{ID: 12, Name: "files", Type: &iceberg.MapType{
				KeyID:         13,
				KeyType:       iceberg.PrimitiveTypes.String,
				ValueID:       14,
				ValueType:     iceberg.PrimitiveTypes.String,
				ValueRequired: false,
			}, Required: false, Doc: ""},
		}, newSchema.Fields())
	})

	t.Run("test update schema with add struct type on top level", func(t *testing.T) {
		table := New([]string{"id"}, testMetadata, "", nil, nil)
		txn := table.NewTransaction()

		newSchema, err := NewUpdateSchema(txn, true, true).AddColumn([]string{"files"}, &iceberg.StructType{
			FieldList: []iceberg.NestedField{
				{ID: 5, Name: "id", Type: iceberg.PrimitiveTypes.String, Required: false, Doc: ""},
				{ID: 6, Name: "name", Type: iceberg.PrimitiveTypes.String, Required: false, Doc: ""},
			},
		}, "", false, nil).Apply()
		assert.NoError(t, err)
		assert.NotNil(t, newSchema)

		assert.Equal(t, []iceberg.NestedField{
			{ID: 1, Name: "id", Type: iceberg.PrimitiveTypes.Int32, Required: true, Doc: ""},
			{ID: 2, Name: "name", Type: iceberg.PrimitiveTypes.String, Required: false, Doc: ""},
			{ID: 3, Name: "age", Type: iceberg.PrimitiveTypes.Int32, Required: false, Doc: ""},
			{ID: 4, Name: "address", Type: &iceberg.StructType{
				FieldList: []iceberg.NestedField{
					{ID: 5, Name: "city", Type: iceberg.PrimitiveTypes.String, Required: false, Doc: ""},
					{ID: 6, Name: "zip", Type: iceberg.PrimitiveTypes.String, Required: false, Doc: ""},
				},
			}, Required: false, Doc: ""},
			{ID: 7, Name: "tags", Type: &iceberg.ListType{
				ElementID:       8,
				Element:         iceberg.PrimitiveTypes.String,
				ElementRequired: false,
			}, Required: false, Doc: ""},
			{ID: 9, Name: "properties", Type: &iceberg.MapType{
				KeyID:         10,
				KeyType:       iceberg.PrimitiveTypes.String,
				ValueID:       11,
				ValueType:     iceberg.PrimitiveTypes.String,
				ValueRequired: false,
			}, Required: false, Doc: ""},
			{ID: 12, Name: "files", Type: &iceberg.StructType{
				FieldList: []iceberg.NestedField{
					{ID: 13, Name: "id", Type: iceberg.PrimitiveTypes.String, Required: false, Doc: ""},
					{ID: 14, Name: "name", Type: iceberg.PrimitiveTypes.String, Required: false, Doc: ""},
				},
			}, Required: false, Doc: ""},
		}, newSchema.Fields())
	})

	t.Run("test update schema with add primitive in struct", func(t *testing.T) {
		table := New([]string{"id"}, testMetadata, "", nil, nil)
		txn := table.NewTransaction()

		newSchema, err := NewUpdateSchema(txn, true, true).AddColumn([]string{"address", "code"}, iceberg.PrimitiveTypes.String, "", false, nil).Apply()
		assert.NoError(t, err)
		assert.NotNil(t, newSchema)

		assert.Equal(t, []iceberg.NestedField{
			{ID: 1, Name: "id", Type: iceberg.PrimitiveTypes.Int32, Required: true, Doc: ""},
			{ID: 2, Name: "name", Type: iceberg.PrimitiveTypes.String, Required: false, Doc: ""},
			{ID: 3, Name: "age", Type: iceberg.PrimitiveTypes.Int32, Required: false, Doc: ""},
			{ID: 4, Name: "address", Type: &iceberg.StructType{
				FieldList: []iceberg.NestedField{
					{ID: 5, Name: "city", Type: iceberg.PrimitiveTypes.String, Required: false, Doc: ""},
					{ID: 6, Name: "zip", Type: iceberg.PrimitiveTypes.String, Required: false, Doc: ""},
					{ID: 12, Name: "code", Type: iceberg.PrimitiveTypes.String, Required: false, Doc: ""},
				},
			}, Required: false, Doc: ""},
			{ID: 7, Name: "tags", Type: &iceberg.ListType{
				ElementID:       8,
				Element:         iceberg.PrimitiveTypes.String,
				ElementRequired: false,
			}, Required: false, Doc: ""},
			{ID: 9, Name: "properties", Type: &iceberg.MapType{
				KeyID:         10,
				KeyType:       iceberg.PrimitiveTypes.String,
				ValueID:       11,
				ValueType:     iceberg.PrimitiveTypes.String,
				ValueRequired: false,
			}, Required: false, Doc: ""},
		}, newSchema.Fields())
	})

	t.Run("test update schema with add struct in struct", func(t *testing.T) {
		table := New([]string{"id"}, testMetadata, "", nil, nil)
		txn := table.NewTransaction()

		newSchema, err := NewUpdateSchema(txn, true, true).AddColumn([]string{"address", "code"}, &iceberg.StructType{
			FieldList: []iceberg.NestedField{
				{ID: 5, Name: "code-1", Type: iceberg.PrimitiveTypes.String, Required: false, Doc: ""},
				{ID: 6, Name: "code-2", Type: iceberg.PrimitiveTypes.String, Required: false, Doc: ""},
			},
		}, "", false, nil).Apply()
		assert.NoError(t, err)
		assert.NotNil(t, newSchema)

		assert.Equal(t, []iceberg.NestedField{
			{ID: 1, Name: "id", Type: iceberg.PrimitiveTypes.Int32, Required: true, Doc: ""},
			{ID: 2, Name: "name", Type: iceberg.PrimitiveTypes.String, Required: false, Doc: ""},
			{ID: 3, Name: "age", Type: iceberg.PrimitiveTypes.Int32, Required: false, Doc: ""},
			{ID: 4, Name: "address", Type: &iceberg.StructType{
				FieldList: []iceberg.NestedField{
					{ID: 5, Name: "city", Type: iceberg.PrimitiveTypes.String, Required: false, Doc: ""},
					{ID: 6, Name: "zip", Type: iceberg.PrimitiveTypes.String, Required: false, Doc: ""},
					{ID: 12, Name: "code", Type: &iceberg.StructType{
						FieldList: []iceberg.NestedField{
							{ID: 13, Name: "code-1", Type: iceberg.PrimitiveTypes.String, Required: false, Doc: ""},
							{ID: 14, Name: "code-2", Type: iceberg.PrimitiveTypes.String, Required: false, Doc: ""},
						},
					}, Required: false, Doc: ""},
				},
			}},
			{ID: 7, Name: "tags", Type: &iceberg.ListType{
				ElementID:       8,
				Element:         iceberg.PrimitiveTypes.String,
				ElementRequired: false,
			}, Required: false, Doc: ""},
			{ID: 9, Name: "properties", Type: &iceberg.MapType{
				KeyID:         10,
				KeyType:       iceberg.PrimitiveTypes.String,
				ValueID:       11,
				ValueType:     iceberg.PrimitiveTypes.String,
				ValueRequired: false,
			}, Required: false, Doc: ""},
		}, newSchema.Fields())
	})

	t.Run("test update schema with multiple adds", func(t *testing.T) {
		table := New([]string{"id"}, testMetadata, "", nil, nil)
		txn := table.NewTransaction()

		newSchema, err := NewUpdateSchema(txn, true, true).AddColumn([]string{"address", "code"}, &iceberg.StructType{
			FieldList: []iceberg.NestedField{
				{ID: 5, Name: "code-1", Type: iceberg.PrimitiveTypes.String, Required: false, Doc: ""},
				{ID: 6, Name: "code-2", Type: iceberg.PrimitiveTypes.String, Required: false, Doc: ""},
			},
		}, "", false, nil).AddColumn([]string{"gender"}, iceberg.PrimitiveTypes.String, "", false, nil).AddColumn([]string{"files"}, &iceberg.ListType{
			Element:         iceberg.PrimitiveTypes.String,
			ElementRequired: false,
		}, "", false, nil).Apply()
		assert.NoError(t, err)
		assert.NotNil(t, newSchema)

		assert.Equal(t, []iceberg.NestedField{
			{ID: 1, Name: "id", Type: iceberg.PrimitiveTypes.Int32, Required: true, Doc: ""},
			{ID: 2, Name: "name", Type: iceberg.PrimitiveTypes.String, Required: false, Doc: ""},
			{ID: 3, Name: "age", Type: iceberg.PrimitiveTypes.Int32, Required: false, Doc: ""},
			{ID: 4, Name: "address", Type: &iceberg.StructType{
				FieldList: []iceberg.NestedField{
					{ID: 5, Name: "city", Type: iceberg.PrimitiveTypes.String, Required: false, Doc: ""},
					{ID: 6, Name: "zip", Type: iceberg.PrimitiveTypes.String, Required: false, Doc: ""},
					{ID: 12, Name: "code", Type: &iceberg.StructType{
						FieldList: []iceberg.NestedField{
							{ID: 13, Name: "code-1", Type: iceberg.PrimitiveTypes.String, Required: false, Doc: ""},
							{ID: 14, Name: "code-2", Type: iceberg.PrimitiveTypes.String, Required: false, Doc: ""},
						},
					}, Required: false, Doc: ""},
				},
			}, Required: false, Doc: ""},
			{ID: 7, Name: "tags", Type: &iceberg.ListType{
				ElementID:       8,
				Element:         iceberg.PrimitiveTypes.String,
				ElementRequired: false,
			}, Required: false, Doc: ""},
			{ID: 9, Name: "properties", Type: &iceberg.MapType{
				KeyID:         10,
				KeyType:       iceberg.PrimitiveTypes.String,
				ValueID:       11,
				ValueType:     iceberg.PrimitiveTypes.String,
				ValueRequired: false,
			}, Required: false, Doc: ""},
			{ID: 15, Name: "gender", Type: iceberg.PrimitiveTypes.String, Required: false, Doc: ""},
			{ID: 16, Name: "files", Type: &iceberg.ListType{
				ElementID:       17,
				Element:         iceberg.PrimitiveTypes.String,
				ElementRequired: false,
			}},
		}, newSchema.Fields())
	})

	t.Run("test update schema with add geometry and geography columns", func(t *testing.T) {
		metaV3, err := NewMetadata(originalSchema, nil, UnsortedSortOrder, "", iceberg.Properties{
			PropertyFormatVersion: "3",
		})
		assert.NoError(t, err)

		table := New([]string{"id"}, metaV3, "", nil, nil)
		txn := table.NewTransaction()

		geog, err := iceberg.GeographyTypeOf("srid:4269", "karney")
		assert.NoError(t, err)

		upd := NewUpdateSchema(txn, true, true).
			AddColumn([]string{"geom"}, iceberg.GeometryType{}, "", false, nil).
			AddColumn([]string{"geog"}, geog, "", false, nil)
		err = upd.Commit()
		assert.NoError(t, err)

		newSchema := txn.meta.CurrentSchema()
		assert.NotNil(t, newSchema)

		geomField, ok := newSchema.FindFieldByName("geom")
		assert.True(t, ok)
		assert.Equal(t, 12, geomField.ID)
		assert.Equal(t, iceberg.GeometryType{}, geomField.Type)

		geogField, ok := newSchema.FindFieldByName("geog")
		assert.True(t, ok)
		assert.Equal(t, 13, geogField.ID)
		assert.True(t, geogField.Type.Equals(geog))
	})

	t.Run("test update schema with add geometry and geography columns errors in v2", func(t *testing.T) {
		table := New([]string{"id"}, testMetadata, "", nil, nil)
		txn := table.NewTransaction()

		geog, err := iceberg.GeographyTypeOf("srid:4269", "karney")
		assert.NoError(t, err)

		upd := NewUpdateSchema(txn, true, true).
			AddColumn([]string{"geom"}, iceberg.GeometryType{}, "", false, nil).
			AddColumn([]string{"geog"}, geog, "", false, nil)
		err = upd.Commit()
		assert.Error(t, err)
		assert.ErrorIs(t, err, iceberg.ErrInvalidSchema)
		assert.Contains(t, err.Error(), "is not supported until v3")
	})
}

func TestApplyChanges(t *testing.T) {
	t.Run("test apply changes on schema", func(t *testing.T) {
		deletes := map[int]struct{}{
			2: {},
		}
		updates := map[int]iceberg.NestedField{
			3: {Name: "age", Type: iceberg.PrimitiveTypes.Int64, Required: true, Doc: ""},
		}
		adds := map[int][]iceberg.NestedField{
			-1: {
				{ID: 12, Name: "gender", Type: iceberg.PrimitiveTypes.String, Required: false, Doc: ""},
			},
		}
		moves := map[int][]move{
			4: {
				{FieldID: 6, RelativeTo: 5, Op: MoveOpBefore},
			},
		}

		st, err := iceberg.Visit(originalSchema, &applyChanges{
			deletes: deletes,
			updates: updates,
			adds:    adds,
			moves:   moves,
		})
		assert.NoError(t, err)
		assert.NotNil(t, st)

		assert.Equal(t, []iceberg.NestedField{
			{ID: 1, Name: "id", Type: iceberg.PrimitiveTypes.Int32, Required: true, Doc: ""},
			{ID: 3, Name: "age", Type: iceberg.PrimitiveTypes.Int64, Required: true, Doc: ""},
			{ID: 4, Name: "address", Type: &iceberg.StructType{
				FieldList: []iceberg.NestedField{
					{ID: 6, Name: "zip", Type: iceberg.PrimitiveTypes.String, Required: false, Doc: ""},
					{ID: 5, Name: "city", Type: iceberg.PrimitiveTypes.String, Required: false, Doc: ""},
				},
			}, Required: false, Doc: ""},
			{ID: 7, Name: "tags", Type: &iceberg.ListType{
				ElementID:       8,
				Element:         iceberg.PrimitiveTypes.String,
				ElementRequired: false,
			}, Required: false, Doc: ""},
			{ID: 9, Name: "properties", Type: &iceberg.MapType{
				KeyID:         10,
				KeyType:       iceberg.PrimitiveTypes.String,
				ValueID:       11,
				ValueType:     iceberg.PrimitiveTypes.String,
				ValueRequired: false,
			}, Required: false, Doc: ""},
			{ID: 12, Name: "gender", Type: iceberg.PrimitiveTypes.String, Required: false, Doc: ""},
		}, st.(*iceberg.StructType).Fields())
	})

	t.Run("test apply changes on add field that delete in same time", func(t *testing.T) {
		originalSchema := iceberg.NewSchema(1,
			iceberg.NestedField{ID: 1, Name: "id", Type: iceberg.PrimitiveTypes.Int32, Required: true, Doc: ""},
			iceberg.NestedField{ID: 2, Name: "name", Type: iceberg.PrimitiveTypes.String, Required: false, Doc: ""},
			iceberg.NestedField{ID: 3, Name: "age", Type: iceberg.PrimitiveTypes.Int32, Required: false, Doc: ""},
		)
		deletes := map[int]struct{}{
			2: {},
		}
		adds := map[int][]iceberg.NestedField{
			-1: {
				{ID: 4, Name: "name", Type: iceberg.PrimitiveTypes.UUID, Required: false, Doc: ""},
			},
		}

		st, err := iceberg.Visit(originalSchema, &applyChanges{
			deletes: deletes,
			adds:    adds,
		})
		assert.NoError(t, err)
		assert.NotNil(t, st)

		assert.Equal(t, []iceberg.NestedField{
			{ID: 1, Name: "id", Type: iceberg.PrimitiveTypes.Int32, Required: true, Doc: ""},
			{ID: 3, Name: "age", Type: iceberg.PrimitiveTypes.Int32, Required: false, Doc: ""},
			{ID: 4, Name: "name", Type: iceberg.PrimitiveTypes.UUID, Required: false, Doc: ""},
		}, st.(*iceberg.StructType).Fields())
	})
}

func TestDeleteColumn(t *testing.T) {
	t.Run("test delete top level column", func(t *testing.T) {
		table := New([]string{"id"}, testMetadata, "", nil, nil)
		txn := table.NewTransaction()

		newSchema, err := NewUpdateSchema(txn, true, true).DeleteColumn([]string{"name"}).Apply()
		assert.NoError(t, err)
		assert.NotNil(t, newSchema)

		fields := newSchema.Fields()
		assert.Len(t, fields, 5)

		fieldNames := make([]string, len(fields))
		for i, field := range fields {
			fieldNames[i] = field.Name
		}
		assert.Contains(t, fieldNames, "id")
		assert.Contains(t, fieldNames, "age")
		assert.Contains(t, fieldNames, "address")
		assert.NotContains(t, fieldNames, "name")
	})

	t.Run("test delete nested column", func(t *testing.T) {
		table := New([]string{"id"}, testMetadata, "", nil, nil)
		txn := table.NewTransaction()

		newSchema, err := NewUpdateSchema(txn, true, true).DeleteColumn([]string{"address", "city"}).Apply()
		assert.NoError(t, err)
		assert.NotNil(t, newSchema)

		addressField, ok := newSchema.FindFieldByName("address")
		assert.True(t, ok)

		structType, ok := addressField.Type.(*iceberg.StructType)
		assert.True(t, ok)
		assert.Len(t, structType.Fields(), 1)
		assert.Equal(t, "zip", structType.Fields()[0].Name)
	})

	t.Run("test delete non-existent column", func(t *testing.T) {
		table := New([]string{"id"}, testMetadata, "", nil, nil)
		txn := table.NewTransaction()

		_, err := NewUpdateSchema(txn, true, true).DeleteColumn([]string{"non_existent"}).Apply()
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "field not found")
	})
}

func TestUpdateColumn(t *testing.T) {
	t.Run("test update column type", func(t *testing.T) {
		table := New([]string{"id"}, testMetadata, "", nil, nil)
		txn := table.NewTransaction()

		newSchema, err := NewUpdateSchema(txn, true, true).UpdateColumn([]string{"age"}, ColumnUpdate{
			FieldType: iceberg.Optional[iceberg.Type]{Valid: true, Val: iceberg.PrimitiveTypes.Int64},
		}).Apply()
		assert.NoError(t, err)
		assert.NotNil(t, newSchema)

		ageField, ok := newSchema.FindFieldByName("age")
		assert.True(t, ok)
		assert.Equal(t, iceberg.PrimitiveTypes.Int64, ageField.Type)
	})

	t.Run("test update column required", func(t *testing.T) {
		table := New([]string{"id"}, testMetadata, "", nil, nil)
		txn := table.NewTransaction()

		newSchema, err := NewUpdateSchema(txn, true, true).UpdateColumn([]string{"name"}, ColumnUpdate{
			Required: iceberg.Optional[bool]{Valid: true, Val: true},
		}).Apply()
		assert.NoError(t, err)
		assert.NotNil(t, newSchema)

		nameField, ok := newSchema.FindFieldByName("name")
		assert.True(t, ok)
		assert.True(t, nameField.Required)
	})

	t.Run("test update column doc", func(t *testing.T) {
		table := New([]string{"id"}, testMetadata, "", nil, nil)
		txn := table.NewTransaction()

		newSchema, err := NewUpdateSchema(txn, true, true).UpdateColumn([]string{"age"}, ColumnUpdate{
			Doc: iceberg.Optional[string]{Valid: true, Val: "User's age in years"},
		}).Apply()
		assert.NoError(t, err)
		assert.NotNil(t, newSchema)

		ageField, ok := newSchema.FindFieldByName("age")
		assert.True(t, ok)
		assert.Equal(t, "User's age in years", ageField.Doc)
	})

	t.Run("test update non-existent column", func(t *testing.T) {
		table := New([]string{"id"}, testMetadata, "", nil, nil)
		txn := table.NewTransaction()

		_, err := NewUpdateSchema(txn, true, true).UpdateColumn([]string{"non_existent"}, ColumnUpdate{
			Doc: iceberg.Optional[string]{Valid: true, Val: "test"},
		}).Apply()
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "field not found")
	})
}

func TestRenameColumn(t *testing.T) {
	t.Run("test rename top level column", func(t *testing.T) {
		table := New([]string{"id"}, testMetadata, "", nil, nil)
		txn := table.NewTransaction()

		newSchema, err := NewUpdateSchema(txn, true, true).RenameColumn([]string{"name"}, "full_name").Apply()
		assert.NoError(t, err)
		assert.NotNil(t, newSchema)

		_, ok := newSchema.FindFieldByName("name")
		assert.False(t, ok)

		field, ok := newSchema.FindFieldByName("full_name")
		assert.True(t, ok)
		assert.Equal(t, iceberg.PrimitiveTypes.String, field.Type)
	})

	t.Run("test rename nested column", func(t *testing.T) {
		table := New([]string{"id"}, testMetadata, "", nil, nil)
		txn := table.NewTransaction()

		newSchema, err := NewUpdateSchema(txn, true, true).RenameColumn([]string{"address", "city"}, "city_name").Apply()
		assert.NoError(t, err)
		assert.NotNil(t, newSchema)

		addressField, ok := newSchema.FindFieldByName("address")
		assert.True(t, ok)

		structType, ok := addressField.Type.(*iceberg.StructType)
		assert.True(t, ok)

		fieldNames := make([]string, len(structType.Fields()))
		for i, field := range structType.Fields() {
			fieldNames[i] = field.Name
		}
		assert.Contains(t, fieldNames, "city_name")
		assert.NotContains(t, fieldNames, "city")
	})

	t.Run("test rename to existing name", func(t *testing.T) {
		table := New([]string{"id"}, testMetadata, "", nil, nil)
		txn := table.NewTransaction()

		_, err := NewUpdateSchema(txn, true, true).RenameColumn([]string{"name"}, "age").Apply()
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "field already exists")
	})
}

func TestMoveColumn(t *testing.T) {
	t.Run("test move column to first", func(t *testing.T) {
		table := New([]string{"id"}, testMetadata, "", nil, nil)
		txn := table.NewTransaction()

		newSchema, err := NewUpdateSchema(txn, true, true).MoveFirst([]string{"age"}).Apply()
		assert.NoError(t, err)
		assert.NotNil(t, newSchema)

		fields := newSchema.Fields()
		assert.Equal(t, "age", fields[0].Name)
		assert.Equal(t, "id", fields[1].Name)
	})

	t.Run("test move column before", func(t *testing.T) {
		table := New([]string{"id"}, testMetadata, "", nil, nil)
		txn := table.NewTransaction()

		newSchema, err := NewUpdateSchema(txn, true, true).MoveBefore([]string{"age"}, []string{"name"}).Apply()
		assert.NoError(t, err)
		assert.NotNil(t, newSchema)

		fields := newSchema.Fields()
		fieldNames := make([]string, len(fields))
		for i, field := range fields {
			fieldNames[i] = field.Name
		}

		ageIndex := -1
		nameIndex := -1
		for i, name := range fieldNames {
			if name == "age" {
				ageIndex = i
			}
			if name == "name" {
				nameIndex = i
			}
		}

		assert.True(t, ageIndex < nameIndex, "age should come before name")
	})

	t.Run("test move column after", func(t *testing.T) {
		table := New([]string{"id"}, testMetadata, "", nil, nil)
		txn := table.NewTransaction()

		newSchema, err := NewUpdateSchema(txn, true, true).MoveAfter([]string{"name"}, []string{"age"}).Apply()
		assert.NoError(t, err)
		assert.NotNil(t, newSchema)

		fields := newSchema.Fields()
		fieldNames := make([]string, len(fields))
		for i, field := range fields {
			fieldNames[i] = field.Name
		}

		ageIndex := -1
		nameIndex := -1
		for i, name := range fieldNames {
			if name == "age" {
				ageIndex = i
			}
			if name == "name" {
				nameIndex = i
			}
		}

		assert.True(t, nameIndex > ageIndex, "name should come after age")
	})

	t.Run("test move non-existent column", func(t *testing.T) {
		table := New([]string{"id"}, testMetadata, "", nil, nil)
		txn := table.NewTransaction()

		_, err := NewUpdateSchema(txn, true, true).MoveFirst([]string{"non_existent"}).Apply()
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "field not found")
	})
}

func TestChainedOperations(t *testing.T) {
	t.Run("test multiple operations in chain", func(t *testing.T) {
		table := New([]string{"id"}, testMetadata, "", nil, nil)
		txn := table.NewTransaction()

		newSchema, err := NewUpdateSchema(txn, true, true).
			AddColumn([]string{"email"}, iceberg.PrimitiveTypes.String, "Email address", false, nil).
			RenameColumn([]string{"name"}, "full_name").
			UpdateColumn([]string{"age"}, ColumnUpdate{
				Required: iceberg.Optional[bool]{Valid: true, Val: true},
			}).
			MoveFirst([]string{"email"}).
			DeleteColumn([]string{"tags"}).
			Apply()

		assert.NoError(t, err)
		assert.NotNil(t, newSchema)

		fields := newSchema.Fields()
		assert.Len(t, fields, 6)

		assert.Equal(t, "email", fields[0].Name)

		_, ok := newSchema.FindFieldByName("name")
		assert.False(t, ok)
		_, ok = newSchema.FindFieldByName("full_name")
		assert.True(t, ok)

		ageField, ok := newSchema.FindFieldByName("age")
		assert.True(t, ok)
		assert.True(t, ageField.Required)

		_, ok = newSchema.FindFieldByName("tags")
		assert.False(t, ok)
	})
}

func TestSetIdentifierField(t *testing.T) {
	t.Run("test set identifier field with single top-level field", func(t *testing.T) {
		table := New([]string{"id"}, testMetadata, "", nil, nil)
		txn := table.NewTransaction()

		// Test that SetIdentifierField returns the same UpdateSchema instance
		updateSchema := NewUpdateSchema(txn, true, true)
		updatedSchema := updateSchema.SetIdentifierField([][]string{{"id"}})
		assert.Equal(t, updateSchema, updatedSchema) // Should return the same instance

		newSchema, err := updatedSchema.Apply()
		assert.NoError(t, err)
		assert.NotNil(t, newSchema)

		// Check that the schema has identifier field IDs set
		assert.Len(t, newSchema.IdentifierFieldIDs, 1)
		if len(newSchema.IdentifierFieldIDs) > 0 {
			assert.Equal(t, 1, newSchema.IdentifierFieldIDs[0]) // id field has ID 1
		}
	})

	t.Run("test set identifier field with multiple fields", func(t *testing.T) {
		table := New([]string{"id"}, testMetadata, "", nil, nil)
		txn := table.NewTransaction()

		newSchema, err := NewUpdateSchema(txn, true, true).SetIdentifierField([][]string{{"id"}, {"name"}}).Apply()
		assert.NoError(t, err)
		assert.NotNil(t, newSchema)

		// Check that the schema has identifier field IDs set
		assert.Len(t, newSchema.IdentifierFieldIDs, 2)
		assert.Contains(t, newSchema.IdentifierFieldIDs, 1) // id field
		assert.Contains(t, newSchema.IdentifierFieldIDs, 2) // name field
	})

	t.Run("test set identifier field with case sensitive matching", func(t *testing.T) {
		table := New([]string{"id"}, testMetadata, "", nil, nil)
		txn := table.NewTransaction()

		_, err := NewUpdateSchema(txn, true, true).SetIdentifierField([][]string{{"ID"}}).Apply()
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "identifier field not found: ID")
	})

	t.Run("test set identifier field with case insensitive matching", func(t *testing.T) {
		table := New([]string{"id"}, testMetadata, "", nil, nil)
		txn := table.NewTransaction()

		newSchema, err := NewUpdateSchema(txn, false, true).SetIdentifierField([][]string{{"ID"}}).Apply()
		assert.NoError(t, err)
		assert.NotNil(t, newSchema)

		// Check that the schema has identifier field IDs set
		assert.Len(t, newSchema.IdentifierFieldIDs, 1)
		assert.Equal(t, 1, newSchema.IdentifierFieldIDs[0]) // id field (case insensitive match)
	})

	t.Run("test set identifier field with non-existent field", func(t *testing.T) {
		table := New([]string{"id"}, testMetadata, "", nil, nil)
		txn := table.NewTransaction()

		_, err := NewUpdateSchema(txn, true, true).SetIdentifierField([][]string{{"non_existent"}}).Apply()
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "identifier field not found: non_existent")
	})

	t.Run("test set identifier field with empty paths", func(t *testing.T) {
		table := New([]string{"id"}, testMetadata, "", nil, nil)
		txn := table.NewTransaction()

		newSchema, err := NewUpdateSchema(txn, true, true).SetIdentifierField([][]string{}).Apply()
		assert.NoError(t, err)
		assert.NotNil(t, newSchema)

		// Check that the schema has no identifier field IDs
		assert.Len(t, newSchema.IdentifierFieldIDs, 0)
	})

	t.Run("test set identifier field chained with other operations", func(t *testing.T) {
		table := New([]string{"id"}, testMetadata, "", nil, nil)
		txn := table.NewTransaction()

		newSchema, err := NewUpdateSchema(txn, true, true).
			AddColumn([]string{"email"}, iceberg.PrimitiveTypes.String, "", false, nil).
			SetIdentifierField([][]string{{"id"}, {"email"}}).
			Apply()
		assert.NoError(t, err)
		assert.NotNil(t, newSchema)

		// Check that the schema has identifier field IDs set
		assert.Len(t, newSchema.IdentifierFieldIDs, 2)
		assert.Contains(t, newSchema.IdentifierFieldIDs, 1)  // id field
		assert.Contains(t, newSchema.IdentifierFieldIDs, 12) // email field (newly added)
	})

	t.Run("test set identifier field with duplicate field paths", func(t *testing.T) {
		table := New([]string{"id"}, testMetadata, "", nil, nil)
		txn := table.NewTransaction()

		newSchema, err := NewUpdateSchema(txn, true, true).SetIdentifierField([][]string{{"id"}, {"id"}}).Apply()
		assert.NoError(t, err)
		assert.NotNil(t, newSchema)

		// Check that the schema has identifier field IDs set (duplicates should be deduplicated)
		assert.Len(t, newSchema.IdentifierFieldIDs, 1)
		assert.Equal(t, 1, newSchema.IdentifierFieldIDs[0]) // id field
	})

	t.Run("test set identifier field replaces existing identifier fields", func(t *testing.T) {
		// Create a schema with existing identifier fields
		schemaWithIdentifiers := iceberg.NewSchemaWithIdentifiers(1, []int{1}, // id is initially an identifier
			iceberg.NestedField{ID: 1, Name: "id", Type: iceberg.PrimitiveTypes.Int32, Required: true, Doc: ""},
			iceberg.NestedField{ID: 2, Name: "name", Type: iceberg.PrimitiveTypes.String, Required: false, Doc: ""},
			iceberg.NestedField{ID: 3, Name: "age", Type: iceberg.PrimitiveTypes.Int32, Required: false, Doc: ""},
		)
		metadata, _ := NewMetadata(schemaWithIdentifiers, nil, UnsortedSortOrder, "", nil)
		table := New([]string{"id"}, metadata, "", nil, nil)
		txn := table.NewTransaction()

		// Set identifier fields to name instead of id
		newSchema, err := NewUpdateSchema(txn, true, true).SetIdentifierField([][]string{{"name"}}).Apply()
		assert.NoError(t, err)
		assert.NotNil(t, newSchema)

		// Check that only name is now an identifier field
		assert.Len(t, newSchema.IdentifierFieldIDs, 1)
		assert.Equal(t, 2, newSchema.IdentifierFieldIDs[0]) // name field has ID 2
	})

	t.Run("test set identifier field multiple times", func(t *testing.T) {
		table := New([]string{"id"}, testMetadata, "", nil, nil)
		txn := table.NewTransaction()

		// Set identifier fields multiple times - last one should win
		newSchema, err := NewUpdateSchema(txn, true, true).
			SetIdentifierField([][]string{{"id"}}).
			SetIdentifierField([][]string{{"name"}}).
			Apply()
		assert.NoError(t, err)
		assert.NotNil(t, newSchema)

		// Check that only the last SetIdentifierField call is applied
		assert.Len(t, newSchema.IdentifierFieldIDs, 1)
		assert.Equal(t, 2, newSchema.IdentifierFieldIDs[0]) // name field has ID 2
	})
}

func TestErrorHandling(t *testing.T) {
	t.Run("test incompatible changes without allowIncompatibleChanges", func(t *testing.T) {
		table := New([]string{"id"}, testMetadata, "", nil, nil)
		txn := table.NewTransaction()

		_, err := NewUpdateSchema(txn, true, false).UpdateColumn([]string{"name"}, ColumnUpdate{
			Required: iceberg.Optional[bool]{Valid: true, Val: true},
		}).Apply()
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "cannot change column nullability from optional to required")
	})

	t.Run("test update geography CRS and edge algorithm without allowIncompatibleChanges", func(t *testing.T) {
		currentGeog, err := iceberg.GeographyTypeOf("srid:4269", "karney")
		assert.NoError(t, err)
		targetGeog, err := iceberg.GeographyTypeOf("srid:4326", "spherical")
		assert.NoError(t, err)

		geoSchema := iceberg.NewSchema(1,
			iceberg.NestedField{ID: 1, Name: "id", Type: iceberg.PrimitiveTypes.Int32, Required: true},
			iceberg.NestedField{ID: 2, Name: "geog", Type: currentGeog, Required: false},
		)
		geoMeta, err := NewMetadata(geoSchema, nil, UnsortedSortOrder, "", iceberg.Properties{
			PropertyFormatVersion: "3",
		})
		assert.NoError(t, err)

		table := New([]string{"geo"}, geoMeta, "", nil, nil)
		txn := table.NewTransaction()

		_, err = NewUpdateSchema(txn, true, false).UpdateColumn([]string{"geog"}, ColumnUpdate{
			FieldType: iceberg.Optional[iceberg.Type]{Valid: true, Val: targetGeog},
		}).Apply()
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "cannot promote geography(srid:4269, karney) to geography(srid:4326)")
	})

	t.Run("test add required field without default value", func(t *testing.T) {
		table := New([]string{"id"}, testMetadata, "", nil, nil)
		txn := table.NewTransaction()

		_, err := NewUpdateSchema(txn, true, false).AddColumn([]string{"required_field"}, iceberg.PrimitiveTypes.String, "", true, nil).Apply()
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "required field required_field has no default value")
	})

	t.Run("test add field with incompatible default value", func(t *testing.T) {
		table := New([]string{"id"}, testMetadata, "", nil, nil)
		txn := table.NewTransaction()

		_, err := NewUpdateSchema(txn, true, true).AddColumn([]string{"age_field"}, iceberg.PrimitiveTypes.String, "", false, iceberg.Int32Literal(25)).Apply()
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "default value type mismatch")
	})
}

func TestBuildUpdates(t *testing.T) {
	t.Run("test BuildUpdates sets correct schema ID when existing schema found", func(t *testing.T) {
		previousSchema := iceberg.NewSchema(0,
			iceberg.NestedField{ID: 1, Name: "id", Type: iceberg.PrimitiveTypes.Int32, Required: true, Doc: ""},
		)

		currentSchema := iceberg.NewSchema(1,
			iceberg.NestedField{ID: 1, Name: "id", Type: iceberg.PrimitiveTypes.Int32, Required: true, Doc: ""},
			iceberg.NestedField{ID: 2, Name: "name", Type: iceberg.PrimitiveTypes.String, Required: false, Doc: ""},
		)

		metadata, _ := NewMetadata(previousSchema, iceberg.UnpartitionedSpec, UnsortedSortOrder, "", nil)
		metadataBuilder, _ := MetadataBuilderFromBase(metadata, "")
		_ = metadataBuilder.AddSchema(currentSchema)
		_ = metadataBuilder.SetCurrentSchemaID(1)
		metadata, _ = metadataBuilder.Build()

		table := New([]string{"table"}, metadata, "", nil, nil)
		txn := table.NewTransaction()
		updateSchema := txn.UpdateSchema(true, false).DeleteColumn([]string{"name"})

		updates, requirements, err := updateSchema.BuildUpdates()
		assert.NoError(t, err)
		assert.NotNil(t, requirements)
		assert.Len(t, requirements, 1)
		assert.IsType(t, &assertCurrentSchemaId{}, requirements[0])
		assert.Equal(t, 1, requirements[0].(*assertCurrentSchemaId).CurrentSchemaID)
		assert.NotNil(t, updates)
		assert.Len(t, updates, 1)
		assert.IsType(t, &setCurrentSchemaUpdate{}, updates[0])
		assert.Equal(t, 0, updates[0].(*setCurrentSchemaUpdate).SchemaID)
	})
}

// TestAddColumnMonotonicFieldIDs exercises the case where the table's
// last-column-id is greater than the current schema's highest field id — for
// example because a previous schema was added that introduced higher ids, or
// because the highest-id columns were later dropped. The Iceberg spec requires
// new field ids to be allocated above last-column-id (never to be reused), so
// AddColumn must seed its id counter from metadata.LastColumnID() rather than
// the current schema's HighestFieldID().
func TestAddColumnMonotonicFieldIDs(t *testing.T) {
	// Start from originalSchema (field ids 1..11, schema id 1).
	baseMeta, err := NewMetadata(originalSchema, iceberg.UnpartitionedSpec, UnsortedSortOrder, "", nil)
	assert.NoError(t, err)

	// Add a second schema that introduces higher field ids. This bumps the
	// metadata's last-column-id to 13 while the current schema is still the
	// original (highest field id 11).
	expanded := iceberg.NewSchema(0,
		iceberg.NestedField{ID: 1, Name: "id", Type: iceberg.PrimitiveTypes.Int32, Required: true},
		iceberg.NestedField{ID: 12, Name: "extra_a", Type: iceberg.PrimitiveTypes.String, Required: false},
		iceberg.NestedField{ID: 13, Name: "extra_b", Type: iceberg.PrimitiveTypes.String, Required: false},
	)

	builder, err := MetadataBuilderFromBase(baseMeta, "")
	assert.NoError(t, err)
	assert.NoError(t, builder.AddSchema(expanded))

	meta, err := builder.Build()
	assert.NoError(t, err)

	assert.Equal(t, 11, meta.CurrentSchema().HighestFieldID(),
		"precondition: current schema should still be the original with highest id 11")
	assert.Equal(t, 13, meta.LastColumnID(),
		"precondition: last-column-id should have been bumped by the expanded schema")

	tbl := New([]string{"id"}, meta, "", nil, nil)
	txn := tbl.NewTransaction()

	newSchema, err := NewUpdateSchema(txn, true, true).
		AddColumn([]string{"fresh"}, iceberg.PrimitiveTypes.String, "", false, nil).
		Apply()
	assert.NoError(t, err)
	assert.NotNil(t, newSchema)

	fresh, ok := newSchema.FindFieldByName("fresh")
	assert.True(t, ok, "new field should be present in the resulting schema")
	assert.Equal(t, 14, fresh.ID,
		"new field id must be allocated above metadata.LastColumnID() (13), not reused from the current schema's highest id (11)")
}

// TestAddColumnAfterDropHighestID is a regression test for #942.
// It reproduces the exact scenario from the original bug: a column is added
// (bumping last-column-id) then dropped (lowering the current schema's
// HighestFieldID back down). A subsequent AddColumn must allocate an id above
// last-column-id, not above HighestFieldID. Reverting #936 (i.e. seeding from
// HighestFieldID instead of LastColumnID) causes the new column to reuse a
// previously assigned id, which violates the Iceberg spec's monotonic id
// invariant.
func TestAddColumnAfterDropHighestID(t *testing.T) {
	// originalSchema has field ids 1..11.
	baseMeta, err := NewMetadata(originalSchema, iceberg.UnpartitionedSpec, UnsortedSortOrder, "", nil)
	assert.NoError(t, err)
	assert.Equal(t, 11, baseMeta.LastColumnID())

	// Step 1: Add a column via UpdateSchema. This allocates id 12 and bumps
	// last-column-id to 12.
	tbl := New([]string{"id"}, baseMeta, "", nil, nil)
	txn := tbl.NewTransaction()

	withExtra, err := NewUpdateSchema(txn, true, true).
		AddColumn([]string{"temp_col"}, iceberg.PrimitiveTypes.String, "", false, nil).
		Apply()
	assert.NoError(t, err)

	tempCol, ok := withExtra.FindFieldByName("temp_col")
	assert.True(t, ok)
	assert.Equal(t, 12, tempCol.ID, "added column should get id 12")

	// Persist the expanded schema into metadata so last-column-id is 12.
	builder1, err := MetadataBuilderFromBase(baseMeta, "")
	assert.NoError(t, err)
	assert.NoError(t, builder1.AddSchema(withExtra))
	assert.NoError(t, builder1.SetCurrentSchemaID(-1))

	afterAdd, err := builder1.Build()
	assert.NoError(t, err)
	assert.Equal(t, 12, afterAdd.LastColumnID())
	assert.Equal(t, 12, afterAdd.CurrentSchema().HighestFieldID())

	// Step 2: Drop the highest-id column. The new current schema's
	// HighestFieldID drops back to 11 while last-column-id stays at 12.
	tbl2 := New([]string{"id"}, afterAdd, "", nil, nil)
	txn2 := tbl2.NewTransaction()

	afterDrop, err := NewUpdateSchema(txn2, true, true).
		DeleteColumn([]string{"temp_col"}).
		Apply()
	assert.NoError(t, err)

	_, found := afterDrop.FindFieldByName("temp_col")
	assert.False(t, found, "temp_col should be gone after drop")

	builder2, err := MetadataBuilderFromBase(afterAdd, "")
	assert.NoError(t, err)
	assert.NoError(t, builder2.AddSchema(afterDrop))
	assert.NoError(t, builder2.SetCurrentSchemaID(-1))

	afterDropMeta, err := builder2.Build()
	assert.NoError(t, err)

	assert.Equal(t, 11, afterDropMeta.CurrentSchema().HighestFieldID(),
		"precondition: current schema's highest id should be 11 after dropping id-12 column")
	assert.Equal(t, 12, afterDropMeta.LastColumnID(),
		"precondition: last-column-id must still be 12 — ids are never reclaimed")

	// Verify schema history still contains the dropped temp_col with id 12.
	// A partial fix that strips dropped fields from historical schemas could
	// still pass the id-monotonicity check above but break interop with
	// Java/PyIceberg/Glue which expect historical schemas to be intact.
	var foundInHistory bool
	for _, s := range afterDropMeta.Schemas() {
		if col, ok := s.FindFieldByID(12); ok {
			assert.Equal(t, "temp_col", col.Name,
				"historical schema must retain temp_col at id 12")
			foundInHistory = true

			break
		}
	}
	assert.True(t, foundInHistory,
		"dropped column (id 12) must still appear in at least one historical schema")

	// Step 3: Add a new column. Its id must be 13 (last-column-id + 1), not
	// 12 (HighestFieldID + 1). Using HighestFieldID would re-assign id 12,
	// colliding with the dropped column still referenced by historical schemas.
	tbl3 := New([]string{"id"}, afterDropMeta, "", nil, nil)
	txn3 := tbl3.NewTransaction()

	finalSchema, err := NewUpdateSchema(txn3, true, true).
		AddColumn([]string{"new_col"}, iceberg.PrimitiveTypes.Int64, "", false, nil).
		Apply()
	assert.NoError(t, err)
	assert.NotNil(t, finalSchema)

	newCol, ok := finalSchema.FindFieldByName("new_col")
	assert.True(t, ok, "new_col should be present in the final schema")
	assert.Equal(t, 13, newCol.ID,
		"new column id must be last-column-id+1 (13), not HighestFieldID+1 (12) — "+
			"reusing 12 would collide with the dropped column still in historical schemas")
}
