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
	"iter"
	"testing"

	"github.com/apache/iceberg-go"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type mockMetadata struct {
	schema       *iceberg.Schema
	lastColumnID int
}

func (m *mockMetadata) Version() int                            { return 2 }
func (m *mockMetadata) TableUUID() uuid.UUID                    { return uuid.UUID{} }
func (m *mockMetadata) Location() string                        { return "s3://test-bucket/test-table" }
func (m *mockMetadata) LastUpdatedMillis() int64                { return 1234567890 }
func (m *mockMetadata) LastColumnID() int                       { return m.lastColumnID }
func (m *mockMetadata) Schemas() []*iceberg.Schema              { return []*iceberg.Schema{m.schema} }
func (m *mockMetadata) CurrentSchema() *iceberg.Schema          { return m.schema }
func (m *mockMetadata) PartitionSpecs() []iceberg.PartitionSpec { return []iceberg.PartitionSpec{} }
func (m *mockMetadata) PartitionSpec() iceberg.PartitionSpec    { return iceberg.NewPartitionSpec() }
func (m *mockMetadata) DefaultPartitionSpec() int               { return 0 }
func (m *mockMetadata) LastPartitionSpecID() *int               { return nil }
func (m *mockMetadata) Snapshots() []Snapshot                   { return []Snapshot{} }
func (m *mockMetadata) CurrentSnapshot() *Snapshot              { return nil }
func (m *mockMetadata) SnapshotByID(int64) *Snapshot            { return nil }
func (m *mockMetadata) SnapshotByName(string) *Snapshot         { return nil }
func (m *mockMetadata) Properties() iceberg.Properties          { return iceberg.Properties{} }
func (m *mockMetadata) SortOrders() []SortOrder                 { return []SortOrder{} }
func (m *mockMetadata) DefaultSortOrder() int                   { return 0 }
func (m *mockMetadata) SortOrder() SortOrder                    { return SortOrder{} }
func (m *mockMetadata) LastSequenceNumber() int64               { return 0 }
func (m *mockMetadata) Ref() SnapshotRef                        { return SnapshotRef{} }
func (m *mockMetadata) Refs() iter.Seq2[string, SnapshotRef] {
	return func(func(string, SnapshotRef) bool) {}
}

func (m *mockMetadata) SnapshotLogs() iter.Seq[SnapshotLogEntry] {
	return func(func(SnapshotLogEntry) bool) {}
}

func (m *mockMetadata) PreviousFiles() iter.Seq[MetadataLogEntry] {
	return func(func(MetadataLogEntry) bool) {}
}
func (m *mockMetadata) NameMapping() iceberg.NameMapping { return nil }
func (m *mockMetadata) Equals(other Metadata) bool       { return false }

func TestUpdateSchemaAddColumn(t *testing.T) {
	structType := &iceberg.StructType{
		FieldList: []iceberg.NestedField{
			{ID: 5, Name: "street", Type: iceberg.PrimitiveTypes.String, Required: true},
			{ID: 6, Name: "city", Type: iceberg.PrimitiveTypes.String, Required: true},
			{ID: 7, Name: "zip_code", Type: iceberg.PrimitiveTypes.String, Required: false},
		},
	}

	schema := iceberg.NewSchema(1,
		iceberg.NestedField{ID: 1, Name: "id", Required: true, Type: iceberg.PrimitiveTypes.Int64},
		iceberg.NestedField{ID: 2, Name: "name", Required: true, Type: iceberg.PrimitiveTypes.String},
		iceberg.NestedField{ID: 3, Name: "age", Required: false, Type: iceberg.PrimitiveTypes.Int32},
		iceberg.NestedField{ID: 4, Name: "address", Required: false, Type: structType},
	)

	metadata := &metadataV2{
		commonMetadata: commonMetadata{
			SchemaList: []*iceberg.Schema{schema},
		},
	}

	txn := &Transaction{
		tbl: &Table{metadata: metadata},
	}

	t.Run("add struct column with incompatible default value", func(t *testing.T) {
		updateSchema := NewUpdateSchema(txn, schema, 7)

		updateSchema.AddColumn([]string{"new_col"}, false, iceberg.PrimitiveTypes.Int32, "doc string", "string_value")
		err := updateSchema.Validate()
		require.Error(t, err)
		assert.Contains(t, err.Error(), "default literal of type string is not assignable to of type int")
	})

	t.Run("add struct column", func(t *testing.T) {
		updateSchema := NewUpdateSchema(txn, schema, 7)

		// Create a struct type with nested fields
		structType := &iceberg.StructType{
			FieldList: []iceberg.NestedField{
				{ID: 8, Name: "street", Type: iceberg.PrimitiveTypes.String, Required: true},
				{ID: 9, Name: "city", Type: iceberg.PrimitiveTypes.String, Required: true},
				{ID: 10, Name: "zip_code", Type: iceberg.PrimitiveTypes.String, Required: false},
			},
		}

		updateSchema.AddColumn([]string{"address_new"}, false, structType, "User address", nil)

		newSchema, err := updateSchema.Build()
		require.NoError(t, err)
		require.NotNil(t, newSchema)

		// Check that the struct field was added
		field, found := newSchema.FindFieldByName("address_new")
		require.True(t, found)
		assert.Equal(t, "address_new", field.Name)
		assert.False(t, field.Required)
		assert.Equal(t, "User address", field.Doc)

		// Verify it's a struct type
		structField, ok := field.Type.(*iceberg.StructType)
		require.True(t, ok)
		assert.Len(t, structField.FieldList, 3)

		// check if we can find the nested fields
		field, found = newSchema.FindFieldByName("address_new.city")
		require.True(t, found)
		assert.Equal(t, "city", field.Name)
		assert.True(t, field.Required)

		// Check nested fields
		assert.Equal(t, "street", structField.FieldList[0].Name)
		assert.Equal(t, iceberg.PrimitiveTypes.String, structField.FieldList[0].Type)
		assert.True(t, structField.FieldList[0].Required)

		assert.Equal(t, "city", structField.FieldList[1].Name)
		assert.Equal(t, iceberg.PrimitiveTypes.String, structField.FieldList[1].Type)
		assert.True(t, structField.FieldList[1].Required)

		assert.Equal(t, "zip_code", structField.FieldList[2].Name)
		assert.Equal(t, iceberg.PrimitiveTypes.String, structField.FieldList[2].Type)
		assert.False(t, structField.FieldList[2].Required)

		// trying to add a new field to the struct
		updateSchema.AddColumn([]string{"address", "new"}, false, iceberg.PrimitiveTypes.String, "new field", nil)

		newSchema, err = updateSchema.Build()
		require.NoError(t, err)
		require.NotNil(t, newSchema)

		field, found = newSchema.FindFieldByName("address.new")
		require.True(t, found)
	})

	t.Run("add simple column", func(t *testing.T) {
		updateSchema := NewUpdateSchema(txn, schema, 7)

		updateSchema.AddColumn([]string{"email"}, false, iceberg.PrimitiveTypes.String, "User email", "default@example.com")

		newSchema, err := updateSchema.Build()
		require.NoError(t, err)
		require.NotNil(t, newSchema)

		// Check that the new field was added
		field, found := newSchema.FindFieldByName("email")
		require.True(t, found)
		assert.Equal(t, "email", field.Name)
		assert.Equal(t, iceberg.PrimitiveTypes.String, field.Type)
		assert.False(t, field.Required)
		assert.Equal(t, "User email", field.Doc)
		assert.Equal(t, "default@example.com", field.InitialDefault)
	})

	t.Run("add required column without default should fail", func(t *testing.T) {
		updateSchema := NewUpdateSchema(txn, schema, 7)

		updateSchema.AddColumn([]string{"required_field"}, true, iceberg.PrimitiveTypes.String, "", nil)
		err := updateSchema.Validate()
		require.Error(t, err)
		assert.Contains(t, err.Error(), "cannot add required column without default value")
	})

	t.Run("add required column with default should succeed", func(t *testing.T) {
		updateSchema := NewUpdateSchema(txn, schema, 7)

		updateSchema.AddColumn([]string{"required_field"}, true, iceberg.PrimitiveTypes.String, "", "default")

		newSchema, err := updateSchema.Build()
		require.NoError(t, err)

		field, found := newSchema.FindFieldByName("required_field")
		require.True(t, found)
		assert.True(t, field.Required)
		assert.Equal(t, "default", field.InitialDefault)
	})

	t.Run("add duplicate column should fail", func(t *testing.T) {
		updateSchema := NewUpdateSchema(txn, schema, 7)

		updateSchema.AddColumn([]string{"name"}, false, iceberg.PrimitiveTypes.String, "", nil)
		err := updateSchema.Validate()
		require.Error(t, err)
		assert.Contains(t, err.Error(), "cannot add column; name already exists")
	})

	t.Run("empty path should fail", func(t *testing.T) {
		updateSchema := NewUpdateSchema(txn, schema, 7)

		updateSchema.AddColumn([]string{}, false, iceberg.PrimitiveTypes.String, "", nil)
		err := updateSchema.Validate()
		require.Error(t, err)
		assert.Contains(t, err.Error(), "path must contain at least the new column name")
	})

	t.Run("add to non-struct parent should fail", func(t *testing.T) {
		updateSchema := NewUpdateSchema(txn, schema, 7)

		updateSchema.AddColumn([]string{"name", "nested"}, false, iceberg.PrimitiveTypes.String, "", nil)
		err := updateSchema.Validate()
		require.Error(t, err)
		assert.Contains(t, err.Error(), "parent is not a nested type")
	})

	t.Run("add to non-existent parent should fail", func(t *testing.T) {
		updateSchema := NewUpdateSchema(txn, schema, 7)

		updateSchema.AddColumn([]string{"nonexistent", "nested"}, false, iceberg.PrimitiveTypes.String, "", nil)
		err := updateSchema.Validate()
		require.Error(t, err)
		assert.Contains(t, err.Error(), "cannot find parent struct")
	})
}

func TestUpdateSchemaDeleteColumn(t *testing.T) {
	schema := iceberg.NewSchema(1,
		iceberg.NestedField{ID: 1, Name: "id", Required: true, Type: iceberg.PrimitiveTypes.Int64},
		iceberg.NestedField{ID: 2, Name: "name", Required: true, Type: iceberg.PrimitiveTypes.String},
		iceberg.NestedField{ID: 3, Name: "age", Required: false, Type: iceberg.PrimitiveTypes.Int32},
	)

	metadata := &metadataV2{
		commonMetadata: commonMetadata{
			SchemaList: []*iceberg.Schema{schema},
		},
	}

	txn := &Transaction{
		tbl: &Table{metadata: metadata},
	}

	t.Run("delete existing column", func(t *testing.T) {
		updateSchema := NewUpdateSchema(txn, schema, 3)

		updateSchema.DeleteColumn([]string{"age"})

		newSchema, err := updateSchema.Build()
		require.NoError(t, err)

		_, found := newSchema.FindFieldByName("age")
		assert.False(t, found, "age field should be deleted")

		// Other fields should still exist
		_, found = newSchema.FindFieldByName("id")
		assert.True(t, found)
		_, found = newSchema.FindFieldByName("name")
		assert.True(t, found)
	})

	t.Run("delete non-existent column should fail", func(t *testing.T) {
		updateSchema := NewUpdateSchema(txn, schema, 3)

		updateSchema.DeleteColumn([]string{"nonexistent"})
		err := updateSchema.Validate()
		require.Error(t, err)
		assert.Contains(t, err.Error(), "cannot delete missing column")
	})

	t.Run("delete and then update same column should fail", func(t *testing.T) {
		updateSchema := NewUpdateSchema(txn, schema, 3)

		updateSchema.UpdateColumn([]string{"age"}, ColumnUpdate{
			Doc: iceberg.Optional[string]{Val: "new doc", Valid: true},
		})
		updateSchema.DeleteColumn([]string{"age"})

		_, err := updateSchema.Build()
		require.Error(t, err)
		assert.Contains(t, err.Error(), "cannot delete a column that has updates")
	})
}

func TestUpdateSchemaUpdateColumnType(t *testing.T) {
	schema := iceberg.NewSchema(1,
		iceberg.NestedField{ID: 1, Name: "id", Required: true, Type: iceberg.PrimitiveTypes.Int32},
		iceberg.NestedField{ID: 2, Name: "name", Required: true, Type: iceberg.PrimitiveTypes.String},
		iceberg.NestedField{ID: 3, Name: "price", Required: false, Type: iceberg.PrimitiveTypes.Float32},
	)

	metadata := &metadataV2{
		commonMetadata: commonMetadata{
			SchemaList: []*iceberg.Schema{schema},
		},
	}

	txn := &Transaction{
		tbl: &Table{metadata: metadata},
	}

	t.Run("valid type promotion int to long", func(t *testing.T) {
		updateSchema := NewUpdateSchema(txn, schema, 3)

		updateSchema.UpdateColumn([]string{"id"}, ColumnUpdate{
			Type: iceberg.Optional[iceberg.Type]{Val: iceberg.PrimitiveTypes.Int64, Valid: true},
		})

		newSchema, err := updateSchema.Build()
		require.NoError(t, err)

		field, found := newSchema.FindFieldByName("id")
		require.True(t, found)
		assert.Equal(t, iceberg.PrimitiveTypes.Int64, field.Type)
	})

	t.Run("valid type promotion float to double", func(t *testing.T) {
		updateSchema := NewUpdateSchema(txn, schema, 3)

		updateSchema.UpdateColumn([]string{"price"}, ColumnUpdate{
			Type: iceberg.Optional[iceberg.Type]{Val: iceberg.PrimitiveTypes.Float64, Valid: true},
		})

		newSchema, err := updateSchema.Build()
		require.NoError(t, err)

		field, found := newSchema.FindFieldByName("price")
		require.True(t, found)
		assert.Equal(t, iceberg.PrimitiveTypes.Float64, field.Type)
	})

	t.Run("invalid type change should fail", func(t *testing.T) {
		updateSchema := NewUpdateSchema(txn, schema, 3)

		updateSchema.UpdateColumn([]string{"name"}, ColumnUpdate{
			Type: iceberg.Optional[iceberg.Type]{Val: iceberg.PrimitiveTypes.Int32, Valid: true},
		})

		err := updateSchema.Validate()
		require.Error(t, err)
		assert.Contains(t, err.Error(), "cannot update type of column")
	})

	t.Run("update non-existent column should fail", func(t *testing.T) {
		updateSchema := NewUpdateSchema(txn, schema, 3)

		updateSchema.UpdateColumn([]string{"nonexistent"}, ColumnUpdate{
			Type: iceberg.Optional[iceberg.Type]{Val: iceberg.PrimitiveTypes.String, Valid: true},
		})

		err := updateSchema.Validate()
		require.Error(t, err)
		assert.Contains(t, err.Error(), "cannot update missing column")
	})
}

func TestUpdateSchemaUpdateColumnDoc(t *testing.T) {
	schema := iceberg.NewSchema(1,
		iceberg.NestedField{ID: 1, Name: "id", Required: true, Type: iceberg.PrimitiveTypes.Int64},
		iceberg.NestedField{ID: 2, Name: "name", Required: true, Type: iceberg.PrimitiveTypes.String, Doc: "old doc"},
	)

	metadata := &metadataV2{
		commonMetadata: commonMetadata{
			SchemaList: []*iceberg.Schema{schema},
		},
	}

	txn := &Transaction{
		tbl: &Table{metadata: metadata},
	}

	t.Run("update documentation", func(t *testing.T) {
		updateSchema := NewUpdateSchema(txn, schema, 2)

		newDoc := "Updated documentation"
		updateSchema.UpdateColumn([]string{"name"}, ColumnUpdate{
			Doc: iceberg.Optional[string]{Val: newDoc, Valid: true},
		})

		newSchema, err := updateSchema.Build()
		require.NoError(t, err)

		field, found := newSchema.FindFieldByName("name")
		require.True(t, found)
		assert.Equal(t, newDoc, field.Doc)
	})

	t.Run("update with same doc should not error", func(t *testing.T) {
		updateSchema := NewUpdateSchema(txn, schema, 2)

		sameDoc := "old doc"
		updateSchema.UpdateColumn([]string{"name"}, ColumnUpdate{
			Doc: iceberg.Optional[string]{Val: sameDoc, Valid: true},
		})

		_, err := updateSchema.Build()
		require.NoError(t, err)
	})

	t.Run("update non-existent column should fail", func(t *testing.T) {
		updateSchema := NewUpdateSchema(txn, schema, 2)

		newDoc := "new doc"
		updateSchema.UpdateColumn([]string{"nonexistent"}, ColumnUpdate{
			Doc: iceberg.Optional[string]{Val: newDoc, Valid: true},
		})

		err := updateSchema.Validate()
		require.Error(t, err)
		assert.Contains(t, err.Error(), "cannot update missing column")
	})
}

func TestUpdateSchemaUpdateColumnDefault(t *testing.T) {
	schema := iceberg.NewSchema(1,
		iceberg.NestedField{ID: 1, Name: "id", Required: true, Type: iceberg.PrimitiveTypes.Int64},
		iceberg.NestedField{ID: 2, Name: "status", Required: false, Type: iceberg.PrimitiveTypes.String, InitialDefault: "active"},
	)

	metadata := &metadataV2{
		commonMetadata: commonMetadata{
			SchemaList: []*iceberg.Schema{schema},
		},
	}

	txn := &Transaction{
		tbl: &Table{metadata: metadata},
	}

	t.Run("update default value", func(t *testing.T) {
		updateSchema := NewUpdateSchema(txn, schema, 2)

		newDefault := "inactive"
		updateSchema.UpdateColumn([]string{"status"}, ColumnUpdate{
			Default: newDefault,
		})

		newSchema, err := updateSchema.Build()
		require.NoError(t, err)

		field, found := newSchema.FindFieldByName("status")
		require.True(t, found)
		assert.Equal(t, newDefault, field.InitialDefault)
	})

	t.Run("set default value to nil removes it", func(t *testing.T) {
		updateSchema := NewUpdateSchema(txn, schema, 2)

		// Since the Default field is of type any, we expect nil to be handled differently
		// This test verifies that nil values are not applied (no change)
		updateSchema.UpdateColumn([]string{"status"}, ColumnUpdate{
			Default: nil,
		})

		newSchema, err := updateSchema.Build()
		require.NoError(t, err)

		field, found := newSchema.FindFieldByName("status")
		require.True(t, found)
		// Since nil means "no change", the original default should remain
		assert.Equal(t, "active", field.InitialDefault)
	})

	t.Run("update with same default should not error", func(t *testing.T) {
		updateSchema := NewUpdateSchema(txn, schema, 2)

		sameDefault := "active"
		updateSchema.UpdateColumn([]string{"status"}, ColumnUpdate{
			Default: sameDefault,
		})

		_, err := updateSchema.Build()
		require.NoError(t, err)
	})

	t.Run("update non-existent column should fail", func(t *testing.T) {
		updateSchema := NewUpdateSchema(txn, schema, 2)

		updateSchema.UpdateColumn([]string{"nonexistent"}, ColumnUpdate{
			Default: "default",
		})

		err := updateSchema.Validate()
		require.Error(t, err)
		assert.Contains(t, err.Error(), "cannot update missing column")
	})
}

func TestUpdateSchemaRequireColumn(t *testing.T) {
	schema := iceberg.NewSchema(1,
		iceberg.NestedField{ID: 1, Name: "id", Required: true, Type: iceberg.PrimitiveTypes.Int64},
		iceberg.NestedField{ID: 2, Name: "optional_field", Required: false, Type: iceberg.PrimitiveTypes.String},
	)

	metadata := &metadataV2{
		commonMetadata: commonMetadata{
			SchemaList: []*iceberg.Schema{schema},
		},
	}

	txn := &Transaction{
		tbl: &Table{metadata: metadata},
	}

	t.Run("make column required without allowing incompatible changes should fail", func(t *testing.T) {
		updateSchema := NewUpdateSchema(txn, schema, 2)

		updateSchema.UpdateColumn([]string{"optional_field"}, ColumnUpdate{
			Required: iceberg.Optional[bool]{Val: true, Valid: true},
		})

		err := updateSchema.Validate()
		require.Error(t, err)
		assert.Contains(t, err.Error(), "cannot change column nullability")
	})

	t.Run("make column required with allowing incompatible changes", func(t *testing.T) {
		updateSchema := NewUpdateSchema(txn, schema, 2)
		updateSchema.AllowIncompatibleChanges()

		updateSchema.UpdateColumn([]string{"optional_field"}, ColumnUpdate{
			Required: iceberg.Optional[bool]{Val: true, Valid: true},
		})

		newSchema, err := updateSchema.Build()
		require.NoError(t, err)

		field, found := newSchema.FindFieldByName("optional_field")
		require.True(t, found)
		assert.True(t, field.Required)
	})
}

func TestUpdateSchemaMakeColumnOptional(t *testing.T) {
	schema := iceberg.NewSchema(1,
		iceberg.NestedField{ID: 1, Name: "id", Required: true, Type: iceberg.PrimitiveTypes.Int64},
		iceberg.NestedField{ID: 2, Name: "required_field", Required: true, Type: iceberg.PrimitiveTypes.String},
	)

	metadata := &metadataV2{
		commonMetadata: commonMetadata{
			SchemaList: []*iceberg.Schema{schema},
		},
	}

	txn := &Transaction{
		tbl: &Table{metadata: metadata},
	}

	t.Run("make column optional", func(t *testing.T) {
		updateSchema := NewUpdateSchema(txn, schema, 2)

		updateSchema.UpdateColumn([]string{"required_field"}, ColumnUpdate{
			Required: iceberg.Optional[bool]{Val: false, Valid: true},
		})

		newSchema, err := updateSchema.Build()
		require.NoError(t, err)

		field, found := newSchema.FindFieldByName("required_field")
		require.True(t, found)
		assert.False(t, field.Required)
	})
}

func TestUpdateSchemaAllowIncompatibleChanges(t *testing.T) {
	schema := iceberg.NewSchema(1,
		iceberg.NestedField{ID: 1, Name: "id", Required: true, Type: iceberg.PrimitiveTypes.Int64},
	)

	metadata := &metadataV2{
		commonMetadata: commonMetadata{
			SchemaList: []*iceberg.Schema{schema},
		},
	}

	txn := &Transaction{
		tbl: &Table{metadata: metadata},
	}

	t.Run("allow incompatible changes should enable adding required column without default", func(t *testing.T) {
		updateSchema := NewUpdateSchema(txn, schema, 1)
		updateSchema.AllowIncompatibleChanges()

		updateSchema.AddColumn([]string{"required_field"}, true, iceberg.PrimitiveTypes.String, "", nil)

		newSchema, err := updateSchema.Build()
		require.NoError(t, err)

		field, found := newSchema.FindFieldByName("required_field")
		require.True(t, found)
		assert.True(t, field.Required)
		assert.Nil(t, field.InitialDefault)
	})
}

func TestUpdateSchemaCaseSensitivity(t *testing.T) {
	schema := iceberg.NewSchema(1,
		iceberg.NestedField{ID: 1, Name: "id", Required: true, Type: iceberg.PrimitiveTypes.Int64},
		iceberg.NestedField{ID: 2, Name: "Name", Required: true, Type: iceberg.PrimitiveTypes.String},
	)

	metadata := &metadataV2{
		commonMetadata: commonMetadata{
			SchemaList: []*iceberg.Schema{schema},
		},
	}

	txn := &Transaction{
		tbl: &Table{metadata: metadata},
	}

	t.Run("case sensitive search by default", func(t *testing.T) {
		updateSchema := NewUpdateSchema(txn, schema, 2)

		// Should work with exact case
		updateSchema.UpdateColumn([]string{"Name"}, ColumnUpdate{
			Doc: iceberg.Optional[string]{Val: "updated", Valid: true},
		})

		_, err := updateSchema.Build()
		require.NoError(t, err)

		// Should fail with different case
		updateSchema2 := NewUpdateSchema(txn, schema, 2)
		updateSchema2.UpdateColumn([]string{"name"}, ColumnUpdate{
			Doc: iceberg.Optional[string]{Val: "updated", Valid: true},
		})

		err = updateSchema2.Validate()
		require.Error(t, err)
		assert.Contains(t, err.Error(), "cannot update missing column")
	})

	t.Run("case insensitive search when configured", func(t *testing.T) {
		updateSchema := NewUpdateSchema(txn, schema, 2)
		updateSchema.SetCaseSensitive(false)

		// Should work with different case
		updateSchema.UpdateColumn([]string{"name"}, ColumnUpdate{
			Doc: iceberg.Optional[string]{Val: "updated", Valid: true},
		})

		newSchema, err := updateSchema.Build()
		require.NoError(t, err)

		field, found := newSchema.FindFieldByName("Name")
		require.True(t, found)
		assert.Equal(t, "updated", field.Doc)
	})
}

func TestUpdateSchemaApplyChanges(t *testing.T) {
	schema := iceberg.NewSchema(1,
		iceberg.NestedField{ID: 1, Name: "id", Required: true, Type: iceberg.PrimitiveTypes.Int64},
		iceberg.NestedField{ID: 2, Name: "name", Required: true, Type: iceberg.PrimitiveTypes.String},
		iceberg.NestedField{ID: 3, Name: "age", Required: false, Type: iceberg.PrimitiveTypes.Int32},
	)

	metadata := &metadataV2{
		commonMetadata: commonMetadata{
			SchemaList: []*iceberg.Schema{schema},
		},
	}

	txn := &Transaction{
		tbl: &Table{metadata: metadata},
	}

	t.Run("apply multiple changes", func(t *testing.T) {
		updateSchema := NewUpdateSchema(txn, schema, 3)

		// Add a new column
		updateSchema.AddColumn([]string{"email"}, false, iceberg.PrimitiveTypes.String, "User email", nil)

		// Update documentation
		doc := "Updated name field"
		updateSchema.UpdateColumn([]string{"name"}, ColumnUpdate{
			Doc: iceberg.Optional[string]{Val: doc, Valid: true},
		})

		// Delete a column
		updateSchema.DeleteColumn([]string{"age"})

		newSchema, err := updateSchema.Build()
		require.NoError(t, err)

		// Verify changes were applied
		assert.Equal(t, 2, newSchema.ID) // Schema ID should increment

		// Check added field
		emailField, found := newSchema.FindFieldByName("email")
		require.True(t, found)
		assert.Equal(t, "User email", emailField.Doc)

		// Check updated field
		nameField, found := newSchema.FindFieldByName("name")
		require.True(t, found)
		assert.Equal(t, "Updated name field", nameField.Doc)

		// Check deleted field
		_, found = newSchema.FindFieldByName("age")
		assert.False(t, found)

		// Check remaining field
		_, found = newSchema.FindFieldByName("id")
		assert.True(t, found)
	})

	t.Run("validate without building", func(t *testing.T) {
		updateSchema := NewUpdateSchema(txn, schema, 3)

		// Add valid operations
		updateSchema.AddColumn([]string{"email"}, false, iceberg.PrimitiveTypes.String, "User email", nil)
		updateSchema.UpdateColumn([]string{"name"}, ColumnUpdate{
			Doc: iceberg.Optional[string]{Val: "updated", Valid: true},
		})

		// Should pass validation
		err := updateSchema.Validate()
		require.NoError(t, err)

		// Add invalid operation
		updateSchema.AddColumn([]string{"name"}, false, iceberg.PrimitiveTypes.String, "duplicate", nil)

		// Should fail validation
		err = updateSchema.Validate()
		require.Error(t, err)
		assert.Contains(t, err.Error(), "cannot add column; name already exists")
	})

	t.Run("reset and remove operations", func(t *testing.T) {
		updateSchema := NewUpdateSchema(txn, schema, 3)

		// Add some operations
		updateSchema.AddColumn([]string{"email"}, false, iceberg.PrimitiveTypes.String, "email", nil)
		updateSchema.AddColumn([]string{"phone"}, false, iceberg.PrimitiveTypes.String, "phone", nil)

		operations := updateSchema.GetQueuedOperations()
		assert.Len(t, operations, 2)

		// Remove last operation
		updateSchema.RemoveLastOperation()
		operations = updateSchema.GetQueuedOperations()
		assert.Len(t, operations, 1)
		assert.Contains(t, operations[0], "email")

		// Reset all operations
		updateSchema.Reset()
		operations = updateSchema.GetQueuedOperations()
		assert.Len(t, operations, 0)
	})
}

func TestUpdateSchemaMove(t *testing.T) {
	// Create a more complex schema with nested structures for comprehensive testing
	addressType := &iceberg.StructType{
		FieldList: []iceberg.NestedField{
			{ID: 5, Name: "street", Type: iceberg.PrimitiveTypes.String, Required: true},
			{ID: 6, Name: "city", Type: iceberg.PrimitiveTypes.String, Required: true},
			{ID: 7, Name: "zip_code", Type: iceberg.PrimitiveTypes.String, Required: false},
		},
	}

	schema := iceberg.NewSchema(1,
		iceberg.NestedField{ID: 1, Name: "id", Required: true, Type: iceberg.PrimitiveTypes.Int64},
		iceberg.NestedField{ID: 2, Name: "name", Required: true, Type: iceberg.PrimitiveTypes.String},
		iceberg.NestedField{ID: 3, Name: "age", Required: false, Type: iceberg.PrimitiveTypes.Int32},
		iceberg.NestedField{ID: 4, Name: "address", Required: false, Type: addressType},
		iceberg.NestedField{ID: 8, Name: "email", Required: false, Type: iceberg.PrimitiveTypes.String},
	)

	metadata := &metadataV2{
		commonMetadata: commonMetadata{
			SchemaList: []*iceberg.Schema{schema},
		},
	}

	txn := &Transaction{
		tbl: &Table{metadata: metadata},
	}

	t.Run("move column to first position", func(t *testing.T) {
		updateSchema := NewUpdateSchema(txn, schema, 8)

		// Move 'email' to first position
		updateSchema.Move([]string{"email"}, nil, OpFirst)

		newSchema, err := updateSchema.Build()
		require.NoError(t, err)

		fields := newSchema.AsStruct().FieldList

		// Email should now be first
		assert.Equal(t, "email", fields[0].Name)
		assert.Equal(t, "id", fields[1].Name)
		assert.Equal(t, "name", fields[2].Name)
		assert.Equal(t, "age", fields[3].Name)
		assert.Equal(t, "address", fields[4].Name)
	})

	t.Run("move column before another column", func(t *testing.T) {
		updateSchema := NewUpdateSchema(txn, schema, 8)

		// Move 'email' before 'name'
		updateSchema.Move([]string{"email"}, []string{"name"}, OpBefore)

		newSchema, err := updateSchema.Build()
		require.NoError(t, err)

		fields := newSchema.AsStruct().FieldList

		// Email should now be before name
		assert.Equal(t, "id", fields[0].Name)
		assert.Equal(t, "email", fields[1].Name)
		assert.Equal(t, "name", fields[2].Name)
		assert.Equal(t, "age", fields[3].Name)
		assert.Equal(t, "address", fields[4].Name)
	})

	t.Run("move column after another column", func(t *testing.T) {
		updateSchema := NewUpdateSchema(txn, schema, 8)

		// Move 'age' after 'name'
		updateSchema.Move([]string{"age"}, []string{"name"}, OpAfter)

		newSchema, err := updateSchema.Build()
		require.NoError(t, err)

		fields := newSchema.AsStruct().FieldList

		// Age should now be after name
		assert.Equal(t, "id", fields[0].Name)
		assert.Equal(t, "name", fields[1].Name)
		assert.Equal(t, "age", fields[2].Name)
		assert.Equal(t, "address", fields[3].Name)
		assert.Equal(t, "email", fields[4].Name)
	})

	t.Run("move nested column within struct", func(t *testing.T) {
		updateSchema := NewUpdateSchema(txn, schema, 8)

		// Move 'zip_code' to first position within address struct
		updateSchema.Move([]string{"address", "zip_code"}, nil, OpFirst)

		newSchema, err := updateSchema.Build()
		require.NoError(t, err)

		addressField, found := newSchema.FindFieldByName("address")
		require.True(t, found)

		addressStruct, ok := addressField.Type.(*iceberg.StructType)
		require.True(t, ok)

		// zip_code should now be first in the struct
		assert.Equal(t, "zip_code", addressStruct.FieldList[0].Name)
		assert.Equal(t, "street", addressStruct.FieldList[1].Name)
		assert.Equal(t, "city", addressStruct.FieldList[2].Name)
	})

	t.Run("move nested column before another nested column", func(t *testing.T) {
		updateSchema := NewUpdateSchema(txn, schema, 8)

		// Move 'city' before 'street' within address struct
		updateSchema.Move([]string{"address", "city"}, []string{"address", "street"}, OpBefore)

		newSchema, err := updateSchema.Build()
		require.NoError(t, err)

		addressField, found := newSchema.FindFieldByName("address")
		require.True(t, found)

		addressStruct, ok := addressField.Type.(*iceberg.StructType)
		require.True(t, ok)

		// city should now be before street
		assert.Equal(t, "city", addressStruct.FieldList[0].Name)
		assert.Equal(t, "street", addressStruct.FieldList[1].Name)
		assert.Equal(t, "zip_code", addressStruct.FieldList[2].Name)
	})

	t.Run("multiple moves in same update", func(t *testing.T) {
		updateSchema := NewUpdateSchema(txn, schema, 8)

		// Move email to first
		updateSchema.Move([]string{"email"}, nil, OpFirst)

		// Then move age after name
		updateSchema.Move([]string{"age"}, []string{"name"}, OpAfter)

		newSchema, err := updateSchema.Build()
		require.NoError(t, err)

		fields := newSchema.AsStruct().FieldList

		// Verify both moves were applied
		assert.Equal(t, "email", fields[0].Name)
		assert.Equal(t, "id", fields[1].Name)
		assert.Equal(t, "name", fields[2].Name)
		assert.Equal(t, "age", fields[3].Name)
		assert.Equal(t, "address", fields[4].Name)
	})

	t.Run("move non-existent column should fail", func(t *testing.T) {
		updateSchema := NewUpdateSchema(txn, schema, 8)

		updateSchema.Move([]string{"nonexistent"}, []string{"name"}, OpBefore)

		err := updateSchema.Validate()
		require.Error(t, err)
		assert.Contains(t, err.Error(), "cannot move missing column")
	})

	t.Run("move before non-existent reference column should fail", func(t *testing.T) {
		updateSchema := NewUpdateSchema(txn, schema, 8)

		updateSchema.Move([]string{"email"}, []string{"nonexistent"}, OpBefore)

		err := updateSchema.Validate()
		require.Error(t, err)
		assert.Contains(t, err.Error(), "reference column for move not found")
	})

	t.Run("move after non-existent reference column should fail", func(t *testing.T) {
		updateSchema := NewUpdateSchema(txn, schema, 8)

		updateSchema.Move([]string{"email"}, []string{"nonexistent"}, OpAfter)

		err := updateSchema.Validate()
		require.Error(t, err)
		assert.Contains(t, err.Error(), "reference column for move not found")
	})

	t.Run("move column relative to itself should fail", func(t *testing.T) {
		updateSchema := NewUpdateSchema(txn, schema, 8)

		updateSchema.Move([]string{"email"}, []string{"email"}, OpBefore)

		err := updateSchema.Validate()
		require.Error(t, err)
		assert.Contains(t, err.Error(), "cannot move column relative to itself")
	})

	t.Run("move columns with different parents should fail", func(t *testing.T) {
		updateSchema := NewUpdateSchema(txn, schema, 8)

		// Try to move a top-level column relative to a nested column
		updateSchema.Move([]string{"email"}, []string{"address", "street"}, OpBefore)

		err := updateSchema.Validate()
		require.Error(t, err)
		assert.Contains(t, err.Error(), "cannot move column across different parent structs")
	})

	t.Run("move with add and delete operations", func(t *testing.T) {
		updateSchema := NewUpdateSchema(txn, schema, 8)

		// Add a new column
		updateSchema.AddColumn([]string{"phone"}, false, iceberg.PrimitiveTypes.String, "Phone number", nil)

		// Delete a column
		updateSchema.DeleteColumn([]string{"age"})

		// Move existing email before name (can't move newly added columns yet)
		updateSchema.Move([]string{"email"}, []string{"name"}, OpBefore)

		newSchema, err := updateSchema.Build()
		require.NoError(t, err)

		fields := newSchema.AsStruct().FieldList

		// Verify operations were applied correctly
		assert.Equal(t, "id", fields[0].Name)
		assert.Equal(t, "email", fields[1].Name)
		assert.Equal(t, "name", fields[2].Name)
		assert.Equal(t, "address", fields[3].Name)
		assert.Equal(t, "phone", fields[4].Name) // newly added column appears at the end

		// Verify age was deleted
		_, found := newSchema.FindFieldByName("age")
		assert.False(t, found)

		// Verify phone was added
		phoneField, found := newSchema.FindFieldByName("phone")
		require.True(t, found)
		assert.Equal(t, "Phone number", phoneField.Doc)
	})
}

func TestUpdateSchemaBackwardCompatibility(t *testing.T) {
	schema := iceberg.NewSchema(1,
		iceberg.NestedField{ID: 1, Name: "id", Required: true, Type: iceberg.PrimitiveTypes.Int64},
		iceberg.NestedField{ID: 2, Name: "name", Required: true, Type: iceberg.PrimitiveTypes.String},
	)

	metadata := &metadataV2{
		commonMetadata: commonMetadata{
			SchemaList: []*iceberg.Schema{schema},
		},
	}

	txn := &Transaction{
		tbl: &Table{metadata: metadata},
	}

	t.Run("Apply method should panic on validation errors", func(t *testing.T) {
		updateSchema := NewUpdateSchema(txn, schema, 2)

		// Add invalid operation
		updateSchema.AddColumn([]string{}, false, iceberg.PrimitiveTypes.String, "", nil)

		// Apply should panic
		_, err := updateSchema.Build()
		require.Error(t, err)
	})

	t.Run("Apply method should return schema on success", func(t *testing.T) {
		updateSchema := NewUpdateSchema(txn, schema, 2)

		// Add valid operation
		updateSchema.AddColumn([]string{"email"}, false, iceberg.PrimitiveTypes.String, "email", nil)

		// Apply should succeed
		newSchema, err := updateSchema.Build()
		require.Error(t, err)
		require.NotNil(t, newSchema)

		field, found := newSchema.FindFieldByName("email")
		require.True(t, found)
		assert.Equal(t, "email", field.Name)
	})
}

func TestUpdateSchemaComplexScenarios(t *testing.T) {
	addressType := &iceberg.StructType{
		FieldList: []iceberg.NestedField{
			{ID: 5, Name: "street", Type: iceberg.PrimitiveTypes.String, Required: true},
			{ID: 6, Name: "city", Type: iceberg.PrimitiveTypes.String, Required: true},
		},
	}

	schema := iceberg.NewSchema(1,
		iceberg.NestedField{ID: 1, Name: "id", Required: true, Type: iceberg.PrimitiveTypes.Int64},
		iceberg.NestedField{ID: 2, Name: "name", Required: true, Type: iceberg.PrimitiveTypes.String},
		iceberg.NestedField{ID: 3, Name: "age", Required: false, Type: iceberg.PrimitiveTypes.Int32},
		iceberg.NestedField{ID: 4, Name: "address", Required: false, Type: addressType},
	)

	metadata := &metadataV2{
		commonMetadata: commonMetadata{
			SchemaList: []*iceberg.Schema{schema},
		},
	}

	txn := &Transaction{
		tbl: &Table{metadata: metadata},
	}

	t.Run("complex schema evolution scenario", func(t *testing.T) {
		updateSchema := NewUpdateSchema(txn, schema, 6)

		// 1. Add email field
		updateSchema.AddColumn([]string{"email"}, false, iceberg.PrimitiveTypes.String, "User email", "default@example.com")

		// 2. Update name documentation
		updateSchema.UpdateColumn([]string{"name"}, ColumnUpdate{
			Doc: iceberg.Optional[string]{Val: "Updated user name", Valid: true},
		})

		// 3. Promote age from int32 to int64
		updateSchema.UpdateColumn([]string{"age"}, ColumnUpdate{
			Type: iceberg.Optional[iceberg.Type]{Val: iceberg.PrimitiveTypes.Int64, Valid: true},
		})

		// 4. Add zip_code to address struct
		updateSchema.AddColumn([]string{"address", "zip_code"}, false, iceberg.PrimitiveTypes.String, "ZIP code", nil)

		// Note: Moving newly added columns is not currently supported in the same transaction
		// Only move existing columns

		newSchema, err := updateSchema.Build()
		require.NoError(t, err)

		// Verify all changes
		fields := newSchema.AsStruct().FieldList

		// Check field order at top level (new columns appear at the end)
		assert.Equal(t, "id", fields[0].Name)
		assert.Equal(t, "name", fields[1].Name)
		assert.Equal(t, "age", fields[2].Name)
		assert.Equal(t, "address", fields[3].Name)
		assert.Equal(t, "email", fields[4].Name) // newly added

		// Check email field
		emailField, found := newSchema.FindFieldByName("email")
		require.True(t, found)
		assert.Equal(t, "User email", emailField.Doc)
		assert.Equal(t, "default@example.com", emailField.InitialDefault)

		// Check name field doc update
		nameField, found := newSchema.FindFieldByName("name")
		require.True(t, found)
		assert.Equal(t, "Updated user name", nameField.Doc)

		// Check age type promotion
		ageField, found := newSchema.FindFieldByName("age")
		require.True(t, found)
		assert.Equal(t, iceberg.PrimitiveTypes.Int64, ageField.Type)

		// Check address struct changes
		addressField, found := newSchema.FindFieldByName("address")
		require.True(t, found)
		addressStruct, ok := addressField.Type.(*iceberg.StructType)
		require.True(t, ok)

		// Check nested field order (new nested fields appear at the end)
		assert.Equal(t, "street", addressStruct.FieldList[0].Name)
		assert.Equal(t, "city", addressStruct.FieldList[1].Name)
		assert.Equal(t, "zip_code", addressStruct.FieldList[2].Name) // newly added

		// Check zip_code field
		zipField, found := newSchema.FindFieldByName("address.zip_code")
		require.True(t, found)
		assert.Equal(t, "ZIP code", zipField.Doc)
		assert.False(t, zipField.Required)
	})

	t.Run("no-op changes should return original schema", func(t *testing.T) {
		updateSchema := NewUpdateSchema(txn, schema, 6)

		newSchema, err := updateSchema.Build()
		require.NoError(t, err)

		// Should return the same schema reference when no changes
		assert.Equal(t, schema, newSchema)
	})

	t.Run("conflicting operations should fail", func(t *testing.T) {
		updateSchema := NewUpdateSchema(txn, schema, 6)

		// Try to update and delete the same field
		updateSchema.UpdateColumn([]string{"age"}, ColumnUpdate{
			Doc: iceberg.Optional[string]{Val: "updated doc", Valid: true},
		})
		updateSchema.DeleteColumn([]string{"age"})

		_, err := updateSchema.Build()
		require.Error(t, err)
		assert.Contains(t, err.Error(), "cannot delete a column that has updates")
	})
}
