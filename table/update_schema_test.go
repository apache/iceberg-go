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

	txn := &Transaction{}

	updateSchema := NewUpdateSchema(txn, schema, 7)

	t.Run("add struct column with default value", func(t *testing.T) {
		updateSchema := NewUpdateSchema(txn, schema, 7)

		_, err := updateSchema.AddColumn([]string{"new_col"}, false, iceberg.PrimitiveTypes.Int32, "doc string", "string_value")
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

		updated, err := updateSchema.AddColumn([]string{"address_new"}, false, structType, "User address", nil)
		require.NoError(t, err)
		require.NotNil(t, updated)

		newSchema := updated.Apply()
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
		updated, err = updateSchema.AddColumn([]string{"address", "new"}, false, iceberg.PrimitiveTypes.String, "new field", nil)
		require.NoError(t, err)
		require.NotNil(t, updated)

		newSchema = updated.Apply()
		require.NotNil(t, newSchema)

		field, found = newSchema.FindFieldByName("address.new")
		require.True(t, found)
	})

	t.Run("add simple column", func(t *testing.T) {
		updated, err := updateSchema.AddColumn([]string{"email"}, false, iceberg.PrimitiveTypes.String, "User email", "default@example.com")
		require.NoError(t, err)
		require.NotNil(t, updated)

		newSchema := updated.Apply()
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
		_, err := updateSchema.AddColumn([]string{"required_field"}, true, iceberg.PrimitiveTypes.String, "", nil)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "cannot add required column without default value")
	})

	t.Run("add required column with default should succeed", func(t *testing.T) {
		updated, err := updateSchema.AddColumn([]string{"required_field"}, true, iceberg.PrimitiveTypes.String, "", "default")
		require.NoError(t, err)
		require.NotNil(t, updated)

		newSchema := updated.Apply()
		field, found := newSchema.FindFieldByName("required_field")
		require.True(t, found)
		assert.True(t, field.Required)
		assert.Equal(t, "default", field.InitialDefault)
	})

	t.Run("add duplicate column should fail", func(t *testing.T) {
		_, err := updateSchema.AddColumn([]string{"name"}, false, iceberg.PrimitiveTypes.String, "", nil)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "cannot add column; name already exists")
	})

	t.Run("empty path should fail", func(t *testing.T) {
		_, err := updateSchema.AddColumn([]string{}, false, iceberg.PrimitiveTypes.String, "", nil)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "path must contain at least the new column name")
	})
}

func TestUpdateSchemaDeleteColumn(t *testing.T) {
	schema := iceberg.NewSchema(1,
		iceberg.NestedField{ID: 1, Name: "id", Required: true, Type: iceberg.PrimitiveTypes.Int64},
		iceberg.NestedField{ID: 2, Name: "name", Required: true, Type: iceberg.PrimitiveTypes.String},
		iceberg.NestedField{ID: 3, Name: "age", Required: false, Type: iceberg.PrimitiveTypes.Int32},
	)

	txn := &Transaction{}
	updateSchema := NewUpdateSchema(txn, schema, 3)

	t.Run("delete existing column", func(t *testing.T) {
		updated, err := updateSchema.DeleteColumn([]string{"age"})
		require.NoError(t, err)
		require.NotNil(t, updated)

		newSchema := updated.Apply()
		_, found := newSchema.FindFieldByName("age")
		assert.False(t, found, "age field should be deleted")

		// Other fields should still exist
		_, found = newSchema.FindFieldByName("id")
		assert.True(t, found)
		_, found = newSchema.FindFieldByName("name")
		assert.True(t, found)
	})

	t.Run("delete non-existent column should fail", func(t *testing.T) {
		_, err := updateSchema.DeleteColumn([]string{"nonexistent"})
		require.Error(t, err)
		assert.Contains(t, err.Error(), "Cannot delete missing column")
	})
}

func TestUpdateSchemaUpdateColumnType(t *testing.T) {
	schema := iceberg.NewSchema(1,
		iceberg.NestedField{ID: 1, Name: "id", Required: true, Type: iceberg.PrimitiveTypes.Int32},
		iceberg.NestedField{ID: 2, Name: "name", Required: true, Type: iceberg.PrimitiveTypes.String},
		iceberg.NestedField{ID: 3, Name: "price", Required: false, Type: iceberg.PrimitiveTypes.Float32},
	)

	txn := &Transaction{}
	updateSchema := NewUpdateSchema(txn, schema, 3)

	t.Run("valid type promotion int to long", func(t *testing.T) {
		updated, err := updateSchema.UpdateColumn([]string{"id"}, ColumnUpdate{
			Type: iceberg.Optional[iceberg.Type]{Val: iceberg.PrimitiveTypes.Int64, Valid: true},
		})
		require.NoError(t, err)
		require.NotNil(t, updated)

		newSchema := updated.Apply()
		field, found := newSchema.FindFieldByName("id")
		require.True(t, found)
		assert.Equal(t, iceberg.PrimitiveTypes.Int64, field.Type)
	})

	t.Run("valid type promotion float to double", func(t *testing.T) {
		updated, err := updateSchema.UpdateColumn([]string{"price"}, ColumnUpdate{
			Type: iceberg.Optional[iceberg.Type]{Val: iceberg.PrimitiveTypes.Float64, Valid: true},
		})
		require.NoError(t, err)
		require.NotNil(t, updated)

		newSchema := updated.Apply()
		field, found := newSchema.FindFieldByName("price")
		require.True(t, found)
		assert.Equal(t, iceberg.PrimitiveTypes.Float64, field.Type)
	})

	t.Run("invalid type change should fail", func(t *testing.T) {
		_, err := updateSchema.UpdateColumn([]string{"name"}, ColumnUpdate{
			Type: iceberg.Optional[iceberg.Type]{Val: iceberg.PrimitiveTypes.Int32, Valid: true},
		})
		require.Error(t, err)
		assert.Contains(t, err.Error(), "Cannot update type of column")
	})

	t.Run("update non-existent column should fail", func(t *testing.T) {
		_, err := updateSchema.UpdateColumn([]string{"nonexistent"}, ColumnUpdate{
			Type: iceberg.Optional[iceberg.Type]{Val: iceberg.PrimitiveTypes.String, Valid: true},
		})
		require.Error(t, err)
		assert.Contains(t, err.Error(), "Cannot update missing column")
	})
}

func TestUpdateSchemaUpdateColumnDoc(t *testing.T) {
	schema := iceberg.NewSchema(1,
		iceberg.NestedField{ID: 1, Name: "id", Required: true, Type: iceberg.PrimitiveTypes.Int64},
		iceberg.NestedField{ID: 2, Name: "name", Required: true, Type: iceberg.PrimitiveTypes.String, Doc: "old doc"},
	)

	txn := &Transaction{}
	updateSchema := NewUpdateSchema(txn, schema, 2)

	t.Run("update documentation", func(t *testing.T) {
		newDoc := "Updated documentation"
		updated, err := updateSchema.UpdateColumn([]string{"name"}, ColumnUpdate{
			Doc: iceberg.Optional[string]{Val: newDoc, Valid: true},
		})
		require.NoError(t, err)
		require.NotNil(t, updated)

		newSchema := updated.Apply()
		field, found := newSchema.FindFieldByName("name")
		require.True(t, found)
		assert.Equal(t, newDoc, field.Doc)
	})

	t.Run("update with same doc should not error", func(t *testing.T) {
		sameDoc := "old doc"
		updated, err := updateSchema.UpdateColumn([]string{"name"}, ColumnUpdate{
			Doc: iceberg.Optional[string]{Val: sameDoc, Valid: true},
		})
		require.NoError(t, err)
		require.NotNil(t, updated)
	})

	t.Run("update non-existent column should fail", func(t *testing.T) {
		newDoc := "new doc"
		_, err := updateSchema.UpdateColumn([]string{"nonexistent"}, ColumnUpdate{
			Doc: iceberg.Optional[string]{Val: newDoc, Valid: true},
		})
		require.Error(t, err)
		assert.Contains(t, err.Error(), "Cannot update missing column")
	})
}

func TestUpdateSchemaUpdateColumnDefault(t *testing.T) {
	schema := iceberg.NewSchema(1,
		iceberg.NestedField{ID: 1, Name: "id", Required: true, Type: iceberg.PrimitiveTypes.Int64},
		iceberg.NestedField{ID: 2, Name: "status", Required: false, Type: iceberg.PrimitiveTypes.String, InitialDefault: "active"},
	)

	txn := &Transaction{}
	updateSchema := NewUpdateSchema(txn, schema, 2)

	t.Run("update default value", func(t *testing.T) {
		newDefault := "inactive"
		updated, err := updateSchema.UpdateColumn([]string{"status"}, ColumnUpdate{
			Default: newDefault,
		})
		require.NoError(t, err)
		require.NotNil(t, updated)

		newSchema := updated.Apply()
		field, found := newSchema.FindFieldByName("status")
		require.True(t, found)
		assert.Equal(t, newDefault, field.InitialDefault)
	})

	t.Run("update with same default should not error", func(t *testing.T) {
		sameDefault := "active"
		updated, err := updateSchema.UpdateColumn([]string{"status"}, ColumnUpdate{
			Default: sameDefault,
		})
		require.NoError(t, err)
		require.NotNil(t, updated)
	})

	t.Run("update non-existent column should fail", func(t *testing.T) {
		_, err := updateSchema.UpdateColumn([]string{"nonexistent"}, ColumnUpdate{
			Default: "default",
		})
		require.Error(t, err)
		assert.Contains(t, err.Error(), "Cannot update missing column")
	})
}

func TestUpdateSchemaRequireColumn(t *testing.T) {
	schema := iceberg.NewSchema(1,
		iceberg.NestedField{ID: 1, Name: "id", Required: true, Type: iceberg.PrimitiveTypes.Int64},
		iceberg.NestedField{ID: 2, Name: "optional_field", Required: false, Type: iceberg.PrimitiveTypes.String},
	)

	txn := &Transaction{}
	updateSchema := NewUpdateSchema(txn, schema, 2)

	t.Run("make column required", func(t *testing.T) {
		// Allow incompatible changes for this test
		updateSchema.AllowIncompatibleChanges()

		updated, err := updateSchema.UpdateColumn([]string{"optional_field"}, ColumnUpdate{
			Required: iceberg.Optional[bool]{Val: true, Valid: true},
		})
		require.NoError(t, err)
		require.NotNil(t, updated)

		newSchema := updated.Apply()
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

	txn := &Transaction{}
	updateSchema := NewUpdateSchema(txn, schema, 2)

	t.Run("make column optional", func(t *testing.T) {
		updated, err := updateSchema.UpdateColumn([]string{"required_field"}, ColumnUpdate{
			Required: iceberg.Optional[bool]{Val: false, Valid: true},
		})
		require.NoError(t, err)
		require.NotNil(t, updated)

		newSchema := updated.Apply()
		field, found := newSchema.FindFieldByName("required_field")
		require.True(t, found)
		assert.False(t, field.Required)
	})
}

func TestUpdateSchemaAllowIncompatibleChanges(t *testing.T) {
	schema := iceberg.NewSchema(1,
		iceberg.NestedField{ID: 1, Name: "id", Required: true, Type: iceberg.PrimitiveTypes.Int64},
	)

	txn := &Transaction{}
	updateSchema := NewUpdateSchema(txn, schema, 1)

	t.Run("allow incompatible changes should enable adding required column without default", func(t *testing.T) {
		updateSchema.AllowIncompatibleChanges()

		updated, err := updateSchema.AddColumn([]string{"required_field"}, true, iceberg.PrimitiveTypes.String, "", nil)
		require.NoError(t, err)
		require.NotNil(t, updated)

		newSchema := updated.Apply()
		field, found := newSchema.FindFieldByName("required_field")
		require.True(t, found)
		assert.True(t, field.Required)
		assert.Nil(t, field.InitialDefault)
	})
}

func TestUpdateSchemaAssignNewColumnID(t *testing.T) {
	schema := iceberg.NewSchema(1,
		iceberg.NestedField{ID: 1, Name: "id", Required: true, Type: iceberg.PrimitiveTypes.Int64},
	)

	txn := &Transaction{}
	updateSchema := NewUpdateSchema(txn, schema, 1)

	t.Run("assign new column ID increments correctly", func(t *testing.T) {
		id1 := updateSchema.assignNewColumnID()
		id2 := updateSchema.assignNewColumnID()
		id3 := updateSchema.assignNewColumnID()

		assert.Equal(t, 2, id1)
		assert.Equal(t, 3, id2)
		assert.Equal(t, 4, id3)
	})
}

func TestUpdateSchemaFindField(t *testing.T) {
	schema := iceberg.NewSchema(1,
		iceberg.NestedField{ID: 1, Name: "id", Required: true, Type: iceberg.PrimitiveTypes.Int64},
		iceberg.NestedField{ID: 2, Name: "Name", Required: true, Type: iceberg.PrimitiveTypes.String},
	)

	txn := &Transaction{}

	t.Run("case sensitive search", func(t *testing.T) {
		updateSchema := NewUpdateSchema(txn, schema, 2)
		// Default is case sensitive

		field := updateSchema.findField([]string{"Name"})
		require.NotNil(t, field)
		assert.Equal(t, "Name", field.Name)

		field = updateSchema.findField([]string{"name"})
		assert.Nil(t, field, "should not find field with different case")
	})

	t.Run("case insensitive search", func(t *testing.T) {
		updateSchema := NewUpdateSchema(txn, schema, 2)
		updateSchema.caseSensitive = false

		field := updateSchema.findField([]string{"name"})
		require.NotNil(t, field)
		assert.Equal(t, "Name", field.Name)

		field = updateSchema.findField([]string{"NAME"})
		require.NotNil(t, field)
		assert.Equal(t, "Name", field.Name)
	})
}

func TestUpdateSchemaApplyChanges(t *testing.T) {
	schema := iceberg.NewSchema(1,
		iceberg.NestedField{ID: 1, Name: "id", Required: true, Type: iceberg.PrimitiveTypes.Int64},
		iceberg.NestedField{ID: 2, Name: "name", Required: true, Type: iceberg.PrimitiveTypes.String},
		iceberg.NestedField{ID: 3, Name: "age", Required: false, Type: iceberg.PrimitiveTypes.Int32},
	)

	txn := &Transaction{}
	updateSchema := NewUpdateSchema(txn, schema, 3)

	t.Run("apply multiple changes", func(t *testing.T) {
		// Add a new column
		updated, err := updateSchema.AddColumn([]string{"email"}, false, iceberg.PrimitiveTypes.String, "User email", nil)
		require.NoError(t, err)

		doc := "Updated name field"
		// Update documentation
		updated, err = updated.UpdateColumn([]string{"name"}, ColumnUpdate{
			Doc: iceberg.Optional[string]{Val: doc, Valid: true},
		})
		require.NoError(t, err)

		// Delete a column
		updated, err = updated.DeleteColumn([]string{"age"})
		require.NoError(t, err)

		newSchema := updated.Apply()
		require.NotNil(t, newSchema)

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

	txn := &Transaction{}

	t.Run("move column to first position", func(t *testing.T) {
		updateSchema := NewUpdateSchema(txn, schema, 8)

		// Move 'email' to first position
		updated, err := updateSchema.Move([]string{"email"}, nil, OpFirst)
		require.NoError(t, err)
		require.NotNil(t, updated)

		newSchema := updated.Apply()
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
		updated, err := updateSchema.Move([]string{"email"}, []string{"name"}, OpBefore)
		require.NoError(t, err)
		require.NotNil(t, updated)

		newSchema := updated.Apply()
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
		updated, err := updateSchema.Move([]string{"age"}, []string{"name"}, OpAfter)
		require.NoError(t, err)
		require.NotNil(t, updated)

		newSchema := updated.Apply()
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
		updated, err := updateSchema.Move([]string{"address", "zip_code"}, nil, OpFirst)
		require.NoError(t, err)
		require.NotNil(t, updated)

		newSchema := updated.Apply()
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
		updated, err := updateSchema.Move([]string{"address", "city"}, []string{"address", "street"}, OpBefore)
		require.NoError(t, err)
		require.NotNil(t, updated)

		newSchema := updated.Apply()
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
		updated, err := updateSchema.Move([]string{"email"}, nil, OpFirst)
		require.NoError(t, err)

		// Then move age after name
		updated, err = updated.Move([]string{"age"}, []string{"name"}, OpAfter)
		require.NoError(t, err)

		newSchema := updated.Apply()
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

		_, err := updateSchema.Move([]string{"nonexistent"}, []string{"name"}, OpBefore)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "cannot move missing column")
	})

	t.Run("move before non-existent reference column should fail", func(t *testing.T) {
		updateSchema := NewUpdateSchema(txn, schema, 8)

		_, err := updateSchema.Move([]string{"email"}, []string{"nonexistent"}, OpBefore)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "reference column for move not found")
	})

	t.Run("move after non-existent reference column should fail", func(t *testing.T) {
		updateSchema := NewUpdateSchema(txn, schema, 8)

		_, err := updateSchema.Move([]string{"email"}, []string{"nonexistent"}, OpAfter)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "reference column for move not found")
	})

	t.Run("move column relative to itself should fail", func(t *testing.T) {
		updateSchema := NewUpdateSchema(txn, schema, 8)

		_, err := updateSchema.Move([]string{"email"}, []string{"email"}, OpBefore)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "cannot move column relative to itself")
	})

	t.Run("move columns with different parents should fail", func(t *testing.T) {
		updateSchema := NewUpdateSchema(txn, schema, 8)

		// Try to move a top-level column relative to a nested column
		_, err := updateSchema.Move([]string{"email"}, []string{"address", "street"}, OpBefore)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "cannot move column across different parent structs")
	})

	t.Run("move with add and delete operations", func(t *testing.T) {
		updateSchema := NewUpdateSchema(txn, schema, 8)

		// Add a new column
		updated, err := updateSchema.AddColumn([]string{"phone"}, false, iceberg.PrimitiveTypes.String, "Phone number", nil)
		require.NoError(t, err)

		// Delete a column
		updated, err = updated.DeleteColumn([]string{"age"})
		require.NoError(t, err)

		// Move the new column to first position
		updated, err = updated.Move([]string{"phone"}, nil, OpFirst)
		require.NoError(t, err)

		// Move email before name
		updated, err = updated.Move([]string{"email"}, []string{"name"}, OpBefore)
		require.NoError(t, err)

		newSchema := updated.Apply()
		fields := newSchema.AsStruct().FieldList

		// Verify all operations were applied correctly
		assert.Equal(t, "phone", fields[0].Name)
		assert.Equal(t, "id", fields[1].Name)
		assert.Equal(t, "email", fields[2].Name)
		assert.Equal(t, "name", fields[3].Name)
		assert.Equal(t, "address", fields[4].Name)

		// Verify age was deleted
		_, found := newSchema.FindFieldByName("age")
		assert.False(t, found)
	})
}

// Helper function to create mock metadata for testing
func createMockMetadata(schema *iceberg.Schema, lastColumnID int) Metadata {
	// For testing purposes, we'll use a simple mock that implements the Metadata interface
	return &mockMetadata{
		schema:       schema,
		lastColumnID: lastColumnID,
	}
}

// Helper function to create a pointer to any value
func anyPtr(v any) *any {
	return &v
}

func boolPtr(b bool) *bool {
	return &b
}
