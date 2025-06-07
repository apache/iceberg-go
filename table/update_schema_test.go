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

	// Create mock metadata
	metadata := createMockMetadata(schema, 7)

	updateSchema := NewUpdateSchema(metadata, schema, 7)

	t.Run("add struct column with default value", func(t *testing.T) {
		updateSchema := NewUpdateSchema(metadata, schema, 7)

		_, err := updateSchema.AddColumn([]string{"new_col"}, false, iceberg.PrimitiveTypes.Int32, "doc string", "string_value")
		require.Error(t, err)
		assert.Contains(t, err.Error(), "default literal of type string is not assignable to of type int")
	})

	t.Run("add struct column", func(t *testing.T) {
		updateSchema := NewUpdateSchema(metadata, schema, 7)

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

		//check if we can find the nested fields
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

		//trying to add a new field to the struct
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

	metadata := createMockMetadata(schema, 3)
	updateSchema := NewUpdateSchema(metadata, schema, 3)

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

// Helper function to create mock metadata for testing
func createMockMetadata(schema *iceberg.Schema, lastColumnID int) Metadata {
	// For testing purposes, we'll use a simple mock that implements the Metadata interface
	return &mockMetadata{
		schema:       schema,
		lastColumnID: lastColumnID,
	}
}
