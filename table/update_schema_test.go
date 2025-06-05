package table

import (
	"fmt"
	"testing"

	"github.com/apache/iceberg-go"
)

func TestAddDeleteColumnUpdateSchema(t *testing.T) {

	schema := iceberg.NewSchema(1,
		iceberg.NestedField{ID: 1, Name: "name", Required: true, Type: iceberg.StringType{}, Doc: "", InitialDefault: nil},
		iceberg.NestedField{ID: 2, Name: "age", Required: true, Type: iceberg.StringType{}, Doc: "comment", InitialDefault: nil},
		iceberg.NestedField{ID: 3, Name: "city", Required: true, Type: iceberg.StringType{}, Doc: "", InitialDefault: nil},
	)

	minimalV1Example := `{
		"format-version": 1,
		"location": "s3://bucket/test/location",
		"last-updated-ms": 1062638573874,
		"last-column-id": 3,
		"schema": {
			"type": "struct",
			"fields": [
				{"id": 1, "name": "name", "required": true, "type": "string"},
				{"id": 2, "name": "age", "required": true, "type": "int", "doc": "comment"},
				{"id": 3, "name": "city", "required": true, "type": "string"}
			]
		},
		"partition-specs":[{"spec-id":0,"fields":[]}],
		"properties": {},
		"current-snapshot-id": -1,
		"snapshots": [{"snapshot-id": 1925, "timestamp-ms": 1602638573822}]
	}`

	meta, err := ParseMetadataString(minimalV1Example)
	if err != nil {
		t.Fatal(err)
	}

	su := NewUpdateSchema(&meta, schema, 3)
	su.AddColumn("", "name_new", 4, true, iceberg.StringType{}, "", nil)
	su.AddColumn("", "name_new_2", 5, true, iceberg.StringType{}, "", nil)

	su.DeleteColumn("name")
	su.UpdateColumnDoc("age", "new doc")
	newSchema := su.applyChanges()
	fmt.Println(newSchema.String())
}

func TestAddDeleteNestedColumnUpdateSchema(t *testing.T) {

	nestedSchema := iceberg.NewSchema(1,
		iceberg.NestedField{ID: 1, Name: "name", Required: true, Type: iceberg.StringType{}, Doc: "", InitialDefault: nil},
		iceberg.NestedField{ID: 2, Name: "age", Required: true, Type: iceberg.StringType{}, Doc: "comment", InitialDefault: nil},
		iceberg.NestedField{
			ID:       3,
			Name:     "id_to_person",
			Required: true,
			Type: &iceberg.StructType{
				FieldList: []iceberg.NestedField{
					{ID: 4, Name: "name", Type: iceberg.PrimitiveTypes.String},
					{ID: 5, Name: "age", Type: iceberg.PrimitiveTypes.Int32, Required: true},
				},
			},
		},
	)

	minimalV1Example := `{
		"format-version": 1,
		"location": "s3://bucket/test/location",
		"last-updated-ms": 1062638573874,
		"last-column-id": 3,
		"schema": {
			"type": "struct",
			"fields": [
				{"id": 1, "name": "name", "required": true, "type": "string"},
				{"id": 2, "name": "age", "required": true, "type": "int", "doc": "comment"},
				{
					"type": {
						"type": "struct",
						"fields": [
							{
								"type": "string",
								"id": 4,
								"name": "name",
								"required": false
							},
							{
								"type": "int",
								"id": 5,
								"name": "age",
								"required": true
							}
						]
					},
					"id": 3,
					"name": "id_to_person",
					"required": true
				}
			]
		},
		"partition-specs":[{"spec-id":0,"fields":[]}],
		"properties": {},
		"current-snapshot-id": -1,
		"snapshots": [{"snapshot-id": 1925, "timestamp-ms": 1602638573822}]
	}`

	meta, err := ParseMetadataString(minimalV1Example)
	if err != nil {
		t.Fatal(err)
	}

	su := NewUpdateSchema(&meta, nestedSchema, 5)
	su.AddColumn("id_to_person", "name_new", 6, true, iceberg.StringType{}, "", nil)
	su.DeleteColumn("name")
	newSchema := su.applyChanges()
	fmt.Println(newSchema.String())
}

func TestUpdateSchemaCommit(t *testing.T) {
	schema := iceberg.NewSchema(1,
		iceberg.NestedField{ID: 1, Name: "name", Required: true, Type: iceberg.StringType{}, Doc: "", InitialDefault: nil},
		iceberg.NestedField{ID: 2, Name: "age", Required: true, Type: iceberg.Int32Type{}, Doc: "comment", InitialDefault: nil},
		iceberg.NestedField{ID: 3, Name: "city", Required: true, Type: iceberg.StringType{}, Doc: "", InitialDefault: nil},
	)

	minimalV1Example := `{
		"format-version": 1,
		"location": "s3://bucket/test/location",
		"last-updated-ms": 1062638573874,
		"last-column-id": 3,
		"schema": {
			"type": "struct",
			"fields": [
				{"id": 1, "name": "name", "required": true, "type": "string"},
				{"id": 2, "name": "age", "required": true, "type": "int", "doc": "comment"},
				{"id": 3, "name": "city", "required": true, "type": "string"}
			]
		},
		"partition-specs":[{"spec-id":0,"fields":[]}],
		"properties": {},
		"current-snapshot-id": -1,
		"snapshots": [{"snapshot-id": 1925, "timestamp-ms": 1602638573822}]
	}`

	meta, err := ParseMetadataString(minimalV1Example)
	if err != nil {
		t.Fatal(err)
	}

	// Create UpdateSchema and make changes
	su := NewUpdateSchema(&meta, schema, 3)
	su.AddColumn("", "email", 4, false, iceberg.StringType{}, "User email address", "default@example.com")
	su.UpdateColumnDoc("age", "User age in years")

	// Commit the changes
	updatedMetadata, err := su.Commit()
	if err != nil {
		t.Fatal(err)
	}

	// Verify the new schema
	newSchema := updatedMetadata.CurrentSchema()
	if newSchema.ID <= schema.ID {
		t.Errorf("Expected new schema ID to be greater than %d, got %d", schema.ID, newSchema.ID)
	}

	// Verify the email field was added
	emailField, found := newSchema.FindFieldByName("email")
	if !found {
		t.Error("Expected to find 'email' field in new schema")
	}
	if emailField.ID != 4 {
		t.Errorf("Expected email field ID to be 4, got %d", emailField.ID)
	}

	// Verify the age field documentation was updated
	ageField, found := newSchema.FindFieldByName("age")
	if !found {
		t.Error("Expected to find 'age' field in new schema")
	}
	if ageField.Doc != "User age in years" {
		t.Errorf("Expected age field doc to be 'User age in years', got '%s'", ageField.Doc)
	}

	// Verify the updated metadata has more schemas
	if len(updatedMetadata.Schemas()) <= len(meta.Schemas()) {
		t.Error("Expected updated metadata to have more schemas")
	}

	fmt.Printf("Successfully committed schema changes. New schema ID: %d\n", newSchema.ID)
}

func TestNewdataType(t *testing.T) {

	nestedSchema := iceberg.NewSchema(1,
		iceberg.NestedField{ID: 1, Name: "name", Required: true, Type: iceberg.StringType{}, Doc: "", InitialDefault: nil},
		iceberg.NestedField{ID: 2, Name: "age", Required: true, Type: iceberg.StringType{}, Doc: "comment", InitialDefault: nil},
		iceberg.NestedField{
			ID:       3,
			Name:     "id_to_person",
			Required: true,
			Type: &iceberg.StructType{
				FieldList: []iceberg.NestedField{
					{ID: 1, Name: "name", Type: iceberg.PrimitiveTypes.String},
					{ID: 2, Name: "age", Type: iceberg.PrimitiveTypes.Int32, Required: true},
				},
			},
		},
	)

	for _, field := range nestedSchema.Fields() {
		printType(field.Type)
	}
}

func printType(t iceberg.Type) {

	typeis := t.Type()
	fmt.Println(typeis)
	if nestedType, ok := t.(iceberg.NestedType); ok {
		if _, ok := nestedType.(*iceberg.MapType); ok {
			fmt.Println("mapType")
		} else if _, ok := nestedType.(*iceberg.ListType); ok {
			fmt.Println("listType")
		} else if _, ok := nestedType.(*iceberg.StructType); ok {
			fmt.Println("structType")
		}
	}
}
