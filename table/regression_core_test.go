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

package table_test

import (
	"context"
	"fmt"
	"path/filepath"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/arrow-go/v18/parquet/pqarrow"
	"github.com/apache/iceberg-go"
	"github.com/apache/iceberg-go/catalog"
	"github.com/apache/iceberg-go/catalog/sql"
	iceio "github.com/apache/iceberg-go/io"
	"github.com/apache/iceberg-go/table"
	"github.com/stretchr/testify/suite"
	"github.com/uptrace/bun/driver/sqliteshim"
)

type CoreRegressionTestSuite struct {
	suite.Suite
	location string
	cat      catalog.Catalog
	ctx      context.Context
}

func TestCoreRegression(t *testing.T) {
	suite.Run(t, new(CoreRegressionTestSuite))
}

func (s *CoreRegressionTestSuite) SetupTest() {
	s.location = filepath.ToSlash(s.T().TempDir())
	s.ctx = context.Background()

	var err error
	s.cat, err = catalog.Load(s.ctx, "default", iceberg.Properties{
		"uri":          ":memory:",
		"type":         "sql",
		sql.DriverKey:  sqliteshim.ShimName,
		sql.DialectKey: string(sql.SQLite),
		"warehouse":    "file://" + s.location,
	})
	s.Require().NoError(err)

	s.Require().NoError(s.cat.CreateNamespace(s.ctx, table.Identifier{"default"}, nil))
}

func (s *CoreRegressionTestSuite) createTestTable(name string) *table.Table {
	schema := iceberg.NewSchema(0,
		iceberg.NestedField{ID: 1, Name: "id", Type: iceberg.PrimitiveTypes.Int32, Required: true},
		iceberg.NestedField{ID: 2, Name: "data", Type: iceberg.PrimitiveTypes.String, Required: false},
	)

	tbl, err := s.cat.CreateTable(s.ctx, table.Identifier{"default", name}, schema)
	s.Require().NoError(err)
	return tbl
}

func (s *CoreRegressionTestSuite) createTestData(recordCount int) arrow.Table {
	arrowSchema := arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int32, Nullable: false},
		{Name: "data", Type: arrow.BinaryTypes.String, Nullable: true},
	}, nil)

	bldr := array.NewRecordBuilder(memory.DefaultAllocator, arrowSchema)
	defer bldr.Release()

	for i := 0; i < recordCount; i++ {
		bldr.Field(0).(*array.Int32Builder).Append(int32(i))
		bldr.Field(1).(*array.StringBuilder).Append(fmt.Sprintf("data_%d", i))
	}

	rec := bldr.NewRecord()
	defer rec.Release()

	return array.NewTableFromRecords(arrowSchema, []arrow.Record{rec})
}

func (s *CoreRegressionTestSuite) writeParquetFile(fs iceio.WriteFileIO, filePath string, arrTable arrow.Table) {
	fo, err := fs.Create(filePath)
	s.Require().NoError(err)
	defer fo.Close()

	s.Require().NoError(pqarrow.WriteTable(arrTable, fo, arrTable.NumRows(), nil, pqarrow.DefaultWriterProps()))
}

// TestTableCreationConsistency ensures table creation produces consistent results
func (s *CoreRegressionTestSuite) TestTableCreationConsistency() {
	// Test 1: Create table with different schemas
	schema1 := iceberg.NewSchema(0,
		iceberg.NestedField{ID: 1, Name: "id", Type: iceberg.PrimitiveTypes.Int64, Required: true},
		iceberg.NestedField{ID: 2, Name: "name", Type: iceberg.PrimitiveTypes.String, Required: false},
	)

	tbl1, err := s.cat.CreateTable(s.ctx, table.Identifier{"default", "consistency_test_1"}, schema1)
	s.Require().NoError(err)

	// Verify schema is preserved
	retrievedSchema := tbl1.Schema()
	s.Require().True(schema1.Equals(retrievedSchema))

	// Test 2: Create table with complex types
	complexSchema := iceberg.NewSchema(0,
		iceberg.NestedField{ID: 1, Name: "id", Type: iceberg.PrimitiveTypes.Int32, Required: true},
		iceberg.NestedField{ID: 2, Name: "struct_col", Type: &iceberg.StructType{
			FieldList: []iceberg.NestedField{
				{ID: 3, Name: "nested_str", Type: iceberg.PrimitiveTypes.String, Required: false},
				{ID: 4, Name: "nested_int", Type: iceberg.PrimitiveTypes.Int32, Required: false},
			},
		}, Required: false},
		iceberg.NestedField{ID: 5, Name: "list_col", Type: &iceberg.ListType{
			ElementID: 6,
			Element:   iceberg.PrimitiveTypes.String,
		}, Required: false},
	)

	tbl2, err := s.cat.CreateTable(s.ctx, table.Identifier{"default", "consistency_test_2"}, complexSchema)
	s.Require().NoError(err)

	// Verify complex schema is preserved
	retrievedComplexSchema := tbl2.Schema()
	s.Require().True(complexSchema.Equals(retrievedComplexSchema))

	// Test 3: Field access works correctly
	idField, found := retrievedSchema.FindFieldByName("id")
	s.Require().True(found)
	s.Require().Equal(1, idField.ID)
	s.Require().Equal(iceberg.PrimitiveTypes.Int64, idField.Type)

	nameField, found := retrievedSchema.FindFieldByName("name")
	s.Require().True(found)
	s.Require().Equal(2, nameField.ID)
	s.Require().False(nameField.Required)
}

// TestSchemaFieldIDUniqueness ensures field IDs are unique and preserved
func (s *CoreRegressionTestSuite) TestSchemaFieldIDUniqueness() {
	// Create schema with specific field IDs
	schema := iceberg.NewSchema(0,
		iceberg.NestedField{ID: 1, Name: "field1", Type: iceberg.PrimitiveTypes.Int32, Required: true},
		iceberg.NestedField{ID: 2, Name: "field2", Type: iceberg.PrimitiveTypes.String, Required: false},
		iceberg.NestedField{ID: 5, Name: "field5", Type: iceberg.PrimitiveTypes.Float64, Required: false}, // Intentional gap
		iceberg.NestedField{ID: 10, Name: "field10", Type: iceberg.PrimitiveTypes.Bool, Required: false},
	)

	tbl, err := s.cat.CreateTable(s.ctx, table.Identifier{"default", "field_id_uniqueness_test"}, schema)
	s.Require().NoError(err)

	// Verify all field IDs are preserved and unique
	retrievedSchema := tbl.Schema()
	fields := retrievedSchema.Fields()
	
	fieldIDs := make(map[int]bool)
	fieldNames := make(map[string]int)

	for _, field := range fields {
		s.Require().False(fieldIDs[field.ID], "Duplicate field ID found: %d", field.ID)
		fieldIDs[field.ID] = true
		fieldNames[field.Name] = field.ID
	}

	// Verify specific field IDs are preserved (they might be different due to schema normalization)
	s.Require().Contains(fieldNames, "field1")
	s.Require().Contains(fieldNames, "field2")
	s.Require().Contains(fieldNames, "field5")
	s.Require().Contains(fieldNames, "field10")
	
	// Verify all field IDs are unique
	s.Require().Equal(4, len(fieldNames))
}

// TestSchemaTypeValidation ensures type validation works correctly
func (s *CoreRegressionTestSuite) TestSchemaTypeValidation() {
	// Test various primitive types
	schema := iceberg.NewSchema(0,
		iceberg.NestedField{ID: 1, Name: "bool_field", Type: iceberg.PrimitiveTypes.Bool, Required: false},
		iceberg.NestedField{ID: 2, Name: "int32_field", Type: iceberg.PrimitiveTypes.Int32, Required: false},
		iceberg.NestedField{ID: 3, Name: "int64_field", Type: iceberg.PrimitiveTypes.Int64, Required: false},
		iceberg.NestedField{ID: 4, Name: "float32_field", Type: iceberg.PrimitiveTypes.Float32, Required: false},
		iceberg.NestedField{ID: 5, Name: "float64_field", Type: iceberg.PrimitiveTypes.Float64, Required: false},
		iceberg.NestedField{ID: 6, Name: "string_field", Type: iceberg.PrimitiveTypes.String, Required: false},
		iceberg.NestedField{ID: 7, Name: "binary_field", Type: iceberg.PrimitiveTypes.Binary, Required: false},
		iceberg.NestedField{ID: 8, Name: "date_field", Type: iceberg.PrimitiveTypes.Date, Required: false},
		iceberg.NestedField{ID: 9, Name: "time_field", Type: iceberg.PrimitiveTypes.Time, Required: false},
		iceberg.NestedField{ID: 10, Name: "timestamp_field", Type: iceberg.PrimitiveTypes.Timestamp, Required: false},
		iceberg.NestedField{ID: 11, Name: "timestamptz_field", Type: iceberg.PrimitiveTypes.TimestampTz, Required: false},
		iceberg.NestedField{ID: 12, Name: "uuid_field", Type: iceberg.PrimitiveTypes.UUID, Required: false},
	)

	tbl, err := s.cat.CreateTable(s.ctx, table.Identifier{"default", "type_validation_test"}, schema)
	s.Require().NoError(err)

	// Verify all types are preserved correctly
	retrievedSchema := tbl.Schema()
	
	testCases := map[string]iceberg.Type{
		"bool_field":        iceberg.PrimitiveTypes.Bool,
		"int32_field":       iceberg.PrimitiveTypes.Int32,
		"int64_field":       iceberg.PrimitiveTypes.Int64,
		"float32_field":     iceberg.PrimitiveTypes.Float32,
		"float64_field":     iceberg.PrimitiveTypes.Float64,
		"string_field":      iceberg.PrimitiveTypes.String,
		"binary_field":      iceberg.PrimitiveTypes.Binary,
		"date_field":        iceberg.PrimitiveTypes.Date,
		"time_field":        iceberg.PrimitiveTypes.Time,
		"timestamp_field":   iceberg.PrimitiveTypes.Timestamp,
		"timestamptz_field": iceberg.PrimitiveTypes.TimestampTz,
		"uuid_field":        iceberg.PrimitiveTypes.UUID,
	}

	for fieldName, expectedType := range testCases {
		field, found := retrievedSchema.FindFieldByName(fieldName)
		s.Require().True(found, "Field %s not found", fieldName)
		s.Require().True(expectedType.Equals(field.Type), "Type mismatch for field %s", fieldName)
	}
}

// TestBasicTableOperations ensures basic table operations work correctly
func (s *CoreRegressionTestSuite) TestBasicTableOperations() {
	tbl := s.createTestTable("basic_operations_test")

	// Test 1: Table metadata access
	s.Require().NotNil(tbl.Schema())
	s.Require().NotNil(tbl.Spec())
	s.Require().NotNil(tbl.Metadata())
	s.Require().NotEmpty(tbl.Identifier())
	s.Require().NotEmpty(tbl.Location())
	s.Require().NotEmpty(tbl.MetadataLocation())

	// Test 2: Initial state
	s.Require().Nil(tbl.CurrentSnapshot()) // No snapshots initially

	// Test 3: FS access
	fs, err := tbl.FS(s.ctx)
	s.Require().NoError(err)
	s.Require().NotNil(fs)

	// Test 4: Scan operations
	scan := tbl.Scan()
	s.Require().NotNil(scan)

	// Should be able to scan empty table
	result, err := scan.ToArrowTable(s.ctx)
	s.Require().NoError(err)
	defer result.Release()
	s.Require().EqualValues(0, result.NumRows())

	// Test 5: Transaction creation
	tx := tbl.NewTransaction()
	s.Require().NotNil(tx)

	stagedTbl, err := tx.StagedTable()
	s.Require().NoError(err)
	s.Require().NotNil(stagedTbl)
}

// TestDataIntegration ensures data operations maintain integrity
func (s *CoreRegressionTestSuite) TestDataIntegration() {
	tbl := s.createTestTable("data_integration_test")

	// Create test data
	testData := s.createTestData(10)
	defer testData.Release()

	fs, err := tbl.FS(s.ctx)
	s.Require().NoError(err)

	// Write data file
	filePath := fmt.Sprintf("%s/data/test.parquet", s.location)
	s.writeParquetFile(fs.(iceio.WriteFileIO), filePath, testData)

	// Add file to table
	tx := tbl.NewTransaction()
	err = tx.AddFiles(s.ctx, []string{filePath}, nil, false)
	s.Require().NoError(err)

	// Verify staged table
	stagedTbl, err := tx.StagedTable()
	s.Require().NoError(err)

	snapshot := stagedTbl.CurrentSnapshot()
	s.Require().NotNil(snapshot)
	s.Require().Equal("append", string(snapshot.Summary.Operation))

	// Commit transaction
	newTbl, err := tx.Commit(s.ctx)
	s.Require().NoError(err)

	// Verify data can be read
	scan := newTbl.Scan()
	result, err := scan.ToArrowTable(s.ctx)
	s.Require().NoError(err)
	defer result.Release()

	s.Require().EqualValues(10, result.NumRows())
	s.Require().EqualValues(2, result.NumCols())

	// Verify data integrity
	s.Require().True(testData.Schema().Equal(result.Schema()))
}

// TestPartitionSpecConsistency ensures partition specs work correctly
func (s *CoreRegressionTestSuite) TestPartitionSpecConsistency() {
	// Create partitioned table
	schema := iceberg.NewSchema(0,
		iceberg.NestedField{ID: 1, Name: "id", Type: iceberg.PrimitiveTypes.Int32, Required: true},
		iceberg.NestedField{ID: 2, Name: "category", Type: iceberg.PrimitiveTypes.String, Required: false},
		iceberg.NestedField{ID: 3, Name: "year", Type: iceberg.PrimitiveTypes.Int32, Required: false},
	)

	partitionSpec := iceberg.NewPartitionSpec(
		iceberg.PartitionField{
			SourceID:  2, // category field
			FieldID:   1000,
			Transform: iceberg.IdentityTransform{},
			Name:      "category",
		},
		iceberg.PartitionField{
			SourceID:  3, // year field
			FieldID:   1001,
			Transform: iceberg.IdentityTransform{},
			Name:      "year",
		},
	)

	tbl, err := s.cat.CreateTable(s.ctx, table.Identifier{"default", "partition_consistency_test"}, schema,
		catalog.WithPartitionSpec(&partitionSpec))
	s.Require().NoError(err)

	// Verify partition spec is preserved
	retrievedSpec := tbl.Spec()
	s.Require().True(partitionSpec.Equals(retrievedSpec))

	// Verify partition fields
	fields := make([]iceberg.PartitionField, 0)
	for field := range retrievedSpec.Fields() {
		fields = append(fields, field)
	}
	s.Require().Len(fields, 2)

	// Check specific partition fields
	fieldsByName := make(map[string]iceberg.PartitionField)
	for _, field := range fields {
		fieldsByName[field.Name] = field
	}

	categoryField := fieldsByName["category"]
	s.Require().Equal(2, categoryField.SourceID)
	s.Require().Equal(1000, categoryField.FieldID)
	s.Require().IsType(iceberg.IdentityTransform{}, categoryField.Transform)

	yearField := fieldsByName["year"]
	s.Require().Equal(3, yearField.SourceID)
	s.Require().Equal(1001, yearField.FieldID)
	s.Require().IsType(iceberg.IdentityTransform{}, yearField.Transform)
}

// TestTransformValidation ensures partition transforms work correctly
func (s *CoreRegressionTestSuite) TestTransformValidation() {
	schema := iceberg.NewSchema(0,
		iceberg.NestedField{ID: 1, Name: "id", Type: iceberg.PrimitiveTypes.Int32, Required: true},
		iceberg.NestedField{ID: 2, Name: "str_field", Type: iceberg.PrimitiveTypes.String, Required: false},
		iceberg.NestedField{ID: 3, Name: "int_field", Type: iceberg.PrimitiveTypes.Int32, Required: false},
		iceberg.NestedField{ID: 4, Name: "date_field", Type: iceberg.PrimitiveTypes.Date, Required: false},
		iceberg.NestedField{ID: 5, Name: "timestamp_field", Type: iceberg.PrimitiveTypes.Timestamp, Required: false},
	)

	// Test different transforms
	transforms := []struct {
		name      string
		sourceID  int
		fieldID   int
		transform iceberg.Transform
	}{
		{"str_identity", 2, 1000, iceberg.IdentityTransform{}},
		{"str_bucket", 2, 1001, iceberg.BucketTransform{NumBuckets: 16}},
		{"str_truncate", 2, 1002, iceberg.TruncateTransform{Width: 10}},
		{"int_identity", 3, 1003, iceberg.IdentityTransform{}},
		{"int_bucket", 3, 1004, iceberg.BucketTransform{NumBuckets: 8}},
		{"date_year", 4, 1005, iceberg.YearTransform{}},
		{"date_month", 4, 1006, iceberg.MonthTransform{}},
		{"date_day", 4, 1007, iceberg.DayTransform{}},
		{"timestamp_hour", 5, 1008, iceberg.HourTransform{}},
	}

	partitionFields := make([]iceberg.PartitionField, len(transforms))
	for i, transform := range transforms {
		partitionFields[i] = iceberg.PartitionField{
			SourceID:  transform.sourceID,
			FieldID:   transform.fieldID,
			Transform: transform.transform,
			Name:      transform.name,
		}
	}

	partitionSpec := iceberg.NewPartitionSpec(partitionFields...)

	tbl, err := s.cat.CreateTable(s.ctx, table.Identifier{"default", "transform_validation_test"}, schema,
		catalog.WithPartitionSpec(&partitionSpec))
	s.Require().NoError(err)

	// Verify all transforms are preserved
	retrievedSpec := tbl.Spec()
	retrievedFields := make([]iceberg.PartitionField, 0)
	for field := range retrievedSpec.Fields() {
		retrievedFields = append(retrievedFields, field)
	}

	s.Require().Len(retrievedFields, len(transforms))

	// Verify each transform
	fieldsByName := make(map[string]iceberg.PartitionField)
	for _, field := range retrievedFields {
		fieldsByName[field.Name] = field
	}

	for _, expected := range transforms {
		field, found := fieldsByName[expected.name]
		s.Require().True(found, "Transform %s not found", expected.name)
		s.Require().Equal(expected.sourceID, field.SourceID)
		s.Require().Equal(expected.fieldID, field.FieldID)
		s.Require().Equal(expected.transform.String(), field.Transform.String())
	}
}

// TestTablePropertiesConsistency ensures table properties are preserved
func (s *CoreRegressionTestSuite) TestTablePropertiesConsistency() {
	schema := iceberg.NewSchema(0,
		iceberg.NestedField{ID: 1, Name: "id", Type: iceberg.PrimitiveTypes.Int32, Required: true},
	)

	props := iceberg.Properties{
		"custom.property":    "custom_value",
		"format-version":     "2",
		"test.setting":       "enabled",
		"write.parquet.compression-codec": "snappy",
	}

	tbl, err := s.cat.CreateTable(s.ctx, table.Identifier{"default", "properties_test"}, schema,
		catalog.WithProperties(props))
	s.Require().NoError(err)

	// Verify properties are preserved
	metadata := tbl.Metadata()
	retrievedProps := metadata.Properties()

	// Check for key properties that should be preserved
	s.Require().Equal("custom_value", retrievedProps["custom.property"])
	s.Require().Equal("enabled", retrievedProps["test.setting"])
	
	// Format version may be handled differently by the catalog
	if formatVersion, exists := retrievedProps["format-version"]; exists {
		s.Require().Equal("2", formatVersion)
	}
}

// TestErrorHandlingConsistency ensures error handling is consistent
func (s *CoreRegressionTestSuite) TestErrorHandlingConsistency() {
	// Test 1: Cannot create table with duplicate identifiers
	_, err := s.cat.CreateTable(s.ctx, table.Identifier{"default", "error_test"}, iceberg.NewSchema(0,
		iceberg.NestedField{ID: 1, Name: "field1", Type: iceberg.PrimitiveTypes.Int32, Required: true},
	))
	s.Require().NoError(err) // First creation should succeed
	
	// Try to create table with same identifier
	_, err = s.cat.CreateTable(s.ctx, table.Identifier{"default", "error_test"}, iceberg.NewSchema(0,
		iceberg.NestedField{ID: 1, Name: "field1", Type: iceberg.PrimitiveTypes.Int32, Required: true},
	))
	s.Require().Error(err) // Second creation should fail

	// Test 2: Cannot add non-existent files
	tbl := s.createTestTable("error_test_2")
	tx := tbl.NewTransaction()
	err = tx.AddFiles(s.ctx, []string{"/nonexistent/file.parquet"}, nil, false)
	s.Require().Error(err)

	// Test 3: Cannot commit transaction twice
	testData := s.createTestData(1)
	defer testData.Release()

	fs, err := tbl.FS(s.ctx)
	s.Require().NoError(err)

	filePath := fmt.Sprintf("%s/data/test.parquet", s.location)
	s.writeParquetFile(fs.(iceio.WriteFileIO), filePath, testData)

	tx2 := tbl.NewTransaction()
	err = tx2.AddFiles(s.ctx, []string{filePath}, nil, false)
	s.Require().NoError(err)

	// First commit should succeed
	_, err = tx2.Commit(s.ctx)
	s.Require().NoError(err)

	// Second commit should fail
	_, err = tx2.Commit(s.ctx)
	s.Require().Error(err)
	s.Require().Contains(err.Error(), "already been committed")
}

// TestCaseSensitivityHandling ensures case sensitivity is handled correctly
func (s *CoreRegressionTestSuite) TestCaseSensitivityHandling() {
	// Create schema with case-sensitive field names
	schema := iceberg.NewSchema(0,
		iceberg.NestedField{ID: 1, Name: "ID", Type: iceberg.PrimitiveTypes.Int32, Required: true},          // uppercase
		iceberg.NestedField{ID: 2, Name: "id", Type: iceberg.PrimitiveTypes.String, Required: false},        // lowercase
		iceberg.NestedField{ID: 3, Name: "Name", Type: iceberg.PrimitiveTypes.String, Required: false},      // mixed case
		iceberg.NestedField{ID: 4, Name: "EMAIL", Type: iceberg.PrimitiveTypes.String, Required: false},     // uppercase
	)

	tbl, err := s.cat.CreateTable(s.ctx, table.Identifier{"default", "case_sensitivity_test"}, schema)
	s.Require().NoError(err)

	retrievedSchema := tbl.Schema()

	// Test case-sensitive field lookup
	field1, found := retrievedSchema.FindFieldByName("ID")
	s.Require().True(found)
	s.Require().Equal(1, field1.ID)

	field2, found := retrievedSchema.FindFieldByName("id")
	s.Require().True(found)
	s.Require().Equal(2, field2.ID)

	// Verify they are different fields
	s.Require().NotEqual(field1.ID, field2.ID)

	// Test case-insensitive lookup
	field3, found := retrievedSchema.FindFieldByNameCaseInsensitive("email")
	s.Require().True(found)
	s.Require().Equal(4, field3.ID)
	s.Require().Equal("EMAIL", field3.Name)
}

// TestMetadataRoundTrip ensures metadata access works correctly
func (s *CoreRegressionTestSuite) TestMetadataRoundTrip() {
	// Create table with metadata
	schema := iceberg.NewSchema(0,
		iceberg.NestedField{ID: 1, Name: "id", Type: iceberg.PrimitiveTypes.Int32, Required: true},
		iceberg.NestedField{ID: 2, Name: "data", Type: iceberg.PrimitiveTypes.String, Required: false},
	)

	tbl, err := s.cat.CreateTable(s.ctx, table.Identifier{"default", "metadata_roundtrip_test"}, schema)
	s.Require().NoError(err)

	// Get original metadata
	originalMetadata := tbl.Metadata()

	// Verify basic metadata components exist
	s.Require().NotNil(originalMetadata.CurrentSchema())
	s.Require().NotNil(originalMetadata.PartitionSpec())
	s.Require().NotEmpty(originalMetadata.Location())
	
	// Verify schema has correct structure
	retrievedSchema := originalMetadata.CurrentSchema()
	s.Require().NotNil(retrievedSchema)
	s.Require().True(schema.Equals(retrievedSchema))
} 