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
	"github.com/uptrace/bun/driver/sqliteshim"
)

// BenchmarkSetup provides common setup for benchmarks
type BenchmarkSetup struct {
	ctx      context.Context
	location string
	cat      catalog.Catalog
}

func newBenchmarkSetup(b *testing.B) *BenchmarkSetup {
	ctx := context.Background()
	location := filepath.ToSlash(b.TempDir())

	cat, err := catalog.Load(ctx, "default", iceberg.Properties{
		"uri":          ":memory:",
		"type":         "sql",
		sql.DriverKey:  sqliteshim.ShimName,
		sql.DialectKey: string(sql.SQLite),
		"warehouse":    "file://" + location,
	})
	if err != nil {
		b.Fatalf("Failed to create catalog: %v", err)
	}

	err = cat.CreateNamespace(ctx, table.Identifier{"bench"}, nil)
	if err != nil {
		b.Fatalf("Failed to create namespace: %v", err)
	}

	return &BenchmarkSetup{
		ctx:      ctx,
		location: location,
		cat:      cat,
	}
}

func (s *BenchmarkSetup) createSimpleSchema() *iceberg.Schema {
	return iceberg.NewSchema(0,
		iceberg.NestedField{ID: 1, Name: "id", Type: iceberg.PrimitiveTypes.Int64, Required: true},
		iceberg.NestedField{ID: 2, Name: "data", Type: iceberg.PrimitiveTypes.String, Required: false},
	)
}

func (s *BenchmarkSetup) createComplexSchema() *iceberg.Schema {
	return iceberg.NewSchema(0,
		iceberg.NestedField{ID: 1, Name: "id", Type: iceberg.PrimitiveTypes.Int64, Required: true},
		iceberg.NestedField{ID: 2, Name: "name", Type: iceberg.PrimitiveTypes.String, Required: false},
		iceberg.NestedField{ID: 3, Name: "age", Type: iceberg.PrimitiveTypes.Int32, Required: false},
		iceberg.NestedField{ID: 4, Name: "score", Type: iceberg.PrimitiveTypes.Float64, Required: false},
		iceberg.NestedField{ID: 5, Name: "active", Type: iceberg.PrimitiveTypes.Bool, Required: false},
		iceberg.NestedField{ID: 6, Name: "created_date", Type: iceberg.PrimitiveTypes.Date, Required: false},
		iceberg.NestedField{ID: 7, Name: "updated_time", Type: iceberg.PrimitiveTypes.Timestamp, Required: false},
		iceberg.NestedField{ID: 8, Name: "metadata", Type: &iceberg.StructType{
			FieldList: []iceberg.NestedField{
				{ID: 9, Name: "source", Type: iceberg.PrimitiveTypes.String, Required: false},
				{ID: 10, Name: "version", Type: iceberg.PrimitiveTypes.Int32, Required: false},
				{ID: 11, Name: "tags", Type: &iceberg.ListType{
					ElementID: 12,
					Element:   iceberg.PrimitiveTypes.String,
				}, Required: false},
			},
		}, Required: false},
	)
}

func (s *BenchmarkSetup) createPartitionedSchema() *iceberg.Schema {
	return iceberg.NewSchema(0,
		iceberg.NestedField{ID: 1, Name: "id", Type: iceberg.PrimitiveTypes.Int64, Required: true},
		iceberg.NestedField{ID: 2, Name: "category", Type: iceberg.PrimitiveTypes.String, Required: false},
		iceberg.NestedField{ID: 3, Name: "region", Type: iceberg.PrimitiveTypes.String, Required: false},
		iceberg.NestedField{ID: 4, Name: "year", Type: iceberg.PrimitiveTypes.Int32, Required: false},
		iceberg.NestedField{ID: 5, Name: "data", Type: iceberg.PrimitiveTypes.String, Required: false},
	)
}

func (s *BenchmarkSetup) createTestData(recordCount int) arrow.Table {
	arrowSchema := arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int64, Nullable: false},
		{Name: "data", Type: arrow.BinaryTypes.String, Nullable: true},
	}, nil)

	bldr := array.NewRecordBuilder(memory.DefaultAllocator, arrowSchema)
	defer bldr.Release()

	for i := 0; i < recordCount; i++ {
		bldr.Field(0).(*array.Int64Builder).Append(int64(i))
		bldr.Field(1).(*array.StringBuilder).Append(fmt.Sprintf("data_%d", i))
	}

	rec := bldr.NewRecord()
	defer rec.Release()

	return array.NewTableFromRecords(arrowSchema, []arrow.Record{rec})
}

func (s *BenchmarkSetup) writeDataFile(tbl *table.Table, data arrow.Table, filename string) string {
	fs, err := tbl.FS(s.ctx)
	if err != nil {
		panic(err)
	}

	filePath := fmt.Sprintf("%s/data/%s", s.location, filename)
	fo, err := fs.(iceio.WriteFileIO).Create(filePath)
	if err != nil {
		panic(err)
	}
	defer fo.Close()

	err = pqarrow.WriteTable(data, fo, data.NumRows(), nil, pqarrow.DefaultWriterProps())
	if err != nil {
		panic(err)
	}

	return filePath
}

// Table Creation Benchmarks

func BenchmarkTableCreationSimple(b *testing.B) {
	setup := newBenchmarkSetup(b)
	schema := setup.createSimpleSchema()

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		tableName := fmt.Sprintf("simple_table_%d", i)
		_, err := setup.cat.CreateTable(setup.ctx, table.Identifier{"bench", tableName}, schema)
		if err != nil {
			b.Fatalf("Failed to create table: %v", err)
		}
	}
}

func BenchmarkTableCreationComplex(b *testing.B) {
	setup := newBenchmarkSetup(b)
	schema := setup.createComplexSchema()

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		tableName := fmt.Sprintf("complex_table_%d", i)
		_, err := setup.cat.CreateTable(setup.ctx, table.Identifier{"bench", tableName}, schema)
		if err != nil {
			b.Fatalf("Failed to create table: %v", err)
		}
	}
}

func BenchmarkTableCreationPartitioned(b *testing.B) {
	setup := newBenchmarkSetup(b)
	schema := setup.createPartitionedSchema()
	
	partitionSpec := iceberg.NewPartitionSpec(
		iceberg.PartitionField{
			SourceID:  2, // category
			FieldID:   1000,
			Transform: iceberg.IdentityTransform{},
			Name:      "category",
		},
		iceberg.PartitionField{
			SourceID:  3, // region
			FieldID:   1001,
			Transform: iceberg.IdentityTransform{},
			Name:      "region",
		},
	)

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		tableName := fmt.Sprintf("partitioned_table_%d", i)
		_, err := setup.cat.CreateTable(setup.ctx, table.Identifier{"bench", tableName}, schema,
			catalog.WithPartitionSpec(&partitionSpec))
		if err != nil {
			b.Fatalf("Failed to create table: %v", err)
		}
	}
}

// Data Writing Benchmarks

func BenchmarkDataWritingSmall(b *testing.B) {
	setup := newBenchmarkSetup(b)
	schema := setup.createSimpleSchema()

	// Create test data (100 records)
	testData := setup.createTestData(100)
	defer testData.Release()

	b.ResetTimer()
	b.ReportAllocs()
	b.SetBytes(int64(testData.NumRows() * 24)) // Rough size estimation

	for i := 0; i < b.N; i++ {
		b.StopTimer()
		// Create unique table for each iteration
		tableName := fmt.Sprintf("write_small_%d", i)
		tbl, err := setup.cat.CreateTable(setup.ctx, table.Identifier{"bench", tableName}, schema)
		if err != nil {
			b.Fatalf("Failed to create table: %v", err)
		}

		filename := fmt.Sprintf("small_%d.parquet", i)
		filePath := setup.writeDataFile(tbl, testData, filename)
		
		tx := tbl.NewTransaction()
		b.StartTimer()

		err = tx.AddFiles(setup.ctx, []string{filePath}, nil, false)
		if err != nil {
			b.Fatalf("Failed to add files: %v", err)
		}

		_, err = tx.Commit(setup.ctx)
		if err != nil {
			b.Fatalf("Failed to commit: %v", err)
		}
	}
}

func BenchmarkDataWritingMedium(b *testing.B) {
	setup := newBenchmarkSetup(b)
	schema := setup.createSimpleSchema()

	// Create test data (10,000 records)
	testData := setup.createTestData(10000)
	defer testData.Release()

	b.ResetTimer()
	b.ReportAllocs()
	b.SetBytes(int64(testData.NumRows() * 24))

	for i := 0; i < b.N; i++ {
		b.StopTimer()
		// Create unique table for each iteration
		tableName := fmt.Sprintf("write_medium_%d", i)
		tbl, err := setup.cat.CreateTable(setup.ctx, table.Identifier{"bench", tableName}, schema)
		if err != nil {
			b.Fatalf("Failed to create table: %v", err)
		}

		filename := fmt.Sprintf("medium_%d.parquet", i)
		filePath := setup.writeDataFile(tbl, testData, filename)
		
		tx := tbl.NewTransaction()
		b.StartTimer()

		err = tx.AddFiles(setup.ctx, []string{filePath}, nil, false)
		if err != nil {
			b.Fatalf("Failed to add files: %v", err)
		}

		_, err = tx.Commit(setup.ctx)
		if err != nil {
			b.Fatalf("Failed to commit: %v", err)
		}
	}
}

func BenchmarkDataWritingBatch(b *testing.B) {
	setup := newBenchmarkSetup(b)
	schema := setup.createSimpleSchema()

	// Create multiple small files
	testData := setup.createTestData(1000)
	defer testData.Release()

	const batchSize = 10

	b.ResetTimer()
	b.ReportAllocs()
	b.SetBytes(int64(testData.NumRows() * batchSize * 24))

	for i := 0; i < b.N; i++ {
		b.StopTimer()
		
		// Create unique table for each iteration
		tableName := fmt.Sprintf("write_batch_%d", i)
		tbl, err := setup.cat.CreateTable(setup.ctx, table.Identifier{"bench", tableName}, schema)
		if err != nil {
			b.Fatalf("Failed to create table: %v", err)
		}

		var filePaths []string
		for j := 0; j < batchSize; j++ {
			filename := fmt.Sprintf("batch_%d_%d.parquet", i, j)
			filePath := setup.writeDataFile(tbl, testData, filename)
			filePaths = append(filePaths, filePath)
		}
		
		tx := tbl.NewTransaction()
		b.StartTimer()

		err = tx.AddFiles(setup.ctx, filePaths, nil, false)
		if err != nil {
			b.Fatalf("Failed to add files: %v", err)
		}

		_, err = tx.Commit(setup.ctx)
		if err != nil {
			b.Fatalf("Failed to commit: %v", err)
		}
	}
}

// Data Reading Benchmarks

func BenchmarkDataReadingSmall(b *testing.B) {
	setup := newBenchmarkSetup(b)
	schema := setup.createSimpleSchema()

	// Setup table with data
	tbl, err := setup.cat.CreateTable(setup.ctx, table.Identifier{"bench", "read_small"}, schema)
	if err != nil {
		b.Fatalf("Failed to create table: %v", err)
	}

	// Add some data
	testData := setup.createTestData(1000)
	defer testData.Release()

	filePath := setup.writeDataFile(tbl, testData, "read_test.parquet")
	tx := tbl.NewTransaction()
	err = tx.AddFiles(setup.ctx, []string{filePath}, nil, false)
	if err != nil {
		b.Fatalf("Failed to add files: %v", err)
	}
	tbl, err = tx.Commit(setup.ctx)
	if err != nil {
		b.Fatalf("Failed to commit: %v", err)
	}

	b.ResetTimer()
	b.ReportAllocs()
	b.SetBytes(int64(testData.NumRows() * 24))

	for i := 0; i < b.N; i++ {
		scan := tbl.Scan()
		result, err := scan.ToArrowTable(setup.ctx)
		if err != nil {
			b.Fatalf("Failed to scan table: %v", err)
		}
		result.Release()
	}
}

func BenchmarkDataReadingWithFilter(b *testing.B) {
	setup := newBenchmarkSetup(b)
	schema := setup.createSimpleSchema()

	// Setup table with data
	tbl, err := setup.cat.CreateTable(setup.ctx, table.Identifier{"bench", "read_filter"}, schema)
	if err != nil {
		b.Fatalf("Failed to create table: %v", err)
	}

	// Add some data
	testData := setup.createTestData(10000)
	defer testData.Release()

	filePath := setup.writeDataFile(tbl, testData, "filter_test.parquet")
	tx := tbl.NewTransaction()
	err = tx.AddFiles(setup.ctx, []string{filePath}, nil, false)
	if err != nil {
		b.Fatalf("Failed to add files: %v", err)
	}
	tbl, err = tx.Commit(setup.ctx)
	if err != nil {
		b.Fatalf("Failed to commit: %v", err)
	}

	// Create filter that matches ~10% of records
	filter := iceberg.LessThan(iceberg.Reference("id"), int64(1000))

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		scan := tbl.Scan(table.WithRowFilter(filter))
		result, err := scan.ToArrowTable(setup.ctx)
		if err != nil {
			b.Fatalf("Failed to scan table: %v", err)
		}
		result.Release()
	}
}

func BenchmarkDataReadingPlanFiles(b *testing.B) {
	setup := newBenchmarkSetup(b)
	schema := setup.createSimpleSchema()

	// Setup table with multiple files
	tbl, err := setup.cat.CreateTable(setup.ctx, table.Identifier{"bench", "plan_files"}, schema)
	if err != nil {
		b.Fatalf("Failed to create table: %v", err)
	}

	// Add multiple files
	testData := setup.createTestData(1000)
	defer testData.Release()

	var filePaths []string
	for i := 0; i < 10; i++ {
		filename := fmt.Sprintf("plan_test_%d.parquet", i)
		filePath := setup.writeDataFile(tbl, testData, filename)
		filePaths = append(filePaths, filePath)
	}

	tx := tbl.NewTransaction()
	err = tx.AddFiles(setup.ctx, filePaths, nil, false)
	if err != nil {
		b.Fatalf("Failed to add files: %v", err)
	}
	tbl, err = tx.Commit(setup.ctx)
	if err != nil {
		b.Fatalf("Failed to commit: %v", err)
	}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		scan := tbl.Scan()
		_, err := scan.PlanFiles(setup.ctx)
		if err != nil {
			b.Fatalf("Failed to plan files: %v", err)
		}
	}
}

// Schema Operation Benchmarks

func BenchmarkSchemaFieldLookup(b *testing.B) {
	schema := iceberg.NewSchema(0,
		iceberg.NestedField{ID: 1, Name: "field1", Type: iceberg.PrimitiveTypes.String, Required: false},
		iceberg.NestedField{ID: 2, Name: "field2", Type: iceberg.PrimitiveTypes.Int32, Required: false},
		iceberg.NestedField{ID: 3, Name: "field3", Type: iceberg.PrimitiveTypes.Float64, Required: false},
		iceberg.NestedField{ID: 4, Name: "field4", Type: iceberg.PrimitiveTypes.Bool, Required: false},
		iceberg.NestedField{ID: 5, Name: "field5", Type: iceberg.PrimitiveTypes.Date, Required: false},
	)

	fieldNames := []string{"field1", "field2", "field3", "field4", "field5"}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		fieldName := fieldNames[i%len(fieldNames)]
		_, found := schema.FindFieldByName(fieldName)
		if !found {
			b.Fatalf("Field %s not found", fieldName)
		}
	}
}

func BenchmarkSchemaFieldLookupCaseInsensitive(b *testing.B) {
	schema := iceberg.NewSchema(0,
		iceberg.NestedField{ID: 1, Name: "Field1", Type: iceberg.PrimitiveTypes.String, Required: false},
		iceberg.NestedField{ID: 2, Name: "FIELD2", Type: iceberg.PrimitiveTypes.Int32, Required: false},
		iceberg.NestedField{ID: 3, Name: "field3", Type: iceberg.PrimitiveTypes.Float64, Required: false},
		iceberg.NestedField{ID: 4, Name: "FiElD4", Type: iceberg.PrimitiveTypes.Bool, Required: false},
		iceberg.NestedField{ID: 5, Name: "field_5", Type: iceberg.PrimitiveTypes.Date, Required: false},
	)

	fieldNames := []string{"field1", "field2", "field3", "field4", "field_5"}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		fieldName := fieldNames[i%len(fieldNames)]
		_, found := schema.FindFieldByNameCaseInsensitive(fieldName)
		if !found {
			b.Fatalf("Field %s not found", fieldName)
		}
	}
}

func BenchmarkSchemaEquality(b *testing.B) {
	schema1 := iceberg.NewSchema(0,
		iceberg.NestedField{ID: 1, Name: "id", Type: iceberg.PrimitiveTypes.Int64, Required: true},
		iceberg.NestedField{ID: 2, Name: "data", Type: iceberg.PrimitiveTypes.String, Required: false},
	)

	schema2 := iceberg.NewSchema(0,
		iceberg.NestedField{ID: 1, Name: "id", Type: iceberg.PrimitiveTypes.Int64, Required: true},
		iceberg.NestedField{ID: 2, Name: "data", Type: iceberg.PrimitiveTypes.String, Required: false},
	)

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		equal := schema1.Equals(schema2)
		if !equal {
			b.Fatal("Schemas should be equal")
		}
	}
}

// Transaction Benchmarks

func BenchmarkTransactionCreation(b *testing.B) {
	setup := newBenchmarkSetup(b)
	schema := setup.createSimpleSchema()

	tbl, err := setup.cat.CreateTable(setup.ctx, table.Identifier{"bench", "transaction_test"}, schema)
	if err != nil {
		b.Fatalf("Failed to create table: %v", err)
	}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		tx := tbl.NewTransaction()
		_, err := tx.StagedTable()
		if err != nil {
			b.Fatalf("Failed to get staged table: %v", err)
		}
	}
}

func BenchmarkTransactionScan(b *testing.B) {
	setup := newBenchmarkSetup(b)
	schema := setup.createSimpleSchema()

	tbl, err := setup.cat.CreateTable(setup.ctx, table.Identifier{"bench", "transaction_scan"}, schema)
	if err != nil {
		b.Fatalf("Failed to create table: %v", err)
	}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		tx := tbl.NewTransaction()
		_, err := tx.Scan()
		if err != nil {
			b.Fatalf("Failed to create scan: %v", err)
		}
	}
}

// Partition Operation Benchmarks

func BenchmarkPartitionSpecFieldIteration(b *testing.B) {
	partitionSpec := iceberg.NewPartitionSpec(
		iceberg.PartitionField{SourceID: 1, FieldID: 1000, Transform: iceberg.IdentityTransform{}, Name: "field1"},
		iceberg.PartitionField{SourceID: 2, FieldID: 1001, Transform: iceberg.BucketTransform{NumBuckets: 16}, Name: "field2"},
		iceberg.PartitionField{SourceID: 3, FieldID: 1002, Transform: iceberg.TruncateTransform{Width: 10}, Name: "field3"},
		iceberg.PartitionField{SourceID: 4, FieldID: 1003, Transform: iceberg.YearTransform{}, Name: "field4"},
		iceberg.PartitionField{SourceID: 5, FieldID: 1004, Transform: iceberg.MonthTransform{}, Name: "field5"},
	)

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		count := 0
		for range partitionSpec.Fields() {
			count++
		}
		if count != 5 {
			b.Fatalf("Expected 5 fields, got %d", count)
		}
	}
}

func BenchmarkPartitionSpecEquality(b *testing.B) {
	spec1 := iceberg.NewPartitionSpec(
		iceberg.PartitionField{SourceID: 1, FieldID: 1000, Transform: iceberg.IdentityTransform{}, Name: "field1"},
		iceberg.PartitionField{SourceID: 2, FieldID: 1001, Transform: iceberg.BucketTransform{NumBuckets: 16}, Name: "field2"},
	)

	spec2 := iceberg.NewPartitionSpec(
		iceberg.PartitionField{SourceID: 1, FieldID: 1000, Transform: iceberg.IdentityTransform{}, Name: "field1"},
		iceberg.PartitionField{SourceID: 2, FieldID: 1001, Transform: iceberg.BucketTransform{NumBuckets: 16}, Name: "field2"},
	)

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		equal := spec1.Equals(spec2)
		if !equal {
			b.Fatal("Partition specs should be equal")
		}
	}
}

// Property and Metadata Benchmarks

func BenchmarkPropertyAccess(b *testing.B) {
	setup := newBenchmarkSetup(b)
	schema := setup.createSimpleSchema()

	props := iceberg.Properties{
		"prop1": "value1",
		"prop2": "value2",
		"prop3": "value3",
		"prop4": "value4",
		"prop5": "value5",
	}

	tbl, err := setup.cat.CreateTable(setup.ctx, table.Identifier{"bench", "properties"}, schema,
		catalog.WithProperties(props))
	if err != nil {
		b.Fatalf("Failed to create table: %v", err)
	}

	keys := []string{"prop1", "prop2", "prop3", "prop4", "prop5"}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		metadata := tbl.Metadata()
		properties := metadata.Properties()
		
		key := keys[i%len(keys)]
		_, exists := properties[key]
		if !exists {
			b.Fatalf("Property %s not found", key)
		}
	}
}

func BenchmarkMetadataAccess(b *testing.B) {
	setup := newBenchmarkSetup(b)
	schema := setup.createComplexSchema()

	tbl, err := setup.cat.CreateTable(setup.ctx, table.Identifier{"bench", "metadata_access"}, schema)
	if err != nil {
		b.Fatalf("Failed to create table: %v", err)
	}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		metadata := tbl.Metadata()
		
		// Access various metadata components
		_ = metadata.CurrentSchema()
		_ = metadata.PartitionSpec()
		_ = metadata.Location()
		_ = metadata.Properties()
	}
}

// Composite Benchmarks (Real-world scenarios)

func BenchmarkEndToEndWorkflow(b *testing.B) {
	setup := newBenchmarkSetup(b)
	schema := setup.createSimpleSchema()

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		b.StopTimer()
		tableName := fmt.Sprintf("e2e_table_%d", i)
		b.StartTimer()

		// Create table
		tbl, err := setup.cat.CreateTable(setup.ctx, table.Identifier{"bench", tableName}, schema)
		if err != nil {
			b.Fatalf("Failed to create table: %v", err)
		}

		b.StopTimer()
		// Prepare data
		testData := setup.createTestData(1000)
		filePath := setup.writeDataFile(tbl, testData, fmt.Sprintf("e2e_%d.parquet", i))
		testData.Release()
		b.StartTimer()

		// Write data
		tx := tbl.NewTransaction()
		err = tx.AddFiles(setup.ctx, []string{filePath}, nil, false)
		if err != nil {
			b.Fatalf("Failed to add files: %v", err)
		}

		tbl, err = tx.Commit(setup.ctx)
		if err != nil {
			b.Fatalf("Failed to commit: %v", err)
		}

		// Read data back
		scan := tbl.Scan()
		result, err := scan.ToArrowTable(setup.ctx)
		if err != nil {
			b.Fatalf("Failed to scan: %v", err)
		}
		result.Release()
	}
} 