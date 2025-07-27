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

package spark

import (
	"context"
	"fmt"
	"log"

	"github.com/apache/iceberg-go"
	"github.com/apache/iceberg-go/table"
)

// SparkIcebergIntegration provides a high-level integration interface
type SparkIcebergIntegration struct {
	util      *SparkUtil
	tableUtil *SparkTableUtil
	scan      *SparkScan
	write     *SparkWrite
	batch     *SparkBatch
	actions   *SparkActions
	streaming *SparkMicroBatchStream
}

// NewSparkIcebergIntegration creates a new integration instance
func NewSparkIcebergIntegration(sparkHost string, sparkPort int, options map[string]string) (*SparkIcebergIntegration, error) {
	ctx := context.Background()

	// Connect to Spark
	session, err := ConnectToSpark(ctx, sparkHost, sparkPort, options)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to Spark: %w", err)
	}

	// Create utility components
	util := NewSparkUtil(session)

	return &SparkIcebergIntegration{
		util:      util,
		tableUtil: NewSparkTableUtil(util),
		scan:      NewSparkScan(util),
		write:     NewSparkWrite(util),
		batch:     NewSparkBatch(util),
		actions:   NewSparkActions(util),
		streaming: NewSparkMicroBatchStream(util),
	}, nil
}

// ExampleUsage demonstrates comprehensive usage of the Spark-Iceberg integration
func (s *SparkIcebergIntegration) ExampleUsage() error {
	ctx := context.Background()

	// 1. Validate Spark compatibility
	log.Println("Validating Spark compatibility...")
	if err := s.util.ValidateSparkCompatibility(ctx); err != nil {
		return fmt.Errorf("Spark compatibility check failed: %w", err)
	}

	// 2. Create Iceberg catalog
	log.Println("Creating Iceberg catalog...")
	catalogProps := map[string]string{
		"type":                 "hive",
		"uri":                  "thrift://hive-metastore:9083",
		"warehouse":            "s3a://warehouse/",
		"io-impl":              "org.apache.iceberg.aws.s3.S3FileIO",
		"s3.endpoint":          "http://minio:9000",
		"s3.access-key-id":     "minioadmin",
		"s3.secret-access-key": "minioadmin",
		"s3.path-style-access": "true",
	}

	if err := s.util.CreateCatalogIfNotExists(ctx, "iceberg_catalog", catalogProps); err != nil {
		return fmt.Errorf("failed to create catalog: %w", err)
	}

	// 3. Create sample schema
	log.Println("Creating sample table schema...")
	schema := iceberg.NewSchema(0,
		iceberg.NestedField{ID: 1, Name: "id", Type: iceberg.PrimitiveTypes.Int64, Required: true},
		iceberg.NestedField{ID: 2, Name: "name", Type: iceberg.PrimitiveTypes.String, Required: true},
		iceberg.NestedField{ID: 3, Name: "email", Type: iceberg.PrimitiveTypes.String, Required: false},
		iceberg.NestedField{ID: 4, Name: "created_at", Type: iceberg.PrimitiveTypes.TimestampTz, Required: true},
		iceberg.NestedField{ID: 5, Name: "department", Type: iceberg.PrimitiveTypes.String, Required: false},
	)

	// 4. Create Iceberg table
	log.Println("Creating Iceberg table...")
	tableIdentifier := table.Identifier{"iceberg_catalog", "demo_db", "users"}
	tableOptions := map[string]string{
		"write.format.default":         "parquet",
		"write.parquet.compression":    "snappy",
		"write.target-file-size-bytes": "134217728", // 128MB
	}

	if err := s.tableUtil.CreateTable(ctx, tableIdentifier, schema, tableOptions); err != nil {
		return fmt.Errorf("failed to create table: %w", err)
	}

	// 5. Write sample data
	log.Println("Writing sample data...")
	if err := s.writeSampleData(ctx, tableIdentifier); err != nil {
		return fmt.Errorf("failed to write sample data: %w", err)
	}

	// 6. Scan and read data
	log.Println("Scanning table data...")
	if err := s.scanTableData(ctx, tableIdentifier); err != nil {
		return fmt.Errorf("failed to scan table data: %w", err)
	}

	// 7. Perform table maintenance
	log.Println("Performing table maintenance...")
	if err := s.performTableMaintenance(ctx, tableIdentifier); err != nil {
		return fmt.Errorf("failed to perform table maintenance: %w", err)
	}

	// 8. Schema evolution example
	log.Println("Demonstrating schema evolution...")
	if err := s.demonstrateSchemaEvolution(ctx, tableIdentifier); err != nil {
		return fmt.Errorf("failed to demonstrate schema evolution: %w", err)
	}

	// 9. Batch processing example
	log.Println("Demonstrating batch processing...")
	if err := s.demonstrateBatchProcessing(ctx, tableIdentifier); err != nil {
		return fmt.Errorf("failed to demonstrate batch processing: %w", err)
	}

	// 10. Streaming example (simplified)
	log.Println("Demonstrating streaming...")
	if err := s.demonstrateStreaming(ctx, tableIdentifier); err != nil {
		log.Printf("Streaming demonstration failed (may not be fully supported): %v", err)
	}

	log.Println("✅ Spark-Iceberg integration example completed successfully!")

	return nil
}

// writeSampleData inserts sample data into the table
func (s *SparkIcebergIntegration) writeSampleData(ctx context.Context, identifier table.Identifier) error {
	// Create a DataFrame with sample data using SQL
	sampleDataSQL := `
		SELECT * FROM VALUES 
		(1L, 'John Doe', 'john@example.com', TIMESTAMP '2023-01-01 10:00:00', 'Engineering'),
		(2L, 'Jane Smith', 'jane@example.com', TIMESTAMP '2023-01-02 11:00:00', 'Marketing'),
		(3L, 'Bob Johnson', 'bob@example.com', TIMESTAMP '2023-01-03 12:00:00', 'Sales'),
		(4L, 'Alice Brown', 'alice@example.com', TIMESTAMP '2023-01-04 13:00:00', 'Engineering'),
		(5L, 'Charlie Wilson', 'charlie@example.com', TIMESTAMP '2023-01-05 14:00:00', 'HR')
		AS t(id, name, email, created_at, department)
	`

	df, err := s.util.ExecuteSQL(ctx, sampleDataSQL)
	if err != nil {
		return fmt.Errorf("failed to create sample data DataFrame: %w", err)
	}

	writeOptions := &WriteOptions{
		Mode:             WriteModeAppend,
		PartitionColumns: []string{"department"},
		TableProperties: map[string]string{
			"write.distribution-mode": "hash",
		},
	}

	return s.write.WriteDataFrame(ctx, df, identifier, writeOptions)
}

// scanTableData demonstrates various scan operations
func (s *SparkIcebergIntegration) scanTableData(ctx context.Context, identifier table.Identifier) error {
	// Basic table scan
	scanOptions := &ScanOptions{
		SelectedColumns: []string{"id", "name", "department"},
		Filter:          "department = 'Engineering'",
		BatchSize:       1000,
	}

	result, err := s.scan.ScanTable(ctx, identifier, scanOptions)
	if err != nil {
		return fmt.Errorf("failed to scan table: %w", err)
	}

	log.Printf("Scanned %d rows from table", result.RowCount)

	// Get table statistics
	stats, err := s.scan.GetTableStatistics(ctx, identifier)
	if err != nil {
		return fmt.Errorf("failed to get table statistics: %w", err)
	}

	log.Printf("Table statistics: %+v", stats)

	// Get table files
	files, err := s.scan.GetTableFiles(ctx, identifier, scanOptions)
	if err != nil {
		return fmt.Errorf("failed to get table files: %w", err)
	}

	log.Printf("Table has %d data files", len(files))

	return nil
}

// performTableMaintenance demonstrates table maintenance operations
func (s *SparkIcebergIntegration) performTableMaintenance(ctx context.Context, identifier table.Identifier) error {
	// Compact table data files
	compactionAction := &RewriteDataFilesAction{
		TableIdentifier: identifier,
		Strategy:        CompactionStrategyBinPack,
		TargetSizeBytes: 134217728, // 128MB
		MaxConcurrency:  4,
	}

	result, err := s.actions.ExecuteRewriteDataFiles(ctx, compactionAction)
	if err != nil {
		return fmt.Errorf("failed to compact table: %w", err)
	}

	log.Printf("Compaction result: %s", result.Message)

	// Rewrite manifests
	manifestAction := &RewriteManifestsAction{
		TableIdentifier: identifier,
		UseSparkServer:  true,
	}

	result, err = s.actions.ExecuteRewriteManifests(ctx, manifestAction)
	if err != nil {
		return fmt.Errorf("failed to rewrite manifests: %w", err)
	}

	log.Printf("Manifest rewrite result: %s", result.Message)

	// Compute table statistics
	statsAction := &ComputeTableStatsAction{
		TableIdentifier: identifier,
		Columns:         []string{"id", "name", "department"},
	}

	result, err = s.actions.ExecuteComputeTableStats(ctx, statsAction)
	if err != nil {
		return fmt.Errorf("failed to compute stats: %w", err)
	}

	log.Printf("Statistics computation result: %s", result.Message)

	return nil
}

// demonstrateSchemaEvolution shows schema evolution capabilities
func (s *SparkIcebergIntegration) demonstrateSchemaEvolution(ctx context.Context, identifier table.Identifier) error {
	evolution := &SchemaEvolution{
		AddColumns: []AddColumn{
			{Name: "age", Type: "INT", Comment: "Employee age"},
			{Name: "salary", Type: "DECIMAL(10,2)", Comment: "Employee salary"},
		},
		RenameColumns: map[string]string{
			"email": "email_address",
		},
	}

	result, err := s.actions.ExecuteSchemaEvolution(ctx, identifier, evolution)
	if err != nil {
		return fmt.Errorf("failed to evolve schema: %w", err)
	}

	log.Printf("Schema evolution result: %s", result.Message)

	return nil
}

// demonstrateBatchProcessing shows batch processing capabilities
func (s *SparkIcebergIntegration) demonstrateBatchProcessing(ctx context.Context, identifier table.Identifier) error {
	// Create a batch job that aggregates data
	tableName := FormatTableIdentifier(identifier)
	aggregationSQL := fmt.Sprintf(`
		SELECT 
			department,
			COUNT(*) as employee_count,
			MIN(created_at) as first_hire_date,
			MAX(created_at) as last_hire_date
		FROM %s
		GROUP BY department
	`, tableName)

	outputIdentifier := table.Identifier{"iceberg_catalog", "demo_db", "department_stats"}

	job := &BatchJob{
		Name:        "department_aggregation",
		Description: "Aggregate employee data by department",
		InputTables: []table.Identifier{identifier},
		OutputTable: outputIdentifier,
		SQL:         aggregationSQL,
		Options: &BatchOptions{
			Parallelism:   4,
			MemoryPerTask: "2g",
			CoresPerTask:  2,
			WriteOptions: &WriteOptions{
				Mode: WriteModeOverwrite,
			},
		},
	}

	result, err := s.batch.ExecuteBatchJob(ctx, job)
	if err != nil {
		return fmt.Errorf("failed to execute batch job: %w", err)
	}

	log.Printf("Batch job result: %s, rows written: %d", result.Status, result.RowsWritten)

	return nil
}

// demonstrateStreaming shows streaming capabilities (simplified)
func (s *SparkIcebergIntegration) demonstrateStreaming(ctx context.Context, identifier table.Identifier) error {
	// This is a simplified example - in practice, you'd have a real streaming source
	log.Println("Note: Streaming example is simplified and may require additional setup")

	streamingOptions := &StreamingOptions{
		CheckpointLocation:     "/tmp/checkpoints/demo",
		Trigger:                TriggerProcessingTime,
		ProcessingTimeInterval: "10 seconds",
		OutputMode:             OutputModeAppend,
		MaxFilesPerTrigger:     10,
	}

	// In a real scenario, you would:
	// 1. Read from a streaming source (Kafka, Kinesis, etc.)
	// 2. Apply transformations
	// 3. Write to Iceberg table

	log.Printf("Streaming configuration: %+v", streamingOptions)
	log.Println("Streaming setup completed (actual streaming would require external source)")

	return nil
}

// Close cleans up resources
func (s *SparkIcebergIntegration) Close() error {
	return s.util.Close()
}

// GetSparkUtil returns the underlying SparkUtil for advanced operations
func (s *SparkIcebergIntegration) GetSparkUtil() *SparkUtil {
	return s.util
}

// GetTableUtil returns the SparkTableUtil for table operations
func (s *SparkIcebergIntegration) GetTableUtil() *SparkTableUtil {
	return s.tableUtil
}

// GetScan returns the SparkScan for scan operations
func (s *SparkIcebergIntegration) GetScan() *SparkScan {
	return s.scan
}

// GetWrite returns the SparkWrite for write operations
func (s *SparkIcebergIntegration) GetWrite() *SparkWrite {
	return s.write
}

// GetBatch returns the SparkBatch for batch operations
func (s *SparkIcebergIntegration) GetBatch() *SparkBatch {
	return s.batch
}

// GetActions returns the SparkActions for table actions
func (s *SparkIcebergIntegration) GetActions() *SparkActions {
	return s.actions
}

// GetStreaming returns the SparkMicroBatchStream for streaming operations
func (s *SparkIcebergIntegration) GetStreaming() *SparkMicroBatchStream {
	return s.streaming
}
