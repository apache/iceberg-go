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

	"github.com/apache/iceberg-go/table"
	// "github.com/apache/spark-connect-go/spark" // Commented out due to library versioning issues
)

// SparkBatch provides batch operations for Iceberg tables through Spark
type SparkBatch struct {
	util      *SparkUtil
	scan      *SparkScan
	write     *SparkWrite
	converter *SparkValueConverter
}

// NewSparkBatch creates a new SparkBatch instance
func NewSparkBatch(util *SparkUtil) *SparkBatch {
	return &SparkBatch{
		util:      util,
		scan:      NewSparkScan(util),
		write:     NewSparkWrite(util),
		converter: NewSparkValueConverter(),
	}
}

// BatchJob represents a batch processing job configuration
type BatchJob struct {
	Name        string
	Description string
	InputTables []table.Identifier
	OutputTable table.Identifier
	SQL         string
	Options     *BatchOptions
}

// BatchOptions contains options for batch operations
type BatchOptions struct {
	// Parallelism level for the job
	Parallelism int
	// Memory allocation for the job
	MemoryPerTask string
	// Number of cores per task
	CoresPerTask int
	// Checkpoint location for fault tolerance
	CheckpointLocation string
	// Custom Spark configurations
	SparkConfigs map[string]string
	// Write options
	WriteOptions *WriteOptions
}

// BatchResult contains the result of a batch operation
type BatchResult struct {
	JobID         string
	Status        string
	RowsRead      int64
	RowsWritten   int64
	ExecutionTime int64 // in milliseconds
	Errors        []string
}

// ExecuteBatchJob executes a batch processing job
func (b *SparkBatch) ExecuteBatchJob(ctx context.Context, job *BatchJob) (*BatchResult, error) {
	// Apply job configurations
	if err := b.applyJobConfigurations(ctx, job.Options); err != nil {
		return nil, fmt.Errorf("failed to apply job configurations: %w", err)
	}

	// Execute the SQL transformation
	df, err := b.util.ExecuteSQL(ctx, job.SQL)
	if err != nil {
		return nil, fmt.Errorf("failed to execute batch SQL: %w", err)
	}

	// Get row count for metrics
	countDF, err := b.util.ExecuteSQL(ctx, fmt.Sprintf("SELECT COUNT(*) FROM (%s) AS batch_result", job.SQL))
	if err != nil {
		return nil, fmt.Errorf("failed to get result count: %w", err)
	}

	countRows, err := countDF.Collect(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to collect count: %w", err)
	}

	var rowsWritten int64
	if len(countRows) > 0 && len(countRows[0]) > 0 {
		if count, ok := countRows[0][0].(int64); ok {
			rowsWritten = count
		}
	}

	// Write the result to the output table
	writeOptions := job.Options.WriteOptions
	if writeOptions == nil {
		writeOptions = &WriteOptions{Mode: WriteModeAppend}
	}

	err = b.write.WriteDataFrame(ctx, df, job.OutputTable, writeOptions)
	if err != nil {
		return nil, fmt.Errorf("failed to write batch result: %w", err)
	}

	return &BatchResult{
		JobID:       job.Name,
		Status:      "SUCCESS",
		RowsWritten: rowsWritten,
		Errors:      []string{},
	}, nil
}

// CompactTable performs table compaction using Spark
func (b *SparkBatch) CompactTable(
	ctx context.Context,
	identifier table.Identifier,
	options *CompactionOptions,
) (*BatchResult, error) {
	tableName := FormatTableIdentifier(identifier)

	// Build compaction SQL
	var sql string
	if options.Strategy == CompactionStrategySort {
		// Sort-based compaction
		sortColumns := ""
		if len(options.SortColumns) > 0 {
			sortColumns = " ORDER BY " + joinColumns(options.SortColumns)
		}
		sql = fmt.Sprintf("CALL iceberg.system.rewrite_data_files('%s'%s)", tableName, sortColumns)
	} else {
		// Bin-packing compaction (default)
		sql = fmt.Sprintf("CALL iceberg.system.rewrite_data_files('%s')", tableName)
	}

	_, err := b.util.ExecuteSQL(ctx, sql)
	if err != nil {
		return nil, fmt.Errorf("failed to execute compaction: %w", err)
	}

	return &BatchResult{
		JobID:  "compact_" + tableName,
		Status: "SUCCESS",
	}, nil
}

// ExpireSnapshots removes old snapshots from an Iceberg table
func (b *SparkBatch) ExpireSnapshots(
	ctx context.Context,
	identifier table.Identifier,
	olderThanTimestamp int64,
	retainLast int,
) (*BatchResult, error) {
	tableName := FormatTableIdentifier(identifier)

	sql := fmt.Sprintf(
		"CALL iceberg.system.expire_snapshots('%s', TIMESTAMP '%d', %d)",
		tableName,
		olderThanTimestamp,
		retainLast,
	)

	_, err := b.util.ExecuteSQL(ctx, sql)
	if err != nil {
		return nil, fmt.Errorf("failed to expire snapshots: %w", err)
	}

	return &BatchResult{
		JobID:  "expire_snapshots_" + tableName,
		Status: "SUCCESS",
	}, nil
}

// RewriteManifests rewrites manifest files for better performance
func (b *SparkBatch) RewriteManifests(
	ctx context.Context,
	identifier table.Identifier,
) (*BatchResult, error) {
	tableName := FormatTableIdentifier(identifier)

	sql := fmt.Sprintf("CALL iceberg.system.rewrite_manifests('%s')", tableName)

	_, err := b.util.ExecuteSQL(ctx, sql)
	if err != nil {
		return nil, fmt.Errorf("failed to rewrite manifests: %w", err)
	}

	return &BatchResult{
		JobID:  "rewrite_manifests_" + tableName,
		Status: "SUCCESS",
	}, nil
}

// MigrateTable migrates a table from another format to Iceberg
func (b *SparkBatch) MigrateTable(
	ctx context.Context,
	sourceTable string,
	targetIdentifier table.Identifier,
	migrationOptions *MigrationOptions,
) (*BatchResult, error) {
	targetTableName := FormatTableIdentifier(targetIdentifier)

	// Build migration SQL based on source format
	var sql string
	switch migrationOptions.SourceFormat {
	case "parquet":
		sql = fmt.Sprintf(
			"CALL iceberg.system.migrate('%s', '%s')",
			sourceTable,
			targetTableName,
		)
	case "delta":
		sql = fmt.Sprintf(
			"CALL iceberg.system.snapshot('%s', '%s')",
			sourceTable,
			targetTableName,
		)
	default:
		return nil, fmt.Errorf("unsupported source format: %s", migrationOptions.SourceFormat)
	}

	_, err := b.util.ExecuteSQL(ctx, sql)
	if err != nil {
		return nil, fmt.Errorf("failed to migrate table: %w", err)
	}

	return &BatchResult{
		JobID:  fmt.Sprintf("migrate_%s_to_%s", sourceTable, targetTableName),
		Status: "SUCCESS",
	}, nil
}

// AnalyzeTable computes statistics for an Iceberg table
func (b *SparkBatch) AnalyzeTable(
	ctx context.Context,
	identifier table.Identifier,
	computeColumnStats bool,
) (*BatchResult, error) {
	tableName := FormatTableIdentifier(identifier)

	var sql string
	if computeColumnStats {
		sql = fmt.Sprintf("ANALYZE TABLE %s COMPUTE STATISTICS FOR ALL COLUMNS", tableName)
	} else {
		sql = fmt.Sprintf("ANALYZE TABLE %s COMPUTE STATISTICS", tableName)
	}

	_, err := b.util.ExecuteSQL(ctx, sql)
	if err != nil {
		return nil, fmt.Errorf("failed to analyze table: %w", err)
	}

	return &BatchResult{
		JobID:  "analyze_" + tableName,
		Status: "SUCCESS",
	}, nil
}

// applyJobConfigurations applies batch job configurations to the Spark session
func (b *SparkBatch) applyJobConfigurations(ctx context.Context, options *BatchOptions) error {
	if options == nil {
		return nil
	}

	// Apply Spark configurations
	for key, value := range options.SparkConfigs {
		sql := fmt.Sprintf("SET %s = %s", key, value)
		_, err := b.util.ExecuteSQL(ctx, sql)
		if err != nil {
			return fmt.Errorf("failed to set Spark config %s: %w", key, err)
		}
	}

	// Set parallelism
	if options.Parallelism > 0 {
		sql := fmt.Sprintf("SET spark.sql.shuffle.partitions = %d", options.Parallelism)
		_, err := b.util.ExecuteSQL(ctx, sql)
		if err != nil {
			return fmt.Errorf("failed to set parallelism: %w", err)
		}
	}

	// Set memory per task
	if options.MemoryPerTask != "" {
		sql := "SET spark.executor.memory = " + options.MemoryPerTask
		_, err := b.util.ExecuteSQL(ctx, sql)
		if err != nil {
			return fmt.Errorf("failed to set memory per task: %w", err)
		}
	}

	// Set cores per task
	if options.CoresPerTask > 0 {
		sql := fmt.Sprintf("SET spark.executor.cores = %d", options.CoresPerTask)
		_, err := b.util.ExecuteSQL(ctx, sql)
		if err != nil {
			return fmt.Errorf("failed to set cores per task: %w", err)
		}
	}

	return nil
}

// CompactionStrategy defines the strategy for table compaction
type CompactionStrategy string

const (
	CompactionStrategyBinPack CompactionStrategy = "binpack"
	CompactionStrategySort    CompactionStrategy = "sort"
)

// CompactionOptions contains options for table compaction
type CompactionOptions struct {
	Strategy    CompactionStrategy
	SortColumns []string
	TargetSize  int64 // Target file size in bytes
}

// MigrationOptions contains options for table migration
type MigrationOptions struct {
	SourceFormat   string
	TargetLocation string
	Properties     map[string]string
}

// joinColumns joins column names with commas
func joinColumns(columns []string) string {
	if len(columns) == 0 {
		return ""
	}
	result := columns[0]
	for i := 1; i < len(columns); i++ {
		result += ", " + columns[i]
	}

	return result
}
