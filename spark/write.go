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
	"errors"
	"fmt"
	"strings"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/iceberg-go"
	"github.com/apache/iceberg-go/table"
	// "github.com/apache/spark-connect-go/spark" // Commented out due to library versioning issues
)

// WriteMode defines the write behavior for Spark write operations
type WriteMode string

const (
	WriteModeAppend    WriteMode = "append"
	WriteModeOverwrite WriteMode = "overwrite"
	WriteModeMerge     WriteMode = "merge"
)

// SparkWrite provides write operations for Iceberg tables through Spark
type SparkWrite struct {
	util      *SparkUtil
	converter *SparkValueConverter
}

// NewSparkWrite creates a new SparkWrite instance
func NewSparkWrite(util *SparkUtil) *SparkWrite {
	return &SparkWrite{
		util:      util,
		converter: NewSparkValueConverter(),
	}
}

// WriteOptions contains options for write operations
type WriteOptions struct {
	Mode               WriteMode
	PartitionColumns   []string
	SortColumns        []string
	TableProperties    map[string]string
	WriterProperties   map[string]string
	SnapshotProperties map[string]string
}

// WriteArrowTable writes an Arrow table to an Iceberg table through Spark
func (w *SparkWrite) WriteArrowTable(
	ctx context.Context,
	arrowTable arrow.Table,
	identifier table.Identifier,
	schema *iceberg.Schema,
	options *WriteOptions,
) error {
	tableName := FormatTableIdentifier(identifier)

	// Create temporary view from Arrow table data
	tempViewName := "temp_view_" + strings.ReplaceAll(strings.Join(identifier, "_"), ".", "_")

	// Convert Arrow table to Spark DataFrame
	// Note: This is a simplified approach - in practice, you'd need to use Spark's Arrow integration
	df, err := w.createDataFrameFromArrowTable(ctx, arrowTable, schema, tempViewName)
	if err != nil {
		return fmt.Errorf("failed to create DataFrame from Arrow table: %w", err)
	}

	// Write DataFrame to Iceberg table
	err = w.writeDataFrame(ctx, df, tableName, options)
	if err != nil {
		return fmt.Errorf("failed to write DataFrame to table %s: %w", tableName, err)
	}

	// Clean up temporary view
	_, err = w.util.ExecuteSQL(ctx, "DROP VIEW IF EXISTS "+tempViewName)
	if err != nil {
		// Log warning but don't fail the operation
		fmt.Printf("Warning: failed to drop temporary view %s: %v\n", tempViewName, err)
	}

	return nil
}

// WriteDataFrame writes a Spark DataFrame to an Iceberg table
func (w *SparkWrite) WriteDataFrame(
	ctx context.Context,
	df *DataFrame,
	identifier table.Identifier,
	options *WriteOptions,
) error {
	tableName := FormatTableIdentifier(identifier)

	return w.writeDataFrame(ctx, df, tableName, options)
}

// InsertInto inserts data into an existing Iceberg table
func (w *SparkWrite) InsertInto(
	ctx context.Context,
	df *DataFrame,
	identifier table.Identifier,
	options *WriteOptions,
) error {
	tableName := FormatTableIdentifier(identifier)

	// Build INSERT INTO statement
	sql := "INSERT INTO " + tableName

	// Add partition specification if provided
	if len(options.PartitionColumns) > 0 {
		partitions := strings.Join(options.PartitionColumns, ", ")
		sql += fmt.Sprintf(" PARTITION (%s)", partitions)
	}

	// Create temporary view for the DataFrame
	tempViewName := "temp_insert_" + strings.ReplaceAll(strings.Join(identifier, "_"), ".", "_")
	err := df.CreateOrReplaceTempView(tempViewName)
	if err != nil {
		return fmt.Errorf("failed to create temporary view: %w", err)
	}

	sql += " SELECT * FROM " + tempViewName

	_, err = w.util.ExecuteSQL(ctx, sql)
	if err != nil {
		return fmt.Errorf("failed to execute INSERT INTO: %w", err)
	}

	// Clean up temporary view
	_, _ = w.util.ExecuteSQL(ctx, "DROP VIEW IF EXISTS "+tempViewName)

	return nil
}

// MergeInto performs a merge operation on an Iceberg table
func (w *SparkWrite) MergeInto(
	ctx context.Context,
	sourceDF *DataFrame,
	targetIdentifier table.Identifier,
	mergeCondition string,
	updateActions map[string]string,
	insertActions map[string]string,
) error {
	targetTableName := FormatTableIdentifier(targetIdentifier)

	// Create temporary view for source DataFrame
	sourceViewName := "temp_merge_source_" + strings.ReplaceAll(strings.Join(targetIdentifier, "_"), ".", "_")
	err := sourceDF.CreateOrReplaceTempView(sourceViewName)
	if err != nil {
		return fmt.Errorf("failed to create source temporary view: %w", err)
	}
	defer func() {
		_, _ = w.util.ExecuteSQL(ctx, "DROP VIEW IF EXISTS "+sourceViewName)
	}()

	// Build MERGE statement
	sql := fmt.Sprintf("MERGE INTO %s AS target USING %s AS source ON %s",
		targetTableName, sourceViewName, mergeCondition)

	// Add update actions
	if len(updateActions) > 0 {
		var updates []string
		for column, expression := range updateActions {
			updates = append(updates, fmt.Sprintf("%s = %s", column, expression))
		}
		sql += " WHEN MATCHED THEN UPDATE SET " + strings.Join(updates, ", ")
	}

	// Add insert actions
	if len(insertActions) > 0 {
		var columns []string
		var values []string
		for column, expression := range insertActions {
			columns = append(columns, column)
			values = append(values, expression)
		}
		sql += fmt.Sprintf(" WHEN NOT MATCHED THEN INSERT (%s) VALUES (%s)",
			strings.Join(columns, ", "), strings.Join(values, ", "))
	}

	_, err = w.util.ExecuteSQL(ctx, sql)
	if err != nil {
		return fmt.Errorf("failed to execute MERGE INTO: %w", err)
	}

	return nil
}

// createDataFrameFromArrowTable creates a Spark DataFrame from an Arrow table
func (w *SparkWrite) createDataFrameFromArrowTable(
	ctx context.Context,
	arrowTable arrow.Table,
	schema *iceberg.Schema,
	tempViewName string,
) (*DataFrame, error) {
	// Convert Arrow table to row data
	records := make([]interface{}, arrowTable.NumRows())

	for i := 0; i < int(arrowTable.NumRows()); i++ {
		row := make(map[string]interface{})

		for j, field := range schema.Fields() {
			column := arrowTable.Column(j)
			chunk := column.Data().Chunk(0) // Assuming single chunk for simplicity

			var value interface{}
			if chunk.IsNull(i) {
				value = nil
			} else {
				var err error
				value, err = w.converter.extractArrowValue(chunk, i, field.Type)
				if err != nil {
					return nil, fmt.Errorf("failed to extract value for field %s: %w", field.Name, err)
				}
			}

			row[field.Name] = value
		}

		records[i] = row
	}

	// This is a simplified approach - in practice, you'd use Spark's Arrow integration
	// For now, we'll create a temporary table by inserting the data
	return w.createDataFrameFromRows(ctx, records, schema, tempViewName)
}

// createDataFrameFromRows creates a DataFrame from row data
func (w *SparkWrite) createDataFrameFromRows(
	ctx context.Context,
	rows []interface{},
	schema *iceberg.Schema,
	tempViewName string,
) (*DataFrame, error) {
	// This is a placeholder implementation
	// In practice, you'd need to use Spark's data source APIs or create a temporary table

	// Create empty DataFrame with schema
	fields := schema.Fields()
	columns := make([]string, len(fields))
	for i, field := range fields {
		columns[i] = field.Name
	}

	// For now, return an error indicating this needs proper implementation
	return nil, errors.New("creating DataFrame from rows not yet fully implemented - requires Spark data source integration")
}

// writeDataFrame writes a DataFrame to an Iceberg table
func (w *SparkWrite) writeDataFrame(
	ctx context.Context,
	df *DataFrame,
	tableName string,
	options *WriteOptions,
) error {
	writer := df.Write()

	// Set write mode
	switch options.Mode {
	case WriteModeAppend:
		writer = writer.Mode("append")
	case WriteModeOverwrite:
		writer = writer.Mode("overwrite")
	case WriteModeMerge:
		return errors.New("merge mode should use MergeInto method instead")
	default:
		writer = writer.Mode("append") // default to append
	}

	// Set format to Iceberg
	writer = writer.Format("iceberg")

	// Add table properties
	for key, value := range options.TableProperties {
		writer = writer.Option(key, value)
	}

	// Add writer properties
	for key, value := range options.WriterProperties {
		writer = writer.Option(key, value)
	}

	// Set partition columns
	if len(options.PartitionColumns) > 0 {
		writer = writer.PartitionBy(options.PartitionColumns...)
	}

	// Set sort columns
	if len(options.SortColumns) > 0 {
		writer = writer.SortBy(options.SortColumns...)
	}

	// Execute write
	err := writer.SaveAsTable(tableName)
	if err != nil {
		return fmt.Errorf("failed to save DataFrame as table: %w", err)
	}

	return nil
}

// DeleteFromTable deletes rows from an Iceberg table based on a condition
func (w *SparkWrite) DeleteFromTable(
	ctx context.Context,
	identifier table.Identifier,
	condition string,
) error {
	tableName := FormatTableIdentifier(identifier)

	sql := fmt.Sprintf("DELETE FROM %s WHERE %s", tableName, condition)
	_, err := w.util.ExecuteSQL(ctx, sql)
	if err != nil {
		return fmt.Errorf("failed to delete from table %s: %w", tableName, err)
	}

	return nil
}

// UpdateTable updates rows in an Iceberg table
func (w *SparkWrite) UpdateTable(
	ctx context.Context,
	identifier table.Identifier,
	updates map[string]string,
	condition string,
) error {
	tableName := FormatTableIdentifier(identifier)

	var updateClauses []string
	for column, expression := range updates {
		updateClauses = append(updateClauses, fmt.Sprintf("%s = %s", column, expression))
	}

	sql := fmt.Sprintf("UPDATE %s SET %s", tableName, strings.Join(updateClauses, ", "))
	if condition != "" {
		sql += " WHERE " + condition
	}

	_, err := w.util.ExecuteSQL(ctx, sql)
	if err != nil {
		return fmt.Errorf("failed to update table %s: %w", tableName, err)
	}

	return nil
}
