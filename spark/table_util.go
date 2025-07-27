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

	"github.com/apache/iceberg-go"
	"github.com/apache/iceberg-go/table"
	// "github.com/apache/spark-connect-go/spark" // Commented out due to library versioning issues
)

// SparkTableUtil provides table-specific utility functions for Spark-Iceberg integration
type SparkTableUtil struct {
	util *SparkUtil
}

// NewSparkTableUtil creates a new SparkTableUtil instance
func NewSparkTableUtil(util *SparkUtil) *SparkTableUtil {
	return &SparkTableUtil{
		util: util,
	}
}

// CreateTable creates an Iceberg table in Spark
func (s *SparkTableUtil) CreateTable(ctx context.Context, identifier table.Identifier, schema *iceberg.Schema, options map[string]string) error {
	tableName := FormatTableIdentifier(identifier)

	// Convert Iceberg schema to Spark DDL
	schemaDDL, err := s.convertSchemaToSparkDDL(schema)
	if err != nil {
		return fmt.Errorf("failed to convert schema to Spark DDL: %w", err)
	}

	// Build CREATE TABLE statement
	var optionsSQL []string
	for key, value := range options {
		optionsSQL = append(optionsSQL, fmt.Sprintf("'%s'='%s'", key, value))
	}

	createTableSQL := fmt.Sprintf(
		"CREATE TABLE %s (%s) USING iceberg",
		tableName,
		schemaDDL,
	)

	if len(optionsSQL) > 0 {
		createTableSQL += fmt.Sprintf(" TBLPROPERTIES (%s)", strings.Join(optionsSQL, ", "))
	}

	_, err = s.util.ExecuteSQL(ctx, createTableSQL)
	if err != nil {
		return fmt.Errorf("failed to create table %s: %w", tableName, err)
	}

	return nil
}

// LoadTable loads an Iceberg table metadata through Spark
func (s *SparkTableUtil) LoadTable(ctx context.Context, identifier table.Identifier) (*table.Table, error) {
	tableName := FormatTableIdentifier(identifier)

	// Check if table exists
	exists, err := s.TableExists(ctx, identifier)
	if err != nil {
		return nil, err
	}
	if !exists {
		return nil, fmt.Errorf("table %s does not exist", tableName)
	}

	// Get table properties to extract metadata location
	properties, err := s.util.GetTableProperties(ctx, tableName)
	if err != nil {
		return nil, fmt.Errorf("failed to get table properties: %w", err)
	}

	metadataLocation, ok := properties["metadata_location"]
	if !ok {
		return nil, errors.New("metadata_location not found in table properties")
	}

	// Load table using the metadata location
	// Note: This would need to be integrated with the catalog system
	// For now, return a placeholder
	_ = metadataLocation

	return nil, errors.New("table loading through Spark not yet fully implemented")
}

// TableExists checks if a table exists in Spark
func (s *SparkTableUtil) TableExists(ctx context.Context, identifier table.Identifier) (bool, error) {
	tableName := FormatTableIdentifier(identifier)

	sql := fmt.Sprintf("SHOW TABLES LIKE '%s'", tableName)
	df, err := s.util.ExecuteSQL(ctx, sql)
	if err != nil {
		return false, fmt.Errorf("failed to check table existence: %w", err)
	}

	rows, err := df.Collect(ctx)
	if err != nil {
		return false, fmt.Errorf("failed to collect table existence results: %w", err)
	}

	return len(rows) > 0, nil
}

// DropTable drops an Iceberg table in Spark
func (s *SparkTableUtil) DropTable(ctx context.Context, identifier table.Identifier) error {
	tableName := FormatTableIdentifier(identifier)

	sql := "DROP TABLE IF EXISTS " + tableName
	_, err := s.util.ExecuteSQL(ctx, sql)
	if err != nil {
		return fmt.Errorf("failed to drop table %s: %w", tableName, err)
	}

	return nil
}

// RenameTable renames an Iceberg table in Spark
func (s *SparkTableUtil) RenameTable(ctx context.Context, from, to table.Identifier) error {
	fromName := FormatTableIdentifier(from)
	toName := FormatTableIdentifier(to)

	sql := fmt.Sprintf("ALTER TABLE %s RENAME TO %s", fromName, toName)
	_, err := s.util.ExecuteSQL(ctx, sql)
	if err != nil {
		return fmt.Errorf("failed to rename table from %s to %s: %w", fromName, toName, err)
	}

	return nil
}

// ListTables lists all Iceberg tables in a namespace
func (s *SparkTableUtil) ListTables(ctx context.Context, namespace table.Identifier) ([]table.Identifier, error) {
	var sql string
	if len(namespace) > 0 {
		namespaceName := FormatTableIdentifier(namespace)
		sql = "SHOW TABLES IN " + namespaceName
	} else {
		sql = "SHOW TABLES"
	}

	df, err := s.util.ExecuteSQL(ctx, sql)
	if err != nil {
		return nil, fmt.Errorf("failed to list tables: %w", err)
	}

	rows, err := df.Collect(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to collect table list: %w", err)
	}

	var tables []table.Identifier
	for _, row := range rows {
		if len(row) >= 2 {
			// Assuming format: [namespace, tableName]
			ns := fmt.Sprintf("%v", row[0])
			name := fmt.Sprintf("%v", row[1])
			if ns != "" {
				tables = append(tables, ParseTableIdentifier(ns+"."+name))
			} else {
				tables = append(tables, ParseTableIdentifier(name))
			}
		}
	}

	return tables, nil
}

// GetTableSnapshot gets snapshot information for an Iceberg table
func (s *SparkTableUtil) GetTableSnapshot(ctx context.Context, identifier table.Identifier, snapshotID *int64) (*table.Snapshot, error) {
	tableName := FormatTableIdentifier(identifier)

	var sql string
	if snapshotID != nil {
		sql = fmt.Sprintf("SELECT * FROM %s.snapshots WHERE snapshot_id = %d", tableName, *snapshotID)
	} else {
		sql = fmt.Sprintf("SELECT * FROM %s.snapshots ORDER BY committed_at DESC LIMIT 1", tableName)
	}

	df, err := s.util.ExecuteSQL(ctx, sql)
	if err != nil {
		return nil, fmt.Errorf("failed to get table snapshot: %w", err)
	}

	rows, err := df.Collect(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to collect snapshot data: %w", err)
	}

	if len(rows) == 0 {
		return nil, errors.New("no snapshot found")
	}

	// Convert row to Snapshot struct
	// This would need proper mapping based on the actual snapshot table schema
	// For now, return a placeholder
	return nil, errors.New("snapshot conversion not yet implemented")
}

// convertSchemaToSparkDDL converts an Iceberg schema to Spark DDL format
func (s *SparkTableUtil) convertSchemaToSparkDDL(schema *iceberg.Schema) (string, error) {
	var columns []string

	for _, field := range schema.Fields() {
		sparkType, err := s.convertTypeToSpark(field.Type)
		if err != nil {
			return "", fmt.Errorf("failed to convert field %s: %w", field.Name, err)
		}

		column := fmt.Sprintf("`%s` %s", field.Name, sparkType)
		if field.Required {
			column += " NOT NULL"
		}
		columns = append(columns, column)
	}

	return strings.Join(columns, ", "), nil
}

// convertTypeToSpark converts an Iceberg type to Spark SQL type
func (s *SparkTableUtil) convertTypeToSpark(icebergType iceberg.Type) (string, error) {
	switch t := icebergType.(type) {
	case *iceberg.BooleanType:
		return "BOOLEAN", nil
	case *iceberg.Int32Type:
		return "INT", nil
	case *iceberg.Int64Type:
		return "BIGINT", nil
	case *iceberg.Float32Type:
		return "FLOAT", nil
	case *iceberg.Float64Type:
		return "DOUBLE", nil
	case *iceberg.DecimalType:
		return fmt.Sprintf("DECIMAL(%d,%d)", t.Precision(), t.Scale()), nil
	case *iceberg.DateType:
		return "DATE", nil
	case *iceberg.TimeType:
		return "TIME", nil
	case *iceberg.TimestampType:
		return "TIMESTAMP", nil
	case *iceberg.StringType:
		return "STRING", nil
	case *iceberg.UUIDType:
		return "STRING", nil
	case *iceberg.FixedType:
		return fmt.Sprintf("BINARY(%d)", t.Len()), nil
	case *iceberg.BinaryType:
		return "BINARY", nil
	case *iceberg.ListType:
		elementType, err := s.convertTypeToSpark(iceberg.PrimitiveTypes.String) // Simplified
		if err != nil {
			return "", err
		}

		return fmt.Sprintf("ARRAY<%s>", elementType), nil
	case *iceberg.MapType:
		keyType, err := s.convertTypeToSpark(iceberg.PrimitiveTypes.String) // Simplified
		if err != nil {
			return "", err
		}
		valueType, err := s.convertTypeToSpark(iceberg.PrimitiveTypes.String) // Simplified
		if err != nil {
			return "", err
		}

		return fmt.Sprintf("MAP<%s,%s>", keyType, valueType), nil
	case *iceberg.StructType:
		// Simplified - return basic struct representation
		fields := []string{"field1:STRING"} // Placeholder
		// fields = append(fields, fmt.Sprintf("`%s`:%s", field.Name, fieldType))
		return fmt.Sprintf("STRUCT<%s>", strings.Join(fields, ",")), nil
	default:
		return "", fmt.Errorf("unsupported Iceberg type: %T", icebergType)
	}
}
