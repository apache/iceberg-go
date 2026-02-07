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

package hive

import (
	"errors"
	"fmt"
	"strings"

	"github.com/apache/iceberg-go"
	"github.com/beltran/gohive/hive_metastore"
)

func schemaToHiveColumns(schema *iceberg.Schema) []*hive_metastore.FieldSchema {
	columns := make([]*hive_metastore.FieldSchema, 0, len(schema.Fields()))
	for _, field := range schema.Fields() {
		columns = append(columns, fieldToHiveColumn(field))
	}

	return columns
}

func fieldToHiveColumn(field iceberg.NestedField) *hive_metastore.FieldSchema {
	return &hive_metastore.FieldSchema{
		Name:    field.Name,
		Type:    icebergTypeToHiveType(field.Type),
		Comment: field.Doc,
	}
}

// icebergTypeToHiveType converts an Iceberg type to a Hive type string.
// Reference: https://cwiki.apache.org/confluence/display/hive/languagemanual+types
func icebergTypeToHiveType(typ iceberg.Type) string {
	switch t := typ.(type) {
	case iceberg.Int64Type:
		return "bigint"
	case iceberg.TimeType:
		// Hive doesn't have a native time type, use string
		return "string"
	case iceberg.TimestampTzType:
		return "timestamp"
	case iceberg.UUIDType:
		// Represent UUID as string
		return "string"
	case iceberg.DecimalType:
		return fmt.Sprintf("decimal(%d,%d)", t.Precision(), t.Scale())
	case iceberg.FixedType:
		return fmt.Sprintf("binary(%d)", t.Len())
	case *iceberg.StructType:
		var fieldStrings []string
		for _, field := range t.Fields() {
			fieldStrings = append(fieldStrings,
				fmt.Sprintf("%s:%s", field.Name, icebergTypeToHiveType(field.Type)))
		}
		return fmt.Sprintf("struct<%s>", strings.Join(fieldStrings, ","))
	case *iceberg.ListType:
		return fmt.Sprintf("array<%s>", icebergTypeToHiveType(t.ElementField().Type))
	case *iceberg.MapType:
		keyField := t.KeyField()
		valueField := t.ValueField()
		return fmt.Sprintf("map<%s,%s>",
			icebergTypeToHiveType(keyField.Type),
			icebergTypeToHiveType(valueField.Type))
	default:
		return typ.String()
	}
}

func constructHiveTable(dbName, tableName, location, metadataLocation string, schema *iceberg.Schema, props map[string]string) *hive_metastore.Table {
	parameters := make(map[string]string)

	// Set Iceberg-specific parameters
	parameters[TableTypeKey] = TableTypeIceberg
	parameters[MetadataLocationKey] = metadataLocation
	parameters[ExternalKey] = "TRUE"

	// Set storage handler - required for Hive to query Iceberg tables
	parameters["storage_handler"] = "org.apache.iceberg.mr.hive.HiveIcebergStorageHandler"

	// Copy additional properties
	for k, v := range props {
		parameters[k] = v
	}

	return &hive_metastore.Table{
		TableName: tableName,
		DbName:    dbName,
		TableType: TableTypeExternalTable,
		Sd: &hive_metastore.StorageDescriptor{
			Cols:         schemaToHiveColumns(schema),
			Location:     location,
			InputFormat:  "org.apache.iceberg.mr.hive.HiveIcebergInputFormat",
			OutputFormat: "org.apache.iceberg.mr.hive.HiveIcebergOutputFormat",
			SerdeInfo: &hive_metastore.SerDeInfo{
				SerializationLib: "org.apache.iceberg.mr.hive.HiveIcebergSerDe",
			},
		},
		Parameters: parameters,
	}
}

func updateHiveTableForCommit(existing *hive_metastore.Table, newMetadataLocation string) *hive_metastore.Table {
	// Copy the existing table
	updated := *existing

	// Update parameters
	if updated.Parameters == nil {
		updated.Parameters = make(map[string]string)
	}

	// Store previous metadata location
	if oldLocation, ok := updated.Parameters[MetadataLocationKey]; ok {
		updated.Parameters[PreviousMetadataLocationKey] = oldLocation
	}

	// Set new metadata location
	updated.Parameters[MetadataLocationKey] = newMetadataLocation

	return &updated
}

// isIcebergTable checks if a Hive table is an Iceberg table.
func isIcebergTable(tbl *hive_metastore.Table) bool {
	if tbl == nil || tbl.Parameters == nil {
		return false
	}

	tableType, ok := tbl.Parameters[TableTypeKey]
	if !ok {
		return false
	}

	return strings.EqualFold(tableType, TableTypeIceberg)
}

func getMetadataLocation(tbl *hive_metastore.Table) (string, error) {
	if tbl == nil || tbl.Parameters == nil {
		return "", errors.New("table has no parameters")
	}

	location, ok := tbl.Parameters[MetadataLocationKey]
	if !ok {
		return "", fmt.Errorf("table does not have %s parameter", MetadataLocationKey)
	}

	return location, nil
}
