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
	"maps"
	"strings"

	"github.com/apache/iceberg-go"
	"github.com/apache/iceberg-go/table"
	"github.com/beltran/gohive/hive_metastore"
)

func schemaToHiveColumns(schema *iceberg.Schema) []*hive_metastore.FieldSchema {
	if schema == nil {
		return nil
	}

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

// Generic Hive SerDe and input/output format used for Iceberg views (not Iceberg table storage handler).
// See HiveOperationsBase.storageDescriptor(..., false) in Java.
const (
	hiveViewInputFormat  = "org.apache.hadoop.mapred.FileInputFormat"
	hiveViewOutputFormat = "org.apache.hadoop.mapred.FileOutputFormat"
	hiveViewSerDe        = "org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe"
)

// constructHiveViewTable builds an HMS Table for an Iceberg view. The table type is VIRTUAL_VIEW,
// parameters include table_type=ICEBERG_VIEW and metadata_location; StorageDescriptor uses
// generic Hive SerDe (not the Iceberg storage handler). ViewOriginalText and ViewExpandedText
// are set to viewSQL.
func constructHiveViewTable(dbName, viewName, location, metadataLocation string, schema *iceberg.Schema, viewSQL string, props map[string]string) *hive_metastore.Table {
	parameters := make(map[string]string)
	for k, v := range props {
		if v != "" {
			parameters[k] = v
		}
	}
	delete(parameters, PreviousMetadataLocationKey)

	parameters[TableTypeKey] = TableTypeIcebergView
	parameters[MetadataLocationKey] = metadataLocation
	parameters[ExternalKey] = "TRUE"

	// Ref: https://github.com/apache/iceberg/blob/11dbe2f091edd4ac492f210c878d22386ec9d605/hive-metastore/src/main/java/org/apache/iceberg/hive/HiveOperationsBase.java#L174-L178
	tbl := &hive_metastore.Table{
		TableName:        viewName,
		DbName:           dbName,
		TableType:        TableTypeVirtualView,
		ViewOriginalText: viewSQL,
		ViewExpandedText: viewSQL,
		Parameters:       parameters,
		Sd: &hive_metastore.StorageDescriptor{
			Cols:         schemaToHiveColumns(schema),
			Location:     location,
			InputFormat:  hiveViewInputFormat,
			OutputFormat: hiveViewOutputFormat,
			SerdeInfo: &hive_metastore.SerDeInfo{
				SerializationLib: hiveViewSerDe,
			},
		},
	}

	return tbl
}

func constructHiveTable(dbName, tableName, location, metadataLocation string, schema *iceberg.Schema, props map[string]string) *hive_metastore.Table {
	parameters := make(map[string]string)

	for k, v := range props {
		parameters[k] = v
	}
	setTranslatedIcebergProperties(parameters, props)
	delete(parameters, PreviousMetadataLocationKey)

	parameters[TableTypeKey] = TableTypeIceberg
	parameters[MetadataLocationKey] = metadataLocation
	parameters[ExternalKey] = "TRUE"
	parameters[StorageHandlerKey] = IcebergStorageHandler

	return &hive_metastore.Table{
		TableName:  tableName,
		DbName:     dbName,
		TableType:  TableTypeExternalTable,
		Sd:         buildIcebergStorageDescriptor(location, schema),
		Parameters: parameters,
	}
}

func buildIcebergStorageDescriptor(location string, schema *iceberg.Schema) *hive_metastore.StorageDescriptor {
	return &hive_metastore.StorageDescriptor{
		Cols:         schemaToHiveColumns(schema),
		Location:     location,
		InputFormat:  "org.apache.iceberg.mr.hive.HiveIcebergInputFormat",
		OutputFormat: "org.apache.iceberg.mr.hive.HiveIcebergOutputFormat",
		SerdeInfo: &hive_metastore.SerDeInfo{
			SerializationLib: "org.apache.iceberg.mr.hive.HiveIcebergSerDe",
		},
	}
}

func cloneStorageDescriptor(existing *hive_metastore.StorageDescriptor) *hive_metastore.StorageDescriptor {
	cloned := *existing
	cloned.Parameters = maps.Clone(existing.Parameters)
	cloned.BucketCols = append([]string(nil), existing.BucketCols...)
	cloned.SortCols = append([]*hive_metastore.Order(nil), existing.SortCols...)
	if existing.SerdeInfo != nil {
		serdeInfo := *existing.SerdeInfo
		serdeInfo.Parameters = maps.Clone(existing.SerdeInfo.Parameters)
		cloned.SerdeInfo = &serdeInfo
	}

	return &cloned
}

func setTranslatedIcebergProperties(parameters map[string]string, props iceberg.Properties) {
	if value, ok := props[GCEnabledKey]; ok {
		parameters[ExternalTablePurgeKey] = value
	}
}

func updateHiveTableForCommit(
	existing *hive_metastore.Table,
	current, staged table.Metadata,
	newMetadataLocation string,
) *hive_metastore.Table {
	updated := *existing
	updated.Parameters = maps.Clone(existing.Parameters)
	if updated.Parameters == nil {
		updated.Parameters = make(map[string]string)
	}

	// HMS has no ownership marker for user properties. As in the Java implementation,
	// obsolete keys are assumed to have been written from the previous Iceberg metadata.
	for key := range current.Properties() {
		delete(updated.Parameters, key)
	}
	if _, ok := current.Properties()[GCEnabledKey]; ok {
		delete(updated.Parameters, ExternalTablePurgeKey)
	}
	for key, value := range staged.Properties() {
		updated.Parameters[key] = value
	}
	setTranslatedIcebergProperties(updated.Parameters, staged.Properties())
	delete(updated.Parameters, PreviousMetadataLocationKey)

	// Read the unmodified parameters so a property cannot replace the real previous pointer.
	if oldLocation, ok := existing.Parameters[MetadataLocationKey]; ok {
		updated.Parameters[PreviousMetadataLocationKey] = oldLocation
	}

	updated.Parameters[TableTypeKey] = TableTypeIceberg
	updated.Parameters[MetadataLocationKey] = newMetadataLocation
	updated.Parameters[ExternalKey] = "TRUE"
	updated.Parameters[StorageHandlerKey] = IcebergStorageHandler

	if existing.Sd == nil {
		updated.Sd = buildIcebergStorageDescriptor(staged.Location(), staged.CurrentSchema())
	} else {
		updated.Sd = cloneStorageDescriptor(existing.Sd)
		updated.Sd.Cols = schemaToHiveColumns(staged.CurrentSchema())
		updated.Sd.Location = staged.Location()
	}

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

// isIcebergView checks if a Hive table is an Iceberg view.
func isIcebergView(tbl *hive_metastore.Table) bool {
	if tbl == nil || tbl.Parameters == nil {
		return false
	}

	// Ensure the HMS table type is a virtual view
	if !strings.EqualFold(tbl.TableType, TableTypeVirtualView) {
		return false
	}

	tableType, ok := tbl.Parameters[TableTypeKey]
	if !ok {
		return false
	}

	return strings.EqualFold(tableType, TableTypeIcebergView)
}

// getViewMetadataLocation returns the metadata location for an Iceberg view.
// Views use the same metadata_location parameter as tables.
func getViewMetadataLocation(tbl *hive_metastore.Table) (string, error) {
	return getMetadataLocation(tbl)
}
