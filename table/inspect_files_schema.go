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
	"github.com/apache/iceberg-go"
)

// DataFilesSchema returns the common content-file schema used by the files
// metadata tables. The partition field is omitted for an unpartitioned table,
// as required by the Iceberg metadata-table spec.
func DataFilesSchema(partitionType *iceberg.StructType) *iceberg.Schema {
	return iceberg.NewSchema(0, inspectContentFileFields(partitionType)...)
}

// FilesSchema returns the schema shared by the files metadata tables.
func FilesSchema(partitionType *iceberg.StructType) *iceberg.Schema {
	return DataFilesSchema(partitionType)
}

// AllFilesSchema returns the schema of the all_files metadata table.
func AllFilesSchema(partitionType *iceberg.StructType) *iceberg.Schema {
	return DataFilesSchema(partitionType)
}

// AllDataFilesSchema returns the schema of the all_data_files metadata table.
func AllDataFilesSchema(partitionType *iceberg.StructType) *iceberg.Schema {
	return DataFilesSchema(partitionType)
}

// AllDeleteFilesSchema returns the schema of the all_delete_files metadata table.
func AllDeleteFilesSchema(partitionType *iceberg.StructType) *iceberg.Schema {
	return DataFilesSchema(partitionType)
}

const (
	inspectContentFieldIDContent            = 134
	inspectContentFieldIDFilePath           = 100
	inspectContentFieldIDFileFormat         = 101
	inspectContentFieldIDSpecID             = 141
	inspectContentFieldIDPartition          = 102
	inspectContentFieldIDRecordCount        = 103
	inspectContentFieldIDFileSize           = 104
	inspectContentFieldIDColumnSizes        = 108
	inspectContentFieldIDValueCounts        = 109
	inspectContentFieldIDNullValueCounts    = 110
	inspectContentFieldIDNaNValueCounts     = 137
	inspectContentFieldIDLowerBounds        = 125
	inspectContentFieldIDUpperBounds        = 128
	inspectContentFieldIDKeyMetadata        = 131
	inspectContentFieldIDSplitOffsets       = 132
	inspectContentFieldIDEqualityIDs        = 135
	inspectContentFieldIDSortOrderID        = 140
	inspectContentFieldIDFirstRowID         = 142
	inspectContentFieldIDReferencedDataFile = 143
	inspectContentFieldIDContentOffset      = 144
	inspectContentFieldIDContentSize        = 145
)

func inspectContentFileFields(partitionType *iceberg.StructType) []iceberg.NestedField {
	fields := []iceberg.NestedField{
		{ID: inspectContentFieldIDContent, Name: "content", Type: iceberg.PrimitiveTypes.Int32, Required: false},
		{ID: inspectContentFieldIDFilePath, Name: "file_path", Type: iceberg.PrimitiveTypes.String, Required: true},
		{ID: inspectContentFieldIDFileFormat, Name: "file_format", Type: iceberg.PrimitiveTypes.String, Required: true},
		{ID: inspectContentFieldIDSpecID, Name: "spec_id", Type: iceberg.PrimitiveTypes.Int32, Required: false},
	}
	if partitionType != nil && len(partitionType.FieldList) > 0 {
		fields = append(fields, iceberg.NestedField{
			ID: inspectContentFieldIDPartition, Name: "partition", Type: partitionType, Required: true,
		})
	}
	fields = append(fields,
		iceberg.NestedField{ID: inspectContentFieldIDRecordCount, Name: "record_count", Type: iceberg.PrimitiveTypes.Int64, Required: true},
		iceberg.NestedField{ID: inspectContentFieldIDFileSize, Name: "file_size_in_bytes", Type: iceberg.PrimitiveTypes.Int64, Required: true},
		iceberg.NestedField{ID: inspectContentFieldIDColumnSizes, Name: "column_sizes", Type: inspectInt64MapType(117, 118), Required: false},
		iceberg.NestedField{ID: inspectContentFieldIDValueCounts, Name: "value_counts", Type: inspectInt64MapType(119, 120), Required: false},
		iceberg.NestedField{ID: inspectContentFieldIDNullValueCounts, Name: "null_value_counts", Type: inspectInt64MapType(121, 122), Required: false},
		iceberg.NestedField{ID: inspectContentFieldIDNaNValueCounts, Name: "nan_value_counts", Type: inspectInt64MapType(138, 139), Required: false},
		iceberg.NestedField{ID: inspectContentFieldIDLowerBounds, Name: "lower_bounds", Type: inspectBinaryMapType(126, 127), Required: false},
		iceberg.NestedField{ID: inspectContentFieldIDUpperBounds, Name: "upper_bounds", Type: inspectBinaryMapType(129, 130), Required: false},
		iceberg.NestedField{ID: inspectContentFieldIDKeyMetadata, Name: "key_metadata", Type: iceberg.PrimitiveTypes.Binary, Required: false},
		iceberg.NestedField{ID: inspectContentFieldIDSplitOffsets, Name: "split_offsets", Type: &iceberg.ListType{ElementID: 133, Element: iceberg.PrimitiveTypes.Int64, ElementRequired: true}, Required: false},
		iceberg.NestedField{ID: inspectContentFieldIDEqualityIDs, Name: "equality_ids", Type: &iceberg.ListType{ElementID: 136, Element: iceberg.PrimitiveTypes.Int32, ElementRequired: true}, Required: false},
		iceberg.NestedField{ID: inspectContentFieldIDSortOrderID, Name: "sort_order_id", Type: iceberg.PrimitiveTypes.Int32, Required: false},
		iceberg.NestedField{ID: inspectContentFieldIDFirstRowID, Name: "first_row_id", Type: iceberg.PrimitiveTypes.Int64, Required: false},
		iceberg.NestedField{ID: inspectContentFieldIDReferencedDataFile, Name: "referenced_data_file", Type: iceberg.PrimitiveTypes.String, Required: false},
		iceberg.NestedField{ID: inspectContentFieldIDContentOffset, Name: "content_offset", Type: iceberg.PrimitiveTypes.Int64, Required: false},
		iceberg.NestedField{ID: inspectContentFieldIDContentSize, Name: "content_size_in_bytes", Type: iceberg.PrimitiveTypes.Int64, Required: false},
	)

	return fields
}

func inspectInt64MapType(keyID, valueID int) *iceberg.MapType {
	return &iceberg.MapType{KeyID: keyID, KeyType: iceberg.PrimitiveTypes.Int32, ValueID: valueID, ValueType: iceberg.PrimitiveTypes.Int64, ValueRequired: true}
}

func inspectBinaryMapType(keyID, valueID int) *iceberg.MapType {
	return &iceberg.MapType{KeyID: keyID, KeyType: iceberg.PrimitiveTypes.Int32, ValueID: valueID, ValueType: iceberg.PrimitiveTypes.Binary, ValueRequired: true}
}
