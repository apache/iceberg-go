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

package internal

import (
	"encoding/json"
	"fmt"

	"github.com/twmb/avro"
)

// NullableSchema wraps schema in a nullable union ["null", schema].
func NullableSchema(schema *avro.Schema) *avro.Schema {
	return Must(avro.Parse(`["null",` + schema.String() + `]`))
}

var requiredLength = [...]int{
	1, 1, 1, 2, 2, 3, 3, 4, 4, 4, 5, 5,
	6, 6, 6, 7, 7, 8, 8, 9, 9, 9, 10, 10, 11, 11, 11, 12, 12,
	13, 13, 13, 14, 14, 15, 15, 16, 16, 16, 17,
}

// DecimalRequiredBytes returns the required number of bytes to store a
// decimal value of the given precision. If the precision is outside
// the range (0, 40], this returns -1 as it is invalid.
func DecimalRequiredBytes(precision int) int {
	if precision <= 0 || precision >= 40 {
		return -1
	}

	return requiredLength[precision]
}

// DecimalSchema returns an Avro fixed schema for a decimal with the given precision and scale.
func DecimalSchema(precision, scale int) *avro.Schema {
	return Must(avro.Parse(fmt.Sprintf(
		`{"type":"fixed","name":"fixed","size":%d,"logicalType":"decimal","precision":%d,"scale":%d}`,
		DecimalRequiredBytes(precision), precision, scale)))
}

var (
	NullSchema           = Must(avro.Parse(`"null"`))
	BoolSchema           = Must(avro.Parse(`"boolean"`))
	NullableBoolSchema   = NullableSchema(BoolSchema)
	BinarySchema         = Must(avro.Parse(`"bytes"`))
	NullableBinarySchema = NullableSchema(BinarySchema)
	StringSchema         = Must(avro.Parse(`"string"`))
	IntSchema            = Must(avro.Parse(`"int"`))
	NullableIntSchema    = NullableSchema(IntSchema)
	LongSchema           = Must(avro.Parse(`"long"`))
	NullableLongSchema   = NullableSchema(LongSchema)
	FloatSchema          = Must(avro.Parse(`"float"`))
	DoubleSchema         = Must(avro.Parse(`"double"`))
	DateSchema           = Must(avro.Parse(`{"type":"int","logicalType":"date"}`))
	TimeSchema           = Must(avro.Parse(`{"type":"long","logicalType":"time-micros"}`))
	TimestampSchema      = Must(avro.Parse(`{"type":"long","logicalType":"timestamp-micros","adjust-to-utc":false}`))
	TimestampTzSchema    = Must(avro.Parse(`{"type":"long","logicalType":"timestamp-micros","adjust-to-utc":true}`))
	UUIDSchema           = Must(avro.Parse(`{"type":"fixed","name":"uuid","size":16,"logicalType":"uuid"}`))
)

// avroSchemas stores pre-built Avro schemas by name for reuse in dynamic schema construction.
var avroSchemas = make(map[string]*avro.Schema)

const fieldSummaryJSON = `{
	"type": "record",
	"name": "r508",
	"field-id": 508,
	"fields": [
		{"name": "contains_null", "type": "boolean", "doc": "true if the field contains null values", "field-id": 509},
		{"name": "contains_nan", "type": ["null", "boolean"], "doc": "true if the field contains NaN values", "field-id": 518},
		{"name": "lower_bound", "type": ["null", "bytes"], "doc": "serialized lower bound", "field-id": 510},
		{"name": "upper_bound", "type": ["null", "bytes"], "doc": "serialized upper bound", "field-id": 511}
	]
}`

func init() {
	avroSchemas["field_summary"] = Must(avro.Parse(fieldSummaryJSON))

	avroSchemas["manifest_list_file_v1"] = Must(avro.Parse(`{
		"type": "record",
		"name": "manifest_file",
		"fields": [
			{"name": "manifest_path", "type": "string", "doc": "Location URI with FS scheme", "field-id": 500},
			{"name": "manifest_length", "type": "long", "doc": "Total file size in bytes", "field-id": 501},
			{"name": "partition_spec_id", "type": "int", "doc": "Spec ID used to write", "field-id": 502},
			{"name": "added_snapshot_id", "type": "long", "doc": "Snapshot ID that added the manifest", "field-id": 503},
			{"name": "added_files_count", "type": ["null", "int"], "doc": "Added entry count", "field-id": 504},
			{"name": "existing_files_count", "type": ["null", "int"], "doc": "Existing entry count", "field-id": 505},
			{"name": "deleted_files_count", "type": ["null", "int"], "doc": "Deleted entry count", "field-id": 506},
			{"name": "partitions", "type": ["null", {"type": "array", "items": {"type": "record", "name": "r508", "field-id": 508, "fields": [{"name": "contains_null", "type": "boolean", "doc": "true if the field contains null values", "field-id": 509}, {"name": "contains_nan", "type": ["null", "boolean"], "doc": "true if the field contains NaN values", "field-id": 518}, {"name": "lower_bound", "type": ["null", "bytes"], "doc": "serialized lower bound", "field-id": 510}, {"name": "upper_bound", "type": ["null", "bytes"], "doc": "serialized upper bound", "field-id": 511}]}, "element-id": 508}], "doc": "Partition field summaries", "field-id": 507},
			{"name": "added_rows_count", "type": ["null", "long"], "doc": "Added row count", "field-id": 512},
			{"name": "existing_rows_count", "type": ["null", "long"], "doc": "Existing row count", "field-id": 513},
			{"name": "deleted_rows_count", "type": ["null", "long"], "doc": "Deleted row count", "field-id": 514},
			{"name": "key_metadata", "type": ["null", "bytes"], "doc": "Key metadata", "field-id": 519}
		]
	}`))

	avroSchemas["manifest_list_file_v2"] = Must(avro.Parse(`{
		"type": "record",
		"name": "manifest_file",
		"fields": [
			{"name": "manifest_path", "type": "string", "doc": "Location URI with FS scheme", "field-id": 500},
			{"name": "manifest_length", "type": "long", "doc": "Total file size in bytes", "field-id": 501},
			{"name": "partition_spec_id", "type": "int", "doc": "Spec ID used to write", "field-id": 502},
			{"name": "content", "type": "int", "doc": "Content type", "default": 0, "field-id": 517},
			{"name": "sequence_number", "type": "long", "doc": "Sequence number", "default": 0, "field-id": 515},
			{"name": "min_sequence_number", "type": "long", "doc": "Minimum sequence number", "default": 0, "field-id": 516},
			{"name": "added_snapshot_id", "type": "long", "doc": "Snapshot ID that added the manifest", "field-id": 503},
			{"name": "added_files_count", "type": "int", "doc": "Added entry count", "field-id": 504},
			{"name": "existing_files_count", "type": "int", "doc": "Existing entry count", "field-id": 505},
			{"name": "deleted_files_count", "type": "int", "doc": "Deleted entry count", "field-id": 506},
			{"name": "partitions", "type": ["null", {"type": "array", "items": {"type": "record", "name": "r508", "field-id": 508, "fields": [{"name": "contains_null", "type": "boolean", "doc": "true if the field contains null values", "field-id": 509}, {"name": "contains_nan", "type": ["null", "boolean"], "doc": "true if the field contains NaN values", "field-id": 518}, {"name": "lower_bound", "type": ["null", "bytes"], "doc": "serialized lower bound", "field-id": 510}, {"name": "upper_bound", "type": ["null", "bytes"], "doc": "serialized upper bound", "field-id": 511}]}, "element-id": 508}], "doc": "Partition field summaries", "field-id": 507},
			{"name": "added_rows_count", "type": "long", "doc": "Added row count", "field-id": 512},
			{"name": "existing_rows_count", "type": "long", "doc": "Existing row count", "field-id": 513},
			{"name": "deleted_rows_count", "type": "long", "doc": "Deleted row count", "field-id": 514},
			{"name": "key_metadata", "type": ["null", "bytes"], "doc": "Key metadata", "field-id": 519}
		]
	}`))

	avroSchemas["data_file_v1"] = Must(avro.Parse(`{
		"type": "record",
		"name": "r2",
		"fields": [
			{"name": "file_path", "type": "string", "doc": "Location URI with FS scheme", "field-id": 100},
			{"name": "file_format", "type": "string", "doc": "File format name: avro, orc, parquet", "field-id": 101},
			{"name": "record_count", "type": "long", "doc": "Number of records in the file", "field-id": 103},
			{"name": "file_size_in_bytes", "type": "long", "doc": "Size of the file in bytes", "field-id": 104},
			{"name": "block_size_in_bytes", "type": "long", "doc": "Deprecated. Always write default in v1. Do not write in v2.", "default": 67108864, "field-id": 105},
			{"name": "column_sizes", "type": ["null", {"type": "array", "items": {"type": "record", "name": "k117_v118", "fields": [{"name": "key", "type": "int", "field-id": 117}, {"name": "value", "type": "long", "field-id": 118}]}, "logicalType": "map"}], "doc": "map of column id to total size on disk", "field-id": 108},
			{"name": "value_counts", "type": ["null", {"type": "array", "items": {"type": "record", "name": "k119_v120", "fields": [{"name": "key", "type": "int", "field-id": 119}, {"name": "value", "type": "long", "field-id": 120}]}, "logicalType": "map"}], "doc": "map of value to count", "field-id": 109},
			{"name": "null_value_counts", "type": ["null", {"type": "array", "items": {"type": "record", "name": "k121_v122", "fields": [{"name": "key", "type": "int", "field-id": 121}, {"name": "value", "type": "long", "field-id": 122}]}, "logicalType": "map"}], "doc": "map of value to count", "field-id": 110},
			{"name": "nan_value_counts", "type": ["null", {"type": "array", "items": {"type": "record", "name": "k138_v139", "fields": [{"name": "key", "type": "int", "field-id": 138}, {"name": "value", "type": "long", "field-id": 139}]}, "logicalType": "map"}], "doc": "map of value to count", "field-id": 137},
			{"name": "lower_bounds", "type": ["null", {"type": "array", "items": {"type": "record", "name": "k126_v127", "fields": [{"name": "key", "type": "int", "field-id": 126}, {"name": "value", "type": "bytes", "field-id": 127}]}, "logicalType": "map"}], "doc": "map of column id to lower bound", "field-id": 125},
			{"name": "upper_bounds", "type": ["null", {"type": "array", "items": {"type": "record", "name": "k129_v130", "fields": [{"name": "key", "type": "int", "field-id": 129}, {"name": "value", "type": "bytes", "field-id": 130}]}, "logicalType": "map"}], "doc": "map of column id to upper bound", "field-id": 128},
			{"name": "key_metadata", "type": ["null", "bytes"], "doc": "Encryption Key Metadata Blob", "field-id": 131},
			{"name": "split_offsets", "type": ["null", {"type": "array", "items": "long", "element-id": 133}], "doc": "splitable offsets", "field-id": 132},
			{"name": "sort_order_id", "type": ["null", "int"], "doc": "Sort order ID", "field-id": 140}
		]
	}`))

	avroSchemas["data_file_v2"] = Must(avro.Parse(`{
		"type": "record",
		"name": "r2",
		"fields": [
			{"name": "content", "type": "int", "doc": "Content type", "default": 0, "field-id": 134},
			{"name": "file_path", "type": "string", "doc": "Location URI with FS scheme", "field-id": 100},
			{"name": "file_format", "type": "string", "doc": "File format name: avro, orc, parquet", "field-id": 101},
			{"name": "record_count", "type": "long", "doc": "Number of records in the file", "field-id": 103},
			{"name": "file_size_in_bytes", "type": "long", "doc": "Size of the file in bytes", "field-id": 104},
			{"name": "column_sizes", "type": ["null", {"type": "array", "items": {"type": "record", "name": "k117_v118", "fields": [{"name": "key", "type": "int", "field-id": 117}, {"name": "value", "type": "long", "field-id": 118}]}, "logicalType": "map"}], "doc": "map of column id to total size on disk", "field-id": 108},
			{"name": "value_counts", "type": ["null", {"type": "array", "items": {"type": "record", "name": "k119_v120", "fields": [{"name": "key", "type": "int", "field-id": 119}, {"name": "value", "type": "long", "field-id": 120}]}, "logicalType": "map"}], "doc": "map of value to count", "field-id": 109},
			{"name": "null_value_counts", "type": ["null", {"type": "array", "items": {"type": "record", "name": "k121_v122", "fields": [{"name": "key", "type": "int", "field-id": 121}, {"name": "value", "type": "long", "field-id": 122}]}, "logicalType": "map"}], "doc": "map of value to count", "field-id": 110},
			{"name": "nan_value_counts", "type": ["null", {"type": "array", "items": {"type": "record", "name": "k138_v139", "fields": [{"name": "key", "type": "int", "field-id": 138}, {"name": "value", "type": "long", "field-id": 139}]}, "logicalType": "map"}], "doc": "map of value to count", "field-id": 137},
			{"name": "lower_bounds", "type": ["null", {"type": "array", "items": {"type": "record", "name": "k126_v127", "fields": [{"name": "key", "type": "int", "field-id": 126}, {"name": "value", "type": "bytes", "field-id": 127}]}, "logicalType": "map"}], "doc": "map of column id to lower bound", "field-id": 125},
			{"name": "upper_bounds", "type": ["null", {"type": "array", "items": {"type": "record", "name": "k129_v130", "fields": [{"name": "key", "type": "int", "field-id": 129}, {"name": "value", "type": "bytes", "field-id": 130}]}, "logicalType": "map"}], "doc": "map of column id to upper bound", "field-id": 128},
			{"name": "key_metadata", "type": ["null", "bytes"], "doc": "Encryption Key Metadata Blob", "field-id": 131},
			{"name": "split_offsets", "type": ["null", {"type": "array", "items": "long", "element-id": 133}], "doc": "splitable offsets", "field-id": 132},
			{"name": "equality_ids", "type": ["null", {"type": "array", "items": "long", "element-id": 136}], "doc": "field ids used to determine row equality in equality delete files", "field-id": 135},
			{"name": "sort_order_id", "type": ["null", "int"], "doc": "Sort order ID", "field-id": 140}
		]
	}`))

	avroSchemas["manifest_entry_v1"] = Must(avro.Parse(`{
		"type": "record",
		"name": "manifest_entry",
		"fields": [
			{"name": "status", "type": "int", "field-id": 0},
			{"name": "snapshot_id", "type": "long", "field-id": 1}
		]
	}`))

	avroSchemas["manifest_entry_v2"] = Must(avro.Parse(`{
		"type": "record",
		"name": "manifest_entry",
		"fields": [
			{"name": "status", "type": "int", "field-id": 0},
			{"name": "snapshot_id", "type": ["null", "long"], "field-id": 1},
			{"name": "sequence_number", "type": ["null", "long"], "field-id": 3},
			{"name": "file_sequence_number", "type": ["null", "long"], "field-id": 4}
		]
	}`))

	avroSchemas["manifest_list_file_v3"] = Must(avro.Parse(`{
		"type": "record",
		"name": "manifest_file",
		"fields": [
			{"name": "manifest_path", "type": "string", "doc": "Location URI with FS scheme", "field-id": 500},
			{"name": "manifest_length", "type": "long", "doc": "Total file size in bytes", "field-id": 501},
			{"name": "partition_spec_id", "type": "int", "doc": "Spec ID used to write", "field-id": 502},
			{"name": "content", "type": "int", "doc": "Content type", "default": 0, "field-id": 517},
			{"name": "sequence_number", "type": "long", "doc": "Sequence number", "default": 0, "field-id": 515},
			{"name": "min_sequence_number", "type": "long", "doc": "Minimum sequence number", "default": 0, "field-id": 516},
			{"name": "added_snapshot_id", "type": "long", "doc": "Snapshot ID that added the manifest", "field-id": 503},
			{"name": "added_files_count", "type": "int", "doc": "Added entry count", "field-id": 504},
			{"name": "existing_files_count", "type": "int", "doc": "Existing entry count", "field-id": 505},
			{"name": "deleted_files_count", "type": "int", "doc": "Deleted entry count", "field-id": 506},
			{"name": "partitions", "type": ["null", {"type": "array", "items": {"type": "record", "name": "r508", "field-id": 508, "fields": [{"name": "contains_null", "type": "boolean", "doc": "true if the field contains null values", "field-id": 509}, {"name": "contains_nan", "type": ["null", "boolean"], "doc": "true if the field contains NaN values", "field-id": 518}, {"name": "lower_bound", "type": ["null", "bytes"], "doc": "serialized lower bound", "field-id": 510}, {"name": "upper_bound", "type": ["null", "bytes"], "doc": "serialized upper bound", "field-id": 511}]}, "element-id": 508}], "doc": "Partition field summaries", "field-id": 507},
			{"name": "added_rows_count", "type": "long", "doc": "Added row count", "field-id": 512},
			{"name": "existing_rows_count", "type": "long", "doc": "Existing row count", "field-id": 513},
			{"name": "deleted_rows_count", "type": "long", "doc": "Deleted row count", "field-id": 514},
			{"name": "key_metadata", "type": ["null", "bytes"], "doc": "Key metadata", "field-id": 519},
			{"name": "first_row_id", "type": ["null", "long"], "doc": "First row ID", "field-id": 520}
		]
	}`))

	avroSchemas["data_file_v3"] = Must(avro.Parse(`{
		"type": "record",
		"name": "r2",
		"fields": [
			{"name": "content", "type": "int", "doc": "Content type", "default": 0, "field-id": 134},
			{"name": "file_path", "type": "string", "doc": "Location URI with FS scheme", "field-id": 100},
			{"name": "file_format", "type": "string", "doc": "File format name: avro, orc, parquet", "field-id": 101},
			{"name": "record_count", "type": "long", "doc": "Number of records in the file", "field-id": 103},
			{"name": "file_size_in_bytes", "type": "long", "doc": "Size of the file in bytes", "field-id": 104},
			{"name": "column_sizes", "type": ["null", {"type": "array", "items": {"type": "record", "name": "k117_v118", "fields": [{"name": "key", "type": "int", "field-id": 117}, {"name": "value", "type": "long", "field-id": 118}]}, "logicalType": "map"}], "doc": "map of column id to total size on disk", "field-id": 108},
			{"name": "value_counts", "type": ["null", {"type": "array", "items": {"type": "record", "name": "k119_v120", "fields": [{"name": "key", "type": "int", "field-id": 119}, {"name": "value", "type": "long", "field-id": 120}]}, "logicalType": "map"}], "doc": "map of value to count", "field-id": 109},
			{"name": "null_value_counts", "type": ["null", {"type": "array", "items": {"type": "record", "name": "k121_v122", "fields": [{"name": "key", "type": "int", "field-id": 121}, {"name": "value", "type": "long", "field-id": 122}]}, "logicalType": "map"}], "doc": "map of value to count", "field-id": 110},
			{"name": "nan_value_counts", "type": ["null", {"type": "array", "items": {"type": "record", "name": "k138_v139", "fields": [{"name": "key", "type": "int", "field-id": 138}, {"name": "value", "type": "long", "field-id": 139}]}, "logicalType": "map"}], "doc": "map of value to count", "field-id": 137},
			{"name": "lower_bounds", "type": ["null", {"type": "array", "items": {"type": "record", "name": "k126_v127", "fields": [{"name": "key", "type": "int", "field-id": 126}, {"name": "value", "type": "bytes", "field-id": 127}]}, "logicalType": "map"}], "doc": "map of column id to lower bound", "field-id": 125},
			{"name": "upper_bounds", "type": ["null", {"type": "array", "items": {"type": "record", "name": "k129_v130", "fields": [{"name": "key", "type": "int", "field-id": 129}, {"name": "value", "type": "bytes", "field-id": 130}]}, "logicalType": "map"}], "doc": "map of column id to upper bound", "field-id": 128},
			{"name": "key_metadata", "type": ["null", "bytes"], "doc": "Encryption Key Metadata Blob", "field-id": 131},
			{"name": "split_offsets", "type": ["null", {"type": "array", "items": "long", "element-id": 133}], "doc": "splitable offsets", "field-id": 132},
			{"name": "equality_ids", "type": ["null", {"type": "array", "items": "long", "element-id": 136}], "doc": "field ids used to determine row equality in equality delete files", "field-id": 135},
			{"name": "sort_order_id", "type": ["null", "int"], "doc": "Sort order ID", "field-id": 140},
			{"name": "first_row_id", "type": ["null", "long"], "doc": "The _row_id for the first row in the data file", "field-id": 142},
			{"name": "referenced_data_file", "type": ["null", "string"], "doc": "Fully qualified location of a data file that all deletes reference", "field-id": 143},
			{"name": "content_offset", "type": ["null", "long"], "doc": "The offset in the file where the content starts", "field-id": 144},
			{"name": "content_size_in_bytes", "type": ["null", "long"], "doc": "The length of the referenced content stored in the file", "field-id": 145}
		]
	}`))

	avroSchemas["manifest_entry_v3"] = Must(avro.Parse(`{
		"type": "record",
		"name": "manifest_entry",
		"fields": [
			{"name": "status", "type": "int", "field-id": 0},
			{"name": "snapshot_id", "type": ["null", "long"], "field-id": 1},
			{"name": "sequence_number", "type": ["null", "long"], "field-id": 3},
			{"name": "file_sequence_number", "type": ["null", "long"], "field-id": 4}
		]
	}`))
}

// newDataFileSchema builds the full data file Avro schema by inserting a partition field
// into the base data file schema for the given spec version.
func newDataFileSchema(partitionType *avro.Schema, version int) (*avro.Schema, error) {
	key := fmt.Sprintf("data_file_v%d", version)
	baseSchema, ok := avroSchemas[key]
	if !ok {
		return nil, fmt.Errorf("no data file schema for version %d", version)
	}

	// Unmarshal the base schema JSON and inject the partition field.
	var base map[string]any
	if err := json.Unmarshal([]byte(baseSchema.String()), &base); err != nil {
		return nil, fmt.Errorf("failed to unmarshal base data file schema: %w", err)
	}

	var partType any
	if err := json.Unmarshal([]byte(partitionType.String()), &partType); err != nil {
		return nil, fmt.Errorf("failed to unmarshal partition type schema: %w", err)
	}

	partitionField := map[string]any{
		"name":     "partition",
		"type":     partType,
		"field-id": 102,
	}

	fields := base["fields"].([]any)
	// Insert partition field after file_format (index 1 in v1, index 2 in v2/v3).
	insertAt := 2
	if version == 1 {
		insertAt = 2
	}
	newFields := make([]any, 0, len(fields)+1)
	newFields = append(newFields, fields[:insertAt]...)
	newFields = append(newFields, partitionField)
	newFields = append(newFields, fields[insertAt:]...)
	base["fields"] = newFields

	newJSON, err := json.Marshal(base)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal data file schema with partition: %w", err)
	}

	// Use a fresh cache so named types from the base schema are re-parsed cleanly.
	var cache avro.SchemaCache

	return cache.Parse(string(newJSON))
}

// NewManifestFileSchema returns the Avro schema for a manifest list file entry
// for the given Iceberg spec version (1, 2, or 3).
func NewManifestFileSchema(version int) (*avro.Schema, error) {
	switch version {
	case 1, 2, 3:
	default:
		return nil, fmt.Errorf("unsupported iceberg spec version: %d", version)
	}

	key := fmt.Sprintf("manifest_list_file_v%d", version)

	return avroSchemas[key], nil
}

// NewManifestEntrySchema returns the full Avro schema for a manifest entry,
// including the data file sub-schema with the given partition type.
func NewManifestEntrySchema(partitionType *avro.Schema, version int) (*avro.Schema, error) {
	switch version {
	case 1, 2, 3:
	default:
		return nil, fmt.Errorf("unsupported iceberg spec version: %d", version)
	}

	dfSchema, err := newDataFileSchema(partitionType, version)
	if err != nil {
		return nil, err
	}

	// Unmarshal the base manifest entry schema and append the data_file field.
	key := fmt.Sprintf("manifest_entry_v%d", version)
	baseSchema := avroSchemas[key]

	var base map[string]any
	if err := json.Unmarshal([]byte(baseSchema.String()), &base); err != nil {
		return nil, fmt.Errorf("failed to unmarshal manifest entry schema: %w", err)
	}

	var dfType any
	if err := json.Unmarshal([]byte(dfSchema.String()), &dfType); err != nil {
		return nil, fmt.Errorf("failed to unmarshal data file schema: %w", err)
	}

	dfField := map[string]any{
		"name":     "data_file",
		"type":     dfType,
		"field-id": 2,
	}

	base["fields"] = append(base["fields"].([]any), dfField)

	newJSON, err := json.Marshal(base)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal manifest entry schema: %w", err)
	}

	// Use a fresh cache so named types are re-parsed cleanly.
	var cache avro.SchemaCache

	return cache.Parse(string(newJSON))
}
