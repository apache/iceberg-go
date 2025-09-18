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
	"fmt"

	"github.com/hamba/avro/v2"
)

func NullableSchema(schema avro.Schema) avro.Schema {
	return Must(avro.NewUnionSchema([]avro.Schema{
		NullSchema, schema,
	}))
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

func DecimalSchema(precision, scale int) avro.Schema {
	return Must(avro.NewFixedSchema("fixed", "",
		DecimalRequiredBytes(precision), avro.NewDecimalLogicalSchema(precision, scale)))
}

var (
	NullSchema           = avro.NewNullSchema()
	BoolSchema           = avro.NewPrimitiveSchema(avro.Boolean, nil)
	NullableBoolSchema   = NullableSchema(BoolSchema)
	BinarySchema         = avro.NewPrimitiveSchema(avro.Bytes, nil)
	NullableBinarySchema = NullableSchema(BinarySchema)
	StringSchema         = avro.NewPrimitiveSchema(avro.String, nil)
	IntSchema            = avro.NewPrimitiveSchema(avro.Int, nil)
	NullableIntSchema    = NullableSchema(IntSchema)
	LongSchema           = avro.NewPrimitiveSchema(avro.Long, nil)
	NullableLongSchema   = NullableSchema(LongSchema)
	FloatSchema          = avro.NewPrimitiveSchema(avro.Float, nil)
	DoubleSchema         = avro.NewPrimitiveSchema(avro.Double, nil)
	DateSchema           = avro.NewPrimitiveSchema(avro.Int, avro.NewPrimitiveLogicalSchema(avro.Date))
	TimeSchema           = avro.NewPrimitiveSchema(avro.Long, avro.NewPrimitiveLogicalSchema(avro.TimeMicros))
	TimestampSchema      = avro.NewPrimitiveSchema(avro.Long, avro.NewPrimitiveLogicalSchema(avro.TimestampMicros),
		avro.WithProps(map[string]any{"adjust-to-utc": false}))
	TimestampTzSchema = avro.NewPrimitiveSchema(avro.Long, avro.NewPrimitiveLogicalSchema(avro.TimestampMicros),
		avro.WithProps(map[string]any{"adjust-to-utc": true}))
	UUIDSchema = Must(avro.NewFixedSchema("uuid", "", 16, avro.NewPrimitiveLogicalSchema(avro.UUID)))

	AvroSchemaCache avro.SchemaCache
)

func newMapSchema(name string, keySchema, valueSchema avro.Schema, keyFieldID, valueFieldID int) avro.Schema {
	return avro.NewArraySchema(
		Must(avro.NewRecordSchema(name, "", []*avro.Field{
			Must(avro.NewField("key", keySchema, WithFieldID(keyFieldID))),
			Must(avro.NewField("value", valueSchema, WithFieldID(valueFieldID))),
		})), avro.WithProps(map[string]any{"logicalType": "map"}))
}

func WithFieldID(id int) avro.SchemaOption {
	return avro.WithProps(map[string]any{"field-id": id})
}

func WithElementID(id int) avro.SchemaOption {
	return avro.WithProps(map[string]any{"element-id": id})
}

func init() {
	AvroSchemaCache.Add("field_summary", Must(avro.NewRecordSchema("r508", "", []*avro.Field{
		Must(avro.NewField("contains_null",
			BoolSchema,
			avro.WithDoc("true if the field contains null values"),
			WithFieldID(509))),
		Must(avro.NewField("contains_nan",
			NullableBoolSchema,
			avro.WithDoc("true if the field contains NaN values"),
			WithFieldID(518))),
		Must(avro.NewField("lower_bound", NullableBinarySchema,
			avro.WithDoc("serialized lower bound"),
			WithFieldID(510))),
		Must(avro.NewField("upper_bound", NullableBinarySchema,
			avro.WithDoc("serialized upper bound"),
			WithFieldID(511))),
	}, WithFieldID(508))))

	AvroSchemaCache.Add("manifest_list_file_v1", Must(avro.NewRecordSchema("manifest_file", "", []*avro.Field{
		Must(avro.NewField("manifest_path",
			StringSchema,
			avro.WithDoc("Location URI with FS scheme"),
			WithFieldID(500))),
		Must(avro.NewField("manifest_length",
			LongSchema,
			avro.WithDoc("Total file size in bytes"),
			WithFieldID(501))),
		Must(avro.NewField("partition_spec_id",
			IntSchema,
			avro.WithDoc("Spec ID used to write"),
			WithFieldID(502))),
		Must(avro.NewField("added_snapshot_id",
			LongSchema,
			avro.WithDoc("Snapshot ID that added the manifest"),
			WithFieldID(503))),
		Must(avro.NewField("added_files_count",
			NullableIntSchema,
			avro.WithDoc("Added entry count"),
			WithFieldID(504))),
		Must(avro.NewField("existing_files_count",
			NullableIntSchema,
			avro.WithDoc("Existing entry count"),
			WithFieldID(505))),
		Must(avro.NewField("deleted_files_count",
			NullableIntSchema,
			avro.WithDoc("Deleted entry count"),
			WithFieldID(506))),
		Must(avro.NewField("partitions",
			NullableSchema(
				avro.NewArraySchema(AvroSchemaCache.Get("field_summary"),
					WithElementID(508))),
			avro.WithDoc("Partition field summaries"),
			WithFieldID(507))),
		Must(avro.NewField("added_rows_count",
			NullableLongSchema,
			avro.WithDoc("Added row count"),
			WithFieldID(512))),
		Must(avro.NewField("existing_rows_count",
			NullableLongSchema,
			avro.WithDoc("Existing row count"),
			WithFieldID(513))),
		Must(avro.NewField("deleted_rows_count",
			NullableLongSchema,
			avro.WithDoc("Deleted row count"),
			WithFieldID(514))),
		Must(avro.NewField("key_metadata", NullableBinarySchema,
			avro.WithDoc("Key metadata"),
			WithFieldID(519))),
	})))

	AvroSchemaCache.Add("manifest_list_file_v2", Must(avro.NewRecordSchema("manifest_file", "", []*avro.Field{
		Must(avro.NewField("manifest_path",
			StringSchema,
			avro.WithDoc("Location URI with FS scheme"),
			WithFieldID(500))),
		Must(avro.NewField("manifest_length",
			LongSchema,
			avro.WithDoc("Total file size in bytes"),
			WithFieldID(501))),
		Must(avro.NewField("partition_spec_id",
			IntSchema,
			avro.WithDoc("Spec ID used to write"),
			WithFieldID(502))),
		Must(avro.NewField("content", IntSchema,
			avro.WithDoc("Content type"),
			avro.WithDefault(0),
			WithFieldID(517))),
		Must(avro.NewField("sequence_number", LongSchema,
			avro.WithDoc("Sequence number"),
			avro.WithDefault(int64(0)),
			WithFieldID(515))),
		Must(avro.NewField("min_sequence_number", LongSchema,
			avro.WithDoc("Minimum sequence number"),
			avro.WithDefault(int64(0)),
			WithFieldID(516))),
		Must(avro.NewField("added_snapshot_id",
			LongSchema,
			avro.WithDoc("Snapshot ID that added the manifest"),
			WithFieldID(503))),
		Must(avro.NewField("added_files_count",
			IntSchema,
			avro.WithDoc("Added entry count"),
			WithFieldID(504))),
		Must(avro.NewField("existing_files_count",
			IntSchema,
			avro.WithDoc("Existing entry count"),
			WithFieldID(505))),
		Must(avro.NewField("deleted_files_count",
			IntSchema,
			avro.WithDoc("Deleted entry count"),
			WithFieldID(506))),
		Must(avro.NewField("partitions",
			NullableSchema(
				avro.NewArraySchema(AvroSchemaCache.Get("field_summary"),
					WithElementID(508))),
			avro.WithDoc("Partition field summaries"),
			WithFieldID(507))),
		Must(avro.NewField("added_rows_count",
			LongSchema,
			avro.WithDoc("Added row count"),
			WithFieldID(512))),
		Must(avro.NewField("existing_rows_count",
			LongSchema,
			avro.WithDoc("Existing row count"),
			WithFieldID(513))),
		Must(avro.NewField("deleted_rows_count",
			LongSchema,
			avro.WithDoc("Deleted row count"),
			WithFieldID(514))),
		Must(avro.NewField("key_metadata", NullableBinarySchema,
			avro.WithDoc("Key metadata"),
			WithFieldID(519))),
	})))

	AvroSchemaCache.Add("data_file_v1", Must(avro.NewRecordSchema("r2", "", []*avro.Field{
		Must(avro.NewField("file_path",
			StringSchema,
			avro.WithDoc("Location URI with FS scheme"),
			WithFieldID(100))),
		Must(avro.NewField("file_format",
			StringSchema,
			avro.WithDoc("File format name: avro, orc, parquet"),
			WithFieldID(101))),
		// skip partition field, we'll add that dynamically as needed
		Must(avro.NewField("record_count",
			LongSchema,
			avro.WithDoc("Number of records in the file"),
			WithFieldID(103))),
		Must(avro.NewField("file_size_in_bytes",
			LongSchema,
			avro.WithDoc("Size of the file in bytes"),
			WithFieldID(104))),
		Must(avro.NewField("block_size_in_bytes",
			LongSchema,
			avro.WithDoc("Deprecated. Always write default in v1. Do not write in v2."),
			avro.WithDefault(int64(64*1024*1024)),
			WithFieldID(105))),
		Must(avro.NewField("column_sizes",
			NullableSchema(newMapSchema("k117_v118", IntSchema, LongSchema, 117, 118)),
			avro.WithDoc("map of column id to total size on disk"),
			WithFieldID(108))),
		Must(avro.NewField("value_counts",
			NullableSchema(newMapSchema("k119_v120", IntSchema, LongSchema, 119, 120)),
			avro.WithDoc("map of value to count"),
			WithFieldID(109))),
		Must(avro.NewField("null_value_counts",
			NullableSchema(newMapSchema("k121_v122", IntSchema, LongSchema, 121, 122)),
			avro.WithDoc("map of value to count"),
			WithFieldID(110))),
		Must(avro.NewField("nan_value_counts",
			NullableSchema(newMapSchema("k138_v139", IntSchema, LongSchema, 138, 139)),
			avro.WithDoc("map of value to count"),
			WithFieldID(137))),
		Must(avro.NewField("lower_bounds",
			NullableSchema(newMapSchema("k126_v127", IntSchema, BinarySchema, 126, 127)),
			avro.WithDoc("map of column id to lower bound"),
			WithFieldID(125))),
		Must(avro.NewField("upper_bounds",
			NullableSchema(newMapSchema("k129_v130", IntSchema, BinarySchema, 129, 130)),
			avro.WithDoc("map of column id to upper bound"),
			WithFieldID(128))),
		Must(avro.NewField("key_metadata", NullableBinarySchema,
			avro.WithDoc("Encryption Key Metadata Blob"),
			WithFieldID(131))),
		Must(avro.NewField("split_offsets",
			NullableSchema(avro.NewArraySchema(LongSchema,
				WithElementID(133))),
			avro.WithDoc("splitable offsets"),
			WithFieldID(132))),
		Must(avro.NewField("sort_order_id",
			NullableIntSchema,
			avro.WithDoc("Sort order ID"),
			WithFieldID(140))),
	})))

	AvroSchemaCache.Add("data_file_v2", Must(avro.NewRecordSchema("r2", "", []*avro.Field{
		Must(avro.NewField("content", IntSchema,
			avro.WithDoc("Content type"),
			avro.WithDefault(0),
			WithFieldID(134))),
		Must(avro.NewField("file_path",
			StringSchema,
			avro.WithDoc("Location URI with FS scheme"),
			WithFieldID(100))),
		Must(avro.NewField("file_format",
			StringSchema,
			avro.WithDoc("File format name: avro, orc, parquet"),
			WithFieldID(101))),
		// skip partition field, we'll add that dynamically as needed
		Must(avro.NewField("record_count",
			LongSchema,
			avro.WithDoc("Number of records in the file"),
			WithFieldID(103))),
		Must(avro.NewField("file_size_in_bytes",
			LongSchema,
			avro.WithDoc("Size of the file in bytes"),
			WithFieldID(104))),
		Must(avro.NewField("column_sizes",
			NullableSchema(newMapSchema("k117_v118", IntSchema, LongSchema, 117, 118)),
			avro.WithDoc("map of column id to total size on disk"),
			WithFieldID(108))),
		Must(avro.NewField("value_counts",
			NullableSchema(newMapSchema("k119_v120", IntSchema, LongSchema, 119, 120)),
			avro.WithDoc("map of value to count"),
			WithFieldID(109))),
		Must(avro.NewField("null_value_counts",
			NullableSchema(newMapSchema("k121_v122", IntSchema, LongSchema, 121, 122)),
			avro.WithDoc("map of value to count"),
			WithFieldID(110))),
		Must(avro.NewField("nan_value_counts",
			NullableSchema(newMapSchema("k138_v139", IntSchema, LongSchema, 138, 139)),
			avro.WithDoc("map of value to count"),
			WithFieldID(137))),
		Must(avro.NewField("lower_bounds",
			NullableSchema(newMapSchema("k126_v127", IntSchema, BinarySchema, 126, 127)),
			avro.WithDoc("map of column id to lower bound"),
			WithFieldID(125))),
		Must(avro.NewField("upper_bounds",
			NullableSchema(newMapSchema("k129_v130", IntSchema, BinarySchema, 129, 130)),
			avro.WithDoc("map of column id to upper bound"),
			WithFieldID(128))),
		Must(avro.NewField("key_metadata", NullableBinarySchema,
			avro.WithDoc("Encryption Key Metadata Blob"),
			WithFieldID(131))),
		Must(avro.NewField("split_offsets",
			NullableSchema(avro.NewArraySchema(LongSchema,
				WithElementID(133))),
			avro.WithDoc("splitable offsets"),
			WithFieldID(132))),
		Must(avro.NewField("equality_ids",
			NullableSchema(avro.NewArraySchema(LongSchema,
				WithElementID(136))),
			avro.WithDoc("field ids used to determine row equality in equality delete files"),
			WithFieldID(135))),
		Must(avro.NewField("sort_order_id",
			NullableIntSchema,
			avro.WithDoc("Sort order ID"),
			WithFieldID(140))),
	})))

	AvroSchemaCache.Add("manifest_entry_v1", Must(avro.NewRecordSchema("manifest_entry", "", []*avro.Field{
		Must(avro.NewField("status", IntSchema, WithFieldID(0))),
		Must(avro.NewField("snapshot_id", LongSchema, WithFieldID(1))),
		// leave data_file for dyanmic generation
	})))

	AvroSchemaCache.Add("manifest_entry_v2", Must(avro.NewRecordSchema("manifest_entry", "", []*avro.Field{
		Must(avro.NewField("status", IntSchema, WithFieldID(0))),
		Must(avro.NewField("snapshot_id", NullableLongSchema, WithFieldID(1))),
		Must(avro.NewField("sequence_number", NullableLongSchema, WithFieldID(3))),
		Must(avro.NewField("file_sequence_number", NullableLongSchema, WithFieldID(4))),
		// leave data_file for dynamic generation
	})))

	AvroSchemaCache.Add("manifest_list_file_v3", Must(avro.NewRecordSchema("manifest_file", "", []*avro.Field{
		Must(avro.NewField("manifest_path",
			StringSchema,
			avro.WithDoc("Location URI with FS scheme"),
			WithFieldID(500))),
		Must(avro.NewField("manifest_length",
			LongSchema,
			avro.WithDoc("Total file size in bytes"),
			WithFieldID(501))),
		Must(avro.NewField("partition_spec_id",
			IntSchema,
			avro.WithDoc("Spec ID used to write"),
			WithFieldID(502))),
		Must(avro.NewField("content", IntSchema,
			avro.WithDoc("Content type"),
			avro.WithDefault(0),
			WithFieldID(517))),
		Must(avro.NewField("sequence_number", LongSchema,
			avro.WithDoc("Sequence number"),
			avro.WithDefault(int64(0)),
			WithFieldID(515))),
		Must(avro.NewField("min_sequence_number", LongSchema,
			avro.WithDoc("Minimum sequence number"),
			avro.WithDefault(int64(0)),
			WithFieldID(516))),
		Must(avro.NewField("added_snapshot_id",
			LongSchema,
			avro.WithDoc("Snapshot ID that added the manifest"),
			WithFieldID(503))),
		Must(avro.NewField("added_files_count",
			IntSchema,
			avro.WithDoc("Added entry count"),
			WithFieldID(504))),
		Must(avro.NewField("existing_files_count",
			IntSchema,
			avro.WithDoc("Existing entry count"),
			WithFieldID(505))),
		Must(avro.NewField("deleted_files_count",
			IntSchema,
			avro.WithDoc("Deleted entry count"),
			WithFieldID(506))),
		Must(avro.NewField("partitions",
			NullableSchema(
				avro.NewArraySchema(AvroSchemaCache.Get("field_summary"),
					WithElementID(508))),
			avro.WithDoc("Partition field summaries"),
			WithFieldID(507))),
		Must(avro.NewField("added_rows_count",
			LongSchema,
			avro.WithDoc("Added row count"),
			WithFieldID(512))),
		Must(avro.NewField("existing_rows_count",
			LongSchema,
			avro.WithDoc("Existing row count"),
			WithFieldID(513))),
		Must(avro.NewField("deleted_rows_count",
			LongSchema,
			avro.WithDoc("Deleted row count"),
			WithFieldID(514))),
		Must(avro.NewField("key_metadata", NullableBinarySchema,
			avro.WithDoc("Key metadata"),
			WithFieldID(519))),
		Must(avro.NewField("first_row_id", NullableLongSchema,
			avro.WithDoc("First row ID"),
			WithFieldID(520))),
	})))
	AvroSchemaCache.Add("data_file_v3", Must(avro.NewRecordSchema("r2", "", []*avro.Field{
		Must(avro.NewField("content", IntSchema,
			avro.WithDoc("Content type"),
			avro.WithDefault(0),
			WithFieldID(134))),
		Must(avro.NewField("file_path",
			StringSchema,
			avro.WithDoc("Location URI with FS scheme"),
			WithFieldID(100))),
		Must(avro.NewField("file_format",
			StringSchema,
			avro.WithDoc("File format name: avro, orc, parquet"),
			WithFieldID(101))),
		// skip partition field, we'll add that dynamically as needed
		Must(avro.NewField("record_count",
			LongSchema,
			avro.WithDoc("Number of records in the file"),
			WithFieldID(103))),
		Must(avro.NewField("file_size_in_bytes",
			LongSchema,
			avro.WithDoc("Size of the file in bytes"),
			WithFieldID(104))),
		Must(avro.NewField("column_sizes",
			NullableSchema(newMapSchema("k117_v118", IntSchema, LongSchema, 117, 118)),
			avro.WithDoc("map of column id to total size on disk"),
			WithFieldID(108))),
		Must(avro.NewField("value_counts",
			NullableSchema(newMapSchema("k119_v120", IntSchema, LongSchema, 119, 120)),
			avro.WithDoc("map of value to count"),
			WithFieldID(109))),
		Must(avro.NewField("null_value_counts",
			NullableSchema(newMapSchema("k121_v122", IntSchema, LongSchema, 121, 122)),
			avro.WithDoc("map of value to count"),
			WithFieldID(110))),
		Must(avro.NewField("nan_value_counts",
			NullableSchema(newMapSchema("k138_v139", IntSchema, LongSchema, 138, 139)),
			avro.WithDoc("map of value to count"),
			WithFieldID(137))),
		Must(avro.NewField("lower_bounds",
			NullableSchema(newMapSchema("k126_v127", IntSchema, BinarySchema, 126, 127)),
			avro.WithDoc("map of column id to lower bound"),
			WithFieldID(125))),
		Must(avro.NewField("upper_bounds",
			NullableSchema(newMapSchema("k129_v130", IntSchema, BinarySchema, 129, 130)),
			avro.WithDoc("map of column id to upper bound"),
			WithFieldID(128))),
		Must(avro.NewField("key_metadata", NullableBinarySchema,
			avro.WithDoc("Encryption Key Metadata Blob"),
			WithFieldID(131))),
		Must(avro.NewField("split_offsets",
			NullableSchema(avro.NewArraySchema(LongSchema,
				WithElementID(133))),
			avro.WithDoc("splitable offsets"),
			WithFieldID(132))),
		Must(avro.NewField("equality_ids",
			NullableSchema(avro.NewArraySchema(LongSchema,
				WithElementID(136))),
			avro.WithDoc("field ids used to determine row equality in equality delete files"),
			WithFieldID(135))),
		Must(avro.NewField("sort_order_id",
			NullableIntSchema,
			avro.WithDoc("Sort order ID"),
			WithFieldID(140))),
		Must(avro.NewField("first_row_id",
			NullableLongSchema,
			avro.WithDoc("The _row_id for the first row in the data file"),
			WithFieldID(142))),
		Must(avro.NewField("referenced_data_file",
			NullableSchema(StringSchema),
			avro.WithDoc("Fully qualified location of a data file that all deletes reference"),
			WithFieldID(143))),
		Must(avro.NewField("content_offset",
			NullableLongSchema,
			avro.WithDoc("The offset in the file where the content starts"),
			WithFieldID(144))),
		Must(avro.NewField("content_size_in_bytes",
			NullableLongSchema,
			avro.WithDoc("The length of the referenced content stored in the file"),
			WithFieldID(145))),
	})))
	AvroSchemaCache.Add("manifest_entry_v3", Must(avro.NewRecordSchema("manifest_entry", "", []*avro.Field{
		Must(avro.NewField("status", IntSchema, WithFieldID(0))),
		Must(avro.NewField("snapshot_id", NullableLongSchema, WithFieldID(1))),
		Must(avro.NewField("sequence_number", NullableLongSchema, WithFieldID(3))),
		Must(avro.NewField("file_sequence_number", NullableLongSchema, WithFieldID(4))),
		// leave data_file for dynamic generation
	})))
}

func newDataFileSchema(partitionType avro.Schema, version int) (avro.Schema, error) {
	key := fmt.Sprintf("data_file_v%d", version)
	schema := AvroSchemaCache.Get(key)

	partField, err := avro.NewField("partition",
		partitionType, WithFieldID(102))
	if err != nil {
		return nil, err
	}

	return avro.NewRecordSchema("r2", "",
		append(schema.(*avro.RecordSchema).Fields(), partField))
}

func NewManifestFileSchema(version int) (avro.Schema, error) {
	switch version {
	case 1, 2, 3:
	default:
		return nil, fmt.Errorf("unsupported iceberg spec version: %d", version)
	}

	key := fmt.Sprintf("manifest_list_file_v%d", version)

	return AvroSchemaCache.Get(key), nil
}

func NewManifestEntrySchema(partitionType avro.Schema, version int) (avro.Schema, error) {
	switch version {
	case 1, 2, 3:
	default:
		return nil, fmt.Errorf("unsupported iceberg spec version: %d", version)
	}

	dfschema, err := newDataFileSchema(partitionType, version)
	if err != nil {
		return nil, err
	}

	dfField, err := avro.NewField("data_file", dfschema, WithFieldID(2))
	if err != nil {
		return nil, err
	}

	key := fmt.Sprintf("manifest_entry_v%d", version)
	schema := AvroSchemaCache.Get(key)

	return avro.NewRecordSchema("manifest_entry", "",
		append(schema.(*avro.RecordSchema).Fields(), dfField))
}
