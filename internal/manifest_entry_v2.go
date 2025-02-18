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

import "github.com/hamba/avro/v2"

func MustNewManifestEntryV2Schema(partitionSchema avro.Schema) avro.Schema {
	return Must(avro.NewRecordSchema("manifest_entry", "", []*avro.Field{
		Must(avro.NewField(
			"status",
			avro.NewPrimitiveSchema(avro.Int, nil),
			avro.WithProps(map[string]any{"field-id": 0}),
		)),
		Must(avro.NewField(
			"snapshot_id",
			Must(avro.NewUnionSchema([]avro.Schema{
				avro.NewNullSchema(),
				avro.NewPrimitiveSchema(avro.Long, nil),
			})),
			avro.WithProps(map[string]any{"field-id": 1}),
		)),
		Must(avro.NewField(
			"sequence_number",
			Must(avro.NewUnionSchema([]avro.Schema{
				avro.NewNullSchema(),
				avro.NewPrimitiveSchema(avro.Long, nil),
			})),
			avro.WithProps(map[string]any{"field-id": 3}),
		)),
		Must(avro.NewField(
			"file_sequence_number",
			Must(avro.NewUnionSchema([]avro.Schema{
				avro.NewNullSchema(),
				avro.NewPrimitiveSchema(avro.Long, nil),
			})),
			avro.WithProps(map[string]any{"field-id": 4}),
		)),
		Must(avro.NewField(
			"data_file",
			MustNewDataFileV2Schema(partitionSchema),
			avro.WithProps(map[string]any{"field-id": 2}),
		)),
	}))
}

func MustNewDataFileV2Schema(partitionSchema avro.Schema) avro.Schema {
	return Must(avro.NewRecordSchema("r2", "", []*avro.Field{
		Must(avro.NewField(
			"content",
			avro.NewPrimitiveSchema(avro.Int, nil),
			avro.WithDoc("Type of content stored by the data file"),
			avro.WithProps(map[string]any{"field-id": 134}),
		)),
		Must(avro.NewField(
			"file_path",
			avro.NewPrimitiveSchema(avro.String, nil),
			avro.WithDoc("Location URI with FS scheme"),
			avro.WithProps(map[string]any{"field-id": 100}),
		)),
		Must(avro.NewField(
			"file_format",
			avro.NewPrimitiveSchema(avro.String, nil),
			avro.WithDoc("File format name: avro, orc, or parquet"),
			avro.WithProps(map[string]any{"field-id": 101}),
		)),
		Must(avro.NewField(
			"partition",
			partitionSchema,
			avro.WithProps(map[string]any{"field-id": 102}),
		)),
		Must(avro.NewField(
			"record_count",
			avro.NewPrimitiveSchema(avro.Long, nil),
			avro.WithDoc("Number of records in the file"),
			avro.WithProps(map[string]any{"field-id": 103}),
		)),
		Must(avro.NewField(
			"file_size_in_bytes",
			avro.NewPrimitiveSchema(avro.Long, nil),
			avro.WithDoc("Total file size in bytes"),
			avro.WithProps(map[string]any{"field-id": 104}),
		)),
		Must(avro.NewField(
			"column_sizes",
			Must(avro.NewUnionSchema([]avro.Schema{
				avro.NewNullSchema(),
				MustNewColumnSizesMapSchema(),
			})),
			avro.WithDoc("Map of column id to total size on disk"),
			avro.WithProps(map[string]any{"field-id": 108}),
		)),
		Must(avro.NewField(
			"value_counts",
			Must(avro.NewUnionSchema([]avro.Schema{
				avro.NewNullSchema(),
				MustNewValueCountsMapSchema(),
			})),
			avro.WithDoc("Map of column id to total count, including null and NaN"),
			avro.WithProps(map[string]any{"field-id": 109}),
		)),
		Must(avro.NewField(
			"null_value_counts",
			Must(avro.NewUnionSchema([]avro.Schema{
				avro.NewNullSchema(),
				MustNewNullValueCountsMapSchema(),
			})),
			avro.WithDoc("Map of column id to null value count"),
			avro.WithProps(map[string]any{"field-id": 110}),
		)),
		Must(avro.NewField(
			"nan_value_counts",
			Must(avro.NewUnionSchema([]avro.Schema{
				avro.NewNullSchema(),
				MustNewNanValueCountsMapSchema(),
			})),
			avro.WithDoc("Map of column id to number of NaN values in the column"),
			avro.WithProps(map[string]any{"field-id": 137}),
		)),
		Must(avro.NewField(
			"lower_bounds",
			Must(avro.NewUnionSchema([]avro.Schema{
				avro.NewNullSchema(),
				MustNewLowerBoundsMapSchema(),
			})),
			avro.WithDoc("Map of column id to lower bound"),
			avro.WithProps(map[string]any{"field-id": 125}),
		)),
		Must(avro.NewField(
			"upper_bounds",
			Must(avro.NewUnionSchema([]avro.Schema{
				avro.NewNullSchema(),
				MustNewUpperBoundsMapSchema(),
			})),
			avro.WithDoc("Map of column id to upper bound"),
			avro.WithProps(map[string]any{"field-id": 128}),
		)),
		Must(avro.NewField(
			"key_metadata",
			Must(avro.NewUnionSchema([]avro.Schema{
				avro.NewNullSchema(),
				avro.NewPrimitiveSchema(avro.Bytes, nil),
			})),
			avro.WithDoc("Encryption key metadata blob"),
			avro.WithProps(map[string]any{"field-id": 131}),
		)),
		Must(avro.NewField(
			"split_offsets",
			Must(avro.NewUnionSchema([]avro.Schema{
				avro.NewNullSchema(),
				avro.NewArraySchema(
					avro.NewPrimitiveSchema(avro.Long, nil),
					avro.WithProps(map[string]any{"element-id": 133}),
				),
			})),
			avro.WithDoc("Splittable offsets"),
			avro.WithProps(map[string]any{"field-id": 132}),
		)),
		Must(avro.NewField(
			"equality_ids",
			Must(avro.NewUnionSchema([]avro.Schema{
				avro.NewNullSchema(),
				avro.NewArraySchema(
					avro.NewPrimitiveSchema(avro.Int, nil),
					avro.WithProps(map[string]any{"element-id": 136}),
				),
			})),
			avro.WithDoc("Field ids used to determine row equality for delete files"),
			avro.WithProps(map[string]any{"field-id": 135}),
		)),
		Must(avro.NewField(
			"sort_order_id",
			Must(avro.NewUnionSchema([]avro.Schema{
				avro.NewNullSchema(),
				avro.NewPrimitiveSchema(avro.Int, nil),
			})),
			avro.WithDoc("Sort order ID"),
			avro.WithProps(map[string]any{"field-id": 140}),
		)),
	}))
}
