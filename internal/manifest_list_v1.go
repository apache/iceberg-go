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
	"github.com/hamba/avro/v2"
)

func MustNewManifestListV1Schema() avro.Schema {
	return Must(avro.NewRecordSchema("manifest_file", "", []*avro.Field{
		Must(avro.NewField(
			"manifest_path",
			avro.NewPrimitiveSchema(avro.String, nil),
			avro.WithDoc("Location URI with FS scheme"),
			avro.WithProps(map[string]any{"field-id": 500}),
		)),
		Must(avro.NewField(
			"manifest_length",
			avro.NewPrimitiveSchema(avro.Long, nil),
			avro.WithDoc("Total file size in bytes"),
			avro.WithProps(map[string]any{"field-id": 501}),
		)),
		Must(avro.NewField(
			"partition_spec_id",
			avro.NewPrimitiveSchema(avro.Int, nil),
			avro.WithDoc("Spec ID used to write"),
			avro.WithProps(map[string]any{"field-id": 502}),
		)),
		Must(avro.NewField(
			"added_snapshot_id",
			avro.NewPrimitiveSchema(avro.Long, nil),
			avro.WithDoc("Snapshot ID that added the manifest"),
			avro.WithProps(map[string]any{"field-id": 503}),
		)),
		Must(avro.NewField(
			"added_data_files_count",
			Must(avro.NewUnionSchema([]avro.Schema{
				avro.NewNullSchema(),
				avro.NewPrimitiveSchema(avro.Int, nil),
			})),
			avro.WithDoc("Added entry count"),
			avro.WithProps(map[string]any{"field-id": 504}),
		)),
		Must(avro.NewField(
			"existing_data_files_count",
			Must(avro.NewUnionSchema([]avro.Schema{
				avro.NewNullSchema(),
				avro.NewPrimitiveSchema(avro.Int, nil),
			})),
			avro.WithDoc("Existing entry count"),
			avro.WithProps(map[string]any{"field-id": 505}),
		)),
		Must(avro.NewField(
			"deleted_data_files_count",
			Must(avro.NewUnionSchema([]avro.Schema{
				avro.NewNullSchema(),
				avro.NewPrimitiveSchema(avro.Int, nil),
			})),
			avro.WithDoc("Deleted entry count"),
			avro.WithProps(map[string]any{"field-id": 506}),
		)),
		Must(avro.NewField(
			"partitions",
			Must(avro.NewUnionSchema([]avro.Schema{
				avro.NewNullSchema(),
				avro.NewArraySchema(
					MustNewPartitionSchema(),
					avro.WithProps(map[string]any{"element-id": 508}),
				),
			})),
			avro.WithDoc("Summary for each partition"),
			avro.WithProps(map[string]any{"field-id": 507}),
		)),
		Must(avro.NewField(
			"added_rows_count",
			Must(avro.NewUnionSchema([]avro.Schema{
				avro.NewNullSchema(),
				avro.NewPrimitiveSchema(avro.Long, nil),
			})),
			avro.WithDoc("Added rows count"),
			avro.WithProps(map[string]any{"field-id": 512}),
		)),
		Must(avro.NewField(
			"existing_rows_count",
			Must(avro.NewUnionSchema([]avro.Schema{
				avro.NewNullSchema(),
				avro.NewPrimitiveSchema(avro.Long, nil),
			})),
			avro.WithDoc("Existing rows count"),
			avro.WithProps(map[string]any{"field-id": 513}),
		)),
		Must(avro.NewField(
			"deleted_rows_count",
			Must(avro.NewUnionSchema([]avro.Schema{
				avro.NewNullSchema(),
				avro.NewPrimitiveSchema(avro.Long, nil),
			})),
			avro.WithDoc("Deleted rows count"),
			avro.WithProps(map[string]any{"field-id": 514}),
		)),
	}))
}

func MustNewPartitionSchema() avro.Schema {
	return Must(avro.NewRecordSchema(
		"r508",
		"",
		[]*avro.Field{
			Must(avro.NewField(
				"contains_null",
				avro.NewPrimitiveSchema(avro.Boolean, nil),
				avro.WithDoc("True if any file has a null partition value"),
				avro.WithProps(map[string]any{"field-id": 509}),
			)),
			Must(avro.NewField(
				"contains_nan",
				Must(avro.NewUnionSchema([]avro.Schema{
					avro.NewNullSchema(),
					avro.NewPrimitiveSchema(avro.Boolean, nil),
				})),
				avro.WithDoc("True if any file has a nan partition value"),
				avro.WithProps(map[string]any{"field-id": 518}),
			)),
			Must(avro.NewField(
				"lower_bound",
				Must(avro.NewUnionSchema([]avro.Schema{
					avro.NewNullSchema(),
					avro.NewPrimitiveSchema(avro.Bytes, nil),
				})),
				avro.WithDoc("Partition lower bound for all files"),
				avro.WithProps(map[string]any{"field-id": 510}),
			)),
			Must(avro.NewField(
				"upper_bound",
				Must(avro.NewUnionSchema([]avro.Schema{
					avro.NewNullSchema(),
					avro.NewPrimitiveSchema(avro.Bytes, nil),
				})),
				avro.WithDoc("Partition upper bound for all files"),
				avro.WithProps(map[string]any{"field-id": 511}),
			)),
		},
	))
}
