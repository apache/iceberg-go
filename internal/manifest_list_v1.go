package internal

import (
	"github.com/hamba/avro/v2"
)

func MustNewManifestListV1Schema() avro.Schema {
	return MustNewRecordSchema("manifest_file", "", []*avro.Field{
		MustNewField(
			"manifest_path",
			avro.NewPrimitiveSchema(avro.String, nil),
			avro.WithDoc("Location URI with FS scheme"),
			avro.WithProps(map[string]any{"field-id": 500}),
		),
		MustNewField(
			"manifest_length",
			avro.NewPrimitiveSchema(avro.Long, nil),
			avro.WithDoc("Total file size in bytes"),
			avro.WithProps(map[string]any{"field-id": 501}),
		),
		MustNewField(
			"partition_spec_id",
			avro.NewPrimitiveSchema(avro.Int, nil),
			avro.WithDoc("Spec ID used to write"),
			avro.WithProps(map[string]any{"field-id": 502}),
		),
		MustNewField(
			"added_snapshot_id",
			avro.NewPrimitiveSchema(avro.Long, nil),
			avro.WithDoc("Snapshot ID that added the manifest"),
			avro.WithProps(map[string]any{"field-id": 503}),
		),
		MustNewField(
			"added_data_files_count",
			MustNewUnionSchema([]avro.Schema{
				avro.NewNullSchema(),
				avro.NewPrimitiveSchema(avro.Int, nil),
			}),
			avro.WithDoc("Added entry count"),
			avro.WithProps(map[string]any{"field-id": 504}),
		),
		MustNewField(
			"existing_data_files_count",
			MustNewUnionSchema([]avro.Schema{
				avro.NewNullSchema(),
				avro.NewPrimitiveSchema(avro.Int, nil),
			}),
			avro.WithDoc("Existing entry count"),
			avro.WithProps(map[string]any{"field-id": 505}),
		),
		MustNewField(
			"deleted_data_files_count",
			MustNewUnionSchema([]avro.Schema{
				avro.NewNullSchema(),
				avro.NewPrimitiveSchema(avro.Int, nil),
			}),
			avro.WithDoc("Deleted entry count"),
			avro.WithProps(map[string]any{"field-id": 506}),
		),
		MustNewField(
			"partitions",
			MustNewUnionSchema([]avro.Schema{
				avro.NewNullSchema(),
				avro.NewArraySchema(
					MustNewPartitionSchema(),
					avro.WithProps(map[string]any{"element-id": 508}),
				),
			}),
			avro.WithDoc("Summary for each partition"),
			avro.WithProps(map[string]any{"field-id": 507}),
		),
		MustNewField(
			"added_rows_count",
			MustNewUnionSchema([]avro.Schema{
				avro.NewNullSchema(),
				avro.NewPrimitiveSchema(avro.Long, nil),
			}),
			avro.WithDoc("Added rows count"),
			avro.WithProps(map[string]any{"field-id": 512}),
		),
		MustNewField(
			"existing_rows_count",
			MustNewUnionSchema([]avro.Schema{
				avro.NewNullSchema(),
				avro.NewPrimitiveSchema(avro.Long, nil),
			}),
			avro.WithDoc("Existing rows count"),
			avro.WithProps(map[string]any{"field-id": 513}),
		),
		MustNewField(
			"deleted_rows_count",
			MustNewUnionSchema([]avro.Schema{
				avro.NewNullSchema(),
				avro.NewPrimitiveSchema(avro.Long, nil),
			}),
			avro.WithDoc("Deleted rows count"),
			avro.WithProps(map[string]any{"field-id": 514}),
		),
	})
}

func MustNewPartitionSchema() avro.Schema {
	return MustNewRecordSchema(
		"r508",
		"",
		[]*avro.Field{
			MustNewField(
				"contains_null",
				avro.NewPrimitiveSchema(avro.Boolean, nil),
				avro.WithDoc("True if any file has a null partition value"),
				avro.WithProps(map[string]any{"field-id": 509}),
			),
			MustNewField(
				"contains_nan",
				MustNewUnionSchema([]avro.Schema{
					avro.NewNullSchema(),
					avro.NewPrimitiveSchema(avro.Boolean, nil),
				}),
				avro.WithDoc("True if any file has a nan partition value"),
				avro.WithProps(map[string]any{"field-id": 518}),
			),
			MustNewField(
				"lower_bound",
				MustNewUnionSchema([]avro.Schema{
					avro.NewNullSchema(),
					avro.NewPrimitiveSchema(avro.Bytes, nil),
				}),
				avro.WithDoc("Partition lower bound for all files"),
				avro.WithProps(map[string]any{"field-id": 510}),
			),
			MustNewField(
				"upper_bound",
				MustNewUnionSchema([]avro.Schema{
					avro.NewNullSchema(),
					avro.NewPrimitiveSchema(avro.Bytes, nil),
				}),
				avro.WithDoc("Partition upper bound for all files"),
				avro.WithProps(map[string]any{"field-id": 511}),
			),
		},
	)
}
