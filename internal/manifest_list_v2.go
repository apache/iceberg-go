package internal

import (
	"github.com/hamba/avro/v2"
)

func MustNewManifestListV2Schema() avro.Schema {
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
			"content",
			avro.NewPrimitiveSchema(avro.Int, nil),
			avro.WithDoc("Contents of the manifest: 0=data, 1=deletes"),
			avro.WithProps(map[string]any{"field-id": 517}),
		),
		MustNewField(
			"sequence_number",
			avro.NewPrimitiveSchema(avro.Long, nil),
			avro.WithDoc("Sequence number when the manifest was added"),
			avro.WithProps(map[string]any{"field-id": 515}),
		),
		MustNewField(
			"min_sequence_number",
			avro.NewPrimitiveSchema(avro.Long, nil),
			avro.WithDoc("Lowest sequence number in the manifest"),
			avro.WithProps(map[string]any{"field-id": 516}),
		),
		MustNewField(
			"added_snapshot_id",
			avro.NewPrimitiveSchema(avro.Long, nil),
			avro.WithDoc("Snapshot ID that added the manifest"),
			avro.WithProps(map[string]any{"field-id": 503}),
		),
		MustNewField(
			"added_files_count",
			avro.NewPrimitiveSchema(avro.Int, nil),
			avro.WithDoc("Added entry count"),
			avro.WithProps(map[string]any{"field-id": 504}),
		),
		MustNewField(
			"existing_files_count",
			avro.NewPrimitiveSchema(avro.Int, nil),
			avro.WithDoc("Existing entry count"),
			avro.WithProps(map[string]any{"field-id": 505}),
		),
		MustNewField(
			"deleted_files_count",
			avro.NewPrimitiveSchema(avro.Int, nil),
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
			avro.NewPrimitiveSchema(avro.Long, nil),
			avro.WithDoc("Added rows count"),
			avro.WithProps(map[string]any{"field-id": 512}),
		),
		MustNewField(
			"existing_rows_count",
			avro.NewPrimitiveSchema(avro.Long, nil),
			avro.WithDoc("Existing rows count"),
			avro.WithProps(map[string]any{"field-id": 513}),
		),
		MustNewField(
			"deleted_rows_count",
			avro.NewPrimitiveSchema(avro.Long, nil),
			avro.WithDoc("Deleted rows count"),
			avro.WithProps(map[string]any{"field-id": 514}),
		),
	})
}
