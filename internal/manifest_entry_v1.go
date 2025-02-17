package internal

import "github.com/hamba/avro/v2"

func MustNewManifestEntryV1Schema(partitionSchema avro.Schema) avro.Schema {
	return MustNewRecordSchema("manifest_entry", "", []*avro.Field{
		MustNewField(
			"status",
			avro.NewPrimitiveSchema(avro.Int, nil),
			avro.WithProps(map[string]any{"field-id": 0}),
		),
		MustNewField(
			"snapshot_id",
			avro.NewPrimitiveSchema(avro.Long, nil),
			avro.WithProps(map[string]any{"field-id": 1}),
		),
		MustNewField(
			"data_file",
			MustNewDataFileV1Schema(partitionSchema),
			avro.WithProps(map[string]any{"field-id": 2}),
		),
	})
}

func MustNewDataFileV1Schema(partitionSchema avro.Schema) avro.Schema {
	return MustNewRecordSchema("r2", "", []*avro.Field{
		MustNewField(
			"file_path",
			avro.NewPrimitiveSchema(avro.String, nil),
			avro.WithDoc("Location URI with FS scheme"),
			avro.WithProps(map[string]any{"field-id": 100}),
		),
		MustNewField(
			"file_format",
			avro.NewPrimitiveSchema(avro.String, nil),
			avro.WithDoc("File format name: avro, orc, or parquet"),
			avro.WithProps(map[string]any{"field-id": 101}),
		),
		MustNewField(
			"partition",
			partitionSchema,
			avro.WithProps(map[string]any{"field-id": 102}),
		),
		MustNewField(
			"record_count",
			avro.NewPrimitiveSchema(avro.Long, nil),
			avro.WithDoc("Number of records in the file"),
			avro.WithProps(map[string]any{"field-id": 103}),
		),
		MustNewField(
			"file_size_in_bytes",
			avro.NewPrimitiveSchema(avro.Long, nil),
			avro.WithDoc("Total file size in bytes"),
			avro.WithProps(map[string]any{"field-id": 104}),
		),
		MustNewField(
			"block_size_in_bytes",
			avro.NewPrimitiveSchema(avro.Long, nil),
			avro.WithProps(map[string]any{"field-id": 105}),
		),
		MustNewField(
			"column_sizes",
			MustNewUnionSchema([]avro.Schema{
				avro.NewNullSchema(),
				MustNewColumnSizesMapSchema(),
			}),
			avro.WithDoc("Map of column id to total size on disk"),
			avro.WithProps(map[string]any{"field-id": 108}),
		),
		MustNewField(
			"value_counts",
			MustNewUnionSchema([]avro.Schema{
				avro.NewNullSchema(),
				MustNewValueCountsMapSchema(),
			}),
			avro.WithDoc("Map of column id to total count, including null and NaN"),
			avro.WithProps(map[string]any{"field-id": 109}),
		),
		MustNewField(
			"null_value_counts",
			MustNewUnionSchema([]avro.Schema{
				avro.NewNullSchema(),
				MustNewNullValueCountsMapSchema(),
			}),
			avro.WithDoc("Map of column id to null value count"),
			avro.WithProps(map[string]any{"field-id": 110}),
		),
		MustNewField(
			"nan_value_counts",
			MustNewUnionSchema([]avro.Schema{
				avro.NewNullSchema(),
				MustNewNanValueCountsMapSchema(),
			}),
			avro.WithDoc("Map of column id to number of NaN values in the column"),
			avro.WithProps(map[string]any{"field-id": 137}),
		),
		MustNewField(
			"lower_bounds",
			MustNewUnionSchema([]avro.Schema{
				avro.NewNullSchema(),
				MustNewLowerBoundsMapSchema(),
			}),
			avro.WithDoc("Map of column id to lower bound"),
			avro.WithProps(map[string]any{"field-id": 125}),
		),
		MustNewField(
			"upper_bounds",
			MustNewUnionSchema([]avro.Schema{
				avro.NewNullSchema(),
				MustNewUpperBoundsMapSchema(),
			}),
			avro.WithDoc("Map of column id to upper bound"),
			avro.WithProps(map[string]any{"field-id": 128}),
		),
		MustNewField(
			"key_metadata",
			MustNewUnionSchema([]avro.Schema{
				avro.NewNullSchema(),
				avro.NewPrimitiveSchema(avro.Bytes, nil),
			}),
			avro.WithDoc("Encryption key metadata blob"),
			avro.WithProps(map[string]any{"field-id": 131}),
		),
		MustNewField(
			"split_offsets",
			MustNewUnionSchema([]avro.Schema{
				avro.NewNullSchema(),
				avro.NewArraySchema(
					avro.NewPrimitiveSchema(avro.Long, nil),
					avro.WithProps(map[string]any{"element-id": 133}),
				),
			}),
			avro.WithDoc("Splittable offsets"),
			avro.WithProps(map[string]any{"field-id": 132}),
		),
		MustNewField(
			"sort_order_id",
			MustNewUnionSchema([]avro.Schema{
				avro.NewNullSchema(),
				avro.NewPrimitiveSchema(avro.Int, nil),
			}),
			avro.WithDoc("Sort order ID"),
			avro.WithProps(map[string]any{"field-id": 140}),
		),
	})
}

func MustNewColumnSizesMapSchema() avro.Schema {
	return avro.NewArraySchema(
		MustNewRecordSchema(
			"k117_v118",
			"",
			[]*avro.Field{
				MustNewField(
					"key",
					avro.NewPrimitiveSchema(avro.Int, nil),
					avro.WithProps(map[string]any{"field-id": 117}),
				),
				MustNewField(
					"value",
					avro.NewPrimitiveSchema(avro.Long, nil),
					avro.WithProps(map[string]any{"field-id": 118}),
				),
			},
		),
		avro.WithProps(map[string]any{"logicalType": "map"}),
	)
}

func MustNewValueCountsMapSchema() avro.Schema {
	return avro.NewArraySchema(
		MustNewRecordSchema(
			"k119_v120",
			"",
			[]*avro.Field{
				MustNewField(
					"key",
					avro.NewPrimitiveSchema(avro.Int, nil),
					avro.WithProps(map[string]any{"field-id": 119}),
				),
				MustNewField(
					"value",
					avro.NewPrimitiveSchema(avro.Long, nil),
					avro.WithProps(map[string]any{"field-id": 120}),
				),
			},
		),
		avro.WithProps(map[string]any{"logicalType": "map"}),
	)
}

func MustNewNullValueCountsMapSchema() avro.Schema {
	return avro.NewArraySchema(
		MustNewRecordSchema(
			"k121_v122",
			"",
			[]*avro.Field{
				MustNewField(
					"key",
					avro.NewPrimitiveSchema(avro.Int, nil),
					avro.WithProps(map[string]any{"field-id": 121}),
				),
				MustNewField(
					"value",
					avro.NewPrimitiveSchema(avro.Long, nil),
					avro.WithProps(map[string]any{"field-id": 122}),
				),
			},
		),
		avro.WithProps(map[string]any{"logicalType": "map"}),
	)
}

func MustNewNanValueCountsMapSchema() avro.Schema {
	return avro.NewArraySchema(
		MustNewRecordSchema(
			"k138_v139",
			"",
			[]*avro.Field{
				MustNewField(
					"key",
					avro.NewPrimitiveSchema(avro.Int, nil),
					avro.WithProps(map[string]any{"field-id": 138}),
				),
				MustNewField(
					"value",
					avro.NewPrimitiveSchema(avro.Long, nil),
					avro.WithProps(map[string]any{"field-id": 139}),
				),
			},
		),
		avro.WithProps(map[string]any{"logicalType": "map"}),
	)
}

func MustNewLowerBoundsMapSchema() avro.Schema {
	return avro.NewArraySchema(
		MustNewRecordSchema(
			"k126_v127",
			"",
			[]*avro.Field{
				MustNewField(
					"key",
					avro.NewPrimitiveSchema(avro.Int, nil),
					avro.WithProps(map[string]any{"field-id": 126}),
				),
				MustNewField(
					"value",
					avro.NewPrimitiveSchema(avro.Bytes, nil),
					avro.WithProps(map[string]any{"field-id": 127}),
				),
			},
		),
		avro.WithProps(map[string]any{"logicalType": "map"}),
	)
}

func MustNewUpperBoundsMapSchema() avro.Schema {
	return avro.NewArraySchema(
		MustNewRecordSchema(
			"k129_v130",
			"",
			[]*avro.Field{
				MustNewField(
					"key",
					avro.NewPrimitiveSchema(avro.Int, nil),
					avro.WithProps(map[string]any{"field-id": 129}),
				),
				MustNewField(
					"value",
					avro.NewPrimitiveSchema(avro.Bytes, nil),
					avro.WithProps(map[string]any{"field-id": 130}),
				),
			},
		),
		avro.WithProps(map[string]any{"logicalType": "map"}),
	)
}
