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
	"context"
	"errors"
	"fmt"
	"sort"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/scalar"
	"github.com/apache/iceberg-go"
	"github.com/google/uuid"
)

// Entries returns every manifest entry in the current snapshot, including
// entries marked deleted. This exposes commit history that data_files and
// delete_files intentionally hide.
func (i InspectTable) Entries(ctx context.Context) (array.RecordReader, error) {
	spec := i.tbl.metadata.PartitionSpec()
	partitionType := spec.PartitionType(i.tbl.metadata.CurrentSchema())
	schema := EntriesSchema(partitionType)
	arrowSchema, err := SchemaToArrowSchema(schema, nil, true, false)
	if err != nil {
		return nil, fmt.Errorf("inspect entries: build arrow schema: %w", err)
	}

	entries, err := i.currentManifestEntries(ctx)
	if err != nil {
		return nil, fmt.Errorf("inspect entries: %w", err)
	}

	bldr := array.NewRecordBuilder(i.alloc, arrowSchema)
	defer bldr.Release()
	for _, entry := range entries {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		bldr.Field(0).(*array.Int32Builder).Append(int32(entry.Status()))
		appendEntriesOptionalInt64(bldr.Field(1).(*array.Int64Builder), entry.SnapshotID())
		appendEntriesOptionalInt64(bldr.Field(2).(*array.Int64Builder), entry.SequenceNum())
		appendEntriesOptionalInt64Ptr(bldr.Field(3).(*array.Int64Builder), entry.FileSequenceNum())
		if err := appendEntriesDataFile(bldr.Field(4).(*array.StructBuilder), partitionType, entry.DataFile()); err != nil {
			return nil, fmt.Errorf("inspect entries: append %w", err)
		}
	}

	rr, err := singleBatchReader(arrowSchema, bldr)
	if err != nil {
		return nil, fmt.Errorf("inspect entries: %w", err)
	}

	return rr, nil
}

func (i InspectTable) currentManifestEntries(ctx context.Context) ([]iceberg.ManifestEntry, error) {
	snapshot := i.tbl.metadata.CurrentSnapshot()
	if snapshot == nil {
		return nil, nil
	}
	if i.tbl.fsF == nil {
		return nil, errors.New("table file IO is not configured")
	}
	fs, err := i.tbl.fsF(ctx)
	if err != nil {
		return nil, err
	}
	manifests, err := snapshot.Manifests(fs)
	if err != nil {
		return nil, err
	}

	entries := make([]iceberg.ManifestEntry, 0)
	for _, manifest := range manifests {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		for entry, err := range manifest.Entries(fs, false) {
			if err != nil {
				return nil, err
			}
			entries = append(entries, entry)
		}
	}

	return entries, nil
}

// EntriesSchema returns the schema of the entries metadata table.
func EntriesSchema(partitionType *iceberg.StructType) *iceberg.Schema {
	return iceberg.NewSchema(0,
		iceberg.NestedField{ID: 0, Name: "status", Type: iceberg.PrimitiveTypes.Int32, Required: true},
		iceberg.NestedField{ID: 1, Name: "snapshot_id", Type: iceberg.PrimitiveTypes.Int64, Required: false},
		iceberg.NestedField{ID: 3, Name: "sequence_number", Type: iceberg.PrimitiveTypes.Int64, Required: false},
		iceberg.NestedField{ID: 4, Name: "file_sequence_number", Type: iceberg.PrimitiveTypes.Int64, Required: false},
		iceberg.NestedField{ID: 2, Name: "data_file", Type: entriesDataFileType(partitionType), Required: true},
	)
}

func entriesDataFileType(partitionType *iceberg.StructType) *iceberg.StructType {
	fields := []iceberg.NestedField{
		{ID: 134, Name: "content", Type: iceberg.PrimitiveTypes.Int32, Required: false},
		{ID: 100, Name: "file_path", Type: iceberg.PrimitiveTypes.String, Required: true},
		{ID: 101, Name: "file_format", Type: iceberg.PrimitiveTypes.String, Required: true},
		{ID: 141, Name: "spec_id", Type: iceberg.PrimitiveTypes.Int32, Required: false},
	}
	if partitionType != nil && len(partitionType.FieldList) > 0 {
		fields = append(fields, iceberg.NestedField{ID: 102, Name: "partition", Type: partitionType, Required: true})
	}
	fields = append(fields,
		iceberg.NestedField{ID: 103, Name: "record_count", Type: iceberg.PrimitiveTypes.Int64, Required: true},
		iceberg.NestedField{ID: 104, Name: "file_size_in_bytes", Type: iceberg.PrimitiveTypes.Int64, Required: true},
		entriesInt64MapField(108, "column_sizes", 117, 118),
		entriesInt64MapField(109, "value_counts", 119, 120),
		entriesInt64MapField(110, "null_value_counts", 121, 122),
		entriesInt64MapField(137, "nan_value_counts", 138, 139),
		iceberg.NestedField{ID: 125, Name: "lower_bounds", Type: entriesBinaryMapType(126, 127), Required: false},
		iceberg.NestedField{ID: 128, Name: "upper_bounds", Type: entriesBinaryMapType(129, 130), Required: false},
		iceberg.NestedField{ID: 131, Name: "key_metadata", Type: iceberg.PrimitiveTypes.Binary, Required: false},
		iceberg.NestedField{ID: 132, Name: "split_offsets", Type: &iceberg.ListType{ElementID: 133, Element: iceberg.PrimitiveTypes.Int64, ElementRequired: true}, Required: false},
		iceberg.NestedField{ID: 135, Name: "equality_ids", Type: &iceberg.ListType{ElementID: 136, Element: iceberg.PrimitiveTypes.Int32, ElementRequired: true}, Required: false},
		iceberg.NestedField{ID: 140, Name: "sort_order_id", Type: iceberg.PrimitiveTypes.Int32, Required: false},
		iceberg.NestedField{ID: 142, Name: "first_row_id", Type: iceberg.PrimitiveTypes.Int64, Required: false},
		iceberg.NestedField{ID: 143, Name: "referenced_data_file", Type: iceberg.PrimitiveTypes.String, Required: false},
		iceberg.NestedField{ID: 144, Name: "content_offset", Type: iceberg.PrimitiveTypes.Int64, Required: false},
		iceberg.NestedField{ID: 145, Name: "content_size_in_bytes", Type: iceberg.PrimitiveTypes.Int64, Required: false},
	)

	return &iceberg.StructType{FieldList: fields}
}

func entriesInt64MapField(id int, name string, keyID, valueID int) iceberg.NestedField {
	return iceberg.NestedField{ID: id, Name: name, Type: &iceberg.MapType{
		KeyID: keyID, KeyType: iceberg.PrimitiveTypes.Int32,
		ValueID: valueID, ValueType: iceberg.PrimitiveTypes.Int64, ValueRequired: true,
	}, Required: false}
}

func entriesBinaryMapType(keyID, valueID int) *iceberg.MapType {
	return &iceberg.MapType{
		KeyID: keyID, KeyType: iceberg.PrimitiveTypes.Int32,
		ValueID: valueID, ValueType: iceberg.PrimitiveTypes.Binary, ValueRequired: true,
	}
}

func appendEntriesDataFile(bldr *array.StructBuilder, partitionType *iceberg.StructType, file iceberg.DataFile) error {
	idx := 0
	bldr.FieldBuilder(idx).(*array.Int32Builder).Append(int32(file.ContentType()))
	idx++
	bldr.FieldBuilder(idx).(*array.StringBuilder).Append(file.FilePath())
	idx++
	bldr.FieldBuilder(idx).(*array.StringBuilder).Append(string(file.FileFormat()))
	idx++
	bldr.FieldBuilder(idx).(*array.Int32Builder).Append(file.SpecID())
	idx++
	if partitionType != nil && len(partitionType.FieldList) > 0 {
		if err := appendEntriesPartition(bldr.FieldBuilder(idx).(*array.StructBuilder), partitionType, file.Partition()); err != nil {
			return err
		}
		idx++
	}
	bldr.FieldBuilder(idx).(*array.Int64Builder).Append(file.Count())
	idx++
	bldr.FieldBuilder(idx).(*array.Int64Builder).Append(file.FileSizeBytes())
	idx++
	appendEntriesInt64Map(bldr.FieldBuilder(idx).(*array.MapBuilder), file.ColumnSizes())
	idx++
	appendEntriesInt64Map(bldr.FieldBuilder(idx).(*array.MapBuilder), file.ValueCounts())
	idx++
	appendEntriesInt64Map(bldr.FieldBuilder(idx).(*array.MapBuilder), file.NullValueCounts())
	idx++
	appendEntriesInt64Map(bldr.FieldBuilder(idx).(*array.MapBuilder), file.NaNValueCounts())
	idx++
	appendEntriesBinaryMap(bldr.FieldBuilder(idx).(*array.MapBuilder), file.LowerBoundValues())
	idx++
	appendEntriesBinaryMap(bldr.FieldBuilder(idx).(*array.MapBuilder), file.UpperBoundValues())
	idx++
	appendEntriesBytes(bldr.FieldBuilder(idx), file.KeyMetadata())
	idx++
	appendEntriesInt64List(bldr.FieldBuilder(idx).(*array.ListBuilder), file.SplitOffsets())
	idx++
	appendEntriesInt32List(bldr.FieldBuilder(idx).(*array.ListBuilder), file.EqualityFieldIDs())
	idx++
	appendEntriesOptionalInt32Ptr(bldr.FieldBuilder(idx).(*array.Int32Builder), file.SortOrderID())
	idx++
	appendEntriesOptionalInt64Ptr(bldr.FieldBuilder(idx).(*array.Int64Builder), file.FirstRowID())
	idx++
	appendEntriesOptionalString(bldr.FieldBuilder(idx).(*array.StringBuilder), file.ReferencedDataFile())
	idx++
	appendEntriesOptionalInt64Ptr(bldr.FieldBuilder(idx).(*array.Int64Builder), file.ContentOffset())
	idx++
	appendEntriesOptionalInt64Ptr(bldr.FieldBuilder(idx).(*array.Int64Builder), file.ContentSizeInBytes())

	return nil
}

func appendEntriesPartition(builder *array.StructBuilder, typ *iceberg.StructType, values map[int]any) error {
	arrowType := builder.Type().(*arrow.StructType)
	builder.Append(true)
	for idx, field := range typ.FieldList {
		value := values[field.ID]
		if value == nil {
			builder.FieldBuilder(idx).AppendNull()

			continue
		}
		valueScalar, err := inspectEntriesValueScalar(value, field.Type, arrowType.Field(idx).Type)
		if err != nil {
			return fmt.Errorf("partition field %q: %w", field.Name, err)
		}
		if err := scalar.Append(builder.FieldBuilder(idx), valueScalar); err != nil {
			return err
		}
	}

	return nil
}

func inspectEntriesValueScalar(value any, typ iceberg.Type, arrowType arrow.DataType) (scalar.Scalar, error) {
	switch typ.(type) {
	case iceberg.DateType:
		switch value := value.(type) {
		case iceberg.Date:
			return scalar.NewDate32Scalar(arrow.Date32(value)), nil
		case int32:
			return scalar.NewDate32Scalar(arrow.Date32(value)), nil
		}
	case iceberg.TimeType:
		if value, ok := value.(iceberg.Time); ok {
			return scalar.NewTime64Scalar(arrow.Time64(value), arrowType), nil
		}
	case iceberg.TimestampType, iceberg.TimestampTzType:
		if value, ok := value.(iceberg.Timestamp); ok {
			return scalar.NewTimestampScalar(arrow.Timestamp(value), arrowType), nil
		}
	case iceberg.TimestampNsType, iceberg.TimestampTzNsType:
		if value, ok := value.(iceberg.TimestampNano); ok {
			return scalar.NewTimestampScalar(arrow.Timestamp(value), arrowType), nil
		}
	case iceberg.UUIDType:
		if value, ok := value.(uuid.UUID); ok {
			return scalar.MakeScalarParam(value[:], arrowType)
		}
	}

	return scalar.MakeScalarParam(value, arrowType)
}

func appendEntriesInt64Map(builder *array.MapBuilder, values map[int]int64) {
	if values == nil {
		builder.AppendNull()

		return
	}
	builder.Append(true)
	keys := builder.KeyBuilder().(*array.Int32Builder)
	items := builder.ItemBuilder().(*array.Int64Builder)
	ids := make([]int, 0, len(values))
	for id := range values {
		ids = append(ids, id)
	}
	sort.Ints(ids)
	for _, id := range ids {
		keys.Append(int32(id))
		items.Append(values[id])
	}
}

func appendEntriesBinaryMap(builder *array.MapBuilder, values map[int][]byte) {
	if values == nil {
		builder.AppendNull()

		return
	}
	builder.Append(true)
	keys := builder.KeyBuilder().(*array.Int32Builder)
	items := builder.ItemBuilder().(*array.BinaryBuilder)
	ids := make([]int, 0, len(values))
	for id := range values {
		ids = append(ids, id)
	}
	sort.Ints(ids)
	for _, id := range ids {
		keys.Append(int32(id))
		items.Append(values[id])
	}
}

func appendEntriesBytes(builder array.Builder, value []byte) {
	if value == nil {
		builder.AppendNull()

		return
	}
	builder.(*array.BinaryBuilder).Append(value)
}

func appendEntriesInt64List(builder *array.ListBuilder, values []int64) {
	if values == nil {
		builder.AppendNull()

		return
	}
	builder.Append(true)
	builder.ValueBuilder().(*array.Int64Builder).AppendValues(values, nil)
}

func appendEntriesInt32List(builder *array.ListBuilder, values []int) {
	if values == nil {
		builder.AppendNull()

		return
	}
	builder.Append(true)
	items := builder.ValueBuilder().(*array.Int32Builder)
	for _, value := range values {
		items.Append(int32(value))
	}
}

func appendEntriesOptionalInt64(builder *array.Int64Builder, value int64) {
	if value < 0 {
		builder.AppendNull()

		return
	}
	builder.Append(value)
}

func appendEntriesOptionalInt64Ptr(builder *array.Int64Builder, value *int64) {
	if value == nil {
		builder.AppendNull()

		return
	}
	builder.Append(*value)
}

func appendEntriesOptionalInt32Ptr(builder *array.Int32Builder, value *int) {
	if value == nil {
		builder.AppendNull()

		return
	}
	builder.Append(int32(*value))
}

func appendEntriesOptionalString(builder *array.StringBuilder, value *string) {
	if value == nil {
		builder.AppendNull()

		return
	}
	builder.Append(*value)
}
