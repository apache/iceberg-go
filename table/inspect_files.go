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

// DataFiles returns the live data files in the current snapshot. Deleted
// manifest entries are omitted, matching the data_files metadata table.
func (i InspectTable) DataFiles(ctx context.Context) (array.RecordReader, error) {
	partitionType := inspectPartitionType(i.tbl.metadata)
	schema := DataFilesSchema(partitionType)
	arrowSchema, err := SchemaToArrowSchema(schema, nil, true, false)
	if err != nil {
		return nil, fmt.Errorf("inspect data files: build arrow schema: %w", err)
	}

	files, err := i.currentContentFiles(ctx, iceberg.ManifestContentData)
	if err != nil {
		return nil, fmt.Errorf("inspect data files: %w", err)
	}

	bldr := array.NewRecordBuilder(i.alloc, arrowSchema)
	defer bldr.Release()
	for _, file := range files {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		if err := appendContentFileRecord(bldr, partitionType, file); err != nil {
			return nil, fmt.Errorf("inspect data files: append %s: %w", file.FilePath(), err)
		}
	}

	rr, err := singleBatchReader(arrowSchema, bldr)
	if err != nil {
		return nil, fmt.Errorf("inspect data files: %w", err)
	}

	return rr, nil
}

func (i InspectTable) currentContentFiles(ctx context.Context, content iceberg.ManifestContent) ([]iceberg.DataFile, error) {
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

	files := make([]iceberg.DataFile, 0)
	for _, manifest := range manifests {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		if manifest.ManifestContent() != content {
			continue
		}
		for entry, err := range manifest.Entries(fs, true) {
			if err != nil {
				return nil, err
			}
			if err := ctx.Err(); err != nil {
				return nil, err
			}
			files = append(files, entry.DataFile())
		}
	}

	return files, nil
}

// inspectPartitionType returns the table-wide partition type. It contains the
// union of partition fields from every spec, which lets metadata tables
// represent live files written before partition evolution.
func inspectPartitionType(metadata Metadata) *iceberg.StructType {
	currentSchema := metadata.CurrentSchema()
	specs := metadata.PartitionSpecs()
	sort.Slice(specs, func(left, right int) bool {
		return specs[left].ID() > specs[right].ID()
	})

	selected := make(map[int]iceberg.PartitionField)
	fieldsByID := make(map[int]iceberg.NestedField)
	for _, spec := range specs {
		partitionType := spec.PartitionType(currentSchema)
		for idx, field := range spec.Fields() {
			active := true
			for _, sourceID := range field.SourceIDs {
				if _, ok := currentSchema.FindTypeByID(sourceID); !ok {
					active = false

					break
				}
			}
			if !active || idx >= len(partitionType.FieldList) {
				continue
			}

			if previous, exists := selected[field.FieldID]; exists {
				// A v1 partition-field drop is represented by a void transform.
				// Keep the newest field name, but use the older non-void type when
				// that is the only concrete type available.
				if isInspectVoidTransform(previous.Transform) && !isInspectVoidTransform(field.Transform) {
					old := fieldsByID[field.FieldID]
					old.Type = partitionType.FieldList[idx].Type
					fieldsByID[field.FieldID] = old
				}

				continue
			}

			selected[field.FieldID] = field
			fieldsByID[field.FieldID] = iceberg.NestedField{
				ID:       field.FieldID,
				Name:     field.Name,
				Type:     partitionType.FieldList[idx].Type,
				Required: false,
			}
		}
	}

	fields := make([]iceberg.NestedField, 0, len(fieldsByID))
	for _, field := range fieldsByID {
		fields = append(fields, field)
	}
	sort.Slice(fields, func(left, right int) bool { return fields[left].ID < fields[right].ID })

	return &iceberg.StructType{FieldList: fields}
}

func isInspectVoidTransform(transform iceberg.Transform) bool {
	switch transform.(type) {
	case iceberg.VoidTransform, *iceberg.VoidTransform:
		return true
	default:
		return false
	}
}

// DataFilesSchema returns the common content-file schema used by the data_files
// and delete_files metadata tables. The partition field is omitted for an
// unpartitioned table, as required by the Iceberg metadata-table spec.
func DataFilesSchema(partitionType *iceberg.StructType) *iceberg.Schema {
	return iceberg.NewSchema(0, inspectContentFileFields(partitionType)...)
}

func inspectContentFileType(partitionType *iceberg.StructType) *iceberg.StructType {
	return &iceberg.StructType{FieldList: inspectContentFileFields(partitionType)}
}

func inspectContentFileFields(partitionType *iceberg.StructType) []iceberg.NestedField {
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
		iceberg.NestedField{ID: 108, Name: "column_sizes", Type: inspectInt64MapType(117, 118), Required: false},
		iceberg.NestedField{ID: 109, Name: "value_counts", Type: inspectInt64MapType(119, 120), Required: false},
		iceberg.NestedField{ID: 110, Name: "null_value_counts", Type: inspectInt64MapType(121, 122), Required: false},
		iceberg.NestedField{ID: 137, Name: "nan_value_counts", Type: inspectInt64MapType(138, 139), Required: false},
		iceberg.NestedField{ID: 125, Name: "lower_bounds", Type: inspectBinaryMapType(126, 127), Required: false},
		iceberg.NestedField{ID: 128, Name: "upper_bounds", Type: inspectBinaryMapType(129, 130), Required: false},
		iceberg.NestedField{ID: 131, Name: "key_metadata", Type: iceberg.PrimitiveTypes.Binary, Required: false},
		iceberg.NestedField{ID: 132, Name: "split_offsets", Type: &iceberg.ListType{ElementID: 133, Element: iceberg.PrimitiveTypes.Int64, ElementRequired: true}, Required: false},
		iceberg.NestedField{ID: 135, Name: "equality_ids", Type: &iceberg.ListType{ElementID: 136, Element: iceberg.PrimitiveTypes.Int32, ElementRequired: true}, Required: false},
		iceberg.NestedField{ID: 140, Name: "sort_order_id", Type: iceberg.PrimitiveTypes.Int32, Required: false},
		iceberg.NestedField{ID: 142, Name: "first_row_id", Type: iceberg.PrimitiveTypes.Int64, Required: false},
		iceberg.NestedField{ID: 143, Name: "referenced_data_file", Type: iceberg.PrimitiveTypes.String, Required: false},
		iceberg.NestedField{ID: 144, Name: "content_offset", Type: iceberg.PrimitiveTypes.Int64, Required: false},
		iceberg.NestedField{ID: 145, Name: "content_size_in_bytes", Type: iceberg.PrimitiveTypes.Int64, Required: false},
	)

	return fields
}

func inspectInt64MapType(keyID, valueID int) *iceberg.MapType {
	return &iceberg.MapType{KeyID: keyID, KeyType: iceberg.PrimitiveTypes.Int32, ValueID: valueID, ValueType: iceberg.PrimitiveTypes.Int64, ValueRequired: true}
}

func inspectBinaryMapType(keyID, valueID int) *iceberg.MapType {
	return &iceberg.MapType{KeyID: keyID, KeyType: iceberg.PrimitiveTypes.Int32, ValueID: valueID, ValueType: iceberg.PrimitiveTypes.Binary, ValueRequired: true}
}

func appendContentFileRecord(bldr *array.RecordBuilder, partitionType *iceberg.StructType, file iceberg.DataFile) error {
	return appendContentFileFields(bldr.Field, partitionType, file)
}

func appendContentFile(builder *array.StructBuilder, partitionType *iceberg.StructType, file iceberg.DataFile) error {
	builder.Append(true)

	return appendContentFileFields(builder.FieldBuilder, partitionType, file)
}

func appendContentFileFields(fieldBuilder func(int) array.Builder, partitionType *iceberg.StructType, file iceberg.DataFile) error {
	idx := 0
	fieldBuilder(idx).(*array.Int32Builder).Append(int32(file.ContentType()))
	idx++
	fieldBuilder(idx).(*array.StringBuilder).Append(file.FilePath())
	idx++
	fieldBuilder(idx).(*array.StringBuilder).Append(string(file.FileFormat()))
	idx++
	fieldBuilder(idx).(*array.Int32Builder).Append(file.SpecID())
	idx++

	if partitionType != nil && len(partitionType.FieldList) > 0 {
		partition := fieldBuilder(idx).(*array.StructBuilder)
		if err := appendInspectPartition(partition, partitionType, file.Partition()); err != nil {
			return err
		}
		idx++
	}

	fieldBuilder(idx).(*array.Int64Builder).Append(file.Count())
	idx++
	fieldBuilder(idx).(*array.Int64Builder).Append(file.FileSizeBytes())
	idx++
	appendInspectInt64Map(fieldBuilder(idx).(*array.MapBuilder), file.ColumnSizes())
	idx++
	appendInspectInt64Map(fieldBuilder(idx).(*array.MapBuilder), file.ValueCounts())
	idx++
	appendInspectInt64Map(fieldBuilder(idx).(*array.MapBuilder), file.NullValueCounts())
	idx++
	appendInspectInt64Map(fieldBuilder(idx).(*array.MapBuilder), file.NaNValueCounts())
	idx++
	appendInspectBinaryMap(fieldBuilder(idx).(*array.MapBuilder), file.LowerBoundValues())
	idx++
	appendInspectBinaryMap(fieldBuilder(idx).(*array.MapBuilder), file.UpperBoundValues())
	idx++
	appendInspectBytes(fieldBuilder(idx), file.KeyMetadata())
	idx++
	appendInspectInt64List(fieldBuilder(idx).(*array.ListBuilder), file.SplitOffsets())
	idx++
	appendInspectInt32List(fieldBuilder(idx).(*array.ListBuilder), file.EqualityFieldIDs())
	idx++
	appendInspectOptionalInt32(fieldBuilder(idx).(*array.Int32Builder), file.SortOrderID())
	idx++
	appendInspectOptionalInt64(fieldBuilder(idx).(*array.Int64Builder), file.FirstRowID())
	idx++
	appendInspectOptionalString(fieldBuilder(idx).(*array.StringBuilder), file.ReferencedDataFile())
	idx++
	appendInspectOptionalInt64(fieldBuilder(idx).(*array.Int64Builder), file.ContentOffset())
	idx++
	appendInspectOptionalInt64(fieldBuilder(idx).(*array.Int64Builder), file.ContentSizeInBytes())

	return nil
}

func appendInspectPartition(builder *array.StructBuilder, partitionType *iceberg.StructType, values map[int]any) error {
	arrowType := builder.Type().(*arrow.StructType)
	builder.Append(true)
	for idx, field := range partitionType.FieldList {
		value := values[field.ID]
		if value == nil {
			builder.FieldBuilder(idx).AppendNull()

			continue
		}
		sc, err := inspectValueScalar(value, field.Type, arrowType.Field(idx).Type)
		if err != nil {
			return fmt.Errorf("partition field %q: %w", field.Name, err)
		}
		if err := scalar.Append(builder.FieldBuilder(idx), sc); err != nil {
			return err
		}
	}

	return nil
}

func inspectValueScalar(value any, typ iceberg.Type, arrowType arrow.DataType) (scalar.Scalar, error) {
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
	case iceberg.DecimalType:
		switch value := value.(type) {
		case iceberg.DecimalLiteral:
			return scalar.NewDecimal128Scalar(value.Val, arrowType), nil
		case iceberg.Decimal:
			return scalar.NewDecimal128Scalar(value.Val, arrowType), nil
		default:
			return nil, fmt.Errorf("unsupported decimal partition value %T", value)
		}
	}

	return scalar.MakeScalarParam(value, arrowType)
}

func appendInspectInt64Map(builder *array.MapBuilder, values map[int]int64) {
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

func appendInspectBinaryMap(builder *array.MapBuilder, values map[int][]byte) {
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

func appendInspectBytes(builder array.Builder, value []byte) {
	if value == nil {
		builder.AppendNull()

		return
	}
	builder.(*array.BinaryBuilder).Append(value)
}

func appendInspectInt64List(builder *array.ListBuilder, values []int64) {
	if values == nil {
		builder.AppendNull()

		return
	}
	builder.Append(true)
	builder.ValueBuilder().(*array.Int64Builder).AppendValues(values, nil)
}

func appendInspectInt32List(builder *array.ListBuilder, values []int) {
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

func appendInspectOptionalInt32(builder *array.Int32Builder, value *int) {
	if value == nil {
		builder.AppendNull()

		return
	}
	builder.Append(int32(*value))
}

func appendInspectOptionalInt64(builder *array.Int64Builder, value *int64) {
	if value == nil {
		builder.AppendNull()

		return
	}
	builder.Append(*value)
}

func appendInspectOptionalString(builder *array.StringBuilder, value *string) {
	if value == nil {
		builder.AppendNull()

		return
	}
	builder.Append(*value)
}
