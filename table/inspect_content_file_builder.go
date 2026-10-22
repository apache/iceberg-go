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
	"fmt"
	"math"
	"sort"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/iceberg-go"
)

func appendContentFileRecord(bldr *array.RecordBuilder, partitionType *iceberg.StructType, file iceberg.DataFile) error {
	contentFileBuilder, err := newInspectContentFileBuilder(bldr, partitionType)
	if err != nil {
		return err
	}

	return contentFileBuilder.append(file)
}

func newInspectContentFileAppender(partitionType *iceberg.StructType) func(*array.RecordBuilder, iceberg.DataFile) error {
	var current *array.RecordBuilder
	var contentFileBuilder inspectContentFileBuilder
	var bindErr error

	return func(bldr *array.RecordBuilder, file iceberg.DataFile) error {
		if bldr != current {
			contentFileBuilder, bindErr = newInspectContentFileBuilder(bldr, partitionType)
			current = bldr
		}
		if bindErr != nil {
			return bindErr
		}

		return contentFileBuilder.append(file)
	}
}

type inspectContentFileBuilder struct {
	content            *array.Int32Builder
	filePath           *array.StringBuilder
	fileFormat         *array.StringBuilder
	specID             *array.Int32Builder
	partition          *inspectPartitionBuilder
	recordCount        *array.Int64Builder
	fileSize           *array.Int64Builder
	columnSizes        *array.MapBuilder
	valueCounts        *array.MapBuilder
	nullValueCounts    *array.MapBuilder
	nanValueCounts     *array.MapBuilder
	lowerBounds        *array.MapBuilder
	upperBounds        *array.MapBuilder
	keyMetadata        *array.BinaryBuilder
	splitOffsets       *array.ListBuilder
	equalityIDs        *array.ListBuilder
	sortOrderID        *array.Int32Builder
	firstRowID         *array.Int64Builder
	referencedDataFile *array.StringBuilder
	contentOffset      *array.Int64Builder
	contentSize        *array.Int64Builder
	// One sorted key-set per statistics column, scoped to this record builder.
	// Most files in a table use the same metric field IDs. A separate cache
	// prevents unlike columns from evicting each other's key order.
	columnSizeFieldIDs []int
	valueCountFieldIDs []int
	nullCountFieldIDs  []int
	nanCountFieldIDs   []int
	lowerBoundFieldIDs []int
	upperBoundFieldIDs []int
}

func newInspectContentFileBuilder(bldr *array.RecordBuilder, partitionType *iceberg.StructType) (inspectContentFileBuilder, error) {
	return newInspectContentFileBuilderFromFields(bldr.Schema().Fields(), bldr.Field, partitionType)
}

func newInspectContentFileStructBuilder(bldr *array.StructBuilder, partitionType *iceberg.StructType) (inspectContentFileBuilder, error) {
	structType, ok := bldr.Type().(*arrow.StructType)
	if !ok {
		return inspectContentFileBuilder{}, fmt.Errorf("content-file builder has type %T, want struct", bldr.Type())
	}

	return newInspectContentFileBuilderFromFields(structType.Fields(), bldr.FieldBuilder, partitionType)
}

func newInspectContentFileBuilderFromFields(
	fields []arrow.Field,
	builder func(int) array.Builder,
	partitionType *iceberg.StructType,
) (inspectContentFileBuilder, error) {
	lookup, err := newInspectBuilderLookup("content-file", fields, builder)
	if err != nil {
		return inspectContentFileBuilder{}, err
	}
	if err := validateInspectBuilderLookup("content-file", lookup, inspectContentFileFieldIDs(partitionType)); err != nil {
		return inspectContentFileBuilder{}, err
	}

	var out inspectContentFileBuilder
	if out.content, err = inspectBuilderAs[*array.Int32Builder](lookup, inspectContentFieldIDContent, "content"); err != nil {
		return out, err
	}
	if out.filePath, err = inspectBuilderAs[*array.StringBuilder](lookup, inspectContentFieldIDFilePath, "file_path"); err != nil {
		return out, err
	}
	if out.fileFormat, err = inspectBuilderAs[*array.StringBuilder](lookup, inspectContentFieldIDFileFormat, "file_format"); err != nil {
		return out, err
	}
	if out.specID, err = inspectBuilderAs[*array.Int32Builder](lookup, inspectContentFieldIDSpecID, "spec_id"); err != nil {
		return out, err
	}
	if partitionType != nil && len(partitionType.FieldList) > 0 {
		partitionBuilder, bindErr := inspectBuilderAs[*array.StructBuilder](lookup, inspectContentFieldIDPartition, "partition")
		if bindErr != nil {
			return out, bindErr
		}
		if out.partition, err = newInspectPartitionBuilder(partitionBuilder, partitionType); err != nil {
			return out, err
		}
	}
	if out.recordCount, err = inspectBuilderAs[*array.Int64Builder](lookup, inspectContentFieldIDRecordCount, "record_count"); err != nil {
		return out, err
	}
	if out.fileSize, err = inspectBuilderAs[*array.Int64Builder](lookup, inspectContentFieldIDFileSize, "file_size_in_bytes"); err != nil {
		return out, err
	}
	if out.columnSizes, err = inspectBuilderAs[*array.MapBuilder](lookup, inspectContentFieldIDColumnSizes, "column_sizes"); err != nil {
		return out, err
	}
	if out.valueCounts, err = inspectBuilderAs[*array.MapBuilder](lookup, inspectContentFieldIDValueCounts, "value_counts"); err != nil {
		return out, err
	}
	if out.nullValueCounts, err = inspectBuilderAs[*array.MapBuilder](lookup, inspectContentFieldIDNullValueCounts, "null_value_counts"); err != nil {
		return out, err
	}
	if out.nanValueCounts, err = inspectBuilderAs[*array.MapBuilder](lookup, inspectContentFieldIDNaNValueCounts, "nan_value_counts"); err != nil {
		return out, err
	}
	if out.lowerBounds, err = inspectBuilderAs[*array.MapBuilder](lookup, inspectContentFieldIDLowerBounds, "lower_bounds"); err != nil {
		return out, err
	}
	if out.upperBounds, err = inspectBuilderAs[*array.MapBuilder](lookup, inspectContentFieldIDUpperBounds, "upper_bounds"); err != nil {
		return out, err
	}
	if out.keyMetadata, err = inspectBuilderAs[*array.BinaryBuilder](lookup, inspectContentFieldIDKeyMetadata, "key_metadata"); err != nil {
		return out, err
	}
	if out.splitOffsets, err = inspectBuilderAs[*array.ListBuilder](lookup, inspectContentFieldIDSplitOffsets, "split_offsets"); err != nil {
		return out, err
	}
	if out.equalityIDs, err = inspectBuilderAs[*array.ListBuilder](lookup, inspectContentFieldIDEqualityIDs, "equality_ids"); err != nil {
		return out, err
	}
	if out.sortOrderID, err = inspectBuilderAs[*array.Int32Builder](lookup, inspectContentFieldIDSortOrderID, "sort_order_id"); err != nil {
		return out, err
	}
	if out.firstRowID, err = inspectBuilderAs[*array.Int64Builder](lookup, inspectContentFieldIDFirstRowID, "first_row_id"); err != nil {
		return out, err
	}
	if out.referencedDataFile, err = inspectBuilderAs[*array.StringBuilder](lookup, inspectContentFieldIDReferencedDataFile, "referenced_data_file"); err != nil {
		return out, err
	}
	if out.contentOffset, err = inspectBuilderAs[*array.Int64Builder](lookup, inspectContentFieldIDContentOffset, "content_offset"); err != nil {
		return out, err
	}
	if out.contentSize, err = inspectBuilderAs[*array.Int64Builder](lookup, inspectContentFieldIDContentSize, "content_size_in_bytes"); err != nil {
		return out, err
	}

	return out, nil
}

func (b *inspectContentFileBuilder) append(file iceberg.DataFile) error {
	b.content.Append(int32(file.ContentType()))
	b.filePath.Append(file.FilePath())
	b.fileFormat.Append(string(file.FileFormat()))
	b.specID.Append(file.SpecID())

	if b.partition != nil {
		if err := b.partition.append(dataFilePartition(file)); err != nil {
			return err
		}
	}

	b.recordCount.Append(file.Count())
	b.fileSize.Append(file.FileSizeBytes())
	columnSizes, keyMetadata, splitOffsets, equalityFieldIDs := dataFileCollections(file)
	if err := appendInspectInt64Map(b.columnSizes, columnSizes, &b.columnSizeFieldIDs); err != nil {
		return err
	}
	valueCounts, nullCounts, nanCounts, lowerBounds, upperBounds := dataFileStats(file)
	if err := appendInspectInt64Map(b.valueCounts, valueCounts, &b.valueCountFieldIDs); err != nil {
		return err
	}
	if err := appendInspectInt64Map(b.nullValueCounts, nullCounts, &b.nullCountFieldIDs); err != nil {
		return err
	}
	if err := appendInspectInt64Map(b.nanValueCounts, nanCounts, &b.nanCountFieldIDs); err != nil {
		return err
	}
	if err := appendInspectBinaryMap(b.lowerBounds, lowerBounds, &b.lowerBoundFieldIDs); err != nil {
		return err
	}
	if err := appendInspectBinaryMap(b.upperBounds, upperBounds, &b.upperBoundFieldIDs); err != nil {
		return err
	}
	appendInspectBytes(b.keyMetadata, keyMetadata)
	if err := appendInspectInt64List(b.splitOffsets, splitOffsets); err != nil {
		return err
	}
	if err := appendInspectInt32List(b.equalityIDs, equalityFieldIDs); err != nil {
		return err
	}
	if err := appendInspectOptionalInt32(b.sortOrderID, file.SortOrderID()); err != nil {
		return err
	}
	appendInspectOptionalInt64(b.firstRowID, file.FirstRowID())
	appendInspectOptionalString(b.referencedDataFile, file.ReferencedDataFile())
	appendInspectOptionalInt64(b.contentOffset, file.ContentOffset())
	appendInspectOptionalInt64(b.contentSize, file.ContentSizeInBytes())

	return nil
}

func inspectContentFileFieldIDs(partitionType *iceberg.StructType) map[int]struct{} {
	ids := map[int]struct{}{
		inspectContentFieldIDContent:            {},
		inspectContentFieldIDFilePath:           {},
		inspectContentFieldIDFileFormat:         {},
		inspectContentFieldIDSpecID:             {},
		inspectContentFieldIDRecordCount:        {},
		inspectContentFieldIDFileSize:           {},
		inspectContentFieldIDColumnSizes:        {},
		inspectContentFieldIDValueCounts:        {},
		inspectContentFieldIDNullValueCounts:    {},
		inspectContentFieldIDNaNValueCounts:     {},
		inspectContentFieldIDLowerBounds:        {},
		inspectContentFieldIDUpperBounds:        {},
		inspectContentFieldIDKeyMetadata:        {},
		inspectContentFieldIDSplitOffsets:       {},
		inspectContentFieldIDEqualityIDs:        {},
		inspectContentFieldIDSortOrderID:        {},
		inspectContentFieldIDFirstRowID:         {},
		inspectContentFieldIDReferencedDataFile: {},
		inspectContentFieldIDContentOffset:      {},
		inspectContentFieldIDContentSize:        {},
	}
	if partitionType != nil && len(partitionType.FieldList) > 0 {
		ids[inspectContentFieldIDPartition] = struct{}{}
	}

	return ids
}

func newInspectBuilderLookup(scope string, fields []arrow.Field, builder func(int) array.Builder) (map[int]array.Builder, error) {
	lookup := make(map[int]array.Builder, len(fields))
	for idx, field := range fields {
		id := getFieldID(field)
		if id == nil {
			return nil, fmt.Errorf("%s field %q is missing a valid field ID", scope, field.Name)
		}
		if _, exists := lookup[*id]; exists {
			return nil, fmt.Errorf("%s schema contains duplicate field ID %d", scope, *id)
		}
		lookup[*id] = builder(idx)
	}

	return lookup, nil
}

func validateInspectBuilderLookup(scope string, lookup map[int]array.Builder, expected map[int]struct{}) error {
	if len(lookup) != len(expected) {
		return fmt.Errorf("%s schema has %d fields, want %d", scope, len(lookup), len(expected))
	}
	for id := range lookup {
		if _, ok := expected[id]; !ok {
			return fmt.Errorf("%s schema contains unexpected field ID %d", scope, id)
		}
	}
	for id := range expected {
		if _, ok := lookup[id]; !ok {
			return fmt.Errorf("%s schema is missing field ID %d", scope, id)
		}
	}

	return nil
}

func inspectBuilderAs[T any](lookup map[int]array.Builder, id int, name string) (T, error) {
	var zero T
	builder, ok := lookup[id]
	if !ok {
		return zero, fmt.Errorf("content-file schema is missing field %q (ID %d)", name, id)
	}
	typed, ok := builder.(T)
	if !ok {
		return zero, fmt.Errorf("content-file field %q (ID %d) has builder type %T", name, id, builder)
	}

	return typed, nil
}

func inspectInt32Value(value int, name string) (int32, error) {
	if value < math.MinInt32 || value > math.MaxInt32 {
		return 0, fmt.Errorf("%s %d does not fit in int32", name, value)
	}

	return int32(value), nil
}

// cachedInspectFieldIDs returns the sorted map keys, reusing the previous
// ordering when the next file has exactly the same key set. Length alone is
// not sufficient: maps with different IDs can have the same number of keys.
// Keeping only one sorted set makes memory bounded by the widest map seen.
func cachedInspectFieldIDs[V any](values map[int]V, cached *[]int) []int {
	ids := *cached
	if len(ids) == len(values) {
		sameKeys := true
		for _, id := range ids {
			if _, ok := values[id]; !ok {
				sameKeys = false

				break
			}
		}
		if sameKeys {
			return ids
		}
	}

	ids = ids[:0]
	for id := range values {
		ids = append(ids, id)
	}
	sort.Ints(ids)
	*cached = ids

	return ids
}

func appendInspectInt64Map(builder *array.MapBuilder, values map[int]int64, scratch *[]int) error {
	if values == nil {
		builder.AppendNull()

		return nil
	}
	builder.Append(true)
	keys, ok := builder.KeyBuilder().(*array.Int32Builder)
	if !ok {
		return fmt.Errorf("map key builder has type %T, want int32", builder.KeyBuilder())
	}
	items, ok := builder.ItemBuilder().(*array.Int64Builder)
	if !ok {
		return fmt.Errorf("map item builder has type %T, want int64", builder.ItemBuilder())
	}
	ids := cachedInspectFieldIDs(values, scratch)
	for _, id := range ids {
		key, err := inspectInt32Value(id, "field ID")
		if err != nil {
			return err
		}
		keys.Append(key)
		items.Append(values[id])
	}

	return nil
}

func appendInspectBinaryMap(builder *array.MapBuilder, values map[int][]byte, scratch *[]int) error {
	if values == nil {
		builder.AppendNull()

		return nil
	}
	builder.Append(true)
	keys, ok := builder.KeyBuilder().(*array.Int32Builder)
	if !ok {
		return fmt.Errorf("map key builder has type %T, want int32", builder.KeyBuilder())
	}
	items, ok := builder.ItemBuilder().(*array.BinaryBuilder)
	if !ok {
		return fmt.Errorf("map item builder has type %T, want binary", builder.ItemBuilder())
	}
	ids := cachedInspectFieldIDs(values, scratch)
	for _, id := range ids {
		key, err := inspectInt32Value(id, "field ID")
		if err != nil {
			return err
		}
		keys.Append(key)
		items.Append(values[id])
	}

	return nil
}

func appendInspectBytes(builder *array.BinaryBuilder, value []byte) {
	if value == nil {
		builder.AppendNull()

		return
	}
	builder.Append(value)
}

func appendInspectInt64List(builder *array.ListBuilder, values []int64) error {
	if values == nil {
		builder.AppendNull()

		return nil
	}
	builder.Append(true)
	items, ok := builder.ValueBuilder().(*array.Int64Builder)
	if !ok {
		return fmt.Errorf("list item builder has type %T, want int64", builder.ValueBuilder())
	}
	items.AppendValues(values, nil)

	return nil
}

func appendInspectInt32List(builder *array.ListBuilder, values []int) error {
	if values == nil {
		builder.AppendNull()

		return nil
	}
	builder.Append(true)
	items, ok := builder.ValueBuilder().(*array.Int32Builder)
	if !ok {
		return fmt.Errorf("list item builder has type %T, want int32", builder.ValueBuilder())
	}
	for _, value := range values {
		converted, err := inspectInt32Value(value, "equality field ID")
		if err != nil {
			return err
		}
		items.Append(converted)
	}

	return nil
}

func appendInspectOptionalInt32(builder *array.Int32Builder, value *int) error {
	if value == nil {
		builder.AppendNull()

		return nil
	}
	converted, err := inspectInt32Value(*value, "sort order ID")
	if err != nil {
		return err
	}
	builder.Append(converted)

	return nil
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
