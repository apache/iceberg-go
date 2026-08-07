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
	"fmt"
	"sort"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/scalar"
	"github.com/apache/iceberg-go"
	"github.com/google/uuid"
)

type inspectPartitionAggregate struct {
	specID              int32
	partition           map[int]any
	dataRecordCount     int64
	dataFileCount       int32
	dataFileSize        int64
	positionDeleteCount int64
	positionDeleteFiles int32
	equalityDeleteCount int64
	equalityDeleteFiles int32
	lastUpdatedAt       *int64
	lastUpdatedSnapshot *int64
	orderingKey         string
}

// Partitions returns one row per live partition, aggregating data and delete
// files from the current snapshot. Different partition specs remain separate
// groups so files from evolved specs are never combined by raw map equality.
func (i InspectTable) Partitions(ctx context.Context) (array.RecordReader, error) {
	spec := i.tbl.metadata.PartitionSpec()
	partitionType := spec.PartitionType(i.tbl.metadata.CurrentSchema())
	schema := PartitionsSchema(partitionType)
	arrowSchema, err := SchemaToArrowSchema(schema, nil, true, false)
	if err != nil {
		return nil, fmt.Errorf("inspect partitions: build arrow schema: %w", err)
	}

	aggregates, err := i.partitionAggregates(ctx)
	if err != nil {
		return nil, fmt.Errorf("inspect partitions: %w", err)
	}

	bldr := array.NewRecordBuilder(i.alloc, arrowSchema)
	defer bldr.Release()
	for _, aggregate := range aggregates {
		if err := appendPartitionAggregate(bldr, partitionType, aggregate); err != nil {
			return nil, fmt.Errorf("inspect partitions: append partition: %w", err)
		}
	}

	rr, err := singleBatchReader(arrowSchema, bldr)
	if err != nil {
		return nil, fmt.Errorf("inspect partitions: %w", err)
	}

	return rr, nil
}

func (i InspectTable) partitionAggregates(ctx context.Context) ([]inspectPartitionAggregate, error) {
	snapshot := i.tbl.metadata.CurrentSnapshot()
	if snapshot == nil {
		return nil, nil
	}
	if i.tbl.fsF == nil {
		return nil, fmt.Errorf("table file IO is not configured")
	}
	fs, err := i.tbl.fsF(ctx)
	if err != nil {
		return nil, err
	}
	manifests, err := snapshot.Manifests(fs)
	if err != nil {
		return nil, err
	}

	aggregates := make(map[string]*inspectPartitionAggregate)
	for _, manifest := range manifests {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		for entry, err := range manifest.Entries(fs, true) {
			if err != nil {
				return nil, err
			}
			file := entry.DataFile()
			partition := file.Partition()
			key := fmt.Sprintf("%d:%s", file.SpecID(), inspectPartitionKey(partition))
			aggregate := aggregates[key]
			if aggregate == nil {
				aggregate = &inspectPartitionAggregate{
					specID:      file.SpecID(),
					partition:   cloneInspectPartition(partition),
					orderingKey: key,
				}
				aggregates[key] = aggregate
			}

			switch file.ContentType() {
			case iceberg.EntryContentData:
				aggregate.dataRecordCount += file.Count()
				aggregate.dataFileCount++
				aggregate.dataFileSize += file.FileSizeBytes()
			case iceberg.EntryContentPosDeletes:
				aggregate.positionDeleteCount += file.Count()
				aggregate.positionDeleteFiles++
			case iceberg.EntryContentEqDeletes:
				aggregate.equalityDeleteCount += file.Count()
				aggregate.equalityDeleteFiles++
			}

			if file.ContentType() == iceberg.EntryContentData ||
				file.ContentType() == iceberg.EntryContentPosDeletes ||
				file.ContentType() == iceberg.EntryContentEqDeletes {
				i.updatePartitionTimestamp(aggregate, entry.SnapshotID())
			}
		}
	}

	result := make([]inspectPartitionAggregate, 0, len(aggregates))
	for _, aggregate := range aggregates {
		result = append(result, *aggregate)
	}
	sort.Slice(result, func(left, right int) bool { return result[left].orderingKey < result[right].orderingKey })

	return result, nil
}

func (i InspectTable) updatePartitionTimestamp(aggregate *inspectPartitionAggregate, snapshotID int64) {
	if snapshotID < 0 {
		return
	}
	snapshot := i.tbl.metadata.SnapshotByID(snapshotID)
	if snapshot == nil {
		return
	}
	timestamp := snapshot.TimestampMs * 1000
	if aggregate.lastUpdatedAt != nil && timestamp <= *aggregate.lastUpdatedAt {
		return
	}
	aggregate.lastUpdatedAt = &timestamp
	id := snapshot.SnapshotID
	aggregate.lastUpdatedSnapshot = &id
}

func inspectPartitionKey(partition map[int]any) string {
	ids := make([]int, 0, len(partition))
	for id := range partition {
		ids = append(ids, id)
	}
	sort.Ints(ids)
	key := ""
	for _, id := range ids {
		key += fmt.Sprintf("%d:%T:%v;", id, partition[id], partition[id])
	}
	return key
}

func cloneInspectPartition(partition map[int]any) map[int]any {
	if partition == nil {
		return nil
	}
	returnMap := make(map[int]any, len(partition))
	for id, value := range partition {
		returnMap[id] = value
	}
	return returnMap
}

// PartitionsSchema returns the partition metadata-table schema. For an
// unpartitioned table the partition and spec_id columns are omitted, matching
// Java's PartitionsTable behavior.
func PartitionsSchema(partitionType *iceberg.StructType) *iceberg.Schema {
	if partitionType == nil || len(partitionType.FieldList) == 0 {
		return iceberg.NewSchema(0,
			iceberg.NestedField{ID: 2, Name: "record_count", Type: iceberg.PrimitiveTypes.Int64, Required: true},
			iceberg.NestedField{ID: 3, Name: "file_count", Type: iceberg.PrimitiveTypes.Int32, Required: true},
			iceberg.NestedField{ID: 11, Name: "total_data_file_size_in_bytes", Type: iceberg.PrimitiveTypes.Int64, Required: true},
			iceberg.NestedField{ID: 5, Name: "position_delete_record_count", Type: iceberg.PrimitiveTypes.Int64, Required: true},
			iceberg.NestedField{ID: 6, Name: "position_delete_file_count", Type: iceberg.PrimitiveTypes.Int32, Required: true},
			iceberg.NestedField{ID: 7, Name: "equality_delete_record_count", Type: iceberg.PrimitiveTypes.Int64, Required: true},
			iceberg.NestedField{ID: 8, Name: "equality_delete_file_count", Type: iceberg.PrimitiveTypes.Int32, Required: true},
			iceberg.NestedField{ID: 9, Name: "last_updated_at", Type: iceberg.PrimitiveTypes.TimestampTz, Required: false},
			iceberg.NestedField{ID: 10, Name: "last_updated_snapshot_id", Type: iceberg.PrimitiveTypes.Int64, Required: false},
		)
	}

	return iceberg.NewSchema(0,
		iceberg.NestedField{ID: 1, Name: "partition", Type: partitionType, Required: true},
		iceberg.NestedField{ID: 4, Name: "spec_id", Type: iceberg.PrimitiveTypes.Int32, Required: true},
		iceberg.NestedField{ID: 2, Name: "record_count", Type: iceberg.PrimitiveTypes.Int64, Required: true},
		iceberg.NestedField{ID: 3, Name: "file_count", Type: iceberg.PrimitiveTypes.Int32, Required: true},
		iceberg.NestedField{ID: 11, Name: "total_data_file_size_in_bytes", Type: iceberg.PrimitiveTypes.Int64, Required: true},
		iceberg.NestedField{ID: 5, Name: "position_delete_record_count", Type: iceberg.PrimitiveTypes.Int64, Required: true},
		iceberg.NestedField{ID: 6, Name: "position_delete_file_count", Type: iceberg.PrimitiveTypes.Int32, Required: true},
		iceberg.NestedField{ID: 7, Name: "equality_delete_record_count", Type: iceberg.PrimitiveTypes.Int64, Required: true},
		iceberg.NestedField{ID: 8, Name: "equality_delete_file_count", Type: iceberg.PrimitiveTypes.Int32, Required: true},
		iceberg.NestedField{ID: 9, Name: "last_updated_at", Type: iceberg.PrimitiveTypes.TimestampTz, Required: false},
		iceberg.NestedField{ID: 10, Name: "last_updated_snapshot_id", Type: iceberg.PrimitiveTypes.Int64, Required: false},
	)
}

func appendPartitionAggregate(bldr *array.RecordBuilder, partitionType *iceberg.StructType, aggregate inspectPartitionAggregate) error {
	idx := 0
	if partitionType != nil && len(partitionType.FieldList) > 0 {
		partition := bldr.Field(idx).(*array.StructBuilder)
		arrowType := partition.Type().(*arrow.StructType)
		partition.Append(true)
		for fieldIndex, field := range partitionType.FieldList {
			value := aggregate.partition[field.ID]
			if value == nil {
				partition.FieldBuilder(fieldIndex).AppendNull()
				continue
			}
			valueScalar, err := inspectPartitionValueScalar(value, field.Type, arrowType.Field(fieldIndex).Type)
			if err != nil {
				return fmt.Errorf("partition field %q: %w", field.Name, err)
			}
			if err := scalar.Append(partition.FieldBuilder(fieldIndex), valueScalar); err != nil {
				return err
			}
		}
		idx++
		bldr.Field(idx).(*array.Int32Builder).Append(aggregate.specID)
		idx++
	}
	bldr.Field(idx).(*array.Int64Builder).Append(aggregate.dataRecordCount)
	idx++
	bldr.Field(idx).(*array.Int32Builder).Append(aggregate.dataFileCount)
	idx++
	bldr.Field(idx).(*array.Int64Builder).Append(aggregate.dataFileSize)
	idx++
	bldr.Field(idx).(*array.Int64Builder).Append(aggregate.positionDeleteCount)
	idx++
	bldr.Field(idx).(*array.Int32Builder).Append(aggregate.positionDeleteFiles)
	idx++
	bldr.Field(idx).(*array.Int64Builder).Append(aggregate.equalityDeleteCount)
	idx++
	bldr.Field(idx).(*array.Int32Builder).Append(aggregate.equalityDeleteFiles)
	idx++
	if aggregate.lastUpdatedAt == nil {
		bldr.Field(idx).(*array.TimestampBuilder).AppendNull()
	} else {
		bldr.Field(idx).(*array.TimestampBuilder).Append(arrow.Timestamp(*aggregate.lastUpdatedAt))
	}
	idx++
	if aggregate.lastUpdatedSnapshot == nil {
		bldr.Field(idx).(*array.Int64Builder).AppendNull()
	} else {
		bldr.Field(idx).(*array.Int64Builder).Append(*aggregate.lastUpdatedSnapshot)
	}

	return nil
}

func inspectPartitionValueScalar(value any, typ iceberg.Type, arrowType arrow.DataType) (scalar.Scalar, error) {
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
