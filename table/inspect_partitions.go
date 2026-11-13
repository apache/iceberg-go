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
	"strconv"
	"strings"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/scalar"
	"github.com/apache/iceberg-go"
	iceio "github.com/apache/iceberg-go/io"
	"golang.org/x/sync/errgroup"
)

// Partitions returns one row per live partition, aggregating data and delete
// files from the current snapshot. Files from evolved specs are coerced into
// the table-wide partition type before grouping.
func (i InspectTable) Partitions(ctx context.Context) (array.RecordReader, error) {
	partitionType, err := unifiedPartitionType(i.tbl.Metadata())
	if err != nil {
		return nil, fmt.Errorf("inspect partitions: %w", err)
	}
	schema := PartitionsSchema(partitionType)
	arrowSchema, err := SchemaToArrowSchema(schema, nil, true, false)
	if err != nil {
		return nil, fmt.Errorf("inspect partitions: build arrow schema: %w", err)
	}

	aggregates, err := i.partitionAggregates(ctx, partitionType)
	if err != nil {
		return nil, fmt.Errorf("inspect partitions: %w", err)
	}

	return i.partitionAggregateReader(ctx, arrowSchema, partitionType, aggregates), nil
}

// partitionAggregateReader encodes at most recordBatchSize partitions per
// Arrow batch. Aggregate discovery is eager, but the Arrow buffers are bounded
// and released as each record is consumed.
func (i InspectTable) partitionAggregateReader(
	ctx context.Context,
	arrowSchema *arrow.Schema,
	partitionType *iceberg.StructType,
	aggregates []*partitionAggregate,
) array.RecordReader {
	return array.ReaderFromIter(arrowSchema, func(yield func(arrow.RecordBatch, error) bool) {
		bldr := array.NewRecordBuilder(i.alloc, arrowSchema)
		defer bldr.Release()

		for index, aggregate := range aggregates {
			if index&1023 == 0 {
				if err := ctx.Err(); err != nil {
					_ = yield(nil, err)

					return
				}
			}
			if err := appendPartitionAggregate(bldr, partitionType, *aggregate); err != nil {
				_ = yield(nil, fmt.Errorf("inspect partitions: append partition: %w", err))

				return
			}
			if (index+1)%recordBatchSize == 0 {
				if !yield(bldr.NewRecordBatch(), nil) {
					return
				}
			}
		}

		// Match the prior single-batch API for empty tables, but do not emit
		// an extra empty batch after an exact multiple of the batch size.
		if len(aggregates) == 0 || len(aggregates)%recordBatchSize != 0 {
			_ = yield(bldr.NewRecordBatch(), nil)
		}
	})
}

// inspectPartitionManifestConcurrency bounds concurrent manifest reads and
// per-manifest partition aggregation state.
const inspectPartitionManifestConcurrency = 8

func (i InspectTable) partitionAggregates(ctx context.Context, partitionType *iceberg.StructType) ([]*partitionAggregate, error) {
	snapshot := i.tbl.Metadata().CurrentSnapshot()
	if snapshot == nil {
		return nil, nil
	}
	provider := i.tbl.ManifestProvider()
	manifests, err := provider.Manifests(ctx, *snapshot)
	if err != nil {
		return nil, err
	}
	fs, err := provider.FS(ctx)
	if err != nil {
		return nil, err
	}

	snapshots := i.tbl.Metadata().Snapshots()
	snapshotTimes := make(map[int64]int64, len(snapshots))
	for _, snapshot := range snapshots {
		snapshotTimes[snapshot.SnapshotID] = snapshot.TimestampMs
	}

	aggregates := make([]*partitionAggregate, 0)
	aggregateTree := newPartitionAggregateTree()

	// Buffer only a bounded number of manifests. Workers group their entries
	// independently, then their results are merged in manifest order.
	// This keeps equal-timestamp tie-breaking deterministic.
	processBatch := func(batch []iceberg.ManifestFile) error {
		if len(batch) == 1 {
			return i.foldInspectPartitionManifest(ctx, fs, batch[0], partitionType,
				snapshotTimes, aggregateTree, &aggregates)
		}

		partial := make([][]*partitionAggregate, len(batch))
		g, gctx := errgroup.WithContext(ctx)
		for index, manifest := range batch {
			g.Go(func() error {
				localTree := newPartitionAggregateTree()

				return i.foldInspectPartitionManifest(gctx, fs, manifest, partitionType,
					snapshotTimes, localTree, &partial[index])
			})
		}
		if err := g.Wait(); err != nil {
			return err
		}

		merged := 0
		for _, manifestAggregates := range partial {
			for _, local := range manifestAggregates {
				// A manifest can contain many distinct partitions. Remain
				// responsive to cancellation during the ordered merge, too.
				if merged&1023 == 0 {
					if err := ctx.Err(); err != nil {
						return err
					}
				}
				merged++
				aggregate := aggregateTree.lookupPartition(local.partition, partitionType)
				if aggregate == nil {
					aggregates = append(aggregates, local)
					aggregateTree.insert(newPartitionRecord(local.partition, partitionType), local)

					continue
				}
				mergeInspectPartitionAggregate(aggregate, local)
			}
		}

		return nil
	}

	batch := make([]iceberg.ManifestFile, 0, inspectPartitionManifestConcurrency)
	for manifest := range manifests {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		batch = append(batch, manifest)
		if len(batch) < inspectPartitionManifestConcurrency {
			continue
		}
		if err := processBatch(batch); err != nil {
			return nil, err
		}
		batch = batch[:0]
	}
	if len(batch) > 0 {
		if err := processBatch(batch); err != nil {
			return nil, err
		}
	}

	sort.Slice(aggregates, func(left, right int) bool {
		return aggregates[left].orderingKey < aggregates[right].orderingKey
	})

	return aggregates, nil
}

// foldInspectPartitionManifest performs the original partition accounting using
// a caller-owned tree. Each concurrent worker owns its own tree and result slice.
func (i InspectTable) foldInspectPartitionManifest(
	ctx context.Context,
	fs iceio.IO,
	manifest iceberg.ManifestFile,
	partitionType *iceberg.StructType,
	snapshotTimes map[int64]int64,
	aggregateTree *partitionAggregateTree,
	aggregates *[]*partitionAggregate,
) error {
	// Avoid opening remote manifest files after the caller has canceled.
	if err := ctx.Err(); err != nil {
		return err
	}

	entryIndex := 0
	for entry, err := range manifest.Entries(ctx, fs, true) {
		if err != nil {
			return err
		}
		if entryIndex&1023 == 0 {
			if err := ctx.Err(); err != nil {
				return err
			}
		}
		entryIndex++

		file := entry.DataFile()
		partitionValues := dataFilePartition(file)
		aggregate := aggregateTree.lookupPartition(partitionValues, partitionType)
		if aggregate == nil {
			partition := coercePartition(partitionValues, partitionType)
			record := newPartitionRecord(partition, partitionType)
			aggregate = &partitionAggregate{
				partition:   clonePartition(partition),
				orderingKey: inspectPartitionKey(partition),
			}
			*aggregates = append(*aggregates, aggregate)
			aggregateTree.insert(record, aggregate)
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
			i.updatePartitionTimestamp(aggregate, snapshotTimes, entry.SnapshotID(), file.SpecID())
		}
	}

	return ctx.Err()
}

func mergeInspectPartitionAggregate(dst, src *partitionAggregate) {
	dst.dataRecordCount += src.dataRecordCount
	dst.dataFileCount += src.dataFileCount
	dst.dataFileSize += src.dataFileSize
	dst.positionDeleteCount += src.positionDeleteCount
	dst.positionDeleteFiles += src.positionDeleteFiles
	dst.equalityDeleteCount += src.equalityDeleteCount
	dst.equalityDeleteFiles += src.equalityDeleteFiles

	if src.lastUpdatedAt != nil &&
		(dst.lastUpdatedAt == nil || *src.lastUpdatedAt > *dst.lastUpdatedAt) {
		dst.lastUpdatedAt = src.lastUpdatedAt
		dst.lastUpdatedSnapshot = src.lastUpdatedSnapshot
		dst.specID = src.specID
	}
}

func (i InspectTable) updatePartitionTimestamp(
	aggregate *partitionAggregate,
	snapshotTimes map[int64]int64,
	snapshotID int64,
	specID int32,
) {
	if snapshotID < 0 {
		return
	}
	timestampMs, ok := snapshotTimes[snapshotID]
	if !ok {
		return
	}
	timestamp := timestampMs * 1000
	if aggregate.lastUpdatedAt != nil && timestamp <= *aggregate.lastUpdatedAt {
		return
	}
	aggregate.lastUpdatedAt = &timestamp
	id := snapshotID
	aggregate.lastUpdatedSnapshot = &id
	aggregate.specID = specID
}

func inspectPartitionKey(partition map[int]any) string {
	ids := make([]int, 0, len(partition))
	for id := range partition {
		ids = append(ids, id)
	}
	sort.Ints(ids)

	var key strings.Builder
	key.Grow(len(ids) * 24)
	var number [32]byte
	for _, id := range ids {
		key.Write(strconv.AppendInt(number[:0], int64(id), 10))
		key.WriteByte(':')

		value := partition[id]
		switch v := value.(type) {
		case int32:
			key.WriteString("int32:")
			key.Write(strconv.AppendInt(number[:0], int64(v), 10))
		case int64:
			key.WriteString("int64:")
			key.Write(strconv.AppendInt(number[:0], v, 10))
		case string:
			key.WriteString("string:")
			key.WriteString(v)
		case bool:
			key.WriteString("bool:")
			if v {
				key.WriteString("true")
			} else {
				key.WriteString("false")
			}
		case []byte:
			// Match fmt's []uint8:[0 1 ...] representation, including nil slices.
			key.WriteString("[]uint8:[")
			for i, byt := range v {
				if i > 0 {
					key.WriteByte(' ')
				}
				key.Write(strconv.AppendInt(number[:0], int64(byt), 10))
			}
			key.WriteByte(']')
		default:
			// Preserve formatting for named Iceberg types and uncommon values.
			fmt.Fprintf(&key, "%T:%v", value, value)
		}
		key.WriteByte(';')
	}

	return key.String()
}

// PartitionsSchema returns the partition metadata-table schema. For an
// unpartitioned table the partition and spec_id columns are omitted, matching
// Java's PartitionsTable behavior.
func PartitionsSchema(partitionType *iceberg.StructType) *iceberg.Schema {
	if partitionType == nil || partitionType.NumFields() == 0 {
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

func appendPartitionAggregate(bldr *array.RecordBuilder, partitionType *iceberg.StructType, aggregate partitionAggregate) error {
	idx := 0
	if partitionType != nil && partitionType.NumFields() > 0 {
		partition := bldr.Field(idx).(*array.StructBuilder)
		arrowType := partition.Type().(*arrow.StructType)
		partition.Append(true)
		for fieldIndex := range partitionType.NumFields() {
			field := partitionType.Field(fieldIndex)
			value := aggregate.partition[field.ID]
			if value == nil {
				partition.FieldBuilder(fieldIndex).AppendNull()

				continue
			}
			valueScalar, err := partitionValueScalar(value, field.Type, arrowType.Field(fieldIndex).Type)
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
