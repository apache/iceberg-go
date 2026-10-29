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
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/iceberg-go"
	"github.com/stretchr/testify/require"
)

func inspectPartitionBatchFixture(
	tb testing.TB,
	count int,
) (*iceberg.StructType, *arrow.Schema, []*inspectPartitionAggregate) {
	tb.Helper()

	partitionType := &iceberg.StructType{FieldList: []iceberg.NestedField{
		{ID: 1000, Name: "part", Type: iceberg.PrimitiveTypes.Int32, Required: true},
	}}
	arrowSchema, err := SchemaToArrowSchema(PartitionsSchema(partitionType), nil, true, false)
	if err != nil {
		tb.Fatal(err)
	}

	aggregates := make([]*inspectPartitionAggregate, count)
	for i := range aggregates {
		aggregates[i] = &inspectPartitionAggregate{
			partition:       map[int]any{1000: int32(i)},
			dataRecordCount: int64(i + 1),
			dataFileCount:   1,
		}
	}

	return partitionType, arrowSchema, aggregates
}

func TestInspectPartitionAggregateReaderBoundedBatches(t *testing.T) {
	for _, count := range []int{0, 1, inspectRecordBatchSize - 1, inspectRecordBatchSize,
		inspectRecordBatchSize + 1, 2*inspectRecordBatchSize + 1} {
		t.Run(fmt.Sprintf("partitions=%d", count), func(t *testing.T) {
			partitionType, arrowSchema, aggregates := inspectPartitionBatchFixture(t, count)
			mem := memory.NewCheckedAllocator(memory.DefaultAllocator)
			t.Cleanup(func() { mem.AssertSize(t, 0) })

			reader := (InspectTable{alloc: mem}).partitionAggregateReader(
				t.Context(), arrowSchema, partitionType, aggregates)
			defer reader.Release()

			seen := 0
			var batchSizes []int
			for reader.Next() {
				batch := reader.RecordBatch()
				batchSizes = append(batchSizes, int(batch.NumRows()))
				require.LessOrEqual(t, batch.NumRows(), int64(inspectRecordBatchSize))

				partition := batch.Column(0).(*array.Struct).Field(0).(*array.Int32)
				recordCount := batch.Column(2).(*array.Int64)
				for offset := range int(batch.NumRows()) {
					require.Equal(t, int32(seen), partition.Value(offset))
					require.Equal(t, int64(seen+1), recordCount.Value(offset))
					seen++
				}
			}
			require.NoError(t, reader.Err())
			require.Equal(t, count, seen)
			if count == 0 {
				require.Equal(t, []int{0}, batchSizes)
			} else {
				want := make([]int, 0, (count+inspectRecordBatchSize-1)/inspectRecordBatchSize)
				for remaining := count; remaining > 0; remaining -= inspectRecordBatchSize {
					want = append(want, min(remaining, inspectRecordBatchSize))
				}
				require.Equal(t, want, batchSizes)
			}
		})
	}
}

// Exercise the public metadata-table reader across a real manifest, including
// the boundary between two Arrow batches.
func TestInspectPartitionsAcrossRecordBatchBoundary(t *testing.T) {
	const count = inspectRecordBatchSize + 1

	spec := partitionedSpec()
	const snapshotID = int64(1)
	sequenceNumber := int64(1)
	entries := make([]iceberg.ManifestEntry, 0, count)
	for index := range count {
		dataFile := newTestDataFileWithCount(
			t, spec, fmt.Sprintf("mem://default/table-location/data/partition-%d.parquet", index),
			map[int]any{1000: int32(index)}, 1)
		entries = append(entries, iceberg.NewManifestEntry(
			iceberg.EntryStatusADDED, new(snapshotID), &sequenceNumber, &sequenceNumber, dataFile))
	}

	tbl := inspectFilesTableWithManifests(t, spec, inspectManifestSpec{
		content: iceberg.ManifestContentData,
		entries: entries,
	})
	reader, err := tbl.Inspect().Partitions(t.Context())
	require.NoError(t, err)
	defer reader.Release()

	seen := make(map[int32]bool, count)
	var previousKey string
	var batchSizes []int
	for reader.Next() {
		batch := reader.RecordBatch()
		batchSizes = append(batchSizes, int(batch.NumRows()))
		partitions := batch.Column(0).(*array.Struct).Field(0).(*array.Int32)
		records := batch.Column(2).(*array.Int64)
		for i := range int(batch.NumRows()) {
			value := partitions.Value(i)
			require.False(t, seen[value], "duplicate partition %d", value)
			seen[value] = true
			require.EqualValues(t, 1, records.Value(i))
			key := inspectPartitionKey(map[int]any{1000: value})
			if len(seen) > 1 && key <= previousKey {
				t.Fatalf("out-of-order partition key %q after %q", key, previousKey)
			}
			previousKey = key
		}
	}
	require.NoError(t, reader.Err())
	require.Len(t, seen, count)
	require.Equal(t, []int{inspectRecordBatchSize, 1}, batchSizes)
}

func TestInspectPartitionAggregateReaderCancellation(t *testing.T) {
	partitionType, arrowSchema, aggregates := inspectPartitionBatchFixture(
		t, 2*inspectRecordBatchSize)
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()

	reader := (InspectTable{alloc: memory.DefaultAllocator}).partitionAggregateReader(
		ctx, arrowSchema, partitionType, aggregates)
	defer reader.Release()

	require.True(t, reader.Next())
	require.EqualValues(t, inspectRecordBatchSize, reader.RecordBatch().NumRows())
	cancel()
	require.False(t, reader.Next())
	require.ErrorIs(t, reader.Err(), context.Canceled)
}

var inspectPartitionBatchBenchmarkSink int64

// BenchmarkInspectPartitionBatchEncoding compares the former eager, single
// batch encoder with the bounded streaming encoder on identical input rows.
func BenchmarkInspectPartitionBatchEncoding(b *testing.B) {
	for _, count := range []int{4096, 16384, 65536} {
		partitionType, arrowSchema, aggregates := inspectPartitionBatchFixture(b, count)
		for _, bounded := range []bool{false, true} {
			mode := "single_batch"
			if bounded {
				mode = "bounded"
			}
			b.Run(fmt.Sprintf("partitions=%d/%s", count, mode), func(b *testing.B) {
				inspector := InspectTable{alloc: memory.DefaultAllocator}
				b.ReportAllocs()
				b.ReportMetric(float64(count), "partitions/op")
				b.ResetTimer()
				var maxBatchRows int64
				for b.Loop() {
					var reader array.RecordReader
					if bounded {
						reader = inspector.partitionAggregateReader(
							context.Background(), arrowSchema, partitionType, aggregates)
					} else {
						builder := array.NewRecordBuilder(memory.DefaultAllocator, arrowSchema)
						for _, aggregate := range aggregates {
							if err := appendPartitionAggregate(builder, partitionType, *aggregate); err != nil {
								b.Fatal(err)
							}
						}
						var err error
						reader, err = singleBatchReader(arrowSchema, builder)
						builder.Release()
						if err != nil {
							b.Fatal(err)
						}
					}
					var seen int64
					for reader.Next() {
						rows := reader.RecordBatch().NumRows()
						seen += rows
						if rows > maxBatchRows {
							maxBatchRows = rows
						}
					}
					readErr := reader.Err()
					reader.Release()
					if readErr != nil {
						b.Fatal(readErr)
					}
					if seen != int64(count) {
						b.Fatalf("read %d partitions, want %d", seen, count)
					}
					inspectPartitionBatchBenchmarkSink = seen
				}
				b.ReportMetric(float64(maxBatchRows), "max_batch_rows")
			})
		}
	}
}
