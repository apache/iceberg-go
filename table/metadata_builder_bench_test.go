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
	"slices"
	"testing"

	"github.com/apache/iceberg-go"
	"github.com/google/uuid"
)

var (
	removeSnapshotsBenchmarkSink int
	metadataBuilderBenchmarkSink int
)

var removeSnapshotsBenchmarkCases = []struct {
	snapshotCount int
	removedCount  int
}{
	{snapshotCount: 1_000, removedCount: 1},
	{snapshotCount: 1_000, removedCount: 8},
	{snapshotCount: 1_000, removedCount: 32},
	{snapshotCount: 1_000, removedCount: 500},
	{snapshotCount: 10_000, removedCount: 1},
	{snapshotCount: 10_000, removedCount: 8},
	{snapshotCount: 10_000, removedCount: 64},
	{snapshotCount: 10_000, removedCount: 5_000},
}

func BenchmarkRemoveSnapshots(b *testing.B) {
	for _, tc := range removeSnapshotsBenchmarkCases {
		b.Run(fmt.Sprintf("snapshots=%d/removed=%d", tc.snapshotCount, tc.removedCount), func(b *testing.B) {
			snapshots := benchmarkSnapshots(tc.snapshotCount)
			log := benchmarkSnapshotLog(tc.snapshotCount)
			removed := benchmarkRemovedSnapshotIDs(tc.removedCount)

			builder := &MetadataBuilder{}
			b.ReportAllocs()
			b.ReportMetric(float64(tc.snapshotCount), "snapshot_entries")
			b.ReportMetric(float64(len(removed)), "removed_snapshots")
			b.ResetTimer()

			for range b.N {
				b.StopTimer()
				builder.snapshotList = slices.Clone(snapshots)
				builder.snapshotLog = slices.Clone(log)
				builder.updates = nil
				b.StartTimer()

				if err := builder.RemoveSnapshots(removed, false); err != nil {
					b.Fatal(err)
				}
				removeSnapshotsBenchmarkSink = len(builder.snapshotList) + len(builder.snapshotLog)
			}
		})
	}
}

func BenchmarkRemoveSnapshotsBuild(b *testing.B) {
	for _, tc := range removeSnapshotsBenchmarkCases {
		b.Run(fmt.Sprintf("snapshots=%d/removed=%d", tc.snapshotCount, tc.removedCount), func(b *testing.B) {
			template := benchmarkSnapshotBuilder(tc.snapshotCount)
			removed := benchmarkRemovedSnapshotIDs(tc.removedCount)

			b.ReportAllocs()
			b.ReportMetric(float64(tc.snapshotCount), "snapshot_entries")
			b.ReportMetric(float64(len(removed)), "removed_snapshots")
			b.ResetTimer()

			for range b.N {
				b.StopTimer()
				builder := template.clone()
				b.StartTimer()

				if err := builder.RemoveSnapshots(removed, false); err != nil {
					b.Fatal(err)
				}
				if _, err := builder.Build(); err != nil {
					b.StopTimer()
					b.Fatal(err)
				}

				b.StopTimer()
				removeSnapshotsBenchmarkSink = len(builder.snapshotLog)
				b.StartTimer()
			}
		})
	}
}

func BenchmarkMetadataBuilderCloneAndBuild(b *testing.B) {
	for _, snapshotCount := range []int{1_000, 10_000} {
		b.Run(fmt.Sprintf("snapshots=%d", snapshotCount), func(b *testing.B) {
			template := benchmarkSnapshotBuilder(snapshotCount)
			template.ensureSnapshotIndex()

			b.ReportAllocs()
			b.ReportMetric(float64(snapshotCount), "snapshot_entries")
			b.ResetTimer()

			for range b.N {
				builder := template.clone()
				if _, err := builder.Build(); err != nil {
					b.Fatal(err)
				}
				metadataBuilderBenchmarkSink = len(builder.snapshotIndex.positions)
			}
		})
	}
}

func BenchmarkMetadataBuilderFromBase(b *testing.B) {
	for _, snapshotCount := range []int{1_000, 10_000} {
		b.Run(fmt.Sprintf("snapshots=%d", snapshotCount), func(b *testing.B) {
			source := benchmarkSnapshotBuilder(snapshotCount)
			source.ensureSnapshotIndex()
			metadata, err := source.Build()
			if err != nil {
				b.Fatal(err)
			}

			b.ReportAllocs()
			b.ReportMetric(float64(snapshotCount), "snapshot_entries")
			b.ResetTimer()

			for range b.N {
				builder, err := MetadataBuilderFromBase(metadata, "")
				if err != nil {
					b.Fatal(err)
				}
				metadataBuilderBenchmarkSink = len(builder.snapshotIndex.positions)
			}
		})
	}
}

func BenchmarkMetadataBuilderFromBaseCollections(b *testing.B) {
	cases := []struct {
		name           string
		schemaCount    int
		specCount      int
		snapshotCount  int
		sortOrderCount int
		propertyCount  int
	}{
		{name: "schemas=1", schemaCount: 1, specCount: 1, snapshotCount: 1, sortOrderCount: 1},
		{name: "schemas=16", schemaCount: 16, specCount: 1, snapshotCount: 1, sortOrderCount: 1},
		{name: "schemas=128", schemaCount: 128, specCount: 1, snapshotCount: 1, sortOrderCount: 1},
		{name: "schemas=1024", schemaCount: 1024, specCount: 1, snapshotCount: 1, sortOrderCount: 1},
		{name: "partition-specs=16", schemaCount: 1, specCount: 16, snapshotCount: 1, sortOrderCount: 1},
		{name: "partition-specs=128", schemaCount: 1, specCount: 128, snapshotCount: 1, sortOrderCount: 1},
		{name: "snapshots=1000", schemaCount: 1, specCount: 1, snapshotCount: 1000, sortOrderCount: 1},
		{name: "sort-orders=32", schemaCount: 1, specCount: 1, snapshotCount: 1, sortOrderCount: 32},
		{name: "properties=32", schemaCount: 1, specCount: 1, snapshotCount: 1, sortOrderCount: 1, propertyCount: 32},
	}

	for _, tc := range cases {
		b.Run(tc.name, func(b *testing.B) {
			metadata := benchmarkMetadataBuilderBase(tc.schemaCount, tc.specCount, tc.snapshotCount, tc.sortOrderCount, tc.propertyCount)

			b.ReportAllocs()
			b.ReportMetric(float64(tc.schemaCount), "schemas")
			b.ReportMetric(float64(tc.specCount), "partition_specs")
			b.ReportMetric(float64(tc.snapshotCount), "snapshots")
			b.ReportMetric(float64(tc.sortOrderCount), "sort_orders")
			b.ReportMetric(float64(tc.propertyCount), "properties")
			b.ResetTimer()

			for range b.N {
				builder, err := MetadataBuilderFromBase(metadata, "")
				if err != nil {
					b.Fatal(err)
				}
				metadataBuilderBenchmarkSink = len(builder.schemaList) + len(builder.specs) +
					len(builder.snapshotList) + len(builder.sortOrderList) + len(builder.props)
			}
		})
	}
}

func benchmarkMetadataBuilderBase(schemaCount, specCount, snapshotCount, sortOrderCount, propertyCount int) Metadata {
	schemas := make([]*iceberg.Schema, schemaCount)
	schemas[0] = iceberg.NewSchema(0, iceberg.NestedField{
		ID:       1,
		Name:     "id",
		Type:     iceberg.PrimitiveTypes.Int64,
		Required: true,
	})
	for i := 1; i < schemaCount; i++ {
		schemas[i] = iceberg.NewSchema(i)
	}

	specs := make([]iceberg.PartitionSpec, specCount)
	for i := range specs {
		specs[i] = iceberg.NewPartitionSpecID(i)
	}

	sortOrders := make([]SortOrder, sortOrderCount)
	sortOrders[0] = UnsortedSortOrder
	for i := 1; i < sortOrderCount; i++ {
		order, err := NewSortOrder(i, []SortField{{
			SourceIDs: []int{1},
			Transform: iceberg.IdentityTransform{},
			Direction: SortASC,
			NullOrder: NullsFirst,
		}})
		if err != nil {
			panic(err)
		}
		sortOrders[i] = order
	}

	snapshots := benchmarkSnapshots(snapshotCount)
	currentSnapshotID := int64(snapshotCount - 1)
	lastPartitionID := 0
	properties := make(iceberg.Properties, propertyCount)
	for i := range propertyCount {
		properties[fmt.Sprintf("property-%d", i)] = "value"
	}

	return &metadataV2{
		LastSeqNum: int64(snapshotCount),
		commonMetadata: commonMetadata{
			FormatVersion:      2,
			UUID:               uuid.New(),
			Loc:                "s3://bucket/test/location",
			LastColumnId:       1,
			SchemaList:         schemas,
			CurrentSchemaID:    0,
			Specs:              specs,
			DefaultSpecID:      0,
			LastPartitionID:    &lastPartitionID,
			Props:              properties,
			SnapshotList:       snapshots,
			snapshotIndex:      buildSnapshotIndex(snapshots),
			CurrentSnapshotID:  &currentSnapshotID,
			SnapshotLog:        benchmarkSnapshotLog(snapshotCount),
			SortOrderList:      sortOrders,
			DefaultSortOrderID: 0,
			SnapshotRefs: map[string]SnapshotRef{
				MainBranch: {SnapshotID: currentSnapshotID, SnapshotRefType: BranchRef},
			},
		},
	}
}

func benchmarkSnapshotLog(snapshotCount int) []SnapshotLogEntry {
	log := make([]SnapshotLogEntry, snapshotCount)
	for i := range log {
		log[i] = SnapshotLogEntry{SnapshotID: int64(i), TimestampMs: int64(i)}
	}

	return log
}

func benchmarkRemovedSnapshotIDs(removedCount int) []int64 {
	removed := make([]int64, removedCount)
	for i := range removed {
		removed[i] = int64(i * 2)
	}

	return removed
}

func benchmarkSnapshotBuilder(snapshotCount int) *MetadataBuilder {
	builder := builderWithoutChanges(2)
	builder.snapshotList = benchmarkSnapshots(snapshotCount)
	builder.snapshotLog = benchmarkSnapshotLog(snapshotCount)
	currentID := int64(snapshotCount - 1)
	builder.currentSnapshotID = &currentID
	builder.refs = map[string]SnapshotRef{
		MainBranch: {SnapshotID: currentID, SnapshotRefType: BranchRef},
	}

	return &builder
}

func benchmarkSnapshots(snapshotCount int) []Snapshot {
	snapshots := make([]Snapshot, snapshotCount)
	for i := range snapshots {
		snapshots[i] = Snapshot{SnapshotID: int64(i), SequenceNumber: 0}
	}

	return snapshots
}
