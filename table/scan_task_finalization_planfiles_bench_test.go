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
	"bytes"
	"context"
	"fmt"
	"testing"

	"github.com/apache/iceberg-go"
)

const (
	planFilesFinalizationBenchmarkManifestCount    = 8
	planFilesFinalizationBenchmarkFilesPerManifest = 2_500
)

var planFilesFinalizationBenchmarkSink int

// BenchmarkPlanFilesTaskFinalization exercises local planning over multiple
// partition specs and partition values, with an optional mixture of split and
// unsplit Parquet tasks. It uses in-memory FileIO and no delete manifests,
// so this is not a full measure of remote IO or delete-index build cost.
// Copy this fixture unchanged to the PR base worktree for before/after runs.
func BenchmarkPlanFilesTaskFinalization(b *testing.B) {
	const fileCount = planFilesFinalizationBenchmarkManifestCount * planFilesFinalizationBenchmarkFilesPerManifest

	// Four manifests are unpartitioned. Among the four identity-partitioned
	// manifests, one in four files has id=7. In the split case one in four
	// files expands into four tasks.
	for _, tc := range []struct {
		name          string
		rowFilter     iceberg.BooleanExpression
		splitOffsets  bool
		expectedTasks int
	}{
		{name: "plain", rowFilter: iceberg.AlwaysTrue{}, expectedTasks: fileCount},
		{name: "identity_residual", rowFilter: iceberg.EqualTo(iceberg.Reference("id"), int32(7)), expectedTasks: fileCount * 5 / 8},
		{name: "split_offsets", rowFilter: iceberg.AlwaysTrue{}, splitOffsets: true, expectedTasks: fileCount * 7 / 4},
	} {
		b.Run(tc.name, func(b *testing.B) {
			tbl := newPlanFilesFinalizationBenchmarkTable(b, tc.splitOffsets)
			scan := tbl.Scan(WithMaxConcurrency(8), WithRowFilter(tc.rowFilter))
			b.ReportAllocs()
			b.ReportMetric(float64(fileCount), "files/op")
			for b.Loop() {
				tasks, err := scan.PlanFiles(context.Background())
				if err != nil {
					b.Fatal(err)
				}
				if len(tasks) != tc.expectedTasks {
					b.Fatalf("PlanFiles returned %d tasks for %s, want %d", len(tasks), tc.name, tc.expectedTasks)
				}
				b.ReportMetric(float64(len(tasks)), "tasks/op")
				planFilesFinalizationBenchmarkSink = len(tasks)
			}
		})
	}
}

func newPlanFilesFinalizationBenchmarkTable(b testing.TB, splitOffsets bool) *Table {
	b.Helper()

	const (
		tableLocation = "mem://plan-files-finalization-benchmark"
		sequenceNum   = int64(1)
	)
	snapshotID := int64(1)

	fs := newTrackingIO()
	schema := simpleSchema()
	spec := partitionedSpec()
	properties := iceberg.Properties{}
	if splitOffsets {
		properties[ReadSplitTargetSizeKey] = "512"
	}
	meta, err := NewMetadata(schema, &spec, UnsortedSortOrder, tableLocation, properties)
	if err != nil {
		b.Fatal(err)
	}
	builder, err := MetadataBuilderFromBase(meta, "")
	if err != nil {
		b.Fatal(err)
	}
	// Alternate the identity partition and unpartitioned specs to exercise
	// residual evaluator lookup and cross-spec manifest planning.
	unpartitioned := iceberg.NewPartitionSpecID(1)
	requireNoErrorBenchmark(b, builder.AddPartitionSpec(&unpartitioned, false))

	manifests := make([]iceberg.ManifestFile, planFilesFinalizationBenchmarkManifestCount)
	for manifestIndex := range manifests {
		entries := make([]iceberg.ManifestEntry, planFilesFinalizationBenchmarkFilesPerManifest)
		manifestSpec := spec
		partition := map[int]any(nil)
		if manifestIndex%2 == 1 {
			manifestSpec = unpartitioned
		}
		for entryIndex := range entries {
			dataPath := fmt.Sprintf("%s/data-%d-%d.parquet", tableLocation, manifestIndex, entryIndex)
			if manifestIndex%2 == 0 {
				partition = map[int]any{1000: int32(6 + entryIndex%4)}
			}
			fileBuilder, err := iceberg.NewDataFileBuilder(
				manifestSpec,
				iceberg.EntryContentData,
				dataPath,
				iceberg.ParquetFile,
				partition,
				nil,
				nil,
				1,
				2048,
			)
			if err != nil {
				b.Fatal(err)
			}
			if splitOffsets && entryIndex%4 == 0 {
				fileBuilder.SplitOffsets([]int64{0, 512, 1024, 1536})
			}
			entries[entryIndex] = iceberg.NewManifestEntryBuilder(
				iceberg.EntryStatusADDED,
				&snapshotID,
				fileBuilder.Build(),
			).SequenceNum(sequenceNum).Build()
		}

		manifestPath := fmt.Sprintf("%s/metadata/manifest-%d.avro", tableLocation, manifestIndex)
		var manifestBuffer bytes.Buffer
		_, err := iceberg.WriteManifest(manifestPath, &manifestBuffer, 2, manifestSpec, schema, snapshotID, entries)
		if err != nil {
			b.Fatal(err)
		}
		if err := fs.WriteFile(manifestPath, manifestBuffer.Bytes()); err != nil {
			b.Fatal(err)
		}
		manifests[manifestIndex] = iceberg.NewManifestBuilder(
			2, manifestPath, int64(manifestBuffer.Len()), int32(manifestSpec.ID()), snapshotID,
		).SequenceNum(sequenceNum, sequenceNum).
			AddedFiles(int32(planFilesFinalizationBenchmarkFilesPerManifest)).
			AddedRows(int64(planFilesFinalizationBenchmarkFilesPerManifest)).
			Build()
	}

	manifestListPath := tableLocation + "/metadata/snap-1.avro"
	var manifestListBuffer bytes.Buffer
	requireNoErrorBenchmark(b, iceberg.WriteManifestList(
		2, &manifestListBuffer, snapshotID, nil, new(sequenceNum), 0, manifests,
	))
	requireNoErrorBenchmark(b, fs.WriteFile(manifestListPath, manifestListBuffer.Bytes()))

	schemaID := meta.CurrentSchema().ID
	requireNoErrorBenchmark(b, builder.AddSnapshot(&Snapshot{
		SnapshotID:     snapshotID,
		SequenceNumber: sequenceNum,
		TimestampMs:    meta.LastUpdatedMillis() + 1,
		ManifestList:   manifestListPath,
		Summary:        &Summary{Operation: OpAppend},
		SchemaID:       &schemaID,
	}))
	requireNoErrorBenchmark(b, builder.SetSnapshotRef(MainBranch, snapshotID, BranchRef))
	metadata, err := builder.Build()
	if err != nil {
		b.Fatal(err)
	}

	return New(
		Identifier{"db", "plan-files-finalization-benchmark"},
		metadata,
		tableLocation+"/metadata/metadata.json",
		testFSF(fs),
		nil,
	)
}

func requireNoErrorBenchmark(b testing.TB, err error) {
	b.Helper()
	if err != nil {
		b.Fatal(err)
	}
}
