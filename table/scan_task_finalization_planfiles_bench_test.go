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

const planFilesFinalizationBenchmarkManifestCount = 8
const planFilesFinalizationBenchmarkFilesPerManifest = 2_500

var planFilesFinalizationBenchmarkSink int

// BenchmarkPlanFilesTaskFinalization measures the effect of scan-task
// finalization in the context of local PlanFiles. The benchmark fixture is
// shared by the pre-change and parallel implementations, so copy this file to
// the PR base worktree when comparing the two revisions.
func BenchmarkPlanFilesTaskFinalization(b *testing.B) {
	tbl := newPlanFilesFinalizationBenchmarkTable(b)
	fileCount := planFilesFinalizationBenchmarkManifestCount * planFilesFinalizationBenchmarkFilesPerManifest

	for _, tc := range []struct {
		name      string
		rowFilter iceberg.BooleanExpression
	}{
		{name: "plain", rowFilter: iceberg.AlwaysTrue{}},
		{name: "identity_residual", rowFilter: iceberg.EqualTo(iceberg.Reference("id"), int32(7))},
	} {
		b.Run(tc.name, func(b *testing.B) {
			scan := tbl.Scan(WithMaxConcurrency(8), WithRowFilter(tc.rowFilter))
			b.ReportAllocs()
			b.ReportMetric(float64(fileCount), "files/op")
			for b.Loop() {
				tasks, err := scan.PlanFiles(context.Background())
				if err != nil {
					b.Fatal(err)
				}
				if len(tasks) != fileCount {
					b.Fatalf("PlanFiles returned %d tasks, want %d", len(tasks), fileCount)
				}
				planFilesFinalizationBenchmarkSink = len(tasks)
			}
		})
	}
}

func newPlanFilesFinalizationBenchmarkTable(b testing.TB) *Table {
	b.Helper()

	const (
		tableLocation = "mem://plan-files-finalization-benchmark"
		snapshotID    = int64(1)
		sequenceNum   = int64(1)
	)

	fs := newTrackingIO()
	schema := simpleSchema()
	spec := partitionedSpec()
	meta, err := NewMetadata(schema, &spec, UnsortedSortOrder, tableLocation, nil)
	if err != nil {
		b.Fatal(err)
	}
	builder, err := MetadataBuilderFromBase(meta, "")
	if err != nil {
		b.Fatal(err)
	}

	manifests := make([]iceberg.ManifestFile, planFilesFinalizationBenchmarkManifestCount)
	for manifestIndex := range manifests {
		entries := make([]iceberg.ManifestEntry, planFilesFinalizationBenchmarkFilesPerManifest)
		for entryIndex := range entries {
			dataPath := fmt.Sprintf("%s/data-%d-%d.parquet", tableLocation, manifestIndex, entryIndex)
			fileBuilder, err := iceberg.NewDataFileBuilder(
				spec,
				iceberg.EntryContentData,
				dataPath,
				iceberg.ParquetFile,
				map[int]any{1000: int32(7)},
				nil,
				nil,
				1,
				2048,
			)
			if err != nil {
				b.Fatal(err)
			}
			entries[entryIndex] = iceberg.NewManifestEntryBuilder(
				iceberg.EntryStatusADDED,
				&snapshotID,
				fileBuilder.Build(),
			).SequenceNum(sequenceNum).Build()
		}

		manifestPath := fmt.Sprintf("%s/metadata/manifest-%d.avro", tableLocation, manifestIndex)
		var manifestBuffer bytes.Buffer
		_, err := iceberg.WriteManifest(manifestPath, &manifestBuffer, 2, spec, schema, snapshotID, entries)
		if err != nil {
			b.Fatal(err)
		}
		if err := fs.WriteFile(manifestPath, manifestBuffer.Bytes()); err != nil {
			b.Fatal(err)
		}
		manifests[manifestIndex] = iceberg.NewManifestFile(
			2, manifestPath, int64(manifestBuffer.Len()), int32(spec.ID()), snapshotID,
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
