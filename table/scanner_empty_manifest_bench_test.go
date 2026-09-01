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
	"github.com/stretchr/testify/require"
)

var skipEmptyManifestsBenchmarkSink int

type emptyManifestBenchmarkCase struct {
	name               string
	manifestCount      int
	entriesPerManifest int
	liveEvery          int
}

func BenchmarkPlanFilesSkipsKnownEmptyManifests(b *testing.B) {
	for _, tc := range []emptyManifestBenchmarkCase{
		{name: "manifests=256/all-empty", manifestCount: 256, entriesPerManifest: 64},
		{name: "manifests=256/10pct-live", manifestCount: 256, entriesPerManifest: 64, liveEvery: 10},
	} {
		b.Run(tc.name, func(b *testing.B) {
			tbl, fs, manifestPaths, expectedTasks := newEmptyManifestBenchmarkTable(b, tc)
			scan := tbl.Scan(WithMaxConcurrency(1))

			b.ReportAllocs()
			b.ResetTimer()
			for b.Loop() {
				tasks, err := scan.PlanFiles(context.Background())
				if err != nil {
					b.Fatal(err)
				}
				if len(tasks) != expectedTasks {
					b.Fatalf("PlanFiles returned %d tasks, want %d", len(tasks), expectedTasks)
				}
				skipEmptyManifestsBenchmarkSink = len(tasks)
			}
			b.StopTimer()

			manifestOpens := 0
			for _, path := range manifestPaths {
				manifestOpens += fs.openCount[path]
			}
			b.ReportMetric(float64(manifestOpens)/float64(b.N), "manifest-opens/op")
			b.ReportMetric(float64(expectedTasks), "tasks/op")
		})
	}
}

func newEmptyManifestBenchmarkTable(
	b testing.TB,
	tc emptyManifestBenchmarkCase,
) (*Table, *trackingCallsIO, []string, int) {
	b.Helper()

	const (
		tableLocation = "mem://empty-manifest-benchmark"
		snapshotID    = int64(1)
		sequenceNum   = int64(1)
	)

	fs := newTrackingCallsIO()
	schema := simpleSchema()
	spec := iceberg.NewPartitionSpec()
	manifests := make([]iceberg.ManifestFile, 0, tc.manifestCount)
	manifestPaths := make([]string, 0, tc.manifestCount)
	expectedTasks := 0

	for i := range tc.manifestCount {
		manifestPath := fmt.Sprintf("%s/metadata/manifest-%d.avro", tableLocation, i)
		dataPath := fmt.Sprintf("%s/data-%d.parquet", tableLocation, i)
		live := tc.liveEvery > 0 && i%tc.liveEvery == 0
		manifest := writeEmptyManifestBenchmarkManifest(
			b, fs, schema, spec, snapshotID, sequenceNum, manifestPath, dataPath,
			tc.entriesPerManifest, live,
		)
		manifests = append(manifests, manifest)
		manifestPaths = append(manifestPaths, manifestPath)
		if live {
			expectedTasks += tc.entriesPerManifest
		}
	}

	manifestListPath := tableLocation + "/metadata/snap-1.avro"
	var listBuf bytes.Buffer
	require.NoError(b, iceberg.WriteManifestList(2, &listBuf, snapshotID, nil, ptr(sequenceNum), 0, manifests))
	require.NoError(b, fs.WriteFile(manifestListPath, listBuf.Bytes()))

	meta, err := NewMetadata(schema, &spec, UnsortedSortOrder, tableLocation, nil)
	require.NoError(b, err)
	builder, err := MetadataBuilderFromBase(meta, "")
	require.NoError(b, err)
	schemaID := meta.CurrentSchema().ID
	require.NoError(b, builder.AddSnapshot(&Snapshot{
		SnapshotID:     snapshotID,
		SequenceNumber: sequenceNum,
		TimestampMs:    meta.LastUpdatedMillis() + 1,
		ManifestList:   manifestListPath,
		Summary:        &Summary{Operation: OpAppend},
		SchemaID:       &schemaID,
	}))
	require.NoError(b, builder.SetSnapshotRef(MainBranch, snapshotID, BranchRef))
	built, err := builder.Build()
	require.NoError(b, err)

	return New(Identifier{"db", "empty-manifest-benchmark"}, built, tableLocation+"/metadata/metadata.json", testFSF(fs), nil), fs, manifestPaths, expectedTasks
}

func writeEmptyManifestBenchmarkManifest(
	b testing.TB,
	fs *trackingCallsIO,
	schema *iceberg.Schema,
	spec iceberg.PartitionSpec,
	snapshotID, sequenceNum int64,
	manifestPath, dataPath string,
	entryCount int,
	live bool,
) iceberg.ManifestFile {
	b.Helper()

	entries := make([]iceberg.ManifestEntry, entryCount)
	status := iceberg.EntryStatusDELETED
	if live {
		status = iceberg.EntryStatusADDED
	}
	for i := range entries {
		dataFile, err := iceberg.NewDataFileBuilder(
			spec, iceberg.EntryContentData, dataPath, iceberg.ParquetFile,
			nil, nil, nil, 1, 1024,
		)
		require.NoError(b, err)
		entries[i] = iceberg.NewManifestEntryBuilder(status, &snapshotID, dataFile.Build()).
			SequenceNum(sequenceNum).
			Build()
	}

	var manifestBuf bytes.Buffer
	manifest, err := iceberg.WriteManifest(manifestPath, &manifestBuf, 2, spec, schema, snapshotID, entries)
	require.NoError(b, err)
	require.NoError(b, fs.WriteFile(manifestPath, manifestBuf.Bytes()))

	return manifest
}
