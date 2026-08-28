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

var rowLimitPlanningBenchmarkSink int

func BenchmarkPlanFilesWithRowLimit(b *testing.B) {
	for _, manifestCount := range []int{100, 1000, 10000} {
		b.Run(fmt.Sprintf("manifests=%d/no-limit", manifestCount), func(b *testing.B) {
			tbl := newRowLimitPlanningBenchmarkTable(b, manifestCount)
			scan := tbl.Scan(WithMaxConcurrency(1))

			b.ReportAllocs()
			b.ResetTimer()
			for range b.N {
				tasks, err := scan.PlanFiles(context.Background())
				if err != nil {
					b.Fatal(err)
				}
				rowLimitPlanningBenchmarkSink = len(tasks)
			}
		})

		b.Run(fmt.Sprintf("manifests=%d/limit-1", manifestCount), func(b *testing.B) {
			tbl := newRowLimitPlanningBenchmarkTable(b, manifestCount)
			scan := tbl.Scan(WithMaxConcurrency(1)).UseRowLimit(1)

			b.ReportAllocs()
			b.ResetTimer()
			for range b.N {
				tasks, err := scan.PlanFiles(context.Background())
				if err != nil {
					b.Fatal(err)
				}
				rowLimitPlanningBenchmarkSink = len(tasks)
			}
		})
	}
}

func newRowLimitPlanningBenchmarkTable(b testing.TB, manifestCount int) *Table {
	b.Helper()

	fs := newTrackingIO()
	schema := simpleSchema()
	const tableLocation = "mem://row-limit-benchmark"

	meta, err := NewMetadata(schema, iceberg.UnpartitionedSpec, UnsortedSortOrder, tableLocation, nil)
	require.NoError(b, err)
	builder, err := MetadataBuilderFromBase(meta, "")
	require.NoError(b, err)

	const snapshotID = int64(1)
	manifests := make([]iceberg.ManifestFile, 0, manifestCount)
	for i := range manifestCount {
		manifestPath := fmt.Sprintf("%s/metadata/manifest-%d.avro", tableLocation, i)
		dataPath := fmt.Sprintf("%s/data-%d.parquet", tableLocation, i)
		manifests = append(manifests, writeRowLimitPlanningBenchmarkManifest(
			b, fs, schema, snapshotID, 1, manifestPath, dataPath))
	}

	manifestListPath := tableLocation + "/metadata/snap-1.avro"
	var listBuf bytes.Buffer
	sequenceNumber := int64(1)
	require.NoError(b, iceberg.WriteManifestList(2, &listBuf, snapshotID, nil, &sequenceNumber, 0, manifests))
	require.NoError(b, fs.WriteFile(manifestListPath, listBuf.Bytes()))

	schemaID := meta.CurrentSchema().ID
	require.NoError(b, builder.AddSnapshot(&Snapshot{
		SnapshotID:     snapshotID,
		SequenceNumber: 1,
		TimestampMs:    meta.LastUpdatedMillis() + 1,
		ManifestList:   manifestListPath,
		Summary:        &Summary{Operation: OpAppend},
		SchemaID:       &schemaID,
	}))
	require.NoError(b, builder.SetSnapshotRef(MainBranch, snapshotID, BranchRef))
	built, err := builder.Build()
	require.NoError(b, err)

	return New(Identifier{"db", "row-limit-benchmark"}, built, tableLocation+"/metadata/metadata.json", testFSF(fs), nil)
}

func writeRowLimitPlanningBenchmarkManifest(
	b testing.TB,
	fs *trackingIO,
	schema *iceberg.Schema,
	snapshotID, sequenceNumber int64,
	manifestPath, dataPath string,
) iceberg.ManifestFile {
	b.Helper()

	spec := iceberg.NewPartitionSpec()
	dataFile, err := iceberg.NewDataFileBuilder(
		spec, iceberg.EntryContentData, dataPath, iceberg.ParquetFile,
		nil, nil, nil, 1, 1024,
	)
	require.NoError(b, err)

	entry := iceberg.NewManifestEntryBuilder(iceberg.EntryStatusADDED, &snapshotID, dataFile.Build()).
		SequenceNum(sequenceNumber).
		Build()

	var manifestBuf bytes.Buffer
	_, err = iceberg.WriteManifest(manifestPath, &manifestBuf, 2, spec, schema, snapshotID,
		[]iceberg.ManifestEntry{entry})
	require.NoError(b, err)
	require.NoError(b, fs.WriteFile(manifestPath, manifestBuf.Bytes()))

	return iceberg.NewManifestFile(2, manifestPath, int64(manifestBuf.Len()), 0, snapshotID).
		SequenceNum(sequenceNumber, sequenceNumber).
		AddedFiles(1).
		AddedRows(1).
		Build()
}
