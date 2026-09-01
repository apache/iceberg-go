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
	"runtime"
	"testing"

	"github.com/apache/iceberg-go"
)

var planTasksBenchmarkSink int

func BenchmarkPlanDataManifestTasks(b *testing.B) {
	for _, workload := range []struct {
		manifestCount int
		entryCount    int
	}{
		{manifestCount: 8, entryCount: 1_000},
		{manifestCount: 32, entryCount: 1_000},
		{manifestCount: 8, entryCount: 10_000},
	} {
		b.Run(fmt.Sprintf("manifests=%d/entries=%d", workload.manifestCount, workload.entryCount), func(b *testing.B) {
			scan, schema, manifests, posDeleteIndex, dvIndex, eqDeleteIndex := newPlanTasksBenchmarkFixture(b, workload.manifestCount, workload.entryCount)

			for _, mode := range []struct {
				name string
				plan func(context.Context) ([]FileScanTask, error)
			}{
				{
					name: "buffered",
					plan: func(ctx context.Context) ([]FileScanTask, error) {
						return planBufferedDataManifestTasks(scan, ctx, manifests, schema,
							posDeleteIndex, dvIndex, eqDeleteIndex)
					},
				},
				{
					name: "streamed",
					plan: func(ctx context.Context) ([]FileScanTask, error) {
						return scan.planDataManifestTasks(ctx, manifests, schema,
							minSequenceNum(manifests), posDeleteIndex, dvIndex, eqDeleteIndex)
					},
				},
			} {
				b.Run(mode.name, func(b *testing.B) {
					b.ReportAllocs()
					b.ResetTimer()
					for b.Loop() {
						tasks, err := mode.plan(context.Background())
						if err != nil {
							b.Fatal(err)
						}
						planTasksBenchmarkSink = len(tasks)
						runtime.KeepAlive(tasks)
					}
				})
			}
			if planTasksBenchmarkSink != workload.manifestCount*workload.entryCount {
				b.Fatalf("planned %d tasks, want %d", planTasksBenchmarkSink,
					workload.manifestCount*workload.entryCount)
			}
		})
	}
}

func newPlanTasksBenchmarkFixture(
	b testing.TB,
	manifestCount, entryCount int,
) (*Scan, *iceberg.Schema, []iceberg.ManifestFile, *positionalDeleteIndex, map[string]iceberg.ManifestEntry, *equalityDeleteIndex) {
	b.Helper()

	fs := newTrackingIO()
	schema := simpleSchema()
	spec := iceberg.NewPartitionSpec()
	meta, err := NewMetadata(schema, &spec, UnsortedSortOrder, "mem://plan-tasks-benchmark", nil)
	if err != nil {
		b.Fatal(err)
	}

	snapshotID := int64(1)
	manifests := make([]iceberg.ManifestFile, manifestCount)
	for manifestIndex := range manifestCount {
		entries := make([]iceberg.ManifestEntry, entryCount)
		sequenceNumber := int64(manifestIndex + 1)
		for entryIndex := range entryCount {
			dataPath := fmt.Sprintf("mem://plan-tasks-benchmark/data-%d-%d.parquet", manifestIndex, entryIndex)
			dataFile, err := iceberg.NewDataFileBuilder(
				spec,
				iceberg.EntryContentData,
				dataPath,
				iceberg.ParquetFile,
				nil,
				nil,
				nil,
				1,
				1024,
			)
			if err != nil {
				b.Fatal(err)
			}
			entries[entryIndex] = iceberg.NewManifestEntryBuilder(
				iceberg.EntryStatusADDED,
				&snapshotID,
				dataFile.Build(),
			).SequenceNum(sequenceNumber).Build()
		}

		manifestPath := fmt.Sprintf("mem://plan-tasks-benchmark/metadata/manifest-%d.avro", manifestIndex)
		var manifestBuf bytes.Buffer
		_, err = iceberg.WriteManifest(manifestPath, &manifestBuf, 2, spec, schema, snapshotID, entries)
		if err != nil {
			b.Fatal(err)
		}
		if err := fs.WriteFile(manifestPath, manifestBuf.Bytes()); err != nil {
			b.Fatal(err)
		}
		manifests[manifestIndex] = iceberg.NewManifestFile(
			2, manifestPath, int64(manifestBuf.Len()), int32(spec.ID()), snapshotID,
		).
			SequenceNum(sequenceNumber, sequenceNumber).
			AddedFiles(int32(entryCount)).
			AddedRows(int64(entryCount)).
			Build()
	}

	posDeleteIndex, err := buildPositionalDeleteIndex(nil)
	if err != nil {
		b.Fatal(err)
	}
	dvIndex, err := buildDVIndex(nil)
	if err != nil {
		b.Fatal(err)
	}
	eqDeleteIndex, err := buildEqualityDeleteIndex(nil, meta, schema)
	if err != nil {
		b.Fatal(err)
	}

	scan := &Scan{
		metadata:      meta,
		rowFilter:     iceberg.AlwaysTrue{},
		caseSensitive: true,
		concurrency:   4,
		ioF:           testFSF(fs),
	}

	return scan, schema, manifests, posDeleteIndex, dvIndex, eqDeleteIndex
}

func planBufferedDataManifestTasks(
	scan *Scan,
	ctx context.Context,
	manifests []iceberg.ManifestFile,
	schema *iceberg.Schema,
	posDeleteIndex *positionalDeleteIndex,
	dvIndex map[string]iceberg.ManifestEntry,
	eqDeleteIndex *equalityDeleteIndex,
) ([]FileScanTask, error) {
	entries, err := scan.collectManifestEntriesWithSchema(
		ctx, manifests, schema, scan.partitionFiltersForSchema(schema))
	if err != nil {
		return nil, err
	}

	tasks := make([]FileScanTask, 0, len(entries.dataEntries))
	for _, entry := range entries.dataEntries {
		task, err := fileScanTaskForDataEntry(entry, posDeleteIndex, dvIndex, eqDeleteIndex)
		if err != nil {
			return nil, err
		}
		tasks = append(tasks, task)
	}

	return tasks, nil
}
