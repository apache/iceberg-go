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
	"runtime"
	"testing"

	"github.com/apache/iceberg-go"
	"golang.org/x/sync/errgroup"
)

func BenchmarkManifestEntryCollection(b *testing.B) {
	benchmarkCases := []struct {
		manifestCount int
		concurrency   int
		entryCount    int
	}{
		{manifestCount: 8, concurrency: 1, entryCount: 1_000},
		{manifestCount: 8, concurrency: 4, entryCount: 1_000},
		{manifestCount: 8, concurrency: 16, entryCount: 1_000},
		{manifestCount: 64, concurrency: 1, entryCount: 1_000},
		{manifestCount: 64, concurrency: 4, entryCount: 1_000},
		{manifestCount: 64, concurrency: 16, entryCount: 1_000},
		{manifestCount: 8, concurrency: 1, entryCount: 10_000},
		{manifestCount: 8, concurrency: 4, entryCount: 10_000},
		{manifestCount: 8, concurrency: 16, entryCount: 10_000},
		{manifestCount: 16, concurrency: 1, entryCount: 10_000},
		{manifestCount: 16, concurrency: 4, entryCount: 10_000},
		{manifestCount: 16, concurrency: 16, entryCount: 10_000},
		{manifestCount: 64, concurrency: 1, entryCount: 10_000},
		{manifestCount: 64, concurrency: 4, entryCount: 10_000},
		{manifestCount: 64, concurrency: 16, entryCount: 10_000},
		{manifestCount: 8, concurrency: 4, entryCount: 100_000},
		{manifestCount: 8, concurrency: 16, entryCount: 100_000},
		{manifestCount: 16, concurrency: 4, entryCount: 100_000},
		{manifestCount: 16, concurrency: 16, entryCount: 100_000},
	}

	for _, workload := range []struct {
		name    string
		content iceberg.ManifestContent
	}{
		{name: "data", content: iceberg.ManifestContentData},
		{name: "deletes", content: iceberg.ManifestContentDeletes},
	} {
		for _, benchmarkCase := range benchmarkCases {
			name := fmt.Sprintf("content=%s/manifests=%d/concurrency=%d/entries=%d",
				workload.name, benchmarkCase.manifestCount, benchmarkCase.concurrency, benchmarkCase.entryCount)
			b.Run(name, func(b *testing.B) {
				entries := benchmarkManifestEntries(benchmarkCase.entryCount, workload.content)
				b.ReportAllocs()
				b.ResetTimer()
				b.ReportMetric(float64(benchmarkCase.manifestCount*benchmarkCase.entryCount), "entries/op")

				var result *manifestEntries
				for range b.N {
					result = collectManifestEntryBatches(entries, benchmarkCase.manifestCount, benchmarkCase.concurrency)
				}

				if got, want := len(result.dataEntries)+len(result.positionalDeleteEntries)+
					len(result.equalityDeleteEntries)+len(result.dvEntries), benchmarkCase.manifestCount*benchmarkCase.entryCount; got != want {
					b.Fatalf("collected %d entries, want %d", got, want)
				}
				runtime.KeepAlive(result)
			})
		}
	}
}

func collectManifestEntryBatches(entries []iceberg.ManifestEntry, manifestCount, concurrency int) *manifestEntries {
	collected := newManifestEntries()
	var g errgroup.Group
	g.SetLimit(min(concurrency, manifestCount))
	for range manifestCount {
		g.Go(func() error {
			manifestEntries := append(make([]iceberg.ManifestEntry, 0, len(entries)), entries...)

			return collected.merge(manifestEntries)
		})
	}

	if err := g.Wait(); err != nil {
		panic(err)
	}

	return collected
}

func benchmarkManifestEntries(count int, content iceberg.ManifestContent) []iceberg.ManifestEntry {
	snapshotID := int64(1)
	dataFiles := []iceberg.DataFile{&mockDataFile{
		path: "data.parquet", contentType: iceberg.EntryContentData,
	}}
	if content == iceberg.ManifestContentDeletes {
		dataFiles = []iceberg.DataFile{
			&mockDataFile{path: "pos-delete.parquet", contentType: iceberg.EntryContentPosDeletes},
			&mockDataFile{path: "eq-delete.parquet", contentType: iceberg.EntryContentEqDeletes},
			&dvMockDataFile{
				mockDataFile:       mockDataFile{path: "dv.puffin", contentType: iceberg.EntryContentPosDeletes, format: iceberg.PuffinFile},
				referencedDataFile: strPtr("data.parquet"),
				contentOffset:      int64Ptr(0),
				contentSizeInBytes: int64Ptr(128),
			},
		}
	}

	entries := make([]iceberg.ManifestEntry, count)
	for i := range entries {
		entries[i] = iceberg.NewManifestEntry(
			iceberg.EntryStatusADDED, &snapshotID, nil, nil, dataFiles[i%len(dataFiles)])
	}

	return entries
}
