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
	"sync"
	"testing"

	"github.com/apache/iceberg-go"
)

func BenchmarkManifestEntryCollection(b *testing.B) {
	for _, workload := range []struct {
		name    string
		content iceberg.ManifestContent
	}{
		{name: "data", content: iceberg.ManifestContentData},
		{name: "deletes", content: iceberg.ManifestContentDeletes},
	} {
		for _, manifestCount := range []int{1, 8, 64} {
			const entryCount = 1_000
			name := fmt.Sprintf("content=%s/manifests=%d/entries=%d", workload.name, manifestCount, entryCount)
			b.Run(name, func(b *testing.B) {
				entries := benchmarkManifestEntries(entryCount, workload.content)
				b.ReportAllocs()
				b.SetBytes(int64(manifestCount * entryCount))
				b.ResetTimer()

				var result *manifestEntries
				for range b.N {
					result = collectManifestEntryBatches(entries, manifestCount)
				}

				if got, want := len(result.dataEntries)+len(result.positionalDeleteEntries)+
					len(result.equalityDeleteEntries)+len(result.dvEntries), manifestCount*entryCount; got != want {
					b.Fatalf("collected %d entries, want %d", got, want)
				}
				runtime.KeepAlive(result)
			})
		}
	}
}

func collectManifestEntryBatches(entries []iceberg.ManifestEntry, manifestCount int) *manifestEntries {
	collected := newManifestEntries()
	start := make(chan struct{})
	var wg sync.WaitGroup
	wg.Add(manifestCount)
	for range manifestCount {
		go func() {
			defer wg.Done()
			<-start

			manifestEntries := append(make([]iceberg.ManifestEntry, 0, len(entries)), entries...)
			if err := collected.merge(manifestEntries); err != nil {
				panic(err)
			}
		}()
	}

	close(start)
	wg.Wait()

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
