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
	"sync"
	"testing"

	iceio "github.com/apache/iceberg-go/io"
)

var snapshotManifestCacheBenchmarkSink int

func BenchmarkPlanFilesSnapshotManifestCache(b *testing.B) {
	const manifestListPath = "mem://row-limit-benchmark/metadata/snap-1.avro"

	for _, manifestCount := range []int{1, 1_000, 10_000} {
		for _, scanCount := range []int{1, 2, 10} {
			for _, cached := range []bool{false, true} {
				name := "without-cache"
				if cached {
					name = "with-cache"
				}
				b.Run(fmt.Sprintf("manifests=%d/scans=%d/%s", manifestCount, scanCount, name), func(b *testing.B) {
					tbl := newRowLimitPlanningBenchmarkTable(b, manifestCount)
					baseFS, err := tbl.fsF(context.Background())
					if err != nil {
						b.Fatal(err)
					}
					fs := &snapshotManifestCacheBenchmarkIO{
						IO:    baseFS,
						opens: make(map[string]int),
					}
					tbl.fsF = testFSF(fs)
					if !cached {
						tbl.manifestCache = nil
					}

					b.ReportAllocs()
					b.ResetTimer()
					for range b.N {
						b.StopTimer()
						if cached {
							tbl.manifestCache = newSnapshotManifestCache()
						} else {
							tbl.manifestCache = nil
						}
						b.StartTimer()
						for range scanCount {
							tasks, err := tbl.Scan(WithMaxConcurrency(1)).PlanFiles(context.Background())
							if err != nil {
								b.Fatal(err)
							}
							snapshotManifestCacheBenchmarkSink = len(tasks)
						}
					}
					b.StopTimer()
					fs.mu.Lock()
					listOpens := fs.opens[manifestListPath]
					fs.mu.Unlock()
					b.ReportMetric(float64(listOpens)/float64(b.N), "manifest-list-opens")
				})
			}
		}
	}
}

type snapshotManifestCacheBenchmarkIO struct {
	iceio.IO

	mu    sync.Mutex
	opens map[string]int
}

func (fs *snapshotManifestCacheBenchmarkIO) Open(name string) (iceio.File, error) {
	fs.mu.Lock()
	fs.opens[name]++
	fs.mu.Unlock()

	return fs.IO.Open(name)
}
