// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
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
	"strings"
	"sync"
	"testing"
	"time"

	iceio "github.com/apache/iceberg-go/io"
)

var manifestContentCacheBenchmarkSink int

func BenchmarkPlanFilesManifestContentCache(b *testing.B) {
	for _, manifestCount := range []int{8, 32} {
		for _, latency := range []time.Duration{0, time.Millisecond} {
			for _, cached := range []bool{false, true} {
				cacheName := "without-cache"
				if cached {
					cacheName = "with-cache"
				}
				b.Run(fmt.Sprintf("manifests=%d/latency=%s/%s", manifestCount, latency, cacheName), func(b *testing.B) {
					tbl := newRowLimitPlanningBenchmarkTable(b, manifestCount)
					baseFS, err := tbl.fsF(context.Background())
					if err != nil {
						b.Fatal(err)
					}
					fs := &manifestContentCacheBenchmarkIO{IO: baseFS, latency: latency}
					tbl.fsF = testFSF(fs)
					if cached {
						tbl.manifestContentCache = newManifestContentCache(0, 100<<20, 8<<20)
					} else {
						tbl.manifestContentCache = nil
					}

					scan := tbl.Scan(WithMaxConcurrency(1))
					tasks, err := scan.PlanFiles(context.Background())
					if err != nil {
						b.Fatal(err)
					}
					if len(tasks) != manifestCount {
						b.Fatalf("planned %d tasks, want %d", len(tasks), manifestCount)
					}
					fs.reset()

					b.ReportAllocs()
					b.ResetTimer()
					for b.Loop() {
						tasks, err := scan.PlanFiles(context.Background())
						if err != nil {
							b.Fatal(err)
						}
						manifestContentCacheBenchmarkSink = len(tasks)
					}
					b.StopTimer()
					b.ReportMetric(float64(fs.count())/float64(b.N), "manifest-opens/op")
				})
			}
		}
	}
}

type manifestContentCacheBenchmarkIO struct {
	iceio.IO
	latency time.Duration

	mu    sync.Mutex
	opens int64
}

func (fs *manifestContentCacheBenchmarkIO) Open(name string) (iceio.File, error) {
	if strings.Contains(name, "/manifest-") {
		if fs.latency > 0 {
			time.Sleep(fs.latency)
		}
		fs.mu.Lock()
		fs.opens++
		fs.mu.Unlock()
	}

	return fs.IO.Open(name)
}

func (fs *manifestContentCacheBenchmarkIO) reset() {
	fs.mu.Lock()
	fs.opens = 0
	fs.mu.Unlock()
}

func (fs *manifestContentCacheBenchmarkIO) count() int64 {
	fs.mu.Lock()
	defer fs.mu.Unlock()

	return fs.opens
}
