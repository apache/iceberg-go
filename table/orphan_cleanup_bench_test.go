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
	"strings"
	"testing"
	"time"

	"github.com/apache/iceberg-go"
	iceio "github.com/apache/iceberg-go/io"
)

var orphanCleanupBenchmarkSink string

// BenchmarkApplyURIEquivalence measures the per-path lookup cost as the
// number of paths and configured equivalence groups grows. Configuration is
// built before the timer starts so only the repeated lookup is measured.
func BenchmarkApplyURIEquivalence(b *testing.B) {
	for _, tc := range []struct {
		paths  int
		groups int
	}{
		{paths: 100, groups: 1},
		{paths: 100, groups: 100},
		{paths: 10_000, groups: 1},
		{paths: 10_000, groups: 100},
	} {
		b.Run(fmt.Sprintf("paths=%d/groups=%d", tc.paths, tc.groups), func(b *testing.B) {
			equivalences := make(map[string]string, tc.groups)
			for i := range tc.groups {
				equivalences[fmt.Sprintf("scheme-%d,scheme-%d-alt", i, i)] = "canonical"
			}
			cfg := newOrphanCleanupConfig(WithEqualSchemes(equivalences))

			schemes := make([]string, tc.paths)
			for i := range schemes {
				schemes[i] = fmt.Sprintf("scheme-%d", i%tc.groups)
			}

			b.ReportAllocs()
			var result string
			b.ResetTimer()
			for b.Loop() {
				for _, scheme := range schemes {
					result = applySchemeEquivalence(scheme, cfg.equalSchemes)
				}
			}
			b.StopTimer()
			// Keep the result observable without including the sink in the benchmark.
			orphanCleanupBenchmarkSink = result
		})
	}
}

func BenchmarkGetReferencedFilesManifestLists(b *testing.B) {
	for _, snapshotCount := range []int{1, 16, 128, 1024} {
		for _, maxWorkers := range []int{1, 4, 16} {
			b.Run(fmt.Sprintf("snapshots=%d/concurrency=%d", snapshotCount, maxWorkers), func(b *testing.B) {
				baseIO := newTrackingIO()
				manifestPath := "s3://bucket/benchmark/manifest-shared.avro"
				mf := writeManifest(b, baseIO, 1, 1, manifestPath, "s3://bucket/benchmark/data.parquet")
				var snapshotJSON []string
				for i := 1; i <= snapshotCount; i++ {
					listPath := fmt.Sprintf("s3://bucket/benchmark/snap-%d.avro", i)
					writeManifestList(b, baseIO, int64(i), listPath, []iceberg.ManifestFile{mf})
					snapshotJSON = append(snapshotJSON,
						fmt.Sprintf(`{"snapshot-id":%d,"timestamp-ms":%d,"manifest-list":%q}`,
							i, i*1000, listPath))
				}

				meta, err := ParseMetadataString(buildMetaJSON(metaJSONOpts{
					snapshots: strings.Join(snapshotJSON, ","),
				}))
				if err != nil {
					b.Fatal(err)
				}

				fs := &benchmarkDelayIO{IO: baseIO, delay: time.Millisecond}
				tbl := New(Identifier{"ns", "orphan-cleanup-benchmark"}, meta,
					"metadata.json", testFSF(fs), nil)

				b.ReportAllocs()
				b.ReportMetric(float64(snapshotCount), "manifest_lists/op")
				b.ResetTimer()
				for b.Loop() {
					if _, err := tbl.getReferencedFiles(context.Background(), fs, maxWorkers, true); err != nil {
						b.Fatal(err)
					}
				}
			})
		}
	}
}

type benchmarkDelayIO struct {
	iceio.IO
	delay time.Duration
}

func (fs *benchmarkDelayIO) Open(name string) (iceio.File, error) {
	f, err := fs.IO.Open(name)
	if err != nil {
		return nil, err
	}
	time.Sleep(fs.delay)

	return f, nil
}

// BenchmarkPurgeFilesNonBulkDeletion measures the bounded fallback used when
// the filesystem does not implement BulkRemovableIO. The delay models the
// round trip to a remote object store; the zero-delay cases show the worker
// pool overhead on local-style deletes.
func BenchmarkPurgeFilesNonBulkDeletion(b *testing.B) {
	for _, fileCount := range []int{100, 1_000, 10_000} {
		files := make([]string, fileCount)
		for i := range files {
			files[i] = fmt.Sprintf("s3://bucket/table/data/file-%d.parquet", i)
		}

		for _, delay := range []time.Duration{0, 100 * time.Microsecond, time.Millisecond} {
			for _, concurrency := range []int{1, 4, 16} {
				b.Run(fmt.Sprintf("files=%d/delay=%s/concurrency=%d", fileCount, delay, concurrency), func(b *testing.B) {
					deleteFunc := func(string) error {
						time.Sleep(delay)

						return nil
					}

					b.ReportAllocs()
					b.ReportMetric(float64(fileCount), "files/op")
					b.ResetTimer()
					for b.Loop() {
						deleted, err := deleteFilesParallel(
							context.Background(),
							files,
							concurrency,
							deleteFunc,
							func(string, error) error { return nil },
						)
						if err != nil {
							b.Fatal(err)
						}
						orphanCleanupBenchmarkSink = deleted[len(deleted)-1]
					}
				})
			}
		}
	}
}
