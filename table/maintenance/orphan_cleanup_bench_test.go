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

package maintenance

import (
	"bytes"
	"context"
	"fmt"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/apache/iceberg-go"
	"github.com/apache/iceberg-go/table"
	"github.com/stretchr/testify/require"
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

func referenceManifestFixture(t testing.TB, n int) (*table.Table, *trackingIO) {
	t.Helper()
	fs := newTrackingIO()
	schema := iceberg.NewSchema(0, iceberg.NestedField{ID: 1, Name: "x", Type: iceberg.PrimitiveTypes.Int64, Required: true})
	spec := iceberg.NewPartitionSpec()
	snapshotID := int64(1)
	entries := make([]iceberg.ManifestEntry, n)
	for i := range entries {
		df, err := iceberg.NewDataFileBuilder(spec, iceberg.EntryContentData, fmt.Sprintf("s3://bucket/data/%d.parquet", i), iceberg.ParquetFile, nil, nil, nil, 1, 1024)
		require.NoError(t, err)
		entries[i] = iceberg.NewManifestEntryBuilder(iceberg.EntryStatusADDED, &snapshotID, df.Build()).DataSequenceNumber(1).Build()
	}
	manifestPath := "s3://bucket/meta/manifest.avro"
	listPath := "s3://bucket/meta/snap.avro"
	var buf bytes.Buffer
	_, err := iceberg.WriteManifest(manifestPath, &buf, 2, spec, schema, snapshotID, entries)
	require.NoError(t, err)
	require.NoError(t, fs.WriteFile(context.Background(), manifestPath, buf.Bytes()))
	mf := iceberg.NewManifestBuilder(2, manifestPath, int64(buf.Len()), 0, snapshotID).SequenceNumber(1, 1).AddedFiles(int32(n)).AddedRows(int64(n)).Build()
	writeManifestList(t, fs, snapshotID, listPath, []iceberg.ManifestFile{mf})
	meta, err := table.ParseMetadataString(buildMetaJSON(metaJSONOpts{snapshots: fmt.Sprintf(`{"snapshot-id":1,"timestamp-ms":1000,"manifest-list":%q}`, listPath)}))
	require.NoError(t, err)

	return table.New(table.Identifier{"ns", "tbl"}, meta, "metadata.json", testFSF(fs), nil), fs
}

func BenchmarkGetReferencedFilesManifestEntries(b *testing.B) {
	for _, n := range []int{1, 16384} {
		b.Run(strconv.Itoa(n), func(b *testing.B) {
			tbl, fs := referenceManifestFixture(b, n)
			b.ReportAllocs()
			for b.Loop() {
				refs, err := getReferencedFiles(context.Background(), tbl, fs, 1, true)
				// Four non-data references: metadata.json, version hint, manifest list, and manifest.
				if err != nil || len(refs) != n+4 {
					b.Fatalf("references=%d error=%v", len(refs), err)
				}
			}
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

				meta, err := table.ParseMetadataString(buildMetaJSON(metaJSONOpts{
					snapshots: strings.Join(snapshotJSON, ","),
				}))
				if err != nil {
					b.Fatal(err)
				}

				fs := &benchmarkDelayIO{IO: baseIO, delay: time.Millisecond}
				tbl := table.New(table.Identifier{"ns", "orphan-cleanup-benchmark"}, meta,
					"metadata.json", testFSF(fs), nil)

				b.ReportAllocs()
				b.ReportMetric(float64(snapshotCount), "manifest_lists/op")
				b.ResetTimer()
				for b.Loop() {
					if _, err := getReferencedFiles(context.Background(), tbl, fs, maxWorkers, true); err != nil {
						b.Fatal(err)
					}
				}
			})
		}
	}
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
			for _, concurrency := range []int{1, 4, 16, 32, 64} {
				b.Run(fmt.Sprintf("files=%d/delay=%s/concurrency=%d", fileCount, delay, concurrency), func(b *testing.B) {
					deleteFunc := func(context.Context, string) error {
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
							func(_ string, err error) error { return err },
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
