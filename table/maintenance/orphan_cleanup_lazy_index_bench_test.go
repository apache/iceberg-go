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
	"fmt"
	"strconv"
	"testing"
)

// This is the pre-optimization orphan discovery path, kept in a benchmark
// only so before/after measurements can use the same binary and inputs.
func findOrphanFilesEagerForBenchmark(
	scannedFiles []scannedFile, referencedFiles map[string]bool, cfg *orphanCleanupConfig,
) (OrphanCleanupPlan, error) {
	referencedIndex := newReferencedFileIndex(referencedFiles, cfg)
	plan := OrphanCleanupPlan{orphanFiles: make([]OrphanFile, 0)}

	for _, f := range scannedFiles {
		// Before: every file was normalized before the raw reference lookup.
		aliases := normalizedFilePathAliases(f.path, cfg)
		normalized := aliases[0]
		if _, ok := referencedFiles[f.path]; ok {
			continue
		}
		if _, ok := referencedFiles[normalized]; ok {
			continue
		}

		matched := false
		for _, alias := range aliases {
			if _, ok := referencedIndex.normalized[alias]; ok {
				matched = true

				break
			}
		}
		if matched {
			continue
		}

		for _, reference := range referencedIndex.byPath[filePathKey(normalized)] {
			decision, err := checkPrefixMismatch(reference, f.path, cfg)
			if err != nil {
				return OrphanCleanupPlan{}, fmt.Errorf("failed to identify orphan %s: %w", f.path, err)
			}
			if decision == prefixMismatchKeep {
				matched = true

				break
			}
		}
		if matched {
			continue
		}

		plan.orphanFileLocations = append(plan.orphanFileLocations, f.path)
		plan.orphanFiles = append(plan.orphanFiles, OrphanFile{
			Path: f.path, SizeBytes: f.size,
		})
		plan.totalSizeBytes += f.size
	}

	return plan, nil
}

func BenchmarkFindOrphanFilesLazyIndex(b *testing.B) {
	for _, count := range []int{1_000, 10_000, 100_000} {
		for _, scenario := range []string{"all-exact", "one-percent-orphans", "one-percent-scheme-aliases"} {
			b.Run(fmt.Sprintf("files=%d/%s", count, scenario), func(b *testing.B) {
				cfg := newOrphanCleanupConfig()
				if scenario == "one-percent-scheme-aliases" {
					cfg = newOrphanCleanupConfig(WithEqualSchemes(map[string]string{"s3,s3a": "s3"}))
				}

				references := make(map[string]bool, count)
				scanned := make([]scannedFile, count)
				wantOrphans := 0
				for i := range scanned {
					path := fmt.Sprintf("s3://bucket/warehouse/table/data/part-%06d.parquet", i)
					if scenario == "one-percent-orphans" && i%100 == 0 {
						wantOrphans++
					} else {
						references[path] = true
					}
					if scenario == "one-percent-scheme-aliases" && i%100 == 0 {
						path = "s3a" + path[2:]
					}
					scanned[i] = scannedFile{path: path, size: int64(i + 1)}
				}

				for _, implementation := range []struct {
					name string
					find func([]scannedFile, map[string]bool, *orphanCleanupConfig) (OrphanCleanupPlan, error)
				}{
					{"before", findOrphanFilesEagerForBenchmark},
					{"after", findOrphanFiles},
				} {
					b.Run(implementation.name, func(b *testing.B) {
						b.ReportAllocs()
						var plan OrphanCleanupPlan
						b.ResetTimer()
						for range b.N {
							var err error
							plan, err = implementation.find(scanned, references, cfg)
							if err != nil {
								b.Fatal(err)
							}
						}
						b.StopTimer()
						if len(plan.orphanFiles) != wantOrphans {
							b.Fatalf("got %d orphans, want %d", len(plan.orphanFiles), wantOrphans)
						}
						orphanCleanupBenchmarkSink = strconv.FormatInt(plan.totalSizeBytes, 10)
					})
				}
			})
		}
	}
}
