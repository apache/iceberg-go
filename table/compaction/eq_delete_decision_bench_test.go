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

package compaction_test

import (
	"fmt"
	"testing"

	"github.com/apache/iceberg-go"
	"github.com/apache/iceberg-go/table/compaction"
)

var benchmarkDeadEqualityDeletes []iceberg.DataFile

func BenchmarkDecideDeadEqualityDeletes(b *testing.B) {
	for _, tc := range []struct {
		name       string
		partitions int
		deletes    int
	}{
		{name: "P=1/D=1", partitions: 1, deletes: 1},
		{name: "P=100/D=100", partitions: 100, deletes: 100},
		{name: "P=1000/D=1000", partitions: 1000, deletes: 1000},
		{name: "P=10000/D=1000", partitions: 10000, deletes: 1000},
		{name: "P=1000/D=10000", partitions: 1000, deletes: 10000},
	} {
		b.Run(tc.name, func(b *testing.B) {
			survey := compaction.NewSurvivorSurvey()
			for i := 0; i < tc.partitions; i++ {
				survey.AddSurvivor(map[int]any{1000: i}, 10)
			}

			specs := compactionTestSpecs()
			candidates := make([]iceberg.ManifestEntry, tc.deletes)
			for i := range candidates {
				candidates[i] = makeEqDeleteEntry(
					b, specs[0], nil, 1, fmt.Sprintf("/eq-%d.parquet", i))
			}

			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				benchmarkDeadEqualityDeletes = compaction.DecideDeadEqualityDeletes(
					survey, candidates)
			}
			b.StopTimer()
			if len(benchmarkDeadEqualityDeletes) != tc.deletes {
				b.Fatalf("expected %d dead equality deletes, got %d", tc.deletes, len(benchmarkDeadEqualityDeletes))
			}
		})
	}
}
