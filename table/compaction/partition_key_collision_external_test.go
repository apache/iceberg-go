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
	"testing"

	"github.com/apache/iceberg-go"
	"github.com/apache/iceberg-go/table"
	"github.com/apache/iceberg-go/table/compaction"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestPlanCompactionSeparatesAmbiguousPartitionKeys(t *testing.T) {
	tests := []struct {
		name string
		a, b map[int]any
	}{
		{name: "separators in multi-field strings",
			a: map[int]any{1: "a;2=b", 2: "c"},
			b: map[int]any{1: "a", 2: "b;2=c"}},
		{name: "null and literal text",
			a: map[int]any{1: nil}, b: map[int]any{1: "<nil>"}},
		{name: "binary and string",
			a: map[int]any{1: []byte{0, 1}},
			b: map[int]any{1: string([]byte{0, 1})}},
	}
	cfg := compaction.DefaultConfig()
	cfg.RewriteAll = true
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			a := newPartitionedDataFile("a.parquet", 1, tt.a)
			b := newPartitionedDataFile("b.parquet", 1, tt.b)
			plan, err := cfg.PlanCompaction([]table.FileScanTask{
				{File: a, Length: a.FileSizeBytes()},
				{File: b, Length: b.FileSizeBytes()},
			})
			require.NoError(t, err)
			require.Len(t, plan.Groups, 2, "different partitions must not share a compaction group")
			assert.NotEqual(t, plan.Groups[0].PartitionKey, plan.Groups[1].PartitionKey)
			require.Len(t, plan.Groups[0].Tasks, 1)
			require.Len(t, plan.Groups[1].Tasks, 1)
			assert.Equal(t, "a.parquet", plan.Groups[0].Tasks[0].File.FilePath())
			assert.Equal(t, "b.parquet", plan.Groups[1].Tasks[0].File.FilePath())
		})
	}
}

func TestDeadEqualityDeleteSurveyDoesNotConfuseNullWithText(t *testing.T) {
	specs := compactionTestSpecs()
	for _, tt := range []struct {
		name     string
		survivor any
		deleted  any
	}{
		{name: "survivor null, delete text", survivor: nil, deleted: "<nil>"},
		{name: "survivor text, delete null", survivor: "<nil>", deleted: nil},
	} {
		t.Run(tt.name, func(t *testing.T) {
			survey := compaction.NewSurvivorSurvey()
			survey.AddSurvivorWithSpec(1, map[int]any{1000: tt.survivor}, 1)
			removed, err := compaction.DecideDeadEqualityDeletesWithSpecs(
				survey,
				[]iceberg.ManifestEntry{makeEqDeleteEntry(
					t, specs[1], map[int]any{1000: tt.deleted}, 5, "/eq.parquet",
				)},
				specs,
			)
			require.NoError(t, err)
			require.Len(t, removed, 1, "a survivor in another partition cannot keep this delete alive")
			assert.Equal(t, "/eq.parquet", removed[0].FilePath())
		})
	}
}

func TestPlanCompactionKeepsEquivalentIntegerWidthsTogether(t *testing.T) {
	cfg := compaction.DefaultConfig()
	cfg.RewriteAll = true
	a := newPartitionedDataFile("int32.parquet", 1, map[int]any{1: int32(42)})
	b := newPartitionedDataFile("int64.parquet", 1, map[int]any{1: int64(42)})
	plan, err := cfg.PlanCompaction([]table.FileScanTask{
		{File: a, Length: a.FileSizeBytes()},
		{File: b, Length: b.FileSizeBytes()},
	})
	require.NoError(t, err)
	require.Len(t, plan.Groups, 1)
	assert.Len(t, plan.Groups[0].Tasks, 2)
}
