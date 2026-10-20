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
	"math"
	"testing"

	"github.com/apache/iceberg-go"
	"github.com/stretchr/testify/require"
)

func TestPositionalDeleteIndexIntegerPartitionFastPath(t *testing.T) {
	for _, tc := range []struct {
		name            string
		deletePartition map[int]any
		dataPartition   map[int]any
		wantMatch       bool
		wantFastPath    bool
	}{
		{"int32_to_int64", map[int]any{1000: int32(7)}, map[int]any{1000: int64(7)}, true, true},
		{"int_to_date", map[int]any{1000: int(-7)}, map[int]any{1000: iceberg.Date(-7)}, true, true},
		{"time_to_timestamp_ns", map[int]any{1000: iceberg.Time(42)}, map[int]any{1000: iceberg.TimestampNano(42)}, true, true},
		{"different_integer", map[int]any{1000: int32(7)}, map[int]any{1000: int64(8)}, false, true},
		{"different_field", map[int]any{1000: int32(7)}, map[int]any{1001: int64(7)}, false, true},
		{"single_string", map[int]any{1000: "a:|;"}, map[int]any{1000: "a:|;"}, true, false},
		{"single_binary", map[int]any{1000: []byte{1, 2}}, map[int]any{1000: []byte{1, 2}}, true, false},
		{"single_null", map[int]any{1000: nil}, map[int]any{1000: nil}, true, false},
		{"float32_to_float64", map[int]any{1000: float32(1.5)}, map[int]any{1000: float64(1.5)}, true, false},
		{"negative_zero", map[int]any{1000: math.Copysign(0, -1)}, map[int]any{1000: float64(0)}, false, false},
		{"two_fields", map[int]any{1000: int32(7), 1001: "west"}, map[int]any{1001: "west", 1000: int64(7)}, true, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			deleteEntry := newPositionalDeleteIndexTestEntry(
				"delete.parquet", 3, tc.deletePartition, 2, nil, "")
			idx, err := buildPositionalDeleteIndex([]iceberg.ManifestEntry{deleteEntry})
			require.NoError(t, err)
			require.Equal(t, tc.wantFastPath, len(idx.byIntegerPartition) == 1)

			dataEntry := newPositionalDeleteIndexDataEntry(
				"data.parquet", 3, tc.dataPartition, 1)
			matches, err := idx.forDataFile(dataEntry)
			require.NoError(t, err)
			require.Equal(t, tc.wantMatch, len(matches) == 1)
		})
	}
}

func TestPositionalDeleteIndexIntegerPartitionSequenceAndSpec(t *testing.T) {
	entries := []iceberg.ManifestEntry{
		newPositionalDeleteIndexTestEntry("later.parquet", 4, map[int]any{1000: int32(5)}, 9, nil, ""),
		newPositionalDeleteIndexTestEntry("before.parquet", 4, map[int]any{1000: iceberg.Date(5)}, 4, nil, ""),
		newPositionalDeleteIndexTestEntry("same.parquet", 4, map[int]any{1000: int64(5)}, 6, nil, ""),
		newPositionalDeleteIndexTestEntry("different-spec.parquet", 5, map[int]any{1000: int32(5)}, 9, nil, ""),
	}
	idx, err := buildPositionalDeleteIndex(entries)
	require.NoError(t, err)
	require.Len(t, idx.byIntegerPartition, 2)

	data := newPositionalDeleteIndexDataEntry("data.parquet", 4, map[int]any{1000: int32(5)}, 6)
	matches, err := idx.forDataFile(data)
	require.NoError(t, err)
	require.Equal(t, []string{"same.parquet", "later.parquet"}, positionalDeletePaths(matches))
}

var positionalDeleteIntegerKeyBenchmarkSink positionalIntegerPartitionKey

func BenchmarkPositionalDeleteIntegerPartitionKey(b *testing.B) {
	const fileCount = 10_000
	files := positionalDeletePartitionKeyBenchmarkFiles(b, 1, false, fileCount)

	b.Run("canonical", func(b *testing.B) {
		b.ReportAllocs()
		b.ReportMetric(fileCount, "files/op")
		for b.Loop() {
			for _, file := range files {
				key, err := canonicalPartitionKey(file.SpecID(), dataFilePartition(file))
				if err != nil {
					b.Fatal(err)
				}
				positionalDeletePartitionKeyBenchmarkSink = key
			}
		}
	})
	b.Run("single_integer", func(b *testing.B) {
		b.ReportAllocs()
		b.ReportMetric(fileCount, "files/op")
		for b.Loop() {
			for _, file := range files {
				key, ok := singleIntegerPartitionKey(file.SpecID(), dataFilePartition(file))
				if !ok {
					b.Fatal("expected one integer partition field")
				}
				positionalDeleteIntegerKeyBenchmarkSink = key
			}
		}
	})
}
