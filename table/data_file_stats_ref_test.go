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
	"testing"

	iceberg "github.com/apache/iceberg-go"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type publicStatsDataFile struct {
	iceberg.DataFile
	getterCalls int
}

type publicPartitionDataFile struct {
	iceberg.DataFile
}

func (f *publicPartitionDataFile) Partition() map[int]any {
	return f.DataFile.Partition()
}

func (f *publicStatsDataFile) ValueCounts() map[int]int64 {
	f.getterCalls++

	return f.DataFile.ValueCounts()
}

func (f *publicStatsDataFile) NullValueCounts() map[int]int64 {
	f.getterCalls++

	return f.DataFile.NullValueCounts()
}

func (f *publicStatsDataFile) NaNValueCounts() map[int]int64 {
	f.getterCalls++

	return f.DataFile.NaNValueCounts()
}

func (f *publicStatsDataFile) LowerBoundValues() map[int][]byte {
	f.getterCalls++

	return f.DataFile.LowerBoundValues()
}

func (f *publicStatsDataFile) UpperBoundValues() map[int][]byte {
	f.getterCalls++

	return f.DataFile.UpperBoundValues()
}

func testDataFileWithStats(t *testing.T) iceberg.DataFile {
	t.Helper()

	spec := iceberg.NewPartitionSpec(iceberg.PartitionField{
		SourceIDs: []int{1},
		FieldID:   1000,
		Name:      "part",
		Transform: iceberg.IdentityTransform{},
	})
	builder, err := iceberg.NewDataFileBuilder(
		spec,
		iceberg.EntryContentData,
		"s3://bucket/file.parquet",
		iceberg.ParquetFile,
		map[int]any{1000: "partition"},
		nil,
		nil,
		2,
		10,
	)
	require.NoError(t, err)

	return builder.
		ValueCounts(map[int]int64{1: 2}).
		NullValueCounts(map[int]int64{1: 0}).
		NaNValueCounts(map[int]int64{1: 0}).
		LowerBoundValues(map[int][]byte{1: {1, 2}}).
		UpperBoundValues(map[int][]byte{1: {3, 4}}).
		Build()
}

func TestDataFileStatsUsesBorrowedView(t *testing.T) {
	file := testDataFileWithStats(t)
	require.Implements(t, (*dataFileStatsRefer)(nil), file)

	valueCounts, nullCounts, nanCounts, lowerBounds, upperBounds := dataFileStats(file)
	assert.Equal(t, map[int]int64{1: 2}, valueCounts)
	assert.Equal(t, map[int]int64{1: 0}, nullCounts)
	assert.Equal(t, map[int]int64{1: 0}, nanCounts)
	assert.Equal(t, map[int][]byte{1: {1, 2}}, lowerBounds)
	assert.Equal(t, map[int][]byte{1: {3, 4}}, upperBounds)

	var measuredValueCounts map[int]int64
	allocs := testing.AllocsPerRun(100, func() {
		measuredValueCounts, _, _, _, _ = dataFileStats(file)
	})
	assert.InDelta(t, 0.0, allocs, 0.5)
	assert.Equal(t, map[int]int64{1: 2}, measuredValueCounts)
}

func TestDataFileStatsFallsBackToPublicGetters(t *testing.T) {
	file := &publicStatsDataFile{DataFile: testDataFileWithStats(t)}

	valueCounts, nullCounts, nanCounts, lowerBounds, upperBounds := dataFileStats(file)
	assert.Equal(t, map[int]int64{1: 2}, valueCounts)
	assert.Equal(t, map[int]int64{1: 0}, nullCounts)
	assert.Equal(t, map[int]int64{1: 0}, nanCounts)
	assert.Equal(t, map[int][]byte{1: {1, 2}}, lowerBounds)
	assert.Equal(t, map[int][]byte{1: {3, 4}}, upperBounds)
	assert.Equal(t, 5, file.getterCalls)
}

func TestDataFilePartitionUsesBorrowedView(t *testing.T) {
	file := testDataFileWithStats(t)
	require.Implements(t, (*dataFilePartitionRefer)(nil), file)

	partition := dataFilePartition(file)
	assert.Equal(t, map[int]any{1000: "partition"}, partition)

	allocs := testing.AllocsPerRun(100, func() {
		partition = dataFilePartition(file)
	})
	assert.InDelta(t, 0.0, allocs, 0.5)
	assert.Equal(t, "partition", partition[1000])
}

func TestDataFilePartitionFallsBackToPublicGetter(t *testing.T) {
	file := &publicPartitionDataFile{DataFile: testDataFileWithStats(t)}

	assert.Equal(t, map[int]any{1000: "partition"}, dataFilePartition(file))
}

func BenchmarkInclusiveMetricsEvalDataFileStats(b *testing.B) {
	lower, err := iceberg.Int64Literal(1).MarshalBinary()
	require.NoError(b, err)
	upper, err := iceberg.Int64Literal(10).MarshalBinary()
	require.NoError(b, err)

	spec := iceberg.NewPartitionSpec(iceberg.PartitionField{
		SourceIDs: []int{1}, FieldID: 1000, Name: "part", Transform: iceberg.IdentityTransform{},
	})
	builder, err := iceberg.NewDataFileBuilder(spec, iceberg.EntryContentData,
		"s3://bucket/file.parquet", iceberg.ParquetFile, map[int]any{1000: int64(1)}, nil, nil, 10, 100)
	require.NoError(b, err)
	file := builder.
		ValueCounts(map[int]int64{1: 10}).
		NullValueCounts(map[int]int64{1: 0}).
		LowerBoundValues(map[int][]byte{1: lower}).
		UpperBoundValues(map[int][]byte{1: upper}).
		Build()
	schema := iceberg.NewSchema(1, iceberg.NestedField{
		ID: 1, Name: "id", Type: iceberg.PrimitiveTypes.Int64,
	})
	eval, err := newInclusiveMetricsEvaluator(schema,
		iceberg.LessThan(iceberg.Reference("id"), int64(20)), true, true)
	require.NoError(b, err)

	b.Run("borrowed stats", func(b *testing.B) {
		b.ReportAllocs()
		for range b.N {
			_, err := eval(file)
			if err != nil {
				b.Fatal(err)
			}
		}
	})
	b.Run("public defensive copies", func(b *testing.B) {
		wrapped := &publicStatsDataFile{DataFile: file}
		b.ReportAllocs()
		for range b.N {
			_, err := eval(wrapped)
			if err != nil {
				b.Fatal(err)
			}
		}
	})
}

func BenchmarkGetPartitionRecordDataFilePartition(b *testing.B) {
	benchmarks := []struct {
		name      string
		spec      iceberg.PartitionSpec
		schema    *iceberg.Schema
		partition map[int]any
	}{
		{
			name:      "unpartitioned",
			spec:      *iceberg.UnpartitionedSpec,
			schema:    iceberg.NewSchema(1, iceberg.NestedField{ID: 1, Name: "id", Type: iceberg.PrimitiveTypes.Int64}),
			partition: nil,
		},
		{
			name: "one-field",
			spec: iceberg.NewPartitionSpec(iceberg.PartitionField{
				SourceIDs: []int{1}, FieldID: 1000, Name: "id_part", Transform: iceberg.IdentityTransform{},
			}),
			schema:    iceberg.NewSchema(1, iceberg.NestedField{ID: 1, Name: "id", Type: iceberg.PrimitiveTypes.Int64}),
			partition: map[int]any{1000: int64(1)},
		},
		{
			name: "three-fields",
			spec: iceberg.NewPartitionSpec(
				iceberg.PartitionField{SourceIDs: []int{1}, FieldID: 1000, Name: "id_part", Transform: iceberg.IdentityTransform{}},
				iceberg.PartitionField{SourceIDs: []int{2}, FieldID: 1001, Name: "name_part", Transform: iceberg.IdentityTransform{}},
				iceberg.PartitionField{SourceIDs: []int{3}, FieldID: 1002, Name: "kind_part", Transform: iceberg.IdentityTransform{}},
			),
			schema: iceberg.NewSchema(1,
				iceberg.NestedField{ID: 1, Name: "id", Type: iceberg.PrimitiveTypes.Int64},
				iceberg.NestedField{ID: 2, Name: "name", Type: iceberg.PrimitiveTypes.String},
				iceberg.NestedField{ID: 3, Name: "kind", Type: iceberg.PrimitiveTypes.String},
			),
			partition: map[int]any{1000: int64(1), 1001: "name", 1002: "kind"},
		},
		{
			name: "binary-field",
			spec: iceberg.NewPartitionSpec(iceberg.PartitionField{
				SourceIDs: []int{1}, FieldID: 1000, Name: "payload_part", Transform: iceberg.IdentityTransform{},
			}),
			schema:    iceberg.NewSchema(1, iceberg.NestedField{ID: 1, Name: "payload", Type: iceberg.PrimitiveTypes.Binary}),
			partition: map[int]any{1000: []byte("partition")},
		},
	}

	for _, tc := range benchmarks {
		b.Run(tc.name, func(b *testing.B) {
			builder, err := iceberg.NewDataFileBuilder(tc.spec, iceberg.EntryContentData,
				"s3://bucket/file.parquet", iceberg.ParquetFile, tc.partition, nil, nil, 10, 100)
			require.NoError(b, err)
			file := builder.Build()
			partType := tc.spec.PartitionType(tc.schema)
			wrapped := &publicPartitionDataFile{DataFile: file}

			b.Run("borrowed", func(b *testing.B) {
				b.ReportAllocs()
				for range b.N {
					_ = GetPartitionRecord(file, partType)
				}
			})
			b.Run("public-copy", func(b *testing.B) {
				b.ReportAllocs()
				for range b.N {
					_ = GetPartitionRecord(wrapped, partType)
				}
			})
		})
	}
}
