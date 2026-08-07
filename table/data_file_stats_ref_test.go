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
	"testing"

	iceberg "github.com/apache/iceberg-go"
	"github.com/apache/iceberg-go/internal"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type publicStatsDataFile struct {
	iceberg.DataFile
	getterCalls int
}

type plainStatsDataFile struct {
	iceberg.DataFile
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
	require.Implements(t, (*internal.DataFileStatsRef)(nil), file)

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

func TestDataFileBoundsUseBorrowedView(t *testing.T) {
	file := testDataFileWithStats(t)
	lowerBounds, upperBounds := internal.BorrowedDataFileBounds(file)
	assert.Equal(t, map[int][]byte{1: {1, 2}}, lowerBounds)
	assert.Equal(t, map[int][]byte{1: {3, 4}}, upperBounds)

	allocs := testing.AllocsPerRun(100, func() {
		lowerBounds, upperBounds = internal.BorrowedDataFileBounds(file)
	})
	assert.InDelta(t, 0.0, allocs, 0.5)
	assert.Equal(t, []byte{1, 2}, lowerBounds[1])
	assert.Equal(t, []byte{3, 4}, upperBounds[1])
}

func TestDataFileReferencedDataFileUsesBorrowedView(t *testing.T) {
	spec := *iceberg.UnpartitionedSpec
	builder, err := iceberg.NewDataFileBuilder(
		spec,
		iceberg.EntryContentPosDeletes,
		"s3://bucket/delete.parquet",
		iceberg.ParquetFile,
		nil,
		nil,
		nil,
		1,
		10,
	)
	require.NoError(t, err)
	file := builder.ReferencedDataFile("s3://bucket/data.parquet").Build()

	ref := internal.BorrowedDataFileReferencedDataFile(file)
	require.NotNil(t, ref)
	assert.Equal(t, "s3://bucket/data.parquet", *ref)

	allocs := testing.AllocsPerRun(100, func() {
		ref = internal.BorrowedDataFileReferencedDataFile(file)
	})
	assert.InDelta(t, 0.0, allocs, 0.5)
	assert.Equal(t, "s3://bucket/data.parquet", *ref)
}

func TestDataFilePartitionUsesBorrowedView(t *testing.T) {
	file := testDataFileWithStats(t)
	require.Implements(t, (*internal.DataFilePartitionRef)(nil), file)

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

func TestGetPartitionRecordClonesBinaryValues(t *testing.T) {
	spec := iceberg.NewPartitionSpec(iceberg.PartitionField{
		SourceIDs: []int{1}, FieldID: 1000, Name: "payload_part", Transform: iceberg.IdentityTransform{},
	})
	schema := iceberg.NewSchema(1, iceberg.NestedField{ID: 1, Name: "payload", Type: iceberg.PrimitiveTypes.Binary})
	builder, err := iceberg.NewDataFileBuilder(spec, iceberg.EntryContentData,
		"s3://bucket/file.parquet", iceberg.ParquetFile, map[int]any{1000: []byte{1, 2}}, nil, nil, 1, 10)
	require.NoError(t, err)
	file := builder.Build()

	record := GetPartitionRecord(file, spec.PartitionType(schema))
	value := record.Get(0).([]byte)
	value[0] = 99

	assert.Equal(t, []byte{1, 2}, file.Partition()[1000])
}

func BenchmarkInclusiveMetricsEvalDataFileStats(b *testing.B) {
	for _, columnCount := range []int{1, 50, 500} {
		b.Run(fmt.Sprintf("%d columns", columnCount), func(b *testing.B) {
			lower, err := iceberg.Int64Literal(1).MarshalBinary()
			require.NoError(b, err)
			upper, err := iceberg.Int64Literal(10).MarshalBinary()
			require.NoError(b, err)

			fields := make([]iceberg.NestedField, columnCount)
			valueCounts := make(map[int]int64, columnCount)
			nullCounts := make(map[int]int64, columnCount)
			nanCounts := make(map[int]int64, columnCount)
			lowerBounds := make(map[int][]byte, columnCount)
			upperBounds := make(map[int][]byte, columnCount)
			for i := range fields {
				fieldID := i + 1
				fields[i] = iceberg.NestedField{ID: fieldID, Name: fmt.Sprintf("id%d", i), Type: iceberg.PrimitiveTypes.Int64}
				valueCounts[fieldID] = 10
				nullCounts[fieldID] = 0
				nanCounts[fieldID] = 0
				lowerBounds[fieldID] = lower
				upperBounds[fieldID] = upper
			}

			builder, err := iceberg.NewDataFileBuilder(*iceberg.UnpartitionedSpec, iceberg.EntryContentData,
				"s3://bucket/file.parquet", iceberg.ParquetFile, nil, nil, nil, 10, 100)
			require.NoError(b, err)
			file := builder.
				ValueCounts(valueCounts).
				NullValueCounts(nullCounts).
				NaNValueCounts(nanCounts).
				LowerBoundValues(lowerBounds).
				UpperBoundValues(upperBounds).
				Build()
			schema := iceberg.NewSchema(1, fields...)
			eval, err := newInclusiveMetricsEvaluator(schema,
				iceberg.LessThan(iceberg.Reference("id0"), int64(20)), true, true)
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
				wrapped := &plainStatsDataFile{DataFile: file}
				b.ReportAllocs()
				for range b.N {
					_, err := eval(wrapped)
					if err != nil {
						b.Fatal(err)
					}
				}
			})
		})
	}
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
