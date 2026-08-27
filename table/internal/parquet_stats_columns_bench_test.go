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

package internal

import (
	"encoding/binary"
	"math"
	"strings"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/extensions"
	"github.com/apache/arrow-go/v18/parquet"
	"github.com/apache/arrow-go/v18/parquet/metadata"
	"github.com/apache/iceberg-go"
)

// benchmarkLegacyStatsColumnResolution models the path and field lookup that
// ran once for every physical column in every row group before descriptors
// were introduced. It deliberately stops after resolution so the paired
// benchmark isolates the work changed by this optimization.
func benchmarkLegacyStatsColumnResolution(meta *metadata.FileMetaData, statsCols map[int]StatisticsCollector, colMapping map[string]int, variantFieldIDs map[int]struct{}) int {
	checksum := 0
	for rg := range meta.NumRowGroups() {
		rowGroup := meta.RowGroup(rg)
		for pos := range rowGroup.NumColumns() {
			colChunk, err := rowGroup.ColumnChunk(pos)
			if err != nil {
				panic(err)
			}

			pathString := colChunk.PathInSchema().String()
			fieldID, ok := colMapping[pathString]
			checksum += len(pathString)
			if !ok {
				path := colChunk.PathInSchema()
				for depth := len(path) - 1; depth >= 1; depth-- {
					ancestorPath := strings.Join(path[:depth], ".")
					if ancestorID, hasAncestor := colMapping[ancestorPath]; hasAncestor {
						if _, isVariant := variantFieldIDs[ancestorID]; !isVariant {
							panic("unexpected non-variant column mapping in benchmark")
						}

						break
					}
				}

				continue
			}

			checksum += fieldID + len(statsCols[fieldID].Mode.Typ)
		}
	}

	return checksum
}

func benchmarkResolvedStatsColumnResolution(meta *metadata.FileMetaData, statsCols map[int]StatisticsCollector, colMapping map[string]int, variantFieldIDs map[int]struct{}) int {
	columns := resolveParquetStatsColumns(meta, statsCols, colMapping, variantFieldIDs)
	checksum := 0
	for rg := range meta.NumRowGroups() {
		rowGroup := meta.RowGroup(rg)
		for pos := range rowGroup.NumColumns() {
			if _, err := rowGroup.ColumnChunk(pos); err != nil {
				panic(err)
			}

			column := columns[pos]
			if column.resolveErr != nil {
				panic(column.resolveErr)
			}
			checksum += len(column.path)
			if column.variantChild || column.skipStats {
				continue
			}

			checksum += column.fieldID + len(column.statsCol.Mode.Typ)
		}
	}

	return checksum
}

func benchmarkStats(min, max []byte, nullCount int64) metadata.EncodedStatistics {
	var stats metadata.EncodedStatistics
	stats.SetMin(min)
	stats.SetMax(max)
	stats.SetNullCount(nullCount)

	return stats
}

func benchmarkAllNullStats(nullCount int64) metadata.EncodedStatistics {
	var stats metadata.EncodedStatistics
	stats.SetNullCount(nullCount)

	return stats
}

func buildStatsColumnsBenchmarkMetadata(b *testing.B, rowGroups, rows int) *metadata.FileMetaData {
	b.Helper()

	meta := buildStatsColumnsMetadata(b)
	builder := metadata.NewFileMetadataBuilder(meta.Schema, parquet.NewWriterProperties(), nil)
	rowGroupBytes := int64(9 * rows * 16)

	for rg := range rowGroups {
		rowGroup := builder.AppendRowGroup()
		rowGroup.SetNumRows(rows)

		idMin := make([]byte, 4)
		idMax := make([]byte, 4)
		binary.LittleEndian.PutUint32(idMin, uint32(rg*rows))
		binary.LittleEndian.PutUint32(idMax, uint32((rg+1)*rows-1))

		scoreMin := make([]byte, 8)
		scoreMax := make([]byte, 8)
		binary.LittleEndian.PutUint64(scoreMin, math.Float64bits(float64(rg)))
		binary.LittleEndian.PutUint64(scoreMax, math.Float64bits(float64(rg+1)))

		stats := []metadata.EncodedStatistics{
			benchmarkStats(idMin, idMax, 0),
			benchmarkStats(scoreMin, scoreMax, 0),
			benchmarkStats([]byte("name-a"), []byte("name-z"), 0),
			benchmarkAllNullStats(int64(rows)),
			benchmarkAllNullStats(int64(rows)),
			benchmarkAllNullStats(int64(rows)),
			benchmarkStats(scoreMin, scoreMax, 0),
			benchmarkAllNullStats(int64(rows)),
			benchmarkStats([]byte("city-a"), []byte("city-z"), 0),
		}

		for pos, stat := range stats {
			chunk := rowGroup.NextColumnChunk()
			chunk.SetStats(stat)
			info := metadata.ChunkMetaInfo{
				NumValues:        int64(rows),
				DataPageOffset:   int64(100 + rg*1000 + pos*100),
				IndexPageOffset:  -1,
				CompressedSize:   16,
				UncompressedSize: 32,
			}
			if err := chunk.Finish(info, false, false, metadata.EncodingStats{}); err != nil {
				b.Fatal(err)
			}
		}

		if err := rowGroup.Finish(rowGroupBytes, int16(rg)); err != nil {
			b.Fatal(err)
		}
	}

	result, err := builder.Finish()
	if err != nil {
		b.Fatal(err)
	}

	return result
}

type statsColumnsBenchmarkFixture struct {
	meta            *metadata.FileMetaData
	arrowSchema     *arrow.Schema
	statsCols       map[int]StatisticsCollector
	colMapping      map[string]int
	variantFieldIDs map[int]struct{}
}

func newStatsColumnsBenchmarkFixture(b *testing.B, rowGroups, rows int) statsColumnsBenchmarkFixture {
	b.Helper()

	return statsColumnsBenchmarkFixture{
		meta: buildStatsColumnsBenchmarkMetadata(b, rowGroups, rows),
		arrowSchema: arrow.NewSchema([]arrow.Field{{
			Name: "payload",
			Type: extensions.NewShreddedVariantType(arrow.StructOf(
				arrow.Field{Name: "score", Type: arrow.PrimitiveTypes.Float64},
				arrow.Field{Name: "city", Type: arrow.BinaryTypes.String},
			)),
			Nullable: true,
		}}, nil),
		statsCols: map[int]StatisticsCollector{
			1: {FieldID: 1, Mode: MetricsMode{Typ: MetricModeFull}, IcebergTyp: iceberg.PrimitiveTypes.Int32},
			2: {FieldID: 2, Mode: MetricsMode{Typ: MetricModeCounts}, IcebergTyp: iceberg.PrimitiveTypes.Float64},
			3: {FieldID: 3, Mode: MetricsMode{Typ: MetricModeCounts}, IcebergTyp: iceberg.PrimitiveTypes.String},
			4: {FieldID: 4, Mode: MetricsMode{Typ: MetricModeFull}, ColName: "payload"},
		},
		colMapping: map[string]int{
			"id":            1,
			"details.score": 2,
			"details.name":  3,
			"payload":       4,
		},
		variantFieldIDs: map[int]struct{}{4: {}},
	}
}

// BenchmarkParquetStatsColumnResolution compares the repeated per-row-group
// lookup with the one-time descriptor resolution. The end-to-end benchmark
// below measures the same change with statistics aggregation included.
func BenchmarkParquetStatsColumnResolution(b *testing.B) {
	const (
		rowGroups = 32
		rows      = 10_000
	)

	fixture := newStatsColumnsBenchmarkFixture(b, rowGroups, rows)

	want := benchmarkLegacyStatsColumnResolution(fixture.meta, fixture.statsCols, fixture.colMapping, fixture.variantFieldIDs)
	if got := benchmarkResolvedStatsColumnResolution(fixture.meta, fixture.statsCols, fixture.colMapping, fixture.variantFieldIDs); got != want {
		b.Fatalf("resolution checksum mismatch: before=%d after=%d", want, got)
	}

	for _, tc := range []struct {
		name string
		fn   func() int
	}{
		{
			name: "before",
			fn: func() int {
				return benchmarkLegacyStatsColumnResolution(fixture.meta, fixture.statsCols, fixture.colMapping, fixture.variantFieldIDs)
			},
		},
		{
			name: "after",
			fn: func() int {
				return benchmarkResolvedStatsColumnResolution(fixture.meta, fixture.statsCols, fixture.colMapping, fixture.variantFieldIDs)
			},
		},
	} {
		b.Run(tc.name, func(b *testing.B) {
			b.ReportAllocs()
			b.ResetTimer()
			for range b.N {
				if got := tc.fn(); got != want {
					b.Fatalf("unexpected resolution checksum: got=%d want=%d", got, want)
				}
			}
			b.ReportMetric(float64(rowGroups*9), "physical-columns/op")
		})
	}
}

func BenchmarkDataFileStatsFromMetaNestedVariant(b *testing.B) {
	const (
		rowGroups = 32
		rows      = 10_000
	)

	fixture := newStatsColumnsBenchmarkFixture(b, rowGroups, rows)

	format := parquetFormat{}
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		stats := format.DataFileStatsFromMeta(fixture.meta, fixture.statsCols, fixture.colMapping, fixture.variantFieldIDs, fixture.arrowSchema)
		if stats.RecordCount != int64(rowGroups*rows) {
			b.Fatalf("unexpected record count: %d", stats.RecordCount)
		}
		if len(stats.VariantLowerBounds) != 1 || len(stats.VariantUpperBounds) != 1 {
			b.Fatalf("expected one variant bound object, got lower=%d upper=%d", len(stats.VariantLowerBounds), len(stats.VariantUpperBounds))
		}
	}
	b.ReportMetric(float64(rowGroups*rows), "rows/op")
	b.ReportMetric(float64(rowGroups*9), "columns/op")
}

func BenchmarkDataFileStatsFromMetaMostlyInactive(b *testing.B) {
	const (
		rowGroups = 32
		rows      = 10_000
	)

	fixture := newStatsColumnsBenchmarkFixture(b, rowGroups, rows)
	statsCols := map[int]StatisticsCollector{
		1: {FieldID: 1, Mode: MetricsMode{Typ: MetricModeNone}, IcebergTyp: iceberg.PrimitiveTypes.Int32},
		2: {FieldID: 2, Mode: MetricsMode{Typ: MetricModeNone}, IcebergTyp: iceberg.PrimitiveTypes.Float64},
		3: {FieldID: 3, Mode: MetricsMode{Typ: MetricModeFull}, IcebergTyp: iceberg.PrimitiveTypes.String},
		4: {FieldID: 4, Mode: MetricsMode{Typ: MetricModeNone}, ColName: "payload"},
	}

	format := parquetFormat{}
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		stats := format.DataFileStatsFromMeta(
			fixture.meta, statsCols, fixture.colMapping, fixture.variantFieldIDs, nil)
		if stats.RecordCount != int64(rowGroups*rows) {
			b.Fatalf("unexpected record count: %d", stats.RecordCount)
		}
		if len(stats.ValueCounts) != 1 {
			b.Fatalf("expected one active column, got %d", len(stats.ValueCounts))
		}
	}
	b.ReportMetric(float64(rowGroups*rows), "rows/op")
	b.ReportMetric(float64(rowGroups*9), "columns/op")
}
