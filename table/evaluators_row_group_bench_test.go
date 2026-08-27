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
	"errors"
	"fmt"
	"testing"

	"github.com/apache/arrow-go/v18/parquet/metadata"
	"github.com/apache/iceberg-go"
)

func BenchmarkParquetRowGroupMetricsMaps(b *testing.B) {
	for _, tc := range []struct {
		rowGroups int
		columns   int
	}{
		{rowGroups: 128, columns: 8},
		{rowGroups: 1024, columns: 32},
	} {
		b.Run(fmt.Sprintf("row_groups=%d/columns=%d", tc.rowGroups, tc.columns), func(b *testing.B) {
			withStats := buildRowGroupMetricsMetadata(b, tc.rowGroups, tc.columns, true)
			withoutStats := buildRowGroupMetricsMetadata(b, tc.rowGroups, tc.columns, false)
			colIndices := make([]int, tc.columns)
			for i := range colIndices {
				colIndices[i] = i
			}

			for _, stats := range []struct {
				name string
				meta *metadata.FileMetaData
			}{
				{name: "no_stats", meta: withoutStats},
				{name: "all_stats", meta: withStats},
			} {
				b.Run(stats.name+"/before", func(b *testing.B) {
					benchmarkRowGroupMetricsMaps(b, stats.meta, colIndices, benchmarkTestRowGroupBefore)
				})
				b.Run(stats.name+"/after", func(b *testing.B) {
					benchmarkRowGroupMetricsMaps(b, stats.meta, colIndices, (*inclusiveMetricsEval).TestRowGroup)
				})
			}
		})
	}
}

func benchmarkRowGroupMetricsMaps(
	b *testing.B,
	meta *metadata.FileMetaData,
	colIndices []int,
	testRowGroup func(*inclusiveMetricsEval, *metadata.RowGroupMetaData, []int) (bool, error),
) {
	b.Helper()
	m := &inclusiveMetricsEval{expr: iceberg.AlwaysTrue{}}
	b.ReportAllocs()
	b.ResetTimer()

	for b.Loop() {
		for rowGroup := range meta.NumRowGroups() {
			keep, err := testRowGroup(m, meta.RowGroup(rowGroup), colIndices)
			if err != nil {
				b.Fatal(err)
			}
			if !keep {
				b.Fatal("unexpected row-group rejection")
			}
		}
	}
}

func benchmarkTestRowGroupBefore(m *inclusiveMetricsEval, rgmeta *metadata.RowGroupMetaData, colIndices []int) (bool, error) {
	// Keep this as a frozen pre-#1899 copy; it should not track future changes to TestRowGroup.
	if !m.includeEmptyFiles && rgmeta.NumRows() == 0 {
		return rowsCannotMatch, nil
	}

	m.valueCounts = make(map[int]int64)
	m.nullCounts = make(map[int]int64)
	m.nanCounts = nil
	m.lowerBounds = make(map[int][]byte)
	m.upperBounds = make(map[int][]byte)

	for _, c := range colIndices {
		colMeta, err := rgmeta.ColumnChunk(c)
		if err != nil {
			return false, err
		}

		if ok, err := colMeta.StatsSet(); !ok || err != nil {
			continue
		}

		stats, err := colMeta.Statistics()
		if err != nil {
			return false, err
		}

		if stats == nil {
			continue
		}

		fieldID := int(stats.Descr().SchemaNode().FieldID())
		m.valueCounts[fieldID] = stats.NumValues()
		if stats.HasNullCount() {
			m.nullCounts[fieldID] = stats.NullCount()
		}
		if stats.HasMinMax() {
			m.lowerBounds[fieldID] = stats.EncodeMin()
			m.upperBounds[fieldID] = stats.EncodeMax()
		}
	}

	result, err := iceberg.VisitExprEvaluator(m.expr, m)
	if errors.Is(err, iceberg.ErrInvalidFixedLength) {
		return rowsMightMatch, nil
	}

	return result, err
}
