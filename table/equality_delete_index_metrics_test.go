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

	"github.com/apache/iceberg-go"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type equalityDeleteMetricsTestFile struct {
	*mockDataFile
	equalityFieldIDs []int
}

func (f *equalityDeleteMetricsTestFile) EqualityFieldIDs() []int {
	return f.equalityFieldIDs
}

func newEqualityDeleteMetricsTestEntry(
	path string,
	specID int32,
	partition map[int]any,
	content iceberg.ManifestEntryContent,
	sequenceNumber int64,
	fieldIDs []int,
	valueCounts, nullCounts, nanCounts map[int]int64,
	lowerBound, upperBound []byte,
) iceberg.ManifestEntry {
	var lowerBounds, upperBounds map[int][]byte
	if lowerBound != nil {
		lowerBounds = map[int][]byte{1: lowerBound}
	}
	if upperBound != nil {
		upperBounds = map[int][]byte{1: upperBound}
	}

	file := &equalityDeleteMetricsTestFile{
		mockDataFile: &mockDataFile{
			path:        path,
			specid:      specID,
			partition:   partition,
			contentType: content,
			format:      iceberg.ParquetFile,
			count:       1,
			valueCounts: valueCounts,
			nullCounts:  nullCounts,
			nanCounts:   nanCounts,
			lowerBounds: lowerBounds,
			upperBounds: upperBounds,
		},
		equalityFieldIDs: fieldIDs,
	}

	return iceberg.NewManifestEntry(
		iceberg.EntryStatusADDED,
		nil,
		&sequenceNumber,
		nil,
		file,
	)
}

func equalityDeleteMetricsTestSchema(typ iceberg.Type, required bool) *iceberg.Schema {
	return iceberg.NewSchema(1, iceberg.NestedField{
		ID: 1, Name: "id", Type: typ, Required: required,
	})
}

func equalityDeleteMetricsTestBounds[T iceberg.LiteralType](t testing.TB, lower, upper T) ([]byte, []byte) {
	t.Helper()

	lowerLiteral := iceberg.NewLiteral(lower)
	upperLiteral := iceberg.NewLiteral(upper)
	lowerBytes, err := lowerLiteral.MarshalBinary()
	require.NoError(t, err)
	upperBytes, err := upperLiteral.MarshalBinary()
	require.NoError(t, err)

	return lowerBytes, upperBytes
}

func equalityDeleteMetricPaths(files []iceberg.DataFile) []string {
	paths := make([]string, len(files))
	for i, file := range files {
		paths[i] = file.FilePath()
	}

	return paths
}

func TestEqualityDeleteIndexPrunesDisjointMetricRanges(t *testing.T) {
	schema := equalityDeleteMetricsTestSchema(iceberg.PrimitiveTypes.Int32, true)
	partition := map[int]any{1000: int32(0)}
	dataLower, dataUpper := equalityDeleteMetricsTestBounds(t, int32(0), int32(25))
	matchingLower, matchingUpper := equalityDeleteMetricsTestBounds(t, int32(10), int32(20))
	disjointLower, disjointUpper := equalityDeleteMetricsTestBounds(t, int32(30), int32(40))

	deleteEntries := []iceberg.ManifestEntry{
		newEqualityDeleteMetricsTestEntry(
			"partition-matching.parquet", 1, partition, iceberg.EntryContentEqDeletes, 2,
			[]int{1}, map[int]int64{1: 1}, map[int]int64{1: 0}, nil,
			matchingLower, matchingUpper),
		newEqualityDeleteMetricsTestEntry(
			"partition-disjoint.parquet", 1, partition, iceberg.EntryContentEqDeletes, 3,
			[]int{1}, map[int]int64{1: 1}, map[int]int64{1: 0}, nil,
			disjointLower, disjointUpper),
		newEqualityDeleteMetricsTestEntry(
			"partition-no-metrics.parquet", 1, partition, iceberg.EntryContentEqDeletes, 4,
			[]int{1}, nil, nil, nil, nil, nil),
		newEqualityDeleteMetricsTestEntry(
			"global-disjoint.parquet", 0, nil, iceberg.EntryContentEqDeletes, 5,
			[]int{1}, map[int]int64{1: 1}, map[int]int64{1: 0}, nil,
			disjointLower, disjointUpper),
		newEqualityDeleteMetricsTestEntry(
			"global-matching.parquet", 0, nil, iceberg.EntryContentEqDeletes, 6,
			[]int{1}, map[int]int64{1: 1}, map[int]int64{1: 0}, nil,
			matchingLower, matchingUpper),
	}

	idx, err := buildEqualityDeleteIndex(deleteEntries, equalityDeleteIndexTestSpecs(), schema)
	require.NoError(t, err)

	dataEntry := newEqualityDeleteMetricsTestEntry(
		"data.parquet", 1, partition, iceberg.EntryContentData, 1,
		nil, map[int]int64{1: 1}, map[int]int64{1: 0}, nil,
		dataLower, dataUpper)
	matched, err := idx.forDataFile(dataEntry)
	require.NoError(t, err)

	assert.Equal(t, []string{
		"global-matching.parquet",
		"partition-matching.parquet",
		"partition-no-metrics.parquet",
	}, equalityDeleteMetricPaths(matched))
}

func TestEqualityDeleteIndexPrunesNullOnlyMetricRanges(t *testing.T) {
	schema := equalityDeleteMetricsTestSchema(iceberg.PrimitiveTypes.Int32, false)
	partition := map[int]any{1000: int32(0)}
	lower, upper := equalityDeleteMetricsTestBounds(t, int32(10), int32(20))

	tests := []struct {
		name         string
		dataValues   map[int]int64
		dataNulls    map[int]int64
		deleteValues map[int]int64
		deleteNulls  map[int]int64
		wantFiles    int
	}{
		{
			name:         "data is all null and delete is all non-null",
			dataValues:   map[int]int64{1: 10},
			dataNulls:    map[int]int64{1: 10},
			deleteValues: map[int]int64{1: 10},
			deleteNulls:  map[int]int64{1: 0},
		},
		{
			name:         "data is all non-null and delete is all null",
			dataValues:   map[int]int64{1: 10},
			dataNulls:    map[int]int64{1: 0},
			deleteValues: map[int]int64{1: 10},
			deleteNulls:  map[int]int64{1: 10},
		},
		{
			name:         "both files may contain null",
			dataValues:   map[int]int64{1: 10},
			dataNulls:    map[int]int64{1: 1},
			deleteValues: map[int]int64{1: 10},
			deleteNulls:  map[int]int64{1: 1},
			wantFiles:    1,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			deleteEntry := newEqualityDeleteMetricsTestEntry(
				"delete.parquet", 1, partition, iceberg.EntryContentEqDeletes, 2,
				[]int{1}, tt.deleteValues, tt.deleteNulls, nil, lower, upper)
			idx, err := buildEqualityDeleteIndex(
				[]iceberg.ManifestEntry{deleteEntry}, equalityDeleteIndexTestSpecs(), schema)
			require.NoError(t, err)

			dataEntry := newEqualityDeleteMetricsTestEntry(
				"data.parquet", 1, partition, iceberg.EntryContentData, 1,
				nil, tt.dataValues, tt.dataNulls, nil, lower, upper)
			matched, err := idx.forDataFile(dataEntry)
			require.NoError(t, err)
			assert.Len(t, matched, tt.wantFiles)
		})
	}
}

func TestEqualityDeleteIndexKeepsUncertainMetricRanges(t *testing.T) {
	schema := equalityDeleteMetricsTestSchema(iceberg.PrimitiveTypes.Int32, true)
	partition := map[int]any{1000: int32(0)}
	dataLower, dataUpper := equalityDeleteMetricsTestBounds(t, int32(0), int32(10))
	deleteLower, deleteUpper := equalityDeleteMetricsTestBounds(t, int32(20), int32(30))

	tests := []struct {
		name         string
		dataLower    []byte
		dataUpper    []byte
		deleteLower  []byte
		deleteUpper  []byte
		dataValues   map[int]int64
		dataNulls    map[int]int64
		deleteValues map[int]int64
		deleteNulls  map[int]int64
		fieldID      int
		wantFiles    int
	}{
		{
			name:         "missing data bounds",
			dataValues:   map[int]int64{1: 1},
			dataNulls:    map[int]int64{1: 0},
			deleteValues: map[int]int64{1: 1},
			deleteNulls:  map[int]int64{1: 0},
			deleteLower:  deleteLower,
			deleteUpper:  deleteUpper,
			wantFiles:    1,
		},
		{
			name:         "malformed data bound",
			dataLower:    []byte{1},
			dataUpper:    dataUpper,
			deleteLower:  deleteLower,
			deleteUpper:  deleteUpper,
			dataValues:   map[int]int64{1: 1},
			dataNulls:    map[int]int64{1: 0},
			deleteValues: map[int]int64{1: 1},
			deleteNulls:  map[int]int64{1: 0},
			wantFiles:    1,
		},
		{
			name:         "missing delete bounds",
			dataLower:    dataLower,
			dataUpper:    dataUpper,
			dataValues:   map[int]int64{1: 1},
			dataNulls:    map[int]int64{1: 0},
			deleteValues: map[int]int64{1: 1},
			deleteNulls:  map[int]int64{1: 0},
			wantFiles:    1,
		},
		{
			name:         "unknown equality field",
			dataLower:    dataLower,
			dataUpper:    dataUpper,
			deleteLower:  deleteLower,
			deleteUpper:  deleteUpper,
			dataValues:   map[int]int64{1: 1},
			dataNulls:    map[int]int64{1: 0},
			deleteValues: map[int]int64{1: 1},
			deleteNulls:  map[int]int64{1: 0},
			fieldID:      99,
			wantFiles:    1,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			fieldID := tt.fieldID
			if fieldID == 0 {
				fieldID = 1
			}
			deleteEntry := newEqualityDeleteMetricsTestEntry(
				"delete.parquet", 1, partition, iceberg.EntryContentEqDeletes, 2,
				[]int{fieldID}, tt.deleteValues, tt.deleteNulls, nil,
				tt.deleteLower, tt.deleteUpper)
			idx, err := buildEqualityDeleteIndex(
				[]iceberg.ManifestEntry{deleteEntry}, equalityDeleteIndexTestSpecs(), schema)
			require.NoError(t, err)

			dataEntry := newEqualityDeleteMetricsTestEntry(
				"data.parquet", 1, partition, iceberg.EntryContentData, 1,
				nil, tt.dataValues, tt.dataNulls, nil, tt.dataLower, tt.dataUpper)
			matched, err := idx.forDataFile(dataEntry)
			require.NoError(t, err)
			assert.Len(t, matched, tt.wantFiles)
		})
	}
}

func TestEqualityDeleteIndexDoesNotUseUncertainFloatBounds(t *testing.T) {
	schema := equalityDeleteMetricsTestSchema(iceberg.PrimitiveTypes.Float32, true)
	partition := map[int]any{1000: int32(0)}
	dataLower, dataUpper := equalityDeleteMetricsTestBounds(t, float32(0), float32(10))
	deleteLower, deleteUpper := equalityDeleteMetricsTestBounds(t, float32(20), float32(30))

	deleteEntry := newEqualityDeleteMetricsTestEntry(
		"delete.parquet", 1, partition, iceberg.EntryContentEqDeletes, 2,
		[]int{1}, map[int]int64{1: 1}, map[int]int64{1: 0}, nil,
		deleteLower, deleteUpper)
	idx, err := buildEqualityDeleteIndex(
		[]iceberg.ManifestEntry{deleteEntry}, equalityDeleteIndexTestSpecs(), schema)
	require.NoError(t, err)

	dataEntry := newEqualityDeleteMetricsTestEntry(
		"data.parquet", 1, partition, iceberg.EntryContentData, 1,
		nil, map[int]int64{1: 1}, map[int]int64{1: 0}, nil,
		dataLower, dataUpper)
	matched, err := idx.forDataFile(dataEntry)
	require.NoError(t, err)

	assert.Len(t, matched, 1, "missing NaN counts must keep float ranges conservative")

	dataEntry = newEqualityDeleteMetricsTestEntry(
		"data-no-nan.parquet", 1, partition, iceberg.EntryContentData, 1,
		nil, map[int]int64{1: 1}, map[int]int64{1: 0}, map[int]int64{1: 0},
		dataLower, dataUpper)
	deleteEntry = newEqualityDeleteMetricsTestEntry(
		"delete-no-nan.parquet", 1, partition, iceberg.EntryContentEqDeletes, 2,
		[]int{1}, map[int]int64{1: 1}, map[int]int64{1: 0}, map[int]int64{1: 0},
		deleteLower, deleteUpper)
	idx, err = buildEqualityDeleteIndex(
		[]iceberg.ManifestEntry{deleteEntry}, equalityDeleteIndexTestSpecs(), schema)
	require.NoError(t, err)

	matched, err = idx.forDataFile(dataEntry)
	require.NoError(t, err)
	assert.Empty(t, matched)
}

func TestEqualityDeleteMetricValueCompareSupportsRangeTypes(t *testing.T) {
	decimalLower, err := iceberg.LiteralFromBytes(iceberg.DecimalTypeOf(9, 0), []byte{1})
	require.NoError(t, err)
	decimalUpper, err := iceberg.LiteralFromBytes(iceberg.DecimalTypeOf(9, 0), []byte{2})
	require.NoError(t, err)
	fixedLower, err := iceberg.LiteralFromBytes(iceberg.FixedTypeOf(2), []byte{1, 0})
	require.NoError(t, err)
	fixedUpper, err := iceberg.LiteralFromBytes(iceberg.FixedTypeOf(2), []byte{2, 0})
	require.NoError(t, err)

	tests := []struct {
		name  string
		typ   iceberg.Type
		lower iceberg.Literal
		upper iceberg.Literal
	}{
		{"boolean", iceberg.PrimitiveTypes.Bool, iceberg.NewLiteral(false), iceberg.NewLiteral(true)},
		{"int32", iceberg.PrimitiveTypes.Int32, iceberg.NewLiteral(int32(1)), iceberg.NewLiteral(int32(2))},
		{"int64", iceberg.PrimitiveTypes.Int64, iceberg.NewLiteral(int64(1)), iceberg.NewLiteral(int64(2))},
		{"float32", iceberg.PrimitiveTypes.Float32, iceberg.NewLiteral(float32(1)), iceberg.NewLiteral(float32(2))},
		{"float64", iceberg.PrimitiveTypes.Float64, iceberg.NewLiteral(float64(1)), iceberg.NewLiteral(float64(2))},
		{"date", iceberg.PrimitiveTypes.Date, iceberg.NewLiteral(iceberg.Date(1)), iceberg.NewLiteral(iceberg.Date(2))},
		{"time", iceberg.PrimitiveTypes.Time, iceberg.NewLiteral(iceberg.Time(1)), iceberg.NewLiteral(iceberg.Time(2))},
		{"timestamp", iceberg.PrimitiveTypes.Timestamp, iceberg.NewLiteral(iceberg.Timestamp(1)), iceberg.NewLiteral(iceberg.Timestamp(2))},
		{"timestamp-nano", iceberg.PrimitiveTypes.TimestampNs, iceberg.NewLiteral(iceberg.TimestampNano(1)), iceberg.NewLiteral(iceberg.TimestampNano(2))},
		{"string", iceberg.PrimitiveTypes.String, iceberg.NewLiteral("a"), iceberg.NewLiteral("b")},
		{"binary", iceberg.PrimitiveTypes.Binary, iceberg.NewLiteral([]byte{1}), iceberg.NewLiteral([]byte{2})},
		{"fixed", iceberg.FixedTypeOf(2), fixedLower, fixedUpper},
		{
			"uuid", iceberg.PrimitiveTypes.UUID,
			iceberg.NewLiteral(uuid.MustParse("00000000-0000-0000-0000-000000000001")),
			iceberg.NewLiteral(uuid.MustParse("00000000-0000-0000-0000-000000000002")),
		},
		{"decimal", iceberg.DecimalTypeOf(9, 0), decimalLower, decimalUpper},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			kind := equalityDeleteRangeKindForType(tt.typ)
			lower, ok := equalityDeleteMetricValueFromLiteral(kind, tt.lower)
			require.True(t, ok)
			upper, ok := equalityDeleteMetricValueFromLiteral(kind, tt.upper)
			require.True(t, ok)

			want := getCmpLiteral(tt.lower)(tt.lower, tt.upper)
			got := equalityDeleteMetricValueCompare(&lower, &upper)
			assert.Equal(t, want < 0, got < 0)
			assert.Equal(t, want == 0, got == 0)
			assert.Equal(t, want > 0, got > 0)
		})
	}
}
