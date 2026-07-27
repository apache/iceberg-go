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

package internal_test

import (
	"bytes"
	"context"
	"errors"
	"slices"
	"testing"
	"time"

	"github.com/apache/arrow-go/v18/arrow/decimal128"
	"github.com/apache/iceberg-go"
	"github.com/apache/iceberg-go/table/internal"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestMetricsModePairs(t *testing.T) {
	tests := []struct {
		str      string
		expected internal.MetricsMode
	}{
		{"none", internal.MetricsMode{Typ: internal.MetricModeNone}},
		{"nOnE", internal.MetricsMode{Typ: internal.MetricModeNone}},
		{"counts", internal.MetricsMode{Typ: internal.MetricModeCounts}},
		{"Counts", internal.MetricsMode{Typ: internal.MetricModeCounts}},
		{"full", internal.MetricsMode{Typ: internal.MetricModeFull}},
		{"FuLl", internal.MetricsMode{Typ: internal.MetricModeFull}},
		{" FuLl", internal.MetricsMode{Typ: internal.MetricModeFull}},
		{"truncate(16)", internal.MetricsMode{Typ: internal.MetricModeTruncate, Len: 16}},
		{"trUncatE(16)", internal.MetricsMode{Typ: internal.MetricModeTruncate, Len: 16}},
		{"trUncatE(7)", internal.MetricsMode{Typ: internal.MetricModeTruncate, Len: 7}},
		{"trUncatE(07)", internal.MetricsMode{Typ: internal.MetricModeTruncate, Len: 7}},
	}

	for _, tt := range tests {
		t.Run(tt.str, func(t *testing.T) {
			mode, err := internal.MatchMetricsMode(tt.str)
			require.NoError(t, err)
			assert.Equal(t, tt.expected, mode)
		})
	}

	_, err := internal.MatchMetricsMode("truncatE(-7)")
	assert.ErrorContains(t, err, "malformed truncate metrics mode: truncatE(-7)")

	_, err = internal.MatchMetricsMode("truncatE(0)")
	assert.ErrorContains(t, err, "invalid truncate length: 0")
}

func TestTruncateUpperBoundString(t *testing.T) {
	assert.Equal(t, "ab", internal.TruncateUpperBoundText("aaaa", 2))
	// \u10FFFF\u10FFFF\x00
	assert.Equal(t, "", internal.TruncateUpperBoundText("\xf4\x8f\xbf\xbf\xf4\x8f\xbf\xbf\x00", 2))
}

func TestTruncateUpperBoundBinary(t *testing.T) {
	tests := []struct {
		name     string
		value    []byte
		truncate int
		expected []byte
	}{
		{"increment", []byte{0x01, 0x02, 0x03}, 2, []byte{0x01, 0x03}},
		{"carry", []byte{0x01, 0x02, 0xff, 0x03}, 3, []byte{0x01, 0x03, 0xff}},
		{"all ff", []byte{0xff, 0xff, 0x00}, 2, nil},
		{"no truncation", []byte{0x01, 0x02}, 2, []byte{0x01, 0x02}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			original := slices.Clone(tt.value)
			first := internal.TruncateUpperBoundBinary(tt.value, tt.truncate)
			second := internal.TruncateUpperBoundBinary(tt.value, tt.truncate)

			assert.Equal(t, tt.expected, first)
			assert.Equal(t, first, second)
			assert.Equal(t, original, tt.value)

			if len(first) > 0 {
				first[0] ^= 0xff
				assert.Equal(t, original, tt.value)
			}
		})
	}
}

func TestMapExecAllWorkersError(t *testing.T) {
	errFail := errors.New("worker failed")

	done := make(chan struct{})
	go func() {
		defer close(done)
		for _, err := range internal.MapExec(context.Background(), 2, slices.Values([]int{1, 2, 3, 4, 5, 6, 7, 8, 9, 10}), func(i int) (int, error) {
			return 0, errFail
		}) {
			_ = err
		}
	}()

	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("MapExec deadlocked when all workers returned errors")
	}
}

func TestMapExecFinish(t *testing.T) {
	var (
		ch = make(chan struct{}, 1)
		f  = func(i int) (int, error) {
			return i * 2, nil
		}
	)

	go func() {
		defer close(ch)
		for _, err := range internal.MapExec(context.Background(), 3, slices.Values([]int{1, 2, 3, 4, 5, 6, 7, 8, 9, 10}), f) {
			assert.NoError(t, err)
		}
	}()

	assert.Eventually(
		t,
		func() bool {
			<-ch

			return true
		},
		time.Second, 10*time.Millisecond,
	)
}

type mockStatsAgg struct {
	min, max iceberg.Literal
}

var _ internal.StatsAgg = (*mockStatsAgg)(nil)

func (m *mockStatsAgg) Min() iceberg.Literal                       { return m.min }
func (m *mockStatsAgg) Max() iceberg.Literal                       { return m.max }
func (m *mockStatsAgg) Update(stats interface{ HasMinMax() bool }) {}
func (m *mockStatsAgg) MinAsBytes() ([]byte, error)                { return nil, nil }
func (m *mockStatsAgg) MaxAsBytes() ([]byte, error)                { return nil, nil }

func TestPartitionValue_LinearTransforms(t *testing.T) {
	schema := iceberg.NewSchema(1, iceberg.NestedField{
		ID:   1,
		Name: "ts_col",
		Type: iceberg.TimestampType{},
	})

	cases := []struct {
		name      string
		transform iceberg.Transform
		min       time.Time
		max       time.Time
	}{
		{
			name:      "day",
			transform: iceberg.DayTransform{},
			min:       time.Date(2023, 5, 15, 0, 0, 0, 0, time.UTC),
			max:       time.Date(2023, 5, 15, 23, 59, 59, 0, time.UTC),
		},
		{
			name:      "month",
			transform: iceberg.MonthTransform{},
			min:       time.Date(2023, 5, 1, 0, 0, 0, 0, time.UTC),
			max:       time.Date(2023, 5, 31, 23, 59, 59, 0, time.UTC),
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			minLit := iceberg.NewLiteral(iceberg.Timestamp(tc.min.UnixMicro()))
			maxLit := iceberg.NewLiteral(iceberg.Timestamp(tc.max.UnixMicro()))

			partitionField := iceberg.PartitionField{
				SourceIDs: []int{1},
				FieldID:   100,
				Name:      tc.name + "_part",
				Transform: tc.transform,
			}

			stats := internal.DataFileStatistics{
				ColAggs: map[int]internal.StatsAgg{
					1: &mockStatsAgg{min: minLit, max: maxLit},
				},
			}

			minLitOpt := iceberg.Optional[iceberg.Literal]{Val: minLit, Valid: true}
			expected := tc.transform.Apply(minLitOpt).Val.Any()
			got := stats.PartitionValue(partitionField, schema)
			assert.Equal(t, expected, got)
		})
	}
}

func TestPartitionValue_MismatchPanics(t *testing.T) {
	schema := iceberg.NewSchema(1, iceberg.NestedField{
		ID:   1,
		Name: "ts_col",
		Type: iceberg.TimestampType{},
	})
	partitionField := iceberg.PartitionField{
		SourceIDs: []int{1},
		FieldID:   100,
		Name:      "date_part",
		Transform: iceberg.DayTransform{},
	}

	day1 := iceberg.NewLiteral(iceberg.Timestamp(time.Date(2023, 5, 15, 12, 0, 0, 0, time.UTC).UnixMicro()))
	day2 := iceberg.NewLiteral(iceberg.Timestamp(time.Date(2023, 5, 16, 12, 0, 0, 0, time.UTC).UnixMicro()))

	stats := internal.DataFileStatistics{
		ColAggs: map[int]internal.StatsAgg{
			1: &mockStatsAgg{min: day1, max: day2},
		},
	}

	assert.Panics(t, func() { stats.PartitionValue(partitionField, schema) })
}

func TestToDataFile_PartitionedCallerValueWins(t *testing.T) {
	schema := iceberg.NewSchema(0,
		iceberg.NestedField{ID: 1, Name: "id", Type: iceberg.PrimitiveTypes.Int32, Required: false},
	)
	spec := iceberg.NewPartitionSpec(iceberg.PartitionField{
		SourceIDs: []int{1},
		FieldID:   1000,
		Name:      "id",
		Transform: iceberg.IdentityTransform{},
	})

	// The column spans more than one value (1..9), so inference would panic.
	// A caller-supplied value must be recorded verbatim and short-circuit inference entirely.
	stats := &internal.DataFileStatistics{
		RecordCount: 2,
		ColAggs: map[int]internal.StatsAgg{
			1: &mockStatsAgg{
				min: iceberg.NewLiteral(int32(1)),
				max: iceberg.NewLiteral(int32(9)),
			},
		},
	}

	df := stats.ToDataFile(internal.DataFileOpts{
		Schema:          schema,
		Spec:            spec,
		Path:            "s3://bucket/data/id=42/file.parquet",
		Format:          iceberg.ParquetFile,
		Content:         iceberg.EntryContentData,
		FileSize:        1024,
		PartitionValues: map[int]any{1000: int32(42)},
	})

	require.Contains(t, df.Partition(), 1000)
	assert.EqualValues(t, 42, df.Partition()[1000],
		"a non-nil caller-supplied partition value must be recorded and must not be overwritten by inference")
}

func TestToDataFile_NonOrderPreservingNilStaysAbsent(t *testing.T) {
	schema := iceberg.NewSchema(0,
		iceberg.NestedField{ID: 1, Name: "id", Type: iceberg.PrimitiveTypes.Int32, Required: false},
	)
	spec := iceberg.NewPartitionSpec(iceberg.PartitionField{
		SourceIDs: []int{1},
		FieldID:   1000,
		Name:      "id_bucket",
		Transform: iceberg.BucketTransform{NumBuckets: 4},
	})

	// A multi-valued column with a non-order-preserving transform must not
	// trigger inference (which would panic); the nil caller value stays nil.
	stats := &internal.DataFileStatistics{
		RecordCount: 2,
		ColAggs: map[int]internal.StatsAgg{
			1: &mockStatsAgg{
				min: iceberg.NewLiteral(int32(1)),
				max: iceberg.NewLiteral(int32(9)),
			},
		},
	}

	assert.NotPanics(t, func() {
		df := stats.ToDataFile(internal.DataFileOpts{
			Schema:          schema,
			Spec:            spec,
			Path:            "s3://bucket/data/file.parquet",
			Format:          iceberg.ParquetFile,
			Content:         iceberg.EntryContentData,
			FileSize:        1024,
			PartitionValues: map[int]any{1000: nil},
		})
		require.Contains(t, df.Partition(), 1000)
		assert.Nil(t, df.Partition()[1000])
	})
}

func TestToDataFile_ReferencedDataFile(t *testing.T) {
	const ref = "s3://bucket/data/data-001.parquet"
	stats := &internal.DataFileStatistics{RecordCount: 2}
	refPtr := ref

	t.Run("position delete records referenced data file", func(t *testing.T) {
		df := stats.ToDataFile(internal.DataFileOpts{
			Schema:             iceberg.PositionalDeleteSchema,
			Spec:               *iceberg.UnpartitionedSpec,
			Path:               "s3://bucket/data/pos-del.parquet",
			Format:             iceberg.ParquetFile,
			Content:            iceberg.EntryContentPosDeletes,
			FileSize:           1024,
			ReferencedDataFile: &refPtr,
		})
		require.NotNil(t, df.ReferencedDataFile())
		assert.Equal(t, ref, *df.ReferencedDataFile())
	})

	t.Run("data content ignores referenced data file", func(t *testing.T) {
		df := stats.ToDataFile(internal.DataFileOpts{
			Schema:             iceberg.PositionalDeleteSchema,
			Spec:               *iceberg.UnpartitionedSpec,
			Path:               "s3://bucket/data/data.parquet",
			Format:             iceberg.ParquetFile,
			Content:            iceberg.EntryContentData,
			FileSize:           1024,
			ReferencedDataFile: &refPtr,
		})
		assert.Nil(t, df.ReferencedDataFile(),
			"ToDataFile must ignore ReferencedDataFile for non-position-delete content")
	})
}

func TestDataFileStatisticsDecimalPartitionManifestRoundTrip(t *testing.T) {
	schema := iceberg.NewSchema(1, iceberg.NestedField{
		ID:   1,
		Name: "price",
		Type: iceberg.DecimalTypeOf(10, 2),
	})
	spec := iceberg.NewPartitionSpecID(1, iceberg.PartitionField{
		SourceIDs: []int{1},
		FieldID:   1000,
		Name:      "price",
		Transform: iceberg.IdentityTransform{},
	})
	decimal := iceberg.Decimal{Val: decimal128.FromI64(-123), Scale: 2}
	lit := iceberg.NewLiteral(decimal)
	stats := internal.DataFileStatistics{
		RecordCount: 1,
		ColAggs: map[int]internal.StatsAgg{
			1: &mockStatsAgg{min: lit, max: lit},
		},
	}
	dataFile := stats.ToDataFile(internal.DataFileOpts{
		Schema:          schema,
		Spec:            spec,
		Path:            "s3://bucket/table/data/file.parquet",
		Format:          iceberg.ParquetFile,
		Content:         iceberg.EntryContentData,
		FileSize:        1024,
		PartitionValues: map[int]any{1000: decimal},
	})

	snapshotID := int64(10)
	sequenceNumber := int64(1)
	entry := iceberg.NewManifestEntry(
		iceberg.EntryStatusADDED,
		&snapshotID,
		&sequenceNumber,
		&sequenceNumber,
		dataFile,
	)
	var buf bytes.Buffer
	manifest, err := iceberg.WriteManifest(
		"s3://bucket/table/metadata/manifest.avro",
		&buf,
		2,
		spec,
		schema,
		snapshotID,
		[]iceberg.ManifestEntry{entry},
	)
	require.NoError(t, err)

	entries, err := iceberg.ReadManifest(manifest, bytes.NewReader(buf.Bytes()), false)
	require.NoError(t, err)
	require.Len(t, entries, 1)
	got, ok := entries[0].DataFile().Partition()[1000].(iceberg.DecimalLiteral)
	require.True(t, ok)
	assert.True(t, got.Equals(iceberg.DecimalLiteral(decimal)))
}
