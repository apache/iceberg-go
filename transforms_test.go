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

package iceberg_test

import (
	"bytes"
	"fmt"
	"reflect"
	"strings"
	"testing"
	"time"
	"unicode/utf8"

	"github.com/apache/arrow-go/v18/arrow/decimal"
	"github.com/apache/arrow-go/v18/arrow/decimal128"
	"github.com/apache/iceberg-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestParseTransform(t *testing.T) {
	tests := []struct {
		toparse  string
		expected iceberg.Transform
	}{
		{"identity", iceberg.IdentityTransform{}},
		{"IdEnTiTy", iceberg.IdentityTransform{}},
		{"void", iceberg.VoidTransform{}},
		{"VOId", iceberg.VoidTransform{}},
		{"year", iceberg.YearTransform{}},
		{"yEAr", iceberg.YearTransform{}},
		{"month", iceberg.MonthTransform{}},
		{"MONtH", iceberg.MonthTransform{}},
		{"day", iceberg.DayTransform{}},
		{"DaY", iceberg.DayTransform{}},
		{"hour", iceberg.HourTransform{}},
		{"hOuR", iceberg.HourTransform{}},
		{"bucket[5]", iceberg.BucketTransform{NumBuckets: 5}},
		{"bucket[100]", iceberg.BucketTransform{NumBuckets: 100}},
		{"BUCKET[5]", iceberg.BucketTransform{NumBuckets: 5}},
		{"bUCKeT[100]", iceberg.BucketTransform{NumBuckets: 100}},
		{"truncate[10]", iceberg.TruncateTransform{Width: 10}},
		{"truncate[255]", iceberg.TruncateTransform{Width: 255}},
		{"TRUNCATE[10]", iceberg.TruncateTransform{Width: 10}},
		{"tRuNCATe[255]", iceberg.TruncateTransform{Width: 255}},
	}

	for _, tt := range tests {
		t.Run(tt.toparse, func(t *testing.T) {
			transform, err := iceberg.ParseTransform(tt.toparse)
			require.NoError(t, err)
			assert.Equal(t, tt.expected, transform)

			txt, err := transform.MarshalText()
			assert.NoError(t, err)
			assert.Equal(t, strings.ToLower(tt.toparse), string(txt))
		})
	}

	errorTests := []struct {
		name    string
		toparse string
	}{
		{"foobar", "foobar"},
		{"bucket no brackets", "bucket"},
		{"truncate no brackets", "truncate"},
		{"bucket no val", "bucket[]"},
		{"truncate no val", "truncate[]"},
		{"bucket neg", "bucket[-1]"},
		{"truncate neg", "truncate[-1]"},
		{"bucket zero", "bucket[0]"},
		{"truncate zero", "truncate[0]"},
		{"bucket atoi overflow", "bucket[999999999999999999999999999999999999999]"},
		{"truncate atoi overflow", "truncate[999999999999999999999999999999999999999]"},
		{"bucket int32 overflow", "bucket[4294967296]"},
		{"truncate int32 overflow", "truncate[4294967296]"},
		{"bucket extra suffix", "bucketx[5]"},
		{"bucket extra token", "bucket_extra[5]"},
		{"truncate extra suffix", "truncatefoo[10]"},
		{"truncate extra token", "truncate_garbage[4]"},
	}

	for _, tt := range errorTests {
		t.Run(tt.name, func(t *testing.T) {
			tr, err := iceberg.ParseTransform(tt.toparse)
			assert.Nil(t, tr)
			assert.ErrorIs(t, err, iceberg.ErrInvalidTransform)
			assert.ErrorContains(t, err, tt.toparse)
		})
	}
}

func TestToHumanString(t *testing.T) {
	decVal, _ := decimal.Decimal128FromString("14.21", 4, 2)
	negDecVal, _ := decimal.Decimal128FromString("-1.50", 9, 2)

	tests := []struct {
		transform iceberg.Transform
		input     any
		expected  string
	}{
		{iceberg.YearTransform{}, int32(47), "2017"},
		{iceberg.MonthTransform{}, int32(575), "2017-12"},
		{iceberg.DayTransform{}, int32(17501), "2017-12-01"},
		{iceberg.YearTransform{}, nil, "null"},
		{iceberg.MonthTransform{}, nil, "null"},
		{iceberg.DayTransform{}, nil, "null"},
		{iceberg.HourTransform{}, nil, "null"},
		{iceberg.HourTransform{}, int32(420042), "2017-12-01-18"},
		{iceberg.YearTransform{}, int32(-1), "1969"},
		{iceberg.MonthTransform{}, int32(-1), "1969-12"},
		{iceberg.DayTransform{}, int32(-1), "1969-12-31"},
		{iceberg.HourTransform{}, int32(-1), "1969-12-31-23"},
		{iceberg.YearTransform{}, int32(0), "1970"},
		{iceberg.MonthTransform{}, int32(0), "1970-01"},
		{iceberg.DayTransform{}, int32(0), "1970-01-01"},
		{iceberg.HourTransform{}, int32(0), "1970-01-01-00"},
		{iceberg.VoidTransform{}, nil, "null"},
		{iceberg.IdentityTransform{}, nil, "null"},
		{iceberg.TruncateTransform{Width: 1}, []byte{0x00, 0x01, 0x02, 0x03}, "AAECAw=="},
		{iceberg.TruncateTransform{Width: 1}, iceberg.Decimal{
			Val: decVal, Scale: 2,
		}, "14.21"},
		{iceberg.TruncateTransform{Width: 1}, int32(123), "123"},
		{iceberg.TruncateTransform{Width: 1}, int64(123), "123"},
		{iceberg.TruncateTransform{Width: 1}, "foo", "foo"},
		{iceberg.IdentityTransform{}, nil, "null"},
		{iceberg.IdentityTransform{}, iceberg.Date(17501), "2017-12-01"},
		{iceberg.IdentityTransform{}, iceberg.Time(36775038194), "10:12:55.038194"},
		{iceberg.IdentityTransform{}, iceberg.Timestamp(1512151975038194), "2017-12-01T18:12:55.038194"},
		{iceberg.IdentityTransform{}, iceberg.TimestampNano(1512151975038194001), "2017-12-01T18:12:55.038194001"},
		{iceberg.IdentityTransform{}, int64(-1234567890000), "-1234567890000"},
		{iceberg.IdentityTransform{}, "a/b/c=d", "a/b/c=d"},
		{iceberg.IdentityTransform{}, []byte("foo"), "Zm9v"},
		{iceberg.IdentityTransform{}, iceberg.Decimal{Val: negDecVal, Scale: 2}, "-1.50"},
	}

	for _, tt := range tests {
		t.Run(tt.expected, func(t *testing.T) {
			assert.Equal(t, tt.expected, tt.transform.ToHumanStr(tt.input))
		})
	}
}

func TestToHumanStrType(t *testing.T) {
	decVal, _ := decimal.Decimal128FromString("14.21", 4, 2)
	tsMicros := iceberg.Timestamp(1705314600000000)
	tsNanos := iceberg.TimestampNano(1705314600000000001)

	tests := []struct {
		name      string
		transform iceberg.Transform
		typ       iceberg.Type
		input     any
		expected  string
	}{
		{"identity_tstz_micros", iceberg.IdentityTransform{}, iceberg.PrimitiveTypes.TimestampTz, tsMicros, "2024-01-15T10:30:00+00:00"},
		{"identity_ts_micros", iceberg.IdentityTransform{}, iceberg.PrimitiveTypes.Timestamp, tsMicros, "2024-01-15T10:30:00"},
		{"identity_tstz_nanos", iceberg.IdentityTransform{}, iceberg.PrimitiveTypes.TimestampTzNs, tsNanos, "2024-01-15T10:30:00.000000001+00:00"},
		{"identity_ts_nanos", iceberg.IdentityTransform{}, iceberg.PrimitiveTypes.TimestampNs, tsNanos, "2024-01-15T10:30:00.000000001"},
		{"identity_date", iceberg.IdentityTransform{}, iceberg.PrimitiveTypes.Date, iceberg.Date(17501), "2017-12-01"},
		{"identity_string", iceberg.IdentityTransform{}, iceberg.PrimitiveTypes.String, "a/b/c=d", "a/b/c=d"},
		{"identity_nil", iceberg.IdentityTransform{}, iceberg.PrimitiveTypes.TimestampTz, nil, "null"},
		{"year", iceberg.YearTransform{}, iceberg.PrimitiveTypes.Date, int32(47), "2017"},
		{"month", iceberg.MonthTransform{}, iceberg.PrimitiveTypes.Date, int32(575), "2017-12"},
		{"day", iceberg.DayTransform{}, iceberg.PrimitiveTypes.Date, int32(17501), "2017-12-01"},
		{"hour", iceberg.HourTransform{}, iceberg.PrimitiveTypes.TimestampTz, int32(420042), "2017-12-01-18"},
		{"year_nil", iceberg.YearTransform{}, iceberg.PrimitiveTypes.Date, nil, "null"},
		{"bucket", iceberg.BucketTransform{NumBuckets: 16}, iceberg.PrimitiveTypes.String, int32(7), "7"},
		{"bucket_nil", iceberg.BucketTransform{NumBuckets: 16}, iceberg.PrimitiveTypes.String, nil, "null"},
		{"truncate_int32", iceberg.TruncateTransform{Width: 1}, iceberg.PrimitiveTypes.Int32, int32(123), "123"},
		{"truncate_string", iceberg.TruncateTransform{Width: 1}, iceberg.PrimitiveTypes.String, "foo", "foo"},
		{"truncate_bytes", iceberg.TruncateTransform{Width: 1}, iceberg.PrimitiveTypes.Binary, []byte{0x00, 0x01, 0x02, 0x03}, "AAECAw=="},
		{"truncate_decimal", iceberg.TruncateTransform{Width: 1}, iceberg.DecimalTypeOf(4, 2), iceberg.Decimal{Val: decVal, Scale: 2}, "14.21"},
		{"truncate_nil", iceberg.TruncateTransform{Width: 1}, iceberg.PrimitiveTypes.String, nil, "null"},
		{"void", iceberg.VoidTransform{}, iceberg.PrimitiveTypes.TimestampTz, tsMicros, "null"},
		{"void_nil", iceberg.VoidTransform{}, iceberg.PrimitiveTypes.String, nil, "null"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.expected, tt.transform.ToHumanStrType(tt.typ, tt.input))
		})
	}
}

func TestPartitionToPath_TimestampTzIdentity(t *testing.T) {
	schema := iceberg.NewSchema(0,
		iceberg.NestedField{ID: 1, Name: "created_ts_tz", Type: iceberg.PrimitiveTypes.TimestampTz, Required: true},
		iceberg.NestedField{ID: 2, Name: "created_ts", Type: iceberg.PrimitiveTypes.Timestamp, Required: true},
		iceberg.NestedField{ID: 3, Name: "created_ts_tz_ns", Type: iceberg.PrimitiveTypes.TimestampTzNs, Required: true},
		iceberg.NestedField{ID: 4, Name: "created_ts_ns", Type: iceberg.PrimitiveTypes.TimestampNs, Required: true},
	)

	spec := iceberg.NewPartitionSpecID(3,
		iceberg.PartitionField{
			SourceIDs: []int{1}, FieldID: 1000,
			Transform: iceberg.IdentityTransform{}, Name: "created_ts_tz",
		},
		iceberg.PartitionField{
			SourceIDs: []int{2}, FieldID: 1001,
			Transform: iceberg.IdentityTransform{}, Name: "created_ts",
		},
		iceberg.PartitionField{
			SourceIDs: []int{3}, FieldID: 1002,
			Transform: iceberg.IdentityTransform{}, Name: "created_ts_tz_ns",
		},
		iceberg.PartitionField{
			SourceIDs: []int{4}, FieldID: 1003,
			Transform: iceberg.IdentityTransform{}, Name: "created_ts_ns",
		},
	)

	tsMicros := iceberg.Timestamp(1705314600000000)
	tsNanos := iceberg.TimestampNano(1705314600000000001)
	record := partitionRecord{tsMicros, tsMicros, tsNanos, tsNanos}

	expected := strings.Join([]string{
		"created_ts_tz=2024-01-15T10%3A30%3A00%2B00%3A00",
		"created_ts=2024-01-15T10%3A30%3A00",
		"created_ts_tz_ns=2024-01-15T10%3A30%3A00.000000001%2B00%3A00",
		"created_ts_ns=2024-01-15T10%3A30%3A00.000000001",
	}, "/")

	assert.Equal(t, expected, spec.PartitionToPath(record, schema))
}

func TestManifestPartitionVals(t *testing.T) {
	// Sanity checks that the source and result types of the transform are
	// compatible with their use to generate partition data in manifests.
	ts := time.Date(1971, 2, 10, 10, 20, 30, 4_000_000, time.UTC)
	tests := []struct {
		transform    iceberg.Transform
		input        iceberg.Literal
		expectResult iceberg.Literal
	}{
		{
			transform:    iceberg.HourTransform{},
			input:        iceberg.TimestampLiteral(ts.UnixMicro()),
			expectResult: iceberg.Int32Literal((365+40)*24 + 10),
		},
		{
			transform:    iceberg.DayTransform{},
			input:        iceberg.TimestampLiteral(ts.UnixMicro()),
			expectResult: iceberg.Int32Literal(365 + 40),
		},
		{
			transform:    iceberg.MonthTransform{},
			input:        iceberg.TimestampLiteral(ts.UnixMicro()),
			expectResult: iceberg.Int32Literal(13),
		},
		{
			transform:    iceberg.YearTransform{},
			input:        iceberg.TimestampLiteral(ts.UnixMicro()),
			expectResult: iceberg.Int32Literal(1),
		},
		{
			transform:    iceberg.TruncateTransform{Width: 100},
			input:        iceberg.Int64Literal(123456789),
			expectResult: iceberg.Int64Literal(123456700),
		},
		{
			transform:    iceberg.IdentityTransform{},
			input:        iceberg.StringLiteral("foobar"),
			expectResult: iceberg.StringLiteral("foobar"),
		},
		{
			transform:    iceberg.BucketTransform{NumBuckets: 128},
			input:        iceberg.StringLiteral("foobar"),
			expectResult: iceberg.Int32Literal(61),
		},
	}
	for _, tt := range tests {
		t.Run(reflect.TypeOf(tt.transform).String(), func(t *testing.T) {
			result := tt.transform.Apply(iceberg.Optional[iceberg.Literal]{Val: tt.input, Valid: true})
			require.True(t, result.Valid)
			assert.Equal(t, tt.expectResult, result.Val)

			schema := iceberg.NewSchema(0, iceberg.NestedField{
				Name: "abc",
				ID:   1,
				Type: tt.input.Type(),
			})
			partitionSpec := iceberg.NewPartitionSpec(iceberg.PartitionField{
				Name:      "transformed_abc",
				SourceIDs: []int{1},
				FieldID:   1000,
				Transform: tt.transform,
			})
			dataFile, err := iceberg.NewDataFileBuilder(
				partitionSpec, iceberg.EntryContentData,
				"1234.parquet", iceberg.ParquetFile,
				map[int]any{1000: result.Val.Any()},
				nil, nil,
				100, 100_000,
			)
			require.NoError(t, err)
			snapshotID := int64(123)
			seqNum := int64(456)
			manifestEntry := iceberg.NewManifestEntry(iceberg.EntryStatusADDED, &snapshotID, &seqNum, &seqNum, dataFile.Build())
			var buf bytes.Buffer
			manifestFile, err := iceberg.WriteManifest(
				"abc.avro", &buf,
				2, partitionSpec, schema, 123,
				[]iceberg.ManifestEntry{manifestEntry},
			)
			require.NoError(t, err)

			// Now make sure we can deserialize and re-serialize the manifest, too.
			entries, err := iceberg.ReadManifest(manifestFile, &buf, false)
			require.NoError(t, err)

			buf.Reset()
			_, err = iceberg.WriteManifest(
				"abc.avro", &buf,
				2, partitionSpec, schema, 123,
				entries,
			)
			require.NoError(t, err)
		})
	}
}

func TestBucketTransform_NumBucketsValidation(t *testing.T) {
	transform := iceberg.BucketTransform{}
	t.Run("ApplyRejectsInvalidBuckets", func(t *testing.T) {
		out := transform.Apply(iceberg.Optional[iceberg.Literal]{
			Valid: true,
			Val:   iceberg.Int32Literal(123),
		})
		require.False(t, out.Valid)
	})

	t.Run("TransformerRejectsInvalidBuckets", func(t *testing.T) {
		fn := transform.Transformer(iceberg.PrimitiveTypes.String)
		out := fn("abc")
		require.False(t, out.Valid)
	})

	t.Run("ProjectRejectsInvalidBuckets", func(t *testing.T) {
		schema := iceberg.NewSchema(1, iceberg.NestedField{
			ID:   1,
			Name: "id",
			Type: iceberg.PrimitiveTypes.Int64,
		})
		bound, err := iceberg.EqualTo(iceberg.Reference("id"), int64(42)).Bind(schema, true)
		require.NoError(t, err)

		_, err = transform.Project("id_bucket", bound.(iceberg.BoundPredicate))
		require.ErrorIs(t, err, iceberg.ErrInvalidArgument)
		require.ErrorContains(t, err, "numBuckets > 0")
	})
}

func TestBucketTransform_MarshalTextRejectsInvalidBuckets(t *testing.T) {
	t.Run("zero", func(t *testing.T) {
		_, err := iceberg.BucketTransform{NumBuckets: 0}.MarshalText()
		require.ErrorIs(t, err, iceberg.ErrInvalidArgument)
		require.ErrorContains(t, err, "numBuckets > 0")
	})
	t.Run("negative", func(t *testing.T) {
		_, err := iceberg.BucketTransform{NumBuckets: -1}.MarshalText()
		require.ErrorIs(t, err, iceberg.ErrInvalidArgument)
		require.ErrorContains(t, err, "numBuckets > 0")
	})
	t.Run("valid", func(t *testing.T) {
		txt, err := iceberg.BucketTransform{NumBuckets: 16}.MarshalText()
		require.NoError(t, err)
		assert.Equal(t, "bucket[16]", string(txt))
	})
}

func TestBucketTransformUnsupportedSourceTypeDoesNotPanic(t *testing.T) {
	transform := iceberg.BucketTransform{NumBuckets: 16}
	fn := transform.Transformer(iceberg.PrimitiveTypes.Bool)
	require.NotPanics(t, func() {
		result := fn(true)
		require.False(t, result.Valid)
	})
}

func TestCanTransform(t *testing.T) {
	tests := []struct {
		transform  iceberg.Transform
		allowed    []iceberg.Type
		notAllowed []iceberg.Type
	}{
		{
			transform: iceberg.IdentityTransform{},
			allowed: []iceberg.Type{
				iceberg.PrimitiveTypes.Bool, iceberg.PrimitiveTypes.Int32, iceberg.PrimitiveTypes.Int64,
				iceberg.PrimitiveTypes.Float32, iceberg.PrimitiveTypes.Float64, iceberg.PrimitiveTypes.Date,
				iceberg.PrimitiveTypes.Time, iceberg.PrimitiveTypes.Timestamp, iceberg.PrimitiveTypes.TimestampTz,
				iceberg.PrimitiveTypes.String, iceberg.PrimitiveTypes.Binary, iceberg.PrimitiveTypes.UUID,
				iceberg.DecimalTypeOf(2, 1), iceberg.FixedTypeOf(2),
			},
			notAllowed: []iceberg.Type{
				&iceberg.StructType{}, &iceberg.ListType{}, &iceberg.MapType{},
				iceberg.VariantType{},
				iceberg.GeometryType{},
				iceberg.GeographyType{},
			},
		},
		{
			transform: iceberg.VoidTransform{},
			allowed: []iceberg.Type{
				iceberg.PrimitiveTypes.Bool, iceberg.PrimitiveTypes.Int32, iceberg.PrimitiveTypes.Int64,
				iceberg.PrimitiveTypes.Float32, iceberg.PrimitiveTypes.Float64, iceberg.PrimitiveTypes.Date,
				iceberg.PrimitiveTypes.Time, iceberg.PrimitiveTypes.Timestamp, iceberg.PrimitiveTypes.TimestampTz,
				iceberg.PrimitiveTypes.TimestampNs, iceberg.PrimitiveTypes.TimestampTzNs,
				iceberg.PrimitiveTypes.String, iceberg.PrimitiveTypes.Binary, iceberg.PrimitiveTypes.UUID,
				iceberg.DecimalTypeOf(2, 1), iceberg.FixedTypeOf(2), &iceberg.StructType{}, &iceberg.ListType{}, &iceberg.MapType{},
				iceberg.VariantType{},
				iceberg.GeographyType{},
				iceberg.GeometryType{},
			},
			notAllowed: []iceberg.Type{},
		},
		{
			transform: iceberg.BucketTransform{},
			allowed: []iceberg.Type{
				iceberg.PrimitiveTypes.Int32, iceberg.PrimitiveTypes.Int64, iceberg.PrimitiveTypes.Date,
				iceberg.PrimitiveTypes.Time, iceberg.PrimitiveTypes.Timestamp, iceberg.PrimitiveTypes.TimestampTz,
				iceberg.PrimitiveTypes.TimestampNs, iceberg.PrimitiveTypes.TimestampTzNs,
				iceberg.DecimalTypeOf(2, 1), iceberg.PrimitiveTypes.String, iceberg.FixedTypeOf(2), iceberg.PrimitiveTypes.Binary,
				iceberg.PrimitiveTypes.UUID,
			},
			notAllowed: []iceberg.Type{
				iceberg.PrimitiveTypes.Bool, iceberg.PrimitiveTypes.Float32, iceberg.PrimitiveTypes.Float64,
				&iceberg.StructType{}, &iceberg.ListType{}, &iceberg.MapType{},
				iceberg.VariantType{},
				iceberg.GeometryType{},
				iceberg.GeographyType{},
			},
		},
		{
			transform: iceberg.TruncateTransform{},
			allowed: []iceberg.Type{
				iceberg.PrimitiveTypes.Int32, iceberg.PrimitiveTypes.Int64, iceberg.PrimitiveTypes.String,
				iceberg.PrimitiveTypes.Binary, iceberg.DecimalTypeOf(2, 1),
			},
			notAllowed: []iceberg.Type{
				iceberg.PrimitiveTypes.Bool, iceberg.PrimitiveTypes.Float32, iceberg.PrimitiveTypes.Float64,
				iceberg.PrimitiveTypes.Date, iceberg.PrimitiveTypes.Time, iceberg.PrimitiveTypes.Timestamp,
				iceberg.PrimitiveTypes.TimestampTz, iceberg.PrimitiveTypes.UUID, iceberg.FixedTypeOf(2),
				iceberg.PrimitiveTypes.TimestampNs, iceberg.PrimitiveTypes.TimestampTzNs,
				&iceberg.StructType{}, &iceberg.ListType{}, &iceberg.MapType{},
				iceberg.VariantType{},
				iceberg.GeometryType{},
				iceberg.GeographyType{},
			},
		},
		{
			transform: iceberg.YearTransform{},
			allowed: []iceberg.Type{
				iceberg.PrimitiveTypes.Date, iceberg.PrimitiveTypes.Timestamp, iceberg.PrimitiveTypes.TimestampTz,
				iceberg.PrimitiveTypes.TimestampNs, iceberg.PrimitiveTypes.TimestampTzNs,
			},
			notAllowed: []iceberg.Type{
				iceberg.PrimitiveTypes.Bool, iceberg.PrimitiveTypes.Int32, iceberg.PrimitiveTypes.Int64,
				iceberg.PrimitiveTypes.Float32, iceberg.PrimitiveTypes.Float64, iceberg.PrimitiveTypes.Time,
				iceberg.PrimitiveTypes.String, iceberg.PrimitiveTypes.Binary, iceberg.PrimitiveTypes.UUID,
				iceberg.DecimalTypeOf(2, 1), iceberg.FixedTypeOf(2), &iceberg.StructType{}, &iceberg.ListType{}, &iceberg.MapType{},
				iceberg.VariantType{},
				iceberg.GeographyType{},
				iceberg.GeometryType{},
			},
		},
		{
			transform: iceberg.MonthTransform{},
			allowed: []iceberg.Type{
				iceberg.PrimitiveTypes.Date, iceberg.PrimitiveTypes.Timestamp, iceberg.PrimitiveTypes.TimestampTz,
				iceberg.PrimitiveTypes.TimestampNs, iceberg.PrimitiveTypes.TimestampTzNs,
			},
			notAllowed: []iceberg.Type{
				iceberg.PrimitiveTypes.Bool, iceberg.PrimitiveTypes.Int32, iceberg.PrimitiveTypes.Int64,
				iceberg.PrimitiveTypes.Float32, iceberg.PrimitiveTypes.Float64, iceberg.PrimitiveTypes.Time,
				iceberg.PrimitiveTypes.String, iceberg.PrimitiveTypes.Binary, iceberg.PrimitiveTypes.UUID,
				iceberg.DecimalTypeOf(2, 1), iceberg.FixedTypeOf(2), &iceberg.StructType{}, &iceberg.ListType{}, &iceberg.MapType{},
				iceberg.VariantType{},
				iceberg.GeographyType{},
				iceberg.GeometryType{},
			},
		},
		{
			transform: iceberg.DayTransform{},
			allowed: []iceberg.Type{
				iceberg.PrimitiveTypes.TimestampNs, iceberg.PrimitiveTypes.TimestampTzNs,
				iceberg.PrimitiveTypes.Date, iceberg.PrimitiveTypes.Timestamp, iceberg.PrimitiveTypes.TimestampTz,
			},
			notAllowed: []iceberg.Type{
				iceberg.PrimitiveTypes.Bool, iceberg.PrimitiveTypes.Int32, iceberg.PrimitiveTypes.Int64,
				iceberg.PrimitiveTypes.Float32, iceberg.PrimitiveTypes.Float64, iceberg.PrimitiveTypes.Time,
				iceberg.PrimitiveTypes.String, iceberg.PrimitiveTypes.Binary, iceberg.PrimitiveTypes.UUID,
				iceberg.DecimalTypeOf(2, 1), iceberg.FixedTypeOf(2), &iceberg.StructType{}, &iceberg.ListType{}, &iceberg.MapType{},
				iceberg.VariantType{},
				iceberg.GeographyType{},
				iceberg.GeometryType{},
			},
		},
		{
			transform: iceberg.HourTransform{},
			allowed: []iceberg.Type{
				iceberg.PrimitiveTypes.TimestampNs, iceberg.PrimitiveTypes.TimestampTzNs,
				iceberg.PrimitiveTypes.Timestamp, iceberg.PrimitiveTypes.TimestampTz,
			},
			notAllowed: []iceberg.Type{
				iceberg.PrimitiveTypes.Bool, iceberg.PrimitiveTypes.Int32, iceberg.PrimitiveTypes.Int64,
				iceberg.PrimitiveTypes.Float32, iceberg.PrimitiveTypes.Float64, iceberg.PrimitiveTypes.Time,
				iceberg.PrimitiveTypes.String, iceberg.PrimitiveTypes.Binary, iceberg.PrimitiveTypes.UUID,
				iceberg.PrimitiveTypes.Date, iceberg.DecimalTypeOf(2, 1), iceberg.FixedTypeOf(2),
				&iceberg.StructType{}, &iceberg.ListType{}, &iceberg.MapType{},
				iceberg.VariantType{},
				iceberg.GeographyType{},
				iceberg.GeometryType{},
			},
		},
	}

	for _, tt := range tests {
		for _, typ := range tt.allowed {
			assert.True(t, tt.transform.CanTransform(typ), "%s: expected CanTransform(%T) to be true", tt.transform.String(), typ)
		}
		for _, typ := range tt.notAllowed {
			assert.False(t, tt.transform.CanTransform(typ), "%s: expected CanTransform(%T) to be false", tt.transform.String(), typ)
		}
	}
}

func TestYearMonthTransformNanoseconds(t *testing.T) {
	type testCase struct {
		name  string
		ts    iceberg.TimestampNano
		year  int32
		month int32
	}

	values := []testCase{
		{
			name:  "post-epoch",
			ts:    iceberg.TimestampNano(time.Date(2024, time.February, 3, 4, 5, 6, 789_000_000, time.UTC).UnixNano()),
			year:  54,
			month: 649,
		},
		{
			name:  "pre-epoch",
			ts:    iceberg.TimestampNano(-1),
			year:  -1,
			month: -1,
		},
	}

	tests := []struct {
		name      string
		transform iceberg.TimeTransform
		expected  func(testCase) int32
	}{
		{name: "year", transform: iceberg.YearTransform{}, expected: func(tc testCase) int32 { return tc.year }},
		{name: "month", transform: iceberg.MonthTransform{}, expected: func(tc testCase) int32 { return tc.month }},
	}

	for _, tt := range tests {
		for _, tc := range values {
			t.Run(tt.name+"/"+tc.name+"/Apply", func(t *testing.T) {
				result := tt.transform.Apply(iceberg.Optional[iceberg.Literal]{
					Valid: true,
					Val:   iceberg.NewLiteral(tc.ts),
				})
				require.True(t, result.Valid)
				assert.Equal(t, iceberg.Int32Literal(tt.expected(tc)), result.Val)
			})

			for _, srcType := range []iceberg.Type{
				iceberg.PrimitiveTypes.TimestampNs,
				iceberg.PrimitiveTypes.TimestampTzNs,
			} {
				t.Run(tt.name+"/"+tc.name+"/Transformer/"+srcType.String(), func(t *testing.T) {
					fn, err := tt.transform.Transformer(srcType)
					require.NoError(t, err)

					result := fn(tc.ts)
					require.True(t, result.Valid)
					assert.Equal(t, tt.expected(tc), result.Val)
				})
			}

			t.Run(tt.name+"/"+tc.name+"/Project", func(t *testing.T) {
				schema := iceberg.NewSchema(1, iceberg.NestedField{
					ID:   1,
					Name: "ts",
					Type: iceberg.PrimitiveTypes.TimestampNs,
				})
				bound, err := iceberg.GreaterThanEqual(
					iceberg.Reference("ts"),
					tc.ts,
				).Bind(schema, true)
				require.NoError(t, err)

				projected, err := tt.transform.Project("ts_part", bound.(iceberg.BoundPredicate))
				require.NoError(t, err)
				assert.True(t, iceberg.GreaterThanEqual(
					iceberg.Reference("ts_part"),
					tt.expected(tc),
				).Equals(projected))
			})
		}
	}
}

func TestBucketTransformTimestampNanoseconds(t *testing.T) {
	transform := iceberg.BucketTransform{NumBuckets: 16}
	values := []struct {
		name string
		ts   iceberg.TimestampNano
	}{
		{name: "post-epoch", ts: iceberg.TimestampNano(123456789)},
		{name: "pre-epoch", ts: iceberg.TimestampNano(-1)},
	}

	for _, srcType := range []iceberg.Type{
		iceberg.PrimitiveTypes.TimestampNs,
		iceberg.PrimitiveTypes.TimestampTzNs,
	} {
		t.Run(srcType.String(), func(t *testing.T) {
			require.True(t, transform.CanTransform(srcType))

			fn := transform.Transformer(srcType)
			for _, tt := range values {
				t.Run(tt.name, func(t *testing.T) {
					applied := transform.Apply(iceberg.Optional[iceberg.Literal]{
						Valid: true,
						Val:   iceberg.NewLiteral(tt.ts),
					})
					require.True(t, applied.Valid)

					var transformed iceberg.Optional[int32]
					require.NotPanics(t, func() {
						transformed = fn(tt.ts)
					})
					require.True(t, transformed.Valid)
					assert.Equal(t, applied.Val, iceberg.NewLiteral(transformed.Val))

					schema := iceberg.NewSchema(1, iceberg.NestedField{
						ID:   1,
						Name: "ts",
						Type: srcType,
					})
					bound, err := iceberg.EqualTo(iceberg.Reference("ts"), tt.ts).Bind(schema, true)
					require.NoError(t, err)

					projected, err := transform.Project("ts_bucket", bound.(iceberg.BoundPredicate))
					require.NoError(t, err)
					assert.True(t, iceberg.EqualTo(
						iceberg.Reference("ts_bucket"),
						transformed.Val,
					).Equals(projected))
				})
			}
		})
	}
}

func TestBucketTransformUnsupportedTypeReturnsInvalidOptional(t *testing.T) {
	transform := iceberg.BucketTransform{NumBuckets: 16}
	fn := transform.Transformer(iceberg.PrimitiveTypes.Bool)

	assert.NotPanics(t, func() {
		result := fn(true)
		assert.False(t, result.Valid)
	})
}

func TestHourTransformPreEpoch(t *testing.T) {
	const microsecondsPerHour = int64(time.Hour / time.Microsecond)

	tests := []struct {
		name     string
		micros   int64
		expected int32
	}{
		// post-epoch sanity checks
		{"epoch", 0, 0},
		{"1us after epoch", 1, 0},
		{"exactly 1 hour", microsecondsPerHour, 1},
		{"1.5 hours", microsecondsPerHour + microsecondsPerHour/2, 1},

		// pre-epoch: the bug produced 0 for all of these
		{"1us before epoch", -1, -1},
		{"half hour before epoch", -microsecondsPerHour / 2, -1},
		{"exactly 1 hour before epoch", -microsecondsPerHour, -1},
		{"1us more than 1 hour before epoch", -microsecondsPerHour - 1, -2},
		{"exactly 2 hours before epoch", -2 * microsecondsPerHour, -2},
	}

	transform := iceberg.HourTransform{}

	t.Run("Transformer", func(t *testing.T) {
		fn, err := transform.Transformer(iceberg.PrimitiveTypes.Timestamp)
		require.NoError(t, err)

		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				result := fn(iceberg.Timestamp(tt.micros))
				require.True(t, result.Valid)
				assert.Equal(t, tt.expected, result.Val)
			})
		}
	})

	t.Run("Apply", func(t *testing.T) {
		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				result := transform.Apply(iceberg.Optional[iceberg.Literal]{
					Valid: true,
					Val:   iceberg.TimestampLiteral(tt.micros),
				})
				require.True(t, result.Valid)
				assert.Equal(t, iceberg.Int32Literal(tt.expected), result.Val)
			})
		}
	})
}

func TestDayTransformPreEpoch(t *testing.T) {
	const microsecondsPerDay = int64((time.Hour * 24) / time.Microsecond)

	tests := []struct {
		name     string
		micros   int64
		expected int32
	}{
		// post-epoch sanity checks
		{"epoch", 0, 0},
		{"1us after epoch", 1, 0},
		{"exactly 1 day", microsecondsPerDay, 1},
		{"1.5 days", microsecondsPerDay + microsecondsPerDay/2, 1},

		// pre-epoch: truncated division would produce 0 for these
		{"1us before epoch", -1, -1},
		{"half day before epoch", -microsecondsPerDay / 2, -1},
		{"exactly 1 day before epoch", -microsecondsPerDay, -1},
		{"1us more than 1 day before epoch", -microsecondsPerDay - 1, -2},
		{"exactly 2 days before epoch", -2 * microsecondsPerDay, -2},
	}

	transform := iceberg.DayTransform{}

	t.Run("Transformer", func(t *testing.T) {
		fn, err := transform.Transformer(iceberg.PrimitiveTypes.Timestamp)
		require.NoError(t, err)

		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				result := fn(iceberg.Timestamp(tt.micros))
				require.True(t, result.Valid)
				assert.Equal(t, tt.expected, result.Val)
			})
		}
	})

	t.Run("Apply", func(t *testing.T) {
		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				result := transform.Apply(iceberg.Optional[iceberg.Literal]{
					Valid: true,
					Val:   iceberg.TimestampLiteral(tt.micros),
				})
				require.True(t, result.Valid)
				assert.Equal(t, iceberg.Int32Literal(tt.expected), result.Val)
			})
		}
	})
}

func TestHourTransformNanoseconds(t *testing.T) {
	const nanosecondsPerHour = int64(time.Hour)

	tests := []struct {
		name     string
		nanos    int64
		expected int32
	}{
		{"epoch", 0, 0},
		{"1ns after epoch", 1, 0},
		{"exactly 1 hour", nanosecondsPerHour, 1},
		{"1.5 hours", nanosecondsPerHour + nanosecondsPerHour/2, 1},
		{"1ns before epoch", -1, -1},
		{"half hour before epoch", -nanosecondsPerHour / 2, -1},
		{"exactly 1 hour before epoch", -nanosecondsPerHour, -1},
		{"1ns more than 1 hour before epoch", -nanosecondsPerHour - 1, -2},
		{"exactly 2 hours before epoch", -2 * nanosecondsPerHour, -2},
	}

	transform := iceberg.HourTransform{}

	for _, srcType := range []iceberg.Type{
		iceberg.PrimitiveTypes.TimestampNs,
		iceberg.PrimitiveTypes.TimestampTzNs,
	} {
		t.Run("Transformer/"+srcType.String(), func(t *testing.T) {
			fn, err := transform.Transformer(srcType)
			require.NoError(t, err)

			for _, tt := range tests {
				t.Run(tt.name, func(t *testing.T) {
					result := fn(iceberg.TimestampNano(tt.nanos))
					require.True(t, result.Valid)
					assert.Equal(t, tt.expected, result.Val)
				})
			}
		})
	}

	t.Run("Apply", func(t *testing.T) {
		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				result := transform.Apply(iceberg.Optional[iceberg.Literal]{
					Valid: true,
					Val:   iceberg.TimestampNsLiteral(tt.nanos),
				})
				require.True(t, result.Valid)
				assert.Equal(t, iceberg.Int32Literal(tt.expected), result.Val)
			})
		}
	})
}

func TestDayTransformNanoseconds(t *testing.T) {
	const nanosecondsPerDay = int64(time.Hour * 24)

	tests := []struct {
		name     string
		nanos    int64
		expected int32
	}{
		{"epoch", 0, 0},
		{"1ns after epoch", 1, 0},
		{"exactly 1 day", nanosecondsPerDay, 1},
		{"1.5 days", nanosecondsPerDay + nanosecondsPerDay/2, 1},
		{"1ns before epoch", -1, -1},
		{"half day before epoch", -nanosecondsPerDay / 2, -1},
		{"exactly 1 day before epoch", -nanosecondsPerDay, -1},
		{"1ns more than 1 day before epoch", -nanosecondsPerDay - 1, -2},
		{"exactly 2 days before epoch", -2 * nanosecondsPerDay, -2},
	}

	transform := iceberg.DayTransform{}

	for _, srcType := range []iceberg.Type{
		iceberg.PrimitiveTypes.TimestampNs,
		iceberg.PrimitiveTypes.TimestampTzNs,
	} {
		t.Run("Transformer/"+srcType.String(), func(t *testing.T) {
			fn, err := transform.Transformer(srcType)
			require.NoError(t, err)

			for _, tt := range tests {
				t.Run(tt.name, func(t *testing.T) {
					result := fn(iceberg.TimestampNano(tt.nanos))
					require.True(t, result.Valid)
					assert.Equal(t, tt.expected, result.Val)
				})
			}
		})
	}

	t.Run("Apply", func(t *testing.T) {
		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				result := transform.Apply(iceberg.Optional[iceberg.Literal]{
					Valid: true,
					Val:   iceberg.TimestampNsLiteral(tt.nanos),
				})
				require.True(t, result.Valid)
				assert.Equal(t, iceberg.Int32Literal(tt.expected), result.Val)
			})
		}
	})
}

func TestTruncateTransform(t *testing.T) {
	tests := []struct {
		width    int
		value    iceberg.Literal
		expected iceberg.Literal
	}{
		{10, iceberg.Int32Literal(1), iceberg.Int32Literal(0)},
		{10, iceberg.Int32Literal(-1), iceberg.Int32Literal(-10)},
		{10, iceberg.Int64Literal(1), iceberg.Int64Literal(0)},
		{10, iceberg.Int64Literal(-1), iceberg.Int64Literal(-10)},
		{50, iceberg.DecimalLiteral{
			Val:   decimal128.FromI64(1065),
			Scale: 2,
		}, iceberg.DecimalLiteral{
			Val:   decimal128.FromI64(1050),
			Scale: 2,
		}},
		{3, iceberg.StringLiteral("abcdef"), iceberg.StringLiteral("abc")},
		{
			3, iceberg.BinaryLiteral([]byte{0x01, 0x02, 0x03, 0x04, 0x05}),
			iceberg.BinaryLiteral([]byte{0x01, 0x02, 0x03}),
		},
	}

	for _, tt := range tests {
		t.Run(fmt.Sprintf("width=%d value=%v", tt.width, tt.value.Any()), func(t *testing.T) {
			transform := iceberg.TruncateTransform{Width: tt.width}
			result := transform.Apply(iceberg.Optional[iceberg.Literal]{Val: tt.value, Valid: true})
			require.True(t, result.Valid)
			assert.Equal(t, tt.expected, result.Val)
		})
	}
}

func TestTruncateTransformStringUnicode(t *testing.T) {
	tests := []struct {
		name     string
		width    int
		input    string
		expected string
	}{
		{
			name:     "truncate_unicode_string_width_2",
			width:    2,
			input:    "イロハニホヘト",
			expected: "イロ",
		},
		{
			name:     "truncate_unicode_string_width_3",
			width:    3,
			input:    "イロハニホヘト",
			expected: "イロハ",
		},
		{
			name:     "truncate_multibyte_string_width_1",
			width:    1,
			input:    "测试",
			expected: "测",
		},
		{
			name:     "truncate_mixed_unicode_and_ascii",
			width:    4,
			input:    "测试raul试测",
			expected: "测试ra",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			transform := iceberg.TruncateTransform{Width: tt.width}

			fn, err := transform.Transformer(iceberg.PrimitiveTypes.String)
			require.NoError(t, err)

			gotAny := fn(tt.input)
			got, ok := gotAny.(string)
			require.True(t, ok, "Transformer must return string for StringType")
			assert.Equal(t, tt.expected, got, "Transformer truncated value must match Java's code-point semantics")
			assert.True(t, utf8.ValidString(got),
				"Transformer output must be valid UTF-8; got bytes %x for input %q",
				got, tt.input)

			result := transform.Apply(iceberg.Optional[iceberg.Literal]{
				Val: iceberg.StringLiteral(tt.input), Valid: true,
			})
			require.True(t, result.Valid)
			assert.Equal(t, iceberg.StringLiteral(tt.expected), result.Val,
				"Apply on StringLiteral must match Transformer semantics")
			assert.True(t, utf8.ValidString(string(result.Val.(iceberg.StringLiteral))),
				"Apply output must be valid UTF-8")
		})
	}
}

func TestTruncateStringDoesNotSplitUTF8(t *testing.T) {
	transform := iceberg.TruncateTransform{Width: 1}

	out := transform.Apply(iceberg.Optional[iceberg.Literal]{
		Valid: true,
		Val:   iceberg.StringLiteral("éx"),
	})
	require.True(t, out.Valid)
	got := string(out.Val.(iceberg.StringLiteral))
	require.True(t, utf8.ValidString(got), "truncate produced invalid UTF-8: %q", got)
	assert.Equal(t, "é", got)

	fn, err := transform.Transformer(iceberg.PrimitiveTypes.String)
	require.NoError(t, err)
	transformed := fn("éx").(string)
	require.True(t, utf8.ValidString(transformed), "transformer produced invalid UTF-8: %q", transformed)
	assert.Equal(t, "é", transformed)

	schema := iceberg.NewSchema(1, iceberg.NestedField{
		ID:   1,
		Name: "s",
		Type: iceberg.PrimitiveTypes.String,
	})
	bound, err := iceberg.EqualTo(iceberg.Reference("s"), "éx").Bind(schema, true)
	require.NoError(t, err)

	projected, err := transform.Project("s_trunc", bound.(iceberg.BoundPredicate))
	require.NoError(t, err)
	assert.True(t, iceberg.EqualTo(iceberg.Reference("s_trunc"), "é").Equals(projected))

	binary := transform.Apply(iceberg.Optional[iceberg.Literal]{
		Valid: true,
		Val:   iceberg.BinaryLiteral([]byte("éx")),
	})
	require.True(t, binary.Valid)
	assert.Equal(t, iceberg.BinaryLiteral([]byte{0xc3}), binary.Val)
}
