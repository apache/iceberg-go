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

package iceberg

import (
	"encoding/binary"
	"fmt"
	"io"
	"testing"
	"time"

	"github.com/apache/iceberg-go/internal"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/twmb/avro"
)

func partitionTypeToAvroSchemaNonNullable(t *StructType) (*avro.Schema, error) {
	fields := make([]avro.SchemaField, len(t.FieldList))
	for i, f := range t.FieldList {
		var node avro.SchemaNode
		switch typ := f.Type.(type) {
		case Int32Type:
			node = internal.IntNode
		case Int64Type:
			node = internal.LongNode
		case Float32Type:
			node = internal.FloatNode
		case Float64Type:
			node = internal.DoubleNode
		case StringType:
			node = internal.StringNode
		case DateType:
			node = internal.DateNode
		case TimeType:
			node = internal.TimeNode
		case TimestampType:
			node = internal.TimestampNode
		case TimestampTzType:
			node = internal.TimestampTzNode
		case TimestampNsType:
			node = internal.TimestampNsNode
		case TimestampTzNsType:
			node = internal.TimestampTzNsNode
		case UUIDType:
			node = internal.UUIDNode
		case BooleanType:
			node = internal.BoolNode
		case BinaryType:
			node = internal.BytesNode
		case FixedType:
			node = internal.FixedNode(typ.Len())
		case DecimalType:
			node = internal.DecimalNode(typ.precision, typ.scale)
		default:
			return nil, fmt.Errorf("unsupported partition type: %s", f.Type.String())
		}

		fields[i] = avro.SchemaField{
			Name:  f.Name,
			Type:  node,
			Props: internal.WithFieldID(f.ID),
		}
	}

	node := avro.SchemaNode{
		Type:   "record",
		Name:   "r102",
		Fields: fields,
	}

	return node.Schema()
}

func TestPartitionTypeToAvroSchemaNullableAndNonNullable(t *testing.T) {
	partitionType := &StructType{
		FieldList: []NestedField{
			{ID: 1, Name: "int32_col", Type: Int32Type{}, Required: false},
			{ID: 2, Name: "int64_col", Type: Int64Type{}, Required: false},
			{ID: 3, Name: "float32_col", Type: Float32Type{}, Required: false},
			{ID: 4, Name: "float64_col", Type: Float64Type{}, Required: false},
			{ID: 5, Name: "string_col", Type: StringType{}, Required: false},
			{ID: 6, Name: "date_col", Type: DateType{}, Required: false},
			{ID: 7, Name: "time_col", Type: TimeType{}, Required: false},
			{ID: 8, Name: "timestamp_col", Type: TimestampType{}, Required: false},
			{ID: 9, Name: "timestamptz_col", Type: TimestampTzType{}, Required: false},
			{ID: 10, Name: "uuid_col", Type: UUIDType{}, Required: false},
			{ID: 11, Name: "bool_col", Type: BooleanType{}, Required: false},
			{ID: 12, Name: "binary_col", Type: BinaryType{}, Required: false},
			{ID: 13, Name: "fixed_col", Type: FixedType{len: 16}, Required: false},
			{ID: 14, Name: "decimal_col", Type: DecimalType{precision: 10, scale: 2}, Required: false},
		},
	}

	partitionData := map[string]any{
		"int32_col":       nil,
		"int64_col":       nil,
		"float32_col":     nil,
		"float64_col":     nil,
		"string_col":      nil,
		"date_col":        nil,
		"time_col":        nil,
		"timestamp_col":   nil,
		"timestamptz_col": nil,
		"uuid_col":        nil,
		"bool_col":        nil,
		"binary_col":      nil,
		"fixed_col":       nil,
		"decimal_col":     nil,
	}

	t.Run("nullable schema accepts nil", func(t *testing.T) {
		schemaNullable, err := partitionTypeToAvroSchema(partitionType)
		require.NoError(t, err)
		require.NotNil(t, schemaNullable)

		encoded, err := schemaNullable.Encode(partitionData)
		require.NoError(t, err)
		require.NotEmpty(t, encoded)

		var decoded map[string]any
		_, err = schemaNullable.Decode(encoded, &decoded)
		require.NoError(t, err)

		assert.Nil(t, decoded["int32_col"])
		assert.Nil(t, decoded["int64_col"])
		assert.Nil(t, decoded["float32_col"])
		assert.Nil(t, decoded["float64_col"])
		assert.Nil(t, decoded["string_col"])
		assert.Nil(t, decoded["date_col"])
		assert.Nil(t, decoded["time_col"])
		assert.Nil(t, decoded["timestamp_col"])
		assert.Nil(t, decoded["timestamptz_col"])
		assert.Nil(t, decoded["uuid_col"])
		assert.Nil(t, decoded["bool_col"])
		assert.Nil(t, decoded["binary_col"])
		assert.Nil(t, decoded["fixed_col"])
		assert.Nil(t, decoded["decimal_col"])
	})

	t.Run("non-nullable schema rejects nil", func(t *testing.T) {
		schemaNonNullable, err := partitionTypeToAvroSchemaNonNullable(partitionType)
		require.NoError(t, err)
		require.NotNil(t, schemaNonNullable)

		encoded, err := schemaNonNullable.Encode(partitionData)
		require.Error(t, err, "expected marshal to fail when values are nil for non-nullable schema")
		assert.Empty(t, encoded)
	})
}

func TestPartitionTypeToAvroSchemaUnknownType(t *testing.T) {
	partitionType := &StructType{FieldList: []NestedField{
		{ID: 1, Name: "dropped", Type: UnknownType{}, Required: false},
	}}

	schema, err := partitionTypeToAvroSchema(partitionType)
	require.NoError(t, err)

	encoded, err := schema.Encode(map[string]any{"dropped": nil})
	require.NoError(t, err)

	var decoded map[string]any
	_, err = schema.Decode(encoded, &decoded)
	require.NoError(t, err)
	assert.Nil(t, decoded["dropped"])
}

// TestPartitionTypeToAvroSchemaDuplicateNamedTypes verifies that a
// partition spec containing multiple fields of the same named Avro type
// (UUID, Fixed, Decimal) compiles without "duplicate named type" errors.
// Relies on twmb/avro v1.4.0's automatic named type deduplication.
func TestPartitionTypeToAvroSchemaDuplicateNamedTypes(t *testing.T) {
	cases := []struct {
		name string
		ptyp *StructType
	}{
		{
			"two uuid fields",
			&StructType{FieldList: []NestedField{
				{ID: 1, Name: "a", Type: UUIDType{}, Required: false},
				{ID: 2, Name: "b", Type: UUIDType{}, Required: false},
			}},
		},
		{
			"two fixed fields same size",
			&StructType{FieldList: []NestedField{
				{ID: 1, Name: "a", Type: FixedType{len: 8}, Required: false},
				{ID: 2, Name: "b", Type: FixedType{len: 8}, Required: false},
			}},
		},
		{
			"two decimal fields same precision/scale",
			&StructType{FieldList: []NestedField{
				{ID: 1, Name: "a", Type: DecimalType{precision: 10, scale: 2}, Required: false},
				{ID: 2, Name: "b", Type: DecimalType{precision: 10, scale: 2}, Required: false},
			}},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			s, err := partitionTypeToAvroSchema(tc.ptyp)
			require.NoError(t, err)
			require.NotNil(t, s)
		})
	}
}

// TestDayTransformPartitionAvroDateEncoding verifies that a day(ts) partition
// field is encoded with the Avro "date" logical type, not as a plain integer.
//
// The fix lives in PartitionSpec.PartitionType: it overrides DayTransform's
// ResultType (Int32) to DateType so the existing DateType branch in
// partitionTypeToAvroSchema emits DateNode automatically.
func TestDayTransformPartitionAvroDateEncoding(t *testing.T) {
	schema := NewSchema(0,
		NestedField{ID: 1, Name: "ts", Type: TimestampTzType{}, Required: true},
	)
	spec := NewPartitionSpecID(0,
		PartitionField{FieldID: 1000, SourceIDs: []int{1}, Name: "ts_day", Transform: DayTransform{}},
	)

	// PartitionType now returns DateType for day-transform fields.
	partitionType := spec.PartitionType(schema)
	require.Equal(t, PrimitiveTypes.Date, partitionType.FieldList[0].Type,
		"PartitionType must map DayTransform result to DateType")

	avroSchema, err := partitionTypeToAvroSchema(partitionType)
	require.NoError(t, err)

	// Encode a day value: 19000 days since epoch = 2022-01-08.
	encoded, err := avroSchema.Encode(map[string]any{"ts_day": int32(19000)})
	require.NoError(t, err)

	// twmb/avro decodes an Avro "date" field as time.Time, not int32.
	// This confirms the date logical type is present — a plain int field
	// decodes as int32 (verified in TestNonDayInt32PartitionAvroIntEncoding).
	var decoded map[string]any
	_, err = avroSchema.Decode(encoded, &decoded)
	require.NoError(t, err)

	got, ok := decoded["ts_day"]
	require.True(t, ok)
	_, isTime := got.(time.Time)
	assert.True(t, isTime, "day-partition field must decode as time.Time (date logical type), got %T", got)
	assert.Equal(t, time.Date(2022, time.January, 8, 0, 0, 0, 0, time.UTC), got)
}

// TestNonDayInt32PartitionAvroIntEncoding verifies that an Int32 partition
// field not produced by DayTransform keeps the plain int Avro encoding and
// decodes as int32, not time.Time.
func TestNonDayInt32PartitionAvroIntEncoding(t *testing.T) {
	schema := NewSchema(0,
		NestedField{ID: 1, Name: "bucket_id", Type: Int32Type{}, Required: true},
	)
	spec := NewPartitionSpecID(0,
		PartitionField{FieldID: 1000, SourceIDs: []int{1}, Name: "bucket_id_identity", Transform: IdentityTransform{}},
	)

	partitionType := spec.PartitionType(schema)
	avroSchema, err := partitionTypeToAvroSchema(partitionType)
	require.NoError(t, err)

	encoded, err := avroSchema.Encode(map[string]any{"bucket_id_identity": int32(42)})
	require.NoError(t, err)

	var decoded map[string]any
	_, err = avroSchema.Decode(encoded, &decoded)
	require.NoError(t, err)

	got, ok := decoded["bucket_id_identity"]
	require.True(t, ok)
	assert.IsType(t, int32(0), got, "plain Int32 partition must decode as int32, not time.Time")
	assert.Equal(t, int32(42), got)
}

// TestNanosecondPartitionAvroSchema verifies that v3 nanosecond timestamp
// types are accepted as partition types and round trip through Avro at
// nanosecond precision. The read path in manifest.go already decodes
// atype.TimestampNanos, so the write path has to emit it.
func TestNanosecondPartitionAvroSchema(t *testing.T) {
	cases := []struct {
		name string
		typ  Type
	}{
		{"timestamp_ns", TimestampNsType{}},
		{"timestamptz_ns", TimestampTzNsType{}},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			partitionType := &StructType{FieldList: []NestedField{
				{ID: 1000, Name: "ts_ns", Type: tc.typ, Required: false},
			}}

			avroSchema, err := partitionTypeToAvroSchema(partitionType)
			require.NoError(t, err)
			require.NotNil(t, avroSchema)
			nonNullableSchema, err := partitionTypeToAvroSchemaNonNullable(partitionType)
			require.NoError(t, err)

			// A value that is not a whole number of microseconds, so a
			// micros-based logical type would lose the trailing 123 ns.
			want := time.Date(2024, time.March, 1, 12, 0, 0, 456789123, time.UTC)

			nonNullableEncoded, err := nonNullableSchema.Encode(map[string]any{"ts_ns": want})
			require.NoError(t, err)
			var nonNullableDecoded map[string]any
			_, err = nonNullableSchema.Decode(nonNullableEncoded, &nonNullableDecoded)
			require.NoError(t, err)
			assert.Equal(t, want, nonNullableDecoded["ts_ns"])

			encoded, err := avroSchema.Encode(map[string]any{"ts_ns": want})
			require.NoError(t, err)

			var decoded map[string]any
			_, err = avroSchema.Decode(encoded, &decoded)
			require.NoError(t, err)

			got, ok := decoded["ts_ns"]
			require.True(t, ok)
			gotTime, isTime := got.(time.Time)
			require.True(t, isTime, "nanosecond partition field must decode as time.Time, got %T", got)
			assert.Equal(t, want.UnixNano(), gotTime.UTC().UnixNano(),
				"nanosecond partition value must round trip without precision loss")
		})
	}
}

// TestNewManifestWriterV3NanosecondPartition verifies that a v3 manifest can
// be written for a table with an identity partition on a nanosecond timestamp
// column. Before the nanosecond arms existed in partitionTypeToAvroSchema,
// this failed with "unsupported partition type: timestamp_ns".
func TestNewManifestWriterV3NanosecondPartition(t *testing.T) {
	cases := []struct {
		name string
		typ  Type
	}{
		{"timestamp_ns", TimestampNsType{}},
		{"timestamptz_ns", TimestampTzNsType{}},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			sc := NewSchema(0,
				NestedField{ID: 1, Name: "ts", Type: tc.typ, Required: true},
			)
			spec := NewPartitionSpecID(0,
				PartitionField{FieldID: 1000, SourceIDs: []int{1}, Name: "ts_identity", Transform: IdentityTransform{}},
			)

			writer, err := NewManifestWriter(3, io.Discard, spec, sc, 1)
			require.NoError(t, err)
			require.NotNil(t, writer)

			values := []TimestampNano{1709294400456789123, -123456789}
			for i, value := range values {
				builder, err := NewDataFileBuilder(spec, EntryContentData,
					fmt.Sprintf("data-%d.parquet", i), ParquetFile,
					map[int]any{1000: value}, nil, nil, 1, 1)
				require.NoError(t, err)
				require.NoError(t, writer.Add(NewManifestEntry(
					EntryStatusADDED, nil, nil, nil, builder.Build())))
			}

			manifest, err := writer.ToManifestFile("manifest.avro", 0)
			require.NoError(t, err)
			summaries := manifest.Partitions()
			require.Len(t, summaries, 1)
			assert.False(t, summaries[0].ContainsNull)
			require.NotNil(t, summaries[0].LowerBound)
			require.NotNil(t, summaries[0].UpperBound)
			assert.Equal(t, binary.LittleEndian.AppendUint64(nil, uint64(values[1])), *summaries[0].LowerBound)
			assert.Equal(t, binary.LittleEndian.AppendUint64(nil, uint64(values[0])), *summaries[0].UpperBound)
		})
	}
}
