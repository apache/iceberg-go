package iceberg

import (
	"testing"

	"github.com/hamba/avro/v2"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestPartitionTypeToAvroSchemaNullable(t *testing.T) {
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

	schema, err := partitionTypeToAvroSchema(partitionType)
	require.NoError(t, err)
	require.NotNil(t, schema)

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

	encoded, err := avro.Marshal(schema, partitionData)
	require.NoError(t, err)
	require.NotEmpty(t, encoded)

	var decoded map[string]any
	err = avro.Unmarshal(schema, encoded, &decoded)
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
}
