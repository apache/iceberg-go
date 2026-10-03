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

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/decimal128"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/iceberg-go"
	"github.com/stretchr/testify/require"
)

func TestInspectPartitionBuilderAppendsTypedAndFallbackValues(t *testing.T) {
	tests := []struct {
		name  string
		typ   iceberg.Type
		value any
		check func(*testing.T, arrow.Array)
	}{
		{
			name:  "boolean",
			typ:   iceberg.PrimitiveTypes.Bool,
			value: true,
			check: func(t *testing.T, value arrow.Array) {
				require.True(t, value.(*array.Boolean).Value(0))
			},
		},
		{
			name:  "int32",
			typ:   iceberg.PrimitiveTypes.Int32,
			value: int32(32),
			check: func(t *testing.T, value arrow.Array) {
				require.Equal(t, int32(32), value.(*array.Int32).Value(0))
			},
		},
		{
			name:  "int64",
			typ:   iceberg.PrimitiveTypes.Int64,
			value: int64(64),
			check: func(t *testing.T, value arrow.Array) {
				require.Equal(t, int64(64), value.(*array.Int64).Value(0))
			},
		},
		{
			name:  "float32",
			typ:   iceberg.PrimitiveTypes.Float32,
			value: float32(1.25),
			check: func(t *testing.T, value arrow.Array) {
				require.Equal(t, float32(1.25), value.(*array.Float32).Value(0))
			},
		},
		{
			name:  "float64",
			typ:   iceberg.PrimitiveTypes.Float64,
			value: float64(2.5),
			check: func(t *testing.T, value arrow.Array) {
				require.Equal(t, float64(2.5), value.(*array.Float64).Value(0))
			},
		},
		{
			name:  "string",
			typ:   iceberg.PrimitiveTypes.String,
			value: "partition",
			check: func(t *testing.T, value arrow.Array) {
				require.Equal(t, "partition", value.(*array.String).Value(0))
			},
		},
		{
			name:  "binary",
			typ:   iceberg.PrimitiveTypes.Binary,
			value: []byte{0, 1, 255},
			check: func(t *testing.T, value arrow.Array) {
				require.Equal(t, []byte{0, 1, 255}, value.(*array.Binary).Value(0))
			},
		},
		{
			name:  "date scalar fallback",
			typ:   iceberg.PrimitiveTypes.Date,
			value: iceberg.Date(12),
			check: func(t *testing.T, value arrow.Array) {
				require.Equal(t, arrow.Date32(12), value.(*array.Date32).Value(0))
			},
		},
		{
			name:  "time scalar fallback",
			typ:   iceberg.PrimitiveTypes.Time,
			value: iceberg.Time(1234),
			check: func(t *testing.T, value arrow.Array) {
				require.Equal(t, arrow.Time64(1234), value.(*array.Time64).Value(0))
			},
		},
		{
			name:  "timestamp scalar fallback",
			typ:   iceberg.PrimitiveTypes.Timestamp,
			value: iceberg.Timestamp(1234),
			check: func(t *testing.T, value arrow.Array) {
				require.Equal(t, arrow.Timestamp(1234), value.(*array.Timestamp).Value(0))
			},
		},
		{
			name:  "nanosecond timestamp scalar fallback",
			typ:   iceberg.PrimitiveTypes.TimestampNs,
			value: iceberg.TimestampNano(1234),
			check: func(t *testing.T, value arrow.Array) {
				require.Equal(t, arrow.Timestamp(1234), value.(*array.Timestamp).Value(0))
			},
		},
		{
			name:  "decimal scalar fallback",
			typ:   iceberg.DecimalTypeOf(10, 2),
			value: iceberg.DecimalLiteral{Val: decimal128.FromI64(123), Scale: 2},
			check: func(t *testing.T, value arrow.Array) {
				require.Equal(t, decimal128.FromI64(123), value.(*array.Decimal128).Value(0))
			},
		},
		{
			name:  "fixed scalar fallback",
			typ:   iceberg.FixedTypeOf(3),
			value: []byte("abc"),
			check: func(t *testing.T, value arrow.Array) {
				require.Equal(t, []byte("abc"), value.(*array.FixedSizeBinary).Value(0))
			},
		},
	}

	fields := make([]iceberg.NestedField, len(tests))
	values := make(map[int]any, len(tests))
	for i, tt := range tests {
		fields[i] = iceberg.NestedField{ID: i + 2, Name: tt.name, Type: tt.typ}
		values[i+2] = tt.value
	}
	builder, partitionBuilder := newInspectPartitionBuilderForTest(t, fields)
	require.NoError(t, partitionBuilder.append(values))

	partition := builder.NewArray().(*array.Struct)
	defer partition.Release()
	for i, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tt.check(t, partition.Field(i))
		})
	}
}

func TestInspectPartitionBuilderAppendsNullValues(t *testing.T) {
	builder, partitionBuilder := newInspectPartitionBuilderForTest(t, []iceberg.NestedField{
		{ID: 2, Name: "value", Type: iceberg.PrimitiveTypes.Int32},
	})
	require.NoError(t, partitionBuilder.append(nil))

	partition := builder.NewArray().(*array.Struct)
	defer partition.Release()
	require.True(t, partition.Field(0).IsNull(0))
}

func TestInspectPartitionBuilderScalarFallbackPreservesValidation(t *testing.T) {
	tests := []struct {
		name    string
		typ     iceberg.Type
		value   any
		wantErr string
	}{
		{name: "compatible integer string", typ: iceberg.PrimitiveTypes.Int32, value: "42"},
		{name: "invalid integer string", typ: iceberg.PrimitiveTypes.Int32, value: "not-an-int", wantErr: "partition field \"value\": strconv.ParseInt"},
		{name: "wrong date value", typ: iceberg.PrimitiveTypes.Date, value: int64(1), wantErr: "partition field \"value\": unsupported date"},
		{name: "wrong fixed width", typ: iceberg.FixedTypeOf(2), value: []byte("x"), wantErr: "invalid scalar value of len"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			builder, partitionBuilder := newInspectPartitionBuilderForTest(t, []iceberg.NestedField{
				{ID: 2, Name: "value", Type: tt.typ},
			})
			err := partitionBuilder.append(map[int]any{2: tt.value})
			if tt.wantErr != "" {
				require.ErrorContains(t, err, tt.wantErr)

				return
			}
			require.NoError(t, err)
			partition := builder.NewArray().(*array.Struct)
			defer partition.Release()
			require.Equal(t, int32(42), partition.Field(0).(*array.Int32).Value(0))
		})
	}
}

func newInspectPartitionBuilderForTest(
	t *testing.T,
	fields []iceberg.NestedField,
) (*array.StructBuilder, *inspectPartitionBuilder) {
	t.Helper()
	partitionType := &iceberg.StructType{FieldList: fields}
	schema, err := SchemaToArrowSchema(iceberg.NewSchema(0, iceberg.NestedField{
		ID: 1, Name: "partition", Type: partitionType, Required: true,
	}), nil, true, false)
	require.NoError(t, err)
	structType, ok := schema.Field(0).Type.(*arrow.StructType)
	require.True(t, ok)
	builder := array.NewStructBuilder(memory.DefaultAllocator, structType)
	t.Cleanup(builder.Release)
	partitionBuilder, err := newInspectPartitionBuilder(builder, partitionType)
	require.NoError(t, err)

	return builder, partitionBuilder
}
