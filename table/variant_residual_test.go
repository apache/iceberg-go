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
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestAppendExtractLiteral covers every extract target builder via the same TypeToArrowType path buildExtractColumn uses.
func TestAppendExtractLiteral(t *testing.T) {
	mem := memory.DefaultAllocator

	for _, tt := range []struct {
		name string
		typ  iceberg.PrimitiveType
		lit  iceberg.Literal
	}{
		{"bool", iceberg.PrimitiveTypes.Bool, iceberg.NewLiteral(true)},
		{"int32", iceberg.PrimitiveTypes.Int32, iceberg.NewLiteral(int32(5))},
		{"int64", iceberg.PrimitiveTypes.Int64, iceberg.NewLiteral(int64(5))},
		{"float32", iceberg.PrimitiveTypes.Float32, iceberg.NewLiteral(float32(1.5))},
		{"float64", iceberg.PrimitiveTypes.Float64, iceberg.NewLiteral(float64(1.5))},
		{"string", iceberg.PrimitiveTypes.String, iceberg.NewLiteral("hi")},
		{"binary", iceberg.PrimitiveTypes.Binary, iceberg.NewLiteral([]byte{1, 2})},
		{"uuid", iceberg.PrimitiveTypes.UUID, iceberg.NewLiteral(uuid.UUID{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16})},
		{"fixed", iceberg.FixedTypeOf(16), iceberg.NewLiteral(make([]byte, 16))},
		{"date", iceberg.PrimitiveTypes.Date, iceberg.NewLiteral(iceberg.Date(100))},
		{"time", iceberg.PrimitiveTypes.Time, iceberg.NewLiteral(iceberg.Time(1_000_000))},
		{"timestamp micros", iceberg.PrimitiveTypes.Timestamp, iceberg.NewLiteral(iceberg.Timestamp(123))},
		{"timestamp nanos", iceberg.PrimitiveTypes.TimestampNs, iceberg.NewLiteral(iceberg.TimestampNano(123))},
		{"decimal", iceberg.DecimalTypeOf(10, 2), iceberg.NewLiteral(iceberg.Decimal{Val: decimal128.FromI64(1234), Scale: 2})},
	} {
		t.Run(tt.name, func(t *testing.T) {
			dt, err := TypeToArrowType(tt.typ, false, false)
			require.NoError(t, err)

			bldr := array.NewBuilder(mem, dt)
			defer bldr.Release()

			require.NoError(t, appendExtractLiteral(bldr, tt.lit))
			arr := bldr.NewArray()
			defer arr.Release()
			assert.Equal(t, 1, arr.Len())
		})
	}
}

func TestAppendExtractLiteralUnsupported(t *testing.T) {
	bldr := array.NewBuilder(memory.DefaultAllocator, arrow.ListOf(arrow.PrimitiveTypes.Int64))
	defer bldr.Release()

	require.ErrorIs(t, appendExtractLiteral(bldr, iceberg.NewLiteral(int64(1))), iceberg.ErrNotImplemented)
}

// TestAppendExtractLiteralTimestampWrongValue hits the TimestampBuilder default arm: a non-timestamp literal into a timestamp builder errors.
func TestAppendExtractLiteralTimestampWrongValue(t *testing.T) {
	dt, err := TypeToArrowType(iceberg.PrimitiveTypes.Timestamp, false, false)
	require.NoError(t, err)
	bldr := array.NewBuilder(memory.DefaultAllocator, dt)
	defer bldr.Release()

	require.ErrorIs(t, appendExtractLiteral(bldr, iceberg.NewLiteral(int64(1))), iceberg.ErrNotImplemented)
}
