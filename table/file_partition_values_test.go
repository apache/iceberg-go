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
	"context"
	"testing"

	"github.com/apache/arrow-go/v18/arrow/decimal128"
	"github.com/apache/iceberg-go"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
)

func TestValidFilePartitionValueType(t *testing.T) {
	fixed, err := iceberg.FixedTypeOf(2)
	require.NoError(t, err)
	dec, err := iceberg.DecimalTypeOf(3, 1)
	require.NoError(t, err)
	tests := []struct {
		typ            iceberg.Type
		valid, invalid any
	}{
		{iceberg.PrimitiveTypes.Bool, true, "true"},
		{iceberg.PrimitiveTypes.Int32, int32(1), int64(1)},
		{iceberg.PrimitiveTypes.Int64, int64(1), int32(1)},
		{iceberg.PrimitiveTypes.Float32, float32(1), float64(1)},
		{iceberg.PrimitiveTypes.Float64, float64(1), float32(1)},
		{iceberg.PrimitiveTypes.String, "x", []byte("x")},
		{iceberg.PrimitiveTypes.Binary, []byte("x"), "x"},
		{fixed, []byte("ab"), []byte("a")},
		{iceberg.PrimitiveTypes.Date, iceberg.Date(1), int32(1)},
		{iceberg.PrimitiveTypes.Time, iceberg.Time(1), int64(1)},
		{iceberg.PrimitiveTypes.Timestamp, iceberg.Timestamp(1), int64(1)},
		{iceberg.PrimitiveTypes.TimestampTz, iceberg.Timestamp(1), int64(1)},
		{iceberg.PrimitiveTypes.UUID, uuid.UUID{}, []byte("x")},
		{dec, iceberg.Decimal{Val: decimal128.FromI64(123), Scale: 1}, iceberg.Decimal{Val: decimal128.FromI64(1000), Scale: 1}},
	}
	for _, tc := range tests {
		t.Run(tc.typ.String(), func(t *testing.T) {
			require.True(t, validFilePartitionValueType(tc.typ, tc.valid))
			require.False(t, validFilePartitionValueType(tc.typ, tc.invalid))
		})
	}
}

func TestFileToDataFileRejectsUnknownTransformWithExplicitPartition(t *testing.T) {
	unknown, err := iceberg.ParseTransform("opaque-user-transform")
	require.NoError(t, err)
	spec := iceberg.NewPartitionSpecID(1, iceberg.PartitionField{
		SourceIDs: []int{1}, FieldID: 1000, Name: "opaque", Transform: unknown,
	})

	// No storage is needed: the invalid write is rejected before opening the file.
	_, err = FileToDataFileWithOptions(context.Background(), nil, "unused.parquet",
		simpleSchema(), spec, 0, nil,
		WithFilePartitionValues(map[int]any{1000: "opaque-value"}))
	require.ErrorIs(t, err, iceberg.ErrInvalidTransform)
}
