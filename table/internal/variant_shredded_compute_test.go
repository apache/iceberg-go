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
	"context"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/compute"
	"github.com/apache/arrow-go/v18/arrow/extensions"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/arrow-go/v18/parquet/variant"
	"github.com/stretchr/testify/require"
)

func TestComputeOnShreddedVariant(t *testing.T) {
	mem := memory.DefaultAllocator
	ctx := compute.WithAllocator(context.Background(), mem)

	shreddedType := extensions.NewShreddedVariantType(arrow.StructOf(
		arrow.Field{Name: "a", Type: arrow.PrimitiveTypes.Int64},
	))
	bldr := extensions.NewVariantBuilder(mem, shreddedType)
	defer bldr.Release()
	for i := 0; i < 5; i++ {
		var b variant.Builder
		require.NoError(t, b.Append(map[string]any{"a": int64(i), "city": "NYC"}))
		v, err := b.Build()
		require.NoError(t, err)
		bldr.Append(v)
	}
	arr := bldr.NewArray()
	defer arr.Release()

	sc := arrow.NewSchema([]arrow.Field{{Name: "payload", Type: shreddedType, Nullable: true}}, nil)
	batch := array.NewRecordBatch(sc, []arrow.Array{arr}, 5)
	defer batch.Release()

	idxBldr := array.NewInt64Builder(mem)
	defer idxBldr.Release()
	idxBldr.AppendValues([]int64{0, 2, 4}, nil)
	indices := idxBldr.NewInt64Array()
	defer indices.Release()

	out, err := compute.Take(ctx, *compute.DefaultTakeOptions(),
		compute.NewDatumWithoutOwning(batch), compute.NewDatumWithoutOwning(indices))
	require.NoError(t, err, "compute.Take must tolerate a shredded variant column")
	defer out.Release()
	require.EqualValues(t, 3, out.(*compute.RecordDatum).Value.NumRows())

	maskBldr := array.NewBooleanBuilder(mem)
	defer maskBldr.Release()
	maskBldr.AppendValues([]bool{true, false, true, false, true}, nil)
	mask := maskBldr.NewBooleanArray()
	defer mask.Release()

	filtered, err := compute.FilterRecordBatch(ctx, batch, mask, compute.DefaultFilterOptions())
	require.NoError(t, err, "compute.FilterRecordBatch must tolerate a shredded variant column")
	defer filtered.Release()
	require.EqualValues(t, 3, filtered.NumRows())
}
