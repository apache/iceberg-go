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
	"github.com/apache/arrow-go/v18/arrow/compute"
	"github.com/apache/arrow-go/v18/arrow/extensions"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/arrow-go/v18/parquet/variant"
	"github.com/apache/iceberg-go"
	"github.com/stretchr/testify/require"
)

// A positional delete (compute.Take) runs before reassembly; assert survivors
// reassemble to correct values via the real delete + projection steps.
func TestShreddedVariantSurvivesPositionalDelete(t *testing.T) {
	mem := memory.NewCheckedAllocator(memory.DefaultAllocator)
	defer mem.AssertSize(t, 0)
	ctx := compute.WithAllocator(t.Context(), mem)

	shredded := extensions.NewShreddedVariantType(arrow.StructOf(
		arrow.Field{Name: "a", Type: arrow.PrimitiveTypes.Int64},
	))
	bldr := extensions.NewVariantBuilder(mem, shredded)
	for i := 0; i < 5; i++ {
		var b variant.Builder
		require.NoError(t, b.Append(map[string]any{"a": int64(i), "city": "NYC"}))
		v, err := b.Build()
		require.NoError(t, err)
		bldr.Append(v)
	}
	arr := bldr.NewArray()
	bldr.Release()

	sc := arrow.NewSchema([]arrow.Field{{
		Name: "var", Type: shredded, Nullable: true,
		Metadata: arrow.NewMetadata([]string{"PARQUET:field_id"}, []string{"2"}),
	}}, nil)
	batch := array.NewRecordBatch(sc, []arrow.Array{arr}, 5)
	arr.Release()

	// Delete positions 1 and 3; survivors are 0, 2, 4. processPositionalDeletes
	// owns (releases) the batch it is handed.
	survivors, err := processPositionalDeletes(ctx, set[int64]{1: {}, 3: {}}, (&rowPositionSource{}).cursor())(batch)
	require.NoError(t, err)

	ice := iceberg.NewSchema(0, iceberg.NestedField{ID: 2, Name: "var", Type: iceberg.VariantType{}})
	out, err := ToRequestedSchema(ctx, ice, ice, survivors, SchemaOptions{})
	require.NoError(t, err)
	survivors.Release()

	varr := out.Column(0).(*extensions.VariantArray)
	require.False(t, varr.IsShredded(), "must reassemble to unshredded after a delete")
	require.Equal(t, 3, varr.Len())
	for i, wantA := range []int64{0, 2, 4} {
		v, err := varr.Value(i)
		require.NoError(t, err)
		obj, ok := v.Value().(variant.ObjectValue)
		require.True(t, ok, "row %d should be an object", i)
		af, err := obj.ValueByKey("a")
		require.NoError(t, err)
		require.EqualValues(t, wantA, af.Value.Value(), "survivor %d", i)
	}
	out.Release()
}
