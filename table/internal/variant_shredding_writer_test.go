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

package internal

import (
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/extensions"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func fieldIDMeta(id string) arrow.Metadata {
	return arrow.NewMetadata([]string{"PARQUET:field_id"}, []string{id})
}

func TestCanShredVariant(t *testing.T) {
	good := []arrow.DataType{
		arrow.FixedWidthTypes.Boolean,
		arrow.PrimitiveTypes.Int64,
		arrow.BinaryTypes.String,
		arrow.FixedWidthTypes.Date32,
		arrow.FixedWidthTypes.Time64us,
		&arrow.TimestampType{Unit: arrow.Microsecond, TimeZone: "UTC"},
		&arrow.TimestampType{Unit: arrow.Nanosecond},
		&arrow.Decimal128Type{Precision: 10, Scale: 2},
		extensions.NewUUIDType(),
		arrow.StructOf(arrow.Field{Name: "a", Type: arrow.PrimitiveTypes.Int64}),
		arrow.ListOf(arrow.BinaryTypes.String),
	}
	for _, dt := range good {
		assert.Truef(t, canShredVariant(dt), "%s should be shreddable", dt)
	}

	bad := []arrow.DataType{
		&arrow.FixedSizeBinaryType{ByteWidth: 16},    // panics NewVariantBuilder
		&arrow.Decimal32Type{Precision: 5, Scale: 2}, // pqarrow cannot write it
		&arrow.Decimal64Type{Precision: 14, Scale: 4},
		arrow.PrimitiveTypes.Uint64,
		arrow.FixedWidthTypes.Float16,
		arrow.FixedWidthTypes.Date64,
		arrow.FixedWidthTypes.Time32s,
		&arrow.TimestampType{Unit: arrow.Second, TimeZone: "UTC"},
		&arrow.TimestampType{Unit: arrow.Microsecond, TimeZone: "America/New_York"},
		&arrow.Time64Type{Unit: arrow.Nanosecond},
		arrow.StructOf(), // empty struct
	}
	for _, dt := range bad {
		assert.Falsef(t, canShredVariant(dt), "%s should NOT be shreddable", dt)
	}
}

func TestShreddedArrowSchemaPreservesFieldMeta(t *testing.T) {
	base := arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int64, Nullable: false, Metadata: fieldIDMeta("1")},
		{Name: "payload", Type: extensions.NewDefaultVariantType(), Nullable: true, Metadata: fieldIDMeta("2")},
	}, nil)

	inner := arrow.StructOf(arrow.Field{Name: "a", Type: arrow.PrimitiveTypes.Int64, Nullable: true})
	out := ShreddedArrowSchema(base, map[int]arrow.DataType{1: inner})

	// id untouched.
	assert.True(t, arrow.TypeEqual(arrow.PrimitiveTypes.Int64, out.Field(0).Type))
	// payload is now a shredded variant, name/nullable/field-id preserved.
	pv, ok := out.Field(1).Type.(*extensions.VariantType)
	require.True(t, ok)
	assert.NotNil(t, pv.TypedValue().Type, "payload must be shredded (has typed_value)")
	assert.Equal(t, "payload", out.Field(1).Name)
	assert.True(t, out.Field(1).Nullable)
	id, ok := out.Field(1).Metadata.GetValue("PARQUET:field_id")
	require.True(t, ok)
	assert.Equal(t, "2", id)

	// Empty / un-shreddable inference returns base unchanged.
	assert.Same(t, base, ShreddedArrowSchema(base, nil))
	assert.Same(t, base, ShreddedArrowSchema(base, map[int]arrow.DataType{1: &arrow.FixedSizeBinaryType{ByteWidth: 4}}))
}

// TestShredRecordVariantsTruthTable asserts the four (value, typed_value) outcomes
// and round-trips the result through UnshredVariant.
func TestShredRecordVariantsTruthTable(t *testing.T) {
	mem := memory.NewCheckedAllocator(memory.DefaultAllocator)
	defer mem.AssertSize(t, 0)

	// Four rows: 0 fully-shredded (residual null), 1 partial-object (residual present),
	// 2 SQL-null, 3 present variant-null.
	nb := extensions.NewVariantBuilder(mem, extensions.NewDefaultVariantType())
	defer nb.Release()
	nb.Append(mkVar(t, map[string]any{"a": bigI64, "b": "x"}))
	nb.Append(mkVar(t, map[string]any{"a": bigI64, "b": "y", "extra": "z"}))
	nb.AppendNull()
	nb.Append(mkVar(t, nil)) // present variant-null
	src := nb.NewArray().(*extensions.VariantArray)
	defer src.Release()

	base := arrow.NewSchema([]arrow.Field{
		{Name: "payload", Type: extensions.NewDefaultVariantType(), Nullable: true, Metadata: fieldIDMeta("1")},
	}, nil)
	rec := array.NewRecordBatch(base, []arrow.Array{src}, 4)
	defer rec.Release()

	inner := arrow.StructOf(
		arrow.Field{Name: "a", Type: arrow.PrimitiveTypes.Int64, Nullable: true},
		arrow.Field{Name: "b", Type: arrow.BinaryTypes.String, Nullable: true},
	)
	shreddedSchema := ShreddedArrowSchema(base, map[int]arrow.DataType{0: inner})

	out, err := ShredRecordVariants(rec, shreddedSchema, mem)
	require.NoError(t, err)
	defer out.Release()

	col := out.Column(0).(*extensions.VariantArray)
	require.True(t, col.IsShredded())
	residual := col.UntypedValues()

	// Truth table.
	assert.True(t, residual.IsNull(0), "row 0 fully shredded -> residual value null")
	assert.False(t, residual.IsNull(1), "row 1 partial object -> residual value present")
	assert.True(t, col.Storage().IsNull(2), "row 2 SQL null -> physical null")
	assert.False(t, col.Storage().IsNull(3), "row 3 present variant-null -> not physical null")

	// Round-trip through the reader path: UnshredVariant must reproduce the source.
	round, err := extensions.UnshredVariant(col, mem)
	require.NoError(t, err)
	defer round.Release()

	require.False(t, round.IsShredded())
	require.Equal(t, src.Len(), round.Len())
	for i := 0; i < src.Len(); i++ {
		assert.Equalf(t, src.Storage().IsNull(i), round.Storage().IsNull(i), "row %d null-ness", i)
		if src.Storage().IsNull(i) {
			continue
		}
		sv, err := src.Value(i)
		require.NoError(t, err)
		rv, err := round.Value(i)
		require.NoError(t, err)
		sj, err := sv.MarshalJSON()
		require.NoError(t, err)
		rj, err := rv.MarshalJSON()
		require.NoError(t, err)
		assert.JSONEqf(t, string(sj), string(rj), "row %d round-trip", i)
	}
}

// TestShredRecordVariantsPassthrough verifies a schema with no shredded variant
// returns the input record (retained), not a rebuild.
func TestShredRecordVariantsPassthrough(t *testing.T) {
	mem := memory.NewCheckedAllocator(memory.DefaultAllocator)
	defer mem.AssertSize(t, 0)

	nb := extensions.NewVariantBuilder(mem, extensions.NewDefaultVariantType())
	defer nb.Release()
	nb.Append(mkVar(t, map[string]any{"a": bigI64}))
	src := nb.NewArray().(*extensions.VariantArray)
	defer src.Release()

	base := arrow.NewSchema([]arrow.Field{
		{Name: "payload", Type: extensions.NewDefaultVariantType(), Nullable: true},
	}, nil)
	rec := array.NewRecordBatch(base, []arrow.Array{src}, 1)
	defer rec.Release()

	out, err := ShredRecordVariants(rec, base, mem) // base has no shredded variant
	require.NoError(t, err)
	defer out.Release()
	assert.Same(t, rec, out)
}
