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
	"testing"

	"github.com/apache/arrow-go/v18/parquet/variant"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// rowStruct is a minimal StructLike backing one row for eval tests.
type rowStruct []any

func (r rowStruct) Size() int            { return len(r) }
func (r rowStruct) Get(pos int) any      { return r[pos] }
func (r rowStruct) Set(pos int, val any) { r[pos] = val }

func variantObject(t *testing.T, m map[string]any) variant.Value {
	t.Helper()
	var b variant.Builder
	require.NoError(t, b.Append(m))
	v, err := b.Build()
	require.NoError(t, err)

	return v
}

func extractBindSchema() *Schema {
	return NewSchema(0,
		NestedField{ID: 1, Name: "payload", Type: VariantType{}},
		NestedField{ID: 2, Name: "name", Type: PrimitiveTypes.String},
	)
}

func TestExtractBind(t *testing.T) {
	term, err := Extract("payload", "$.a.b", PrimitiveTypes.Int64).Bind(extractBindSchema(), true)
	require.NoError(t, err)

	be, ok := term.(BoundExtract)
	require.True(t, ok)
	assert.Equal(t, "$['a']['b']", be.Path())
	assert.True(t, PrimitiveTypes.Int64.Equals(be.Type()))
	assert.Equal(t, 1, be.Ref().Field().ID)
}

// TestExtractBindBracketPath proves a bracket-notation path binds and round-trips through Path().
func TestExtractBindBracketPath(t *testing.T) {
	term, err := Extract("payload", "$['a']['b']", PrimitiveTypes.Int64).Bind(extractBindSchema(), true)
	require.NoError(t, err)

	be, ok := term.(BoundExtract)
	require.True(t, ok)
	assert.Equal(t, "$['a']['b']", be.Path())
}

func TestExtractBindRejects(t *testing.T) {
	for _, tt := range []struct {
		name   string
		term   UnboundTerm
		wantIs error
	}{
		{"non-variant source", Extract("name", "$.a", PrimitiveTypes.Int64), ErrInvalidArgument},
		{"nil target type", Extract("payload", "$.a", nil), ErrInvalidArgument},
		{"unknown target type", Extract("payload", "$.a", UnknownType{}), ErrInvalidArgument},
		{"array index path", Extract("payload", "$[0]", PrimitiveTypes.Int64), ErrInvalidArgument},
		{"unknown field", Extract("missing", "$.a", PrimitiveTypes.Int64), nil},
	} {
		t.Run(tt.name, func(t *testing.T) {
			_, err := tt.term.Bind(extractBindSchema(), true)
			if tt.wantIs != nil {
				require.ErrorIs(t, err, tt.wantIs)
			} else {
				require.Error(t, err)
			}
		})
	}
}

// TestExtractBindRequiredColumnNoFastPath: IsNull/NotNull over an extract on a required
// variant column must not collapse via the required-field fast path, since the column
// being required says nothing about the sub-path.
func TestExtractBindRequiredColumnStaysPredicate(t *testing.T) {
	schema := NewSchema(0,
		NestedField{ID: 1, Name: "payload", Type: VariantType{}, Required: true},
	)
	ext := Extract("payload", "$.a", PrimitiveTypes.Int64)

	isNull, err := IsNull(ext).Bind(schema, true)
	require.NoError(t, err)
	assert.NotEqual(t, AlwaysFalse{}, isNull, "IsNull(extract) on a required column must not bind to AlwaysFalse")
	_, ok := isNull.(BoundPredicate)
	assert.True(t, ok, "IsNull(extract) should remain a bound predicate")

	notNull, err := NotNull(ext).Bind(schema, true)
	require.NoError(t, err)
	assert.NotEqual(t, AlwaysTrue{}, notNull, "NotNull(extract) on a required column must not bind to AlwaysTrue")
	_, ok = notNull.(BoundPredicate)
	assert.True(t, ok, "NotNull(extract) should remain a bound predicate")
}

// TestBoundExtractEval covers the StructLike row-evaluation path: eval, evalToLiteral, evalIsNull.
func TestBoundExtractEval(t *testing.T) {
	schema := extractBindSchema()
	term, err := Extract("payload", "$.a", PrimitiveTypes.Int64).Bind(schema, true)
	require.NoError(t, err)
	be := term.(*boundExtract[int64])

	present := rowStruct{variantObject(t, map[string]any{"a": int64(42)}), "x"}
	absent := rowStruct{variantObject(t, map[string]any{"b": int64(1)}), "x"}

	// present path
	got := be.eval(present)
	require.True(t, got.Valid)
	assert.Equal(t, int64(42), got.Val)

	lit := be.evalToLiteral(present)
	require.True(t, lit.Valid)
	assert.Equal(t, int64(42), lit.Val.Any())
	assert.False(t, be.evalIsNull(present))

	// absent path -> not valid, IsNull true
	assert.False(t, be.eval(absent).Valid)
	assert.False(t, be.evalToLiteral(absent).Valid)
	assert.True(t, be.evalIsNull(absent))
}

// TestExtractFieldIDsIncludesVariantColumn proves a filter over an extract term reports
// the variant column's field id, so scan projection pulls the column into the read set.
func TestExtractFieldIDsIncludesVariantColumn(t *testing.T) {
	schema := extractBindSchema()
	pred := LiteralPredicate(OpEQ, Extract("payload", "$.a", PrimitiveTypes.Int64), NewLiteral(int64(5)))
	bound, err := BindExpr(schema, pred, true)
	require.NoError(t, err)

	ids, err := ExtractFieldIDs(bound)
	require.NoError(t, err)
	assert.Equal(t, []int{1}, ids, "only the variant column field id (payload=1) is in the projected read set")
}

func TestBoundExtractExtractValue(t *testing.T) {
	schema := extractBindSchema()
	term, err := Extract("payload", "$.a.b", PrimitiveTypes.String).Bind(schema, true)
	require.NoError(t, err)
	be := term.(BoundExtract)

	lit, ok := be.ExtractValue(variantObject(t, map[string]any{"a": map[string]any{"b": "deep"}}))
	require.True(t, ok)
	assert.Equal(t, "deep", lit.Any())

	_, ok = be.ExtractValue(variantObject(t, map[string]any{"a": map[string]any{"c": "other"}}))
	assert.False(t, ok, "absent nested path is not extractable")
}

func TestExtractBindAllTargetTypes(t *testing.T) {
	schema := extractBindSchema()
	for _, typ := range []PrimitiveType{
		PrimitiveTypes.Bool, PrimitiveTypes.Int32, PrimitiveTypes.Int64,
		PrimitiveTypes.Float32, PrimitiveTypes.Float64, PrimitiveTypes.Date,
		PrimitiveTypes.Time, PrimitiveTypes.Timestamp, PrimitiveTypes.TimestampTz,
		PrimitiveTypes.TimestampNs, PrimitiveTypes.TimestampTzNs, PrimitiveTypes.String,
		PrimitiveTypes.Binary, PrimitiveTypes.UUID, FixedTypeOf(16), DecimalTypeOf(10, 2),
	} {
		t.Run(typ.String(), func(t *testing.T) {
			bound, err := Extract("payload", "$.a", typ).Bind(schema, true)
			require.NoError(t, err)
			assert.True(t, bound.Type().Equals(typ), "bound type = %s, want %s", bound.Type(), typ)
			_, ok := bound.(BoundExtract)
			assert.True(t, ok, "bound term must be a BoundExtract")
		})
	}
}

func TestUnboundExtractAccessors(t *testing.T) {
	e, ok := Extract("payload", "$.a", PrimitiveTypes.Int64).(*unboundExtract)
	require.True(t, ok)
	assert.Equal(t, Reference("payload"), e.Ref())
	assert.Equal(t, "$.a", e.Path())
	assert.True(t, e.Type().Equals(PrimitiveTypes.Int64))
	assert.Contains(t, e.String(), "extract")
	assert.True(t, e.Equals(Extract("payload", "$.a", PrimitiveTypes.Int64)))
	assert.False(t, e.Equals(Extract("payload", "$.b", PrimitiveTypes.Int64)), "different path is not equal")
}
