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
	"encoding/json"
	"math"
	"testing"

	"github.com/DataDog/iceberg-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func geoTestSchema() *iceberg.Schema {
	return iceberg.NewSchema(1,
		iceberg.NestedField{ID: 1, Name: "geom", Type: iceberg.GeometryType{}, Required: false},
		iceberg.NestedField{ID: 2, Name: "geog", Type: iceberg.GeographyType{}, Required: false},
		iceberg.NestedField{ID: 3, Name: "num", Type: iceberg.PrimitiveTypes.Int32, Required: false},
	)
}

func TestBBoxIntersectsConstruction(t *testing.T) {
	bbox := iceberg.BoundingBox{MinX: 0, MinY: 0, MaxX: 10, MaxY: 10}
	pred := iceberg.BBoxIntersects(iceberg.Reference("geom"), bbox)

	assert.Equal(t, iceberg.OpBBoxIntersects, pred.Op())
	assert.Equal(t, iceberg.Reference("geom"), pred.Term())
	assert.Contains(t, pred.String(), "BBoxIntersects")

	// Negation flips the operation and round-trips back.
	neg := pred.Negate()
	assert.Equal(t, iceberg.OpBBoxNotIntersects, neg.Op())
	assert.True(t, pred.Equals(neg.Negate()))
	assert.False(t, pred.Equals(neg))
}

func TestBBoxIntersectsNilTermPanics(t *testing.T) {
	assert.Panics(t, func() {
		iceberg.BBoxIntersects(nil, iceberg.BoundingBox{})
	})
}

func TestBoundingBoxValid(t *testing.T) {
	nan := math.NaN()
	inf := math.Inf(1)
	tests := []struct {
		name string
		bbox iceberg.BoundingBox
		want bool
	}{
		{"zero", iceberg.BoundingBox{}, true},
		{"normal", iceberg.BoundingBox{MinX: 0, MinY: 0, MaxX: 10, MaxY: 10}, true},
		{"open half-plane via inf", iceberg.BoundingBox{MinX: -inf, MinY: -inf, MaxX: inf, MaxY: inf}, true},
		{"inverted x", iceberg.BoundingBox{MinX: 10, MinY: 0, MaxX: 0, MaxY: 10}, false},
		{"inverted y", iceberg.BoundingBox{MinX: 0, MinY: 10, MaxX: 10, MaxY: 0}, false},
		{"nan min", iceberg.BoundingBox{MinX: nan, MinY: 0, MaxX: 10, MaxY: 10}, false},
		{"nan max", iceberg.BoundingBox{MinX: 0, MinY: 0, MaxX: 10, MaxY: nan}, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.want, tt.bbox.Valid())
		})
	}
}

func TestBBoxIntersectsInvalidBoxPanics(t *testing.T) {
	// An inverted or NaN box would silently mis-prune, so construction rejects it.
	assert.Panics(t, func() {
		iceberg.BBoxIntersects(iceberg.Reference("geom"),
			iceberg.BoundingBox{MinX: 10, MinY: 0, MaxX: 0, MaxY: 10})
	})
	assert.Panics(t, func() {
		iceberg.BBoxIntersects(iceberg.Reference("geom"),
			iceberg.BoundingBox{MinX: math.NaN(), MinY: 0, MaxX: 10, MaxY: 10})
	})
}

func TestBBoxIntersectsBind(t *testing.T) {
	sc := geoTestSchema()
	bbox := iceberg.BoundingBox{MinX: 1, MinY: 2, MaxX: 3, MaxY: 4}

	for _, name := range []string{"geom", "geog"} {
		t.Run(name, func(t *testing.T) {
			bound, err := iceberg.BBoxIntersects(iceberg.Reference(name), bbox).Bind(sc, true)
			require.NoError(t, err, "binding to %s", name)

			bp, ok := bound.(iceberg.BoundBBoxPredicate)
			require.True(t, ok)
			assert.Equal(t, iceberg.OpBBoxIntersects, bp.Op())
			assert.True(t, bbox.Equals(bp.BBox()))
			assert.Equal(t, name, bp.Ref().Field().Name)
		})
	}
}

func TestBBoxIntersectsBindNonGeoRejected(t *testing.T) {
	sc := geoTestSchema()
	_, err := iceberg.BBoxIntersects(iceberg.Reference("num"), iceberg.BoundingBox{}).Bind(sc, true)
	require.Error(t, err)
	assert.ErrorIs(t, err, iceberg.ErrType)
}

// TestBBoxIntersectsNotAUnaryPredicate locks in the invariant that a bound bbox
// predicate must NOT satisfy BoundUnaryPredicate. If it did (e.g. by regaining an
// AsUnbound(Reference) method), every type-switch on BoundUnaryPredicate -
// column-name translation, transform projection - would silently match it and
// rebuild it as a generic unary predicate, which reaches substrait (panic) or
// drops rows on a missing column. Bbox is dispatched via BoundBBoxPredicate only.
func TestBBoxIntersectsNotAUnaryPredicate(t *testing.T) {
	sc := geoTestSchema()
	bound, err := iceberg.BBoxIntersects(iceberg.Reference("geom"),
		iceberg.BoundingBox{MinX: 0, MinY: 0, MaxX: 10, MaxY: 10}).Bind(sc, true)
	require.NoError(t, err)

	_, isBBox := bound.(iceberg.BoundBBoxPredicate)
	require.True(t, isBBox)
	_, isUnary := bound.(iceberg.BoundUnaryPredicate)
	assert.False(t, isUnary, "a bound bbox predicate must not satisfy BoundUnaryPredicate")
}

func TestBBoxIntersectsBindUnknownField(t *testing.T) {
	sc := geoTestSchema()
	_, err := iceberg.BBoxIntersects(iceberg.Reference("missing"), iceberg.BoundingBox{}).Bind(sc, true)
	require.Error(t, err)
}

// Bound negation preserves the box and can be rewritten through RewriteNotExpr.
func TestBBoxNotIntersectsRewrite(t *testing.T) {
	sc := geoTestSchema()
	bbox := iceberg.BoundingBox{MinX: 1, MinY: 2, MaxX: 3, MaxY: 4}

	bound, err := iceberg.BBoxIntersects(iceberg.Reference("geom"), bbox).Bind(sc, true)
	require.NoError(t, err)

	rewritten, err := iceberg.RewriteNotExpr(iceberg.NewNot(bound))
	require.NoError(t, err)
	assert.Equal(t, iceberg.OpBBoxNotIntersects, rewritten.Op())
}

// Geospatial predicates have no REST JSON representation and must surface an
// error rather than silently marshalling to an empty object.
func TestBBoxIntersectsNotJSONSerializable(t *testing.T) {
	sc := geoTestSchema()
	unbound := iceberg.BBoxIntersects(iceberg.Reference("geom"), iceberg.BoundingBox{})

	_, err := json.Marshal(unbound)
	require.Error(t, err)

	bound, err := unbound.Bind(sc, true)
	require.NoError(t, err)
	_, err = json.Marshal(bound)
	require.Error(t, err)
}

// TestBBoxTranslateColumnNames guards column-name translation, the single
// function where a bbox predicate previously went wrong in two directions: with
// the geo column present it survived as a BoundUnaryPredicate and later panicked
// in substrait, and with the column absent (a file written before the geo column
// existed) it collapsed to AlwaysFalse and silently dropped every row. Both must
// resolve to AlwaysTrue so the record filter conservatively keeps every row -
// bbox pruning is the metrics evaluator's job, not the record filter's.
func TestBBoxTranslateColumnNames(t *testing.T) {
	sc := geoTestSchema()
	bound, err := iceberg.BBoxIntersects(iceberg.Reference("geom"),
		iceberg.BoundingBox{MinX: 0, MinY: 0, MaxX: 10, MaxY: 10}).Bind(sc, true)
	require.NoError(t, err)

	t.Run("column present", func(t *testing.T) {
		out, err := iceberg.TranslateColumnNames(bound, sc)
		require.NoError(t, err)
		assert.Equal(t, iceberg.AlwaysTrue{}, out)
	})

	t.Run("column absent (schema evolution)", func(t *testing.T) {
		// A file schema that predates the geometry column.
		fileSchema := iceberg.NewSchema(1,
			iceberg.NestedField{ID: 3, Name: "num", Type: iceberg.PrimitiveTypes.Int32})
		out, err := iceberg.TranslateColumnNames(bound, fileSchema)
		require.NoError(t, err)
		assert.Equal(t, iceberg.AlwaysTrue{}, out,
			"a file missing the geo column must keep every row, not drop them")
	})
}

// SanitizeExpression must not fail on a bbox predicate: it has no user literal to
// mask and no REST JSON form, so it collapses to always-true while the rest of
// the expression sanitizes normally. Covers both the unbound path (how a scan's
// row filter is stored) and the bound path.
func TestBBoxIntersectsSanitize(t *testing.T) {
	sc := geoTestSchema()
	bbox := iceberg.BoundingBox{MinX: 0, MinY: 0, MaxX: 10, MaxY: 10}
	unbound := iceberg.BBoxIntersects(iceberg.Reference("geom"), bbox)

	// Unbound path: scan.rowFilter is stored unbound.
	sanitized, err := iceberg.SanitizeExpression(unbound)
	require.NoError(t, err)
	assert.Equal(t, iceberg.AlwaysTrue{}, sanitized)

	// Bound path.
	bound, err := unbound.Bind(sc, true)
	require.NoError(t, err)
	sanitized, err = iceberg.SanitizeExpression(bound)
	require.NoError(t, err)
	assert.Equal(t, iceberg.AlwaysTrue{}, sanitized)

	// Within a larger expression, the non-geo conjunct still sanitizes and the
	// result serializes to Expression JSON.
	combined, err := iceberg.SanitizeExpression(iceberg.NewAnd(unbound,
		iceberg.EqualTo(iceberg.Reference("num"), int32(5))))
	require.NoError(t, err)
	_, err = json.Marshal(combined)
	require.NoError(t, err)
}
