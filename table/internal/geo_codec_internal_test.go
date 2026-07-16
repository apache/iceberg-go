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
	"encoding/binary"
	"math"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/twpayne/go-geom"
	"github.com/twpayne/go-geom/encoding/wkb"
	"github.com/twpayne/go-geom/encoding/wkt"
)

// wkbOf converts a WKT string to little-endian WKB for test input.
func wkbOf(t *testing.T, s string) []byte {
	t.Helper()
	g, err := wkt.Unmarshal(s)
	require.NoError(t, err)
	b, err := wkb.Marshal(g, wkb.NDR)
	require.NoError(t, err)

	return b
}

// decodeBound decodes an Iceberg geospatial single-value bound (little-endian
// float64 coordinates in X, Y[, Z][, M] order) into its coordinate slice.
func decodeBound(t *testing.T, data []byte) []float64 {
	t.Helper()
	require.Zero(t, len(data)%8, "bound length %d is not a multiple of 8", len(data))
	coords := make([]float64, len(data)/8)
	for i := range coords {
		coords[i] = math.Float64frombits(binary.LittleEndian.Uint64(data[i*8:]))
	}

	return coords
}

func TestGeoBoundsAccumulatorXY(t *testing.T) {
	acc := newGeoBoundsAccumulator(false)
	require.NoError(t, acc.AddWKB(wkbOf(t, "POINT (30 10)")))
	require.NoError(t, acc.AddWKB(wkbOf(t, "LINESTRING (5 40, 40 5)")))
	require.NoError(t, acc.AddWKB(wkbOf(t, "POLYGON ((20 20, 25 35, 35 25, 20 20))")))

	lower, upper := acc.Bounds()
	require.Len(t, lower, 16)
	require.Len(t, upper, 16)

	assert.Equal(t, []float64{5, 5}, decodeBound(t, lower))
	assert.Equal(t, []float64{40, 40}, decodeBound(t, upper))
}

func TestGeoBoundsAccumulatorXYZ(t *testing.T) {
	acc := newGeoBoundsAccumulator(false)
	require.NoError(t, acc.AddWKB(wkbOf(t, "POINT Z (1 2 3)")))
	require.NoError(t, acc.AddWKB(wkbOf(t, "POINT Z (4 0 -1)")))

	lower, upper := acc.Bounds()
	require.Len(t, lower, 24)
	require.Len(t, upper, 24)

	assert.Equal(t, []float64{1, 0, -1}, decodeBound(t, lower))
	assert.Equal(t, []float64{4, 2, 3}, decodeBound(t, upper))
}

func TestGeoBoundsAccumulatorXYM(t *testing.T) {
	acc := newGeoBoundsAccumulator(false)
	require.NoError(t, acc.AddWKB(wkbOf(t, "POINT M (1 2 100)")))
	require.NoError(t, acc.AddWKB(wkbOf(t, "POINT M (4 0 50)")))

	lower, upper := acc.Bounds()
	// XYM is 32 bytes with the Z slot written as NaN so readers can tell it
	// apart from XYZM.
	require.Len(t, lower, 32)
	require.Len(t, upper, 32)

	lo := decodeBound(t, lower)
	assert.Equal(t, []float64{1, 0}, lo[:2])
	assert.True(t, math.IsNaN(lo[2]), "XYM lower Z slot must be NaN")
	assert.Equal(t, float64(50), lo[3])

	hi := decodeBound(t, upper)
	assert.Equal(t, []float64{4, 2}, hi[:2])
	assert.True(t, math.IsNaN(hi[2]), "XYM upper Z slot must be NaN")
	assert.Equal(t, float64(100), hi[3])
}

func TestGeoBoundsAccumulatorXYZM(t *testing.T) {
	acc := newGeoBoundsAccumulator(false)
	require.NoError(t, acc.AddWKB(wkbOf(t, "POINT ZM (1 2 3 100)")))
	require.NoError(t, acc.AddWKB(wkbOf(t, "POINT ZM (4 0 -1 50)")))

	lower, upper := acc.Bounds()
	require.Len(t, lower, 32)
	require.Len(t, upper, 32)

	assert.Equal(t, []float64{1, 0, -1, 50}, decodeBound(t, lower))
	assert.Equal(t, []float64{4, 2, 3, 100}, decodeBound(t, upper))
}

// TestGeoBoundsAccumulatorMixedZMOmitsToXY verifies the omit-on-ambiguity rule:
// a column mixing XYZ and XYM geometries (Z and M never co-occur in one row)
// must not be promoted to XYZM, since no row carries both. Emitting XYZM would
// imply every row has a valid Z and M in range and drive wrong-answer pruning,
// so the bounds collapse to a safe XY box.
func TestGeoBoundsAccumulatorMixedZMOmitsToXY(t *testing.T) {
	acc := newGeoBoundsAccumulator(false)
	require.NoError(t, acc.AddWKB(wkbOf(t, "POINT Z (1 2 3)")))
	require.NoError(t, acc.AddWKB(wkbOf(t, "POINT M (4 0 100)")))

	lower, upper := acc.Bounds()
	require.Len(t, lower, 16, "mixed XYZ/XYM must collapse to an XY box")
	require.Len(t, upper, 16)

	assert.Equal(t, []float64{1, 0}, decodeBound(t, lower))
	assert.Equal(t, []float64{4, 2}, decodeBound(t, upper))
}

// TestGeoBoundsAccumulatorMixedXYZMAndXYZDropsM verifies that an optional
// dimension present in only some geometries is dropped even when the two dims
// do co-occur somewhere: an XYZM row followed by an XYZ row keeps Z (carried by
// every geometry) but drops M (carried by only one), yielding XYZ bounds rather
// than an XYZM box that would claim the XYZ row has an M in range.
func TestGeoBoundsAccumulatorMixedXYZMAndXYZDropsM(t *testing.T) {
	acc := newGeoBoundsAccumulator(false)
	require.NoError(t, acc.AddWKB(wkbOf(t, "POINT ZM (1 2 3 100)")))
	require.NoError(t, acc.AddWKB(wkbOf(t, "POINT Z (4 0 -1)")))

	lower, upper := acc.Bounds()
	require.Len(t, lower, 24, "M carried by only one row must be dropped, leaving XYZ")
	require.Len(t, upper, 24)

	assert.Equal(t, []float64{1, 0, -1}, decodeBound(t, lower))
	assert.Equal(t, []float64{4, 2, 3}, decodeBound(t, upper))
}

func TestGeoBoundsAccumulatorGeometryCollection(t *testing.T) {
	acc := newGeoBoundsAccumulator(false)
	require.NoError(t, acc.AddWKB(wkbOf(t, "GEOMETRYCOLLECTION (POINT (4 6), LINESTRING (4 6, 7 10))")))

	lower, upper := acc.Bounds()
	assert.Equal(t, []float64{4, 6}, decodeBound(t, lower))
	assert.Equal(t, []float64{7, 10}, decodeBound(t, upper))
}

func TestGeoBoundsAccumulatorSkipsNaN(t *testing.T) {
	acc := newGeoBoundsAccumulator(false)
	require.NoError(t, acc.AddWKB(wkbOf(t, "POINT (10 10)")))
	// A NaN in Y must be skipped so it doesn't poison the Y bounds, while the
	// finite X=5 still extends the X bounds (per the spec, POINT(5 NaN)
	// contributes a value to X but none to Y).
	acc.extend(geom.NewPointFlat(geom.XY, []float64{5, math.NaN()}))
	require.NoError(t, acc.AddWKB(wkbOf(t, "POINT (20 20)")))

	lower, upper := acc.Bounds()
	assert.Equal(t, []float64{5, 10}, decodeBound(t, lower))
	assert.Equal(t, []float64{20, 20}, decodeBound(t, upper))
}

func TestGeoBoundsAccumulatorMissingDimensionNoBox(t *testing.T) {
	acc := newGeoBoundsAccumulator(false)
	// Y is always NaN: with no finite Y value, no bounding box is produced.
	acc.extend(geom.NewPointFlat(geom.XY, []float64{1, math.NaN()}))

	lower, upper := acc.Bounds()
	assert.Nil(t, lower)
	assert.Nil(t, upper)
}

func TestGeoBoundsAccumulatorEmpty(t *testing.T) {
	acc := newGeoBoundsAccumulator(false)
	lower, upper := acc.Bounds()
	assert.Nil(t, lower)
	assert.Nil(t, upper)

	agg, err := acc.StatsAgg()
	require.NoError(t, err)
	assert.Nil(t, agg)
}

// TestGeoBoundsAccumulatorGeographyOmitted verifies that geography columns
// never emit bounds. Geography edges are geodesics, so vertex min/max is not a
// safe bounding box (latitude bulge, antimeridian wraparound); omitting bounds
// keeps pruning safe until geodesic-aware computation exists.
func TestGeoBoundsAccumulatorGeographyOmitted(t *testing.T) {
	acc := newGeoBoundsAccumulator(true)
	require.NoError(t, acc.AddWKB(wkbOf(t, "POINT (170 10)")))
	require.NoError(t, acc.AddWKB(wkbOf(t, "POINT (-170 40)")))

	lower, upper := acc.Bounds()
	assert.Nil(t, lower, "geography bounds must be omitted")
	assert.Nil(t, upper, "geography bounds must be omitted")

	agg, err := acc.StatsAgg()
	require.NoError(t, err)
	assert.Nil(t, agg, "geography must produce no stats aggregator")
}

func TestGeoBoundsAccumulatorStatsAgg(t *testing.T) {
	acc := newGeoBoundsAccumulator(false)
	require.NoError(t, acc.AddWKB(wkbOf(t, "POINT (30 10)")))
	require.NoError(t, acc.AddWKB(wkbOf(t, "POINT (5 40)")))

	agg, err := acc.StatsAgg()
	require.NoError(t, err)
	require.NotNil(t, agg)

	lowerBytes, err := agg.MinAsBytes()
	require.NoError(t, err)
	upperBytes, err := agg.MaxAsBytes()
	require.NoError(t, err)

	assert.Equal(t, []float64{5, 10}, decodeBound(t, lowerBytes))
	assert.Equal(t, []float64{30, 40}, decodeBound(t, upperBytes))
}

func TestGeoBoundsAccumulatorInvalidWKB(t *testing.T) {
	acc := newGeoBoundsAccumulator(false)
	assert.Error(t, acc.AddWKB([]byte{0x01, 0x02, 0x03}))
}

// TestEncodeGeoBoundRoundTrip pins the exact byte layout of the single-value
// serialization for each dimensionality.
func TestEncodeGeoBoundRoundTrip(t *testing.T) {
	vals := [geoNumDims]float64{}
	vals[geoDimX], vals[geoDimY], vals[geoDimZ], vals[geoDimM] = 1, 2, 3, 4

	assert.Len(t, encodeGeoBound(vals, geom.XY), 16)
	assert.Equal(t, []float64{1, 2}, decodeBound(t, encodeGeoBound(vals, geom.XY)))

	assert.Len(t, encodeGeoBound(vals, geom.XYZ), 24)
	assert.Equal(t, []float64{1, 2, 3}, decodeBound(t, encodeGeoBound(vals, geom.XYZ)))

	xym := encodeGeoBound(vals, geom.XYM)
	assert.Len(t, xym, 32)
	dec := decodeBound(t, xym)
	assert.Equal(t, []float64{1, 2}, dec[:2])
	assert.True(t, math.IsNaN(dec[2]), "XYM Z slot must be NaN")
	assert.Equal(t, float64(4), dec[3])

	assert.Len(t, encodeGeoBound(vals, geom.XYZM), 32)
	assert.Equal(t, []float64{1, 2, 3, 4}, decodeBound(t, encodeGeoBound(vals, geom.XYZM)))
}
