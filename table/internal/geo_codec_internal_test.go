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

// decodePoint decodes a WKB point and returns its layout and flat coordinates.
func decodePoint(t *testing.T, data []byte) (geom.Layout, []float64) {
	t.Helper()
	g, err := wkb.Unmarshal(data)
	require.NoError(t, err)
	pt, ok := g.(*geom.Point)
	require.True(t, ok, "expected *geom.Point, got %T", g)

	return pt.Layout(), pt.FlatCoords()
}

func TestGeoBoundsAccumulatorXY(t *testing.T) {
	acc := newGeoBoundsAccumulator()
	require.NoError(t, acc.AddWKB(wkbOf(t, "POINT (30 10)")))
	require.NoError(t, acc.AddWKB(wkbOf(t, "LINESTRING (5 40, 40 5)")))
	require.NoError(t, acc.AddWKB(wkbOf(t, "POLYGON ((20 20, 25 35, 35 25, 20 20))")))

	lower, upper, err := acc.Bounds()
	require.NoError(t, err)
	require.NotNil(t, lower)
	require.NotNil(t, upper)

	layout, lo := decodePoint(t, lower)
	assert.Equal(t, geom.XY, layout)
	assert.Equal(t, []float64{5, 5}, lo)

	layout, hi := decodePoint(t, upper)
	assert.Equal(t, geom.XY, layout)
	assert.Equal(t, []float64{40, 40}, hi)
}

func TestGeoBoundsAccumulatorXYZ(t *testing.T) {
	acc := newGeoBoundsAccumulator()
	require.NoError(t, acc.AddWKB(wkbOf(t, "POINT Z (1 2 3)")))
	require.NoError(t, acc.AddWKB(wkbOf(t, "POINT Z (4 0 -1)")))

	lower, upper, err := acc.Bounds()
	require.NoError(t, err)

	layout, lo := decodePoint(t, lower)
	assert.Equal(t, geom.XYZ, layout)
	assert.Equal(t, []float64{1, 0, -1}, lo)

	_, hi := decodePoint(t, upper)
	assert.Equal(t, []float64{4, 2, 3}, hi)
}

func TestGeoBoundsAccumulatorXYM(t *testing.T) {
	acc := newGeoBoundsAccumulator()
	require.NoError(t, acc.AddWKB(wkbOf(t, "POINT M (1 2 100)")))
	require.NoError(t, acc.AddWKB(wkbOf(t, "POINT M (4 0 50)")))

	lower, upper, err := acc.Bounds()
	require.NoError(t, err)

	layout, lo := decodePoint(t, lower)
	assert.Equal(t, geom.XYM, layout)
	assert.Equal(t, []float64{1, 0, 50}, lo)

	_, hi := decodePoint(t, upper)
	assert.Equal(t, []float64{4, 2, 100}, hi)
}

func TestGeoBoundsAccumulatorGeometryCollection(t *testing.T) {
	acc := newGeoBoundsAccumulator()
	require.NoError(t, acc.AddWKB(wkbOf(t, "GEOMETRYCOLLECTION (POINT (4 6), LINESTRING (4 6, 7 10))")))

	lower, upper, err := acc.Bounds()
	require.NoError(t, err)

	_, lo := decodePoint(t, lower)
	assert.Equal(t, []float64{4, 6}, lo)
	_, hi := decodePoint(t, upper)
	assert.Equal(t, []float64{7, 10}, hi)
}

func TestGeoBoundsAccumulatorSkipsNaN(t *testing.T) {
	acc := newGeoBoundsAccumulator()
	require.NoError(t, acc.AddWKB(wkbOf(t, "POINT (10 10)")))
	// A NaN in Y must be skipped so it doesn't poison the Y bounds, while the
	// finite X=5 still extends the X bounds (per the spec, POINT(5 NaN)
	// contributes a value to X but none to Y).
	acc.extend(geom.NewPointFlat(geom.XY, []float64{5, math.NaN()}))
	require.NoError(t, acc.AddWKB(wkbOf(t, "POINT (20 20)")))

	lower, upper, err := acc.Bounds()
	require.NoError(t, err)

	layout, lo := decodePoint(t, lower)
	assert.Equal(t, geom.XY, layout)
	assert.Equal(t, []float64{5, 10}, lo)
	_, hi := decodePoint(t, upper)
	assert.Equal(t, []float64{20, 20}, hi)
}

func TestGeoBoundsAccumulatorMissingDimensionNoBox(t *testing.T) {
	acc := newGeoBoundsAccumulator()
	// Y is always NaN: with no finite Y value, no bounding box is produced.
	acc.extend(geom.NewPointFlat(geom.XY, []float64{1, math.NaN()}))

	lower, upper, err := acc.Bounds()
	require.NoError(t, err)
	assert.Nil(t, lower)
	assert.Nil(t, upper)
}

func TestGeoBoundsAccumulatorEmpty(t *testing.T) {
	acc := newGeoBoundsAccumulator()
	lower, upper, err := acc.Bounds()
	require.NoError(t, err)
	assert.Nil(t, lower)
	assert.Nil(t, upper)

	agg, err := acc.StatsAgg()
	require.NoError(t, err)
	assert.Nil(t, agg)
}

func TestGeoBoundsAccumulatorStatsAgg(t *testing.T) {
	acc := newGeoBoundsAccumulator()
	require.NoError(t, acc.AddWKB(wkbOf(t, "POINT (30 10)")))
	require.NoError(t, acc.AddWKB(wkbOf(t, "POINT (5 40)")))

	agg, err := acc.StatsAgg()
	require.NoError(t, err)
	require.NotNil(t, agg)

	lowerBytes, err := agg.MinAsBytes()
	require.NoError(t, err)
	upperBytes, err := agg.MaxAsBytes()
	require.NoError(t, err)

	_, lo := decodePoint(t, lowerBytes)
	assert.Equal(t, []float64{5, 10}, lo)
	_, hi := decodePoint(t, upperBytes)
	assert.Equal(t, []float64{30, 40}, hi)
}

func TestGeoBoundsAccumulatorInvalidWKB(t *testing.T) {
	acc := newGeoBoundsAccumulator()
	assert.Error(t, acc.AddWKB([]byte{0x01, 0x02, 0x03}))
}
