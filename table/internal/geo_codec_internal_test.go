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

	"github.com/DataDog/iceberg-go"
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

// WKB type words used by the EWKB tests below. ISO WKB encodes the dimension in
// the type value itself (PointZ = 1001), while EWKB sets flags in the high bits
// of the type word (ewkbFlagZ etc., shared with geo_codec.go) and optionally
// embeds an SRID after it.
const (
	wkbPoint               = 1
	wkbLineString          = 2
	wkbGeometryCollection  = 7
	wkbPointZ              = 1001
	wkbPointM              = 2001
	wkbPointZM             = 3001
	wkbLineStringZ         = 1002
	wkbGeometryCollectionZ = 1007
)

// wkbBuilder assembles a WKB value byte by byte: the byte-order marker, then
// uint32 headers and float64 coordinates in that byte order.
type wkbBuilder struct {
	buf   []byte
	order binary.AppendByteOrder
}

// newWKBBuilder builds a little-endian (NDR) value.
func newWKBBuilder(typeWord uint32) *wkbBuilder {
	return (&wkbBuilder{buf: []byte{wkbLittleEndian}, order: binary.LittleEndian}).u32(typeWord)
}

// newXDRWKBBuilder builds a big-endian (XDR) value.
func newXDRWKBBuilder(typeWord uint32) *wkbBuilder {
	return (&wkbBuilder{buf: []byte{wkbBigEndian}, order: binary.BigEndian}).u32(typeWord)
}

func (b *wkbBuilder) u32(v uint32) *wkbBuilder {
	b.buf = b.order.AppendUint32(b.buf, v)

	return b
}

func (b *wkbBuilder) f64(vals ...float64) *wkbBuilder {
	for _, v := range vals {
		b.buf = b.order.AppendUint64(b.buf, math.Float64bits(v))
	}

	return b
}

// nested appends complete WKB values, each carrying its own byte-order marker,
// as the sub-geometries of a collection.
func (b *wkbBuilder) nested(vals ...[]byte) *wkbBuilder {
	for _, v := range vals {
		b.buf = append(b.buf, v...)
	}

	return b
}

func (b *wkbBuilder) bytes() []byte { return b.buf }

// assertCoords compares bound coordinates treating NaN as equal to NaN, which
// assert.Equal does not (the XYM bound carries NaN in its Z slot).
func assertCoords(t *testing.T, want, got []float64) {
	t.Helper()
	require.Len(t, got, len(want))
	for i := range want {
		if math.IsNaN(want[i]) {
			assert.True(t, math.IsNaN(got[i]), "coord %d: want NaN, got %v", i, got[i])

			continue
		}
		assert.Equal(t, want[i], got[i], "coord %d", i)
	}
}

// TestGeoBoundsAccumulatorEWKB verifies that bounds are computed from both ISO
// WKB (as Iceberg prescribes) and EWKB-flagged values, which some writers emit:
// dimension flags in the high bits of the type word, with an optional embedded
// SRID that is irrelevant to the bounding box.
func TestGeoBoundsAccumulatorEWKB(t *testing.T) {
	tests := []struct {
		name       string
		wkb        []byte
		wantLower  []float64
		wantUpper  []float64
		wantLength int
	}{
		{
			name:       "iso point xy",
			wkb:        newWKBBuilder(wkbPoint).f64(1, 2).bytes(),
			wantLower:  []float64{1, 2},
			wantUpper:  []float64{1, 2},
			wantLength: 16,
		},
		{
			name:       "iso point z",
			wkb:        newWKBBuilder(wkbPointZ).f64(1, 2, 3).bytes(),
			wantLower:  []float64{1, 2, 3},
			wantUpper:  []float64{1, 2, 3},
			wantLength: 24,
		},
		{
			name:       "ewkb point z",
			wkb:        newWKBBuilder(wkbPoint|ewkbFlagZ).f64(1, 2, 3).bytes(),
			wantLower:  []float64{1, 2, 3},
			wantUpper:  []float64{1, 2, 3},
			wantLength: 24,
		},
		{
			name:       "ewkb point z with srid",
			wkb:        newWKBBuilder(wkbPoint|ewkbFlagZ|ewkbFlagSRID).u32(4326).f64(1, 2, 3).bytes(),
			wantLower:  []float64{1, 2, 3},
			wantUpper:  []float64{1, 2, 3},
			wantLength: 24,
		},
		{
			name:       "ewkb point xy with srid",
			wkb:        newWKBBuilder(wkbPoint|ewkbFlagSRID).u32(4326).f64(1, 2).bytes(),
			wantLower:  []float64{1, 2},
			wantUpper:  []float64{1, 2},
			wantLength: 16,
		},
		{
			name:       "iso point m",
			wkb:        newWKBBuilder(wkbPointM).f64(1, 2, 100).bytes(),
			wantLower:  []float64{1, 2, math.NaN(), 100},
			wantUpper:  []float64{1, 2, math.NaN(), 100},
			wantLength: 32,
		},
		{
			name:       "ewkb point m",
			wkb:        newWKBBuilder(wkbPoint|ewkbFlagM).f64(1, 2, 100).bytes(),
			wantLower:  []float64{1, 2, math.NaN(), 100},
			wantUpper:  []float64{1, 2, math.NaN(), 100},
			wantLength: 32,
		},
		{
			name:       "iso point zm",
			wkb:        newWKBBuilder(wkbPointZM).f64(1, 2, 3, 100).bytes(),
			wantLower:  []float64{1, 2, 3, 100},
			wantUpper:  []float64{1, 2, 3, 100},
			wantLength: 32,
		},
		{
			name:       "ewkb point zm",
			wkb:        newWKBBuilder(wkbPoint|ewkbFlagZ|ewkbFlagM).f64(1, 2, 3, 100).bytes(),
			wantLower:  []float64{1, 2, 3, 100},
			wantUpper:  []float64{1, 2, 3, 100},
			wantLength: 32,
		},
		{
			name:       "ewkb linestring z",
			wkb:        newWKBBuilder(wkbLineString|ewkbFlagZ).u32(2).f64(1, 2, 3, 4, 5, 6).bytes(),
			wantLower:  []float64{1, 2, 3},
			wantUpper:  []float64{4, 5, 6},
			wantLength: 24,
		},
		{
			name:       "ewkb point z big endian",
			wkb:        newXDRWKBBuilder(wkbPoint|ewkbFlagZ).f64(1, 2, 3).bytes(),
			wantLower:  []float64{1, 2, 3},
			wantUpper:  []float64{1, 2, 3},
			wantLength: 24,
		},
		{
			name:       "ewkb linestring z with srid",
			wkb:        newWKBBuilder(wkbLineString|ewkbFlagZ|ewkbFlagSRID).u32(4326).u32(2).f64(1, 2, 3, 4, 5, 6).bytes(),
			wantLower:  []float64{1, 2, 3},
			wantUpper:  []float64{4, 5, 6},
			wantLength: 24,
		},
		{
			// A collection is the only value whose sub-geometries are decoded
			// recursively, and Trino and PostGIS both emit these; each sub-geometry
			// repeats the byte-order marker and the flagged type word.
			name: "ewkb geometry collection z",
			wkb: newWKBBuilder(wkbGeometryCollection|ewkbFlagZ).u32(2).nested(
				newWKBBuilder(wkbPoint|ewkbFlagZ).f64(1, 2, 3).bytes(),
				newWKBBuilder(wkbLineString|ewkbFlagZ).u32(2).f64(4, 5, 6, 7, 8, 9).bytes(),
			).bytes(),
			wantLower:  []float64{1, 2, 3},
			wantUpper:  []float64{7, 8, 9},
			wantLength: 24,
		},
		{
			name: "ewkb geometry collection z with srid",
			wkb: newWKBBuilder(wkbGeometryCollection|ewkbFlagZ|ewkbFlagSRID).u32(4326).u32(2).nested(
				newWKBBuilder(wkbPoint|ewkbFlagZ|ewkbFlagSRID).u32(4326).f64(1, 2, 3).bytes(),
				newWKBBuilder(wkbPoint|ewkbFlagZ|ewkbFlagSRID).u32(4326).f64(7, 8, 9).bytes(),
			).bytes(),
			wantLower:  []float64{1, 2, 3},
			wantUpper:  []float64{7, 8, 9},
			wantLength: 24,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			acc := newGeoBoundsAccumulator(false)
			require.NoError(t, acc.AddWKB(tt.wkb))

			lower, upper := acc.Bounds()
			require.Len(t, lower, tt.wantLength)
			require.Len(t, upper, tt.wantLength)

			assertCoords(t, tt.wantLower, decodeBound(t, lower))
			assertCoords(t, tt.wantUpper, decodeBound(t, upper))
		})
	}
}

// TestGeoBoundsAccumulatorEWKBMatchesISO verifies that the two encodings of the
// same coordinates produce byte-identical bounds, so a file's statistics do not
// depend on which encoding its writer used.
func TestGeoBoundsAccumulatorEWKBMatchesISO(t *testing.T) {
	tests := []struct {
		name     string
		iso      []byte
		ewkb     []byte
		geograph bool
	}{
		{
			name: "point xy",
			iso:  newWKBBuilder(wkbPoint).f64(1, 2).bytes(),
			ewkb: newWKBBuilder(wkbPoint|ewkbFlagSRID).u32(4326).f64(1, 2).bytes(),
		},
		{
			name: "point z",
			iso:  newWKBBuilder(wkbPointZ).f64(1, 2, 3).bytes(),
			ewkb: newWKBBuilder(wkbPoint|ewkbFlagZ).f64(1, 2, 3).bytes(),
		},
		{
			name: "point m",
			iso:  newWKBBuilder(wkbPointM).f64(1, 2, 100).bytes(),
			ewkb: newWKBBuilder(wkbPoint|ewkbFlagM).f64(1, 2, 100).bytes(),
		},
		{
			name: "point zm",
			iso:  newWKBBuilder(wkbPointZM).f64(1, 2, 3, 100).bytes(),
			ewkb: newWKBBuilder(wkbPoint|ewkbFlagZ|ewkbFlagM).f64(1, 2, 3, 100).bytes(),
		},
		{
			name: "point z big endian",
			iso:  newWKBBuilder(wkbPointZ).f64(1, 2, 3).bytes(),
			ewkb: newXDRWKBBuilder(wkbPoint|ewkbFlagZ).f64(1, 2, 3).bytes(),
		},
		{
			name: "linestring z",
			iso:  newWKBBuilder(wkbLineStringZ).u32(2).f64(1, 2, 3, 4, 5, 6).bytes(),
			ewkb: newWKBBuilder(wkbLineString|ewkbFlagZ).u32(2).f64(1, 2, 3, 4, 5, 6).bytes(),
		},
		{
			// Geography emits no bounds for either encoding, but the value must
			// still decode: a decode error aborts the whole file rewrite.
			name:     "geography point z",
			iso:      newWKBBuilder(wkbPointZ).f64(1, 2, 3).bytes(),
			ewkb:     newWKBBuilder(wkbPoint|ewkbFlagZ).f64(1, 2, 3).bytes(),
			geograph: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			isoAcc := newGeoBoundsAccumulator(tt.geograph)
			require.NoError(t, isoAcc.AddWKB(tt.iso))
			isoLower, isoUpper := isoAcc.Bounds()

			ewkbAcc := newGeoBoundsAccumulator(tt.geograph)
			require.NoError(t, ewkbAcc.AddWKB(tt.ewkb))
			ewkbLower, ewkbUpper := ewkbAcc.Bounds()

			// Both accumulators must have consumed coordinates. Bounds alone cannot
			// show this for geography, where the comparison is nil against nil and
			// would stay green if the decode returned an empty geometry.
			assert.Positive(t, isoAcc.geoms, "ISO value contributed no geometry")
			assert.Positive(t, ewkbAcc.geoms, "EWKB value contributed no geometry")
			assert.Equal(t, isoAcc.min, ewkbAcc.min, "accumulated minimums must match")
			assert.Equal(t, isoAcc.max, ewkbAcc.max, "accumulated maximums must match")

			assert.Equal(t, isoLower, ewkbLower)
			assert.Equal(t, isoUpper, ewkbUpper)
			if tt.geograph {
				assert.Nil(t, ewkbLower, "geography bounds must be omitted")
			}
		})
	}
}

// TestGeoBoundsAccumulatorRejectsInvalidWKB verifies that malformed values still
// error rather than panicking or silently contributing no coordinates.
//
// The two collection cases pin the boundary of the encoding heuristic: isEWKB
// sniffs only the outer type word, so a collection whose sub-geometries use the
// other encoding reaches the wrong decoder. Mixing encodings within one value is
// unsupported, and the failure mode is an error that aborts the file rather than
// bounds computed from a partial decode.
func TestGeoBoundsAccumulatorRejectsInvalidWKB(t *testing.T) {
	tests := []struct {
		name string
		wkb  []byte
	}{
		{name: "empty", wkb: nil},
		{name: "byte order only", wkb: []byte{wkbLittleEndian}},
		{name: "unknown byte order", wkb: []byte{0x07, 0x01, 0x00, 0x00, 0x00}},
		{name: "truncated type word", wkb: []byte{wkbLittleEndian, 0x01, 0x00}},
		{name: "unknown iso type", wkb: newWKBBuilder(42).f64(1, 2).bytes()},
		{name: "unknown iso dimension", wkb: newWKBBuilder(9001).f64(1, 2).bytes()},
		{name: "unknown ewkb type", wkb: newWKBBuilder(42|ewkbFlagZ).f64(1, 2, 3).bytes()},
		{name: "truncated coords", wkb: newWKBBuilder(wkbPoint|ewkbFlagZ).f64(1, 2).bytes()},
		{name: "missing srid", wkb: newWKBBuilder(wkbPoint | ewkbFlagSRID).bytes()},
		{
			name: "ewkb collection with iso sub-geometry",
			wkb: newWKBBuilder(wkbGeometryCollection | ewkbFlagZ).u32(1).nested(
				newWKBBuilder(wkbPointZ).f64(1, 2, 3).bytes(),
			).bytes(),
		},
		{
			name: "iso collection with ewkb sub-geometry",
			wkb: newWKBBuilder(wkbGeometryCollectionZ).u32(1).nested(
				newWKBBuilder(wkbPoint|ewkbFlagZ).f64(1, 2, 3).bytes(),
			).bytes(),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			acc := newGeoBoundsAccumulator(false)
			require.Error(t, acc.AddWKB(tt.wkb))

			lower, upper := acc.Bounds()
			assert.Zero(t, acc.geoms, "a rejected value must contribute no geometry")
			assert.Nil(t, lower)
			assert.Nil(t, upper)
		})
	}
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

// encGeo encodes a bound point from explicit coordinates for aggregator tests.
func encGeo(x, y float64) []byte {
	var v [geoNumDims]float64
	v[geoDimX], v[geoDimY] = x, y

	return encodeGeoBound(v, geom.XY)
}

func encGeoZ(x, y, z float64) []byte {
	var v [geoNumDims]float64
	v[geoDimX], v[geoDimY], v[geoDimZ] = x, y, z

	return encodeGeoBound(v, geom.XYZ)
}

func encGeoM(x, y, mv float64) []byte {
	var v [geoNumDims]float64
	v[geoDimX], v[geoDimY], v[geoDimM] = x, y, mv

	return encodeGeoBound(v, geom.XYM)
}

// TestDecodeGeoBound pins that decodeGeoBound is the exact inverse of
// encodeGeoBound for each layout, including the XYM/XYZM disambiguation by the
// NaN Z slot, and rejects invalid lengths.
func TestDecodeGeoBound(t *testing.T) {
	var v [geoNumDims]float64
	v[geoDimX], v[geoDimY], v[geoDimZ], v[geoDimM] = 1, 2, 3, 4

	xy, layout, ok := decodeGeoBound(encodeGeoBound(v, geom.XY))
	require.True(t, ok)
	assert.Equal(t, geom.XY, layout)
	assert.Equal(t, [2]float64{1, 2}, [2]float64{xy[geoDimX], xy[geoDimY]})

	xyz, layout, ok := decodeGeoBound(encodeGeoBound(v, geom.XYZ))
	require.True(t, ok)
	assert.Equal(t, geom.XYZ, layout)
	assert.Equal(t, float64(3), xyz[geoDimZ])

	xym, layout, ok := decodeGeoBound(encodeGeoBound(v, geom.XYM))
	require.True(t, ok)
	assert.Equal(t, geom.XYM, layout, "NaN Z slot must decode as XYM, not XYZM")
	assert.Equal(t, float64(4), xym[geoDimM])

	xyzm, layout, ok := decodeGeoBound(encodeGeoBound(v, geom.XYZM))
	require.True(t, ok)
	assert.Equal(t, geom.XYZM, layout)
	assert.Equal(t, [2]float64{3, 4}, [2]float64{xyzm[geoDimZ], xyzm[geoDimM]})

	for _, n := range []int{0, 8, 15, 40} {
		_, _, ok := decodeGeoBound(make([]byte, n))
		assert.False(t, ok, "length %d must be rejected", n)
	}
}

func TestGeoBoundsAggregatorXY(t *testing.T) {
	var agg GeoBoundsAggregator
	require.NoError(t, agg.Add(encGeo(5, 5), encGeo(10, 20)))
	require.NoError(t, agg.Add(encGeo(1, 8), encGeo(30, 12)))

	lower, upper := agg.Bounds()
	require.Len(t, lower, 16)
	assert.Equal(t, []float64{1, 5}, decodeBound(t, lower))
	assert.Equal(t, []float64{30, 20}, decodeBound(t, upper))
}

func TestGeoBoundsAggregatorXYZ(t *testing.T) {
	var agg GeoBoundsAggregator
	require.NoError(t, agg.Add(encGeoZ(1, 2, 3), encGeoZ(4, 5, 6)))
	require.NoError(t, agg.Add(encGeoZ(0, 1, -1), encGeoZ(2, 9, 3)))

	lower, upper := agg.Bounds()
	require.Len(t, lower, 24)
	assert.Equal(t, []float64{0, 1, -1}, decodeBound(t, lower))
	assert.Equal(t, []float64{4, 9, 6}, decodeBound(t, upper))
}

func TestGeoBoundsAggregatorXYM(t *testing.T) {
	var agg GeoBoundsAggregator
	require.NoError(t, agg.Add(encGeoM(1, 2, 50), encGeoM(4, 5, 100)))
	require.NoError(t, agg.Add(encGeoM(0, 1, 10), encGeoM(2, 9, 70)))

	lower, upper := agg.Bounds()
	require.Len(t, lower, 32)
	lo := decodeBound(t, lower)
	assert.Equal(t, []float64{0, 1}, lo[:2])
	assert.True(t, math.IsNaN(lo[2]), "XYM lower Z slot must be NaN")
	assert.Equal(t, float64(10), lo[3])
	hi := decodeBound(t, upper)
	assert.Equal(t, float64(100), hi[3])
}

// TestGeoBoundsAggregatorMixedDimensionDropsZ verifies the omit-on-ambiguity
// rule across files: a file carrying Z followed by a plain-XY file must collapse
// to an XY box, since not every file carried Z. Emitting Z would claim the XY
// file has a Z value in range.
func TestGeoBoundsAggregatorMixedDimensionDropsZ(t *testing.T) {
	var agg GeoBoundsAggregator
	require.NoError(t, agg.Add(encGeoZ(1, 2, 3), encGeoZ(4, 5, 6)))
	require.NoError(t, agg.Add(encGeo(0, 1), encGeo(10, 9)))

	lower, upper := agg.Bounds()
	require.Len(t, lower, 16, "a file without Z must collapse the aggregate to XY")
	assert.Equal(t, []float64{0, 1}, decodeBound(t, lower))
	assert.Equal(t, []float64{10, 9}, decodeBound(t, upper))
}

// TestGeoBoundsAggregatorEmptyInputs verifies that empty bound pairs (as emitted
// for geography or all-null geometry columns) contribute nothing.
func TestGeoBoundsAggregatorEmptyInputs(t *testing.T) {
	var agg GeoBoundsAggregator
	require.NoError(t, agg.Add(nil, nil))
	require.NoError(t, agg.Add([]byte{}, []byte{}))

	lower, upper := agg.Bounds()
	assert.Nil(t, lower)
	assert.Nil(t, upper)
}

func TestGeoBoundsAggregatorInvalidLength(t *testing.T) {
	var agg GeoBoundsAggregator
	assert.Error(t, agg.Add([]byte{0x01, 0x02}, encGeo(1, 2)))
	assert.Error(t, agg.Add(encGeo(1, 2), []byte{0x01, 0x02}))
}

// TestGeoBoundsAggregatorMismatchedLayouts guards against combining a lower and
// upper of different dimensionality, which would silently corrupt the box.
func TestGeoBoundsAggregatorMismatchedLayouts(t *testing.T) {
	var agg GeoBoundsAggregator
	assert.Error(t, agg.Add(encGeo(1, 2), encGeoZ(4, 5, 6)))
}

// TestGeoBoundsAggregatorRejectsGeography verifies a geography aggregator refuses
// non-empty bounds: scalar min/max folding would silently unwrap an
// antimeridian-crossing box (lower_x > upper_x), producing bounds that prune rows
// they should keep. Empty bounds - what iceberg-go emits for geography - stay a
// harmless no-op, and the aggregate produces no box.
func TestGeoBoundsAggregatorRejectsGeography(t *testing.T) {
	agg := NewGeoBoundsAggregator(true)

	require.NoError(t, agg.Add(nil, nil))
	require.NoError(t, agg.Add([]byte{}, []byte{}))

	// A wrapped box that scalar folding would mis-merge must be refused.
	err := agg.Add(encGeo(170, 10), encGeo(-170, 20))
	require.ErrorIs(t, err, iceberg.ErrNotImplemented)

	lower, upper := agg.Bounds()
	assert.Nil(t, lower)
	assert.Nil(t, upper)
}

// TestNewGeoBoundsAggregatorGeometry verifies the constructor's geometry path
// aggregates identically to the zero value.
func TestNewGeoBoundsAggregatorGeometry(t *testing.T) {
	agg := NewGeoBoundsAggregator(false)
	require.NoError(t, agg.Add(encGeo(5, 5), encGeo(10, 20)))
	require.NoError(t, agg.Add(encGeo(1, 8), encGeo(30, 12)))

	lower, upper := agg.Bounds()
	assert.Equal(t, []float64{1, 5}, decodeBound(t, lower))
	assert.Equal(t, []float64{30, 20}, decodeBound(t, upper))
}

func TestGeoBoundsXY(t *testing.T) {
	minX, minY, maxX, maxY, ok := GeoBoundsXY(encGeo(5, 10), encGeo(30, 40))
	require.True(t, ok)
	assert.Equal(t, [4]float64{5, 10, 30, 40}, [4]float64{minX, minY, maxX, maxY})

	// Higher-dimension bounds still yield their XY extents.
	minX, minY, maxX, maxY, ok = GeoBoundsXY(encGeoZ(1, 2, 3), encGeoZ(4, 5, 6))
	require.True(t, ok)
	assert.Equal(t, [4]float64{1, 2, 4, 5}, [4]float64{minX, minY, maxX, maxY})

	// Missing or malformed bounds are unusable for pruning.
	_, _, _, _, ok = GeoBoundsXY(nil, encGeo(1, 2))
	assert.False(t, ok, "missing lower bound")
	_, _, _, _, ok = GeoBoundsXY(encGeo(1, 2), []byte{0x01})
	assert.False(t, ok, "malformed upper bound")

	// A NaN X/Y coordinate cannot bound anything.
	var nan [geoNumDims]float64
	nan[geoDimX], nan[geoDimY] = math.NaN(), 2
	_, _, _, _, ok = GeoBoundsXY(encodeGeoBound(nan, geom.XY), encGeo(3, 4))
	assert.False(t, ok, "NaN coordinate")

	// Inverted bounds (lower > upper) from an untrusted writer are unusable:
	// treating them as valid would make BBoxIntersectsXY always report no-overlap
	// and prune a file that should be kept.
	_, _, _, _, ok = GeoBoundsXY(encGeo(30, 10), encGeo(5, 40))
	assert.False(t, ok, "inverted X bound must be rejected")
	_, _, _, _, ok = GeoBoundsXY(encGeo(5, 40), encGeo(30, 10))
	assert.False(t, ok, "inverted Y bound must be rejected")
}

func TestBBoxIntersectsXY(t *testing.T) {
	// file box [0,0]-[10,10]
	tests := []struct {
		name                       string
		qMinX, qMinY, qMaxX, qMaxY float64
		want                       bool
	}{
		{"overlapping", 5, 5, 15, 15, true},
		{"contained", 2, 2, 3, 3, true},
		{"touching edge", 10, 0, 20, 10, true},
		{"touching corner", 10, 10, 20, 20, true},
		{"disjoint right", 11, 0, 20, 10, false},
		{"disjoint above", 0, 11, 10, 20, false},
		{"disjoint diagonal", 20, 20, 30, 30, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.want,
				BBoxIntersectsXY(0, 0, 10, 10, tt.qMinX, tt.qMinY, tt.qMaxX, tt.qMaxY))
			// intersection is symmetric
			assert.Equal(t, tt.want,
				BBoxIntersectsXY(tt.qMinX, tt.qMinY, tt.qMaxX, tt.qMaxY, 0, 0, 10, 10))
		})
	}
}
