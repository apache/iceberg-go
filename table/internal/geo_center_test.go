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
	"github.com/twpayne/go-geom/encoding/ewkb"
	"github.com/twpayne/go-geom/encoding/wkb"
	"github.com/twpayne/go-geom/encoding/wkt"
)

// isoWKB encodes s as ISO WKB. Empty points are encoded as NaN coordinates,
// the ISO convention, so collections with empty members can be built.
func isoWKB(t *testing.T, s string, order binary.ByteOrder) []byte {
	t.Helper()

	g, err := wkt.Unmarshal(s)
	require.NoError(t, err)
	b, err := wkb.Marshal(g, order, wkbEmptyPointOption)
	require.NoError(t, err)

	return b
}

func TestWKBCenterXY(t *testing.T) {
	tests := []struct {
		name string
		wkt  string
		x, y float64
		ok   bool
	}{
		{"point", "POINT (3 4)", 3, 4, true},
		{"linestring", "LINESTRING (0 0, 10 4)", 5, 2, true},
		{"polygon", "POLYGON ((0 0, 8 0, 8 6, 0 6, 0 0))", 4, 3, true},
		{"multipoint", "MULTIPOINT ((0 0), (10 20))", 5, 10, true},
		{"point z ignores z", "POINT Z (1 2 99)", 1, 2, true},
		{"point m ignores m", "POINT M (1 2 99)", 1, 2, true},
		{"collection", "GEOMETRYCOLLECTION (POINT (0 0), LINESTRING (10 0, 10 10))", 5, 5, true},
		// geom.T.Bounds panics on a collection inside a collection; the
		// center must walk it instead.
		{"nested collection", "GEOMETRYCOLLECTION (GEOMETRYCOLLECTION (POINT (3 4)))", 3, 4, true},
		{"doubly nested collection", "GEOMETRYCOLLECTION (GEOMETRYCOLLECTION (GEOMETRYCOLLECTION (POINT (3 4)), POINT (5 6)))", 4, 5, true},
		// Empty members decode to NaN coordinates and must not poison the
		// finite members.
		{"collection with empty point", "GEOMETRYCOLLECTION (POINT (3 4), POINT EMPTY)", 3, 4, true},
		{"multipoint with empty point", "MULTIPOINT ((3 4), EMPTY)", 3, 4, true},
		{"collection with empty nested collection", "GEOMETRYCOLLECTION (POINT (3 4), GEOMETRYCOLLECTION EMPTY)", 3, 4, true},
		{"empty point", "POINT EMPTY", 0, 0, false},
		{"empty polygon", "POLYGON EMPTY", 0, 0, false},
		{"empty collection", "GEOMETRYCOLLECTION EMPTY", 0, 0, false},
		{"collection of only empties", "GEOMETRYCOLLECTION (POINT EMPTY, GEOMETRYCOLLECTION EMPTY)", 0, 0, false},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			for _, order := range []struct {
				name string
				bo   binary.ByteOrder
			}{{"little endian", wkb.NDR}, {"big endian", wkb.XDR}} {
				x, y, ok, err := WKBCenterXY(isoWKB(t, tc.wkt, order.bo))
				require.NoError(t, err, order.name)
				assert.Equal(t, tc.ok, ok, order.name)
				if tc.ok {
					assert.Equal(t, tc.x, x, order.name)
					assert.Equal(t, tc.y, y, order.name)
				}
			}
		})
	}
}

func TestWKBCenterXYAcceptsEWKB(t *testing.T) {
	// Values with an SRID or EWKB Z/M flags are rejected by the ISO decoder
	// but accepted by the writer, so the center must accept them too.
	tests := []struct {
		name string
		g    geom.T
		x, y float64
	}{
		{"srid", geom.NewPointFlat(geom.XY, []float64{3, 4}).SetSRID(4326), 3, 4},
		{"z flag", geom.NewPointFlat(geom.XYZ, []float64{3, 4, 99}), 3, 4},
		{"m flag", geom.NewPointFlat(geom.XYM, []float64{3, 4, 99}), 3, 4},
		{"zm flags with srid", geom.NewLineStringFlat(geom.XYZM, []float64{0, 0, 1, 2, 10, 20, 3, 4}).SetSRID(3857), 5, 10},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			for _, order := range []struct {
				name string
				bo   binary.ByteOrder
			}{{"little endian", ewkb.NDR}, {"big endian", ewkb.XDR}} {
				b, err := ewkb.Marshal(tc.g, order.bo)
				require.NoError(t, err)
				require.True(t, isEWKB(b), "test value must carry EWKB flags")

				x, y, ok, err := WKBCenterXY(b)
				require.NoError(t, err, order.name)
				assert.True(t, ok, order.name)
				assert.Equal(t, tc.x, x, order.name)
				assert.Equal(t, tc.y, y, order.name)
			}
		})
	}
}

func TestWKBCenterXYExtremeAndNonFinite(t *testing.T) {
	huge := math.MaxFloat64
	b, err := wkb.Marshal(geom.NewLineStringFlat(geom.XY, []float64{-huge, -huge, huge, huge}), wkb.NDR)
	require.NoError(t, err)
	x, y, ok, err := WKBCenterXY(b)
	require.NoError(t, err)
	assert.True(t, ok)
	assert.Equal(t, 0.0, x, "min+max would overflow; halving first keeps it finite")
	assert.Equal(t, 0.0, y)

	b, err = wkb.Marshal(geom.NewPointFlat(geom.XY, []float64{math.Inf(1), 1}), wkb.NDR)
	require.NoError(t, err)
	_, _, ok, err = WKBCenterXY(b)
	require.NoError(t, err)
	assert.False(t, ok, "an infinite coordinate means no location")

	// A NaN X with a finite Y: X never sees a finite value, so there is no
	// location even though Y does.
	b, err = wkb.Marshal(geom.NewPointFlat(geom.XY, []float64{math.NaN(), 1}), wkb.NDR)
	require.NoError(t, err)
	_, _, ok, err = WKBCenterXY(b)
	require.NoError(t, err)
	assert.False(t, ok)

	// A NaN Z must not disturb a finite XY.
	b, err = wkb.Marshal(geom.NewPointFlat(geom.XYZ, []float64{3, 4, math.NaN()}), wkb.NDR)
	require.NoError(t, err)
	x, y, ok, err = WKBCenterXY(b)
	require.NoError(t, err)
	assert.True(t, ok)
	assert.Equal(t, 3.0, x)
	assert.Equal(t, 4.0, y)
}

func TestWKBCenterXYErrors(t *testing.T) {
	for name, data := range map[string][]byte{
		"nil":       nil,
		"empty":     {},
		"garbage":   {0xff, 0x00, 0x01},
		"truncated": isoWKB(t, "POINT (1 2)", wkb.NDR)[:12],
	} {
		t.Run(name, func(t *testing.T) {
			_, _, _, err := WKBCenterXY(data)
			require.Error(t, err)
		})
	}
}
