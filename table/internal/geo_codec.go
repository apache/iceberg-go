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

	"github.com/apache/iceberg-go"
	"github.com/twpayne/go-geom"
	"github.com/twpayne/go-geom/encoding/wkb"
)

// Geo bounding box dimension indices. X is longitude/easting, Y is
// latitude/northing; Z and M are optional. These index the per-dimension
// min/max/has arrays of geoBoundsAccumulator.
const (
	geoDimX = iota
	geoDimY
	geoDimZ
	geoDimM
	geoNumDims
)

// geoBoundsAccumulator computes a geospatial bounding box from a stream of WKB
// values. Iceberg stores geometry/geography column bounds using the single-value
// serialization for geospatial types (spec Appendix D): the concatenation of
// little-endian float64 coordinate values in X, Y[, Z][, M] order. The lower
// bound carries the per-dimension minimums and the upper bound the maximums.
// Null and NaN coordinate values are skipped, and a dimension that never sees a
// finite value is omitted from the resulting bounds. If the X or Y dimension is
// missing entirely, no bounding box is produced.
//
// Bounds are only emitted for geometry (planar edges). Geography bounds are
// omitted: see Bounds.
type geoBoundsAccumulator struct {
	min [geoNumDims]float64
	max [geoNumDims]float64
	has [geoNumDims]bool

	// isGeography marks a column whose edges are geodesics on a sphere, for
	// which raw vertex min/max is not a safe bounding box (see Bounds).
	isGeography bool
}

func newGeoBoundsAccumulator(isGeography bool) *geoBoundsAccumulator {
	return &geoBoundsAccumulator{isGeography: isGeography}
}

// AddWKB unmarshals a single WKB value and extends the bounding box with its
// coordinates.
func (a *geoBoundsAccumulator) AddWKB(data []byte) error {
	g, err := wkb.Unmarshal(data)
	if err != nil {
		return err
	}

	a.extend(g)

	return nil
}

// extend walks every coordinate of g, recursing into geometry collections,
// which cannot expose flat coordinates directly.
func (a *geoBoundsAccumulator) extend(g geom.T) {
	if gc, ok := g.(*geom.GeometryCollection); ok {
		for _, sub := range gc.Geoms() {
			a.extend(sub)
		}

		return
	}

	stride := g.Stride()
	flat := g.FlatCoords()
	if stride < 2 || len(flat) == 0 {
		return
	}

	layout := g.Layout()
	zIdx, mIdx := layout.ZIndex(), layout.MIndex()
	for base := 0; base+stride <= len(flat); base += stride {
		a.update(geoDimX, flat[base])
		a.update(geoDimY, flat[base+1])
		if zIdx >= 0 {
			a.update(geoDimZ, flat[base+zIdx])
		}
		if mIdx >= 0 {
			a.update(geoDimM, flat[base+mIdx])
		}
	}
}

func (a *geoBoundsAccumulator) update(dim int, v float64) {
	if math.IsNaN(v) {
		return
	}

	if !a.has[dim] {
		a.min[dim], a.max[dim] = v, v
		a.has[dim] = true

		return
	}

	if v < a.min[dim] {
		a.min[dim] = v
	}
	if v > a.max[dim] {
		a.max[dim] = v
	}
}

// layout selects the point layout for the bound points from the dimensions that
// saw finite values. The bool is false when the box has no X or Y dimension and
// therefore should not be emitted.
func (a *geoBoundsAccumulator) layout() (geom.Layout, bool) {
	if !a.has[geoDimX] || !a.has[geoDimY] {
		return geom.NoLayout, false
	}

	switch {
	case a.has[geoDimZ] && a.has[geoDimM]:
		return geom.XYZM, true
	case a.has[geoDimZ]:
		return geom.XYZ, true
	case a.has[geoDimM]:
		return geom.XYM, true
	default:
		return geom.XY, true
	}
}

// encodeGeoBound serializes a bound point using the Iceberg single-value
// serialization for geospatial types: little-endian float64 coordinates in
// X, Y[, Z][, M] order. Lengths are XY=16, XYZ=24, XYM=32, XYZM=32 bytes. For
// XYM the Z slot is written as NaN so a reader can tell XYM (NaN in slot 3)
// apart from XYZM (finite Z in slot 3).
func encodeGeoBound(vals [geoNumDims]float64, layout geom.Layout) []byte {
	var coords []float64
	switch layout {
	case geom.XYZ:
		coords = []float64{vals[geoDimX], vals[geoDimY], vals[geoDimZ]}
	case geom.XYM:
		coords = []float64{vals[geoDimX], vals[geoDimY], math.NaN(), vals[geoDimM]}
	case geom.XYZM:
		coords = []float64{vals[geoDimX], vals[geoDimY], vals[geoDimZ], vals[geoDimM]}
	default:
		coords = []float64{vals[geoDimX], vals[geoDimY]}
	}

	buf := make([]byte, len(coords)*8)
	for i, c := range coords {
		binary.LittleEndian.PutUint64(buf[i*8:], math.Float64bits(c))
	}

	return buf
}

// Bounds encodes the lower and upper bound points using the Iceberg geospatial
// single-value serialization (see encodeGeoBound). Both are nil when no bounding
// box could be produced (e.g. an all-null column).
//
// Geography bounds are always omitted. Geography edges are geodesics on a
// sphere, so a box built from raw vertex min/max is unsafe: a geodesic edge can
// reach a higher latitude than either endpoint, and the correct longitude
// interval may wrap across the antimeridian (xmin > xmax). Emitting naive vertex
// bounds would prune rows that actually fall inside a query region — a
// silent-wrong-results bug. Missing bounds only disable pruning, which is always
// safe, so geography is left unbounded until geodesic/antimeridian-aware
// computation is added.
func (a *geoBoundsAccumulator) Bounds() (lower, upper []byte) {
	layout, ok := a.layout()
	if !ok || a.isGeography {
		return nil, nil
	}

	return encodeGeoBound(a.min, layout), encodeGeoBound(a.max, layout)
}

// StatsAgg materializes the accumulated bounds into a StatsAgg carrying the
// precomputed single-point bounds, or nil when no box was produced. Unlike the
// generic statsAggregator, geo bounds are computed from raw WKB during the write
// (Parquet byte-array min/max over WKB are meaningless), so the aggregator just
// replays the precomputed bytes through the existing ToDataFile bounds plumbing.
func (a *geoBoundsAccumulator) StatsAgg() (StatsAgg, error) {
	lower, upper := a.Bounds()
	if lower == nil {
		return nil, nil
	}

	return &geoStatsAgg{lower: lower, upper: upper}, nil
}

// geoStatsAgg is a StatsAgg holding precomputed geo single-point bounds encoded
// with the Iceberg geospatial single-value serialization (see encodeGeoBound).
type geoStatsAgg struct {
	lower, upper []byte
}

func (g *geoStatsAgg) Min() iceberg.Literal {
	if g.lower == nil {
		return nil
	}

	return iceberg.BinaryLiteral(g.lower)
}

func (g *geoStatsAgg) Max() iceberg.Literal {
	if g.upper == nil {
		return nil
	}

	return iceberg.BinaryLiteral(g.upper)
}

func (g *geoStatsAgg) Update(interface{ HasMinMax() bool }) {}

func (g *geoStatsAgg) MinAsBytes() ([]byte, error) { return g.lower, nil }
func (g *geoStatsAgg) MaxAsBytes() ([]byte, error) { return g.upper, nil }
