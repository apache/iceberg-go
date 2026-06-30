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
// values. Iceberg stores geometry/geography column bounds as the WKB encoding
// of two points: the lower bound carries the per-dimension minimums and the
// upper bound carries the maximums. Per the Parquet/Iceberg geospatial spec,
// null and NaN coordinate values are skipped, and a dimension that never sees a
// finite value is omitted from the resulting points. If the X or Y dimension is
// missing entirely, no bounding box is produced.
type geoBoundsAccumulator struct {
	min [geoNumDims]float64
	max [geoNumDims]float64
	has [geoNumDims]bool
}

func newGeoBoundsAccumulator() *geoBoundsAccumulator {
	return &geoBoundsAccumulator{}
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

func geoPointCoords(vals [geoNumDims]float64, layout geom.Layout) []float64 {
	switch layout {
	case geom.XYZ:
		return []float64{vals[geoDimX], vals[geoDimY], vals[geoDimZ]}
	case geom.XYM:
		return []float64{vals[geoDimX], vals[geoDimY], vals[geoDimM]}
	case geom.XYZM:
		return []float64{vals[geoDimX], vals[geoDimY], vals[geoDimZ], vals[geoDimM]}
	default:
		return []float64{vals[geoDimX], vals[geoDimY]}
	}
}

// Bounds encodes the lower and upper bound points as WKB. Both are nil when no
// bounding box could be produced (e.g. an all-null column).
func (a *geoBoundsAccumulator) Bounds() (lower, upper []byte, err error) {
	layout, ok := a.layout()
	if !ok {
		return nil, nil, nil
	}

	lower, err = wkb.Marshal(geom.NewPointFlat(layout, geoPointCoords(a.min, layout)), wkb.NDR)
	if err != nil {
		return nil, nil, err
	}

	upper, err = wkb.Marshal(geom.NewPointFlat(layout, geoPointCoords(a.max, layout)), wkb.NDR)
	if err != nil {
		return nil, nil, err
	}

	return lower, upper, nil
}

// StatsAgg materializes the accumulated bounds into a StatsAgg carrying the
// precomputed WKB point bounds, or nil when no box was produced. Unlike the
// generic statsAggregator, geo bounds are computed from raw WKB during the write
// (Parquet byte-array min/max over WKB are meaningless), so the aggregator just
// replays the precomputed bytes through the existing ToDataFile bounds plumbing.
func (a *geoBoundsAccumulator) StatsAgg() (StatsAgg, error) {
	lower, upper, err := a.Bounds()
	if err != nil {
		return nil, err
	}
	if lower == nil {
		return nil, nil
	}

	return &geoStatsAgg{lower: lower, upper: upper}, nil
}

// geoStatsAgg is a StatsAgg holding precomputed WKB single-point bounds.
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
