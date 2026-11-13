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
	"context"
	"math"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/compute"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/iceberg-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/twpayne/go-geom"
	"github.com/twpayne/go-geom/encoding/wkb"
	"github.com/twpayne/go-geom/encoding/wkbcommon"
	"github.com/twpayne/go-geom/encoding/wkt"
)

func mustHilbertGrid(t *testing.T, bits int) hilbertGrid {
	t.Helper()

	g, err := newHilbertGrid(bits)
	require.NoError(t, err)

	return g
}

func hilbertTestWKB(t *testing.T, s string) []byte {
	t.Helper()

	g, err := wkt.Unmarshal(s)
	require.NoError(t, err)
	// ISO WKB has no empty-point encoding; the convention, which go-geom
	// follows with this option, is a point of NaN coordinates. Without it
	// go-geom refuses to encode an empty point, even inside a collection.
	b, err := wkb.Marshal(g, wkb.NDR, wkbcommon.WKBOptionEmptyPointHandling(wkbcommon.EmptyPointHandlingNaN))
	require.NoError(t, err)

	return b
}

// hilbertTestBatch builds a one-column batch of binary values; a nil
// value is a null row.
func hilbertTestBatch(t *testing.T, dt arrow.BinaryDataType, values [][]byte) arrow.RecordBatch {
	t.Helper()

	bldr := array.NewBinaryBuilder(memory.DefaultAllocator, dt)
	defer bldr.Release()
	for _, v := range values {
		if v == nil {
			bldr.AppendNull()
		} else {
			bldr.Append(v)
		}
	}
	col := bldr.NewArray()
	defer col.Release()

	sc := arrow.NewSchema([]arrow.Field{{Name: "geom", Type: dt, Nullable: true}}, nil)

	return array.NewRecordBatch(sc, []arrow.Array{col}, int64(len(values)))
}

func TestNewHilbertGrid(t *testing.T) {
	for _, bits := range []int{0, -1, 33, 100} {
		_, err := newHilbertGrid(bits)
		require.ErrorIs(t, err, ErrInvalidOperation, "bits=%d", bits)
	}
	for bits, want := range map[int]uint64{1: 2, 4: 16, 16: 65536, 32: 1 << 32} {
		g, err := newHilbertGrid(bits)
		require.NoError(t, err)
		assert.Equal(t, bits, g.bits)
		assert.Equal(t, want, g.cellsPerAxis)
	}
}

func TestHilbertGridCurveDistanceForCellKnownOrder(t *testing.T) {
	// The first-order curve visits (0,0), (0,1), (1,1), (1,0).
	assert.Equal(t, uint64(0), mustHilbertGrid(t, 1).curveDistanceForCell(0, 0))
	assert.Equal(t, uint64(1), mustHilbertGrid(t, 1).curveDistanceForCell(0, 1))
	assert.Equal(t, uint64(2), mustHilbertGrid(t, 1).curveDistanceForCell(1, 1))
	assert.Equal(t, uint64(3), mustHilbertGrid(t, 1).curveDistanceForCell(1, 0))
}

func TestHilbertGridCurveDistanceForCellIsContinuousBijection(t *testing.T) {
	for _, bits := range []int{2, 3, 5} {
		grid := mustHilbertGrid(t, bits)
		side := grid.cellsPerAxis
		cells := make([][2]uint64, side*side)
		seen := make([]bool, side*side)
		for x := range side {
			for y := range side {
				d := grid.curveDistanceForCell(x, y)
				require.Less(t, d, side*side)
				require.False(t, seen[d], "bits=%d: index %d assigned twice", bits, d)
				seen[d] = true
				cells[d] = [2]uint64{x, y}
			}
		}
		for d := 1; d < len(cells); d++ {
			dx := int64(cells[d][0]) - int64(cells[d-1][0])
			dy := int64(cells[d][1]) - int64(cells[d-1][1])
			require.Equal(t, int64(1), dx*dx+dy*dy, "bits=%d: steps %d and %d not adjacent", bits, d-1, d)
		}
	}
}

func TestHilbertGridCurveDistanceForCellMaxBits(t *testing.T) {
	// The last cell of the full 32-bit grid must not overflow.
	assert.Equal(t, uint64(math.MaxUint32)<<32|uint64(math.MaxUint32), mustHilbertGrid(t, 32).curveDistanceForCell(math.MaxUint32, 0))
}

func TestHilbertGridCellForCoord(t *testing.T) {
	assert.Equal(t, uint64(0), mustHilbertGrid(t, 4).cellForCoord(-5, 0, 10))
	assert.Equal(t, uint64(15), mustHilbertGrid(t, 4).cellForCoord(50, 0, 10))
	assert.Equal(t, uint64(0), mustHilbertGrid(t, 4).cellForCoord(0, 0, 10))
	assert.Equal(t, uint64(15), mustHilbertGrid(t, 4).cellForCoord(10, 0, 10))
	assert.Equal(t, uint64(0), mustHilbertGrid(t, 4).cellForCoord(3, 3, 3), "degenerate range")
	assert.Equal(t, uint64(math.MaxUint32), mustHilbertGrid(t, 32).cellForCoord(1, 0, 1))
}

func TestHilbertGridCurveDistanceForCellKeepsNeighborsClose(t *testing.T) {
	// Cells of one quadrant occupy one contiguous quarter of the curve,
	// which is what makes a sorted file spatially compact.
	grid := mustHilbertGrid(t, 4)
	side := grid.cellsPerAxis
	half := side / 2
	for qx := range uint64(2) {
		for qy := range uint64(2) {
			lo, hi := uint64(math.MaxUint64), uint64(0)
			for x := qx * half; x < (qx+1)*half; x++ {
				for y := qy * half; y < (qy+1)*half; y++ {
					d := grid.curveDistanceForCell(x, y)
					lo, hi = min(lo, d), max(hi, d)
				}
			}
			assert.Equal(t, half*half-1, hi-lo, "quadrant (%d,%d) is not a contiguous run", qx, qy)
			assert.Zero(t, lo%(half*half), "quadrant (%d,%d) does not start on a quarter boundary", qx, qy)
		}
	}
}

func TestHilbertGridCellForCoordEdgeCases(t *testing.T) {
	assert.Equal(t, uint64(0), mustHilbertGrid(t, 4).cellForCoord(math.NaN(), 0, 10), "NaN clamps to the first cell")
	assert.Equal(t, uint64(15), mustHilbertGrid(t, 4).cellForCoord(math.Inf(1), 0, 10))
	assert.Equal(t, uint64(0), mustHilbertGrid(t, 4).cellForCoord(math.Inf(-1), 0, 10))
	assert.Equal(t, uint64(0), mustHilbertGrid(t, 4).cellForCoord(5, 10, 0), "inverted range is degenerate")
	assert.Equal(t, uint64(0), mustHilbertGrid(t, 1).cellForCoord(4.9, 0, 10))
	assert.Equal(t, uint64(1), mustHilbertGrid(t, 1).cellForCoord(5, 0, 10), "midpoint starts the upper half")
	assert.Equal(t, uint64(8), mustHilbertGrid(t, 4).cellForCoord(5, 0, 10), "midpoint of a 4-bit grid")
	assert.Equal(t, uint64(14), mustHilbertGrid(t, 4).cellForCoord(9.3, 0, 10), "the last cell is a full-width bucket, not just hi")
	assert.Equal(t, uint64(15), mustHilbertGrid(t, 4).cellForCoord(9.4, 0, 10))
	// Monotonic: a larger value never lands in an earlier cell.
	grid := mustHilbertGrid(t, 6)
	prev := uint64(0)
	for i := range 101 {
		cell := grid.cellForCoord(float64(i), 0, 100)
		assert.GreaterOrEqual(t, cell, prev)
		prev = cell
	}
}

// wkbCenter is a thin wrapper over internal.WKBCenterXY, whose decoding
// is covered in table/internal; these cases cover only the wrapper's own
// behaviour: the empty-value error and the no-location NaN mapping.
func TestWKBCenterErrors(t *testing.T) {
	_, _, err := wkbCenter(nil)
	require.Error(t, err, "nil value")

	_, _, err = wkbCenter([]byte{})
	require.Error(t, err, "zero-length value")
}

func TestWKBCenterNoLocationIsNaN(t *testing.T) {
	for _, s := range []string{"POINT EMPTY", "GEOMETRYCOLLECTION (POINT EMPTY, POINT EMPTY)"} {
		x, y, err := wkbCenter(hilbertTestWKB(t, s))
		require.NoError(t, err, s)
		assert.True(t, math.IsNaN(x) && math.IsNaN(y), "%s: got (%v, %v)", s, x, y)
	}

	x, y, err := wkbCenter(hilbertTestWKB(t, "POINT (3 4)"))
	require.NoError(t, err)
	assert.Equal(t, 3.0, x)
	assert.Equal(t, 4.0, y)
}

func TestAppendGeometryCenters(t *testing.T) {
	values := [][]byte{
		hilbertTestWKB(t, "POINT (1 2)"),
		nil,
		hilbertTestWKB(t, "POINT EMPTY"),
		hilbertTestWKB(t, "LINESTRING (0 0, 4 8)"),
	}
	for _, dt := range []arrow.BinaryDataType{arrow.BinaryTypes.Binary, arrow.BinaryTypes.LargeBinary} {
		t.Run(dt.String(), func(t *testing.T) {
			batch := hilbertTestBatch(t, dt, values)
			defer batch.Release()

			var xs, ys []float64
			require.NoError(t, appendGeometryCenters(batch.Column(0), &xs, &ys))
			require.Len(t, xs, 4)
			require.Len(t, ys, 4)

			assert.Equal(t, 1.0, xs[0])
			assert.Equal(t, 2.0, ys[0])
			assert.True(t, math.IsNaN(xs[1]) && math.IsNaN(ys[1]), "null row")
			assert.True(t, math.IsNaN(xs[2]) && math.IsNaN(ys[2]), "empty geometry")
			assert.Equal(t, 2.0, xs[3])
			assert.Equal(t, 4.0, ys[3])
		})
	}
}

func TestAppendGeometryCentersAppends(t *testing.T) {
	batch := hilbertTestBatch(t, arrow.BinaryTypes.Binary, [][]byte{hilbertTestWKB(t, "POINT (5 6)")})
	defer batch.Release()

	xs, ys := []float64{-1}, []float64{-2}
	require.NoError(t, appendGeometryCenters(batch.Column(0), &xs, &ys))
	assert.Equal(t, []float64{-1, 5}, xs)
	assert.Equal(t, []float64{-2, 6}, ys)
}

func TestAppendGeometryCentersErrors(t *testing.T) {
	t.Run("unsupported storage", func(t *testing.T) {
		bldr := array.NewInt64Builder(memory.DefaultAllocator)
		defer bldr.Release()
		bldr.Append(1)
		col := bldr.NewArray()
		defer col.Release()

		var xs, ys []float64
		require.ErrorIs(t, appendGeometryCenters(col, &xs, &ys), iceberg.ErrNotImplemented)
	})

	t.Run("malformed row is reported with its index", func(t *testing.T) {
		batch := hilbertTestBatch(t, arrow.BinaryTypes.Binary, [][]byte{
			hilbertTestWKB(t, "POINT (1 1)"),
			{0xff, 0x01},
		})
		defer batch.Release()

		var xs, ys []float64
		err := appendGeometryCenters(batch.Column(0), &xs, &ys)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "row 1")
	})
}

func TestHilbertStrategyRowCurvePositions(t *testing.T) {
	batch := hilbertTestBatch(t, arrow.BinaryTypes.Binary, [][]byte{
		hilbertTestWKB(t, "POINT (0 0)"),
		nil,
		hilbertTestWKB(t, "POINT (10 10)"),
		hilbertTestWKB(t, "POINT EMPTY"),
		hilbertTestWKB(t, "POINT (0 10)"),
	})
	defer batch.Release()

	t.Run("derived extent", func(t *testing.T) {
		s := hilbertStrategy{grid: mustHilbertGrid(t, 2)}
		positions, err := s.rowCurvePositions(context.Background(), []arrow.RecordBatch{batch}, 0, batch.NumRows())
		require.NoError(t, err)
		require.Len(t, positions, 5)

		// The data spans the full extent, so the corners map to the corner
		// cells of the 4x4 grid.
		assert.Equal(t, mustHilbertGrid(t, 2).curveDistanceForCell(0, 0), positions[0].distance)
		assert.Equal(t, mustHilbertGrid(t, 2).curveDistanceForCell(3, 3), positions[2].distance)
		assert.Equal(t, mustHilbertGrid(t, 2).curveDistanceForCell(0, 3), positions[4].distance)
		for _, i := range []int{0, 2, 4} {
			assert.True(t, positions[i].located, "row %d", i)
		}
		assert.False(t, positions[1].located, "null is unlocated")
		assert.False(t, positions[3].located, "empty is unlocated")
	})

	t.Run("fixed extent", func(t *testing.T) {
		// A much larger extent squeezes the data into the first cell.
		s := hilbertStrategy{grid: mustHilbertGrid(t, 2), cfg: hilbertConfig{bounds: &[4]float64{0, 0, 1000, 1000}}}
		positions, err := s.rowCurvePositions(context.Background(), []arrow.RecordBatch{batch}, 0, batch.NumRows())
		require.NoError(t, err)
		for _, i := range []int{0, 2, 4} {
			assert.Equal(t, mustHilbertGrid(t, 2).curveDistanceForCell(0, 0), positions[i].distance, "row %d", i)
		}
	})

	t.Run("fixed extent clamps outside values", func(t *testing.T) {
		s := hilbertStrategy{grid: mustHilbertGrid(t, 2), cfg: hilbertConfig{bounds: &[4]float64{0, 0, 5, 5}}}
		positions, err := s.rowCurvePositions(context.Background(), []arrow.RecordBatch{batch}, 0, batch.NumRows())
		require.NoError(t, err)
		assert.Equal(t, mustHilbertGrid(t, 2).curveDistanceForCell(3, 3), positions[2].distance, "(10,10) clamps to the far corner")
		assert.Equal(t, mustHilbertGrid(t, 2).curveDistanceForCell(0, 3), positions[4].distance)
	})

	t.Run("all rows without a location", func(t *testing.T) {
		only := hilbertTestBatch(t, arrow.BinaryTypes.Binary, [][]byte{nil, hilbertTestWKB(t, "POINT EMPTY")})
		defer only.Release()

		s := hilbertStrategy{grid: mustHilbertGrid(t, 8)}
		positions, err := s.rowCurvePositions(context.Background(), []arrow.RecordBatch{only}, 0, only.NumRows())
		require.NoError(t, err)
		assert.Equal(t, []curvePosition{{}, {}}, positions, "unlocated rows are the zero value")
	})

	t.Run("single distinct point", func(t *testing.T) {
		same := hilbertTestBatch(t, arrow.BinaryTypes.Binary, [][]byte{
			hilbertTestWKB(t, "POINT (4 4)"), hilbertTestWKB(t, "POINT (4 4)"),
		})
		defer same.Release()

		s := hilbertStrategy{grid: mustHilbertGrid(t, 8)}
		positions, err := s.rowCurvePositions(context.Background(), []arrow.RecordBatch{same}, 0, same.NumRows())
		require.NoError(t, err)
		assert.Equal(t, []curvePosition{{located: true}, {located: true}}, positions, "a degenerate extent maps to the first cell")
	})

	t.Run("spans batches", func(t *testing.T) {
		other := hilbertTestBatch(t, arrow.BinaryTypes.Binary, [][]byte{hilbertTestWKB(t, "POINT (20 20)")})
		defer other.Release()

		s := hilbertStrategy{grid: mustHilbertGrid(t, 2)}
		positions, err := s.rowCurvePositions(context.Background(), []arrow.RecordBatch{batch, other}, 0, batch.NumRows()+other.NumRows())
		require.NoError(t, err)
		require.Len(t, positions, 6)
		// The extent now reaches 20, so (10,10) is no longer the far corner.
		assert.Equal(t, mustHilbertGrid(t, 2).curveDistanceForCell(3, 3), positions[5].distance)
		assert.NotEqual(t, mustHilbertGrid(t, 2).curveDistanceForCell(3, 3), positions[2].distance)
	})

	t.Run("canceled context", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		cancel()

		s := hilbertStrategy{grid: mustHilbertGrid(t, 2)}
		_, err := s.rowCurvePositions(ctx, []arrow.RecordBatch{batch}, 0, batch.NumRows())
		require.ErrorIs(t, err, context.Canceled)
	})
}

func TestHilbertStrategyYieldPartitionOrdersRows(t *testing.T) {
	// Four points, one per quadrant of a 2x2 grid, in scan order
	// (1,0) (0,0) (1,1) (0,1). The curve visits (0,0) (0,1) (1,1) (1,0).
	batch := hilbertTestBatch(t, arrow.BinaryTypes.Binary, [][]byte{
		hilbertTestWKB(t, "POINT (1 0)"),
		hilbertTestWKB(t, "POINT (0 0)"),
		hilbertTestWKB(t, "POINT (1 1)"),
		hilbertTestWKB(t, "POINT (0 1)"),
		nil,
	})

	s := hilbertStrategy{column: "geom", grid: mustHilbertGrid(t, 1), batchRows: 2}
	var got [][2]float64
	var nulls, batches int
	for rec, err := range func(yield func(arrow.RecordBatch, error) bool) {
		s.yieldPartition(context.Background(), []arrow.RecordBatch{batch}, yield)
	} {
		require.NoError(t, err)
		batches++
		assert.LessOrEqual(t, rec.NumRows(), int64(2), "batchRows bounds each output batch")
		col := rec.Column(0).(*array.Binary)
		for i := range col.Len() {
			if col.IsNull(i) {
				nulls++

				continue
			}
			x, y, err := wkbCenter(col.Value(i))
			require.NoError(t, err)
			got = append(got, [2]float64{x, y})
		}
		rec.Release()
	}

	assert.Equal(t, [][2]float64{{0, 0}, {0, 1}, {1, 1}, {1, 0}}, got)
	assert.Equal(t, 1, nulls)
	assert.Equal(t, 3, batches, "five rows in batches of two")
}

func TestHilbertStrategyYieldPartitionErrors(t *testing.T) {
	batch := hilbertTestBatch(t, arrow.BinaryTypes.Binary, [][]byte{hilbertTestWKB(t, "POINT (1 1)")})
	defer batch.Release()

	collect := func(s hilbertStrategy, batches []arrow.RecordBatch) error {
		var first error
		s.yieldPartition(context.Background(), batches, func(rec arrow.RecordBatch, err error) bool {
			if err != nil && first == nil {
				first = err
			}
			if rec != nil {
				rec.Release()
			}

			return true
		})

		return first
	}

	t.Run("missing column", func(t *testing.T) {
		err := collect(hilbertStrategy{column: "nope", grid: mustHilbertGrid(t, 4), batchRows: 10}, []arrow.RecordBatch{batch})
		require.Error(t, err)
		assert.Contains(t, err.Error(), `"nope"`)
	})

	t.Run("differing schemas", func(t *testing.T) {
		other := hilbertTestBatch(t, arrow.BinaryTypes.LargeBinary, [][]byte{hilbertTestWKB(t, "POINT (2 2)")})
		defer other.Release()

		err := collect(hilbertStrategy{column: "geom", grid: mustHilbertGrid(t, 4), batchRows: 10}, []arrow.RecordBatch{batch, other})
		require.Error(t, err)
		assert.Contains(t, err.Error(), "differing schemas")
	})

	t.Run("malformed geometry", func(t *testing.T) {
		bad := hilbertTestBatch(t, arrow.BinaryTypes.Binary, [][]byte{{0xff, 0x01}})
		defer bad.Release()

		err := collect(hilbertStrategy{column: "geom", grid: mustHilbertGrid(t, 4), batchRows: 10}, []arrow.RecordBatch{bad})
		require.Error(t, err)
		assert.Contains(t, err.Error(), "hilbert compaction")
	})

	t.Run("no batches", func(t *testing.T) {
		require.NoError(t, collect(hilbertStrategy{column: "geom", grid: mustHilbertGrid(t, 4), batchRows: 10}, nil))
	})
}

func TestHilbertGridCellForCoordCellsAreEqualWidth(t *testing.T) {
	// 1000 evenly spread values over 8 cells: each cell gets 125.
	grid := mustHilbertGrid(t, 3)
	counts := make([]int, 8)
	for i := range 1000 {
		counts[grid.cellForCoord(float64(i)+0.5, 0, 1000)]++
	}
	for cell, n := range counts {
		assert.Equal(t, 125, n, "cell %d", cell)
	}
}

func TestCurvePositionCompare(t *testing.T) {
	located := func(d uint64) curvePosition { return curvePosition{distance: d, located: true} }
	unlocated := curvePosition{}

	assert.Negative(t, located(1).compare(located(2)))
	assert.Positive(t, located(2).compare(located(1)))
	assert.Zero(t, located(5).compare(located(5)))
	assert.Zero(t, unlocated.compare(unlocated))
	// Unlocated rows sort after every located row, including one at the
	// largest distance a 32-bit grid can produce, which is MaxUint64.
	assert.Negative(t, located(math.MaxUint64).compare(unlocated))
	assert.Positive(t, unlocated.compare(located(math.MaxUint64)))
	assert.Positive(t, unlocated.compare(located(0)))
}

// hilbertOrderedIDs runs yieldPartition over batches of (id, geom) rows
// and returns the ids in output order; a null geometry row's id is still
// returned, so the test can check where nulls land.
func hilbertOrderedIDs(t *testing.T, s hilbertStrategy, values [][]byte) []int {
	t.Helper()

	geomBldr := array.NewBinaryBuilder(memory.DefaultAllocator, arrow.BinaryTypes.Binary)
	defer geomBldr.Release()
	idBldr := array.NewInt32Builder(memory.DefaultAllocator)
	defer idBldr.Release()
	for i, v := range values {
		idBldr.Append(int32(i))
		if v == nil {
			geomBldr.AppendNull()
		} else {
			geomBldr.Append(v)
		}
	}
	idCol, geomCol := idBldr.NewArray(), geomBldr.NewArray()
	defer idCol.Release()
	defer geomCol.Release()
	sc := arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int32},
		{Name: "geom", Type: arrow.BinaryTypes.Binary, Nullable: true},
	}, nil)
	batch := array.NewRecordBatch(sc, []arrow.Array{idCol, geomCol}, int64(len(values)))
	defer batch.Release()

	var got []int
	s.yieldPartition(context.Background(), []arrow.RecordBatch{batch}, func(rec arrow.RecordBatch, err error) bool {
		require.NoError(t, err)
		defer rec.Release()
		for _, id := range rec.Column(0).(*array.Int32).Int32Values() {
			got = append(got, int(id))
		}

		return true
	})

	return got
}

func TestHilbertStrategyYieldPartitionNullsLastAt32Bits(t *testing.T) {
	// On a 32-bit grid, cell (MaxUint32, 0) has distance MaxUint64, so a
	// numeric sentinel for "no location" would collide with it. With
	// bounds [0,1] x [0,1], POINT (1 0) lands exactly in that cell.
	s := hilbertStrategy{
		column:    "geom",
		grid:      mustHilbertGrid(t, 32),
		cfg:       hilbertConfig{bounds: &[4]float64{0, 0, 1, 1}},
		batchRows: 100,
	}
	lastCell := hilbertTestWKB(t, "POINT (1 0)")
	first := hilbertTestWKB(t, "POINT (0 0)")

	// Nulls and empties before, between and after rows in the last cell.
	got := hilbertOrderedIDs(t, s, [][]byte{
		nil,                              // 0
		lastCell,                         // 1
		hilbertTestWKB(t, "POINT EMPTY"), // 2
		lastCell,                         // 3
		nil,                              // 4
		first,                            // 5
		hilbertTestWKB(t, "POINT (5 0)"), // 6: x clamps to the edge, so it lands in the last cell too
	})
	assert.Equal(t, []int{5, 1, 3, 6, 0, 2, 4}, got,
		"located rows first (stable within a cell), then unlocated rows in scan order")
}

func TestHilbertGridCellForCoordExtremeRange(t *testing.T) {
	// hi - lo overflows to +Inf for this range; the cell must still spread
	// across the grid rather than collapsing to cell 0.
	grid := mustHilbertGrid(t, 4)
	lo, hi := -math.MaxFloat64, math.MaxFloat64
	assert.Equal(t, uint64(0), grid.cellForCoord(lo, lo, hi))
	assert.Equal(t, uint64(8), grid.cellForCoord(0, lo, hi))
	assert.Equal(t, uint64(15), grid.cellForCoord(hi, lo, hi))
	assert.Equal(t, uint64(4), grid.cellForCoord(-hi/2, lo, hi))
	assert.Equal(t, uint64(12), grid.cellForCoord(hi/2, lo, hi))

	// A range spanning just over half the domain also overflows.
	lo, hi = -math.MaxFloat64/2, math.MaxFloat64
	assert.Equal(t, uint64(15), grid.cellForCoord(hi, lo, hi))
	assert.Equal(t, uint64(0), grid.cellForCoord(lo, lo, hi))
	assert.Greater(t, grid.cellForCoord(0, lo, hi), uint64(0))
	assert.Less(t, grid.cellForCoord(0, lo, hi), uint64(15))

	// Still monotonic across the whole domain.
	prev := uint64(0)
	for _, c := range []float64{lo, lo / 2, -1e300, -1, 0, 1, 1e300, hi / 2, hi} {
		cell := grid.cellForCoord(c, lo, hi)
		assert.GreaterOrEqual(t, cell, prev, "coord %g", c)
		prev = cell
	}
}

func TestHilbertStrategyRowCurvePositionsExtremeDerivedExtent(t *testing.T) {
	// The extent derived from the data can also span the float64 domain.
	huge := math.MaxFloat64
	points := make([][]byte, 0, 3)
	for _, xy := range [][2]float64{{-huge, -huge}, {0, 0}, {huge, huge}} {
		b, err := wkb.Marshal(geom.NewPointFlat(geom.XY, xy[:]), wkb.NDR)
		require.NoError(t, err)
		points = append(points, b)
	}
	batch := hilbertTestBatch(t, arrow.BinaryTypes.Binary, points)
	defer batch.Release()

	s := hilbertStrategy{grid: mustHilbertGrid(t, 4)}
	positions, err := s.rowCurvePositions(context.Background(), []arrow.RecordBatch{batch}, 0, batch.NumRows())
	require.NoError(t, err)
	require.Len(t, positions, 3)
	assert.Equal(t, s.grid.curveDistanceForCell(0, 0), positions[0].distance)
	assert.Equal(t, s.grid.curveDistanceForCell(8, 8), positions[1].distance)
	assert.Equal(t, s.grid.curveDistanceForCell(15, 15), positions[2].distance)
}

func TestHilbertStrategyYieldPartitionEarlyStopReleases(t *testing.T) {
	// Stopping after the first output batch must not leak the batches the
	// gather allocates: nothing more is allocated and the one handed out
	// is owned by the consumer.
	mem := memory.NewCheckedAllocator(memory.DefaultAllocator)
	defer mem.AssertSize(t, 0)
	ctx := compute.WithAllocator(context.Background(), mem)

	// Scan order is reversed from curve order so every output is gathered
	// rather than sliced from the input.
	batch := hilbertTestBatch(t, arrow.BinaryTypes.Binary, [][]byte{
		hilbertTestWKB(t, "POINT (1 0)"),
		hilbertTestWKB(t, "POINT (1 1)"),
		hilbertTestWKB(t, "POINT (0 1)"),
		hilbertTestWKB(t, "POINT (0 0)"),
	})
	defer batch.Release()

	s := hilbertStrategy{column: "geom", grid: mustHilbertGrid(t, 1), batchRows: 2}
	yielded := 0
	stopped := s.yieldPartition(ctx, []arrow.RecordBatch{batch}, func(rec arrow.RecordBatch, err error) bool {
		require.NoError(t, err)
		yielded++
		assert.Equal(t, int64(2), rec.NumRows())
		rec.Release()

		return false
	})
	assert.False(t, stopped, "yieldPartition reports that iteration stopped")
	assert.Equal(t, 1, yielded)
}
