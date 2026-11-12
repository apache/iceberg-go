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
	"cmp"
	"context"
	"errors"
	"fmt"
	"iter"
	"math"
	"slices"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/iceberg-go"
	tblutils "github.com/apache/iceberg-go/table/internal"
)

const (
	// defaultHilbertBits is the number of bits per axis of the Hilbert grid
	// when [WithHilbertBits] is not given: 2^16 cells per axis.
	defaultHilbertBits = 16
	// maxHilbertBits is the largest supported grid; the curve index of a
	// 2^32 x 2^32 grid still fits in a uint64.
	maxHilbertBits = 32
)

// HilbertOption configures [WithCompactionGeometryHilbertOrder].
type HilbertOption func(*hilbertConfig)

// hilbertConfig holds the settings of one Hilbert compaction. Build it
// with [HilbertOption] values rather than by hand.
type hilbertConfig struct {
	// bits is the number of bits per axis of the grid geometries are
	// snapped to, so the grid has 2^bits x 2^bits cells. It is validated
	// to [1, maxHilbertBits] when the strategy is created.
	bits int

	// bounds is the fixed extent mapped onto the grid, ordered
	// {minX, minY, maxX, maxY} in the column's coordinate units. Nil means
	// derive the extent from the data of each partition. When set it is
	// validated to be finite with min < max on both axes.
	bounds *[4]float64
}

// WithHilbertBits sets the number of bits per axis of the grid the
// geometries are snapped to before their Hilbert index is computed, in
// [1, 32]. The default is 16 (65536 x 65536 cells). More bits resolve
// closer geometries at no extra memory cost.
func WithHilbertBits(bits int) HilbertOption {
	return func(c *hilbertConfig) { c.bits = bits }
}

// WithHilbertBounds fixes the extent mapped onto the Hilbert grid, in the
// column's coordinate units (x then y, for example lon/lat). Geometries
// outside it are clamped to its edge. By default the extent is the
// bounding box of the geometries of each partition of the group, which
// gives the best locality within one group; fix it to get the same cell
// size across groups (for example the whole world) so files of different
// groups line up on one curve.
func WithHilbertBounds(minX, minY, maxX, maxY float64) HilbertOption {
	return func(c *hilbertConfig) { c.bounds = &[4]float64{minX, minY, maxX, maxY} }
}

// hilbertStrategy writes each partition of a compaction group ordered by
// the Hilbert index of its geometry column, so rows that are close in
// space land in the same file and row group and file-level bounding
// boxes overlap little.
type hilbertStrategy struct {
	column    string
	cfg       hilbertConfig
	grid      hilbertGrid
	spec      iceberg.PartitionSpec
	schema    *iceberg.Schema
	batchRows int64
}

func newHilbertStrategy(tbl *Table, column string, opts []HilbertOption, batchRows int64) (hilbertStrategy, error) {
	cfg := hilbertConfig{bits: defaultHilbertBits}
	for _, opt := range opts {
		opt(&cfg)
	}
	grid, err := newHilbertGrid(cfg.bits)
	if err != nil {
		return hilbertStrategy{}, err
	}
	if b := cfg.bounds; b != nil {
		for _, v := range b {
			if math.IsNaN(v) || math.IsInf(v, 0) {
				return hilbertStrategy{}, fmt.Errorf("%w: hilbert bounds must be finite", ErrInvalidOperation)
			}
		}
		if b[0] >= b[2] || b[1] >= b[3] {
			return hilbertStrategy{}, fmt.Errorf("%w: hilbert bounds must have min < max on both axes", ErrInvalidOperation)
		}
	}

	schema := tbl.Schema()
	idx := slices.IndexFunc(schema.Fields(), func(f iceberg.NestedField) bool { return f.Name == column })
	if idx < 0 {
		return hilbertStrategy{}, fmt.Errorf("%w: hilbert compaction: %q is not a top-level column", ErrInvalidOperation, column)
	}
	switch schema.Fields()[idx].Type.(type) {
	case iceberg.GeometryType, iceberg.GeographyType:
	default:
		return hilbertStrategy{}, fmt.Errorf("%w: hilbert compaction: column %q has type %s, not geometry or geography",
			ErrInvalidOperation, column, schema.Fields()[idx].Type)
	}

	return hilbertStrategy{
		column:    column,
		cfg:       cfg,
		grid:      grid,
		spec:      tbl.Spec(),
		schema:    schema,
		batchRows: batchRows,
	}, nil
}

func (s hilbertStrategy) records(ctx context.Context, scan *Scan, tasks []FileScanTask) (*arrow.Schema, iter.Seq2[arrow.RecordBatch, error], error) {
	sc, records, err := scan.ReadTasks(ctx, tasks)
	if err != nil {
		return nil, nil, err
	}

	return sc, s.orderedRecords(ctx, records), nil
}

// writeOptions claims no sort order: Hilbert order is not an Iceberg sort
// order. The presorted option still matters, because without it the
// writer would re-sort every batch by the table's default sort order and
// destroy the curve order.
func (s hilbertStrategy) writeOptions() []WriteRecordOption {
	return []WriteRecordOption{withPresortedInput(UnsortedSortOrderID)}
}

// orderedRecords materializes records, splits the rows by the table's
// current partition spec and yields each partition's rows in Hilbert
// order, one partition after another. The whole input is held in memory
// until the last partition is emitted.
func (s hilbertStrategy) orderedRecords(ctx context.Context, records iter.Seq2[arrow.RecordBatch, error]) iter.Seq2[arrow.RecordBatch, error] {
	return func(yield func(arrow.RecordBatch, error) bool) {
		parts, _, err := collectCompactionPartitions(ctx, s.spec, s.schema, UnsortedSortOrder, records)
		defer func() { releaseCompactionPartitions(parts) }()
		if err != nil {
			yield(nil, err)

			return
		}

		for i := range parts {
			if !s.yieldPartition(ctx, parts[i].batches, yield) {
				return
			}
			releaseCompactionPartitions(parts[i : i+1])
			parts[i].batches = nil
		}
	}
}

func (s hilbertStrategy) yieldPartition(ctx context.Context, batches []arrow.RecordBatch, yield func(arrow.RecordBatch, error) bool) bool {
	if len(batches) == 0 {
		return true
	}

	total, err := checkCompactionBatches(batches)
	if err != nil {
		yield(nil, err)

		return false
	}

	colIdx := batches[0].Schema().FieldIndices(s.column)
	if len(colIdx) != 1 {
		yield(nil, fmt.Errorf("hilbert compaction: column %q not found in scan output", s.column))

		return false
	}

	positions, err := s.rowCurvePositions(ctx, batches, colIdx[0], total)
	if err != nil {
		yield(nil, fmt.Errorf("hilbert compaction: %w", err))

		return false
	}

	refs := naturalRowRefs(batches, total)
	order := make([]rowAtPosition, len(refs))
	for i, ref := range refs {
		order[i] = rowAtPosition{pos: positions[i], ref: ref}
	}
	// Stable, so rows at the same position (and rows with no location,
	// which all sort last) keep their scan order.
	slices.SortStableFunc(order, func(a, b rowAtPosition) int { return a.pos.compare(b.pos) })
	for i := range order {
		refs[i] = order[i].ref
	}

	return yieldOrderedRows(ctx, batches, refs, s.batchRows, yield)
}

// curvePosition is where a row falls in the Hilbert order.
type curvePosition struct {
	// distance is the row's distance along the curve. It is meaningful
	// only when located is true.
	distance uint64
	// located is false for a row with no location (a null or empty
	// geometry). Such rows sort after every located row. The flag is
	// separate from distance because every uint64 is a valid distance on
	// a 32-bit grid, so no numeric value can be reserved as a sentinel.
	located bool
}

// compare orders located rows by distance and puts unlocated rows last.
func (p curvePosition) compare(q curvePosition) int {
	if p.located != q.located {
		if p.located {
			return -1
		}

		return 1
	}

	return cmp.Compare(p.distance, q.distance)
}

// rowAtPosition pairs a row with its position in the Hilbert order.
type rowAtPosition struct {
	pos curvePosition
	ref rowRef
}

// rowCurvePositions returns the position in the Hilbert order of every row
// of batches, in scan order. Rows with a null or empty geometry are
// unlocated and so sort last.
func (s hilbertStrategy) rowCurvePositions(ctx context.Context, batches []arrow.RecordBatch, col int, total int64) ([]curvePosition, error) {
	xs := make([]float64, 0, total)
	ys := make([]float64, 0, total)
	for _, batch := range batches {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		if err := appendGeometryCenters(batch.Column(col), &xs, &ys); err != nil {
			return nil, err
		}
	}

	minX, minY, maxX, maxY := math.Inf(1), math.Inf(1), math.Inf(-1), math.Inf(-1)
	if b := s.cfg.bounds; b != nil {
		minX, minY, maxX, maxY = b[0], b[1], b[2], b[3]
	} else {
		for i, x := range xs {
			if math.IsNaN(x) {
				continue
			}
			minX, maxX = min(minX, x), max(maxX, x)
			minY, maxY = min(minY, ys[i]), max(maxY, ys[i])
		}
	}

	positions := make([]curvePosition, len(xs))
	for i, x := range xs {
		if math.IsNaN(x) {
			continue // unlocated: the zero value
		}
		positions[i] = curvePosition{
			located: true,
			distance: s.grid.curveDistanceForCell(
				s.grid.cellForCoord(x, minX, maxX),
				s.grid.cellForCoord(ys[i], minY, maxY)),
		}
	}

	return positions, nil
}

// hilbertGrid is a square grid of 2^bits x 2^bits cells whose cells are
// numbered along a Hilbert curve. Construct it with [newHilbertGrid] so
// bits is always in range.
type hilbertGrid struct {
	// bits is the number of bits per axis.
	bits int
	// cellsPerAxis is 2^bits. Cells are numbered 0 to cellsPerAxis-1 on
	// each axis; this depends only on bits, not on the data.
	cellsPerAxis uint64
}

func newHilbertGrid(bits int) (hilbertGrid, error) {
	if bits < 1 || bits > maxHilbertBits {
		return hilbertGrid{}, fmt.Errorf("%w: hilbert bits must be in the range of [1, %d], got %d",
			ErrInvalidOperation, maxHilbertBits, bits)
	}

	return hilbertGrid{bits: bits, cellsPerAxis: uint64(1) << bits}, nil
}

// cellForCoord maps coord, in the range [lo, hi] of one axis, onto one of
// the grid's equal-width cells on that axis, clamping values outside the
// range. hi itself falls in the last cell. A degenerate range (all
// geometries on one line) maps everything to cell 0.
func (g hilbertGrid) cellForCoord(coord, lo, hi float64) uint64 {
	lastCell := g.cellsPerAxis - 1
	if !(hi > lo) {
		return 0
	}

	// Clamp before normalizing, so the endpoints never go through the
	// division below. The negated comparison also sends NaN to cell 0.
	switch {
	case !(coord > lo):
		return 0
	case coord >= hi:
		return lastCell
	}

	// Normalize on halved values. The plain (coord-lo)/(hi-lo) overflows
	// to +Inf when the range spans more than MaxFloat64 (for example
	// [-MaxFloat64, MaxFloat64]), which maps every coordinate to cell 0 and
	// silently defeats clustering. Halving only lowers the exponent, so it
	// is exact except for subnormals, and both differences then fit.
	halfSpan := hi/2 - lo/2
	if !(halfSpan > 0) {
		// Only possible when lo and hi differ by a subnormal amount; the
		// halves collapsed to equal values. Treat it as degenerate.
		return 0
	}
	frac := (coord/2 - lo/2) / halfSpan

	return min(uint64(frac*float64(g.cellsPerAxis)), lastCell)
}

// curveDistanceForCell returns the distance along the Hilbert curve of
// the cell (cellX, cellY), where both are cell numbers in
// [0, cellsPerAxis), not coordinates in the data's units (see cellForCoord).
// The distance is the cell's position in the curve's visiting order, from
// 0 for the first cell to cellsPerAxis^2 - 1 for the last. Consecutive
// distances are always edge-adjacent cells, which is why sorting by this
// distance keeps nearby points together.
//
// The curve is built recursively. Split the grid into four quadrants and
// visit them in the order lower-left, upper-left, upper-right,
// lower-right; each quadrant is itself split the same way, down to single
// cells. A cell's distance is therefore found top-down: at each level,
// work out which quadrant the cell is in and add the number of cells in
// all quadrants visited before it.
func (g hilbertGrid) curveDistanceForCell(cellX, cellY uint64) uint64 {
	cellsPerAxis := g.cellsPerAxis

	// x and y are working copies of the cell: the loop below rotates and
	// mirrors them as it descends the levels of the curve.
	x, y := cellX, cellY

	// distance accumulates the cell's position along the curve. Each level
	// of the loop adds the number of cells that the curve visits before
	// the cell's quadrant, so after the last level it is the final answer.
	var distance uint64

	// s is the width, in cells, of one quadrant of the current square, so
	// it is half the square's width. It starts at half the width of the
	// whole grid (the coarsest split) and halves on every pass, down to 1
	// (single cells). For an 8-wide grid, s descends to values 4, 2, 1 by
	// bit shifting
	for s := cellsPerAxis >> 1; s > 0; s >>= 1 {
		// rx and ry say which half of the current square the cell is in on
		// each axis. Both start at 0 and are set to 1 only when the
		// matching bit of the cell number is set:
		//
		//	rx == 0: the cell is in the left half (smaller x)
		//	rx == 1: the cell is in the right half (larger x)
		//	ry == 0: the cell is in the bottom half (smaller y)
		//	ry == 1: the cell is in the top half (larger y)
		//
		// x&s isolates bit s, which is the one that separates the two
		// halves at this level. A zero result means that bit is clear, so
		// the cell is in the lower half and rx or ry stays 0. The cell's
		// other bits are ignored here: the higher ones were settled by
		// earlier levels and the lower ones are settled by later levels.
		//
		// "Left" and "bottom" are relative to the current orientation of
		// the curve. The rotation step at the end of each pass turns x and
		// y so that this holds at every level.
		var rx, ry uint64
		if x&s != 0 {
			rx = 1
		}
		if y&s != 0 {
			ry = 1
		}

		// quadrant is the position of the cell's quadrant in the visiting
		// order:
		//
		//	(rx, ry) = (0,0) -> 0   lower-left
		//	           (0,1) -> 1   upper-left
		//	           (1,1) -> 2   upper-right
		//	           (1,0) -> 3   lower-right
		//
		// 3*rx ^ ry produces exactly that numbering.
		quadrant := (3 * rx) ^ ry

		// Every earlier quadrant holds s*s cells, all visited before this
		// one, so they add quadrant*s*s to the distance. The largest
		// possible sum is cellsPerAxis^2 - 1, which fits in a uint64 even
		// for 32 bits.
		distance += s * s * quadrant

		// The curve inside a quadrant must be a rotated or mirrored copy of
		// the whole curve so that it enters where the previous quadrant
		// ended and leaves where the next one starts. Instead of rotating
		// the curve, rotate the cell the opposite way and keep using the
		// same orientation for the next, finer level.
		//
		// Quadrants 1 and 2 (ry == 1) are traversed in the same orientation
		// as the whole curve, so the cell is left as it is.
		if ry == 0 {
			if rx == 1 {
				// Quadrant 3 is the whole curve flipped end to end: mirror
				// the cell on both axes. n-1-x flips every bit of x. That
				// includes the higher bits already consumed, which is
				// harmless because later levels only read lower bits.
				x = cellsPerAxis - 1 - x
				y = cellsPerAxis - 1 - y
			}
			// Quadrants 0 and 3 are also turned a quarter turn, which
			// swapping x and y does.
			x, y = y, x
		}
	}

	return distance
}

// appendGeometryCenters appends the center of the bounding box of every
// WKB geometry of col; rows with a null, empty or non-finite geometry
// append NaN.
func appendGeometryCenters(col arrow.Array, xs, ys *[]float64) error {
	if ext, ok := col.(array.ExtensionArray); ok {
		col = ext.Storage()
	}

	var value func(i int) []byte
	switch a := col.(type) {
	case *array.Binary:
		value = a.Value
	case *array.LargeBinary:
		value = a.Value
	case *array.BinaryView:
		value = a.Value
	default:
		return fmt.Errorf("%w: geometry column stored as %s", iceberg.ErrNotImplemented, col.DataType())
	}

	for i := range col.Len() {
		x, y := math.NaN(), math.NaN()
		if col.IsValid(i) {
			var err error
			if x, y, err = wkbCenter(value(i)); err != nil {
				return fmt.Errorf("row %d: %w", i, err)
			}
		}
		*xs = append(*xs, x)
		*ys = append(*ys, y)
	}

	return nil
}

// wkbCenter returns the center of the XY bounding box of a WKB geometry,
// or NaN when the geometry has no location: it is empty, every member of
// it is empty, or its coordinates are not finite. It decodes through the
// same code as the writer's geo statistics, so it accepts every encoding
// the writer accepts (ISO WKB and EWKB, with or without an SRID), walks
// nested geometry collections and ignores empty members.
func wkbCenter(b []byte) (x, y float64, err error) {
	if len(b) == 0 {
		return math.NaN(), math.NaN(), errors.New("empty WKB value")
	}
	x, y, ok, err := tblutils.WKBCenterXY(b)
	if err != nil {
		return 0, 0, fmt.Errorf("decode WKB: %w", err)
	}
	if !ok {
		return math.NaN(), math.NaN(), nil
	}

	return x, y, nil
}
