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

package table_test

import (
	"context"
	"encoding/hex"
	"fmt"
	"math"
	"math/rand/v2"
	"path/filepath"
	"slices"
	"strings"
	"testing"

	"github.com/apache/iceberg-go"
	iceio "github.com/apache/iceberg-go/io"
	"github.com/apache/iceberg-go/table"
	tblutils "github.com/apache/iceberg-go/table/internal"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/twpayne/go-geom"
	"github.com/twpayne/go-geom/encoding/ewkb"
	"github.com/twpayne/go-geom/encoding/wkb"
	"github.com/twpayne/go-geom/encoding/wkbcommon"
	"github.com/twpayne/go-geom/encoding/wkt"
)

const (
	hilbertGridSide   = 8
	hilbertGeomField  = 2
	hilbertLabelField = 3
)

// newHilbertTestTable builds an unpartitioned v3 table with an id and a
// geometry column. Its default sort order is by id, so a writer that
// re-sorted the compacted rows would visibly undo the curve order.
func newHilbertTestTable(t *testing.T) *table.Table {
	t.Helper()

	return newHilbertTestTableWith(t, iceberg.UnpartitionedSpec, iceberg.GeometryType{})
}

// newHilbertTestTableWith builds the id/geom/label test table with the
// given partition spec and geo column type.
func newHilbertTestTableWith(t *testing.T, spec *iceberg.PartitionSpec, geoType iceberg.Type) *table.Table {
	t.Helper()

	location := filepath.ToSlash(t.TempDir())
	schema := iceberg.NewSchema(0,
		iceberg.NestedField{ID: 1, Name: "id", Type: iceberg.PrimitiveTypes.Int64, Required: true},
		iceberg.NestedField{ID: hilbertGeomField, Name: "geom", Type: geoType, Required: false},
		iceberg.NestedField{ID: hilbertLabelField, Name: "label", Type: iceberg.PrimitiveTypes.String, Required: false},
	)
	meta, err := table.NewMetadata(schema, spec, idSortOrder(t, 1, iceberg.IdentityTransform{}), location,
		iceberg.Properties{table.PropertyFormatVersion: "3"})
	require.NoError(t, err)

	return table.New(
		table.Identifier{"db", "hilbert_compaction_test"},
		meta, location+"/metadata/v1.metadata.json",
		func(context.Context) (iceio.IO, error) { return iceio.LocalFS{}, nil },
		&rowDeltaCatalog{metadata: meta},
	)
}

// hilbertRow is one row of a test data file. An empty wkbHex is a null
// geometry.
type hilbertRow struct {
	id     int64
	wkbHex string
	label  string
}

func pointHex(t *testing.T, x, y float64) string {
	t.Helper()

	b, err := wkb.Marshal(geom.NewPointFlat(geom.XY, []float64{x, y}), wkb.NDR)
	require.NoError(t, err)

	return hex.EncodeToString(b)
}

// wktHex encodes s as ISO WKB hex, with empty points as NaN coordinates.
func wktHex(t *testing.T, s string) string {
	t.Helper()

	g, err := wkt.Unmarshal(s)
	require.NoError(t, err)
	b, err := wkb.Marshal(g, wkb.NDR, wkbcommon.WKBOptionEmptyPointHandling(wkbcommon.EmptyPointHandlingNaN))
	require.NoError(t, err)

	return hex.EncodeToString(b)
}

// gridRows returns one row per grid cell id, where id is y*side+x, all
// with label. A negative id makes a row with a null geometry.
func gridRows(t *testing.T, label string, ids []int64) []hilbertRow {
	t.Helper()

	rows := make([]hilbertRow, len(ids))
	for i, id := range ids {
		rows[i] = hilbertRow{id: id, label: label}
		if id >= 0 {
			rows[i].wkbHex = pointHex(t, float64(id%hilbertGridSide), float64(id/hilbertGridSide))
		}
	}

	return rows
}

// gridCell returns the (x, y) cell of a gridRows id.
func gridCell(id int64) (x, y int64) { return id % hilbertGridSide, id / hilbertGridSide }

// addHilbertRows writes one data file holding rows and adds it to tbl.
func addHilbertRows(t *testing.T, tbl *table.Table, name string, rows []hilbertRow) *table.Table {
	t.Helper()

	arrowSc, err := table.SchemaToArrowSchemaWithOptions(tbl.Schema(), table.ArrowSchemaOptions{
		IncludeFieldIDs: true,
		TableProperties: tbl.Properties(),
	})
	require.NoError(t, err)

	jsonRows := make([]string, len(rows))
	for i, r := range rows {
		geomJSON := "null"
		if r.wkbHex != "" {
			geomJSON = fmt.Sprintf("%q", r.wkbHex)
		}
		jsonRows[i] = fmt.Sprintf(`{"id": %d, "geom": %s, "label": %q}`, r.id, geomJSON, r.label)
	}
	dataPath := tbl.Location() + "/data/" + name + ".parquet"
	writeParquetFile(t, dataPath, arrowSc, "["+strings.Join(jsonRows, ",")+"]")

	tx := tbl.NewTransaction()
	require.NoError(t, tx.AddDataFilesByPath(t.Context(), []string{dataPath}, nil, false))
	tbl, err = tx.Commit(t.Context())
	require.NoError(t, err)

	return tbl
}

// addHilbertTestFile writes one file of grid cells (see gridRows) with
// label "x".
func addHilbertTestFile(t *testing.T, tbl *table.Table, name string, ids []int64) *table.Table {
	t.Helper()

	return addHilbertRows(t, tbl, name, gridRows(t, "x", ids))
}

// shuffledGridIDs returns the ids of every cell of the grid in a fixed
// pseudo-random order.
func shuffledGridIDs() []int64 {
	cells := make([]int64, hilbertGridSide*hilbertGridSide)
	for i := range cells {
		cells[i] = int64(i)
	}
	rand.New(rand.NewPCG(1, 2)).Shuffle(len(cells), func(i, j int) { cells[i], cells[j] = cells[j], cells[i] })

	return cells
}

// assertWalksCurve checks that consecutive ids are edge-adjacent grid
// cells, which is what a Hilbert order of a full grid guarantees, and that
// every cell of the grid appears once.
func assertWalksCurve(t *testing.T, ids []int64) {
	t.Helper()

	require.Len(t, ids, hilbertGridSide*hilbertGridSide)
	seen := map[int64]bool{}
	for i, id := range ids {
		seen[id] = true
		if i == 0 {
			continue
		}
		px, py := gridCell(ids[i-1])
		x, y := gridCell(id)
		dx, dy := x-px, y-py
		assert.Equal(t, int64(1), dx*dx+dy*dy, "rows %d and %d are not adjacent cells: %d -> %d", i-1, i, ids[i-1], id)
	}
	assert.Len(t, seen, hilbertGridSide*hilbertGridSide)
}

// hilbertOutputIDs runs a Hilbert compaction of every file of tbl as one
// group and returns the ids of the output files in order, plus the files.
func hilbertOutputIDs(t *testing.T, tbl *table.Table, opts ...table.CompactionGroupOption) ([]int64, []iceberg.DataFile) {
	t.Helper()

	group := sortTestGroup(t, tbl)
	result, err := table.ExecuteCompactionGroup(t.Context(), tbl, group,
		append([]table.CompactionGroupOption{table.WithCompactionGeometryHilbertOrder("geom", table.WithHilbertBits(3))}, opts...)...)
	require.NoError(t, err)
	require.NotEmpty(t, result.NewDataFiles)

	var got []int64
	for _, df := range result.NewDataFiles {
		// Hilbert order is not an Iceberg sort order.
		assert.Nil(t, df.SortOrderID())
		got = append(got, readInt64Column(t, df.FilePath(), "id")...)
	}

	return got, result.NewDataFiles
}

func TestExecuteCompactionGroup_HilbertOrder(t *testing.T) {
	tbl := newHilbertTestTable(t)

	// Shuffle the 64 grid cells over four files, plus two null geometries.
	cells := shuffledGridIDs()
	for f := range 4 {
		ids := append([]int64(nil), cells[f*16:(f+1)*16]...)
		if f == 1 {
			ids = append(ids, -1, -2)
		}
		tbl = addHilbertTestFile(t, tbl, fmt.Sprintf("f%d", f), ids)
	}

	got, _ := hilbertOutputIDs(t, tbl)
	require.Len(t, got, 66)

	// Null geometries go last; the 64 cells before them walk the curve.
	assert.Equal(t, []int64{-1, -2}, got[64:])
	assertWalksCurve(t, got[:64])
}

func TestExecuteCompactionGroup_HilbertOrderRollsFiles(t *testing.T) {
	tbl := newHilbertTestTable(t)
	cells := shuffledGridIDs()
	for f := range 4 {
		tbl = addHilbertTestFile(t, tbl, fmt.Sprintf("f%d", f), cells[f*16:(f+1)*16])
	}

	// Sixteen-row batches and a one-byte target roll a file after every
	// batch, so the 64 cells land in four files of sixteen.
	got, files := hilbertOutputIDs(t, tbl,
		table.WithCompactionArrowBatchSize(16),
		table.WithCompactionTargetFileSize(1))
	require.Len(t, files, 4)
	assertWalksCurve(t, got)

	// Sixteen consecutive cells of the 8x8 curve are exactly one 4x4
	// quadrant, so each file's rows, and the geometry bounds the writer
	// records for it, cover a 4x4 box rather than the whole grid.
	for _, df := range files {
		ids := readInt64Column(t, df.FilePath(), "id")
		require.Len(t, ids, 16)
		minX, minY, maxX, maxY := int64(math.MaxInt64), int64(math.MaxInt64), int64(-1), int64(-1)
		for _, id := range ids {
			x, y := gridCell(id)
			minX, maxX = min(minX, x), max(maxX, x)
			minY, maxY = min(minY, y), max(maxY, y)
		}
		assert.Equal(t, int64(3), maxX-minX, "file %s spans x %d..%d", df.FilePath(), minX, maxX)
		assert.Equal(t, int64(3), maxY-minY, "file %s spans y %d..%d", df.FilePath(), minY, maxY)

		bMinX, bMinY, bMaxX, bMaxY, ok := tblutils.GeoBoundsXY(
			df.LowerBoundValues()[hilbertGeomField], df.UpperBoundValues()[hilbertGeomField])
		require.True(t, ok, "file %s has no geometry bounds", df.FilePath())
		assert.Equal(t, [4]float64{float64(minX), float64(minY), float64(maxX), float64(maxY)},
			[4]float64{bMinX, bMinY, bMaxX, bMaxY}, "file %s records bounds wider than its rows", df.FilePath())
	}
}

func TestExecuteCompactionGroup_HilbertOrderSplitsPartitions(t *testing.T) {
	spec := iceberg.NewPartitionSpec(iceberg.PartitionField{
		SourceIDs: []int{hilbertLabelField}, FieldID: 1000, Transform: iceberg.IdentityTransform{}, Name: "label",
	})
	tbl := newHilbertTestTableWith(t, &spec, iceberg.GeometryType{})

	// Each partition holds the full grid, shuffled, interleaved across the
	// group's files as when a group spans files written under an older
	// spec. Partition a also has a null.
	cells := shuffledGridIDs()
	for f := range 4 {
		tbl = addHilbertRows(t, tbl, fmt.Sprintf("a%d", f), gridRows(t, "a", cells[f*16:(f+1)*16]))
		tbl = addHilbertRows(t, tbl, fmt.Sprintf("b%d", f), gridRows(t, "b", cells[f*16:(f+1)*16]))
	}
	tbl = addHilbertRows(t, tbl, "a-null", gridRows(t, "a", []int64{-1}))

	group := sortTestGroup(t, tbl)
	result, err := table.ExecuteCompactionGroup(t.Context(), tbl, group,
		table.WithCompactionGeometryHilbertOrder("geom", table.WithHilbertBits(3)))
	require.NoError(t, err)

	got := map[string][]int64{}
	for _, df := range result.NewDataFiles {
		part, ok := df.Partition()[1000].(string)
		require.True(t, ok)
		got[part] = append(got[part], readInt64Column(t, df.FilePath(), "id")...)
	}
	require.Len(t, got, 2)
	require.Len(t, got["a"], 65)
	assert.Equal(t, int64(-1), got["a"][64], "the null row goes last in its partition")
	assertWalksCurve(t, got["a"][:64])
	assertWalksCurve(t, got["b"])
}

func TestExecuteCompactionGroup_HilbertOrderGeography(t *testing.T) {
	geog, err := iceberg.GeographyTypeOf("OGC:CRS84", "spherical")
	require.NoError(t, err)
	tbl := newHilbertTestTableWith(t, iceberg.UnpartitionedSpec, geog)
	cells := shuffledGridIDs()
	for f := range 4 {
		tbl = addHilbertTestFile(t, tbl, fmt.Sprintf("f%d", f), cells[f*16:(f+1)*16])
	}

	// Coordinates are treated as planar, so the walk is the same as for a
	// geometry column.
	got, _ := hilbertOutputIDs(t, tbl)
	assertWalksCurve(t, got)
}

// TestExecuteCompactionGroup_HilbertOrderAcceptsWriterInputs rewrites
// geometries that the ordinary bin-pack rewrite accepts and checks that
// the Hilbert rewrite accepts them too, with the same rows, instead of
// failing or crashing in the center computation. The table is
// unpartitioned, which is the write path without panic recovery.
func TestExecuteCompactionGroup_HilbertOrderAcceptsWriterInputs(t *testing.T) {
	ewkbHex := func(g geom.T) string {
		b, err := ewkb.Marshal(g, ewkb.NDR)
		require.NoError(t, err)

		return hex.EncodeToString(b)
	}

	tests := []struct {
		name string
		rows []hilbertRow
		// want is the expected id order; nil to only check the row set.
		want []int64
	}{
		{
			name: "nested geometry collection",
			rows: []hilbertRow{
				{id: 1, wkbHex: wktHex(t, "GEOMETRYCOLLECTION (GEOMETRYCOLLECTION (POINT (7 7)))")},
				{id: 2, wkbHex: wktHex(t, "GEOMETRYCOLLECTION (GEOMETRYCOLLECTION (POINT (0 0)))")},
				{id: 3, wkbHex: wktHex(t, "GEOMETRYCOLLECTION (GEOMETRYCOLLECTION (GEOMETRYCOLLECTION (POINT (0 7))))")},
			},
			want: []int64{2, 3, 1},
		},
		{
			name: "collection with an empty member keeps its location",
			rows: []hilbertRow{
				{id: 1, wkbHex: wktHex(t, "GEOMETRYCOLLECTION (POINT (7 7), POINT EMPTY)")},
				{id: 2, wkbHex: wktHex(t, "MULTIPOINT ((0 0), EMPTY)")},
				{id: 3, wkbHex: wktHex(t, "POINT (0 7)")},
				// Truly unlocated: must sort after the three located rows.
				{id: 4, wkbHex: wktHex(t, "GEOMETRYCOLLECTION (POINT EMPTY)")},
			},
			want: []int64{2, 3, 1, 4},
		},
		{
			name: "ewkb with srid",
			rows: []hilbertRow{
				{id: 1, wkbHex: ewkbHex(geom.NewPointFlat(geom.XY, []float64{7, 7}).SetSRID(4326))},
				{id: 2, wkbHex: ewkbHex(geom.NewPointFlat(geom.XY, []float64{0, 0}).SetSRID(4326))},
				{id: 3, wkbHex: ewkbHex(geom.NewPointFlat(geom.XY, []float64{0, 7}).SetSRID(4326))},
			},
			want: []int64{2, 3, 1},
		},
		{
			name: "ewkb z and m flags",
			rows: []hilbertRow{
				{id: 1, wkbHex: ewkbHex(geom.NewPointFlat(geom.XYZ, []float64{7, 7, 1}))},
				{id: 2, wkbHex: ewkbHex(geom.NewPointFlat(geom.XYM, []float64{0, 0, 1}))},
				{id: 3, wkbHex: ewkbHex(geom.NewPointFlat(geom.XYZM, []float64{0, 7, 1, 2}))},
			},
			want: []int64{2, 3, 1},
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			tbl := newHilbertTestTable(t)
			tbl = addHilbertRows(t, tbl, "f0", tc.rows)
			group := sortTestGroup(t, tbl)

			// Control: the ordinary rewrite accepts the input.
			control, err := table.ExecuteCompactionGroup(t.Context(), tbl, group)
			require.NoError(t, err, "bin-pack rewrite of the same input must succeed")
			require.Len(t, control.NewDataFiles, 1)
			wantSet := readInt64Column(t, control.NewDataFiles[0].FilePath(), "id")

			result, err := table.ExecuteCompactionGroup(t.Context(), tbl, group,
				table.WithCompactionGeometryHilbertOrder("geom", table.WithHilbertBits(3)))
			require.NoError(t, err)
			require.Len(t, result.NewDataFiles, 1)
			got := readInt64Column(t, result.NewDataFiles[0].FilePath(), "id")
			assert.ElementsMatch(t, wantSet, got, "the Hilbert rewrite must keep every row")
			if tc.want != nil {
				assert.Equal(t, tc.want, got)
			}
		})
	}
}

func TestExecuteCompactionGroup_HilbertOrderAppliesPositionDeletes(t *testing.T) {
	tbl := newHilbertTestTable(t)
	cells := shuffledGridIDs()
	tbl = addHilbertTestFile(t, tbl, "f0", cells[:32])
	tbl = addHilbertTestFile(t, tbl, "f1", cells[32:])
	group := sortTestGroup(t, tbl)
	require.Len(t, group.Tasks, 2)

	// Delete the first two rows of each file.
	deleted := []int64{cells[0], cells[1], cells[32], cells[33]}
	byPath := map[string][]int64{}
	for _, task := range group.Tasks {
		byPath[task.File.FilePath()] = readInt64Column(t, task.File.FilePath(), "id")
	}
	var delRows []string
	for path, ids := range byPath {
		for pos, id := range ids {
			if slices.Contains(deleted, id) {
				delRows = append(delRows, fmt.Sprintf(`{"file_path": %q, "pos": %d}`, path, pos))
			}
		}
	}
	require.Len(t, delRows, 4)
	delPath := tbl.Location() + "/data/pos-del.parquet"
	writeParquetFile(t, delPath, table.PositionalDeleteArrowSchema, "["+strings.Join(delRows, ",")+"]")
	bldr, err := iceberg.NewDataFileBuilder(*iceberg.UnpartitionedSpec, iceberg.EntryContentPosDeletes,
		delPath, iceberg.ParquetFile, nil, nil, nil, int64(len(delRows)), 1)
	require.NoError(t, err)
	posDel := bldr.Build()
	for i := range group.Tasks {
		group.Tasks[i].DeleteFiles = []iceberg.DataFile{posDel}
	}

	result, err := table.ExecuteCompactionGroup(t.Context(), tbl, group,
		table.WithCompactionGeometryHilbertOrder("geom", table.WithHilbertBits(3)),
		table.WithCompactionArrowBatchSize(7),
		table.WithCompactionTargetFileSize(1))
	require.NoError(t, err)
	assert.Len(t, result.SafePosDeletes, 1, "the fully applied delete file is reported for removal")

	var got []int64
	for _, df := range result.NewDataFiles {
		got = append(got, readInt64Column(t, df.FilePath(), "id")...)
	}
	require.Len(t, got, 60)
	for _, id := range deleted {
		assert.NotContains(t, got, id)
	}

	// The survivors are in curve order: with cells missing the walk can
	// jump, but it never goes backwards along the curve. Check that by
	// comparing against the full curve order from a rewrite without
	// deletes.
	full, _ := hilbertOutputIDs(t, tbl)
	rank := map[int64]int{}
	for i, id := range full {
		rank[id] = i
	}
	for i := 1; i < len(got); i++ {
		assert.Less(t, rank[got[i-1]], rank[got[i]], "rows %d and %d are out of curve order", i-1, i)
	}
}

func TestRewriteDataFiles_HilbertOrderPreservesRowLineage(t *testing.T) {
	tbl := newHilbertTestTable(t)
	cells := shuffledGridIDs()
	for f := range 4 {
		tbl = addHilbertTestFile(t, tbl, fmt.Sprintf("f%d", f), cells[f*16:(f+1)*16])
	}

	// v3 assigns each row a _row_id when its file is committed. Reordering
	// the rows must keep every row's id.
	before := rowIDByID(t, tbl)
	require.Len(t, before, 64)

	group := sortTestGroup(t, tbl)
	for _, task := range group.Tasks {
		require.NotNil(t, task.FirstRowID, "source files must carry row lineage")
	}
	res, err := tbl.RewriteDataFiles(t.Context(), []table.CompactionTaskGroup{group}, table.RewriteDataFilesOptions{
		GroupOptions: []table.CompactionGroupOption{
			table.WithCompactionGeometryHilbertOrder("geom", table.WithHilbertBits(3)),
			table.WithCompactionArrowBatchSize(16),
			table.WithCompactionTargetFileSize(1),
		},
	})
	require.NoError(t, err)
	tbl = res.Table

	assert.Equal(t, before, rowIDByID(t, tbl), "every row keeps its _row_id across the rewrite")

	tasks, err := tbl.Scan().PlanFiles(t.Context())
	require.NoError(t, err)
	require.Len(t, tasks, 4)
	slices.SortFunc(tasks, func(a, b table.FileScanTask) int {
		return strings.Compare(a.File.FilePath(), b.File.FilePath())
	})
	var got []int64
	for _, task := range tasks {
		assert.Nil(t, task.File.SortOrderID())
		got = append(got, readInt64Column(t, task.File.FilePath(), "id")...)
	}
	assertWalksCurve(t, got)
}

func TestExecuteCompactionGroup_HilbertOrderRejectsBadConfig(t *testing.T) {
	tbl := newHilbertTestTable(t)
	tbl = addHilbertTestFile(t, tbl, "f0", []int64{0, 1, 2})
	group := sortTestGroup(t, tbl)

	order := idSortOrder(t, 1, iceberg.IdentityTransform{})
	for name, opts := range map[string][]table.CompactionGroupOption{
		"missing column":    {table.WithCompactionGeometryHilbertOrder("nope")},
		"not a geo column":  {table.WithCompactionGeometryHilbertOrder("label")},
		"zero bits":         {table.WithCompactionGeometryHilbertOrder("geom", table.WithHilbertBits(0))},
		"too many bits":     {table.WithCompactionGeometryHilbertOrder("geom", table.WithHilbertBits(33))},
		"inverted bounds":   {table.WithCompactionGeometryHilbertOrder("geom", table.WithHilbertBounds(5, 0, 1, 1))},
		"with sort order":   {table.WithCompactionGeometryHilbertOrder("geom"), table.WithCompactionSortOrder(order)},
		"non-finite bounds": {table.WithCompactionGeometryHilbertOrder("geom", table.WithHilbertBounds(0, 0, math.Inf(1), 1))},
	} {
		t.Run(name, func(t *testing.T) {
			_, err := table.ExecuteCompactionGroup(t.Context(), tbl, group, opts...)
			require.ErrorIs(t, err, table.ErrInvalidOperation)
		})
	}
}
