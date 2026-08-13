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
	"strings"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/compute"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/DataDog/iceberg-go"
	"github.com/DataDog/iceberg-go/io"
	tblutils "github.com/DataDog/iceberg-go/table/internal"
	"github.com/geoarrow/geoarrow-go"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/twpayne/go-geom/encoding/wkb"
	"github.com/twpayne/go-geom/encoding/wkt"
)

const (
	geoTestIDFieldID   = 1
	geoTestGeomFieldID = 2
	geoTestGeogFieldID = 3
)

// wktToWKB converts Well-Known Text into little-endian (NDR) Well-Known Binary,
// the on-disk encoding iceberg-go writes for geometry/geography columns.
func wktToWKB(t *testing.T, s string) geoarrow.WKBBytes {
	t.Helper()

	g, err := wkt.Unmarshal(s)
	require.NoError(t, err)
	b, err := wkb.Marshal(g, wkb.NDR)
	require.NoError(t, err)

	return geoarrow.WKBBytes(b)
}

// newGeoTestWriter builds an unpartitioned v3 table with an id/geom/geog schema
// and returns a data file writer over it. props feed the metrics-mode
// configuration (e.g. write.metadata.metrics.column.geom=none) so tests can
// exercise the stats-plan dispatch that decides whether geo bounds are recorded.
//
// The geo columns are all top-level; nested geo columns are still unhandled in
// the writer (see the TODO(#992) in parquet_files.go), so nothing here covers
// them.
func newGeoTestWriter(t *testing.T, dir string, props iceberg.Properties) (*defaultDataFileWriter, *iceberg.Schema, *arrow.Schema) {
	t.Helper()

	geogType, err := iceberg.GeographyTypeOf("OGC:CRS84", "spherical")
	require.NoError(t, err)

	schema := iceberg.NewSchema(0,
		iceberg.NestedField{ID: geoTestIDFieldID, Name: "id", Type: iceberg.PrimitiveTypes.Int32, Required: false},
		iceberg.NestedField{ID: geoTestGeomFieldID, Name: "geom", Type: iceberg.GeometryType{}, Required: false},
		iceberg.NestedField{ID: geoTestGeogFieldID, Name: "geog", Type: geogType, Required: false},
	)

	// Geometry/geography are v3 types.
	mb, err := NewMetadataBuilder(3)
	require.NoError(t, err)
	require.NoError(t, mb.AddSchema(schema))
	require.NoError(t, mb.SetCurrentSchemaID(0))
	unpartitioned := *iceberg.UnpartitionedSpec
	require.NoError(t, mb.AddPartitionSpec(&unpartitioned, true))
	require.NoError(t, mb.SetDefaultSpecID(0))
	// The stats plan is computed from the table properties on the builder (see
	// defaultDataFileWriter.writeFile), so metrics-mode config must live here.
	if len(props) > 0 {
		require.NoError(t, mb.SetProperties(props))
	}

	arrowSchema, err := SchemaToArrowSchema(schema, nil, true, false)
	require.NoError(t, err)

	writer, err := newDataFileWriter(dir, &io.LocalFS{}, mb, props)
	require.NoError(t, err)

	return writer, schema, arrowSchema
}

// TestWriteGeometryColumnPopulatesBounds writes a geometry and a geography
// column end-to-end through the data file writer and checks the manifest-level
// geo bounds that iceberg-go computes from the WKB values. arrow-go's Parquet
// writer does not emit native GeoStatistics, so iceberg-go derives the column
// bounds itself (see geoBoundsAccumulator) and threads them into the DataFile
// exactly as the manifest carries them. Geometry gets a planar XY bounding box;
// geography stays unbounded because a planar box over geodesic edges is unsafe.
//
// This is the public-path counterpart to internal.TestWriteDataFileGeoBounds:
// that test feeds hand-built StatsCols straight to WriteDataFile, whereas this
// one drives the full table writer (newDataFileWriter/writeFile/WriteTask over a
// MetadataBuilder), so it also covers geo stats-collector setup in the path a
// caller actually uses.
func TestWriteGeometryColumnPopulatesBounds(t *testing.T) {
	t.Parallel()

	writer, schema, arrowSchema := newGeoTestWriter(t, t.TempDir(), iceberg.Properties{})

	// Two geometry points spanning X in [0, 30] and Y in [-5, 10].
	geomLo := wktToWKB(t, "POINT (0 -5)")
	geomHi := wktToWKB(t, "POINT (30 10)")
	geog := wktToWKB(t, "POINT (12 4)")

	// The records below feed geoarrow's JSON parser the lowercase-hex WKB from
	// WKBBytes.String() and rely on it round-tripping back into WKB; if that ever
	// stopped, the accumulator would see no values and the geometry bound
	// assertions below would fail confusingly. parquet_files_test.go leans on the
	// same .String() hex pattern.
	rec, _, err := array.RecordFromJSON(memory.DefaultAllocator, arrowSchema, strings.NewReader(`[
		{"id": 1, "geom": "`+geomLo.String()+`", "geog": "`+geog.String()+`"},
		{"id": 2, "geom": "`+geomHi.String()+`", "geog": null}
	]`))
	require.NoError(t, err)

	df, err := writer.writeFile(t.Context(), nil, WriteTask{
		Uuid:      uuid.New(),
		ID:        0,
		FileCount: 1,
		Schema:    schema,
		Batches:   []arrow.RecordBatch{rec},
	})
	require.NoError(t, err)
	require.EqualValues(t, 2, df.Count())

	// Geometry column carries a planar XY bounding box in the manifest bounds.
	lower := df.LowerBoundValues()
	upper := df.UpperBoundValues()
	require.Contains(t, lower, geoTestGeomFieldID, "geometry column must record a lower bound")
	require.Contains(t, upper, geoTestGeomFieldID, "geometry column must record an upper bound")

	minX, minY, maxX, maxY, ok := tblutils.GeoBoundsXY(lower[geoTestGeomFieldID], upper[geoTestGeomFieldID])
	require.True(t, ok, "geometry bounds must decode to a planar XY box")
	assert.Equal(t, 0.0, minX)
	assert.Equal(t, -5.0, minY)
	assert.Equal(t, 30.0, maxX)
	assert.Equal(t, 10.0, maxY)

	// The non-geo id column still gets ordinary min/max bounds. The geo-type guard
	// in DataFileStatsFromMeta suppresses generic Parquet stats for geo columns;
	// pinning id here catches a regression that over-suppresses an adjacent
	// non-geo column.
	require.Contains(t, lower, geoTestIDFieldID, "non-geo id column must still record a lower bound")
	require.Contains(t, upper, geoTestIDFieldID, "non-geo id column must still record an upper bound")

	// Geography does not record bounds in the current implementation: the V3 spec
	// permits geography bounds (with the xmin > xmax antimeridian-wrapping
	// convention), but iceberg-go leaves them unbounded as a deliberate
	// conservative choice until geodesic/antimeridian-aware computation lands (see
	// geoBoundsAccumulator). This assertion should flip when that computation is
	// added.
	//
	// Note for whoever adds it: geography needs its own decode, not a reuse of
	// GeoBoundsXY. That helper rejects xmin > xmax as inverted (correct for planar
	// geometry), which would silently drop the valid antimeridian-crossing bounds
	// Java/PyIceberg emit for geography.
	assert.NotContains(t, lower, geoTestGeogFieldID, "geography column does not record bounds in the current implementation")
	assert.NotContains(t, upper, geoTestGeogFieldID, "geography column does not record bounds in the current implementation")
}

// TestWriteGeometryColumnAllNull writes a geometry column whose values are all
// null and asserts the field drops out of the manifest bounds map through the
// full write path. With no WKB values to accumulate, geoBoundsAccumulator has
// nothing to encode, so no bound must be emitted.
func TestWriteGeometryColumnAllNull(t *testing.T) {
	t.Parallel()

	writer, schema, arrowSchema := newGeoTestWriter(t, t.TempDir(), iceberg.Properties{})

	rec, _, err := array.RecordFromJSON(memory.DefaultAllocator, arrowSchema, strings.NewReader(`[
		{"id": 1, "geom": null, "geog": null},
		{"id": 2, "geom": null, "geog": null}
	]`))
	require.NoError(t, err)

	df, err := writer.writeFile(t.Context(), nil, WriteTask{
		Uuid:      uuid.New(),
		ID:        0,
		FileCount: 1,
		Schema:    schema,
		Batches:   []arrow.RecordBatch{rec},
	})
	require.NoError(t, err)
	require.EqualValues(t, 2, df.Count())

	lower := df.LowerBoundValues()
	upper := df.UpperBoundValues()
	// This asserts absence, which also holds if the accumulator were never wired
	// up at all (e.g. collectGeoColumns stopped registering geo columns).
	// TestWriteGeometryColumnPopulatesBounds is the positive guard against that
	// bypass; this test pins the all-null case on top of it.
	assert.NotContains(t, lower, geoTestGeomFieldID, "all-null geometry column must not record a lower bound")
	assert.NotContains(t, upper, geoTestGeomFieldID, "all-null geometry column must not record an upper bound")

	// The non-geo id column is unaffected by the geometry column being all null.
	require.Contains(t, lower, geoTestIDFieldID, "non-geo id column must still record a lower bound")
	require.Contains(t, upper, geoTestIDFieldID, "non-geo id column must still record an upper bound")
}

// TestWriteGeometryColumnMetricsNone sets write.metadata.metrics.column.geom=none
// and asserts computeStatsPlan actually suppresses the geometry bounds. This
// exercises the stats-plan dispatch (arrowStatsCollector -> applyGeoBounds) that
// internal.TestWriteDataFileGeoBounds cannot reach, since that test feeds
// hand-built StatsCols straight to WriteDataFile. The adjacent non-geo id column
// keeps its ordinary bounds so the suppression is scoped to the geometry column.
func TestWriteGeometryColumnMetricsNone(t *testing.T) {
	t.Parallel()

	props := iceberg.Properties{
		MetricsModeColumnConfPrefix + ".geom": string(tblutils.MetricModeNone),
	}
	writer, schema, arrowSchema := newGeoTestWriter(t, t.TempDir(), props)

	geomLo := wktToWKB(t, "POINT (0 -5)")
	geomHi := wktToWKB(t, "POINT (30 10)")

	rec, _, err := array.RecordFromJSON(memory.DefaultAllocator, arrowSchema, strings.NewReader(`[
		{"id": 1, "geom": "`+geomLo.String()+`", "geog": null},
		{"id": 2, "geom": "`+geomHi.String()+`", "geog": null}
	]`))
	require.NoError(t, err)

	df, err := writer.writeFile(t.Context(), nil, WriteTask{
		Uuid:      uuid.New(),
		ID:        0,
		FileCount: 1,
		Schema:    schema,
		Batches:   []arrow.RecordBatch{rec},
	})
	require.NoError(t, err)
	require.EqualValues(t, 2, df.Count())

	lower := df.LowerBoundValues()
	upper := df.UpperBoundValues()
	assert.NotContains(t, lower, geoTestGeomFieldID, "geometry column bounds must be suppressed under metrics mode none")
	assert.NotContains(t, upper, geoTestGeomFieldID, "geometry column bounds must be suppressed under metrics mode none")

	// Suppression is scoped to the geom column: id still gets ordinary bounds.
	require.Contains(t, lower, geoTestIDFieldID, "non-geo id column must keep bounds when only geom is set to none")
	require.Contains(t, upper, geoTestIDFieldID, "non-geo id column must keep bounds when only geom is set to none")
}

// TestWriteGeometryColumnCheckedAllocator runs the geometry write path on a
// checked allocator and asserts zero residual once the writer is done. The
// geo-bounds accumulator works on plain Go fields and returns []byte, so the
// checked allocator cannot see it; what this pins is that the Arrow write
// pipeline (ToRequestedSchema, the pqarrow writer) releases every buffer it
// allocates on the geo write path.
func TestWriteGeometryColumnCheckedAllocator(t *testing.T) {
	t.Parallel()

	mem := memory.NewCheckedAllocator(memory.DefaultAllocator)
	defer mem.AssertSize(t, 0)
	ctx := compute.WithAllocator(t.Context(), mem)

	writer, schema, arrowSchema := newGeoTestWriter(t, t.TempDir(), iceberg.Properties{})

	geomLo := wktToWKB(t, "POINT (0 -5)")
	geomHi := wktToWKB(t, "POINT (30 10)")
	geog := wktToWKB(t, "POINT (12 4)")

	rec, _, err := array.RecordFromJSON(mem, arrowSchema, strings.NewReader(`[
		{"id": 1, "geom": "`+geomLo.String()+`", "geog": "`+geog.String()+`"},
		{"id": 2, "geom": "`+geomHi.String()+`", "geog": null}
	]`))
	require.NoError(t, err)

	df, err := writer.writeFile(ctx, nil, WriteTask{
		Uuid:      uuid.New(),
		ID:        0,
		FileCount: 1,
		Schema:    schema,
		Batches:   []arrow.RecordBatch{rec},
	})
	require.NoError(t, err)
	require.EqualValues(t, 2, df.Count())
	require.Contains(t, df.LowerBoundValues(), geoTestGeomFieldID, "geometry column must record a lower bound")
	require.Contains(t, df.UpperBoundValues(), geoTestGeomFieldID, "geometry column must record an upper bound")
}
