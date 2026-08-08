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
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/iceberg-go"
	"github.com/apache/iceberg-go/io"
	tblutils "github.com/apache/iceberg-go/table/internal"
	"github.com/geoarrow/geoarrow-go"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/twpayne/go-geom/encoding/wkb"
	"github.com/twpayne/go-geom/encoding/wkt"
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

	const (
		geomFieldID = 2
		geogFieldID = 3
	)

	geogType, err := iceberg.GeographyTypeOf("OGC:CRS84", "spherical")
	require.NoError(t, err)

	schema := iceberg.NewSchema(0,
		iceberg.NestedField{ID: 1, Name: "id", Type: iceberg.PrimitiveTypes.Int32, Required: false},
		iceberg.NestedField{ID: geomFieldID, Name: "geom", Type: iceberg.GeometryType{}, Required: false},
		iceberg.NestedField{ID: geogFieldID, Name: "geog", Type: geogType, Required: false},
	)

	// Geometry/geography are v3 types.
	mb, err := NewMetadataBuilder(3)
	require.NoError(t, err)
	require.NoError(t, mb.AddSchema(schema))
	require.NoError(t, mb.SetCurrentSchemaID(0))
	unpartitioned := *iceberg.UnpartitionedSpec
	require.NoError(t, mb.AddPartitionSpec(&unpartitioned, true))
	require.NoError(t, mb.SetDefaultSpecID(0))

	arrowSchema, err := SchemaToArrowSchema(schema, nil, true, false)
	require.NoError(t, err)

	// Two geometry points spanning X in [0, 30] and Y in [-5, 10].
	geomLo := wktToWKB(t, "POINT (0 -5)")
	geomHi := wktToWKB(t, "POINT (30 10)")
	geog := wktToWKB(t, "POINT (12 4)")

	rec, _, err := array.RecordFromJSON(memory.DefaultAllocator, arrowSchema, strings.NewReader(`[
		{"id": 1, "geom": "`+geomLo.String()+`", "geog": "`+geog.String()+`"},
		{"id": 2, "geom": "`+geomHi.String()+`", "geog": null}
	]`))
	require.NoError(t, err)
	defer rec.Release()

	writer, err := newDataFileWriter(t.TempDir(), &io.LocalFS{}, mb, iceberg.Properties{})
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
	require.Contains(t, lower, geomFieldID, "geometry column must record a lower bound")
	require.Contains(t, upper, geomFieldID, "geometry column must record an upper bound")

	minX, minY, maxX, maxY, ok := tblutils.GeoBoundsXY(lower[geomFieldID], upper[geomFieldID])
	require.True(t, ok, "geometry bounds must decode to a planar XY box")
	assert.Equal(t, 0.0, minX)
	assert.Equal(t, -5.0, minY)
	assert.Equal(t, 30.0, maxX)
	assert.Equal(t, 10.0, maxY)

	// Geography stays unbounded: a planar box over geodesic edges is unsafe.
	assert.NotContains(t, lower, geogFieldID, "geography column must not record bounds")
	assert.NotContains(t, upper, geogFieldID, "geography column must not record bounds")
}
