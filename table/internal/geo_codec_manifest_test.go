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

package internal_test

import (
	"bytes"
	"encoding/binary"
	"math"
	"testing"

	iceberg "github.com/apache/iceberg-go"
	"github.com/apache/iceberg-go/table/internal"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// encGeoXY encodes an XY bound point in the Iceberg geospatial single-value
// serialization (little-endian float64 X, Y) without reaching into the internal
// encoder, so this stays a black-box test of the manifest round-trip.
func encGeoXY(x, y float64) []byte {
	buf := make([]byte, 16)
	binary.LittleEndian.PutUint64(buf[0:], math.Float64bits(x))
	binary.LittleEndian.PutUint64(buf[8:], math.Float64bits(y))

	return buf
}

func decGeoXY(b []byte) (x, y float64) {
	return math.Float64frombits(binary.LittleEndian.Uint64(b[0:])),
		math.Float64frombits(binary.LittleEndian.Uint64(b[8:]))
}

// TestManifestGeoBoundsRoundTripAndAggregate writes a manifest holding several
// data files whose geometry column carries per-file bounds, reads it back, and
// checks two things #993 requires: each file's bound round-trips and decodes as
// an (unorderable) GeoLiteral rather than a plain binary literal, and the
// GeoBoundsAggregator combines the per-file boxes across files into the correct
// overall bounding box.
func TestManifestGeoBoundsRoundTripAndAggregate(t *testing.T) {
	const geoFieldID = 1
	geoType := iceberg.GeometryType{}
	schema := iceberg.NewSchema(0, iceberg.NestedField{
		ID: geoFieldID, Name: "geom", Type: geoType, Required: false,
	})
	spec := iceberg.NewPartitionSpec() // geometry cannot be a partition source

	type fileBounds struct{ loX, loY, hiX, hiY float64 }
	files := []fileBounds{
		{loX: 5, loY: 5, hiX: 10, hiY: 20},
		{loX: 1, loY: 8, hiX: 30, hiY: 12},
		{loX: -4, loY: 0, hiX: 3, hiY: 40},
	}

	snapshotID := int64(42)
	entries := make([]iceberg.ManifestEntry, 0, len(files))
	for i, fb := range files {
		df, err := iceberg.NewDataFileBuilder(
			spec, iceberg.EntryContentData,
			[]string{"/data/a.parquet", "/data/b.parquet", "/data/c.parquet"}[i],
			iceberg.ParquetFile, nil, nil, nil, 10, 1000,
		)
		require.NoError(t, err)
		df.LowerBoundValues(map[int][]byte{geoFieldID: encGeoXY(fb.loX, fb.loY)})
		df.UpperBoundValues(map[int][]byte{geoFieldID: encGeoXY(fb.hiX, fb.hiY)})
		entries = append(entries, iceberg.NewManifestEntry(
			iceberg.EntryStatusADDED, &snapshotID, nil, nil, df.Build()))
	}

	var buf bytes.Buffer
	mf, err := iceberg.WriteManifest("/geo-manifest.avro", &buf, 2, spec, schema, snapshotID, entries)
	require.NoError(t, err)

	read, err := iceberg.ReadManifest(mf, bytes.NewReader(buf.Bytes()), false)
	require.NoError(t, err)
	require.Len(t, read, len(files))

	var agg internal.GeoBoundsAggregator
	for i, entry := range read {
		lb := entry.DataFile().LowerBoundValues()[geoFieldID]
		ub := entry.DataFile().UpperBoundValues()[geoFieldID]
		require.Len(t, lb, 16, "file %d lower bound must round-trip", i)
		require.Len(t, ub, 16, "file %d upper bound must round-trip", i)

		// The bound must decode as a GeoLiteral, not a comparable BinaryLiteral,
		// so nothing downstream silently orders coordinate bytes.
		lit, err := iceberg.LiteralFromBytes(geoType, lb)
		require.NoError(t, err)
		_, ok := lit.(iceberg.GeoLiteral)
		assert.True(t, ok, "geo bound must decode as GeoLiteral, got %T", lit)

		require.NoError(t, agg.Add(lb, ub))
	}

	lower, upper := agg.Bounds()
	require.Len(t, lower, 16)
	require.Len(t, upper, 16)

	loX, loY := decGeoXY(lower)
	hiX, hiY := decGeoXY(upper)
	assert.Equal(t, [2]float64{-4, 0}, [2]float64{loX, loY}, "aggregate lower is per-dimension min")
	assert.Equal(t, [2]float64{30, 40}, [2]float64{hiX, hiY}, "aggregate upper is per-dimension max")
}
