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
	"bytes"
	"context"
	"encoding/binary"
	"math"
	"testing"

	"github.com/DataDog/iceberg-go"
	iceio "github.com/DataDog/iceberg-go/io"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// geoBound2D encodes a single bound point in the Iceberg geospatial single-value
// serialization: little-endian float64 X then Y (16 bytes).
func geoBound2D(x, y float64) []byte {
	b := make([]byte, 16)
	binary.LittleEndian.PutUint64(b[0:], math.Float64bits(x))
	binary.LittleEndian.PutUint64(b[8:], math.Float64bits(y))

	return b
}

var geoMetricsSchema = iceberg.NewSchema(0,
	iceberg.NestedField{ID: 1, Name: "geom", Type: iceberg.GeometryType{}, Required: false},
	iceberg.NestedField{ID: 2, Name: "geog", Type: iceberg.GeographyType{}, Required: false},
)

// TestInclusiveMetricsBBoxIntersects verifies that a data file is dropped only
// when its geometry bounds cannot intersect the query bbox.
func TestInclusiveMetricsBBoxIntersects(t *testing.T) {
	// A file whose geometry column spans the box [0,0]-[10,10].
	lower, upper := geoBound2D(0, 0), geoBound2D(10, 10)
	file := &mockDataFile{
		count:       2,
		valueCounts: map[int]int64{1: 2},
		nullCounts:  map[int]int64{1: 0},
		lowerBounds: map[int][]byte{1: lower},
		upperBounds: map[int][]byte{1: upper},
	}

	tests := []struct {
		name string
		bbox iceberg.BoundingBox
		want bool // true => might match (kept), false => pruned
	}{
		{"overlapping", iceberg.BoundingBox{MinX: 5, MinY: 5, MaxX: 15, MaxY: 15}, true},
		{"contained", iceberg.BoundingBox{MinX: 2, MinY: 2, MaxX: 3, MaxY: 3}, true},
		{"touching corner", iceberg.BoundingBox{MinX: 10, MinY: 10, MaxX: 20, MaxY: 20}, true},
		{"disjoint right", iceberg.BoundingBox{MinX: 11, MinY: 0, MaxX: 20, MaxY: 10}, false},
		{"disjoint diagonal", iceberg.BoundingBox{MinX: 20, MinY: 20, MaxX: 30, MaxY: 30}, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			eval, err := newInclusiveMetricsEvaluator(
				geoMetricsSchema, iceberg.BBoxIntersects(iceberg.Reference("geom"), tt.bbox), true, true)
			require.NoError(t, err)

			got, err := eval(file)
			require.NoError(t, err)
			assert.Equal(t, tt.want, got)
		})
	}
}

// TestInclusiveMetricsBBoxIntersectsAllNull prunes a file whose geometry column
// is entirely null: no geometry can intersect any query box.
func TestInclusiveMetricsBBoxIntersectsAllNull(t *testing.T) {
	file := &mockDataFile{
		count:       3,
		valueCounts: map[int]int64{1: 3},
		nullCounts:  map[int]int64{1: 3},
	}

	eval, err := newInclusiveMetricsEvaluator(
		geoMetricsSchema,
		iceberg.BBoxIntersects(iceberg.Reference("geom"), iceberg.BoundingBox{MinX: 0, MinY: 0, MaxX: 10, MaxY: 10}),
		true, true)
	require.NoError(t, err)

	got, err := eval(file)
	require.NoError(t, err)
	assert.False(t, got, "all-null geometry column must be pruned")
}

// TestInclusiveMetricsBBoxIntersectsNoBounds keeps a file when the geometry
// column has no usable bounds. Geography columns never emit bounds, so a
// geography predicate can never prune - which is always safe.
func TestInclusiveMetricsBBoxIntersectsNoBounds(t *testing.T) {
	file := &mockDataFile{
		count:       2,
		valueCounts: map[int]int64{2: 2},
		nullCounts:  map[int]int64{2: 0},
	}

	eval, err := newInclusiveMetricsEvaluator(
		geoMetricsSchema,
		iceberg.BBoxIntersects(iceberg.Reference("geog"), iceberg.BoundingBox{MinX: 100, MinY: 100, MaxX: 200, MaxY: 200}),
		true, true)
	require.NoError(t, err)

	got, err := eval(file)
	require.NoError(t, err)
	assert.True(t, got, "geography column has no bounds, so the file cannot be pruned")
}

// TestInclusiveMetricsBBoxIntersectsGeographyWithBounds guards the antimeridian
// hazard: a geography file written by another engine can carry bounds, and those
// bounds may cross the antimeridian (lower_x > upper_x). A planar min/max compare
// would mis-handle the wrapped box and wrongly prune, so geography columns must
// never be pruned from bounds - even when the bounds look disjoint from the query.
func TestInclusiveMetricsBBoxIntersectsGeographyWithBounds(t *testing.T) {
	file := &mockDataFile{
		count:       2,
		valueCounts: map[int]int64{2: 2},
		nullCounts:  map[int]int64{2: 0},
		// Bounds that look disjoint from the query box under a planar compare.
		lowerBounds: map[int][]byte{2: geoBound2D(0, 0)},
		upperBounds: map[int][]byte{2: geoBound2D(10, 10)},
	}

	eval, err := newInclusiveMetricsEvaluator(
		geoMetricsSchema,
		iceberg.BBoxIntersects(iceberg.Reference("geog"), iceberg.BoundingBox{MinX: 100, MinY: 100, MaxX: 200, MaxY: 200}),
		true, true)
	require.NoError(t, err)

	got, err := eval(file)
	require.NoError(t, err)
	assert.True(t, got, "geography must not be pruned from planar bounds (antimeridian hazard)")
}

// TestInclusiveMetricsBBoxNotIntersects never prunes: intersecting bounds don't
// prove any geometry lies outside the query box.
func TestInclusiveMetricsBBoxNotIntersects(t *testing.T) {
	file := &mockDataFile{
		count:       2,
		valueCounts: map[int]int64{1: 2},
		nullCounts:  map[int]int64{1: 0},
		lowerBounds: map[int][]byte{1: geoBound2D(0, 0)},
		upperBounds: map[int][]byte{1: geoBound2D(10, 10)},
	}

	pred := iceberg.BBoxIntersects(iceberg.Reference("geom"),
		iceberg.BoundingBox{MinX: 20, MinY: 20, MaxX: 30, MaxY: 30}).Negate()
	eval, err := newInclusiveMetricsEvaluator(geoMetricsSchema, pred, true, true)
	require.NoError(t, err)

	got, err := eval(file)
	require.NoError(t, err)
	assert.True(t, got, "not-intersects cannot prune from bounds alone")
}

// TestScanPrunesDisjointGeometryFile drives a bbox filter through a real
// table.Scan - binding, projection, and the manifest->metrics pruning path -
// rather than calling the metrics evaluator directly. It proves the
// metrics-pruning path end-to-end: a data file whose geometry bounds are
// disjoint from the query box is pruned, while an overlapping one survives.
//
// PlanFiles does not run ReadTasks/arrowScan, so this does not exercise
// column-name translation or the substrait conversion; those regressions (a
// bbox predicate dropped to AlwaysFalse during translation, or panicking in
// substrait) are pinned by TestBBoxTranslateColumnNames and
// TestBBoxPredicateConvertsToTypedError, not here.
func TestScanPrunesDisjointGeometryFile(t *testing.T) {
	ctx := context.Background()
	spec := iceberg.NewPartitionSpec() // geometry cannot be a partition source
	memIO := iceio.NewMemFS()

	const geoFieldID = 1
	schema := iceberg.NewSchema(0,
		iceberg.NestedField{ID: geoFieldID, Name: "geom", Type: iceberg.GeometryType{}, Required: false},
	)

	meta, err := NewMetadata(schema, &spec, UnsortedSortOrder, "mem://default/table",
		iceberg.Properties{PropertyFormatVersion: "3"}) // geometry requires v3
	require.NoError(t, err)

	builder, err := MetadataBuilderFromBase(meta, "")
	require.NoError(t, err)

	snapshotID := int64(1)
	newGeoEntry := func(path string, lower, upper []byte) iceberg.ManifestEntry {
		df, err := iceberg.NewDataFileBuilder(spec, iceberg.EntryContentData, path,
			iceberg.ParquetFile, nil, nil, nil, 2, 1024)
		require.NoError(t, err)

		return iceberg.NewManifestEntry(iceberg.EntryStatusADDED, &snapshotID, nil, nil,
			df.LowerBoundValues(map[int][]byte{geoFieldID: lower}).
				UpperBoundValues(map[int][]byte{geoFieldID: upper}).
				Build())
	}

	const overlappingPath = "mem://default/table/data/overlapping.parquet"
	// File [0,0]-[10,10] overlaps the query box; file [20,20]-[30,30] is disjoint.
	entries := []iceberg.ManifestEntry{
		newGeoEntry(overlappingPath, geoBound2D(0, 0), geoBound2D(10, 10)),
		newGeoEntry("mem://default/table/data/disjoint.parquet", geoBound2D(20, 20), geoBound2D(30, 30)),
	}

	manifestPath := "mem://default/table/metadata/manifest.avro"
	var manifestBuf bytes.Buffer
	manifest, err := iceberg.WriteManifest(manifestPath, &manifestBuf, 3, spec, schema, snapshotID, entries)
	require.NoError(t, err)
	require.NoError(t, memIO.WriteFile(manifestPath, manifestBuf.Bytes()))

	manifestListPath := "mem://default/table/metadata/snap-1-manifest-list.avro"
	var listBuf bytes.Buffer
	seqNum := int64(1)
	require.NoError(t, iceberg.WriteManifestList(3, &listBuf, snapshotID, nil, &seqNum, 0,
		[]iceberg.ManifestFile{manifest}))
	require.NoError(t, memIO.WriteFile(manifestListPath, listBuf.Bytes()))

	firstRowID, addedRows := int64(0), int64(4)
	require.NoError(t, builder.AddSnapshot(&Snapshot{
		SnapshotID:     snapshotID,
		SequenceNumber: seqNum,
		TimestampMs:    meta.LastUpdatedMillis() + 1,
		ManifestList:   manifestListPath,
		Summary:        &Summary{Operation: OpAppend},
		FirstRowID:     &firstRowID, // v3 row-lineage bookkeeping
		AddedRows:      &addedRows,
	}))
	require.NoError(t, builder.SetSnapshotRef(MainBranch, snapshotID, BranchRef))

	built, err := builder.Build()
	require.NoError(t, err)

	tbl := New(Identifier{"db", "tbl"}, built, "metadata.json",
		func(context.Context) (iceio.IO, error) { return memIO, nil }, nil)

	// Query box [5,5]-[15,15]: overlaps the first file, disjoint from the second.
	scan := tbl.Scan(WithRowFilter(iceberg.BBoxIntersects(iceberg.Reference("geom"),
		iceberg.BoundingBox{MinX: 5, MinY: 5, MaxX: 15, MaxY: 15})))

	tasks, err := scan.PlanFiles(ctx)
	require.NoError(t, err)
	require.Len(t, tasks, 1, "only the overlapping geometry file survives pruning")
	assert.Equal(t, overlappingPath, tasks[0].File.FilePath())
}
