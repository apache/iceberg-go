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
	"fmt"
	"math"
	"testing"

	"github.com/apache/iceberg-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func rowCountManifest(path string, addedRows, existingRows int64) iceberg.ManifestFile {
	return iceberg.NewManifestFile(3, path, 1, 0, 1).
		AddedRows(addedRows).
		ExistingRows(existingRows).
		Build()
}

func TestLimitManifestListByRows(t *testing.T) {
	tests := []struct {
		name        string
		manifests   []iceberg.ManifestFile
		limit       int64
		wantPaths   []string
		wantLimited bool
	}{
		{
			name: "first manifest reaches limit",
			manifests: []iceberg.ManifestFile{
				rowCountManifest("manifest-1", 8, 4),
				rowCountManifest("manifest-2", 2, 0),
			},
			limit:       10,
			wantPaths:   []string{"manifest-1"},
			wantLimited: true,
		},
		{
			name: "cumulative counts reach limit",
			manifests: []iceberg.ManifestFile{
				rowCountManifest("manifest-1", 2, 1),
				rowCountManifest("manifest-2", 3, 2),
				rowCountManifest("manifest-3", 4, 0),
			},
			limit:       8,
			wantPaths:   []string{"manifest-1", "manifest-2"},
			wantLimited: true,
		},
		{
			name: "zero row manifests are retained in prefix",
			manifests: []iceberg.ManifestFile{
				rowCountManifest("manifest-1", 0, 0),
				rowCountManifest("manifest-2", 5, 0),
				rowCountManifest("manifest-3", 1, 0),
			},
			limit:       5,
			wantPaths:   []string{"manifest-1", "manifest-2"},
			wantLimited: true,
		},
		{
			name: "single manifest does not narrow",
			manifests: []iceberg.ManifestFile{
				rowCountManifest("manifest-1", 5, 0),
			},
			limit:       1,
			wantPaths:   []string{"manifest-1"},
			wantLimited: false,
		},
		{
			name: "unknown count after prefix does not block limit",
			manifests: []iceberg.ManifestFile{
				rowCountManifest("manifest-1", 5, 0),
				rowCountManifest("manifest-2", -1, 0),
			},
			limit:       3,
			wantPaths:   []string{"manifest-1"},
			wantLimited: true,
		},
		{
			name: "unknown count before prefix falls back",
			manifests: []iceberg.ManifestFile{
				rowCountManifest("manifest-1", -1, 0),
				rowCountManifest("manifest-2", 5, 0),
			},
			limit:       3,
			wantPaths:   []string{"manifest-1", "manifest-2"},
			wantLimited: false,
		},
		{
			name: "negative existing count falls back",
			manifests: []iceberg.ManifestFile{
				rowCountManifest("manifest-1", 5, -1),
				rowCountManifest("manifest-2", 5, 0),
			},
			limit:       3,
			wantPaths:   []string{"manifest-1", "manifest-2"},
			wantLimited: false,
		},
		{
			name: "row count overflow falls back",
			manifests: []iceberg.ManifestFile{
				rowCountManifest("manifest-1", math.MaxInt64, 1),
				rowCountManifest("manifest-2", 5, 0),
			},
			limit:       3,
			wantPaths:   []string{"manifest-1", "manifest-2"},
			wantLimited: false,
		},
		{
			name: "limit needs all manifests",
			manifests: []iceberg.ManifestFile{
				rowCountManifest("manifest-1", 2, 0),
				rowCountManifest("manifest-2", 3, 0),
			},
			limit:       5,
			wantPaths:   []string{"manifest-1", "manifest-2"},
			wantLimited: false,
		},
		{
			name: "total rows are below limit",
			manifests: []iceberg.ManifestFile{
				rowCountManifest("manifest-1", 2, 0),
				rowCountManifest("manifest-2", 1, 0),
			},
			limit:       5,
			wantPaths:   []string{"manifest-1", "manifest-2"},
			wantLimited: false,
		},
		{
			name: "zero and negative limits do not narrow",
			manifests: []iceberg.ManifestFile{
				rowCountManifest("manifest-1", 5, 0),
				rowCountManifest("manifest-2", 5, 0),
			},
			limit:       0,
			wantPaths:   []string{"manifest-1", "manifest-2"},
			wantLimited: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, limited := limitManifestListByRows(tt.manifests, tt.limit)
			require.Equal(t, tt.wantLimited, limited)
			require.Len(t, got, len(tt.wantPaths))
			for i, path := range tt.wantPaths {
				assert.Equal(t, path, got[i].FilePath())
			}
		})
	}

	negativeLimit := int64(-1)
	manifests := []iceberg.ManifestFile{rowCountManifest("manifest-1", 5, 0)}
	got, limited := limitManifestListByRows(manifests, negativeLimit)
	require.False(t, limited)
	require.Equal(t, manifests, got)
}

func TestCanLimitLocalPlanning(t *testing.T) {
	tests := []struct {
		name             string
		limit            int64
		rowFilter        iceberg.BooleanExpression
		totalDeleteFiles int64
		want             bool
	}{
		{name: "positive limit and no filter", limit: 10, rowFilter: iceberg.AlwaysTrue{}, want: true},
		{name: "nil filter", limit: 10, want: true},
		{name: "no limit", limit: ScanNoLimit, rowFilter: iceberg.AlwaysTrue{}, want: false},
		{name: "zero limit", limit: 0, rowFilter: iceberg.AlwaysTrue{}, want: false},
		{
			name:      "row filter",
			limit:     10,
			rowFilter: iceberg.EqualTo(iceberg.Reference("id"), int32(1)),
			want:      false,
		},
		{
			name:             "delete manifests",
			limit:            10,
			rowFilter:        iceberg.AlwaysTrue{},
			totalDeleteFiles: 1,
			want:             false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			scan := &Scan{limit: tt.limit, rowFilter: tt.rowFilter}
			acc := &scanMetricsAccumulator{totalDeleteManifests: tt.totalDeleteFiles}
			assert.Equal(t, tt.want, scan.canLimitLocalPlanning(acc))
		})
	}
}

func TestPlanFilesLocalUsesRowLimitToStopOpeningManifests(t *testing.T) {
	fs := newTrackingCallsIO()
	schema := simpleSchema()
	const tableLocation = "mem://row-limit"

	meta, err := NewMetadata(schema, iceberg.UnpartitionedSpec, UnsortedSortOrder, tableLocation,
		iceberg.Properties{PropertyFormatVersion: "2"})
	require.NoError(t, err)
	builder, err := MetadataBuilderFromBase(meta, "")
	require.NoError(t, err)

	const snapshotID = int64(1)
	manifestPaths := []string{
		tableLocation + "/metadata/manifest-1.avro",
		tableLocation + "/metadata/manifest-2.avro",
		tableLocation + "/metadata/manifest-3.avro",
	}
	manifests := make([]iceberg.ManifestFile, 0, len(manifestPaths))
	for i, manifestPath := range manifestPaths {
		dataPath := fmt.Sprintf("%s/data-%d.parquet", tableLocation, i+1)
		manifests = append(manifests, writeManifest(t, fs.trackingIO, snapshotID, int64(i+1), manifestPath, dataPath))
	}
	manifestListPath := tableLocation + "/metadata/snap-1.avro"
	writeManifestList(t, fs.trackingIO, snapshotID, manifestListPath, manifests)

	schemaID := meta.CurrentSchema().ID
	require.NoError(t, builder.AddSnapshot(&Snapshot{
		SnapshotID:     snapshotID,
		SequenceNumber: 1,
		TimestampMs:    meta.LastUpdatedMillis() + 1,
		ManifestList:   manifestListPath,
		Summary:        &Summary{Operation: OpAppend},
		SchemaID:       &schemaID,
	}))
	require.NoError(t, builder.SetSnapshotRef(MainBranch, snapshotID, BranchRef))
	built, err := builder.Build()
	require.NoError(t, err)

	tbl := New(Identifier{"db", "row-limit"}, built, tableLocation+"/metadata/metadata.json", testFSF(fs), nil)
	tasks, err := tbl.Scan(WithMaxConcurrency(1)).UseRowLimit(2).PlanFiles(context.Background())
	require.NoError(t, err)
	require.Len(t, tasks, 2)

	assert.Equal(t, 1, fs.openCount[manifestListPath])
	assert.Equal(t, 1, fs.openCount[manifestPaths[0]])
	assert.Equal(t, 1, fs.openCount[manifestPaths[1]])
	assert.Zero(t, fs.openCount[manifestPaths[2]])
}
