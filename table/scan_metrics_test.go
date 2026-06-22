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
	"testing"
	"time"

	"github.com/apache/iceberg-go"
	"github.com/apache/iceberg-go/metrics"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func metricsTestMetadata(t *testing.T) Metadata {
	t.Helper()
	schema := iceberg.NewSchema(7,
		iceberg.NestedField{ID: 1, Name: "id", Type: iceberg.PrimitiveTypes.Int64, Required: true},
		iceberg.NestedField{ID: 2, Name: "data", Type: iceberg.PrimitiveTypes.String},
	)
	meta, err := NewMetadata(schema, iceberg.UnpartitionedSpec, UnsortedSortOrder,
		"mem://bucket/tbl", iceberg.Properties{})
	require.NoError(t, err)

	return meta
}

func TestBuildScanReport(t *testing.T) {
	meta := metricsTestMetadata(t)
	scan := &Scan{
		metadata:       meta,
		identifier:     Identifier{"db", "tbl"},
		selectedFields: []string{"*"},
		caseSensitive:  true,
	}

	acc := &scanMetricsAccumulator{
		totalDataManifests:    3,
		scannedDataManifests:  2,
		skippedDataManifests:  1,
		totalDeleteManifests:  1,
		resultDataFiles:       10,
		totalFileSize:         2048,
		positionalDeleteFiles: 1,
		equalityDeleteFiles:   2,
		dvs:                   1,
		resultDeleteFiles:     4,
		totalDeleteFileSize:   512,
	}

	sr := scan.buildScanReport(acc, 5*time.Millisecond)

	assert.Equal(t, "db.tbl", sr.TableName)
	assert.Equal(t, meta.CurrentSchema().ID, sr.SchemaID)
	assert.Equal(t, []int{1, 2}, sr.ProjectedFieldIDs)
	assert.Equal(t, []string{"id", "data"}, sr.ProjectedFieldNames)

	m := sr.Metrics
	require.NotNil(t, m.TotalPlanningDuration)
	assert.Equal(t, metrics.TimeUnitNanoseconds, m.TotalPlanningDuration.TimeUnit)
	assert.Equal(t, (5 * time.Millisecond).Nanoseconds(), m.TotalPlanningDuration.TotalDuration)
	assert.Equal(t, int64(1), m.TotalPlanningDuration.Count)

	require.NotNil(t, m.TotalDataManifests)
	assert.Equal(t, metrics.UnitCount, m.TotalDataManifests.Unit)
	assert.Equal(t, int64(3), m.TotalDataManifests.Value)
	assert.Equal(t, int64(2), m.ScannedDataManifests.Value)
	assert.Equal(t, int64(1), m.SkippedDataManifests.Value)
	assert.Equal(t, int64(10), m.ResultDataFiles.Value)
	assert.Equal(t, int64(4), m.ResultDeleteFiles.Value)
	assert.Equal(t, int64(1), m.PositionalDeleteFiles.Value)
	assert.Equal(t, int64(2), m.EqualityDeleteFiles.Value)
	assert.Equal(t, int64(1), m.DVs.Value)

	require.NotNil(t, m.TotalFileSizeInBytes)
	assert.Equal(t, metrics.UnitBytes, m.TotalFileSizeInBytes.Unit)
	assert.Equal(t, int64(2048), m.TotalFileSizeInBytes.Value)
	assert.Equal(t, int64(512), m.TotalDeleteFileSizeInBytes.Value)

	// Unmeasured metrics are left unset (omitted on the wire).
	assert.Nil(t, m.SkippedDataFiles)
	assert.Nil(t, m.IndexedDeleteFiles)
}

func TestProjectedFieldsSelectedSubset(t *testing.T) {
	meta := metricsTestMetadata(t)
	scan := &Scan{metadata: meta, selectedFields: []string{"data"}, caseSensitive: true}

	ids, names := scan.projectedFields(meta.CurrentSchema())
	assert.Equal(t, []int{2}, ids)
	assert.Equal(t, []string{"data"}, names)
}

func TestApplyResultDeleteMetrics(t *testing.T) {
	posA := &mockDataFile{path: "s3://b/pos-a.parquet", filesize: 100}
	posB := &mockDataFile{path: "s3://b/pos-b.parquet", filesize: 200}
	eq := &mockDataFile{path: "s3://b/eq.parquet", filesize: 40}
	dv := &mockDataFile{path: "s3://b/dv.puffin", filesize: 10}

	// posA applies to both data files: it must be counted (and sized) once.
	tasks := []FileScanTask{
		{
			File:        &mockDataFile{path: "s3://b/data-1.parquet", filesize: 1000},
			DeleteFiles: []iceberg.DataFile{posA, posB},
		},
		{
			File:                &mockDataFile{path: "s3://b/data-2.parquet", filesize: 2000},
			DeleteFiles:         []iceberg.DataFile{posA},
			EqualityDeleteFiles: []iceberg.DataFile{eq},
			DeletionVectorFiles: []iceberg.DataFile{dv},
		},
	}

	var acc scanMetricsAccumulator
	acc.applyResultDeleteMetrics(tasks)

	assert.Equal(t, int64(2), acc.positionalDeleteFiles, "posA is shared, counted once")
	assert.Equal(t, int64(1), acc.equalityDeleteFiles)
	assert.Equal(t, int64(1), acc.dvs)
	assert.Equal(t, int64(4), acc.resultDeleteFiles)
	// 100 (posA once) + 200 (posB) + 40 (eq) + 10 (dv)
	assert.Equal(t, int64(350), acc.totalDeleteFileSize)
}

func TestPlanFilesEmitsScanReport(t *testing.T) {
	// A table with no snapshot still completes planning (zero results) and must
	// emit exactly one ScanReport through the configured reporter.
	rep := &metrics.InMemoryReporter{}
	meta := metricsTestMetadata(t)
	tbl := New(Identifier{"db", "tbl"}, meta, "metadata.json", nil, nil,
		WithMetricsReporter(rep))

	tasks, err := tbl.Scan().PlanFiles(context.Background())
	require.NoError(t, err)
	require.Empty(t, tasks)

	reports := rep.Reports()
	require.Len(t, reports, 1)

	sr, ok := reports[0].(metrics.ScanReport)
	require.True(t, ok, "expected a ScanReport, got %T", reports[0])
	assert.Equal(t, "db.tbl", sr.TableName)
	assert.Equal(t, meta.CurrentSchema().ID, sr.SchemaID)
	require.NotNil(t, sr.Metrics.TotalPlanningDuration)
	require.NotNil(t, sr.Metrics.ResultDataFiles)
	assert.Equal(t, int64(0), sr.Metrics.ResultDataFiles.Value)
}

func TestPlanFilesNopReporterDoesNotPanic(t *testing.T) {
	// Default (no reporter configured) must plan without panicking.
	tbl := New(Identifier{"db", "tbl"}, metricsTestMetadata(t), "metadata.json", nil, nil)
	assert.NotPanics(t, func() {
		_, _ = tbl.Scan().PlanFiles(context.Background())
	})
}

func TestPlanFilesRemoteDoesNotEmitScanReport(t *testing.T) {
	// Remote (server-side) planning reports its own metrics; the client must not
	// emit a local ScanReport (which would carry only zeroed counters).
	rep := &metrics.InMemoryReporter{}
	scan := &Scan{
		planner:      &fakeScanPlanner{result: ScanPlanningResult{Tasks: []FileScanTask{{}}}, supports: true},
		planningMode: ScanPlanningRemote,
		reporter:     rep,
	}

	tasks, err := scan.PlanFiles(context.Background())
	require.NoError(t, err)
	require.Len(t, tasks, 1)
	assert.Empty(t, rep.Reports(), "remote planning must not emit a local ScanReport")
}
