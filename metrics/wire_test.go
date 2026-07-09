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

package metrics

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"

	"github.com/apache/iceberg-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// remarshalEqual asserts that JSON in -> value -> JSON out is semantically
// identical (key order / whitespace independent).
func assertJSONEqual(t *testing.T, want, got []byte) {
	t.Helper()
	var w, g any
	require.NoError(t, json.Unmarshal(want, &w))
	require.NoError(t, json.Unmarshal(got, &g))
	assert.Equal(t, w, g)
}

func TestReportMetricsRequest_ScanInterop(t *testing.T) {
	fixture, err := os.ReadFile(filepath.Join("testdata", "scan_report.json"))
	require.NoError(t, err)

	var req ReportMetricsRequest
	require.NoError(t, json.Unmarshal(fixture, &req))

	sr, ok := req.Report.(ScanReport)
	require.True(t, ok, "expected ScanReport, got %T", req.Report)

	// Spot-check decoded fields.
	assert.Equal(t, "nyc.taxis", sr.TableName)
	assert.Equal(t, int64(3450729581828559851), sr.SnapshotID)
	assert.Equal(t, []int{1, 2, 3}, sr.ProjectedFieldIDs)
	assert.JSONEq(t, `true`, string(sr.Filter))
	require.NotNil(t, sr.Metrics.TotalPlanningDuration)
	assert.Equal(t, TimeUnitNanoseconds, sr.Metrics.TotalPlanningDuration.TimeUnit)
	assert.Equal(t, int64(2644235116), sr.Metrics.TotalPlanningDuration.TotalDuration)
	require.NotNil(t, sr.Metrics.TotalFileSizeInBytes)
	assert.Equal(t, UnitBytes, sr.Metrics.TotalFileSizeInBytes.Unit)
	assert.Equal(t, int64(10), sr.Metrics.TotalFileSizeInBytes.Value)
	assert.Equal(t, map[string]string{"engine": "go"}, sr.Metadata)

	// Round-trip must reproduce the fixture exactly (semantically).
	out, err := json.Marshal(req)
	require.NoError(t, err)
	assertJSONEqual(t, fixture, out)
}

func TestReportMetricsRequest_CommitInterop(t *testing.T) {
	fixture, err := os.ReadFile(filepath.Join("testdata", "commit_report.json"))
	require.NoError(t, err)

	var req ReportMetricsRequest
	require.NoError(t, json.Unmarshal(fixture, &req))

	cr, ok := req.Report.(CommitReport)
	require.True(t, ok, "expected CommitReport, got %T", req.Report)

	assert.Equal(t, "nyc.taxis", cr.TableName)
	assert.Equal(t, int64(2), cr.SequenceNumber)
	assert.Equal(t, "append", cr.Operation)
	require.NotNil(t, cr.Metrics.AddedRecords)
	assert.Equal(t, int64(12345), cr.Metrics.AddedRecords.Value)
	require.NotNil(t, cr.Metrics.AddedFilesSizeBytes)
	assert.Equal(t, UnitBytes, cr.Metrics.AddedFilesSizeBytes.Unit)
	require.NotNil(t, cr.Metrics.TotalDuration)
	assert.Equal(t, int64(1234567890), cr.Metrics.TotalDuration.TotalDuration)

	out, err := json.Marshal(req)
	require.NoError(t, err)
	assertJSONEqual(t, fixture, out)
}

func TestScanReport_DefaultsFilterToAlwaysTrue(t *testing.T) {
	// A ScanReport with no Filter must still emit the spec-required filter field.
	sr := ScanReport{TableName: "t", SnapshotID: 1}
	out, err := json.Marshal(sr)
	require.NoError(t, err)

	var decoded map[string]json.RawMessage
	require.NoError(t, json.Unmarshal(out, &decoded))
	require.Contains(t, decoded, "filter")
	assert.JSONEq(t, `true`, string(decoded["filter"]))
	// Empty projected lists serialize as [] (not null) to match the spec arrays.
	assert.JSONEq(t, `[]`, string(decoded["projected-field-ids"]))
	assert.JSONEq(t, `[]`, string(decoded["projected-field-names"]))
}

// TestScanReport_FilterMatchesCanonicalAlwaysTrue pins the default filter to
// iceberg's canonical always-true wire form (a bare JSON boolean) rather than a
// hand-written literal, so the two can't silently drift. A {"type":...} object
// or any other shape would be rejected as invalid Expression JSON by a
// spec-compliant catalog.
func TestScanReport_FilterMatchesCanonicalAlwaysTrue(t *testing.T) {
	want, err := json.Marshal(iceberg.AlwaysTrue{})
	require.NoError(t, err)

	out, err := json.Marshal(ScanReport{TableName: "t"})
	require.NoError(t, err)
	var decoded map[string]json.RawMessage
	require.NoError(t, json.Unmarshal(out, &decoded))
	assert.JSONEq(t, string(want), string(decoded["filter"]))
}

func TestReportMetricsRequest_MarshalDiscriminator(t *testing.T) {
	scanOut, err := json.Marshal(NewReportMetricsRequest(ScanReport{TableName: "t"}))
	require.NoError(t, err)
	var m map[string]json.RawMessage
	require.NoError(t, json.Unmarshal(scanOut, &m))
	assert.JSONEq(t, `"scan-report"`, string(m["report-type"]))
	assert.Contains(t, m, "table-name", "report fields are flattened, not nested")

	commitOut, err := json.Marshal(NewReportMetricsRequest(CommitReport{TableName: "t"}))
	require.NoError(t, err)
	require.NoError(t, json.Unmarshal(commitOut, &m))
	assert.JSONEq(t, `"commit-report"`, string(m["report-type"]))
}

func TestReportMetricsRequest_MarshalErrors(t *testing.T) {
	_, err := json.Marshal(NewReportMetricsRequest(nil))
	assert.Error(t, err, "nil report must not marshal")

	// A typed-nil pointer is a non-nil interface, so it must be rejected too
	// rather than silently marshaling to a report with no fields.
	_, err = json.Marshal(NewReportMetricsRequest((*ScanReport)(nil)))
	assert.Error(t, err, "typed-nil report must not marshal")
}

func TestReportMetricsRequest_UnmarshalErrors(t *testing.T) {
	var req ReportMetricsRequest
	assert.Error(t, json.Unmarshal([]byte(`{}`), &req), "missing report-type")
	assert.Error(t, json.Unmarshal([]byte(`{"report-type":"bogus"}`), &req), "unknown report-type")
}

func TestScanMetricsResult_ToleratesUnknownKeys(t *testing.T) {
	// Readers must tolerate metric keys they don't recognize (the spec metrics
	// field is an open map) — unknown keys are ignored, not an error.
	in := []byte(`{
		"report-type":"scan-report",
		"table-name":"t","snapshot-id":1,"schema-id":0,
		"projected-field-ids":[],"projected-field-names":[],"filter":{"type":"true"},
		"metrics":{"result-data-files":{"unit":"count","value":2},"some-future-metric":{"unit":"count","value":9}}
	}`)
	var req ReportMetricsRequest
	require.NoError(t, json.Unmarshal(in, &req))
	sr := req.Report.(ScanReport)
	require.NotNil(t, sr.Metrics.ResultDataFiles)
	assert.Equal(t, int64(2), sr.Metrics.ResultDataFiles.Value)
}
