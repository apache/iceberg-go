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
	"slices"
	"strings"
	"time"

	"github.com/apache/iceberg-go"
	"github.com/apache/iceberg-go/metrics"
)

// scanMetricsAccumulator gathers scan-planning counts. Every field is written
// from a single goroutine — the manifest counts in
// fetchPartitionSpecFilteredManifestsWithSchema and the result/delete counts in
// planFilesLocal after collectManifestEntriesWithSchema's concurrency barrier —
// so plain integers are race-free; no field is touched from the concurrent
// manifest workers.
//
// The scanned/skipped manifest counts measure partition-spec pruning (the
// filter applied while listing manifests). The per-entry skipped-data-files /
// skipped-delete-files counts (which would be incremented inside the concurrent
// openManifest loop, and would use atomics) and indexed-delete-files are left
// for a follow-up and omitted rather than reported as zero.
type scanMetricsAccumulator struct {
	totalDataManifests     int64
	totalDeleteManifests   int64
	scannedDataManifests   int64
	scannedDeleteManifests int64
	skippedDataManifests   int64
	skippedDeleteManifests int64

	resultDataFiles       int64
	resultDeleteFiles     int64
	totalFileSize         int64
	totalDeleteFileSize   int64
	positionalDeleteFiles int64
	equalityDeleteFiles   int64
	dvs                   int64
}

func counterCount(v int64) *metrics.CounterResult {
	return metrics.NewCounterResult(metrics.UnitCount, v)
}

func counterBytes(v int64) *metrics.CounterResult {
	return metrics.NewCounterResult(metrics.UnitBytes, v)
}

// applyResultDeleteMetrics derives the result-scoped delete-file metrics from
// the planned tasks. Each delete file is counted once by path across all the
// data files it applies to, split by kind (positional, equality, deletion
// vector); positional deletes suppressed by a deletion vector never appear on a
// task, so they are excluded. This keeps the counts consistent with
// result-data-files and total-file-size-in-bytes.
func (acc *scanMetricsAccumulator) applyResultDeleteMetrics(tasks []FileScanTask) {
	posDeletes := make(map[string]int64)
	eqDeletes := make(map[string]int64)
	dvs := make(map[string]int64)
	for _, t := range tasks {
		for _, df := range t.DeleteFiles {
			posDeletes[df.FilePath()] = df.FileSizeBytes()
		}
		for _, df := range t.EqualityDeleteFiles {
			eqDeletes[df.FilePath()] = df.FileSizeBytes()
		}
		for _, df := range t.DeletionVectorFiles {
			dvs[df.FilePath()] = df.FileSizeBytes()
		}
	}

	acc.positionalDeleteFiles = int64(len(posDeletes))
	acc.equalityDeleteFiles = int64(len(eqDeletes))
	acc.dvs = int64(len(dvs))
	acc.resultDeleteFiles = acc.positionalDeleteFiles + acc.equalityDeleteFiles + acc.dvs
	for _, deletes := range []map[string]int64{posDeletes, eqDeletes, dvs} {
		for _, size := range deletes {
			acc.totalDeleteFileSize += size
		}
	}
}

// buildScanReport assembles the ScanReport for a completed planning operation.
// Only the metrics that are actually measured are populated; unmeasured ones
// (e.g. skipped-data-files, indexed-delete-files) are left unset and omitted.
func (scan *Scan) buildScanReport(acc *scanMetricsAccumulator, planning time.Duration) metrics.ScanReport {
	// Resolve the schema the scan actually planned against (the snapshot schema
	// for snapshot/as-of scans, the current schema otherwise). Best-effort: a
	// report must never fail the scan, so fall back to the current schema.
	schema, err := scan.effectiveSchema()
	if err != nil || schema == nil {
		schema = scan.metadata.CurrentSchema()
	}

	ids, names := scan.projectedFields(schema)

	var snapshotID int64
	if snap := scan.Snapshot(); snap != nil {
		snapshotID = snap.SnapshotID
	}

	return metrics.ScanReport{
		TableName:           strings.Join(scan.identifier, "."),
		SnapshotID:          snapshotID,
		SchemaID:            schema.ID,
		ProjectedFieldIDs:   ids,
		ProjectedFieldNames: names,
		Metrics: metrics.ScanMetricsResult{
			TotalPlanningDuration:      metrics.NewNanosTimerResult(1, planning.Nanoseconds()),
			ResultDataFiles:            counterCount(acc.resultDataFiles),
			ResultDeleteFiles:          counterCount(acc.resultDeleteFiles),
			TotalDataManifests:         counterCount(acc.totalDataManifests),
			TotalDeleteManifests:       counterCount(acc.totalDeleteManifests),
			ScannedDataManifests:       counterCount(acc.scannedDataManifests),
			ScannedDeleteManifests:     counterCount(acc.scannedDeleteManifests),
			SkippedDataManifests:       counterCount(acc.skippedDataManifests),
			SkippedDeleteManifests:     counterCount(acc.skippedDeleteManifests),
			TotalFileSizeInBytes:       counterBytes(acc.totalFileSize),
			TotalDeleteFileSizeInBytes: counterBytes(acc.totalDeleteFileSize),
			EqualityDeleteFiles:        counterCount(acc.equalityDeleteFiles),
			PositionalDeleteFiles:      counterCount(acc.positionalDeleteFiles),
			DVs:                        counterCount(acc.dvs),
		},
	}
}

// projectedFields resolves the scan's selected fields to their ids and names
// against schema. A "*" (or empty) selection projects all top-level fields.
// Unresolvable names are skipped.
func (scan *Scan) projectedFields(schema *iceberg.Schema) ([]int, []string) {
	if len(scan.selectedFields) == 0 || slices.Contains(scan.selectedFields, "*") {
		fields := schema.Fields()
		ids := make([]int, len(fields))
		names := make([]string, len(fields))
		for i, f := range fields {
			ids[i], names[i] = f.ID, f.Name
		}

		return ids, names
	}

	ids := make([]int, 0, len(scan.selectedFields))
	names := make([]string, 0, len(scan.selectedFields))
	for _, name := range scan.selectedFields {
		nf, ok := schema.FindFieldByName(name)
		if !ok && !scan.caseSensitive {
			nf, ok = schema.FindFieldByNameCaseInsensitive(name)
		}
		if ok {
			ids = append(ids, nf.ID)
			names = append(names, nf.Name)
		}
	}

	return ids, names
}
