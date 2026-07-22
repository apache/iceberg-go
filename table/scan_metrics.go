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
	"encoding/json"
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
//
// Parity note: Java's ScanMetricsUtil.fileTask counts result-delete-files per
// data-file task with no cross-task dedup, so a positional delete covering 5
// data files counts as 5 there and once here. Counting once by path is the
// deliberate choice for this result-scoped metric; keep it if touching this.
//
// Deletion vectors are keyed by the data file they reference, not by their
// Puffin file path: a single Puffin file holds one DV blob per data file (see
// DVWriter.Flush), so all DVs from one flush share a FilePath() and would
// collapse into one entry if keyed by it. Their size is the per-blob
// content_size_in_bytes, not the whole Puffin file size, matching Java's
// ScanTaskUtil.contentSizeInBytes.
func (acc *scanMetricsAccumulator) applyResultDeleteMetrics(tasks []FileScanTask) {
	posDeletes := make(map[string]int64)
	eqDeletes := make(map[string]int64)
	dvByRef := make(map[string]int64)
	for _, t := range tasks {
		for _, df := range t.DeleteFiles {
			posDeletes[df.FilePath()] = df.FileSizeBytes()
		}
		for _, df := range t.EqualityDeleteFiles {
			eqDeletes[df.FilePath()] = df.FileSizeBytes()
		}
		for _, df := range t.DeletionVectorFiles {
			// Key by the referenced data file so multiple DVs sharing one
			// Puffin path stay distinct; fall back to the Puffin path if the
			// reference is somehow unset. Size is the per-blob content size.
			key := df.FilePath()
			if ref := df.ReferencedDataFile(); ref != nil {
				key = *ref
			}
			var size int64
			if csb := df.ContentSizeInBytes(); csb != nil {
				size = *csb
			}
			dvByRef[key] = size
		}
	}

	acc.positionalDeleteFiles = int64(len(posDeletes))
	acc.equalityDeleteFiles = int64(len(eqDeletes))
	acc.dvs = int64(len(dvByRef))
	acc.resultDeleteFiles = acc.positionalDeleteFiles + acc.equalityDeleteFiles + acc.dvs
	for _, deletes := range []map[string]int64{posDeletes, eqDeletes, dvByRef} {
		for _, size := range deletes {
			acc.totalDeleteFileSize += size
		}
	}
}

// buildScanReport assembles the ScanReport for a completed planning operation.
// schema is the schema the scan planned against, threaded in from the planner so
// the report describes exactly what was used rather than re-resolving it here.
// Only the metrics that are actually measured are populated; unmeasured ones
// (e.g. skipped-data-files, indexed-delete-files) are left unset and omitted.
func (scan *Scan) buildScanReport(acc *scanMetricsAccumulator, schema *iceberg.Schema, planning time.Duration) metrics.ScanReport {
	ids, names := scan.projectedFields(schema)

	var snapshotID int64
	if snap := scan.Snapshot(); snap != nil {
		snapshotID = snap.SnapshotID
	}

	// Serialize the actual row filter as Expression JSON. On a marshal error we
	// leave Filter unset so ScanReport.MarshalJSON falls back to always-true
	// rather than failing the scan; literal sanitization (Java's ExpressionUtil)
	// is a follow-up.
	var filter json.RawMessage
	if scan.rowFilter != nil {
		if raw, err := json.Marshal(scan.rowFilter); err == nil {
			filter = raw
		}
	}

	return metrics.ScanReport{
		TableName:           strings.Join(scan.identifier, "."),
		SnapshotID:          snapshotID,
		SchemaID:            schema.ID,
		ProjectedFieldIDs:   ids,
		ProjectedFieldNames: names,
		Filter:              filter,
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
