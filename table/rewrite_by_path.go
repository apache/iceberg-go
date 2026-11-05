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
	"runtime"
	"slices"

	"github.com/apache/iceberg-go"
	tblutils "github.com/apache/iceberg-go/table/internal"
)

// StageRewrittenDataFilesByPath stages a replace snapshot using Parquet files
// already written by another engine. It does not read or rewrite data rows.
// Call Transaction.Commit to persist the staged snapshot.
//
// This helper supports unpartitioned v1 and v2 tables, including historical
// unpartitioned specs, and only when no removed file has applicable deletes.
// The replacement files must contain the same live rows and the same total
// number of physical rows. Column counts are compared when complete on both
// sides, and enabled value counts must not disappear. Missing optional null
// and NaN statistics are unknown, not zero. Neither footer statistics nor
// record counts can prove row-level equivalence. The caller is responsible
// for preserving the rows. For delete-aware compaction and partitioned tables,
// use RewriteDataFiles or NewRewrite with FileToDataFile instead.
//
// Both path lists must be non-empty, unique, and disjoint. Removed paths must
// be live data files in the target branch and added paths must not already be
// referenced there. Format v3 is not supported because path metadata alone
// cannot preserve row lineage.
func (t *Transaction) StageRewrittenDataFilesByPath(
	ctx context.Context,
	filesToDelete, filesToAdd []string,
	snapshotProps iceberg.Properties,
) error {
	meta, err := t.txnMeta()
	if err != nil {
		return err
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if meta.formatVersion >= 3 {
		return fmt.Errorf("%w: path-based rewrites do not support v3 row lineage", iceberg.ErrNotImplemented)
	}

	spec, err := meta.CurrentSpec()
	if err != nil {
		return err
	}
	if spec.NumFields() != 0 {
		return fmt.Errorf("%w: path-based rewrites do not support partitioned tables", iceberg.ErrNotImplemented)
	}
	if len(filesToDelete) == 0 || len(filesToAdd) == 0 {
		return fmt.Errorf("%w: a data-file rewrite requires both removed and added files", ErrInvalidOperation)
	}

	toDelete := make(map[string]struct{}, len(filesToDelete))
	for _, path := range filesToDelete {
		if path == "" {
			return fmt.Errorf("%w: removed file paths must be non-empty", ErrInvalidOperation)
		}
		if _, exists := toDelete[path]; exists {
			return fmt.Errorf("%w: duplicate removed file path %q", ErrInvalidOperation, path)
		}
		toDelete[path] = struct{}{}
	}

	toAdd := make(map[string]struct{}, len(filesToAdd))
	for _, path := range filesToAdd {
		if path == "" {
			return fmt.Errorf("%w: added file paths must be non-empty", ErrInvalidOperation)
		}
		if _, exists := toAdd[path]; exists {
			return fmt.Errorf("%w: duplicate added file path %q", ErrInvalidOperation, path)
		}
		if _, exists := toDelete[path]; exists {
			return fmt.Errorf("%w: cannot remove and add the same file %q", ErrInvalidOperation, path)
		}
		toAdd[path] = struct{}{}
	}

	snapshot := t.planningSnapshot(meta)
	if snapshot == nil {
		return fmt.Errorf("%w: cannot rewrite files in a table without an existing snapshot", ErrInvalidOperation)
	}

	fs, err := t.tbl.fsF(ctx)
	if err != nil {
		return err
	}
	if _, err := requireWriteFileIO(fs); err != nil {
		return err
	}

	// The scan below only exposes delete files that apply to planned data
	// files. A live delete file with no matching data file still reserves its
	// path in the table, so check all delete manifest entries before staging.
	for entry, err := range snapshot.entries(fs, iceberg.ManifestContentDeletes, true) {
		if err != nil {
			return err
		}
		if err := ctx.Err(); err != nil {
			return err
		}

		path := entry.DataFile().FilePath()
		if _, exists := toAdd[path]; exists {
			return fmt.Errorf("%w: added file %q is already referenced by the table as a delete file",
				ErrInvalidOperation, path)
		}
	}

	// Plan the target snapshot to discover delete files that apply to each
	// source file. Parquet splitting can yield several tasks per data file.
	scan, err := t.Scan(WithSnapshotID(snapshot.SnapshotID))
	if err != nil {
		return err
	}
	tasks, err := scan.PlanFiles(ctx)
	if err != nil {
		return err
	}

	removed := make([]iceberg.DataFile, 0, len(filesToDelete))
	found := make(map[string]struct{}, len(filesToDelete))
	var originalRecordCount int64
	for _, task := range tasks {
		df := task.File
		path := df.FilePath()
		if _, exists := toAdd[path]; exists {
			return fmt.Errorf("%w: added file %q is already referenced by the table", ErrInvalidOperation, path)
		}
		if _, selected := toDelete[path]; !selected {
			continue
		}
		if len(task.DeleteFiles) > 0 || len(task.EqualityDeleteFiles) > 0 || len(task.DeletionVectorFiles) > 0 {
			return fmt.Errorf("%w: cannot rewrite data file %q with applicable delete files; use NewRewrite or RewriteDataFiles", ErrInvalidOperation, path)
		}
		if _, duplicateTask := found[path]; duplicateTask {
			continue
		}
		if df.ContentType() != iceberg.EntryContentData {
			return fmt.Errorf("%w: cannot rewrite non-data file %q", ErrInvalidOperation, path)
		}
		sourceSpec, err := meta.GetSpecByID(int(df.SpecID()))
		if err != nil {
			return fmt.Errorf("resolving partition spec for source file %q: %w", path, err)
		}
		if sourceSpec.NumFields() != 0 {
			return fmt.Errorf("%w: source data file %q uses partitioned spec %d",
				iceberg.ErrNotImplemented, path, df.SpecID())
		}
		if df.Count() < 0 || df.Count() > math.MaxInt64-originalRecordCount {
			return fmt.Errorf("%w: invalid total record count for removed data files", ErrInvalidOperation)
		}
		originalRecordCount += df.Count()
		found[path] = struct{}{}
		removed = append(removed, df)
	}

	if len(found) != len(toDelete) {
		missing := make([]string, 0, len(toDelete)-len(found))
		for path := range toDelete {
			if _, exists := found[path]; !exists {
				missing = append(missing, path)
			}
		}
		slices.Sort(missing)

		return fmt.Errorf("%w: removed data file paths are not live in target branch: %v", ErrInvalidOperation, missing)
	}

	// Reading footers does not require a name mapping. Do not modify the
	// transaction until the replacement metadata and counts are validated.
	added, err := filesToDataFiles(ctx, fs, meta, filesToAdd, runtime.GOMAXPROCS(0), nil)
	if err != nil {
		return err
	}

	var replacementRecordCount int64
	for _, df := range added {
		if df.Count() < 0 || df.Count() > math.MaxInt64-replacementRecordCount {
			return fmt.Errorf("%w: invalid total record count for replacement data files", ErrInvalidOperation)
		}
		replacementRecordCount += df.Count()
	}
	if replacementRecordCount != originalRecordCount {
		return fmt.Errorf("%w: path-based rewrite record count changed from %d to %d",
			ErrInvalidOperation, originalRecordCount, replacementRecordCount)
	}
	statsPlan, err := computeStatsPlan(meta.CurrentSchema(), meta.props)
	if err != nil {
		return err
	}
	if err := validatePathRewriteMetrics(removed, added, statsPlan); err != nil {
		return err
	}

	rewrite := t.NewRewrite(snapshotProps)
	for _, df := range removed {
		rewrite.DeleteFile(df)
	}
	for _, df := range added {
		rewrite.AddDataFile(df)
	}

	return rewrite.Commit(ctx)
}

// completePathRewriteMetricTotals sums only columns reported by every file.
// Missing statistics are unknown, not zero.
func completePathRewriteMetricTotals(
	files []iceberg.DataFile,
	name string,
	get func(iceberg.DataFile) map[int]int64,
) (map[int]int64, error) {
	totals := make(map[int]int64)
	seen := make(map[int]int)
	for _, df := range files {
		for fieldID, count := range get(df) {
			total := totals[fieldID]
			if count < 0 || count > math.MaxInt64-total {
				return nil, fmt.Errorf("%w: invalid column %d %s count in data file %q",
					ErrInvalidOperation, fieldID, name, df.FilePath())
			}
			totals[fieldID] = total + count
			seen[fieldID]++
		}
	}

	for fieldID := range totals {
		if seen[fieldID] != len(files) {
			delete(totals, fieldID)
		}
	}

	return totals, nil
}

func validatePathRewriteMetrics(
	removed, added []iceberg.DataFile,
	statsPlan map[int]tblutils.StatisticsCollector,
) error {
	for _, metric := range []struct {
		name string
		get  func(iceberg.DataFile) map[int]int64
	}{
		{"value", iceberg.DataFile.ValueCounts},
		{"null", iceberg.DataFile.NullValueCounts},
		{"NaN", iceberg.DataFile.NaNValueCounts},
	} {
		before, err := completePathRewriteMetricTotals(removed, metric.name, metric.get)
		if err != nil {
			return err
		}
		after, err := completePathRewriteMetricTotals(added, metric.name, metric.get)
		if err != nil {
			return err
		}
		for fieldID, originalCount := range before {
			replacementCount, exists := after[fieldID]
			if !exists {
				// Parquet chunk metadata supplies value counts even without
				// optional statistics. When collection is enabled, their
				// absence indicates that the replacement lost a column.
				if metric.name == "value" {
					col, enabled := statsPlan[fieldID]
					if enabled && col.Mode.Typ != tblutils.MetricModeNone {
						return fmt.Errorf("%w: path-based rewrite missing column %d value count in replacement files",
							ErrInvalidOperation, fieldID)
					}
				}
				// Null/NaN statistics may be absent in valid Parquet files.
				continue
			}
			if replacementCount != originalCount {
				return fmt.Errorf("%w: path-based rewrite changed column %d %s count from %d to %d",
					ErrInvalidOperation, fieldID, metric.name, originalCount, replacementCount)
			}
		}
	}

	return nil
}
