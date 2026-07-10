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

// Package compaction provides bin-pack compaction planning for Iceberg tables.
package compaction

import (
	"bytes"
	"fmt"

	"github.com/apache/iceberg-go"
	"github.com/apache/iceberg-go/internal"
	"github.com/apache/iceberg-go/table"
)

// Config holds tunable thresholds for bin-pack compaction.
type Config struct {
	// TargetFileSizeBytes is the desired output file size.
	// Default: table.WriteTargetFileSizeBytesDefault (512 MB).
	TargetFileSizeBytes int64

	// MinFileSizeBytes is the lower bound for a file to be considered "optimal".
	// Files smaller than this are candidates for compaction.
	// Default: 75% of TargetFileSizeBytes.
	MinFileSizeBytes int64

	// MaxFileSizeBytes is the upper bound. Files larger than this are never rewritten
	// (unless they exceed DeleteFileThreshold).
	// Default: 180% of TargetFileSizeBytes.
	MaxFileSizeBytes int64

	// MinInputFiles is the minimum number of files in a group to justify rewriting.
	// Groups with fewer files are dropped from the plan.
	// Default: DefaultMinInputFiles.
	MinInputFiles uint

	// DeleteFileThreshold is the minimum number of delete files associated with
	// a data file to force it into compaction regardless of file size.
	// Default: 5.
	DeleteFileThreshold int

	// DeleteRatioThreshold forces a data file into compaction when the fraction
	// of its rows shadowed by file-scoped deletes (deletion vectors and
	// path-scoped positional deletes) reaches this value, regardless of file
	// size. File-scoped deletes apply to exactly one data file and carry an
	// exact deleted-row count, so size-based selection alone never reclaims a
	// right-sized file's dead space; this rule does. Range [0,1]; 0 disables
	// it. Default: DefaultDeleteRatioThreshold.
	DeleteRatioThreshold float64

	// PackingLookback controls how many open bins the packer considers
	// before evicting. Higher values produce better packing at the cost
	// of memory. Default: DefaultPackingLookback.
	PackingLookback uint

	// PreserveDeadEqualityDeletes, when true, retains equality delete
	// files that are provably dead after the rewrite. The cleanup
	// predicate matches the v2 reader (see scanner.go
	// matchEqualityDeletesToData and DecideDeadEqualityDeletes): an
	// eq-delete is dead iff no surviving applicable data file has
	// seq < eq-delete.seq, where "applicable" means same partition
	// tuple OR either side has empty partition. SpecID is NOT part of
	// the predicate.
	//
	// Zero value (false) is the recommended default: dead eq-deletes are
	// expunged during the rewrite commit, which keeps manifest fanout
	// bounded under sustained CDC workloads where eq-deletes accumulate
	// one-per-snapshot. Set to true only if a downstream consumer
	// depends on the historical eq-delete files surviving in the live
	// snapshot's manifests (rare).
	//
	// Honored only in atomic mode (RewriteDataFilesOptions.PartialProgress=false).
	// The CLI / library caller is responsible for translating this flag
	// into the snapshot walk + RewriteDataFilesOptions.ExtraDeleteFilesToRemove
	// — see CollectDeadEqualityDeletes.
	PreserveDeadEqualityDeletes bool
}

const (
	// DefaultMinInputFiles is the default minimum number of files per group.
	DefaultMinInputFiles uint = 5

	// DefaultPackingLookback is the default packing lookback.
	DefaultPackingLookback uint = 128

	// DefaultDeleteRatioThreshold mirrors Java's delete-ratio default: a file
	// whose file-scoped deletes (deletion vectors and path-scoped positional
	// deletes) shadow at least 30% of its rows is rewritten.
	DefaultDeleteRatioThreshold float64 = 0.3
)

// DefaultConfig returns a Config with production defaults.
func DefaultConfig() Config {
	target := int64(table.WriteTargetFileSizeBytesDefault)

	return Config{
		TargetFileSizeBytes:  target,
		MinFileSizeBytes:     target * 3 / 4, // 75%
		MaxFileSizeBytes:     target * 9 / 5, // 180%
		MinInputFiles:        DefaultMinInputFiles,
		DeleteFileThreshold:  5,
		DeleteRatioThreshold: DefaultDeleteRatioThreshold,
		PackingLookback:      DefaultPackingLookback,
	}
}

// Validate checks that the config values are sensible.
func (cfg Config) Validate() error {
	if cfg.TargetFileSizeBytes <= 0 {
		return fmt.Errorf("target file size must be positive, got %d", cfg.TargetFileSizeBytes)
	}
	if cfg.MinFileSizeBytes < 0 {
		return fmt.Errorf("min file size must be non-negative, got %d", cfg.MinFileSizeBytes)
	}
	if cfg.MaxFileSizeBytes <= 0 {
		return fmt.Errorf("max file size must be positive, got %d", cfg.MaxFileSizeBytes)
	}
	if cfg.MinFileSizeBytes >= cfg.MaxFileSizeBytes {
		return fmt.Errorf("min file size (%d) must be less than max (%d)",
			cfg.MinFileSizeBytes, cfg.MaxFileSizeBytes)
	}
	if cfg.TargetFileSizeBytes < cfg.MinFileSizeBytes || cfg.TargetFileSizeBytes > cfg.MaxFileSizeBytes {
		return fmt.Errorf("target file size (%d) must be between min (%d) and max (%d)",
			cfg.TargetFileSizeBytes, cfg.MinFileSizeBytes, cfg.MaxFileSizeBytes)
	}
	if cfg.DeleteFileThreshold < 1 {
		return fmt.Errorf("delete file threshold must be >= 1, got %d", cfg.DeleteFileThreshold)
	}
	if cfg.DeleteRatioThreshold < 0 || cfg.DeleteRatioThreshold > 1 {
		return fmt.Errorf("delete ratio threshold must be in [0,1], got %g", cfg.DeleteRatioThreshold)
	}

	return nil
}

// Group represents a set of files in the same partition
// that should be compacted together.
type Group struct {
	// PartitionKey is an opaque grouping key derived from partition values.
	// Empty string for unpartitioned tables.
	PartitionKey string

	// Tasks are the input FileScanTasks that should be compacted together.
	Tasks []table.FileScanTask

	// TotalSizeBytes is the sum of data file sizes in this group.
	TotalSizeBytes int64

	// DeleteFileCount is the total number of delete files across all tasks.
	DeleteFileCount int
}

// Plan is the output of analyzing a table for compaction.
type Plan struct {
	// Groups are the sets of files to compact, each within a single partition.
	Groups []Group

	// SkippedFiles is the count of files that are already optimal or oversized.
	SkippedFiles int

	// TotalInputFiles is the total number of files scanned.
	TotalInputFiles int

	// TotalInputBytes is the total size of all scanned files.
	TotalInputBytes int64

	// EstOutputFiles is the estimated number of output files after compaction.
	EstOutputFiles int

	// EstOutputBytes is the estimated total size of output files.
	// This is an upper-bound estimate: actual output may be smaller because
	// deleted rows are removed and Parquet compression/encoding is more
	// effective on larger files with better column statistics.
	EstOutputBytes int64
}

// PlanCompaction analyzes the given scan tasks and produces a Plan.
// Tasks are grouped by partition, classified as candidates or skipped,
// and candidates are bin-packed into groups targeting TargetFileSizeBytes.
func (cfg Config) PlanCompaction(tasks []table.FileScanTask) (Plan, error) {
	if err := cfg.Validate(); err != nil {
		return Plan{}, fmt.Errorf("invalid compaction config: %w", err)
	}

	plan := Plan{
		TotalInputFiles: len(tasks),
	}

	for _, t := range tasks {
		plan.TotalInputBytes += t.File.FileSizeBytes()
	}

	// Group tasks by partition key.
	type partitionBucket struct {
		key        string
		candidates []table.FileScanTask
	}
	partitions := make(map[string]partitionBucket)
	var partitionOrder []string

	for _, t := range tasks {
		key := partitionBucketKey(t.File.SpecID(), t.File.Partition())
		bucket, ok := partitions[key]
		if !ok {
			bucket = partitionBucket{key: key}
			partitionOrder = append(partitionOrder, key)
		}

		if !cfg.isCandidate(t) {
			plan.SkippedFiles++
			partitions[key] = bucket

			continue
		}
		bucket.candidates = append(bucket.candidates, t)
		partitions[key] = bucket
	}

	// Bin-pack candidates per partition.
	packer := internal.SlicePacker[table.FileScanTask]{
		TargetWeight:    cfg.TargetFileSizeBytes,
		Lookback:        int(cfg.PackingLookback),
		LargestBinFirst: false,
	}

	for _, key := range partitionOrder {
		bucket := partitions[key]
		if len(bucket.candidates) < int(cfg.MinInputFiles) {
			plan.SkippedFiles += len(bucket.candidates)

			continue
		}

		bins := packer.PackEnd(bucket.candidates, func(t table.FileScanTask) int64 {
			return t.File.FileSizeBytes()
		})

		for _, bin := range bins {
			if len(bin) < int(cfg.MinInputFiles) {
				plan.SkippedFiles += len(bin)

				continue
			}

			group := Group{
				PartitionKey: key,
				Tasks:        bin,
			}
			for _, t := range bin {
				group.TotalSizeBytes += t.File.FileSizeBytes()
				group.DeleteFileCount += len(t.DeleteFiles) + len(t.EqualityDeleteFiles) + len(t.DeletionVectorFiles)
			}

			estFiles := max(1, int((group.TotalSizeBytes+cfg.TargetFileSizeBytes-1)/cfg.TargetFileSizeBytes))
			plan.EstOutputFiles += estFiles
			plan.EstOutputBytes += group.TotalSizeBytes

			plan.Groups = append(plan.Groups, group)
		}
	}

	return plan, nil
}

// isCandidate returns true if a file should be considered for compaction.
func (cfg Config) isCandidate(t table.FileScanTask) bool {
	size := t.File.FileSizeBytes()

	// Deletion vectors are counted here alongside positional and equality
	// delete files, matching Java's unified task.deletes() count. A data file
	// has at most one DV, so at the minimum DeleteFileThreshold of 1 a single
	// DV trips this rule before the ratio rule below runs; at the default of 5
	// it does not. The ratio rule still owns right-sized DV-backed files at any
	// threshold above 1.
	deleteCount := len(t.DeleteFiles) + len(t.EqualityDeleteFiles) + len(t.DeletionVectorFiles)

	// Too many delete files — always compact regardless of size.
	if deleteCount >= cfg.DeleteFileThreshold {
		return true
	}

	// Enough rows shadowed by file-scoped deletes — compact to reclaim the dead
	// space. Mirrors Java BinPackRewriteFilePlanner.tooHighDeleteRatio: sum the
	// record counts of file-scoped deletes (deletion vectors and path-scoped
	// positional deletes — see isFileScoped), cap at the data file's record
	// count, and compare the ratio. Equality deletes and partition-scoped
	// positional deletes are not file-scoped (their counts cannot be attributed
	// to one data file) and are left to the count rule.
	if cfg.DeleteRatioThreshold > 0 {
		if records := t.File.Count(); records > 0 {
			deleted := min(fileScopedDeletedRows(t), records)
			if float64(deleted)/float64(records) >= cfg.DeleteRatioThreshold {
				return true
			}
		}
	}

	// Oversized with few deletes — skip.
	if size > cfg.MaxFileSizeBytes {
		return false
	}

	// Right-sized — skip (optimal).
	if size >= cfg.MinFileSizeBytes {
		return false
	}

	// Undersized — candidate.
	return true
}

// fileScopedDeletedRows sums the record counts of the task's file-scoped
// deletes — deletion vectors and path-scoped positional deletes. These apply
// to exactly one data file, so their counts are attributable to it. The sum is
// not capped here; callers cap at the data file's record count.
func fileScopedDeletedRows(t table.FileScanTask) int64 {
	var deleted int64
	for _, d := range t.DeletionVectorFiles {
		if isFileScoped(d) {
			deleted += d.Count()
		}
	}
	for _, d := range t.DeleteFiles {
		if isFileScoped(d) {
			deleted += d.Count()
		}
	}

	return deleted
}

// isFileScoped reports whether a delete file applies to exactly one data file.
// Equality deletes and partition-scoped positional deletes are not file-scoped.
func isFileScoped(d iceberg.DataFile) bool {
	if d.ContentType() == iceberg.EntryContentEqDeletes {
		return false
	}

	return referencedDataFilePath(d) != ""
}

// referencedDataFilePath resolves the single data file a delete targets, or ""
// when it is partition-scoped (no single target). Mirrors Java
// ContentFileUtil.referencedDataFile: explicit referenced_data_file first, then
// equal file_path lower/upper bounds — the bounds fallback matters because
// referenced_data_file is optional in V2 and our own scan planning never sets
// it (see matchDeletesToData).
func referencedDataFilePath(d iceberg.DataFile) string {
	if ref := d.ReferencedDataFile(); ref != nil && *ref != "" {
		return *ref
	}

	lower := d.LowerBoundValues()[filePathFieldID]
	upper := d.UpperBoundValues()[filePathFieldID]
	if len(lower) == 0 || !bytes.Equal(lower, upper) {
		return ""
	}

	lit, err := iceberg.LiteralFromBytes(iceberg.PrimitiveTypes.String, lower)
	if err != nil {
		return ""
	}

	return lit.(iceberg.TypedLiteral[string]).Value()
}

// filePathFieldID is the reserved field ID of the file_path column in a
// positional delete file, sourced from the canonical delete schema.
var filePathFieldID = func() int {
	f, _ := iceberg.PositionalDeleteSchema.FindFieldByName("file_path")

	return f.ID
}()
