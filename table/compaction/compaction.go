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
	"fmt"

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
)

// DefaultConfig returns a Config with production defaults.
func DefaultConfig() Config {
	target := int64(table.WriteTargetFileSizeBytesDefault)

	return Config{
		TargetFileSizeBytes: target,
		MinFileSizeBytes:    target * 3 / 4, // 75%
		MaxFileSizeBytes:    target * 9 / 5, // 180%
		MinInputFiles:       DefaultMinInputFiles,
		DeleteFileThreshold: 5,
		PackingLookback:     DefaultPackingLookback,
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
				group.DeleteFileCount += len(t.DeleteFiles) + len(t.EqualityDeleteFiles)
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
	deleteCount := len(t.DeleteFiles) + len(t.EqualityDeleteFiles)

	// Too many deletes — always compact regardless of size.
	if deleteCount >= cfg.DeleteFileThreshold {
		return true
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
