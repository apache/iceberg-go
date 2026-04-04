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
	"fmt"
	"slices"
	"strings"

	"github.com/apache/iceberg-go/internal"
)

// CompactionConfig holds tunable thresholds for bin-pack compaction.
type CompactionConfig struct {
	// TargetFileSizeBytes is the desired output file size.
	// Default: WriteTargetFileSizeBytesDefault (512 MB).
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
	// Default: 5.
	MinInputFiles int

	// DeleteFileThreshold is the minimum number of delete files associated with
	// a data file to force it into compaction regardless of file size.
	// Default: 5.
	DeleteFileThreshold int
}

// DefaultCompactionConfig returns a CompactionConfig with production defaults.
func DefaultCompactionConfig() CompactionConfig {
	target := int64(WriteTargetFileSizeBytesDefault)

	return CompactionConfig{
		TargetFileSizeBytes: target,
		MinFileSizeBytes:    target * 3 / 4, // 75%
		MaxFileSizeBytes:    target * 9 / 5, // 180%
		MinInputFiles:       5,
		DeleteFileThreshold: 5,
	}
}

// Validate checks that the config values are sensible.
func (cfg CompactionConfig) Validate() error {
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
	if cfg.MinInputFiles < 1 {
		return fmt.Errorf("min input files must be >= 1, got %d", cfg.MinInputFiles)
	}
	if cfg.DeleteFileThreshold < 1 {
		return fmt.Errorf("delete file threshold must be >= 1, got %d", cfg.DeleteFileThreshold)
	}

	return nil
}

// CompactionGroup represents a set of files in the same partition
// that should be compacted together.
type CompactionGroup struct {
	// PartitionKey is an opaque grouping key derived from partition values.
	// Empty string for unpartitioned tables.
	PartitionKey string

	// Tasks are the input FileScanTasks that should be compacted together.
	Tasks []FileScanTask

	// TotalSizeBytes is the sum of data file sizes in this group.
	TotalSizeBytes int64

	// DeleteFileCount is the total number of delete files across all tasks.
	DeleteFileCount int
}

// CompactionPlan is the output of analyzing a table for compaction.
type CompactionPlan struct {
	// Groups are the sets of files to compact, each within a single partition.
	Groups []CompactionGroup

	// SkippedFiles is the count of files that are already optimal or oversized.
	SkippedFiles int

	// TotalInputFiles is the total number of files scanned.
	TotalInputFiles int

	// TotalInputBytes is the total size of all scanned files.
	TotalInputBytes int64

	// EstOutputFiles is the estimated number of output files after compaction.
	EstOutputFiles int

	// EstOutputBytes is the estimated total size of output files.
	// Conservative: assumes same size as input (actual may be smaller after delete application).
	EstOutputBytes int64
}

// Plan analyzes the given scan tasks and produces a CompactionPlan.
// Tasks are grouped by partition, classified as candidates or skipped,
// and candidates are bin-packed into groups targeting TargetFileSizeBytes.
func (cfg CompactionConfig) Plan(tasks []FileScanTask) (*CompactionPlan, error) {
	if err := cfg.Validate(); err != nil {
		return nil, fmt.Errorf("invalid compaction config: %w", err)
	}

	plan := &CompactionPlan{
		TotalInputFiles: len(tasks),
	}

	for _, t := range tasks {
		plan.TotalInputBytes += t.File.FileSizeBytes()
	}

	// Group tasks by partition key.
	type partitionBucket struct {
		key        string
		candidates []FileScanTask
	}
	partitions := make(map[string]*partitionBucket)
	var partitionOrder []string

	for _, t := range tasks {
		key := partitionKeyString(t)
		bucket, ok := partitions[key]
		if !ok {
			bucket = &partitionBucket{key: key}
			partitions[key] = bucket
			partitionOrder = append(partitionOrder, key)
		}

		if !cfg.isCandidate(t) {
			plan.SkippedFiles++

			continue
		}
		bucket.candidates = append(bucket.candidates, t)
	}

	// Bin-pack candidates per partition.
	// Lookback controls how many open bins the packer considers before evicting.
	// A higher value produces better packing at the cost of memory. 128 is a
	// reasonable default that handles heterogeneous file sizes well.
	const packingLookback = 128
	packer := internal.SlicePacker[FileScanTask]{
		TargetWeight:    cfg.TargetFileSizeBytes,
		Lookback:        packingLookback,
		LargestBinFirst: false,
	}

	for _, key := range partitionOrder {
		bucket := partitions[key]
		if len(bucket.candidates) < cfg.MinInputFiles {
			plan.SkippedFiles += len(bucket.candidates)

			continue
		}

		// Clone to avoid PackEnd mutating bucket.candidates via slices.Reverse.
		bins := packer.PackEnd(slices.Clone(bucket.candidates), func(t FileScanTask) int64 {
			return t.File.FileSizeBytes()
		})

		for _, bin := range bins {
			if len(bin) < cfg.MinInputFiles {
				plan.SkippedFiles += len(bin)

				continue
			}

			group := CompactionGroup{
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
// Files that are oversized (> MaxFileSizeBytes) are skipped unless they
// have enough associated delete files to exceed DeleteFileThreshold.
func (cfg CompactionConfig) isCandidate(t FileScanTask) bool {
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

// partitionKeyString returns a deterministic string key for grouping tasks by partition.
// Keys are sorted by field ID to ensure stability regardless of map iteration order.
func partitionKeyString(t FileScanTask) string {
	partition := t.File.Partition()
	if len(partition) == 0 {
		return ""
	}

	keys := make([]int, 0, len(partition))
	for k := range partition {
		keys = append(keys, k)
	}
	slices.Sort(keys)
	var b strings.Builder
	fmt.Fprintf(&b, "%d:", t.File.SpecID())
	for i, k := range keys {
		if i > 0 {
			b.WriteByte(',')
		}
		fmt.Fprintf(&b, "%d=%v", k, partition[k])
	}

	return b.String()
}
