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

package compaction

import (
	"fmt"
	"maps"
	"math"
	"slices"

	"github.com/DataDog/iceberg-go"
)

// SurvivorSurvey describes the surviving data files in a snapshot after a
// planned rewrite has logically removed its rewrite set. It is the input to
// [DecideDeadEqualityDeletes] and [DecideDeadEqualityDeletesWithSpecs].
//
// EmptyPartMinSeq and PartMinSeq retain the original survey layout used by
// DecideDeadEqualityDeletes. Spec-aware callers should populate the survey
// with [SurvivorSurvey.AddSurvivorWithSpec].
//
// Sentinel "no survivor in this bucket" is math.MaxInt64.
type SurvivorSurvey struct {
	EmptyPartMinSeq int64
	PartMinSeq      map[string]int64

	minSeq         int64
	specPartMinSeq map[string]int64
}

// PartitionSpecLookup resolves the partition spec used to write a data file.
type PartitionSpecLookup interface {
	PartitionSpecByID(int) *iceberg.PartitionSpec
}

// NewSurvivorSurvey returns a survey initialized with the no-survivor
// sentinel for the empty-partition bucket and an empty per-partition
// map. Callers populate via AddSurvivor.
func NewSurvivorSurvey() *SurvivorSurvey {
	return &SurvivorSurvey{
		EmptyPartMinSeq: math.MaxInt64,
		PartMinSeq:      make(map[string]int64),
		minSeq:          math.MaxInt64,
		specPartMinSeq:  make(map[string]int64),
	}
}

// AddSurvivor records a surviving data file's partition and sequence number
// using the conservative compatibility rule.
//
// Defensive: if seq < 0 (sentinel for "unset"), the file is recorded
// with seq=0 (smallest real value), which keeps it permanently alive
// against every eq-delete. Better to preserve uncertain state than to
// drop deletes that may still apply.
func (s *SurvivorSurvey) AddSurvivor(partition map[int]any, seq int64) {
	if seq < 0 {
		seq = 0
	}
	if len(partition) == 0 {
		s.EmptyPartMinSeq = min(seq, s.EmptyPartMinSeq)

		return
	}

	key := partitionMatchKey(partition)
	if cur, ok := s.PartMinSeq[key]; ok {
		seq = min(seq, cur)
	}
	s.PartMinSeq[key] = seq
}

// AddSurvivorWithSpec records a surviving data file's spec ID, partition, and
// sequence number for exact equality-delete matching. It also maintains the
// conservative survey used by [DecideDeadEqualityDeletes].
func (s *SurvivorSurvey) AddSurvivorWithSpec(specID int32, partition map[int]any, seq int64) {
	s.AddSurvivor(partition, seq)
	if seq < 0 {
		seq = 0
	}
	s.minSeq = min(seq, s.minSeq)

	key := partitionBucketKey(specID, partition)
	if cur, ok := s.specPartMinSeq[key]; ok {
		seq = min(seq, cur)
	}
	s.specPartMinSeq[key] = seq
}

func (s *SurvivorSurvey) conservativeMinSeq() int64 {
	if len(s.PartMinSeq) == 0 {
		return s.EmptyPartMinSeq
	}

	return min(s.EmptyPartMinSeq, slices.Min(slices.Collect(maps.Values(s.PartMinSeq))))
}

func (s *SurvivorSurvey) specAwareApplicableMinSeq(
	eqSpecID int32,
	eqPartition map[int]any,
	isUnpartitioned bool,
) int64 {
	if isUnpartitioned {
		return s.minSeq
	}
	if v, ok := s.specPartMinSeq[partitionBucketKey(eqSpecID, eqPartition)]; ok {
		return v
	}

	return math.MaxInt64
}

// DecideDeadEqualityDeletes conservatively returns equality-delete files that
// are dead without partition spec metadata. Because a non-empty partition
// tuple may belong to a void-only spec whose deletes apply globally, this
// compatibility API assumes every candidate could be global. It may preserve
// deletes that exact spec-aware matching can prove dead. New callers with
// table metadata should use [DecideDeadEqualityDeletesWithSpecs].
func DecideDeadEqualityDeletes(survey *SurvivorSurvey, candidates []iceberg.ManifestEntry) []iceberg.DataFile {
	if survey == nil || len(candidates) == 0 {
		return nil
	}

	dead := make([]iceberg.DataFile, 0, len(candidates))
	seen := make(map[string]struct{}, len(candidates))
	for _, e := range candidates {
		df := e.DataFile()
		path := df.FilePath()
		if _, ok := seen[path]; ok {
			continue
		}
		if e.SequenceNum() < 0 {
			continue
		}
		if survey.conservativeMinSeq() >= e.SequenceNum() {
			seen[path] = struct{}{}
			dead = append(dead, df)
		}
	}

	return dead
}

// DecideDeadEqualityDeletesWithSpecs is the exact spec-aware predicate for
// equality-delete cleanup during compaction. Given a survey of surviving data
// file sequences populated with [SurvivorSurvey.AddSurvivorWithSpec], it
// returns the equality-delete files that no surviving data file could apply to.
//
// The cleanup rule is:
//
//	E applies to D iff E.seq > D.seq AND (
//	    E.spec.isUnpartitioned() ||
//	    (E.specID == D.specID && E.partition == D.partition)
//	)
//
// E is dead iff no applicable surviving D has D.seq < E.seq —
// equivalently, the applicable min-seq is >= E.seq.
//
// Defensive: candidates with sequence number < 0 (sentinel for unset)
// are skipped — preserved rather than risk dropping an unidentifiable
// file.
//
// Dedup by file path: the same eq-delete file may appear in multiple
// manifest entries after manifest merging.
func DecideDeadEqualityDeletesWithSpecs(
	survey *SurvivorSurvey,
	candidates []iceberg.ManifestEntry,
	specs PartitionSpecLookup,
) ([]iceberg.DataFile, error) {
	if survey == nil || len(candidates) == 0 {
		return nil, nil
	}

	dead := make([]iceberg.DataFile, 0, len(candidates))
	seen := make(map[string]struct{}, len(candidates))
	unpartitionedBySpecID := make(map[int32]bool)
	for _, e := range candidates {
		df := e.DataFile()
		path := df.FilePath()
		if _, ok := seen[path]; ok {
			continue
		}
		if e.SequenceNum() < 0 {
			continue
		}
		isUnpartitioned, ok := unpartitionedBySpecID[df.SpecID()]
		if !ok {
			spec := specs.PartitionSpecByID(int(df.SpecID()))
			if spec == nil {
				return nil, fmt.Errorf("deciding equality delete file %s: partition spec ID %d not found",
					path, df.SpecID())
			}
			isUnpartitioned = spec.IsUnpartitioned()
			unpartitionedBySpecID[df.SpecID()] = isUnpartitioned
		}
		if survey.specAwareApplicableMinSeq(df.SpecID(), df.Partition(), isUnpartitioned) >= e.SequenceNum() {
			seen[path] = struct{}{}
			dead = append(dead, df)
		}
	}

	return dead, nil
}

// partitionMatchKey returns a deterministic key for a partition tuple without
// a spec ID. It is used only by the conservative compatibility API.
func partitionMatchKey(part map[int]any) string {
	if len(part) == 0 {
		return ""
	}

	return string(appendPartitionTuple(nil, part))
}

// partitionBucketKey returns a deterministic string key for a
// (specID, partition) tuple. Used by the planner to group tasks for
// bin-packing — different specs must NOT mix because a compacted
// output file inherits a single spec.
func partitionBucketKey(specID int32, part map[int]any) string {
	if len(part) == 0 {
		return fmt.Sprintf("%d:_", specID)
	}

	return string(appendPartitionTuple(fmt.Appendf(nil, "%d:", specID), part))
}

// appendPartitionTuple emits the sorted "id=value;" tuple into dst.
// Caller is responsible for any leading prefix (e.g. specID).
func appendPartitionTuple(dst []byte, part map[int]any) []byte {
	ids := make([]int, 0, len(part))
	for id := range part {
		ids = append(ids, id)
	}
	slices.Sort(ids)
	for _, id := range ids {
		dst = fmt.Appendf(dst, "%d=%v;", id, part[id])
	}

	return dst
}
