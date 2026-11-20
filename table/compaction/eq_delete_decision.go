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
	"math"

	"github.com/apache/iceberg-go"
	"github.com/apache/iceberg-go/internal"
	tblutils "github.com/apache/iceberg-go/table/internal"
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
	specPartMinSeq map[survivorPartition]int64
	partitionErr   error
}

type survivorPartition struct {
	specID int32
	tuple  string
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
		specPartMinSeq:  make(map[survivorPartition]int64),
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
	seq = max(seq, 0)
	if len(partition) == 0 {
		s.EmptyPartMinSeq = min(seq, s.EmptyPartMinSeq)

		return
	}

	// An unsupported tuple uses the empty key. This API takes the minimum
	// across all buckets, so the survivor still protects older data.
	tuple, _ := survivorPartitionTuple(partition)
	updatePartitionMinSeq(s.PartMinSeq, tuple, seq)
}

// AddSurvivorWithSpec records a surviving data file's spec ID, partition, and
// sequence number for exact equality-delete matching. It also maintains the
// conservative survey used by [DecideDeadEqualityDeletes]. If a partition
// cannot be encoded, [DecideDeadEqualityDeletesWithSpecs] returns an error.
func (s *SurvivorSurvey) AddSurvivorWithSpec(specID int32, partition map[int]any, seq int64) {
	seq = max(seq, 0)
	s.minSeq = min(seq, s.minSeq)

	tuple, err := survivorPartitionTuple(partition)
	if err != nil {
		if s.partitionErr == nil {
			s.partitionErr = fmt.Errorf("encoding survivor partition for spec %d: %w", specID, err)
		}
		updatePartitionMinSeq(s.PartMinSeq, "", seq)

		return
	}
	if len(partition) == 0 {
		s.EmptyPartMinSeq = min(seq, s.EmptyPartMinSeq)
	} else {
		updatePartitionMinSeq(s.PartMinSeq, tuple, seq)
	}
	updatePartitionMinSeq(s.specPartMinSeq, survivorPartition{specID: specID, tuple: tuple}, seq)
}

func (s *SurvivorSurvey) conservativeMinSeq() int64 {
	minSeq := s.EmptyPartMinSeq
	for _, seq := range s.PartMinSeq {
		minSeq = min(minSeq, seq)
	}

	return minSeq
}

func (s *SurvivorSurvey) specAwareApplicableMinSeq(
	eqSpecID int32,
	eqPartition map[int]any,
	isUnpartitioned bool,
) (int64, error) {
	if isUnpartitioned {
		return s.minSeq, nil
	}
	tuple, err := survivorPartitionTuple(eqPartition)
	if err != nil {
		return 0, err
	}
	if v, ok := s.specPartMinSeq[survivorPartition{specID: eqSpecID, tuple: tuple}]; ok {
		return v, nil
	}

	return math.MaxInt64, nil
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
	minSeq := survey.conservativeMinSeq()
	for _, e := range candidates {
		df := e.DataFile()
		path := df.FilePath()
		if _, ok := seen[path]; ok {
			continue
		}
		if e.DataSequenceNumber() < 0 {
			continue
		}
		if minSeq >= e.DataSequenceNumber() {
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
	if survey.partitionErr != nil {
		return nil, survey.partitionErr
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
		if e.DataSequenceNumber() < 0 {
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
		minSeq, err := survey.specAwareApplicableMinSeq(
			df.SpecID(), internal.BorrowedDataFilePartition(df), isUnpartitioned,
		)
		if err != nil {
			return nil, fmt.Errorf("deciding equality delete file %q: %w", path, err)
		}
		if minSeq >= e.DataSequenceNumber() {
			seen[path] = struct{}{}
			dead = append(dead, df)
		}
	}

	return dead, nil
}

func survivorPartitionTuple(partition map[int]any) (string, error) {
	var storage [256]byte
	tuple, err := tblutils.AppendPartitionTuple(storage[:0], partition)

	return string(tuple), err
}

func updatePartitionMinSeq[K comparable](partitions map[K]int64, key K, seq int64) {
	if cur, ok := partitions[key]; ok {
		seq = min(seq, cur)
	}
	partitions[key] = seq
}
