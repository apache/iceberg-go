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
	"cmp"
	"fmt"
	"math"
	"slices"
	"sort"

	"github.com/DataDog/iceberg-go"
	"github.com/google/uuid"
)

// equalityDeleteIndex groups equality deletes by partition and sequence number.
// Unpartitioned equality deletes are global and apply across partition specs;
// partitioned deletes only apply to data files in the same spec and partition.
type equalityDeleteIndex struct {
	global      []iceberg.ManifestEntry
	byPartition map[equalityDeletePartitionKey][]iceberg.ManifestEntry
}

type partitionSpecLookup interface {
	PartitionSpecByID(int) *iceberg.PartitionSpec
}

type equalityDeletePartitionKey struct {
	specID  int32
	fieldID int
	value   any
	tuple   string
	single  bool
}

type (
	equalityDeleteIntegerPartitionValue int64
	equalityDeleteFloatPartitionValue   uint64
	equalityDeleteStringPartitionValue  string
	equalityDeleteEncodedPartitionValue string
	equalityDeleteBinaryPartitionValue  string
	equalityDeleteNaNPartitionValue     struct{}
	equalityDeleteNilPartitionValue     struct{}
)

// Use a comparable value directly for the common single-field partition spec
// so each data-file lookup does not allocate an encoded tuple. Multi-field
// specs use the same normalized encoding as conflict validation.
func newEqualityDeletePartitionKey(
	specID int32,
	partition map[int]any,
) (equalityDeletePartitionKey, error) {
	if len(partition) == 1 {
		for fieldID, value := range partition {
			comparable, err := comparableEqualityDeletePartitionValue(value)
			if err != nil {
				return equalityDeletePartitionKey{}, fmt.Errorf("partition field %d: %w", fieldID, err)
			}

			return equalityDeletePartitionKey{
				specID: specID, fieldID: fieldID, value: comparable, single: true,
			}, nil
		}
	}

	tuple, err := partitionConflictKey(specID, partition)
	if err != nil {
		return equalityDeletePartitionKey{}, err
	}

	return equalityDeletePartitionKey{specID: specID, tuple: tuple}, nil
}

func comparableEqualityDeletePartitionValue(value any) (any, error) {
	switch value := value.(type) {
	case nil:
		return equalityDeleteNilPartitionValue{}, nil
	case bool:
		return value, nil
	case int:
		return equalityDeleteIntegerPartitionValue(value), nil
	case int32:
		return equalityDeleteIntegerPartitionValue(value), nil
	case int64:
		return equalityDeleteIntegerPartitionValue(value), nil
	case iceberg.Date:
		return equalityDeleteIntegerPartitionValue(value), nil
	case iceberg.Time:
		return equalityDeleteIntegerPartitionValue(value), nil
	case iceberg.Timestamp:
		return equalityDeleteIntegerPartitionValue(value), nil
	case iceberg.TimestampNano:
		return equalityDeleteIntegerPartitionValue(value), nil
	case float32:
		value64 := float64(value)
		if math.IsNaN(value64) {
			return equalityDeleteNaNPartitionValue{}, nil
		}

		return equalityDeleteFloatPartitionValue(math.Float64bits(value64)), nil
	case float64:
		if math.IsNaN(value) {
			return equalityDeleteNaNPartitionValue{}, nil
		}

		return equalityDeleteFloatPartitionValue(math.Float64bits(value)), nil
	case string:
		return equalityDeleteStringPartitionValue(value), nil
	case []byte:
		return equalityDeleteBinaryPartitionValue(value), nil
	case uuid.UUID:
		return equalityDeleteBinaryPartitionValue(value[:]), nil
	default:
		encoded, err := appendPartitionConflictValue(nil, value)
		if err != nil {
			return nil, err
		}

		return equalityDeleteEncodedPartitionValue(encoded), nil
	}
}

func buildEqualityDeleteIndex(
	entries []iceberg.ManifestEntry,
	specs partitionSpecLookup,
) (*equalityDeleteIndex, error) {
	idx := &equalityDeleteIndex{}
	unpartitionedBySpecID := make(map[int32]bool)
	for _, entry := range entries {
		df := entry.DataFile()
		isUnpartitioned, ok := unpartitionedBySpecID[df.SpecID()]
		if !ok {
			spec := specs.PartitionSpecByID(int(df.SpecID()))
			if spec == nil {
				return nil, fmt.Errorf("indexing equality delete file %s: %w: id %d",
					df.FilePath(), ErrPartitionSpecNotFound, df.SpecID())
			}
			isUnpartitioned = spec.IsUnpartitioned()
			unpartitionedBySpecID[df.SpecID()] = isUnpartitioned
		}
		if isUnpartitioned {
			idx.global = append(idx.global, entry)

			continue
		}

		partition := df.Partition()
		key, err := newEqualityDeletePartitionKey(df.SpecID(), partition)
		if err != nil {
			return nil, fmt.Errorf("indexing equality delete file %s: %w", df.FilePath(), err)
		}
		if idx.byPartition == nil {
			idx.byPartition = make(map[equalityDeletePartitionKey][]iceberg.ManifestEntry)
		}
		idx.byPartition[key] = append(idx.byPartition[key], entry)
	}

	sortBySequence := func(entries []iceberg.ManifestEntry) {
		slices.SortStableFunc(entries, func(a, b iceberg.ManifestEntry) int {
			return cmp.Compare(a.SequenceNum(), b.SequenceNum())
		})
	}
	sortBySequence(idx.global)
	for _, partitionEntries := range idx.byPartition {
		sortBySequence(partitionEntries)
	}

	return idx, nil
}

// forDataFile returns equality deletes with a strictly greater sequence number.
// The strict comparison keeps rows added in the same snapshot as an equality
// delete, matching the RowDelta semantics and Java's DeleteFileIndex.
func (idx *equalityDeleteIndex) forDataFile(dataEntry iceberg.ManifestEntry) ([]iceberg.DataFile, error) {
	if len(idx.global) == 0 && len(idx.byPartition) == 0 {
		return nil, nil
	}

	partitionEntries := []iceberg.ManifestEntry(nil)
	if len(idx.byPartition) > 0 {
		dataFile := dataEntry.DataFile()
		partition := dataFile.Partition()
		if len(partition) > 0 {
			key, err := newEqualityDeletePartitionKey(dataFile.SpecID(), partition)
			if err != nil {
				return nil, fmt.Errorf("matching equality deletes to data file %s: %w", dataFile.FilePath(), err)
			}
			partitionEntries = idx.byPartition[key]
		}
	}

	dataSeqNum := dataEntry.SequenceNum()
	out := appendEqualityDeletesAfter(nil, idx.global, dataSeqNum)
	out = appendEqualityDeletesAfter(out, partitionEntries, dataSeqNum)

	return out, nil
}

func appendEqualityDeletesAfter(
	out []iceberg.DataFile,
	entries []iceberg.ManifestEntry,
	dataSeqNum int64,
) []iceberg.DataFile {
	start := sort.Search(len(entries), func(i int) bool {
		return entries[i].SequenceNum() > dataSeqNum
	})
	for _, entry := range entries[start:] {
		out = append(out, entry.DataFile())
	}

	return out
}
