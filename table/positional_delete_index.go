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
	"slices"
	"sort"

	"github.com/apache/iceberg-go"
)

// positionalDeleteIndex groups file-scoped deletes by referenced data path
// and all remaining deletes by partition. Each bucket is sequence-sorted so a
// data-file lookup only visits deletes that can apply to that file.
type positionalDeleteIndex struct {
	byPath             map[string][]iceberg.ManifestEntry
	byPartition        map[string][]positionalPartitionDeleteEntry
	byIntegerPartition map[positionalIntegerPartitionKey][]positionalPartitionDeleteEntry
}

// positionalPartitionDeleteEntry caches the only path statistics needed for
// matching a partition-scoped delete against many data files. Own the bound
// strings rather than retaining borrowed DataFile metric byte slices.
type positionalPartitionDeleteEntry struct {
	entry       iceberg.ManifestEntry
	pathMetrics positionalDeletePathMetrics
}

type positionalDeletePathMetrics struct {
	lower    string
	upper    string
	hasUpper bool
	skip     bool
}

func newPositionalPartitionDeleteEntry(entry iceberg.ManifestEntry) positionalPartitionDeleteEntry {
	return positionalPartitionDeleteEntry{
		entry:       entry,
		pathMetrics: positionalDeletePathMetricsFor(entry.DataFile()),
	}
}

func positionalDeletePathMetricsFor(deleteFile iceberg.DataFile) positionalDeletePathMetrics {
	if deleteFile.Count() == 0 {
		return positionalDeletePathMetrics{skip: true}
	}

	valueCounts, nullCounts, nanCounts, lowerBounds, upperBounds := dataFileStats(deleteFile)
	if valueCount, ok := valueCounts[filePathFieldID]; ok {
		if nullCount, ok := nullCounts[filePathFieldID]; ok && nullCount == valueCount {
			return positionalDeletePathMetrics{skip: true}
		}
		if nanCount, ok := nanCounts[filePathFieldID]; ok && nanCount == valueCount {
			return positionalDeletePathMetrics{skip: true}
		}
	}

	var metrics positionalDeletePathMetrics
	if lower := lowerBounds[filePathFieldID]; lower != nil {
		metrics.lower = string(lower)
	}
	if upper := upperBounds[filePathFieldID]; upper != nil {
		metrics.upper = string(upper)
		metrics.hasUpper = true
	}

	return metrics
}

func (m positionalDeletePathMetrics) mayMatch(path string) bool {
	return !m.skip && m.lower <= path && (!m.hasUpper || path <= m.upper)
}

// Most integer-backed partition transforms (bucket, day, hour, identity)
// need only a small comparable key, not a sorted, encoded partition tuple.
// Use the same integer normalization as canonicalPartitionKey so decoders
// returning int, int32, int64, or Iceberg temporal types still match.
type positionalIntegerPartitionKey struct {
	specID  int32
	fieldID int
	value   int64
}

func singleIntegerPartitionKey(specID int32, partition map[int]any) (positionalIntegerPartitionKey, bool) {
	if len(partition) != 1 {
		return positionalIntegerPartitionKey{}, false
	}

	for fieldID, value := range partition {
		var number int64
		switch value := value.(type) {
		case int:
			number = int64(value)
		case int32:
			number = int64(value)
		case int64:
			number = value
		case iceberg.Date:
			number = int64(value)
		case iceberg.Time:
			number = int64(value)
		case iceberg.Timestamp:
			number = int64(value)
		case iceberg.TimestampNano:
			number = int64(value)
		default:
			return positionalIntegerPartitionKey{}, false
		}

		return positionalIntegerPartitionKey{specID: specID, fieldID: fieldID, value: number}, true
	}

	return positionalIntegerPartitionKey{}, false
}

func buildPositionalDeleteIndex(entries []iceberg.ManifestEntry) (*positionalDeleteIndex, error) {
	idx := &positionalDeleteIndex{}
	for _, entry := range entries {
		deleteFile := entry.DataFile()
		if path := referencedDataFilePath(deleteFile); path != "" {
			if idx.byPath == nil {
				idx.byPath = make(map[string][]iceberg.ManifestEntry)
			}
			idx.byPath[path] = append(idx.byPath[path], entry)

			continue
		}

		indexed := newPositionalPartitionDeleteEntry(entry)
		partition := dataFilePartition(deleteFile)
		if key, ok := singleIntegerPartitionKey(deleteFile.SpecID(), partition); ok {
			if idx.byIntegerPartition == nil {
				idx.byIntegerPartition = make(map[positionalIntegerPartitionKey][]positionalPartitionDeleteEntry)
			}
			idx.byIntegerPartition[key] = append(idx.byIntegerPartition[key], indexed)

			continue
		}

		partitionKey, err := canonicalPartitionKey(deleteFile.SpecID(), partition)
		if err != nil {
			return nil, fmt.Errorf("indexing positional delete file %s: %w", deleteFile.FilePath(), err)
		}
		if idx.byPartition == nil {
			idx.byPartition = make(map[string][]positionalPartitionDeleteEntry)
		}
		idx.byPartition[partitionKey] = append(idx.byPartition[partitionKey], indexed)
	}

	sortBySequence := func(entries []iceberg.ManifestEntry) {
		slices.SortStableFunc(entries, func(a, b iceberg.ManifestEntry) int {
			return cmp.Compare(a.DataSequenceNumber(), b.DataSequenceNumber())
		})
	}
	for _, pathEntries := range idx.byPath {
		sortBySequence(pathEntries)
	}
	sortPartitionBySequence := func(entries []positionalPartitionDeleteEntry) {
		slices.SortStableFunc(entries, func(a, b positionalPartitionDeleteEntry) int {
			return cmp.Compare(a.entry.DataSequenceNumber(), b.entry.DataSequenceNumber())
		})
	}
	for _, partitionEntries := range idx.byPartition {
		sortPartitionBySequence(partitionEntries)
	}
	for _, partitionEntries := range idx.byIntegerPartition {
		sortPartitionBySequence(partitionEntries)
	}

	return idx, nil
}

// forDataFile returns positional deletes with a greater than or equal sequence
// number. Partition-scoped candidates are pruned using file_path metrics and
// returned before path-scoped deletes, matching Java's ordering.
func (idx *positionalDeleteIndex) forDataFile(dataEntry iceberg.ManifestEntry) ([]iceberg.DataFile, error) {
	if len(idx.byPath) == 0 && len(idx.byPartition) == 0 && len(idx.byIntegerPartition) == 0 {
		return nil, nil
	}

	dataFile := dataEntry.DataFile()
	var partitionEntries []positionalPartitionDeleteEntry
	if len(idx.byPartition) > 0 || len(idx.byIntegerPartition) > 0 {
		partition := dataFilePartition(dataFile)
		if key, ok := singleIntegerPartitionKey(dataFile.SpecID(), partition); ok {
			partitionEntries = idx.byIntegerPartition[key]
		} else if len(idx.byPartition) > 0 {
			partitionKey, err := canonicalPartitionKey(dataFile.SpecID(), partition)
			if err != nil {
				return nil, fmt.Errorf("matching positional deletes to data file %s: %w", dataFile.FilePath(), err)
			}
			partitionEntries = idx.byPartition[partitionKey]
		}
	}

	dataSeqNum := dataEntry.DataSequenceNumber()
	out, err := appendPartitionDeletesFromSequence(
		nil, partitionEntries, dataSeqNum, dataFile.FilePath())
	if err != nil {
		return nil, err
	}
	out = appendPositionalDeletesFromSequence(
		out, idx.byPath[dataFile.FilePath()], dataSeqNum)

	return out, nil
}

func appendPartitionDeletesFromSequence(
	out []iceberg.DataFile,
	entries []positionalPartitionDeleteEntry,
	dataSeqNum int64,
	dataFilePath string,
) ([]iceberg.DataFile, error) {
	start := sort.Search(len(entries), func(i int) bool {
		return entries[i].entry.DataSequenceNumber() >= dataSeqNum
	})
	if start == len(entries) {
		return out, nil
	}

	for _, indexed := range entries[start:] {
		if indexed.pathMetrics.mayMatch(dataFilePath) {
			out = append(out, indexed.entry.DataFile())
		}
	}

	return out, nil
}

// filePathMayMatch mirrors inclusive metrics evaluation for the required
// file_path field in a position-delete file. It only checks the bounds needed
// by the positional-delete index and keeps missing or malformed metadata
// conservative.
func filePathMayMatch(deleteFile iceberg.DataFile, dataFilePath string) bool {
	return positionalDeletePathMetricsFor(deleteFile).mayMatch(dataFilePath)
}

func appendPositionalDeletesFromSequence(
	out []iceberg.DataFile,
	entries []iceberg.ManifestEntry,
	dataSeqNum int64,
) []iceberg.DataFile {
	start := sort.Search(len(entries), func(i int) bool {
		return entries[i].DataSequenceNumber() >= dataSeqNum
	})
	for _, entry := range entries[start:] {
		out = append(out, entry.DataFile())
	}

	return out
}
