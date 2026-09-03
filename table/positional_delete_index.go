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
	"bytes"
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
	byPath      map[string][]deleteFileIndexEntry
	byPartition map[string][]deleteFileIndexEntry
}

func buildPositionalDeleteIndex(entries []iceberg.ManifestEntry) (*positionalDeleteIndex, error) {
	idx := &positionalDeleteIndex{}
	for _, entry := range entries {
		deleteFile := entry.DataFile()
		partition := dataFilePartition(deleteFile)
		if path := referencedDataFilePath(deleteFile); path != "" {
			indexedFile := compactDeleteFileForIndexWithReference(deleteFile, partition, nil, &path)
			if idx.byPath == nil {
				idx.byPath = make(map[string][]deleteFileIndexEntry)
			}
			idx.byPath[path] = append(idx.byPath[path], deleteFileIndexEntry{
				file: indexedFile, sequenceNum: entry.SequenceNum(),
			})

			continue
		}

		partitionKey, err := canonicalPartitionKey(deleteFile.SpecID(), partition)
		if err != nil {
			return nil, fmt.Errorf("indexing positional delete file %s: %w", deleteFile.FilePath(), err)
		}
		indexedFile := compactDeleteFileForIndex(deleteFile, partition, []int{filePathFieldID})
		if idx.byPartition == nil {
			idx.byPartition = make(map[string][]deleteFileIndexEntry)
		}
		idx.byPartition[partitionKey] = append(idx.byPartition[partitionKey], deleteFileIndexEntry{
			file: indexedFile, sequenceNum: entry.SequenceNum(),
		})
	}

	sortBySequence := func(entries []deleteFileIndexEntry) {
		slices.SortStableFunc(entries, func(a, b deleteFileIndexEntry) int {
			return cmp.Compare(a.sequenceNum, b.sequenceNum)
		})
	}
	for _, pathEntries := range idx.byPath {
		sortBySequence(pathEntries)
	}
	for _, partitionEntries := range idx.byPartition {
		sortBySequence(partitionEntries)
	}

	return idx, nil
}

// forDataFile returns positional deletes with a greater than or equal sequence
// number. Partition-scoped candidates are pruned using file_path metrics and
// returned before path-scoped deletes, matching Java's ordering.
func (idx *positionalDeleteIndex) forDataFile(dataEntry iceberg.ManifestEntry) ([]iceberg.DataFile, error) {
	if len(idx.byPath) == 0 && len(idx.byPartition) == 0 {
		return nil, nil
	}

	dataFile := dataEntry.DataFile()
	var partitionEntries []deleteFileIndexEntry
	if len(idx.byPartition) > 0 {
		partitionKey, err := canonicalPartitionKey(dataFile.SpecID(), dataFilePartition(dataFile))
		if err != nil {
			return nil, fmt.Errorf("matching positional deletes to data file %s: %w", dataFile.FilePath(), err)
		}
		partitionEntries = idx.byPartition[partitionKey]
	}

	dataSeqNum := dataEntry.SequenceNum()
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
	entries []deleteFileIndexEntry,
	dataSeqNum int64,
	dataFilePath string,
) ([]iceberg.DataFile, error) {
	start := sort.Search(len(entries), func(i int) bool {
		return entries[i].sequenceNum >= dataSeqNum
	})
	if start == len(entries) {
		return out, nil
	}

	for _, entry := range entries[start:] {
		deleteFile := entry.file
		if filePathMayMatch(deleteFile, dataFilePath) {
			out = append(out, deleteFile)
		}
	}

	return out, nil
}

// filePathMayMatch mirrors inclusive metrics evaluation for the required
// file_path field in a position-delete file. It only checks the bounds needed
// by the positional-delete index and keeps missing or malformed metadata
// conservative.
func filePathMayMatch(deleteFile iceberg.DataFile, dataFilePath string) bool {
	if deleteFile.Count() == 0 {
		return false
	}

	valueCounts, nullCounts, nanCounts, lowerBounds, upperBounds := dataFileStats(deleteFile)
	if valueCount, ok := valueCounts[filePathFieldID]; ok {
		if nullCount, ok := nullCounts[filePathFieldID]; ok && nullCount == valueCount {
			return false
		}
		if nanCount, ok := nanCounts[filePathFieldID]; ok && nanCount == valueCount {
			return false
		}
	}

	lower, hasLower := lowerBounds[filePathFieldID]
	upper, hasUpper := upperBounds[filePathFieldID]
	if !(hasLower && hasUpper && lower != nil && upper != nil && bytes.Compare(lower, upper) > 0) {
		if lower != nil && bytes.Compare(lower, []byte(dataFilePath)) > 0 {
			return false
		}
		if upper != nil && bytes.Compare(upper, []byte(dataFilePath)) < 0 {
			return false
		}
	}

	return true
}

func appendPositionalDeletesFromSequence(
	out []iceberg.DataFile,
	entries []deleteFileIndexEntry,
	dataSeqNum int64,
) []iceberg.DataFile {
	start := sort.Search(len(entries), func(i int) bool {
		return entries[i].sequenceNum >= dataSeqNum
	})
	for _, entry := range entries[start:] {
		out = append(out, entry.file)
	}

	return out
}
