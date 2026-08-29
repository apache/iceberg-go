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

	"github.com/apache/iceberg-go"
	iceberginternal "github.com/apache/iceberg-go/internal"
)

// deleteFileIndexEntry is the part of a manifest entry used after delete
// manifests have been read. Keeping the sequence number separate lets the
// index release the full ManifestEntry, including its unused metadata maps.
type deleteFileIndexEntry struct {
	file        iceberg.DataFile
	sequenceNum int64
}

// compactDeleteFileForIndex copies a delete file without retaining the wide
// metadata maps that are not needed by scan planning or delete-file readers.
// The returned file still carries all non-statistics metadata exposed by the
// DataFile interface, plus only the selected statistics fields.
//
// Positional-delete indexes select file_pathFieldID because partition-scoped
// position deletes use those bounds for candidate pruning. Equality-delete
// indexes select their equality field IDs. Deletion vectors select no stats.
func compactDeleteFileForIndex(
	file iceberg.DataFile,
	partition map[int]any,
	statFieldIDs []int,
) (iceberg.DataFile, error) {
	partitionSpec := syntheticPartitionSpec(file.SpecID(), partition)
	builder, err := iceberg.NewDataFileBuilder(
		partitionSpec,
		file.ContentType(),
		file.FilePath(),
		file.FileFormat(),
		partition,
		nil,
		nil,
		file.Count(),
		file.FileSizeBytes(),
	)
	if err != nil {
		// Keep the existing file for malformed or external metadata. Index
		// construction historically accepted such DataFile implementations;
		// valid manifest files take the compact path above without changing the
		// error behavior of the surrounding planner.
		return file, nil
	}

	valueCounts, nullCounts, nanCounts, lowerBounds, upperBounds := dataFileStatsForFields(file, statFieldIDs)
	if valueCounts != nil {
		builder.ValueCounts(valueCounts)
	}
	if nullCounts != nil {
		builder.NullValueCounts(nullCounts)
	}
	if nanCounts != nil {
		builder.NaNValueCounts(nanCounts)
	}
	if lowerBounds != nil {
		builder.LowerBoundValues(lowerBounds)
	}
	if upperBounds != nil {
		builder.UpperBoundValues(upperBounds)
	}

	// Read these small non-statistics fields directly. The borrowed collection
	// helper also exposes column sizes and the built-in implementation lazily
	// initializes every statistics map while preparing that view.
	keyMetadata := file.KeyMetadata()
	splitOffsets := file.SplitOffsets()
	equalityFieldIDs := file.EqualityFieldIDs()
	if keyMetadata != nil {
		builder.KeyMetadata(keyMetadata)
	}
	if splitOffsets != nil {
		builder.SplitOffsets(splitOffsets)
	}
	if equalityFieldIDs != nil {
		builder.EqualityFieldIDs(equalityFieldIDs)
	}

	sortOrderID, firstRowID, referencedDataFile, contentOffset, contentSize := iceberginternal.BorrowedDataFilePointers(file)
	if sortOrderID != nil {
		builder.SortOrderID(*sortOrderID)
	}
	if firstRowID != nil {
		builder.FirstRowID(*firstRowID)
	}
	if referencedDataFile != nil {
		builder.ReferencedDataFile(*referencedDataFile)
	}
	if contentOffset != nil {
		builder.ContentOffset(*contentOffset)
	}
	if contentSize != nil {
		builder.ContentSizeInBytes(*contentSize)
	}

	return builder.Build(), nil
}

// syntheticPartitionSpec gives the built-in DataFile implementation enough
// field metadata to expose the copied partition map and to remain usable by
// the DataFile codec. Partition values are already transformed values, so the
// identity transforms here are only a local storage description.
func syntheticPartitionSpec(specID int32, partition map[int]any) iceberg.PartitionSpec {
	fields := make([]iceberg.PartitionField, 0, len(partition))
	for fieldID := range partition {
		fields = append(fields, iceberg.PartitionField{
			SourceIDs: []int{fieldID},
			FieldID:   fieldID,
			Name:      fmt.Sprintf("partition_%d", fieldID),
			Transform: iceberg.IdentityTransform{},
		})
	}
	slices.SortFunc(fields, func(a, b iceberg.PartitionField) int {
		return cmp.Compare(a.FieldID, b.FieldID)
	})

	return iceberg.NewPartitionSpecID(int(specID), fields...)
}

func dataFileStatsForFields(
	file iceberg.DataFile,
	fieldIDs []int,
) (
	valueCounts map[int]int64,
	nullCounts map[int]int64,
	nanCounts map[int]int64,
	lowerBounds map[int][]byte,
	upperBounds map[int][]byte,
) {
	if len(fieldIDs) == 0 {
		return nil, nil, nil, nil, nil
	}

	fieldSet := make(map[int]struct{}, len(fieldIDs))
	for _, fieldID := range fieldIDs {
		fieldSet[fieldID] = struct{}{}
	}
	allValueCounts, allNullCounts, allNaNCounts, allLowerBounds, allUpperBounds := dataFileStats(file)
	valueCounts = selectInt64Stats(allValueCounts, fieldSet)
	nullCounts = selectInt64Stats(allNullCounts, fieldSet)
	nanCounts = selectInt64Stats(allNaNCounts, fieldSet)
	lowerBounds = selectByteStats(allLowerBounds, fieldSet)
	upperBounds = selectByteStats(allUpperBounds, fieldSet)

	return valueCounts, nullCounts, nanCounts, lowerBounds, upperBounds
}

func selectInt64Stats(source map[int]int64, fields map[int]struct{}) map[int]int64 {
	if source == nil {
		return nil
	}

	selected := make(map[int]int64, len(fields))
	for fieldID := range fields {
		if value, ok := source[fieldID]; ok {
			selected[fieldID] = value
		}
	}
	if len(selected) == 0 {
		return nil
	}

	return selected
}

func selectByteStats(source map[int][]byte, fields map[int]struct{}) map[int][]byte {
	if source == nil {
		return nil
	}

	selected := make(map[int][]byte, len(fields))
	for fieldID := range fields {
		if value, ok := source[fieldID]; ok {
			selected[fieldID] = slices.Clone(value)
		}
	}
	if len(selected) == 0 {
		return nil
	}

	return selected
}
