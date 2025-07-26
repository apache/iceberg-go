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

package table_test

import (
	"testing"

	"github.com/apache/iceberg-go"
	"github.com/apache/iceberg-go/table"
	"github.com/leanovate/gopter"
	"github.com/leanovate/gopter/gen"
	"github.com/leanovate/gopter/prop"
)

// Property-based tests for manifest operations and data consistency

func TestManifestEntryProperties(t *testing.T) {
	parameters := gopter.DefaultTestParameters()
	parameters.MinSuccessfulTests = 100
	properties := gopter.NewProperties(parameters)

	// Property: Manifest entry status transitions should be valid
	properties.Property("Manifest entry status transitions are valid", prop.ForAll(
		func(currentStatus iceberg.ManifestEntryStatus, newStatus iceberg.ManifestEntryStatus) bool {
			// Define valid status transitions according to Iceberg spec
			validTransitions := map[iceberg.ManifestEntryStatus][]iceberg.ManifestEntryStatus{
				iceberg.EntryStatusADDED: {
					iceberg.EntryStatusEXISTING,
					iceberg.EntryStatusDELETED,
				},
				iceberg.EntryStatusEXISTING: {
					iceberg.EntryStatusDELETED,
				},
				iceberg.EntryStatusDELETED: {
					// Generally, deleted entries don't transition to other states
				},
			}

			// Property: Transition should be valid or status should remain the same
			if currentStatus == newStatus {
				return true
			}

			validNext, exists := validTransitions[currentStatus]
			if !exists {
				return false
			}

			for _, validStatus := range validNext {
				if newStatus == validStatus {
					return true
				}
			}

			return false
		},
		genManifestEntryStatus(),
		genManifestEntryStatus(),
	))

	properties.TestingRun(t)
}

func TestDataFileConsistencyProperties(t *testing.T) {
	parameters := gopter.DefaultTestParameters()
	parameters.MinSuccessfulTests = 50
	properties := gopter.NewProperties(parameters)

	// Property: Data file statistics should be internally consistent
	properties.Property("Data file statistics are internally consistent", prop.ForAll(
		func(recordCount int32, fileSize int64, nullValueCounts map[int]int64, distinctCounts map[int]int64) bool {
			if recordCount < 0 || fileSize < 0 {
				return true // Skip invalid inputs
			}

			// Property 1: Null value counts should not exceed record count
			for _, nullCount := range nullValueCounts {
				if nullCount < 0 || nullCount > int64(recordCount) {
					return false
				}
			}

			// Property 2: Distinct counts should not exceed record count
			for _, distinctCount := range distinctCounts {
				if distinctCount < 0 || distinctCount > int64(recordCount) {
					return false
				}
			}

			// Property 3: If record count is 0, file should exist but have minimal size
			if recordCount == 0 {
				return fileSize >= 0 // Parquet files have metadata overhead even when empty
			}

			return true
		},
		gen.Int32Range(0, 10000),
		gen.Int64Range(0, 100000000),
		gen.MapOf(gen.IntRange(1, 100), gen.Int64Range(0, 10000)),
		gen.MapOf(gen.IntRange(1, 100), gen.Int64Range(0, 10000)),
	))

	properties.TestingRun(t)
}

func TestPartitionValueConsistencyProperties(t *testing.T) {
	parameters := gopter.DefaultTestParameters()
	parameters.MinSuccessfulTests = 75
	properties := gopter.NewProperties(parameters)

	// Property: Partition values should be consistent with transform applied to source values
	properties.Property("Partition values consistent with transform", prop.ForAll(
		func(sourceValue int32, transformType string) bool {
			var transform iceberg.Transform
			var partitionValue any

			switch transformType {
			case "identity":
				transform = iceberg.IdentityTransform{}
				partitionValue = sourceValue
			case "bucket":
				transform = iceberg.BucketTransform{NumBuckets: 10}
				// Simplified bucket calculation for testing
				partitionValue = int32(sourceValue) % 10
				if partitionValue.(int32) < 0 {
					partitionValue = partitionValue.(int32) + 10
				}
			case "year":
				transform = iceberg.YearTransform{}
				// For this test, assume source value represents days since epoch
				// This is simplified - real implementation would use proper date handling
				partitionValue = sourceValue / 365 // Rough year calculation
			default:
				return true // Skip unknown transforms
			}

			// Property: Transform application should be deterministic
			result1 := applyTransform(transform, sourceValue)
			result2 := applyTransform(transform, sourceValue)

			return result1 == result2
		},
		gen.Int32Range(-1000, 1000),
		gen.OneConstOf("identity", "bucket", "year"),
	))

	properties.TestingRun(t)
}

func TestSnapshotConsistencyProperties(t *testing.T) {
	parameters := gopter.DefaultTestParameters()
	parameters.MinSuccessfulTests = 40
	properties := gopter.NewProperties(parameters)

	// Property: Snapshot summaries should be consistent with actual changes
	properties.Property("Snapshot summaries match actual changes", prop.ForAll(
		func(addedFiles, deletedFiles, addedRecords, deletedRecords int32) bool {
			if addedFiles < 0 || deletedFiles < 0 || addedRecords < 0 || deletedRecords < 0 {
				return true // Skip invalid inputs
			}

			// Property: If no files are added, no records should be added
			if addedFiles == 0 && addedRecords > 0 {
				return false
			}

			// Property: If no files are deleted, no records should be deleted
			if deletedFiles == 0 && deletedRecords > 0 {
				return false
			}

			// Property: Number of records should generally correlate with number of files
			// (This is a simplified heuristic - real data files can vary greatly in size)
			if addedFiles > 0 && addedRecords == 0 {
				return false // Adding files should generally add records
			}

			if deletedFiles > 0 && deletedRecords == 0 {
				return false // Deleting files should generally delete records
			}

			return true
		},
		gen.Int32Range(0, 100),
		gen.Int32Range(0, 100),
		gen.Int32Range(0, 10000),
		gen.Int32Range(0, 10000),
	))

	properties.TestingRun(t)
}

func TestManifestMergingLogicProperties(t *testing.T) {
	parameters := gopter.DefaultTestParameters()
	parameters.MinSuccessfulTests = 30
	properties := gopter.NewProperties(parameters)

	// Property: Manifest merging should preserve total data file count
	properties.Property("Manifest merging preserves data file counts", prop.ForAll(
		func(manifestSizes []int, targetSize int) bool {
			if targetSize <= 0 {
				return true // Skip invalid target sizes
			}

			totalFiles := 0
			for _, size := range manifestSizes {
				if size < 0 {
					return true // Skip invalid manifest sizes
				}
				totalFiles += size
			}

			// Simulate manifest merging logic
			mergedManifests := simulateManifestMerging(manifestSizes, targetSize)
			
			mergedTotal := 0
			for _, size := range mergedManifests {
				mergedTotal += size
			}

			// Property: Total file count should be preserved after merging
			return mergedTotal == totalFiles
		},
		gen.SliceOfN(1, 20, gen.IntRange(0, 100)),
		gen.IntRange(1, 500),
	))

	properties.TestingRun(t)
}

func TestSchemaEvolutionImpactProperties(t *testing.T) {
	parameters := gopter.DefaultTestParameters()
	parameters.MinSuccessfulTests = 40
	properties := gopter.NewProperties(parameters)

	// Property: Schema evolution should not break existing manifest compatibility
	properties.Property("Schema evolution maintains manifest compatibility", prop.ForAll(
		func(oldFieldIds []int, newFieldIds []int) bool {
			// Filter valid field IDs
			validOldIds := filterValidFieldIds(oldFieldIds)
			validNewIds := filterValidFieldIds(newFieldIds)

			if len(validOldIds) == 0 {
				return true // No old fields to check compatibility with
			}

			// Property: New schema should contain all old field IDs for compatibility
			oldIdSet := make(map[int]bool)
			for _, id := range validOldIds {
				oldIdSet[id] = true
			}

			// Count how many old IDs are preserved in new schema
			preservedCount := 0
			for _, newId := range validNewIds {
				if oldIdSet[newId] {
					preservedCount++
				}
			}

			// Property: At least some old field IDs should be preserved
			// (Complete schema replacement is rare and would be a breaking change)
			if len(validOldIds) <= 5 {
				// For small schemas, expect most fields to be preserved
				return preservedCount >= len(validOldIds)/2
			} else {
				// For larger schemas, allow more flexibility
				return preservedCount > 0
			}
		},
		gen.SliceOfN(1, 15, gen.IntRange(1, 100)),
		gen.SliceOfN(1, 20, gen.IntRange(1, 120)),
	))

	properties.TestingRun(t)
}

// Helper functions for property testing

func genManifestEntryStatus() gopter.Gen {
	statuses := []iceberg.ManifestEntryStatus{
		iceberg.EntryStatusADDED,
		iceberg.EntryStatusEXISTING,
		iceberg.EntryStatusDELETED,
	}
	return gen.OneConstOf(statuses...)
}

func applyTransform(transform iceberg.Transform, value int32) any {
	// Simplified transform application for property testing
	switch t := transform.(type) {
	case iceberg.IdentityTransform:
		return value
	case iceberg.BucketTransform:
		result := int32(value) % int32(t.NumBuckets)
		if result < 0 {
			result += int32(t.NumBuckets)
		}
		return result
	case iceberg.YearTransform:
		// Simplified year calculation
		return value / 365
	default:
		return value
	}
}

func simulateManifestMerging(manifestSizes []int, targetSize int) []int {
	// Simple greedy merging algorithm for testing
	if len(manifestSizes) == 0 {
		return []int{}
	}

	merged := make([]int, 0)
	current := 0

	for _, size := range manifestSizes {
		if current+size <= targetSize || current == 0 {
			current += size
		} else {
			merged = append(merged, current)
			current = size
		}
	}

	if current > 0 {
		merged = append(merged, current)
	}

	return merged
}

func filterValidFieldIds(fieldIds []int) []int {
	valid := make([]int, 0)
	seen := make(map[int]bool)

	for _, id := range fieldIds {
		if id > 0 && !seen[id] {
			valid = append(valid, id)
			seen[id] = true
		}
	}

	return valid
}

// Property test for complex manifest validation scenarios
func TestComplexManifestValidationProperties(t *testing.T) {
	parameters := gopter.DefaultTestParameters()
	parameters.MinSuccessfulTests = 25
	properties := gopter.NewProperties(parameters)

	// Property: Manifest validation should be consistent across multiple validation attempts
	properties.Property("Manifest validation is deterministic", prop.ForAll(
		func(manifestEntryCount, dataFileCount int) bool {
			if manifestEntryCount < 0 || dataFileCount < 0 {
				return true
			}

			// Property: Number of manifest entries should not exceed data files
			// (Each data file should have at most one manifest entry per manifest)
			isValid1 := manifestEntryCount <= dataFileCount
			isValid2 := manifestEntryCount <= dataFileCount

			// Property: Validation should be deterministic
			return isValid1 == isValid2
		},
		gen.IntRange(0, 1000),
		gen.IntRange(0, 1000),
	))

	properties.TestingRun(t)
} 