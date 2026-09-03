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
	"encoding/json"
	"sync"
	"testing"

	"github.com/apache/iceberg-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Why: metadata lookups must use the derived ID index without changing the
// existing default-spec fallback or missing-ID behavior.
// Condition: metadata contains non-sequential partition spec IDs and the
// index is initialized from the same slice.
// Assertion: first, default, and missing lookups return the expected values.
func TestCommonMetadataPartitionSpecIndexLookups(t *testing.T) {
	specs := partitionSpecIndexTestSpecsAtLeast(partitionSpecIndexMinSize+8, 0, 7, 42)
	metadata := commonMetadata{
		Specs:              specs,
		DefaultSpecID:      42,
		partitionSpecIndex: buildPartitionSpecIndex(specs),
	}

	byID := metadata.PartitionSpecByID(7)
	require.NotNil(t, byID)
	assert.Equal(t, 7, byID.ID())

	defaultSpec := metadata.PartitionSpec()
	assert.Equal(t, 42, defaultSpec.ID())
	assert.Nil(t, metadata.PartitionSpecByID(99))
}

// Why: small metadata should not allocate an index that its linear lookup path
// will never read.
// Condition: parse a valid metadata document containing one partition spec.
// Assertion: the decoded common metadata keeps only the slice identity.
func TestParsedMetadataSkipsSmallPartitionSpecIndex(t *testing.T) {
	metadata, err := ParseMetadataBytes([]byte(ExampleTableMetadataV2))
	require.NoError(t, err)

	common := metadataCommon(metadata)
	require.Nil(t, common.partitionSpecIndex)
}

// Why: builders created from existing metadata should use the same small-slice
// lookup policy as parsed metadata.
// Condition: create a builder from parsed metadata and look up its final spec.
// Assertion: the lookup works without allocating a map for one spec.
func TestMetadataBuilderFromBaseSkipsSmallPartitionSpecIndex(t *testing.T) {
	metadata, err := ParseMetadataBytes([]byte(ExampleTableMetadataV2))
	require.NoError(t, err)

	builder, err := MetadataBuilderFromBase(metadata, "")
	require.NoError(t, err)
	require.Nil(t, builder.partitionSpecIndex)

	id := builder.specs[len(builder.specs)-1].ID()
	spec, err := builder.GetSpecByID(id)
	require.NoError(t, err)
	require.NotNil(t, spec)
	assert.Equal(t, id, spec.ID())
}

// Why: in-package fixtures can replace metadata slices directly, so a stale
// derived index must not return a spec at the old position or hide a new one.
// Condition: the indexed slice is replaced with another slice of equal length.
// Assertion: lookups use the replacement slice without mutating the cached
// index.
func TestCommonMetadataPartitionSpecIndexFallsBackAfterSliceReplacement(t *testing.T) {
	specs := partitionSpecIndexTestSpecsAtLeast(partitionSpecIndexMinSize+8, 1, 2)
	metadata := commonMetadata{
		Specs:              specs,
		DefaultSpecID:      2,
		partitionSpecIndex: buildPartitionSpecIndex(specs),
	}
	originalIndex := metadata.partitionSpecIndex

	metadata.Specs = partitionSpecIndexTestSpecsAtLeast(partitionSpecIndexMinSize+8, 3, 4)
	metadata.DefaultSpecID = 4
	byID := metadata.PartitionSpecByID(4)
	require.NotNil(t, byID)
	assert.Equal(t, 4, byID.ID())
	defaultSpec := metadata.PartitionSpec()
	assert.Equal(t, 4, defaultSpec.ID())
	assert.Nil(t, metadata.PartitionSpecByID(2))
	assert.Same(t, originalIndex, metadata.partitionSpecIndex)
	assert.Equal(t, 0, metadata.partitionSpecIndex.positions[1])
	assert.Equal(t, 1, metadata.partitionSpecIndex.positions[2])
}

func TestCommonMetadataPartitionSpecIndexFallsBackAfterSliceReplacementConcurrent(t *testing.T) {
	metadata := &commonMetadata{
		Specs:              partitionSpecIndexTestSpecsAtLeast(partitionSpecIndexMinSize+8, 1, 2),
		partitionSpecIndex: buildPartitionSpecIndex(partitionSpecIndexTestSpecsAtLeast(partitionSpecIndexMinSize+8, 1, 2)),
	}
	metadata.Specs = partitionSpecIndexTestSpecsAtLeast(partitionSpecIndexMinSize+8, 3, 4)
	originalIndex := metadata.partitionSpecIndex

	const goroutineCount = 8
	var wg sync.WaitGroup
	wg.Add(goroutineCount)
	for range goroutineCount {
		go func() {
			defer wg.Done()
			for range 100 {
				if spec := metadata.PartitionSpecByID(4); spec == nil || spec.ID() != 4 {
					t.Errorf("expected partition spec 4, got %v", spec)
				}
			}
		}()
	}
	wg.Wait()

	assert.Same(t, originalIndex, metadata.partitionSpecIndex)
}

func TestRejectsDuplicatePartitionSpecIDs(t *testing.T) {
	var raw map[string]json.RawMessage
	require.NoError(t, json.Unmarshal([]byte(ExampleTableMetadataV2), &raw))

	var specs []json.RawMessage
	require.NoError(t, json.Unmarshal(raw["partition-specs"], &specs))
	specs = append(specs, specs[0])
	encodedSpecs, err := json.Marshal(specs)
	require.NoError(t, err)
	raw["partition-specs"] = encodedSpecs
	data, err := json.Marshal(raw)
	require.NoError(t, err)

	_, err = ParseMetadataBytes(data)
	require.ErrorIs(t, err, ErrInvalidMetadata)
	assert.ErrorContains(t, err, "duplicate partition spec ID 0")
}

// Why: in-package fixtures can mutate an existing spec slice without changing
// its length or backing array, which cannot be detected by index metadata alone.
// Condition: a spec is replaced in place after the index is built.
// Assertion: lookup still finds the replacement and does not return the old ID.
func TestCommonMetadataPartitionSpecIndexFallsBackAfterElementMutation(t *testing.T) {
	specs := partitionSpecIndexTestSpecsAtLeast(partitionSpecIndexMinSize+8, 1, 2)
	oldID := specs[7].ID()
	metadata := commonMetadata{
		Specs:              specs,
		DefaultSpecID:      1,
		partitionSpecIndex: buildPartitionSpecIndex(specs),
	}

	specs[7] = iceberg.NewPartitionSpecID(3)

	byID := metadata.PartitionSpecByID(3)
	require.NotNil(t, byID)
	assert.Equal(t, 3, byID.ID())
	assert.Nil(t, metadata.PartitionSpecByID(oldID))
}

// Why: builders can also be used by package-level fixtures that replace their
// spec slice without updating derived state.
// Condition: an indexed builder receives a replacement slice of equal length.
// Assertion: GetSpecByID resolves the replacement slice and refreshes its index.
func TestMetadataBuilderPartitionSpecIndexFallsBackAfterSliceReplacement(t *testing.T) {
	specs := partitionSpecIndexTestSpecsAtLeast(partitionSpecIndexMinSize+8, 1, 2)
	builder := MetadataBuilder{
		specs:              specs,
		partitionSpecIndex: buildPartitionSpecIndex(specs),
	}
	originalIndex := builder.partitionSpecIndex

	builder.specs = partitionSpecIndexTestSpecsAtLeast(partitionSpecIndexMinSize+8, 3, 4)
	byID, err := builder.GetSpecByID(4)
	require.NoError(t, err)
	require.NotNil(t, byID)
	assert.Equal(t, 4, byID.ID())
	_, err = builder.GetSpecByID(2)
	assert.ErrorIs(t, err, ErrPartitionSpecNotFound)
	assert.NotSame(t, originalIndex, builder.partitionSpecIndex)
	assert.Equal(t, 0, builder.partitionSpecIndex.positions[3])
	assert.Equal(t, 1, builder.partitionSpecIndex.positions[4])
	assert.Equal(t, 0, originalIndex.positions[1])
	assert.Equal(t, 1, originalIndex.positions[2])
}

// Why: builder updates and clones must keep the spec index aligned without
// sharing mutable lookup state with a built metadata value or sibling builder.
// Condition: add a spec, clone the builder, add another spec to the clone, and
// remove the first added spec from the original.
// Assertion: each builder resolves the IDs in its own current spec slice.
func TestMetadataBuilderPartitionSpecIndexFollowsUpdates(t *testing.T) {
	builder := builderWithoutChanges(2)
	for id := 1; len(builder.specs) < partitionSpecIndexMinSize; id++ {
		builder.specs = append(builder.specs, iceberg.NewPartitionSpecID(id))
	}
	builder.partitionSpecIndex = buildPartitionSpecIndex(builder.specs)
	require.Len(t, builder.specs, partitionSpecIndexMinSize)
	require.Equal(t, 0, builder.partitionSpecIndex.positions[0])

	added := iceberg.NewPartitionSpecID(99, iceberg.PartitionField{
		SourceIDs: []int{1}, Name: "x", Transform: iceberg.IdentityTransform{},
	})
	require.NoError(t, builder.AddPartitionSpec(&added, false))
	require.Equal(t, partitionSpecIndexMinSize, builder.partitionSpecIndex.positions[partitionSpecIndexMinSize])

	got, err := builder.GetSpecByID(partitionSpecIndexMinSize)
	require.NoError(t, err)
	require.NotNil(t, got)
	assert.Equal(t, partitionSpecIndexMinSize, got.ID())

	clone := builder.clone()
	cloneAdded := iceberg.NewPartitionSpecID(99, iceberg.PartitionField{
		SourceIDs: []int{3}, Name: "z", Transform: iceberg.IdentityTransform{},
	})
	require.NoError(t, clone.AddPartitionSpec(&cloneAdded, false))
	assert.NotContains(t, builder.partitionSpecIndex.positions, partitionSpecIndexMinSize+1)
	assert.Equal(t, partitionSpecIndexMinSize+1, clone.partitionSpecIndex.positions[partitionSpecIndexMinSize+1])

	require.NoError(t, builder.RemovePartitionSpecs([]int{1}))
	assert.NotContains(t, builder.partitionSpecIndex.positions, 1)
	_, err = builder.GetSpecByID(1)
	assert.ErrorIs(t, err, ErrPartitionSpecNotFound)
	got, err = clone.GetSpecByID(1)
	require.NoError(t, err)
	assert.Equal(t, 1, got.ID())
}

func TestMetadataBuilderPartitionSpecIndexBuildsAtThreshold(t *testing.T) {
	builder := builderWithoutChanges(2)
	for id := 1; len(builder.specs) < partitionSpecIndexMinSize-1; id++ {
		builder.specs = append(builder.specs, iceberg.NewPartitionSpecID(id))
	}
	builder.partitionSpecIndex = buildPartitionSpecIndex(builder.specs)
	require.Nil(t, builder.partitionSpecIndex)

	added := iceberg.NewPartitionSpecID(99, iceberg.PartitionField{
		SourceIDs: []int{1}, Name: "threshold", Transform: iceberg.IdentityTransform{},
	})
	require.NoError(t, builder.AddPartitionSpec(&added, false))
	require.NotNil(t, builder.partitionSpecIndex.positions)
	assert.Equal(t, partitionSpecIndexMinSize-1,
		builder.partitionSpecIndex.positions[partitionSpecIndexMinSize-1])
}

// Why: removing unknown IDs is a no-op and must not leave the derived index
// pointing at a newly allocated but unindexed slice.
// Condition: remove an ID that is not present in the builder.
// Assertion: the specs slice and its index remain unchanged.
func TestMetadataBuilderRemoveUnknownPartitionSpecKeepsIndex(t *testing.T) {
	builder := builderWithoutChanges(2)
	originalIndex := builder.partitionSpecIndex
	originalFirst := &builder.specs[0]

	require.NoError(t, builder.RemovePartitionSpecs([]int{99}))
	assert.Same(t, originalIndex, builder.partitionSpecIndex)
	assert.Same(t, originalFirst, &builder.specs[0])
}

// Why: a built metadata value shares the derived index with its builder until
// the builder mutates its spec list.
// Condition: build metadata, then add a new partition spec to the builder.
// Assertion: the builder sees the new spec while the already-built metadata
// remains unchanged.
func TestMetadataBuilderPartitionSpecIndexIsolatedFromBuiltMetadata(t *testing.T) {
	builder := builderWithoutChanges(2)
	for id := 1; len(builder.specs) < partitionSpecIndexMinSize; id++ {
		builder.specs = append(builder.specs, iceberg.NewPartitionSpecID(id))
	}
	builder.partitionSpecIndex = buildPartitionSpecIndex(builder.specs)
	metadata, err := builder.Build()
	require.NoError(t, err)
	common := metadataCommon(metadata)
	originalIndex := common.partitionSpecIndex

	added := iceberg.NewPartitionSpecID(99, iceberg.PartitionField{
		SourceIDs: []int{3}, Name: "z", Transform: iceberg.IdentityTransform{},
	})
	require.NoError(t, builder.AddPartitionSpec(&added, false))

	assert.NotSame(t, originalIndex, builder.partitionSpecIndex)
	assert.NotContains(t, originalIndex.positions, partitionSpecIndexMinSize)
	assert.Contains(t, builder.partitionSpecIndex.positions, partitionSpecIndexMinSize)
	assert.Nil(t, common.PartitionSpecByID(partitionSpecIndexMinSize))
	got, err := builder.GetSpecByID(1)
	require.NoError(t, err)
	assert.Equal(t, 1, got.ID())
}

func TestCommonMetadataPartitionSpecLookupsConcurrent(t *testing.T) {
	specs := partitionSpecIndexTestSpecs(1, 2)
	metadata := &commonMetadata{
		Specs:              specs,
		DefaultSpecID:      2,
		partitionSpecIndex: buildPartitionSpecIndex(specs),
	}

	const (
		goroutineCount = 8
		lookupCount    = 100
	)

	var wg sync.WaitGroup
	wg.Add(goroutineCount)
	for i := range goroutineCount {
		go func(i int) {
			defer wg.Done()

			for range lookupCount {
				if i%2 == 0 {
					spec := metadata.PartitionSpecByID(1)
					if spec == nil || spec.ID() != 1 {
						t.Errorf("expected partition spec 1, got %v", spec)

						return
					}

					continue
				}

				spec := metadata.PartitionSpec()
				if spec.ID() != 2 {
					t.Errorf("expected default partition spec 2, got %d", spec.ID())

					return
				}
			}
		}(i)
	}
	wg.Wait()
}

func TestMetadataBuilderPartitionSpecIndexCopyOnWriteConcurrent(t *testing.T) {
	builderValue := builderWithoutChanges(2)
	const indexedSpecCount = 1_024
	for id := 1; id < indexedSpecCount; id++ {
		builderValue.specs = append(builderValue.specs, iceberg.NewPartitionSpecID(id))
	}
	builderValue.partitionSpecIndex = buildPartitionSpecIndex(builderValue.specs)

	metadata, err := builderValue.Build()
	require.NoError(t, err)
	common := metadataCommon(metadata)
	builder := &builderValue

	const lookupCount = 1_000

	start := make(chan struct{})
	readerStarted := make(chan struct{})
	writerErr := make(chan error, 1)
	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		<-start

		for i := range lookupCount {
			spec := common.PartitionSpecByID(0)
			if spec == nil || spec.ID() != 0 {
				t.Errorf("expected partition spec 0, got %v", spec)

				return
			}
			if i == 0 {
				close(readerStarted)
			}
		}
	}()
	go func() {
		defer wg.Done()
		<-start
		<-readerStarted

		added := iceberg.NewPartitionSpecID(99, iceberg.PartitionField{
			SourceIDs: []int{1}, Name: "copy-on-write", Transform: iceberg.IdentityTransform{},
		})
		writerErr <- builder.AddPartitionSpec(&added, false)
	}()
	close(start)
	wg.Wait()
	require.NoError(t, <-writerErr)
	assert.NotSame(t, common.partitionSpecIndex, builder.partitionSpecIndex)
	assert.NotContains(t, common.partitionSpecIndex.positions, indexedSpecCount)
	assert.Contains(t, builder.partitionSpecIndex.positions, indexedSpecCount)
}

func partitionSpecIndexTestSpecs(ids ...int) []iceberg.PartitionSpec {
	specs := make([]iceberg.PartitionSpec, len(ids))
	for i, id := range ids {
		specs[i] = iceberg.NewPartitionSpecID(id)
	}

	return specs
}

func partitionSpecIndexTestSpecsAtLeast(count int, ids ...int) []iceberg.PartitionSpec {
	specs := partitionSpecIndexTestSpecs(ids...)
	for id := 1_000; len(specs) < count; id++ {
		specs = append(specs, iceberg.NewPartitionSpecID(id))
	}

	return specs
}
