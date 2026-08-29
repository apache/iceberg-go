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
	"sync"
	"testing"

	"github.com/apache/iceberg-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestCommonMetadataSchemaIndexLookups(t *testing.T) {
	schemas := schemaIndexTestSchemas(10, 20, 42)
	metadata := commonMetadata{
		SchemaList:      schemas,
		CurrentSchemaID: 42,
		schemaIndex:     buildSchemaIndex(schemas),
	}

	got := metadata.schemaByID(20)
	require.NotNil(t, got)
	assert.Equal(t, 20, got.ID)

	current := metadata.CurrentSchema()
	require.NotNil(t, current)
	assert.Equal(t, 42, current.ID)
	assert.Nil(t, metadata.schemaByID(99))
}

func TestParsedMetadataBuildsSchemaIndex(t *testing.T) {
	metadata, err := ParseMetadataBytes([]byte(ExampleTableMetadataV2))
	require.NoError(t, err)

	common := metadataCommon(metadata)
	require.Len(t, common.schemaIndex.schemas, len(common.SchemaList))
	for _, schema := range common.SchemaList {
		assert.Same(t, schema, common.schemaIndex.schemas[schema.ID])
	}
}

func TestMetadataBuilderFromBaseBuildsSchemaIndex(t *testing.T) {
	metadata, err := ParseMetadataBytes([]byte(ExampleTableMetadataV2))
	require.NoError(t, err)

	builder, err := MetadataBuilderFromBase(metadata, "")
	require.NoError(t, err)
	require.Len(t, builder.schemaIndex.schemas, len(builder.schemaList))
	for _, schema := range builder.schemaList {
		assert.Same(t, schema, builder.schemaIndex.schemas[schema.ID])
	}
}

func TestCommonMetadataSchemaIndexFallsBackAfterSliceReplacement(t *testing.T) {
	schemas := schemaIndexTestSchemas(1, 2)
	metadata := commonMetadata{
		SchemaList:  schemas,
		schemaIndex: buildSchemaIndex(schemas),
	}
	originalIndex := metadata.schemaIndex

	replacement := schemaIndexTestSchemas(3, 4)
	replacement[0] = schemas[0]
	metadata.SchemaList = replacement
	got := metadata.schemaByID(4)
	require.NotNil(t, got)
	assert.Equal(t, 4, got.ID)
	assert.Nil(t, metadata.schemaByID(2))
	assert.Same(t, originalIndex, metadata.schemaIndex)
	assert.Same(t, schemas[1], originalIndex.schemas[2])
}

func TestCommonMetadataSchemaIndexFallsBackAfterElementMutation(t *testing.T) {
	schemas := schemaIndexTestSchemas(1, 2)
	metadata := commonMetadata{
		SchemaList:  schemas,
		schemaIndex: buildSchemaIndex(schemas),
	}

	schemas[1].ID = 3
	assert.Nil(t, metadata.schemaByID(2))
}

func TestMetadataBuilderSchemaIndexFallsBackAfterSliceReplacement(t *testing.T) {
	schemas := schemaIndexTestSchemas(1, 2)
	builder := MetadataBuilder{
		schemaList:  schemas,
		schemaIndex: buildSchemaIndex(schemas),
	}
	originalIndex := builder.schemaIndex

	replacement := schemaIndexTestSchemas(3, 4)
	replacement[0] = schemas[0]
	builder.schemaList = replacement
	got, err := builder.GetSchemaByID(4)
	require.NoError(t, err)
	assert.Equal(t, 4, got.ID)
	_, err = builder.GetSchemaByID(2)
	assert.Error(t, err)
	assert.Same(t, originalIndex, builder.schemaIndex)
	assert.Same(t, schemas[1], originalIndex.schemas[2])
}

func TestMetadataBuilderSchemaIndexFollowsUpdates(t *testing.T) {
	builder := builderWithoutChanges(2)
	assert.Contains(t, builder.schemaIndex.schemas, builder.currentSchemaID)

	added := iceberg.NewSchema(99, iceberg.NestedField{
		ID: 4, Name: "new", Type: iceberg.PrimitiveTypes.Int64, Required: true,
	})
	require.NoError(t, builder.AddSchema(added))
	assert.Same(t, added, builder.schemaIndex.schemas[99])
	got, err := builder.GetSchemaByID(99)
	require.NoError(t, err)
	assert.Same(t, added, got)

	clone := builder.clone()
	cloneAdded := iceberg.NewSchema(100, iceberg.NestedField{
		ID: 5, Name: "clone", Type: iceberg.PrimitiveTypes.Int64, Required: true,
	})
	require.NoError(t, clone.AddSchema(cloneAdded))
	assert.NotContains(t, builder.schemaIndex.schemas, 100)
	assert.Same(t, cloneAdded, clone.schemaIndex.schemas[100])

	require.NoError(t, builder.RemoveSchemas([]int{99}))
	assert.NotContains(t, builder.schemaIndex.schemas, 99)
	_, err = builder.GetSchemaByID(99)
	assert.Error(t, err)
	got, err = clone.GetSchemaByID(99)
	require.NoError(t, err)
	assert.Same(t, added, got)
}

func TestMetadataBuilderSchemaIndexIsolatedFromBuiltMetadata(t *testing.T) {
	builder := builderWithoutChanges(2)
	metadata, err := builder.Build()
	require.NoError(t, err)
	common := metadataCommon(metadata)
	originalIndex := common.schemaIndex

	added := iceberg.NewSchema(99, iceberg.NestedField{
		ID: 4, Name: "new", Type: iceberg.PrimitiveTypes.Int64, Required: true,
	})
	require.NoError(t, builder.AddSchema(added))

	assert.NotSame(t, originalIndex, builder.schemaIndex)
	assert.NotContains(t, originalIndex.schemas, 99)
	assert.Contains(t, builder.schemaIndex.schemas, 99)
	assert.Nil(t, common.schemaByID(99))
}

func TestCommonMetadataSchemaLookupsConcurrent(t *testing.T) {
	schemas := schemaIndexTestSchemas(1, 2, 3)
	metadata := &commonMetadata{
		SchemaList:      schemas,
		CurrentSchemaID: 3,
		schemaIndex:     buildSchemaIndex(schemas),
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
				var schema *iceberg.Schema
				if i%2 == 0 {
					schema = metadata.schemaByID(2)
				} else {
					schema = metadata.CurrentSchema()
				}
				if schema == nil {
					t.Errorf("expected schema, got nil")

					return
				}
			}
		}(i)
	}
	wg.Wait()
}

func TestMetadataBuilderSchemaIndexCopyOnWriteConcurrent(t *testing.T) {
	builderValue := builderWithoutChanges(2)
	metadata, err := builderValue.Build()
	require.NoError(t, err)
	common := metadataCommon(metadata)
	builder := &builderValue

	readerStarted := make(chan struct{})
	writerErr := make(chan error, 1)
	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		for range 1_000 {
			schema := common.CurrentSchema()
			if schema == nil || schema.ID != builder.currentSchemaID {
				t.Errorf("expected current schema %d, got %v", builder.currentSchemaID, schema)

				return
			}
			select {
			case <-readerStarted:
			default:
				close(readerStarted)
			}
		}
	}()
	go func() {
		defer wg.Done()
		<-readerStarted
		added := iceberg.NewSchema(99, iceberg.NestedField{
			ID: 4, Name: "copy-on-write", Type: iceberg.PrimitiveTypes.Int64, Required: true,
		})
		writerErr <- builder.AddSchema(added)
	}()
	wg.Wait()
	require.NoError(t, <-writerErr)
	assert.NotSame(t, common.schemaIndex, builder.schemaIndex)
	assert.NotContains(t, common.schemaIndex.schemas, 99)
	assert.Contains(t, builder.schemaIndex.schemas, 99)
}

func schemaIndexTestSchemas(ids ...int) []*iceberg.Schema {
	schemas := make([]*iceberg.Schema, len(ids))
	for i, id := range ids {
		schemas[i] = iceberg.NewSchema(id)
	}

	return schemas
}
