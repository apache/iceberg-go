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

package rest

import (
	"encoding/json"
	"fmt"
	"slices"
	"testing"

	"github.com/apache/iceberg-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// cloningSchemaHistoryMetadata models the defensive schema copies returned by
// real table metadata and counts how often the decode plan requests them.
type cloningSchemaHistoryMetadata struct {
	*scanTaskDecoderMetadata
	history        []*iceberg.Schema
	currentLookups int
	historyLookups int
}

func cloneDecoderSchemaForTest(schema *iceberg.Schema) *iceberg.Schema {
	if schema == nil {
		return nil
	}

	return iceberg.NewSchemaWithIdentifiers(
		schema.ID, slices.Clone(schema.IdentifierFieldIDs), schema.Fields()...,
	)
}

func (m *cloningSchemaHistoryMetadata) CurrentSchema() *iceberg.Schema {
	m.currentLookups++

	return cloneDecoderSchemaForTest(m.schema)
}

func (m *cloningSchemaHistoryMetadata) Schemas() []*iceberg.Schema {
	m.historyLookups++
	copies := make([]*iceberg.Schema, len(m.history))
	for index, schema := range m.history {
		copies[index] = cloneDecoderSchemaForTest(schema)
	}

	return copies
}

func TestPartitionDecodePlanReusesHistoricalSchemaCopies(t *testing.T) {
	t.Parallel()

	current := iceberg.NewSchema(30,
		iceberg.NestedField{ID: 1, Name: "active", Type: iceberg.PrimitiveTypes.Int64},
	)
	firstHistory := iceberg.NewSchema(20,
		iceberg.NestedField{ID: 2, Name: "old_date", Type: iceberg.PrimitiveTypes.Date},
		iceberg.NestedField{ID: 3, Name: "old_long", Type: iceberg.PrimitiveTypes.Int64},
	)
	// An ID appearing in multiple historical schemas must keep the first match.
	secondHistory := iceberg.NewSchema(10,
		iceberg.NestedField{ID: 2, Name: "old_string", Type: iceberg.PrimitiveTypes.String},
	)
	metadata := &cloningSchemaHistoryMetadata{
		scanTaskDecoderMetadata: &scanTaskDecoderMetadata{schema: current},
		history: []*iceberg.Schema{nil, firstHistory, secondHistory},
	}
	spec := iceberg.NewPartitionSpecID(7,
		iceberg.PartitionField{SourceIDs: []int{1}, FieldID: 1000, Name: "active", Transform: iceberg.IdentityTransform{}},
		iceberg.PartitionField{SourceIDs: []int{2}, FieldID: 1001, Name: "old_date", Transform: iceberg.IdentityTransform{}},
		iceberg.PartitionField{SourceIDs: []int{3}, FieldID: 1002, Name: "old_long", Transform: iceberg.IdentityTransform{}},
		iceberg.PartitionField{SourceIDs: []int{2}, FieldID: 1003, Name: "old_date_again", Transform: iceberg.IdentityTransform{}},
		iceberg.PartitionField{SourceIDs: []int{404}, FieldID: 1004, Name: "missing", Transform: iceberg.IdentityTransform{}},
	)

	plan, err := newPartitionDecodePlan(&spec, metadata)
	require.NoError(t, err)
	assert.Equal(t, 1, metadata.currentLookups)
	assert.Equal(t, 1, metadata.historyLookups)
	require.Len(t, plan.fields, 5)
	assert.Equal(t, iceberg.PrimitiveTypes.Int64, plan.fields[0].resultType)
	assert.Equal(t, iceberg.PrimitiveTypes.Date, plan.fields[1].resultType)
	assert.Equal(t, iceberg.PrimitiveTypes.Int64, plan.fields[2].resultType)
	assert.Equal(t, iceberg.PrimitiveTypes.Date, plan.fields[3].resultType)
	assert.False(t, plan.fields[4].sourceTypeFound)

	// Old, dropped partition fields remain readable even after schema evolution.
	partition, _, err := decodePartition([]json.RawMessage{
		json.RawMessage("12"), json.RawMessage("\"2026-07-17\""),
		json.RawMessage("45"), json.RawMessage("\"2026-07-18\""),
		json.RawMessage("null"),
	}, plan)
	require.NoError(t, err)
	assert.Equal(t, int64(12), partition[1000])
	assert.Equal(t, iceberg.Date(20651), partition[1001])
	assert.Equal(t, int64(45), partition[1002])
	assert.Equal(t, iceberg.Date(20652), partition[1003])
	assert.Nil(t, partition[1004])
}

func TestPartitionDecodePlanSkipsUnneededSchemaCopies(t *testing.T) {
	t.Parallel()

	current := iceberg.NewSchema(10,
		iceberg.NestedField{ID: 1, Name: "id", Type: iceberg.PrimitiveTypes.Int64},
		iceberg.NestedField{ID: 2, Name: "category", Type: iceberg.PrimitiveTypes.String},
	)
	for _, tc := range []struct {
		name         string
		sourceIDs    []int
		currentCalls int
		historyCalls int
	}{
		{name: "all current", sourceIDs: []int{1, 2, 1}, currentCalls: 1},
		{name: "unpartitioned"},
		{name: "missing source", sourceIDs: []int{101, 102}, currentCalls: 1, historyCalls: 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			metadata := &cloningSchemaHistoryMetadata{
				scanTaskDecoderMetadata: &scanTaskDecoderMetadata{schema: current},
			}
			fields := make([]iceberg.PartitionField, len(tc.sourceIDs))
			for index, id := range tc.sourceIDs {
				fields[index] = iceberg.PartitionField{
					SourceIDs: []int{id},
					FieldID: 1000 + index,
					Name: fmt.Sprintf("part_%d", index),
					Transform: iceberg.IdentityTransform{},
				}
			}
			spec := iceberg.NewPartitionSpecID(7, fields...)
			plan, err := newPartitionDecodePlan(&spec, metadata)
			require.NoError(t, err)
			assert.Len(t, plan.fields, len(fields))
			assert.Equal(t, tc.currentCalls, metadata.currentLookups)
			assert.Equal(t, tc.historyCalls, metadata.historyLookups)
		})
	}
}

func benchmarkOriginalPartitionSourceType(fieldID int, metadata *cloningSchemaHistoryMetadata) (iceberg.Type, bool) {
	if current := metadata.CurrentSchema(); current != nil {
		if typ, ok := current.FindTypeByID(fieldID); ok {
			return typ, true
		}
	}
	for _, schema := range metadata.Schemas() {
		if schema == nil {
			continue
		}
		if typ, ok := schema.FindTypeByID(fieldID); ok {
			return typ, true
		}
	}

	return nil, false
}

func historicalSchemaBenchmarkFixture(schemaCount, fieldCount, sourceCount int) (*cloningSchemaHistoryMetadata, []int) {
	history := make([]*iceberg.Schema, schemaCount)
	for version := range history {
		fields := make([]iceberg.NestedField, fieldCount)
		for index := range fields {
			fields[index] = iceberg.NestedField{
				ID: 1 + version*fieldCount + index,
				Name: fmt.Sprintf("field_%d", index),
				Type: iceberg.PrimitiveTypes.Int64,
			}
		}
		history[version] = iceberg.NewSchema(version, fields...)
	}
	sources := make([]int, sourceCount)
	for index := range sources {
		version := index * schemaCount / sourceCount
		sources[index] = 1 + version*fieldCount + index%fieldCount
	}
	metadata := &cloningSchemaHistoryMetadata{
		scanTaskDecoderMetadata: &scanTaskDecoderMetadata{
			schema: iceberg.NewSchema(schemaCount+1, iceberg.NestedField{
				ID: 900000, Name: "current", Type: iceberg.PrimitiveTypes.String,
			}),
		},
		history: history,
	}

	return metadata, sources
}

var benchmarkPartitionSourceTypeSink int

// BenchmarkPartitionSourceTypeSchemaHistory compares only the source-type
// lookup kernel. It does not measure JSON decoding or an end-to-end REST scan.
func BenchmarkPartitionSourceTypeSchemaHistory(b *testing.B) {
	for _, tc := range []struct {
		schemas, fields, sources int
	}{
		{schemas: 16, fields: 32, sources: 4},
		{schemas: 64, fields: 64, sources: 16},
		{schemas: 128, fields: 64, sources: 32},
	} {
		name := fmt.Sprintf("%d_schemas_%d_fields_%d_sources", tc.schemas, tc.fields, tc.sources)
		metadata, sources := historicalSchemaBenchmarkFixture(tc.schemas, tc.fields, tc.sources)
		b.Run(name+"/before", func(b *testing.B) {
			b.ReportAllocs()
			hits := 0
			for b.Loop() {
				for _, id := range sources {
					if typ, ok := benchmarkOriginalPartitionSourceType(id, metadata); !ok || typ == nil {
						b.Fatal("failed historical source lookup", id)
					}
					hits++
				}
			}
			benchmarkPartitionSourceTypeSink = hits
		})
		b.Run(name+"/after", func(b *testing.B) {
			b.ReportAllocs()
			hits := 0
			for b.Loop() {
				var lookup partitionSourceTypeLookup
				for _, id := range sources {
					if typ, ok := lookup.find(id, metadata); !ok || typ == nil {
						b.Fatal("failed historical source lookup", id)
					}
					hits++
				}
			}
			benchmarkPartitionSourceTypeSink = hits
		})
	}
}
