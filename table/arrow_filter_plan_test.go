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

func TestCompiledFileFilterPlansReusePhysicalSchema(t *testing.T) {
	physicalFields := []iceberg.NestedField{{
		ID: 1, Name: "id", Type: iceberg.PrimitiveTypes.Int64, Required: true,
	}}
	firstSchema := iceberg.NewSchema(1, physicalFields...)
	secondSchema := iceberg.NewSchema(2, physicalFields...)
	filter, err := iceberg.BindExpr(firstSchema,
		iceberg.EqualTo(iceberg.Reference("id"), int64(1)), true)
	require.NoError(t, err)

	scan := &arrowScan{
		boundRowFilter: filter,
		caseSensitive:  true,
	}
	first, err := scan.cachedFileFilterPlans(firstSchema, true)
	require.NoError(t, err)
	second, err := scan.cachedFileFilterPlans(secondSchema, true)
	require.NoError(t, err)

	assert.Same(t, first, second)
	assert.Same(t, first.record, first.pruning)
	assert.Len(t, scan.filterPlanCache.plans, 1)
}

func TestCompiledFileFilterPlanSkipsAlwaysTrueStatsEvaluator(t *testing.T) {
	plan := &compiledFileFilterPlan{statsFilter: iceberg.AlwaysTrue{}}

	assert.Nil(t, plan.statsEvaluator())
}

func TestCompiledFileFilterPlansSeparatePhysicalTypes(t *testing.T) {
	int32Schema := iceberg.NewSchema(1, iceberg.NestedField{
		ID: 1, Name: "id", Type: iceberg.PrimitiveTypes.Int32, Required: true,
	})
	int64Schema := iceberg.NewSchema(2, iceberg.NestedField{
		ID: 1, Name: "id", Type: iceberg.PrimitiveTypes.Int64, Required: true,
	})
	filter, err := iceberg.BindExpr(int64Schema,
		iceberg.EqualTo(iceberg.Reference("id"), int64(1)), true)
	require.NoError(t, err)

	scan := &arrowScan{
		boundRowFilter:  filter,
		rowGroupFilter:  filter,
		filterSchema:    int64Schema,
		projectedSchema: int64Schema,
		caseSensitive:   true,
	}
	int32Plans, err := scan.cachedFileFilterPlans(int32Schema, true)
	require.NoError(t, err)
	int64Plans, err := scan.cachedFileFilterPlans(int64Schema, true)
	require.NoError(t, err)

	assert.NotSame(t, int32Plans, int64Plans)
	assert.Len(t, scan.filterPlanCache.plans, 2)
	require.Len(t, int32Plans.pruning.bloomPreds, 1)
	require.Len(t, int64Plans.pruning.bloomPreds, 1)
	require.Len(t, int32Plans.pruning.dictionaryPreds, 1)
	require.Len(t, int64Plans.pruning.dictionaryPreds, 1)
	assert.Equal(t, []byte{1, 0, 0, 0}, int32Plans.pruning.bloomPreds[0].PhysBytes[0])
	assert.Equal(t, []byte{1, 0, 0, 0, 0, 0, 0, 0}, int64Plans.pruning.bloomPreds[0].PhysBytes[0])
	assert.Equal(t, []byte{1, 0, 0, 0}, int32Plans.pruning.dictionaryPreds[0].PhysBytes[0])
	assert.Equal(t, []byte{1, 0, 0, 0, 0, 0, 0, 0}, int64Plans.pruning.dictionaryPreds[0].PhysBytes[0])
}

func TestPhysicalSchemaKeyIncludesNestedFieldIDs(t *testing.T) {
	firstSchema := iceberg.NewSchema(1, iceberg.NestedField{
		ID: 1, Name: "items", Type: &iceberg.ListType{
			ElementID:       2,
			Element:         iceberg.PrimitiveTypes.Int64,
			ElementRequired: false,
		},
	})
	secondSchema := iceberg.NewSchema(2, iceberg.NestedField{
		ID: 1, Name: "items", Type: &iceberg.ListType{
			ElementID:       3,
			Element:         iceberg.PrimitiveTypes.Int64,
			ElementRequired: false,
		},
	})

	firstKey, err := physicalSchemaKey(firstSchema)
	require.NoError(t, err)
	secondKey, err := physicalSchemaKey(secondSchema)
	require.NoError(t, err)

	assert.NotEqual(t, firstKey, secondKey)
}

func TestCompiledFileFilterPlansConcurrent(t *testing.T) {
	schema := iceberg.NewSchema(1,
		iceberg.NestedField{ID: 1, Name: "id", Type: iceberg.PrimitiveTypes.Int64, Required: true},
		iceberg.NestedField{ID: 2, Name: "value", Type: iceberg.PrimitiveTypes.String},
	)
	filter, err := iceberg.BindExpr(schema, iceberg.NewAnd(
		iceberg.EqualTo(iceberg.Reference("id"), int64(1)),
		iceberg.NotEqualTo(iceberg.Reference("value"), "ignored"),
	), true)
	require.NoError(t, err)

	scan := &arrowScan{boundRowFilter: filter, caseSensitive: true}
	const workerCount = 32
	results := make([]struct {
		plans *compiledFileFilterPlans
		err   error
	}, workerCount)

	var wg sync.WaitGroup
	wg.Add(workerCount)
	for i := range workerCount {
		go func(i int) {
			defer wg.Done()
			results[i].plans, results[i].err = scan.cachedFileFilterPlans(schema, true)
		}(i)
	}
	wg.Wait()

	for _, result := range results {
		require.NoError(t, result.err)
		require.Same(t, results[0].plans, result.plans)
	}
	assert.Len(t, scan.filterPlanCache.plans, 1)
}

func TestCompiledFileFilterPlansBuildPruningLazily(t *testing.T) {
	schema := iceberg.NewSchema(1, iceberg.NestedField{
		ID: 1, Name: "id", Type: iceberg.PrimitiveTypes.Int64, Required: true,
	})
	filter, err := iceberg.BindExpr(schema,
		iceberg.EqualTo(iceberg.Reference("id"), int64(1)), true)
	require.NoError(t, err)

	scan := &arrowScan{
		boundRowFilter: filter,
		rowGroupFilter: filter,
		caseSensitive:  true,
	}
	plans, err := scan.cachedFileFilterPlans(schema, false)
	require.NoError(t, err)
	assert.Nil(t, plans.pruning)

	plans, err = scan.cachedFileFilterPlans(schema, true)
	require.NoError(t, err)
	assert.NotNil(t, plans.pruning)
}

func TestCompiledFileFilterPlansDisablePruningForMissingInitialDefault(t *testing.T) {
	fileSchema := iceberg.NewSchema(1, iceberg.NestedField{
		ID: 1, Name: "id", Type: iceberg.PrimitiveTypes.Int64, Required: true,
	})
	logicalSchema := iceberg.NewSchema(2,
		iceberg.NestedField{ID: 1, Name: "id", Type: iceberg.PrimitiveTypes.Int64, Required: true},
		iceberg.NestedField{
			ID: 2, Name: "flag", Type: iceberg.PrimitiveTypes.Int32,
			InitialDefault: int32(1),
		},
	)
	filter, err := iceberg.BindExpr(logicalSchema, iceberg.NewAnd(
		iceberg.GreaterThan(iceberg.Reference("id"), int64(5)),
		iceberg.NotNull(iceberg.Reference("flag")),
	), true)
	require.NoError(t, err)

	scan := &arrowScan{
		rowGroupFilter:  filter,
		projectedSchema: logicalSchema,
		caseSensitive:   true,
	}
	plans, err := scan.cachedFileFilterPlans(fileSchema, true)
	require.NoError(t, err)

	assert.True(t, plans.pruning.statsFilter.Equals(iceberg.AlwaysTrue{}))
	assert.Empty(t, plans.pruning.bloomPreds)
	assert.Empty(t, plans.pruning.dictionaryPreds)
}

func TestPhysicalSchemaKeyIgnoresMetadata(t *testing.T) {
	fields := []iceberg.NestedField{{
		ID: 1, Name: "root", Type: &iceberg.StructType{FieldList: []iceberg.NestedField{
			{ID: 2, Name: "value", Type: iceberg.PrimitiveTypes.Int64, Required: true},
		}},
	}}
	original := iceberg.NewSchema(1, fields...)
	key, err := physicalSchemaKey(original)
	require.NoError(t, err)

	fields = original.Fields()
	fields[0].Doc = "root documentation"
	child := &fields[0].Type.(*iceberg.StructType).FieldList[0]
	child.Doc = "value documentation"
	child.InitialDefault = int64(7)
	child.WriteDefault = int64(9)
	withMetadata := iceberg.NewSchemaWithIdentifiers(2, []int{2}, fields...)
	metadataKey, err := physicalSchemaKey(withMetadata)
	require.NoError(t, err)
	assert.Equal(t, key, metadataKey)

	emptyKey, err := physicalSchemaKey(iceberg.NewSchema(1))
	require.NoError(t, err)
	assert.Empty(t, emptyKey)
}

func TestPhysicalSchemaKeyDistinguishesNestedLayouts(t *testing.T) {
	schema := iceberg.NewSchema(1, iceberg.NestedField{
		ID: 1, Name: "root", Type: &iceberg.StructType{FieldList: []iceberg.NestedField{
			{ID: 2, Name: "items", Type: &iceberg.ListType{
				ElementID: 3, Element: iceberg.PrimitiveTypes.Int64,
			}},
			{ID: 4, Name: "lookup", Type: &iceberg.MapType{
				KeyID: 5, KeyType: iceberg.PrimitiveTypes.String,
				ValueID: 6, ValueType: iceberg.DecimalTypeOf(10, 2),
			}},
			{ID: 7, Name: "fixed", Type: iceberg.FixedTypeOf(8)},
			{ID: 8, Name: "variant", Type: iceberg.VariantType{}},
		}},
	})
	original, err := physicalSchemaKey(schema)
	require.NoError(t, err)

	for _, tc := range []struct {
		name   string
		change func(*iceberg.StructType)
	}{
		{"field_id", func(s *iceberg.StructType) { s.FieldList[0].ID = 20 }},
		{"field_name", func(s *iceberg.StructType) { s.FieldList[0].Name = "items:;{}" }},
		{"field_required", func(s *iceberg.StructType) { s.FieldList[0].Required = true }},
		{"field_order", func(s *iceberg.StructType) { s.FieldList[0], s.FieldList[1] = s.FieldList[1], s.FieldList[0] }},
		{"list_id", func(s *iceberg.StructType) { s.FieldList[0].Type.(*iceberg.ListType).ElementID = 30 }},
		{"list_required", func(s *iceberg.StructType) { s.FieldList[0].Type.(*iceberg.ListType).ElementRequired = true }},
		{"list_type", func(s *iceberg.StructType) {
			s.FieldList[0].Type.(*iceberg.ListType).Element = iceberg.PrimitiveTypes.Int32
		}},
		{"map_key_id", func(s *iceberg.StructType) { s.FieldList[1].Type.(*iceberg.MapType).KeyID = 50 }},
		{"map_key_type", func(s *iceberg.StructType) {
			s.FieldList[1].Type.(*iceberg.MapType).KeyType = iceberg.PrimitiveTypes.Int32
		}},
		{"map_value_id", func(s *iceberg.StructType) { s.FieldList[1].Type.(*iceberg.MapType).ValueID = 60 }},
		{"map_value_required", func(s *iceberg.StructType) { s.FieldList[1].Type.(*iceberg.MapType).ValueRequired = true }},
		{"decimal_precision", func(s *iceberg.StructType) {
			s.FieldList[1].Type.(*iceberg.MapType).ValueType = iceberg.DecimalTypeOf(11, 2)
		}},
		{"decimal_scale", func(s *iceberg.StructType) {
			s.FieldList[1].Type.(*iceberg.MapType).ValueType = iceberg.DecimalTypeOf(10, 3)
		}},
		{"fixed_size", func(s *iceberg.StructType) { s.FieldList[2].Type = iceberg.FixedTypeOf(16) }},
		{"variant_type", func(s *iceberg.StructType) { s.FieldList[3].Type = iceberg.PrimitiveTypes.Binary }},
		{"empty_struct", func(s *iceberg.StructType) { s.FieldList = nil }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			fields := schema.Fields()
			tc.change(fields[0].Type.(*iceberg.StructType))
			key, err := physicalSchemaKey(iceberg.NewSchema(1, fields...))
			require.NoError(t, err)
			assert.NotEqual(t, original, key)
		})
	}
}

func TestPhysicalSchemaKeyInvalidSchema(t *testing.T) {
	for _, tc := range []struct {
		name string
		typ  iceberg.Type
	}{
		{"nil_type", nil},
		{"nil_struct", (*iceberg.StructType)(nil)},
		{"nil_list", (*iceberg.ListType)(nil)},
		{"nil_map", (*iceberg.MapType)(nil)},
		{"nil_struct_field", &iceberg.StructType{FieldList: []iceberg.NestedField{{ID: 2, Name: "child"}}}},
		{"nil_list_element", &iceberg.ListType{ElementID: 2}},
		{"nil_map_key", &iceberg.MapType{KeyID: 2, ValueID: 3, ValueType: iceberg.PrimitiveTypes.String}},
		{"nil_map_value", &iceberg.MapType{KeyID: 2, KeyType: iceberg.PrimitiveTypes.String, ValueID: 3}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			key, err := physicalSchemaKey(iceberg.NewSchema(1, iceberg.NestedField{ID: 1, Name: "invalid", Type: tc.typ}))
			require.ErrorIs(t, err, iceberg.ErrInvalidSchema)
			assert.Empty(t, key)
		})
	}

	key, err := physicalSchemaKey(nil)
	require.ErrorIs(t, err, iceberg.ErrInvalidSchema)
	assert.Empty(t, key)

	_, err = (&arrowScan{}).cachedFileFilterPlans(nil, true)
	require.ErrorIs(t, err, iceberg.ErrInvalidArgument)
}

func TestPhysicalSchemaKeySupportsParameterizedPrimitiveTypes(t *testing.T) {
	geometry, err := iceberg.GeometryTypeOf("srid:4326")
	require.NoError(t, err)
	geography, err := iceberg.GeographyTypeOf("srid:4326", "spherical")
	require.NoError(t, err)

	schema := iceberg.NewSchema(1,
		iceberg.NestedField{ID: 1, Name: "geometry", Type: geometry},
		iceberg.NestedField{ID: 2, Name: "geography", Type: geography},
	)
	original, err := physicalSchemaKey(schema)
	require.NoError(t, err)

	otherGeometry, err := iceberg.GeometryTypeOf("srid:3857")
	require.NoError(t, err)
	other := iceberg.NewSchema(1,
		iceberg.NestedField{ID: 1, Name: "geometry", Type: otherGeometry},
		iceberg.NestedField{ID: 2, Name: "geography", Type: geography},
	)
	changed, err := physicalSchemaKey(other)
	require.NoError(t, err)
	assert.NotEqual(t, original, changed)
}
