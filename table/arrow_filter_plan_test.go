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
		ID: 1, Name: "items", Type: iceberg.NewListType(2, iceberg.PrimitiveTypes.Int64, false),
	})
	secondSchema := iceberg.NewSchema(2, iceberg.NestedField{
		ID: 1, Name: "items", Type: iceberg.NewListType(3, iceberg.PrimitiveTypes.Int64, false),
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
		ID: 1, Name: "root", Type: iceberg.NewStructType(
			iceberg.NestedField{ID: 2, Name: "value", Type: iceberg.PrimitiveTypes.Int64, Required: true},
		),
	}}
	original := iceberg.NewSchema(1, fields...)
	key, err := physicalSchemaKey(original)
	require.NoError(t, err)

	fields = original.Fields()
	fields[0].Doc = "root documentation"
	children := fields[0].Type.(*iceberg.StructType).Fields()
	children[0].Doc = "value documentation"
	children[0].InitialDefault = int64(7)
	children[0].WriteDefault = int64(9)
	fields[0].Type = iceberg.NewStructType(children...)
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
		ID: 1, Name: "root", Type: iceberg.NewStructType(
			iceberg.NestedField{ID: 2, Name: "items", Type: iceberg.NewListType(3, iceberg.PrimitiveTypes.Int64, false)},
			iceberg.NestedField{ID: 4, Name: "lookup", Type: iceberg.NewMapType(5, iceberg.PrimitiveTypes.String, 6, iceberg.MustDecimalTypeOf(10, 2), false)},
			iceberg.NestedField{ID: 7, Name: "fixed", Type: iceberg.MustFixedTypeOf(8)},
			iceberg.NestedField{ID: 8, Name: "variant", Type: iceberg.VariantType{}},
		),
	})
	original, err := physicalSchemaKey(schema)
	require.NoError(t, err)

	decimal := iceberg.MustDecimalTypeOf(10, 2)
	for _, tc := range []struct {
		name   string
		change func([]iceberg.NestedField) []iceberg.NestedField
	}{
		{"field_id", func(f []iceberg.NestedField) []iceberg.NestedField { f[0].ID = 20; return f }},
		{"field_name", func(f []iceberg.NestedField) []iceberg.NestedField { f[0].Name = "items:;{}"; return f }},
		{"field_required", func(f []iceberg.NestedField) []iceberg.NestedField { f[0].Required = true; return f }},
		{"field_order", func(f []iceberg.NestedField) []iceberg.NestedField { f[0], f[1] = f[1], f[0]; return f }},
		{"list_id", func(f []iceberg.NestedField) []iceberg.NestedField {
			f[0].Type = iceberg.NewListType(30, iceberg.PrimitiveTypes.Int64, false)
			return f
		}},
		{"list_required", func(f []iceberg.NestedField) []iceberg.NestedField {
			f[0].Type = iceberg.NewListType(3, iceberg.PrimitiveTypes.Int64, true)
			return f
		}},
		{"list_type", func(f []iceberg.NestedField) []iceberg.NestedField {
			f[0].Type = iceberg.NewListType(3, iceberg.PrimitiveTypes.Int32, false)
			return f
		}},
		{"map_key_id", func(f []iceberg.NestedField) []iceberg.NestedField {
			f[1].Type = iceberg.NewMapType(50, iceberg.PrimitiveTypes.String, 6, decimal, false)
			return f
		}},
		{"map_key_type", func(f []iceberg.NestedField) []iceberg.NestedField {
			f[1].Type = iceberg.NewMapType(5, iceberg.PrimitiveTypes.Int32, 6, decimal, false)
			return f
		}},
		{"map_value_id", func(f []iceberg.NestedField) []iceberg.NestedField {
			f[1].Type = iceberg.NewMapType(5, iceberg.PrimitiveTypes.String, 60, decimal, false)
			return f
		}},
		{"map_value_required", func(f []iceberg.NestedField) []iceberg.NestedField {
			f[1].Type = iceberg.NewMapType(5, iceberg.PrimitiveTypes.String, 6, decimal, true)
			return f
		}},
		{"decimal_precision", func(f []iceberg.NestedField) []iceberg.NestedField {
			f[1].Type = iceberg.NewMapType(5, iceberg.PrimitiveTypes.String, 6, iceberg.MustDecimalTypeOf(11, 2), false)
			return f
		}},
		{"decimal_scale", func(f []iceberg.NestedField) []iceberg.NestedField {
			f[1].Type = iceberg.NewMapType(5, iceberg.PrimitiveTypes.String, 6, iceberg.MustDecimalTypeOf(10, 3), false)
			return f
		}},
		{"fixed_size", func(f []iceberg.NestedField) []iceberg.NestedField { f[2].Type = iceberg.MustFixedTypeOf(16); return f }},
		{"variant_type", func(f []iceberg.NestedField) []iceberg.NestedField {
			f[3].Type = iceberg.PrimitiveTypes.Binary
			return f
		}},
		{"empty_struct", func([]iceberg.NestedField) []iceberg.NestedField { return nil }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			fields := schema.Fields()
			root := fields[0].Type.(*iceberg.StructType)
			fields[0].Type = iceberg.NewStructType(tc.change(root.Fields())...)
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
		{"nil_struct_field", iceberg.NewStructType(iceberg.NestedField{ID: 2, Name: "child"})},
		{"nil_list_element", iceberg.NewListType(2, nil, false)},
		{"nil_map_key", iceberg.NewMapType(2, nil, 3, iceberg.PrimitiveTypes.String, false)},
		{"nil_map_value", iceberg.NewMapType(2, iceberg.PrimitiveTypes.String, 3, nil, false)},
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
