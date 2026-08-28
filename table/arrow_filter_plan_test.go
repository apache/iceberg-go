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
	assert.Equal(t, []byte{1, 0, 0, 0}, int32Plans.pruning.bloomPreds[0].PhysBytes[0])
	assert.Equal(t, []byte{1, 0, 0, 0, 0, 0, 0, 0}, int64Plans.pruning.bloomPreds[0].PhysBytes[0])
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
}
