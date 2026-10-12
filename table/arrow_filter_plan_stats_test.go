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
	"testing"

	"github.com/apache/iceberg-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestCompiledFileFilterPlanTracksStatsFieldIDs(t *testing.T) {
	schema := iceberg.NewSchema(1,
		iceberg.NestedField{ID: 1, Name: "id", Type: iceberg.PrimitiveTypes.Int64, Required: true},
		iceberg.NestedField{ID: 2, Name: "value", Type: iceberg.PrimitiveTypes.Int64, Required: true},
		iceberg.NestedField{ID: 3, Name: "payload", Type: iceberg.PrimitiveTypes.String},
	)
	filter, err := iceberg.BindExpr(schema, iceberg.NewAnd(
		iceberg.EqualTo(iceberg.Reference("id"), int64(7)),
		iceberg.GreaterThan(iceberg.Reference("value"), int64(10)),
	), true)
	require.NoError(t, err)

	plan, err := compileFileFilterPlan(schema, filter, true, false, true)
	require.NoError(t, err)

	assert.ElementsMatch(t, []int{1, 2}, plan.statsFieldIDs)
}

func TestCompiledFileFilterPlanKeepsEmptyStatsFieldIDsForAlwaysFalse(t *testing.T) {
	schema := iceberg.NewSchema(1,
		iceberg.NestedField{ID: 1, Name: "id", Type: iceberg.PrimitiveTypes.Int64, Required: true},
	)
	plan, err := compileFileFilterPlan(schema, iceberg.AlwaysFalse{}, true, false, true)
	require.NoError(t, err)
	require.NotNil(t, plan.statsFieldIDs)
	assert.Empty(t, plan.statsFieldIDs)

	eval := plan.statsEvaluator()
	require.NotNil(t, eval)
	keep, err := eval(buildRowGroupMetricsMetadata(t, 1, 1, true).RowGroup(0), nil)
	require.NoError(t, err)
	assert.False(t, keep)
}
