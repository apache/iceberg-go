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
	"context"
	"testing"

	"github.com/apache/iceberg-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type splitCapabilityPlanner struct {
	remote bool
	full   bool
}

func (p *splitCapabilityPlanner) SupportsRemoteScanPlanning() bool { return p.remote }
func (p *splitCapabilityPlanner) SupportsFullRemoteScanPlanning() bool {
	return p.full
}
func (p *splitCapabilityPlanner) PlanFiles(context.Context, ScanPlanningRequest) (ScanPlanningResult, error) {
	return ScanPlanningResult{}, nil
}

type basicCapabilityPlanner struct {
	remote bool
}

func (p *basicCapabilityPlanner) SupportsRemoteScanPlanning() bool { return p.remote }
func (p *basicCapabilityPlanner) PlanFiles(context.Context, ScanPlanningRequest) (ScanPlanningResult, error) {
	return ScanPlanningResult{}, nil
}

func TestSupportsAutomaticRemotePlanningUsesFullCapabilityWhenAvailable(t *testing.T) {
	t.Parallel()

	assert.False(t, supportsAutomaticRemotePlanning(&splitCapabilityPlanner{remote: true, full: false}))
	assert.True(t, supportsAutomaticRemotePlanning(&splitCapabilityPlanner{remote: true, full: true}))
	assert.True(t, supportsAutomaticRemotePlanning(&basicCapabilityPlanner{remote: true}))
	assert.False(t, supportsAutomaticRemotePlanning(nil))
}

func TestRemotePlanningSelectedFieldsExpandsWildcardNestedProjection(t *testing.T) {
	t.Parallel()

	schema := iceberg.NewSchema(0,
		iceberg.NestedField{ID: 1, Name: "id", Type: iceberg.PrimitiveTypes.Int64, Required: true},
		iceberg.NestedField{
			ID:   2,
			Name: "address",
			Type: &iceberg.StructType{FieldList: []iceberg.NestedField{
				{ID: 3, Name: "city", Type: iceberg.PrimitiveTypes.String},
				{ID: 4, Name: "zip", Type: iceberg.PrimitiveTypes.String},
			}},
		},
	)

	scan := &Scan{selectedFields: []string{"*"}, caseSensitive: true}
	got, err := remotePlanningSelectedFields(scan, schema)
	require.NoError(t, err)
	assert.Equal(t, []string{"id", "address.city", "address.zip"}, got)
}
