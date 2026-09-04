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
	"fmt"
	"testing"

	"github.com/apache/iceberg-go"
	"github.com/stretchr/testify/require"
)

func TestPartitionValidationPlanPreservesPartitionValidationErrors(t *testing.T) {
	spec := iceberg.NewPartitionSpecID(7,
		iceberg.PartitionField{
			SourceIDs: []int{1},
			FieldID:   1000,
			Name:      "first",
			Transform: iceberg.IdentityTransform{},
		},
		iceberg.PartitionField{
			SourceIDs: []int{2},
			FieldID:   1001,
			Name:      "second",
			Transform: iceberg.IdentityTransform{},
		},
	)
	plan := newPartitionValidationPlan(&spec)

	for _, tc := range []struct {
		name      string
		partition map[int]any
		wantError string
	}{
		{
			name:      "missing first field",
			partition: map[int]any{},
			wantError: "missing partition value for field id 1000 (first)",
		},
		{
			name:      "missing second field",
			partition: map[int]any{1000: int32(1)},
			wantError: "missing partition value for field id 1001 (second)",
		},
		{
			name: "missing field with unknown field",
			partition: map[int]any{
				1000: int32(1),
				9999: int32(3),
			},
			wantError: "missing partition value for field id 1001 (second)",
		},
		{
			name: "unknown field",
			partition: map[int]any{
				1000: int32(1),
				1001: int32(2),
				9999: int32(3),
			},
			wantError: "unknown partition field id 9999 for spec id 7",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			df := newTestDataFile(t, spec, "mem://partition-validation-test.parquet", tc.partition)
			err := plan.validate(df)
			require.ErrorContains(t, err, tc.wantError)
		})
	}

	duplicateSpec := iceberg.NewPartitionSpecID(7,
		iceberg.PartitionField{
			SourceIDs: []int{1},
			FieldID:   1000,
			Name:      "first",
			Transform: iceberg.IdentityTransform{},
		},
		iceberg.PartitionField{
			SourceIDs: []int{2},
			FieldID:   1000,
			Name:      "duplicate",
			Transform: iceberg.IdentityTransform{},
		},
	)
	duplicatePlan := newPartitionValidationPlan(&duplicateSpec)
	duplicateFile := newTestDataFile(t, duplicateSpec, "mem://partition-validation-test-duplicate.parquet", map[int]any{
		1000: int32(1),
		9999: int32(3),
	})
	require.ErrorContains(t, duplicatePlan.validate(duplicateFile), "unknown partition field id 9999 for spec id 7")

	valid := newTestDataFile(t, spec, "mem://partition-validation-test-valid.parquet", map[int]any{
		1000: int32(1),
		1001: int32(2),
	})
	require.NoError(t, plan.validate(valid))
	require.NoError(t, plan.validate(valid))
}

func TestPartitionValidationPlanConcurrentValidation(t *testing.T) {
	spec := iceberg.NewPartitionSpecID(7, iceberg.PartitionField{
		SourceIDs: []int{1},
		FieldID:   1000,
		Name:      "id",
		Transform: iceberg.IdentityTransform{},
	})
	plan := newPartitionValidationPlan(&spec)

	for i := range 16 {
		for _, tc := range []struct {
			name      string
			partition map[int]any
			wantError string
		}{
			{"valid", map[int]any{1000: int32(1)}, ""},
			{"missing", nil, "missing partition value for field id 1000 (id)"},
			{"unknown", map[int]any{1000: int32(1), 9999: int32(2)}, "unknown partition field id 9999 for spec id 7"},
		} {
			t.Run(fmt.Sprintf("%s/%d", tc.name, i), func(t *testing.T) {
				t.Parallel()

				df := newTestDataFile(t, spec, "mem://concurrent-validation.parquet", tc.partition)
				for range 100 {
					err := plan.validate(df)
					if tc.wantError == "" {
						require.NoError(t, err)
					} else {
						require.EqualError(t, err, tc.wantError)
					}
				}
			})
		}
	}
}
