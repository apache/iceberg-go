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

package iceberg_test

import (
	"testing"

	"github.com/apache/iceberg-go"
	"github.com/stretchr/testify/require"
)

type pointerSourceTransform struct{ iceberg.IdentityTransform }

func (pointerSourceTransform) CanTransform(source iceberg.Type) bool {
	_, ok := source.(*iceberg.StringType)

	return ok
}

func TestPartitionSpecSourceTransformCompatibility(t *testing.T) {
	unknown, err := iceberg.ParseTransform("custom_transform")
	require.NoError(t, err)

	tests := []struct {
		name      string
		typ       iceberg.Type
		transform iceberg.Transform
		wantErr   string
	}{
		{"day of date", iceberg.PrimitiveTypes.Date, iceberg.DayTransform{}, ""},
		{"day of timestamp", iceberg.PrimitiveTypes.Timestamp, iceberg.DayTransform{}, ""},
		{"day of timestamp with zone", iceberg.PrimitiveTypes.TimestampTz, iceberg.DayTransform{}, ""},
		{"day of nanosecond timestamp", iceberg.PrimitiveTypes.TimestampNs, iceberg.DayTransform{}, ""},
		{"hour of nanosecond timestamp with zone", iceberg.PrimitiveTypes.TimestampTzNs, iceberg.HourTransform{}, ""},
		{"day of string", iceberg.PrimitiveTypes.String, iceberg.DayTransform{}, "invalid source type string for transform day"},
		{"day of pointer timestamp", &iceberg.TimestampType{}, iceberg.DayTransform{}, ""},
		{"day of pointer string", &iceberg.StringType{}, iceberg.DayTransform{}, "invalid source type string for transform day"},
		{"year of string", iceberg.PrimitiveTypes.String, iceberg.YearTransform{}, "invalid source type string for transform year"},
		{"hour of date", iceberg.PrimitiveTypes.Date, iceberg.HourTransform{}, "invalid source type date for transform hour"},
		{"bucket of string", iceberg.PrimitiveTypes.String, iceberg.BucketTransform{NumBuckets: 8}, ""},
		{"bucket of pointer string", &iceberg.StringType{}, iceberg.BucketTransform{NumBuckets: 8}, ""},
		{"bucket of boolean", iceberg.PrimitiveTypes.Bool, iceberg.BucketTransform{NumBuckets: 8}, "invalid source type boolean for transform bucket[8]"},
		{"truncate of string", iceberg.PrimitiveTypes.String, iceberg.TruncateTransform{Width: 4}, ""},
		{"truncate of float", iceberg.PrimitiveTypes.Float32, iceberg.TruncateTransform{Width: 4}, "invalid source type float for transform truncate[4]"},
		{"identity of geometry", iceberg.GeometryType{}, iceberg.IdentityTransform{}, "invalid source type geometry for transform identity"},
		{"identity of pointer geometry", &iceberg.GeometryType{}, iceberg.IdentityTransform{}, "invalid source type geometry for transform identity"},
		{"identity of struct", iceberg.NewStructType(), iceberg.IdentityTransform{}, "non-primitive source field"},
		{"void of struct", iceberg.NewStructType(), iceberg.VoidTransform{}, ""},
		{"unknown transform", iceberg.PrimitiveTypes.String, unknown, ""},
		{"custom pointer source", &iceberg.StringType{}, pointerSourceTransform{}, ""},
		{"custom value source", iceberg.PrimitiveTypes.String, pointerSourceTransform{}, "invalid source type string for transform identity"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			schema := iceberg.NewSchema(0, iceberg.NestedField{ID: 1, Name: "source", Type: tt.typ})
			constructors := map[string]func() error{
				"name": func() error {
					_, err := iceberg.NewPartitionSpecOpts(iceberg.AddPartitionFieldByName("source", "partition", tt.transform, schema, nil))

					return err
				},
				"source ID": func() error {
					_, err := iceberg.NewPartitionSpecOpts(iceberg.AddPartitionFieldBySourceID(1, "partition", tt.transform, schema, nil))

					return err
				},
				"bind": func() error {
					spec := iceberg.NewPartitionSpec(iceberg.PartitionField{SourceIDs: []int{1}, FieldID: 1000, Name: "partition", Transform: tt.transform})
					_, err := spec.BindToSchema(schema, nil, nil)

					return err
				},
			}
			for name, create := range constructors {
				t.Run(name, func(t *testing.T) {
					err := create()
					if tt.wantErr == "" {
						require.NoError(t, err)
					} else {
						require.ErrorIs(t, err, iceberg.ErrInvalidPartitionSpec)
						require.ErrorContains(t, err, tt.wantErr)
					}
				})
			}
		})
	}
}

func TestPartitionSpecNestedSources(t *testing.T) {
	schema := iceberg.NewSchema(0,
		iceberg.NestedField{ID: 1, Name: "nested", Type: iceberg.NewStructType(
			iceberg.NestedField{ID: 2, Name: "value", Type: iceberg.PrimitiveTypes.String})},
		iceberg.NestedField{ID: 3, Name: "list", Type: iceberg.NewListType(4, iceberg.NewStructType(
			iceberg.NestedField{ID: 5, Name: "value", Type: iceberg.PrimitiveTypes.String}), false)},
		iceberg.NestedField{ID: 6, Name: "map", Type: iceberg.NewMapType(7, iceberg.PrimitiveTypes.String, 8, iceberg.NewStructType(
			iceberg.NestedField{ID: 9, Name: "value", Type: iceberg.PrimitiveTypes.String}), false)},
	)
	unknown, err := iceberg.ParseTransform("custom_two_arg")
	require.NoError(t, err)

	for _, tt := range []struct {
		name      string
		sourceIDs []int
		transform iceberg.Transform
		wantErr   bool
	}{
		{"nested struct", []int{2}, iceberg.IdentityTransform{}, false},
		{"list element", []int{4}, iceberg.IdentityTransform{}, true},
		{"nested in list", []int{5}, iceberg.IdentityTransform{}, true},
		{"map key", []int{7}, iceberg.IdentityTransform{}, true},
		{"nested in map", []int{9}, iceberg.IdentityTransform{}, true},
		{"unknown with nested struct", []int{2, 7}, unknown, true},
		{"void in list", []int{5}, iceberg.VoidTransform{}, false},
	} {
		t.Run(tt.name, func(t *testing.T) {
			_, err := iceberg.NewPartitionSpecOpts(iceberg.AddPartitionFieldBySourceIDs(tt.sourceIDs, "partition", tt.transform, schema, nil))
			if tt.wantErr {
				require.ErrorIs(t, err, iceberg.ErrInvalidPartitionSpec)
			} else {
				require.NoError(t, err)
			}
		})
	}
}

func TestPartitionSpecRejectsTypedNilTransforms(t *testing.T) {
	schema := iceberg.NewSchema(0, iceberg.NestedField{ID: 1, Name: "source", Type: iceberg.PrimitiveTypes.String})
	for _, transform := range []iceberg.Transform{(*iceberg.DayTransform)(nil), (*iceberg.BucketTransform)(nil), (*iceberg.VoidTransform)(nil)} {
		_, err := iceberg.NewPartitionSpecOpts(iceberg.AddPartitionFieldBySourceID(1, "partition", transform, schema, nil))
		require.ErrorIs(t, err, iceberg.ErrInvalidPartitionSpec)
		require.ErrorContains(t, err, "has no transform")
	}
}

func TestPartitionSpecKnownTransformsRejectMultipleSources(t *testing.T) {
	schema := iceberg.NewSchema(0,
		iceberg.NestedField{ID: 1, Name: "first", Type: iceberg.PrimitiveTypes.Timestamp},
		iceberg.NestedField{ID: 2, Name: "second", Type: iceberg.PrimitiveTypes.String},
	)
	transforms := []iceberg.Transform{
		iceberg.IdentityTransform{},
		iceberg.VoidTransform{},
		iceberg.BucketTransform{NumBuckets: 4},
		iceberg.TruncateTransform{Width: 4},
		iceberg.YearTransform{},
		iceberg.MonthTransform{},
		iceberg.DayTransform{},
		iceberg.HourTransform{},
		&iceberg.DayTransform{},
	}
	for _, transform := range transforms {
		t.Run(transform.String(), func(t *testing.T) {
			_, err := iceberg.NewPartitionSpecOpts(iceberg.AddPartitionFieldBySourceIDs([]int{1, 2}, "partition", transform, schema, nil))
			require.ErrorIs(t, err, iceberg.ErrInvalidPartitionSpec)
			require.ErrorContains(t, err, "requires a single source field")

			spec := iceberg.NewPartitionSpec(iceberg.PartitionField{
				SourceIDs: []int{1, 2}, FieldID: 1000, Name: "partition", Transform: transform,
			})
			_, err = spec.BindToSchema(schema, nil, nil)
			require.ErrorIs(t, err, iceberg.ErrInvalidPartitionSpec)
		})
	}

	unknown, err := iceberg.ParseTransform("custom_two_arg")
	require.NoError(t, err)
	spec, err := iceberg.NewPartitionSpecOpts(iceberg.AddPartitionFieldBySourceIDs([]int{1, 2}, "partition", unknown, schema, nil))
	require.NoError(t, err)
	require.Equal(t, []int{1, 2}, spec.Field(0).SourceIDs)
}
