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

package table_test

import (
	"testing"

	"github.com/apache/iceberg-go"
	"github.com/apache/iceberg-go/table"
	"github.com/stretchr/testify/require"
)

func satisfyingSortField(ids []int, transform iceberg.Transform, direction table.SortDirection, nullOrder table.NullOrder) table.SortField {
	return table.SortField{
		SourceIDs: ids,
		Transform: transform,
		Direction: direction,
		NullOrder: nullOrder,
	}
}

func satisfyingSortOrder(t *testing.T, id int, fields ...table.SortField) table.SortOrder {
	t.Helper()

	order, err := table.NewSortOrder(id, fields)
	require.NoError(t, err)

	return order
}

func TestSortOrderSatisfies(t *testing.T) {
	byID := satisfyingSortField([]int{1}, iceberg.IdentityTransform{}, table.SortASC, table.NullsFirst)
	byName := satisfyingSortField([]int{2}, iceberg.IdentityTransform{}, table.SortDESC, table.NullsLast)
	id := satisfyingSortOrder(t, 1, byID)
	idWithDifferentOrderID := satisfyingSortOrder(t, 99, byID)
	idAndName := satisfyingSortOrder(t, 2, byID, byName)
	name := satisfyingSortOrder(t, 3, byName)
	unsorted := table.UnsortedSortOrder

	cases := []struct {
		name     string
		actual   table.SortOrder
		required table.SortOrder
		want     bool
	}{
		{"unsorted satisfies unsorted", unsorted, unsorted, true},
		{"any sorted order satisfies unsorted", idAndName, unsorted, true},
		{"unsorted does not satisfy sorted", unsorted, id, false},
		{"an order satisfies itself", idAndName, idAndName, true},
		{"order IDs do not matter", id, idWithDifferentOrderID, true},
		{"longer sort satisfies a prefix", idAndName, id, true},
		{"shorter sort does not satisfy longer", id, idAndName, false},
		{"non-prefix sort does not satisfy", idAndName, name, false},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			require.Equal(t, tc.want, tc.actual.Satisfies(tc.required))
		})
	}
}

func TestSortFieldSatisfiesMatchesSourceDirectionAndNulls(t *testing.T) {
	base := satisfyingSortField([]int{1, 2}, iceberg.IdentityTransform{}, table.SortDESC, table.NullsLast)

	tests := []struct {
		name     string
		required table.SortField
		want     bool
	}{
		{"same field", base, true},
		{"different first source", satisfyingSortField([]int{9, 2}, iceberg.IdentityTransform{}, table.SortDESC, table.NullsLast), false},
		{"different second source", satisfyingSortField([]int{1, 3}, iceberg.IdentityTransform{}, table.SortDESC, table.NullsLast), false},
		{"missing source", satisfyingSortField([]int{1}, iceberg.IdentityTransform{}, table.SortDESC, table.NullsLast), false},
		{"reordered sources", satisfyingSortField([]int{2, 1}, iceberg.IdentityTransform{}, table.SortDESC, table.NullsLast), false},
		{"different direction", satisfyingSortField([]int{1, 2}, iceberg.IdentityTransform{}, table.SortASC, table.NullsLast), false},
		{"different null order", satisfyingSortField([]int{1, 2}, iceberg.IdentityTransform{}, table.SortDESC, table.NullsFirst), false},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			require.Equal(t, tc.want, base.Satisfies(tc.required))
		})
	}
}

func TestSortFieldSatisfiesTransformImplications(t *testing.T) {
	identity := iceberg.IdentityTransform{}
	truncate2 := iceberg.TruncateTransform{Width: 2}
	truncate4 := iceberg.TruncateTransform{Width: 4}
	year := iceberg.YearTransform{}
	month := iceberg.MonthTransform{}
	day := iceberg.DayTransform{}
	hour := iceberg.HourTransform{}
	bucket4 := iceberg.BucketTransform{NumBuckets: 4}
	bucket8 := iceberg.BucketTransform{NumBuckets: 8}

	tests := []struct {
		name     string
		actual   iceberg.Transform
		required iceberg.Transform
		want     bool
	}{
		{"identity implies identity", identity, identity, true},
		{"identity implies truncation", identity, truncate2, true},
		{"identity implies time transform", identity, hour, true},
		{"truncation does not imply identity", truncate2, identity, false},
		{"wider truncation implies narrower", truncate4, truncate2, true},
		{"narrower truncation does not imply wider", truncate2, truncate4, false},
		{"truncation implies itself", truncate4, truncate4, true},
		{"time transform does not imply truncation", day, truncate2, false},
		{"truncation does not imply time transform", truncate4, year, false},
		{"hour implies day", hour, day, true},
		{"hour implies month", hour, month, true},
		{"hour implies year", hour, year, true},
		{"day implies month", day, month, true},
		{"day implies year", day, year, true},
		{"month implies year", month, year, true},
		{"month does not imply day", month, day, false},
		{"year does not imply month", year, month, false},
		{"day does not imply hour", day, hour, false},
		{"same bucket transform", bucket4, bucket4, true},
		{"different bucket transforms", bucket4, bucket8, false},
		{"identity does not imply bucket", identity, bucket4, false},
		{"pointer truncate transform", &truncate4, &truncate2, true},
		{"pointer time transform", &hour, &year, true},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			actual := satisfyingSortField([]int{7}, tc.actual, table.SortASC, table.NullsLast)
			required := satisfyingSortField([]int{7}, tc.required, table.SortASC, table.NullsLast)
			require.Equal(t, tc.want, actual.Satisfies(required))
		})
	}
}

func TestSortOrderSatisfiesTransformOnPrefixOnly(t *testing.T) {
	year := satisfyingSortField([]int{1}, iceberg.YearTransform{}, table.SortASC, table.NullsLast)
	day := satisfyingSortField([]int{1}, iceberg.DayTransform{}, table.SortASC, table.NullsLast)
	byName := satisfyingSortField([]int{2}, iceberg.IdentityTransform{}, table.SortDESC, table.NullsLast)

	actual := satisfyingSortOrder(t, 1, day, byName)
	required := satisfyingSortOrder(t, 2, year)
	require.True(t, actual.Satisfies(required))
	require.False(t, required.Satisfies(actual))

	wrongSuffix := satisfyingSortOrder(t, 3, year, satisfyingSortField([]int{2}, iceberg.IdentityTransform{}, table.SortASC, table.NullsLast))
	require.False(t, actual.Satisfies(wrongSuffix))
}

func TestSortFieldSatisfiesRejectsNilTransforms(t *testing.T) {
	valid := satisfyingSortField([]int{1}, iceberg.IdentityTransform{}, table.SortASC, table.NullsFirst)
	missing := satisfyingSortField([]int{1}, nil, table.SortASC, table.NullsFirst)
	require.False(t, valid.Satisfies(missing))
	require.False(t, missing.Satisfies(valid))

	var nilTruncate *iceberg.TruncateTransform
	typedNil := satisfyingSortField([]int{1}, nilTruncate, table.SortASC, table.NullsFirst)
	require.False(t, typedNil.Satisfies(valid))
	require.False(t, valid.Satisfies(typedNil))
}
