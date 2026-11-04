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
	"reflect"
	"slices"

	"github.com/apache/iceberg-go"
)

// Satisfies reports whether this sort field guarantees the ordering required
// by another field. Source IDs, direction, and null ordering must match.
func (s SortField) Satisfies(required SortField) bool {
	if !slices.Equal(s.SourceIDs, required.SourceIDs) ||
		s.Direction != required.Direction || s.NullOrder != required.NullOrder ||
		isNilSortTransform(s.Transform) || isNilSortTransform(required.Transform) {
		return false
	}

	if s.Transform.Equals(required.Transform) {
		return true
	}

	switch transform := s.Transform.(type) {
	case iceberg.IdentityTransform, *iceberg.IdentityTransform:
		// Identity preserves the order of every monotonic transform.
		return required.Transform.PreservesOrder()
	case iceberg.TruncateTransform:
		return truncateSatisfies(transform.Width, required.Transform)
	case *iceberg.TruncateTransform:
		return truncateSatisfies(transform.Width, required.Transform)
	}

	actualGranularity := timeSortGranularity(s.Transform)
	requiredGranularity := timeSortGranularity(required.Transform)

	// A finer time order also satisfies coarser orders: hour > day > month > year.
	return actualGranularity != 0 && requiredGranularity != 0 && actualGranularity >= requiredGranularity
}

// Satisfies reports whether this sort order guarantees another sort order.
// The required fields must be a compatible prefix of this order. The order IDs
// are deliberately ignored, and every order satisfies the unsorted order.
func (s SortOrder) Satisfies(required SortOrder) bool {
	if len(s.fields) < len(required.fields) {
		return false
	}

	for i, field := range required.fields {
		if !s.fields[i].Satisfies(field) {
			return false
		}
	}

	return true
}

func isNilSortTransform(transform iceberg.Transform) bool {
	if transform == nil {
		return true
	}

	value := reflect.ValueOf(transform)

	return value.Kind() == reflect.Pointer && value.IsNil()
}

func truncateSatisfies(width int, required iceberg.Transform) bool {
	switch target := required.(type) {
	case iceberg.TruncateTransform:
		return width >= target.Width
	case *iceberg.TruncateTransform:
		return target != nil && width >= target.Width
	default:
		return false
	}
}

func timeSortGranularity(transform iceberg.Transform) int {
	switch transform.(type) {
	case iceberg.YearTransform, *iceberg.YearTransform:
		return 1
	case iceberg.MonthTransform, *iceberg.MonthTransform:
		return 2
	case iceberg.DayTransform, *iceberg.DayTransform:
		return 3
	case iceberg.HourTransform, *iceberg.HourTransform:
		return 4
	default:
		return 0
	}
}
