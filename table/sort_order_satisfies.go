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

// Satisfies checks whether this sort field satisfies another using Iceberg
// Java's transform ordering rules. Source IDs, direction, and null ordering
// must match. This is a metadata compatibility check, not a type-aware proof
// of physical row ordering (notably for numeric truncation).
func (s SortField) Satisfies(required SortField) bool {
	if len(s.SourceIDs) == 0 || !slices.Equal(s.SourceIDs, required.SourceIDs) ||
		s.Direction != required.Direction || s.NullOrder != required.NullOrder ||
		isNilSatisfactionTransform(s.Transform) || isNilSatisfactionTransform(required.Transform) {
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

// Satisfies checks whether this sort order satisfies another using Iceberg
// Java's field-by-field prefix rules. Order IDs are ignored, and every order
// satisfies the unsorted order. It does not inspect the sorted data.
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

func isNilSatisfactionTransform(transform iceberg.Transform) bool {
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
