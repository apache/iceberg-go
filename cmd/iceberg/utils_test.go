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

package main

import (
	"maps"
	"testing"

	"github.com/apache/iceberg-go"
	"github.com/apache/iceberg-go/table"
)

func TestParseProperties(t *testing.T) {
	tests := []struct {
		name  string
		input string
		want  iceberg.Properties
		isErr bool
	}{
		{
			name:  "empty string",
			input: "",
			want:  iceberg.Properties{},
		},
		{
			name:  "single property",
			input: "key1=value1",
			want:  iceberg.Properties{"key1": "value1"},
		},
		{
			name:  "multiple properties",
			input: "key1=value1,key2=value2,key3=value3",
			want:  iceberg.Properties{"key1": "value1", "key2": "value2", "key3": "value3"},
		},
		{
			name:  "with spaces",
			input: " key1 = value1 , key2 = value2 ",
			want:  iceberg.Properties{"key1": "value1", "key2": "value2"},
		},
		{
			name:  "invalid format - no equals",
			input: "key1value1",
			isErr: true,
		},
		{
			name:  "invalid format - empty key",
			input: "=value1",
			isErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := parseProperties(tt.input)
			if (err != nil) != tt.isErr {
				t.Errorf("parseProperties() error = %v, isErr %v", err, tt.isErr)

				return
			}
			if !tt.isErr && !maps.Equal(got, tt.want) {
				t.Errorf("parseProperties() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestParsePartitionSpec(t *testing.T) {
	tests := []struct {
		name  string
		input string
		isErr bool
	}{
		{
			name:  "empty string",
			input: "",
		},
		{
			name:  "single field",
			input: "field1",
		},
		{
			name:  "multiple fields",
			input: "field1,field2,field3",
		},
		{
			name:  "with spaces",
			input: " field1 , field2 , field3 ",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := parsePartitionSpec(tt.input)
			if (err != nil) != tt.isErr {
				t.Errorf("parsePartitionSpec() error = %v, isErr %v", err, tt.isErr)

				return
			}
			if !tt.isErr && got == nil {
				t.Errorf("parsePartitionSpec() returned nil for valid input")
			}
		})
	}
}

func TestParseSortOrder(t *testing.T) {
	tests := []struct {
		name                string
		input               string
		isErr               bool
		expectedFieldsCount int
		expectedNullOrders  []table.NullOrder // for validation
		expectedDirections  []table.SortDirection
	}{
		{
			name:                "empty string",
			input:               "",
			expectedFieldsCount: 0,
		},
		{
			name:                "single field ascending (default null order)",
			input:               "field1:asc",
			expectedFieldsCount: 1,
			expectedDirections:  []table.SortDirection{table.SortASC},
			expectedNullOrders:  []table.NullOrder{table.NullsFirst},
		},
		{
			name:                "single field descending (default null order)",
			input:               "field1:desc",
			expectedFieldsCount: 1,
			expectedDirections:  []table.SortDirection{table.SortDESC},
			expectedNullOrders:  []table.NullOrder{table.NullsLast},
		},
		{
			name:                "single field with explicit nulls-first",
			input:               "field1:asc:nulls-first",
			expectedFieldsCount: 1,
			expectedDirections:  []table.SortDirection{table.SortASC},
			expectedNullOrders:  []table.NullOrder{table.NullsFirst},
		},
		{
			name:                "single field with explicit nulls-last",
			input:               "field1:desc:nulls-last",
			expectedFieldsCount: 1,
			expectedDirections:  []table.SortDirection{table.SortDESC},
			expectedNullOrders:  []table.NullOrder{table.NullsLast},
		},
		{
			name:                "asc with nulls-last (overriding default)",
			input:               "field1:asc:nulls-last",
			expectedFieldsCount: 1,
			expectedDirections:  []table.SortDirection{table.SortASC},
			expectedNullOrders:  []table.NullOrder{table.NullsLast},
		},
		{
			name:                "desc with nulls-first (overriding default)",
			input:               "field1:desc:nulls-first",
			expectedFieldsCount: 1,
			expectedDirections:  []table.SortDirection{table.SortDESC},
			expectedNullOrders:  []table.NullOrder{table.NullsFirst},
		},
		{
			name:                "multiple fields with mixed null orders",
			input:               "field1:asc,field2:desc:nulls-first,field3:asc:nulls-last",
			expectedFieldsCount: 3,
			expectedDirections:  []table.SortDirection{table.SortASC, table.SortDESC, table.SortASC},
			expectedNullOrders:  []table.NullOrder{table.NullsFirst, table.NullsFirst, table.NullsLast},
		},
		{
			name:                "with spaces",
			input:               " field1 : asc : nulls-last , field2 : desc ",
			expectedFieldsCount: 2,
			expectedDirections:  []table.SortDirection{table.SortASC, table.SortDESC},
			expectedNullOrders:  []table.NullOrder{table.NullsLast, table.NullsLast},
		},
		{
			name:  "invalid direction",
			input: "field1:invalid",
			isErr: true,
		},
		{
			name:  "invalid null order",
			input: "field1:asc:invalid-order",
			isErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := parseSortOrder(tt.input)
			if (err != nil) != tt.isErr {
				t.Errorf("parseSortOrder() error = %v, isErr %v", err, tt.isErr)

				return
			}
			if !tt.isErr {
				// For an empty string, we expect UnsortedSortOrder with OrderID 0
				if tt.input == "" {
					if got.OrderID != 0 {
						t.Errorf("parseSortOrder() for empty string should return OrderID 0, got %d", got.OrderID)
					}
				} else if got.OrderID == 0 {
					t.Errorf("parseSortOrder() returned invalid sort order for valid input")
				}

				// Validate the number of fields
				if len(got.Fields) != tt.expectedFieldsCount {
					t.Errorf("parseSortOrder() returned %d fields, expected %d", len(got.Fields), tt.expectedFieldsCount)

					return
				}

				// Validate sort directions and null orders
				for i, field := range got.Fields {
					if i < len(tt.expectedDirections) && field.Direction != tt.expectedDirections[i] {
						t.Errorf("parseSortOrder() field %d direction = %v, expected %v", i, field.Direction, tt.expectedDirections[i])
					}
					if i < len(tt.expectedNullOrders) && field.NullOrder != tt.expectedNullOrders[i] {
						t.Errorf("parseSortOrder() field %d null order = %v, expected %v", i, field.NullOrder, tt.expectedNullOrders[i])
					}
				}
			}
		})
	}
}
