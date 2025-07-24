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
	"fmt"
	"strings"

	"github.com/apache/iceberg-go"
	"github.com/apache/iceberg-go/table"
)

func parseProperties(propStr string) (iceberg.Properties, error) {
	if propStr == "" {
		return iceberg.Properties{}, nil
	}
	props := make(iceberg.Properties)
	pairs := strings.Split(propStr, ",")

	for _, pair := range pairs {
		parts := strings.SplitN(strings.TrimSpace(pair), "=", 2)
		if len(parts) != 2 {
			return nil, fmt.Errorf("invalid property pair: %s (expected key=value)", pair)
		}
		key := strings.TrimSpace(parts[0])
		value := strings.TrimSpace(parts[1])
		if key == "" {
			return nil, fmt.Errorf("property key cannot be empty in: %s", pair)
		}
		props[key] = value
	}

	return props, nil
}

func parsePartitionSpec(specStr string) (*iceberg.PartitionSpec, error) {
	if specStr == "" {
		return iceberg.UnpartitionedSpec, nil
	}

	fields := strings.Split(specStr, ",")
	var partitionFields []iceberg.PartitionField

	for i, field := range fields {
		field = strings.TrimSpace(field)
		if field == "" {
			continue
		}

		partitionFields = append(partitionFields, iceberg.PartitionField{
			SourceID:  i + 1,
			FieldID:   i + iceberg.PartitionDataIDStart,
			Name:      field,
			Transform: iceberg.IdentityTransform{},
		})
	}
	if len(partitionFields) == 0 {
		return iceberg.UnpartitionedSpec, nil
	}
	spec := iceberg.NewPartitionSpec(partitionFields...)

	return &spec, nil
}

func parseSortOrder(sortStr string) (table.SortOrder, error) {
	if sortStr == "" {
		return table.UnsortedSortOrder, nil
	}

	fields := strings.Split(sortStr, ",")
	var sortFields []table.SortField

	for i, field := range fields {
		field = strings.TrimSpace(field)
		if field == "" {
			continue
		}
		parts := strings.Split(field, ":")
		direction := "asc" // default value
		var nullOrder table.NullOrder

		if len(parts) > 1 {
			direction = strings.TrimSpace(parts[1])
		}

		var sortDirection table.SortDirection

		switch strings.ToLower(direction) {
		case "asc":
			sortDirection = table.SortASC
		case "desc":
			sortDirection = table.SortDESC
		default:
			return table.UnsortedSortOrder, fmt.Errorf("invalid sort direction: %s", direction)
		}

		// Parse null order
		if len(parts) > 2 {
			nullOrderStr := strings.TrimSpace(parts[2])
			switch strings.ToLower(nullOrderStr) {
			case "nulls-first":
				nullOrder = table.NullsFirst
			case "nulls-last":
				nullOrder = table.NullsLast
			default:
				return table.UnsortedSortOrder, fmt.Errorf("invalid null order: %s", nullOrderStr)
			}
		} else {
			if sortDirection == table.SortDESC {
				nullOrder = table.NullsLast
			} else {
				nullOrder = table.NullsFirst
			}
		}
		sortFields = append(sortFields, table.SortField{
			SourceID:  i + 1,
			Transform: iceberg.IdentityTransform{},
			Direction: sortDirection,
			NullOrder: nullOrder,
		})
	}
	if len(sortFields) == 0 {
		return table.UnsortedSortOrder, nil
	}

	return table.SortOrder{
		OrderID: table.InitialSortOrderID,
		Fields:  sortFields,
	}, nil
}
