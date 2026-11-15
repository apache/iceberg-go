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
	"slices"
	"sort"

	"github.com/apache/iceberg-go"
	iceinternal "github.com/apache/iceberg-go/internal"
)

// unifiedPartitionType returns the table-wide partition type. It contains the
// union of partition fields from every spec, which lets metadata tables
// represent live files written before partition evolution.
func unifiedPartitionType(metadata Metadata) (*iceberg.StructType, error) {
	return unifiedPartitionTypeWithSchema(metadata, metadata.CurrentSchema())
}

func unifiedPartitionTypeWithSchema(
	metadata Metadata,
	currentSchema *iceberg.Schema,
) (*iceberg.StructType, error) {
	specs := metadata.PartitionSpecs()
	sort.Slice(specs, func(left, right int) bool {
		return specs[left].ID() > specs[right].ID()
	})

	selected := make(map[int]iceberg.PartitionField)
	fieldsByID := make(map[int]iceberg.NestedField)
	for _, spec := range specs {
		for _, field := range spec.Fields() {
			if isUnknownPartitionTransform(field.Transform) {
				return nil, fmt.Errorf("%w: cannot build metadata partition type for field %d with unknown transform %s",
					iceberg.ErrInvalidPartitionSpec, field.FieldID, field.Transform)
			}
		}

		partitionType := spec.PartitionType(currentSchema)
		for idx, field := range spec.Fields() {
			active := true
			for _, sourceID := range field.SourceIDs {
				// This is an existence check only. FindTypeByID clones the
				// complete nested type on every call, which is wasteful while
				// building the union of position-delete partition fields.
				if _, ok := currentSchema.FindFieldByIDRef(sourceID, iceinternal.SchemaRef{}); !ok {
					active = false

					break
				}
			}
			if !active || idx >= partitionType.NumFields() {
				continue
			}

			if previous, exists := selected[field.FieldID]; exists {
				if !slices.Equal(previous.SourceIDs, field.SourceIDs) {
					return nil, fmt.Errorf("%w: partition field ID %d has incompatible source IDs %v and %v",
						iceberg.ErrInvalidPartitionSpec, field.FieldID, previous.SourceIDs, field.SourceIDs)
				}

				previousVoid := isVoidPartitionTransform(previous.Transform)
				fieldVoid := isVoidPartitionTransform(field.Transform)
				if previousVoid || fieldVoid {
					if previousVoid && !fieldVoid {
						selected[field.FieldID] = field
						old := fieldsByID[field.FieldID]
						old.Type = partitionType.Field(idx).Type
						fieldsByID[field.FieldID] = old
					}

					continue
				}

				if !previous.Transform.Equals(field.Transform) {
					return nil, fmt.Errorf("%w: partition field ID %d has incompatible transforms %q and %q",
						iceberg.ErrInvalidPartitionSpec, field.FieldID, previous.Transform, field.Transform)
				}

				continue
			}

			selected[field.FieldID] = field
			fieldsByID[field.FieldID] = iceberg.NestedField{
				ID:       field.FieldID,
				Name:     field.Name,
				Type:     partitionType.Field(idx).Type,
				Required: false,
			}
		}
	}

	fields := make([]iceberg.NestedField, 0, len(fieldsByID))
	for _, field := range fieldsByID {
		fields = append(fields, field)
	}
	sort.Slice(fields, func(left, right int) bool { return fields[left].ID < fields[right].ID })

	return iceberg.NewStructType(fields...), nil
}

func isVoidPartitionTransform(transform iceberg.Transform) bool {
	switch transform.(type) {
	case iceberg.VoidTransform, *iceberg.VoidTransform:
		return true
	default:
		return false
	}
}

func isUnknownPartitionTransform(transform iceberg.Transform) bool {
	switch transform.(type) {
	case iceberg.UnknownTransform, *iceberg.UnknownTransform:
		return true
	default:
		return false
	}
}
