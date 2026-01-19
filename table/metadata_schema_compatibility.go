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
	"cmp"
	"fmt"
	"slices"
	"strconv"
	"strings"

	"github.com/apache/iceberg-go"
)

type ErrIncompatibleSchema struct {
	fields        []IncompatibleField
	formatVersion int
}

func (e ErrIncompatibleSchema) Error() string {
	var problems strings.Builder
	for _, f := range e.fields {
		if f.UnsupportedType != nil {
			problems.WriteString(fmt.Sprintf("\n- invalid type for %s: %s is not supported until v%d", f.ColName, f.Field.Type, f.UnsupportedType.MinFormatVersion))
		}
		if f.InvalidDefault != nil {
			problems.WriteString(fmt.Sprintf("\n- invalid initial default for %s: non-null default (%v) is not supported until v%d", f.ColName, f.Field.InitialDefault, f.InvalidDefault.MinFormatVersion))
		}
	}

	return fmt.Sprintf("%s: for v%d:%s", iceberg.ErrInvalidSchema, e.formatVersion, problems.String())
}

func (e ErrIncompatibleSchema) Unwrap() error {
	return iceberg.ErrInvalidSchema
}

type IncompatibleField struct {
	Field           iceberg.NestedField
	ColName         string
	UnsupportedType *UnsupportedType
	InvalidDefault  *InvalidDefault
}

type UnsupportedType struct {
	MinFormatVersion int
}

type InvalidDefault struct {
	MinFormatVersion int
	WriteDefault     any
}

// checkSchemaCompatibility checks that the schema is compatible with the table's format version.
// This validates that the schema does not contain types or features that were released
// in later format versions.
// Java: Schema::checkCompatibility
func checkSchemaCompatibility(sc *iceberg.Schema, formatVersion int) error {
	const defaultValuesMinFormatVersion = 3
	problems := make([]IncompatibleField, 0)

	if err := validateUnknownTypes(sc); err != nil {
		return fmt.Errorf("failed to validate unknown types: %w", err)
	}

	if _, err := iceberg.IndexNameByID(sc); err != nil {
		return fmt.Errorf("invalid schema: %w", err)
	}

	fieldsIt, err := sc.FlatFields()
	if err != nil {
		return fmt.Errorf("failed to check Schema compatibility: %w", err)
	}

	for _, field := range slices.SortedFunc(fieldsIt, func(a, b iceberg.NestedField) int {
		return cmp.Compare(a.ID, b.ID)
	}) {
		colName, found := sc.FindColumnName(field.ID)
		if !found {
			panic("invalid schema: field with id " + strconv.Itoa(field.ID) + " not found, this is a bug, please report.")
		}

		minFormatVersion := minFormatVersionForType(field.Type)
		if formatVersion < minFormatVersion {
			problems = append(problems, IncompatibleField{
				Field:           field,
				ColName:         colName,
				UnsupportedType: &UnsupportedType{MinFormatVersion: minFormatVersion},
			})
		}

		if field.InitialDefault != nil && formatVersion < defaultValuesMinFormatVersion {
			problems = append(problems, IncompatibleField{
				Field:          field,
				ColName:        colName,
				InvalidDefault: &InvalidDefault{MinFormatVersion: defaultValuesMinFormatVersion, WriteDefault: field.InitialDefault},
			})
		}
	}

	if len(problems) != 0 {
		return ErrIncompatibleSchema{fields: problems, formatVersion: formatVersion}
	}

	return nil
}

// minFormatVersionForType returns the minimum table format version required
// for the given type. Returns 1 for types supported in all versions, or a higher
// version number for types that require newer format versions.
func minFormatVersionForType(t iceberg.Type) int {
	switch t.(type) {
	case iceberg.TimestampNsType, iceberg.TimestampTzNsType, iceberg.UnknownType:
		return 3
	default:
		// All other types supported in v1+
		return 1
	}
}

func validateUnknownTypes(sc *iceberg.Schema) error {
	validator := &unknownTypeValidator{}
	result, err := iceberg.Visit(sc, validator)
	if err != nil {
		return err
	}

	return result
}

type unknownTypeValidator struct{}

func (v *unknownTypeValidator) Schema(_ *iceberg.Schema, structResult error) error {
	return structResult
}

func (v *unknownTypeValidator) Struct(_ iceberg.StructType, fieldResults []error) error {
	for _, err := range fieldResults {
		if err != nil {
			return err
		}
	}

	return nil
}

func (v *unknownTypeValidator) Field(field iceberg.NestedField, fieldResult error) error {
	if fieldResult != nil {
		return fieldResult
	}
	if _, isUnknown := field.Type.(iceberg.UnknownType); isUnknown {
		if field.Required {
			return fmt.Errorf("unknown type field '%s' (id: %d) must be optional, but was marked as required", field.Name, field.ID)
		}
		if field.InitialDefault != nil {
			return fmt.Errorf("unknown type field '%s' (id: %d) must have null initial-default, but got: %v", field.Name, field.ID, field.InitialDefault)
		}
		if field.WriteDefault != nil {
			return fmt.Errorf("unknown type field '%s' (id: %d) must have null write-default, but got: %v", field.Name, field.ID, field.WriteDefault)
		}
	}

	return nil
}

func (v *unknownTypeValidator) List(list iceberg.ListType, elemResult error) error {
	if elemResult != nil {
		return elemResult
	}
	elem := list.ElementField()

	if _, isUnknown := elem.Type.(iceberg.UnknownType); isUnknown {
		if elem.Required {
			return fmt.Errorf("unknown type field '%s' (id: %d) must be optional, but was marked required", elem.Name, elem.ID)
		}
		if elem.InitialDefault != nil {
			return fmt.Errorf("unknown type field '%s' (id: %d) must have null-initial-default, but got: %v", elem.Name, elem.ID, elem.InitialDefault)
		}
		if elem.WriteDefault != nil {
			return fmt.Errorf("unknown type field '%s' (id: %d) must have null write-default but got: %v", elem.Name, elem.ID, elem.WriteDefault)
		}
	}

	return nil
}

func (v *unknownTypeValidator) Map(mapType iceberg.MapType, keyResult, valueResult error) error {
	if keyResult != nil {
		return keyResult
	}

	if valueResult != nil {
		return valueResult
	}

	key := mapType.KeyField()

	if _, isUnknown := key.Type.(iceberg.UnknownType); isUnknown {
		if key.Required {
			return fmt.Errorf("unknown type field '%s' (id: %d) must be optional, but was marked required", key.Name, key.ID)
		}
		if key.InitialDefault != nil {
			return fmt.Errorf("unknown type field '%s' (id: %d) must have null-initial-default, but got: %v", key.Name, key.ID, key.InitialDefault)
		}
		if key.WriteDefault != nil {
			return fmt.Errorf("unknown type field '%s' (id: %d) must have null write-default but got: %v", key.Name, key.ID, key.WriteDefault)
		}
	}

	value := mapType.ValueField()
	if _, isUnknown := value.Type.(iceberg.UnknownType); isUnknown {
		if value.Required {
			return fmt.Errorf("unknown type field '%s' (id: %d) must be optional, but was marked required", value.Name, value.ID)
		}
		if value.InitialDefault != nil {
			return fmt.Errorf("unknown type field '%s' (id: %d) must have null-initial-default, but got: %v", value.Name, value.ID, value.InitialDefault)
		}
		if value.WriteDefault != nil {
			return fmt.Errorf("unknown type field '%s' (id: %d) must have null write-default but got: %v", value.Name, value.ID, value.WriteDefault)
		}
	}

	return nil
}

func (v *unknownTypeValidator) Primitive(_ iceberg.PrimitiveType) error {
	return nil
}
