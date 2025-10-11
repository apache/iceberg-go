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
		if f.unsupportedType != nil {
			problems.WriteString(fmt.Sprintf("\n- invalid type for %s: %s is not supported until v%d", f.colName, f.field.Type, f.unsupportedType.minFormatVersion))
		}
		if f.invalidDefault != nil {
			problems.WriteString(fmt.Sprintf("\n- invalid initial default for %s: non-null default (%v) is not supported until v%d", f.colName, f.field.InitialDefault, f.invalidDefault.minFormatVersion))
		}
	}

	return fmt.Sprintf("%s: for v%d:%s", iceberg.ErrInvalidSchema, e.formatVersion, problems.String())
}

func (e ErrIncompatibleSchema) Unwrap() error {
	return iceberg.ErrInvalidSchema
}

type IncompatibleField struct {
	field           iceberg.NestedField
	colName         string
	unsupportedType *UnsupportedType
	invalidDefault  *InvalidDefault
}

type UnsupportedType struct {
	minFormatVersion int
}

type InvalidDefault struct {
	minFormatVersion int
	writeDefault     any
}

// checkSchemaCompatibility checks that the schema is compatible with the table's format version.
// This validates that the schema does not contain types or features that were released
// in later format versions.
// Java: Schema::checkCompatibility
func checkSchemaCompatibility(sc *iceberg.Schema, formatVersion int) error {
	const defaultValuesMinFormatVersion = 3
	problems := make([]IncompatibleField, 0)

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
				field:           field,
				colName:         colName,
				unsupportedType: &UnsupportedType{minFormatVersion: minFormatVersion},
			})
		}

		if field.InitialDefault != nil && formatVersion < defaultValuesMinFormatVersion {
			problems = append(problems, IncompatibleField{
				field:          field,
				colName:        colName,
				invalidDefault: &InvalidDefault{minFormatVersion: defaultValuesMinFormatVersion, writeDefault: field.InitialDefault},
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
	case iceberg.TimestampNsType, iceberg.TimestampTzNsType:
		return 3
	default:
		// All other types supported in v1+
		return 1
	}
}
