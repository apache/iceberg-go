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

package iceberg

import (
	"fmt"
	"strings"
)

// IsPromotionAllowed reports whether schema evolution may change a column
// from one type to another: int to long, float to double, or widening a
// decimal's precision with the same scale.
func IsPromotionAllowed(from, to PrimitiveType) bool {
	if from.Equals(to) {
		return true
	}

	switch f := from.(type) {
	case Int32Type:
		_, ok := to.(Int64Type)

		return ok
	case Float32Type:
		_, ok := to.(Float64Type)

		return ok
	case DecimalType:
		t, ok := to.(DecimalType)
		if !ok {
			return false
		}

		return f.Scale() == t.Scale() && f.Precision() <= t.Precision()
	}

	return false
}

// ReadCompatibilityErrors returns the problems with reading data written
// with writeSchema using readSchema. Fields are matched by ID and field
// order is not checked.
func ReadCompatibilityErrors(readSchema, writeSchema *Schema) ([]string, error) {
	return checkCompatibility(readSchema, writeSchema, false, true)
}

// WriteCompatibilityErrors returns the problems with writing data in
// writeSchema to a table whose schema is readSchema. If checkOrdering is
// set, reordered fields are also reported.
func WriteCompatibilityErrors(readSchema, writeSchema *Schema, checkOrdering bool) ([]string, error) {
	return checkCompatibility(readSchema, writeSchema, checkOrdering, true)
}

// TypeCompatibilityErrors is WriteCompatibilityErrors without the
// nullability checks.
func TypeCompatibilityErrors(readSchema, writeSchema *Schema, checkOrdering bool) ([]string, error) {
	return checkCompatibility(readSchema, writeSchema, checkOrdering, false)
}

func checkCompatibility(readSchema, writeSchema *Schema, checkOrdering, checkNullability bool) ([]string, error) {
	if writeSchema == nil {
		return nil, fmt.Errorf("%w: cannot check compatibility against nil schema", ErrInvalidArgument)
	}

	return PreOrderVisit(readSchema, &compatibilityChecker{
		schema:           writeSchema,
		checkOrdering:    checkOrdering,
		checkNullability: checkNullability,
	})
}

// compatibilityChecker walks the read schema, tracking the matching type in
// the write schema. Errors starting with ":" get the enclosing field's name
// prepended; others are joined to it with ".".
type compatibilityChecker struct {
	schema           *Schema
	checkOrdering    bool
	checkNullability bool

	current Type
	// PreOrderVisit sends list elements and map keys/values through Field,
	// but only struct fields should be looked up by ID.
	inContainer bool
}

func (c *compatibilityChecker) Schema(_ *Schema, structErrors func() []string) []string {
	st := c.schema.asStructRef()
	c.current = &st
	defer func() { c.current = nil }()

	return structErrors()
}

func (c *compatibilityChecker) Struct(readStruct StructType, fieldErrors []func() []string) []string {
	st, ok := c.current.(*StructType)
	if !ok {
		return []string{fmt.Sprintf(": %s cannot be read as a struct", c.current)}
	}

	var errs []string
	for _, fieldErrs := range fieldErrors {
		errs = append(errs, fieldErrs()...)
	}

	if c.checkOrdering {
		ordinals := make(map[int]int, len(st.FieldList))
		for i, f := range st.FieldList {
			ordinals[f.ID] = i
		}

		lastOrdinal := -1
		for _, readField := range readStruct.FieldList {
			ordinal, ok := ordinals[readField.ID]
			if !ok {
				continue
			}
			if lastOrdinal >= ordinal {
				errs = append(errs, fmt.Sprintf("%s is out of order, before %s",
					readField.Name, st.FieldList[lastOrdinal].Name))
			}
			lastOrdinal = ordinal
		}
	}

	return errs
}

func (c *compatibilityChecker) Field(readField NestedField, fieldErrors func() []string) []string {
	if c.inContainer {
		c.inContainer = false

		return fieldErrors()
	}

	st := c.current.(*StructType)
	var (
		writeField NestedField
		found      bool
	)
	for _, f := range st.FieldList {
		if f.ID == readField.ID {
			writeField, found = f, true

			break
		}
	}

	if !found {
		if readField.Required {
			return []string{readField.Name + " is required, but is missing"}
		}

		// an optional field is read as nulls
		return nil
	}

	c.current = writeField.Type
	defer func() { c.current = st }()

	var errs []string
	if c.checkNullability && readField.Required && !writeField.Required {
		errs = append(errs, readField.Name+" should be required, but is optional")
	}

	for _, err := range fieldErrors() {
		if strings.HasPrefix(err, ":") {
			errs = append(errs, readField.Name+err)
		} else {
			errs = append(errs, readField.Name+"."+err)
		}
	}

	return errs
}

func (c *compatibilityChecker) List(readList ListType, elementErrors func() []string) []string {
	list, ok := c.current.(*ListType)
	if !ok {
		return []string{fmt.Sprintf(": %s cannot be read as a list", c.current)}
	}

	var errs []string
	if readList.ElementRequired && !list.ElementRequired {
		errs = append(errs, ": elements should be required, but are optional")
	}

	c.current, c.inContainer = list.Element, true
	defer func() { c.current = list }()

	return append(errs, elementErrors()...)
}

func (c *compatibilityChecker) Map(readMap MapType, keyErrors, valueErrors func() []string) []string {
	m, ok := c.current.(*MapType)
	if !ok {
		return []string{fmt.Sprintf(": %s cannot be read as a map", c.current)}
	}
	defer func() { c.current = m }()

	var errs []string
	if readMap.ValueRequired && !m.ValueRequired {
		errs = append(errs, ": values should be required, but are optional")
	}

	c.current, c.inContainer = m.KeyType, true
	errs = append(errs, keyErrors()...)

	c.current, c.inContainer = m.ValueType, true

	return append(errs, valueErrors()...)
}

func (c *compatibilityChecker) Primitive(readPrimitive PrimitiveType) []string {
	if c.current.Equals(readPrimitive) {
		return nil
	}

	writePrimitive, ok := c.current.(PrimitiveType)
	if !ok {
		return []string{fmt.Sprintf(": %s cannot be read as a %s", c.current.Type(), readPrimitive)}
	}

	if !IsPromotionAllowed(writePrimitive, readPrimitive) {
		return []string{fmt.Sprintf(": %s cannot be promoted to %s", writePrimitive, readPrimitive)}
	}

	return nil
}

func (c *compatibilityChecker) Variant(readVariant VariantType) []string {
	if _, ok := c.current.(VariantType); ok {
		return nil
	}

	// promotion to variant is not allowed
	return []string{fmt.Sprintf(": %s cannot be read as a %s", c.current, readVariant)}
}
