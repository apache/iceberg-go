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
	"slices"

	"github.com/apache/iceberg-go/internal"
)

// CloneDefaultValue returns a deep copy of an Iceberg initial or write default value.
func CloneDefaultValue(value any) any {
	switch value := value.(type) {
	case []byte:
		return slices.Clone(value)
	case BinaryLiteral:
		return BinaryLiteral(slices.Clone([]byte(value)))
	case FixedLiteral:
		return FixedLiteral(slices.Clone([]byte(value)))
	case []any:
		cloned := make([]any, len(value))
		for i, item := range value {
			cloned[i] = CloneDefaultValue(item)
		}

		return cloned
	case map[string]any:
		cloned := make(map[string]any, len(value))
		for key, item := range value {
			cloned[key] = CloneDefaultValue(item)
		}

		return cloned
	default:
		// Iceberg scalar defaults are value types (bool, numeric values,
		// strings, UUIDs, and Decimals). Any mutable reference type must get
		// an explicit deep-copy case above.
		return value
	}
}

// hexDefault returns the hex single-value form of a binary or fixed default
// given as bytes. It reports false for any other type or value, including a
// default that is already a string.
func hexDefault(typ Type, value any) (string, bool) {
	switch typ.(type) {
	case BinaryType, FixedType:
		switch value := value.(type) {
		case []byte:
			return internal.EncodeDefaultBytes(value), true
		case BinaryLiteral:
			return internal.EncodeDefaultBytes(value), true
		case FixedLiteral:
			return internal.EncodeDefaultBytes(value), true
		}
	}

	return "", false
}

// normalizeDefaults stores binary and fixed defaults in their hex single-value
// form, the same form metadata JSON decodes to, so a schema holds no byte
// slices that callers could share or change. When something is rewritten it
// copies the fields and nested types on the path to that default and reports
// true; otherwise it returns fields itself. The input is never modified.
func normalizeDefaults(fields []NestedField) ([]NestedField, bool) {
	var out []NestedField
	for i, field := range fields {
		normalized, ok := normalizeFieldDefaults(field)
		if !ok {
			continue
		}
		if out == nil {
			out = slices.Clone(fields)
		}
		out[i] = normalized
	}
	if out == nil {
		return fields, false
	}

	return out, true
}

func normalizeFieldDefaults(field NestedField) (NestedField, bool) {
	changed := false
	if value, ok := hexDefault(field.Type, field.InitialDefault); ok {
		field.InitialDefault, changed = value, true
	}
	if value, ok := hexDefault(field.Type, field.WriteDefault); ok {
		field.WriteDefault, changed = value, true
	}
	if typ, ok := normalizeTypeDefaults(field.Type); ok {
		field.Type, changed = typ, true
	}

	return field, changed
}

func normalizeTypeDefaults(typ Type) (Type, bool) {
	switch typ := typ.(type) {
	case *StructType:
		if typ == nil {
			return nil, false
		}
		fields, ok := normalizeDefaults(typ.FieldList)
		if !ok {
			return nil, false
		}

		return &StructType{FieldList: fields}, true
	case *ListType:
		if typ == nil {
			return nil, false
		}
		elem, ok := normalizeTypeDefaults(typ.Element)
		if !ok {
			return nil, false
		}
		cloned := *typ
		cloned.Element = elem

		return &cloned, true
	case *MapType:
		if typ == nil {
			return nil, false
		}
		key, keyOK := normalizeTypeDefaults(typ.KeyType)
		value, valueOK := normalizeTypeDefaults(typ.ValueType)
		if !keyOK && !valueOK {
			return nil, false
		}
		cloned := *typ
		if keyOK {
			cloned.KeyType = key
		}
		if valueOK {
			cloned.ValueType = value
		}

		return &cloned, true
	}

	return nil, false
}
