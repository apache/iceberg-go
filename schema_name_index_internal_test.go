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
	"errors"
	"reflect"
	"strconv"
	"strings"
	"testing"
)

func TestIndexByNameNestedListAliases(t *testing.T) {
	schema := NewSchema(1,
		NestedField{ID: 1, Name: "payload", Type: &StructType{FieldList: []NestedField{
			{ID: 2, Name: "list", Type: &ListType{
				ElementID: 3,
				Element: &StructType{FieldList: []NestedField{
					{ID: 4, Name: "alpha", Type: PrimitiveTypes.Int64},
					{ID: 5, Name: "hidden", Type: &StructType{FieldList: []NestedField{
						{ID: 6, Name: "beta", Type: PrimitiveTypes.String},
					}}},
				}},
			}},
		}}},
		NestedField{ID: 7, Name: "sibling", Type: PrimitiveTypes.String},
		NestedField{ID: 8, Name: "lookup", Type: &MapType{
			KeyID: 9, KeyType: PrimitiveTypes.String,
			ValueID: 10, ValueType: &StructType{FieldList: []NestedField{
				{ID: 11, Name: "東京", Type: PrimitiveTypes.Int32},
			}},
		}},
	)

	got, err := IndexByName(schema)
	if err != nil {
		t.Fatal(err)
	}
	want := map[string]int{
		"payload":                          1,
		"payload.list":                     2,
		"payload.list.element":             3,
		"payload.list.element.alpha":       4,
		"payload.list.alpha":               4,
		"payload.list.element.hidden":      5,
		"payload.list.hidden":              5,
		"payload.list.element.hidden.beta": 6,
		"payload.list.hidden.beta":         6,
		"sibling":                          7,
		"lookup":                           8,
		"lookup.key":                       9,
		"lookup.value":                     10,
		"lookup.value.東京":                  11,
	}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("IndexByName() = %v, want %v", got, want)
	}

	gotIDs, err := IndexNameByID(schema)
	if err != nil {
		t.Fatal(err)
	}
	if gotIDs[4] != "payload.list.element.alpha" || gotIDs[6] != "payload.list.element.hidden.beta" {
		t.Errorf("IndexNameByID() lost full list element paths: %v", gotIDs)
	}
	if len(gotIDs) != 11 {
		t.Errorf("IndexNameByID() has %d entries, want 11", len(gotIDs))
	}
}

func TestIndexByNameDeepSiblingRestoration(t *testing.T) {
	fields := []NestedField{{ID: 1, Name: "leaf", Type: PrimitiveTypes.Int64}}
	parts := []string{"leaf"}
	for depth := range 32 {
		name := "nested_" + strconv.Itoa(depth)
		fields = []NestedField{{
			ID: depth + 2, Name: name, Type: &StructType{FieldList: fields},
		}}
		parts = append([]string{name}, parts...)
	}
	fields = append(fields, NestedField{ID: 34, Name: "sibling", Type: PrimitiveTypes.String})
	schema := NewSchema(1, fields...)

	byName, err := IndexByName(schema)
	if err != nil {
		t.Fatal(err)
	}
	if got := byName[strings.Join(parts, ".")]; got != 1 {
		t.Errorf("deep field ID = %d, want 1", got)
	}
	if got := byName["sibling"]; got != 34 {
		t.Errorf("sibling ID = %d, want 34", got)
	}

	byID, err := IndexNameByID(schema)
	if err != nil {
		t.Fatal(err)
	}
	if got := byID[1]; got != strings.Join(parts, ".") {
		t.Errorf("deep field name = %q, want %q", got, strings.Join(parts, "."))
	}
	if got := byID[34]; got != "sibling" {
		t.Errorf("sibling name = %q, want sibling", got)
	}
}

func TestLazyNameToIDLowerRejectsOnlyAmbiguousNames(t *testing.T) {
	schema := NewSchema(0,
		NestedField{ID: 1, Name: "ID", Type: PrimitiveTypes.Int64},
		NestedField{ID: 2, Name: "id", Type: PrimitiveTypes.Int64},
		NestedField{ID: 3, Name: "unique", Type: PrimitiveTypes.Int64},
	)

	index, err := schema.lazyNameToIDLower()
	if err != nil {
		t.Fatalf("lazyNameToIDLower() error = %v, want success", err)
	}
	if len(index) != 2 {
		t.Fatalf("lazyNameToIDLower() has %d names, want 2", len(index))
	}

	for _, name := range []string{"id", "ID", "Id"} {
		_, found, err := schema.findFieldByNameCaseInsensitive(name)
		if found || !errors.Is(err, ErrInvalidSchema) ||
			!strings.Contains(err.Error(), `"ID" and "id"`) {
			t.Errorf("findFieldByNameCaseInsensitive(%q) = %v, %v; want named collision", name, found, err)
		}
		if _, ok := schema.FindFieldByNameCaseInsensitive(name); ok {
			t.Errorf("FindFieldByNameCaseInsensitive(%q) resolved an ambiguous name", name)
		}
		if _, err := Reference(name).Bind(schema, false); !errors.Is(err, ErrInvalidSchema) ||
			!strings.Contains(err.Error(), `"ID" and "id"`) {
			t.Errorf("Reference(%q).Bind() = %v, want named collision", name, err)
		}
		if _, err := schema.Select(false, name); !errors.Is(err, ErrInvalidSchema) ||
			!strings.Contains(err.Error(), `"ID" and "id"`) {
			t.Errorf("Select(false, %q) = %v, want named collision", name, err)
		}
		if _, err := schema.Select(false, "UnIqUe", name); !errors.Is(err, ErrInvalidSchema) ||
			!strings.Contains(err.Error(), `"ID" and "id"`) {
			t.Errorf("Select(false, unique, %q) = %v, want named collision", name, err)
		}
	}

	if got, ok := schema.FindFieldByNameCaseInsensitive("UnIqUe"); !ok || got.ID != 3 {
		t.Errorf("FindFieldByNameCaseInsensitive(unique) = %v, %v; want field 3", got, ok)
	}
	if _, err := Reference("UNIQUE").Bind(schema, false); err != nil {
		t.Errorf("Reference(unique).Bind() error = %v, want success", err)
	}
	selected, err := schema.Select(false, "UnIqUe")
	if err != nil {
		t.Fatalf("Select(false, unique) error = %v, want success", err)
	}
	if got, ok := selected.FindFieldByName("unique"); !ok || got.ID != 3 {
		t.Errorf("Select(false, unique) selected %v, %v; want field 3", got, ok)
	}

	if _, err := Reference("ID").Bind(schema, true); err != nil {
		t.Errorf("case-sensitive Reference.Bind() error = %v, want success", err)
	}
}

func TestCaseInsensitiveFieldLookupRejectsNestedCollisions(t *testing.T) {
	schema := NewSchema(0, NestedField{
		ID: 1, Name: "person", Required: true,
		Type: NewStructType(
			NestedField{ID: 2, Name: "Age", Type: PrimitiveTypes.Int32, Required: true},
			NestedField{ID: 3, Name: "age", Type: PrimitiveTypes.String, Required: true},
			NestedField{ID: 4, Name: "City", Type: PrimitiveTypes.String},
		),
	})

	for range 8 {
		t.Run("concurrent lookups", func(t *testing.T) {
			t.Parallel()
			_, found, err := schema.findFieldByNameCaseInsensitive("PERSON.AGE")
			if found || !errors.Is(err, ErrInvalidSchema) ||
				!strings.Contains(err.Error(), `"person.Age" and "person.age"`) {
				t.Errorf("findFieldByNameCaseInsensitive(PERSON.AGE) = %v, %v; want named collision", found, err)
			}
			if _, found := schema.FindTypeByNameCaseInsensitive("PERSON.AGE"); found {
				t.Error("FindTypeByNameCaseInsensitive resolved an ambiguous child")
			}
			if _, err := schema.Select(false, "person.age"); !errors.Is(err, ErrInvalidSchema) {
				t.Errorf("Select(false, person.age) = %v; want invalid schema", err)
			}
			if _, err := Reference("PERSON.AGE").Bind(schema, false); !errors.Is(err, ErrInvalidSchema) {
				t.Errorf("Reference(PERSON.AGE).Bind() = %v; want invalid schema", err)
			}

			if parent, found := schema.FindFieldByNameCaseInsensitive("PERSON"); !found || parent.ID != 1 {
				t.Errorf("FindFieldByNameCaseInsensitive(PERSON) = %v, %v; want field 1", parent, found)
			}
			if city, found := schema.FindFieldByNameCaseInsensitive("PERSON.CITY"); !found || city.ID != 4 {
				t.Errorf("FindFieldByNameCaseInsensitive(PERSON.CITY) = %v, %v; want field 4", city, found)
			}
			selected, err := schema.Select(false, "PERSON.CITY")
			if err != nil {
				t.Fatalf("Select(false, PERSON.CITY) = %v; want success", err)
			}
			if _, found := selected.FindFieldByID(4); !found {
				t.Error("Select(false, PERSON.CITY) did not select field 4")
			}
		})
	}
}

func TestNameIndexPathEmptySegments(t *testing.T) {
	var path nameIndexPath
	path.push("")
	if got := path.prefixedName("leaf"); got != ".leaf" {
		t.Errorf("empty prefix: got %q, want .leaf", got)
	}
	path.push("inner")
	if got := path.prefixedName("leaf"); got != ".inner.leaf" {
		t.Errorf("nested empty prefix: got %q, want .inner.leaf", got)
	}
	path.pop()
	if got := path.prefixedName("other"); got != ".other" {
		t.Errorf("after pop: got %q, want .other", got)
	}
	path.pop()
	if got := path.prefixedName("top"); got != "top" {
		t.Errorf("empty stack: got %q, want top", got)
	}
}
