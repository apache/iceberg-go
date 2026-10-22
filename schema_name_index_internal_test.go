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
