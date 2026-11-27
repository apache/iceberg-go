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

package internal_test

import (
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/iceberg-go/table/internal"
	"github.com/stretchr/testify/assert"
)

func withID(f arrow.Field, id string) arrow.Field {
	f.Metadata = arrow.NewMetadata([]string{"PARQUET:field_id"}, []string{id})

	return f
}

func TestHasAnyFieldID(t *testing.T) {
	str := arrow.BinaryTypes.String
	noID := arrow.Field{Name: "x", Type: str, Nullable: true}
	// pqarrow reports a missing Parquet field ID as -1.
	minusOne := withID(noID, "-1")

	tests := []struct {
		name   string
		fields []arrow.Field
		want   bool
	}{
		{"no fields", nil, false},
		{"no ids", []arrow.Field{noID, minusOne}, false},
		{"top level", []arrow.Field{noID, withID(noID, "1")}, true},
		{"struct field only", []arrow.Field{{Name: "s", Type: arrow.StructOf(withID(noID, "2"))}}, true},
		{"list element only", []arrow.Field{{Name: "l", Type: arrow.ListOfField(withID(noID, "3"))}}, true},
		{"map key only", []arrow.Field{{Name: "m", Type: arrow.MapOfFields(
			withID(arrow.Field{Name: "key", Type: str}, "4"), arrow.Field{Name: "value", Type: str, Nullable: true})}}, true},
		{"map value only", []arrow.Field{{Name: "m", Type: arrow.MapOfFields(
			arrow.Field{Name: "key", Type: str}, withID(arrow.Field{Name: "value", Type: str, Nullable: true}, "5"))}}, true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.want, internal.HasAnyFieldID(arrow.NewSchema(tt.fields, nil)))
		})
	}
}

func TestDropFieldsWithoutIDs(t *testing.T) {
	str := arrow.BinaryTypes.String
	elem := arrow.Field{Name: "element", Type: str, Nullable: true}
	key := arrow.Field{Name: "key", Type: str}
	val := arrow.Field{Name: "value", Type: str, Nullable: true}

	fullList := arrow.ListOfField(withID(elem, "10"))
	fullMap := arrow.MapOfFields(withID(key, "11"), withID(val, "12"))
	listOfBrokenList := arrow.ListOfField(withID(
		arrow.Field{Name: "element", Type: arrow.ListOfField(elem), Nullable: true}, "13"))
	mapOfBrokenStruct := arrow.MapOfFields(withID(key, "14"), withID(arrow.Field{
		Name: "value", Nullable: true,
		Type: arrow.StructOf(withID(arrow.Field{Name: "kept", Type: str}, "16"), arrow.Field{Name: "dropped", Type: str}),
	}, "15"))

	in := arrow.NewSchema([]arrow.Field{
		withID(arrow.Field{Name: "a", Type: str}, "1"),
		{Name: "no_id", Type: str},
		withID(arrow.Field{Name: "list", Type: fullList}, "2"),
		withID(arrow.Field{Name: "list_no_elem_id", Type: arrow.ListOfField(elem)}, "3"),
		withID(arrow.Field{Name: "large_list_no_elem_id", Type: arrow.LargeListOfField(elem)}, "4"),
		withID(arrow.Field{Name: "map", Type: fullMap}, "5"),
		withID(arrow.Field{Name: "map_no_key_id", Type: arrow.MapOfFields(key, withID(val, "12"))}, "6"),
		withID(arrow.Field{Name: "map_no_value_id", Type: arrow.MapOfFields(withID(key, "11"), val)}, "7"),
		withID(arrow.Field{Name: "list_of_broken_list", Type: listOfBrokenList}, "8"),
		withID(arrow.Field{Name: "map_of_struct", Type: mapOfBrokenStruct}, "9"),
		{Name: "struct_no_id", Type: arrow.StructOf(withID(arrow.Field{Name: "child", Type: str}, "17"))},
		withID(arrow.Field{Name: "struct", Type: arrow.StructOf(
			withID(arrow.Field{Name: "kept", Type: str}, "19"),
			arrow.Field{Name: "dropped", Type: str},
			withID(arrow.Field{Name: "dropped_list", Type: arrow.ListOfField(elem)}, "20"),
		)}, "18"),
	}, nil)

	want := arrow.NewSchema([]arrow.Field{
		withID(arrow.Field{Name: "a", Type: str}, "1"),
		withID(arrow.Field{Name: "list", Type: fullList}, "2"),
		withID(arrow.Field{Name: "map", Type: fullMap}, "5"),
		withID(arrow.Field{Name: "map_of_struct", Type: arrow.MapOfFields(withID(key, "14"), withID(arrow.Field{
			Name: "value", Nullable: true,
			Type: arrow.StructOf(withID(arrow.Field{Name: "kept", Type: str}, "16")),
		}, "15"))}, "9"),
		withID(arrow.Field{Name: "struct", Type: arrow.StructOf(
			withID(arrow.Field{Name: "kept", Type: str}, "19"),
		)}, "18"),
	}, nil)

	got := internal.DropFieldsWithoutIDs(in)
	assert.Truef(t, want.Equal(got), "want %s\ngot %s", want, got)
}
