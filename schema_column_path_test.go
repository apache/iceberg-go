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
	"strconv"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestColumnPathSegmentsNested(t *testing.T) {
	t.Parallel()

	paths := [][]string{
		{"payload"},
		{"a.b"},
		{"wrap.per", "pay.load"},
		{"根", "子.字段", "载荷"},
	}
	for _, depth := range []int{4, 8, 32, 64} {
		names := make([]string, depth)
		for i := range names {
			names[i] = fmt.Sprintf("level.%d", i)
		}
		paths = append(paths, names)
	}
	for _, names := range paths {
		t.Run(strings.Join(names, "/"), func(t *testing.T) {
			t.Parallel()
			schema := columnPathTestSchema(names)
			for id := 1; id <= len(names); id++ {
				path := schema.columnPathSegments(id)
				require.Equal(t, names[:id], path)
				path[0] = "changed"
				assert.Equal(t, names[:id], schema.columnPathSegments(id))
			}
			for _, id := range []int{-1, 0, len(names) + 1} {
				assert.Nil(t, schema.columnPathSegments(id))
			}
		})
	}

	t.Run("empty schema", func(t *testing.T) {
		assert.Nil(t, NewSchema(0).columnPathSegments(1))
	})
}

func TestColumnPathSegmentsContainers(t *testing.T) {
	t.Parallel()

	schema := NewSchema(0,
		NestedField{ID: 1, Name: "items", Type: &ListType{
			ElementID: 2,
			Element:   &StructType{FieldList: []NestedField{{ID: 3, Name: "pay.load", Type: VariantType{}}}},
		}},
		NestedField{ID: 4, Name: "attributes", Type: &MapType{
			KeyID: 5, KeyType: PrimitiveTypes.String,
			ValueID: 6, ValueType: &StructType{FieldList: []NestedField{{ID: 7, Name: "pay.load", Type: VariantType{}}}},
		}},
	)
	for _, tt := range []struct {
		id   int
		path []string
	}{
		{1, []string{"items"}},
		{2, []string{"items", "element"}},
		{3, []string{"items", "element", "pay.load"}},
		{4, []string{"attributes"}},
		{5, []string{"attributes", "key"}},
		{6, []string{"attributes", "value"}},
		{7, []string{"attributes", "value", "pay.load"}},
	} {
		t.Run(strconv.Itoa(tt.id), func(t *testing.T) {
			t.Parallel()
			assert.Equal(t, tt.path, schema.columnPathSegments(tt.id))
		})
	}
}

func TestTranslateColumnNamesForScanNestedSourcePath(t *testing.T) {
	t.Parallel()

	for _, names := range [][]string{
		{"pay.load"},
		{"wrap.per", "pay.load"},
		{"根", "子.字段", "载荷"},
	} {
		t.Run(strings.Join(names, "/"), func(t *testing.T) {
			t.Parallel()
			schema := columnPathTestSchema(names)
			bound, err := EqualTo(Extract(Reference(strings.Join(names, ".")), "$.value", PrimitiveTypes.Int64), int64(7)).Bind(schema, true)
			require.NoError(t, err)
			translated, columns, err := TranslateColumnNamesForScan(bound, schema)
			require.NoError(t, err)
			require.Len(t, columns, 1)
			assert.Equal(t, names, columns[0].SourcePath)
			assert.True(t, EqualTo(Reference(columns[0].Name), int64(7)).Equals(translated))
		})
	}
}

func columnPathTestSchema(names []string) *Schema {
	field := NestedField{ID: len(names), Name: names[len(names)-1], Type: VariantType{}}
	for i := len(names) - 2; i >= 0; i-- {
		field = NestedField{ID: i + 1, Name: names[i], Type: &StructType{FieldList: []NestedField{field}}}
	}

	return NewSchema(0, field)
}
