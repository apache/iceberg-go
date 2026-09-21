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
	"testing"

	"github.com/apache/iceberg-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestRemotePlanningSelectedFieldsPreservesBorrowedSchema(t *testing.T) {
	t.Parallel()

	for _, warmCache := range []bool{false, true} {
		t.Run(fmt.Sprintf("warm_cache_%t", warmCache), func(t *testing.T) {
			t.Parallel()

			// Reuse the nested, default-heavy fixture from the wildcard benchmark.
			fields := remoteWildcardProjectionBenchmarkSchema(3).Fields()
			slices.Reverse(fields)
			schema := iceberg.NewSchema(0, fields...)
			before := schema.Fields()
			if warmCache {
				_, ok := schema.FindColumnName(1)
				require.True(t, ok)
			}

			want := []string{
				"field_00000",
				"field_00000.city",
				"field_00000.zip",
				"field_00001.element.city",
				"field_00001.element.zip",
				"field_00002.value.city",
				"field_00002.value.zip",
			}
			scan := &Scan{selectedFields: []string{"*"}, caseSensitive: true}
			got, err := remotePlanningSelectedFields(scan, schema)
			require.NoError(t, err)
			require.Equal(t, want, got)
			assert.Equal(t, before, schema.Fields())

			got[0] = "changed"
			name, ok := schema.FindColumnName(1)
			require.True(t, ok)
			assert.Equal(t, "field_00000", name)

			again, err := remotePlanningSelectedFields(scan, schema)
			require.NoError(t, err)
			assert.Equal(t, want, again)
			assert.Equal(t, before, schema.Fields())
		})
	}
}

func TestRemotePlanningSelectedFieldsConcurrentSchemaReads(t *testing.T) {
	t.Parallel()

	schema := remoteWildcardProjectionBenchmarkSchema(3)
	want := []string{
		"field_00000",
		"field_00000.city",
		"field_00000.zip",
		"field_00001.element.city",
		"field_00001.element.zip",
		"field_00002.value.city",
		"field_00002.value.zip",
	}
	for worker := range 8 {
		t.Run(fmt.Sprintf("reader_%d", worker), func(t *testing.T) {
			t.Parallel()

			scan := &Scan{selectedFields: []string{"*"}, caseSensitive: true}
			for range 10 {
				got, err := remotePlanningSelectedFields(scan, schema)
				require.NoError(t, err)
				require.Equal(t, want, got)

				name, ok := schema.FindColumnName(2)
				require.True(t, ok)
				assert.Equal(t, "field_00000.city", name)
			}
		})
	}
}
