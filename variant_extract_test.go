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
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func extractBindSchema() *Schema {
	return NewSchema(0,
		NestedField{ID: 1, Name: "payload", Type: VariantType{}},
		NestedField{ID: 2, Name: "name", Type: PrimitiveTypes.String},
	)
}

func TestExtractBind(t *testing.T) {
	term, err := Extract("payload", "$.a.b", PrimitiveTypes.Int64).Bind(extractBindSchema(), true)
	require.NoError(t, err)

	be, ok := term.(BoundExtract)
	require.True(t, ok)
	assert.Equal(t, "$['a']['b']", be.Path())
	assert.True(t, PrimitiveTypes.Int64.Equals(be.Type()))
	assert.Equal(t, 1, be.Ref().Field().ID)
}

func TestExtractBindRejects(t *testing.T) {
	for _, tt := range []struct {
		name string
		term UnboundTerm
	}{
		{"non-variant source", Extract("name", "$.a", PrimitiveTypes.Int64)},
		{"nil target type", Extract("payload", "$.a", nil)},
		{"unknown target type", Extract("payload", "$.a", UnknownType{})},
		{"bracket path", Extract("payload", "$['a']", PrimitiveTypes.Int64)},
		{"unknown field", Extract("missing", "$.a", PrimitiveTypes.Int64)},
	} {
		t.Run(tt.name, func(t *testing.T) {
			_, err := tt.term.Bind(extractBindSchema(), true)
			require.Error(t, err)
		})
	}
}
