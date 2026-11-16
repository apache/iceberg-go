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

package iceberg_test

import (
	"testing"

	"github.com/apache/iceberg-go"
	"github.com/stretchr/testify/require"
)

func TestSchemaFromJSONFieldsRejectsDuplicateIDs(t *testing.T) {
	tests := []struct {
		name   string
		fields string
	}{
		{
			name:   "top-level fields",
			fields: `[{"id":1,"name":"a","required":true,"type":"int"},{"id":1,"name":"b","required":false,"type":"string"}]`,
		},
		{
			name:   "nested struct field",
			fields: `[{"id":1,"name":"parent","required":true,"type":{"type":"struct","fields":[{"id":1,"name":"child","required":false,"type":"int"}]}}]`,
		},
		{
			name:   "list element",
			fields: `[{"id":1,"name":"items","required":false,"type":{"type":"list","element-id":1,"element-required":true,"element":"int"}}]`,
		},
		{
			name:   "map key and value",
			fields: `[{"id":1,"name":"values","required":false,"type":{"type":"map","key-id":2,"key":"string","value-id":2,"value-required":false,"value":"int"}}]`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			schema, err := iceberg.NewSchemaFromJSONFields(1, tt.fields)
			require.Nil(t, schema)
			require.ErrorIs(t, err, iceberg.ErrInvalidSchema)
		})
	}
}
