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
	"testing"

	"github.com/apache/iceberg-go"
	"github.com/stretchr/testify/require"
)

func TestPhysicalSchemaKeyDistinctTypeTags(t *testing.T) {
	types := []iceberg.Type{
		iceberg.BooleanType{},
		iceberg.Int32Type{},
		iceberg.Int64Type{},
		iceberg.Float32Type{},
		iceberg.Float64Type{},
		iceberg.DateType{},
		iceberg.TimeType{},
		iceberg.TimestampType{},
		iceberg.TimestampTzType{},
		iceberg.TimestampNsType{},
		iceberg.TimestampTzNsType{},
		iceberg.StringType{},
		iceberg.UUIDType{},
		iceberg.BinaryType{},
		iceberg.UnknownType{},
		iceberg.VariantType{},
		iceberg.FixedTypeOf(8),
		iceberg.FixedTypeOf(16),
		iceberg.DecimalTypeOf(10, 2),
		iceberg.DecimalTypeOf(11, 2),
		iceberg.DecimalTypeOf(10, 3),
	}
	keys := make(map[string]string, len(types))
	for _, typ := range types {
		t.Run(typ.String(), func(t *testing.T) {
			schema := iceberg.NewSchema(1, iceberg.NestedField{ID: 1, Name: "value", Type: typ})
			key, err := physicalSchemaKey(schema)
			require.NoError(t, err)
			require.NotContains(t, keys, key, "type %s collides with %s", typ, keys[key])
			keys[key] = typ.String()
		})
	}
}

func TestPhysicalSchemaKeyRejectsNilPrimitives(t *testing.T) {
	for _, typ := range []iceberg.Type{
		(*iceberg.Int32Type)(nil),
		(*iceberg.FixedType)(nil),
		(*iceberg.DecimalType)(nil),
	} {
		key, err := physicalSchemaKey(iceberg.NewSchema(1,
			iceberg.NestedField{ID: 1, Name: "invalid", Type: typ}))
		require.ErrorIs(t, err, iceberg.ErrInvalidSchema)
		require.Empty(t, key)
	}
}

type panickingPhysicalPrimitive struct {
	iceberg.StringType
}

func (panickingPhysicalPrimitive) String() string {
	panic("unexpected primitive panic")
}

func TestPhysicalSchemaKeyPreservesUnexpectedPanics(t *testing.T) {
	schema := iceberg.NewSchema(1, iceberg.NestedField{
		ID: 1, Name: "value", Type: panickingPhysicalPrimitive{},
	})
	require.PanicsWithValue(t, "unexpected primitive panic", func() {
		_, _ = physicalSchemaKey(schema)
	})
}
