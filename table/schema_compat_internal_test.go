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
	"strconv"
	"testing"

	"github.com/apache/iceberg-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestCheckSchemaCompatReportsEveryField(t *testing.T) {
	requested := iceberg.NewSchema(0,
		iceberg.NestedField{ID: 1, Name: "id", Type: iceberg.PrimitiveTypes.Int64, Required: true},
		iceberg.NestedField{ID: 2, Name: "location", Type: &iceberg.StructType{FieldList: []iceberg.NestedField{
			{ID: 3, Name: "lat", Type: iceberg.PrimitiveTypes.Float64},
		}}},
		iceberg.NestedField{ID: 4, Name: "note", Type: iceberg.PrimitiveTypes.String},
	)
	provided := iceberg.NewSchema(0,
		iceberg.NestedField{ID: 1, Name: "id", Type: iceberg.PrimitiveTypes.Int64, Required: true},
		iceberg.NestedField{ID: 2, Name: "location", Type: &iceberg.StructType{FieldList: []iceberg.NestedField{
			{ID: 3, Name: "lat", Type: iceberg.PrimitiveTypes.String},
		}}},
	)

	err := checkSchemaCompat(requested, provided)
	require.ErrorIs(t, err, iceberg.ErrInvalidSchema)
	assert.Equal(t, `invalid schema: mismatch in fields:
   Table Field                                            Requested Field
✅  1: id: required long                                   1: id: required long
✅  2: location: optional struct<3: lat: optional double>  2: location: optional struct<3: lat: optional string>
❌  3: lat: optional double                                3: lat: optional string
✅  4: note: optional string                               missing
`, err.Error())
}

func TestCheckSchemaCompatAcceptsCompatibleSchema(t *testing.T) {
	requested := iceberg.NewSchema(0,
		iceberg.NestedField{ID: 1, Name: "id", Type: iceberg.PrimitiveTypes.Int64},
		iceberg.NestedField{ID: 2, Name: "note", Type: iceberg.PrimitiveTypes.String},
	)
	provided := iceberg.NewSchema(0,
		iceberg.NestedField{ID: 1, Name: "id", Type: iceberg.PrimitiveTypes.Int32},
	)

	assert.NoError(t, checkSchemaCompat(requested, provided))
}

// BenchmarkCheckSchemaCompat checks a wide, compatible schema, as each data
// file added to a table is.
func BenchmarkCheckSchemaCompat(b *testing.B) {
	fields := make([]iceberg.NestedField, 250)
	for i := range fields {
		fields[i] = iceberg.NestedField{
			ID: i + 1, Name: "col_" + strconv.Itoa(i), Type: iceberg.PrimitiveTypes.String,
		}
	}
	requested := iceberg.NewSchema(0, fields...)
	provided := iceberg.NewSchema(0, fields...)

	b.ReportAllocs()
	for b.Loop() {
		if err := checkSchemaCompat(requested, provided); err != nil {
			b.Fatal(err)
		}
	}
}
