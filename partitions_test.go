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
	"encoding/json"
	"testing"

	"github.com/apache/iceberg-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestPartitionSpec(t *testing.T) {
	assert.Equal(t, 999, iceberg.UnpartitionedSpec.LastAssignedFieldID())

	bucket := iceberg.BucketTransform{NumBuckets: 4}
	idField1 := iceberg.PartitionField{
		SourceID: 3, FieldID: 1001, Name: "id", Transform: bucket,
	}
	spec1 := iceberg.NewPartitionSpec(idField1)

	assert.Zero(t, spec1.ID())
	assert.Equal(t, 1, spec1.NumFields())
	assert.Equal(t, idField1, spec1.Field(0))
	assert.NotEqual(t, idField1, spec1)
	assert.False(t, spec1.IsUnpartitioned())
	assert.True(t, spec1.CompatibleWith(&spec1))
	assert.True(t, spec1.Equals(spec1))
	assert.Equal(t, 1001, spec1.LastAssignedFieldID())
	assert.Equal(t, "[\n\t1001: id: bucket[4](3)\n]", spec1.String())

	// only differs by PartitionField FieldID
	idField2 := iceberg.PartitionField{
		SourceID: 3, FieldID: 1002, Name: "id", Transform: bucket,
	}
	spec2 := iceberg.NewPartitionSpec(idField2)

	assert.False(t, spec1.Equals(spec2))
	assert.True(t, spec1.CompatibleWith(&spec2))
	assert.Equal(t, []iceberg.PartitionField{idField1}, spec1.FieldsBySourceID(3))
	assert.Empty(t, spec1.FieldsBySourceID(1925))

	spec3 := iceberg.NewPartitionSpec(idField1, idField2)
	assert.False(t, spec1.CompatibleWith(&spec3))
	assert.Equal(t, 1002, spec3.LastAssignedFieldID())
}

func TestUnpartitionedWithVoidField(t *testing.T) {
	spec := iceberg.NewPartitionSpec(iceberg.PartitionField{
		SourceID: 3, FieldID: 1001, Name: "void", Transform: iceberg.VoidTransform{},
	})

	assert.True(t, spec.IsUnpartitioned())

	spec2 := iceberg.NewPartitionSpec(iceberg.PartitionField{
		SourceID: 3, FieldID: 1001, Name: "void", Transform: iceberg.VoidTransform{},
	}, iceberg.PartitionField{
		SourceID: 3, FieldID: 1002, Name: "bucket", Transform: iceberg.BucketTransform{NumBuckets: 2},
	})

	assert.False(t, spec2.IsUnpartitioned())
}

func TestSerializeUnpartitionedSpec(t *testing.T) {
	data, err := json.Marshal(iceberg.UnpartitionedSpec)
	require.NoError(t, err)

	assert.JSONEq(t, `{"spec-id": 0, "fields": []}`, string(data))
	assert.True(t, iceberg.UnpartitionedSpec.IsUnpartitioned())
}

func TestSerializePartitionSpec(t *testing.T) {
	spec := iceberg.NewPartitionSpecID(3,
		iceberg.PartitionField{
			SourceID: 1, FieldID: 1000,
			Transform: iceberg.TruncateTransform{Width: 19}, Name: "str_truncate",
		},
		iceberg.PartitionField{
			SourceID: 2, FieldID: 1001,
			Transform: iceberg.BucketTransform{NumBuckets: 25}, Name: "int_bucket",
		},
	)

	data, err := json.Marshal(spec)
	require.NoError(t, err)

	assert.JSONEq(t, `{
		"spec-id": 3,
		"fields": [
			{
				"source-id": 1,
				"field-id": 1000,
				"transform": "truncate[19]",
				"name": "str_truncate"
			},
			{
				"source-id": 2,
				"field-id": 1001,
				"transform": "bucket[25]",
				"name": "int_bucket"
			}
		]
	}`, string(data))

	var outspec iceberg.PartitionSpec
	require.NoError(t, json.Unmarshal(data, &outspec))

	assert.True(t, spec.Equals(outspec))
}

func TestPartitionType(t *testing.T) {
	spec := iceberg.NewPartitionSpecID(3,
		iceberg.PartitionField{
			SourceID: 1, FieldID: 1000,
			Transform: iceberg.TruncateTransform{Width: 19}, Name: "str_truncate",
		},
		iceberg.PartitionField{
			SourceID: 2, FieldID: 1001,
			Transform: iceberg.BucketTransform{NumBuckets: 25}, Name: "int_bucket",
		},
		iceberg.PartitionField{
			SourceID: 3, FieldID: 1002,
			Transform: iceberg.IdentityTransform{}, Name: "bool_identity",
		},
		iceberg.PartitionField{
			SourceID: 1, FieldID: 1003,
			Transform: iceberg.VoidTransform{}, Name: "str_void",
		},
	)

	expected := &iceberg.StructType{
		FieldList: []iceberg.NestedField{
			{ID: 1000, Name: "str_truncate", Type: iceberg.PrimitiveTypes.String},
			{ID: 1001, Name: "int_bucket", Type: iceberg.PrimitiveTypes.Int32},
			{ID: 1002, Name: "bool_identity", Type: iceberg.PrimitiveTypes.Bool},
			{ID: 1003, Name: "str_void", Type: iceberg.PrimitiveTypes.String},
		},
	}
	actual := spec.PartitionType(tableSchemaSimple)
	assert.Truef(t, expected.Equals(actual), "expected: %s, got: %s", expected, actual)
}

type partitionRecord []any

func (p partitionRecord) Size() int            { return len(p) }
func (p partitionRecord) Get(pos int) any      { return p[pos] }
func (p partitionRecord) Set(pos int, val any) { p[pos] = val }

func TestPartitionSpecToPath(t *testing.T) {
	schema := iceberg.NewSchema(0,
		iceberg.NestedField{ID: 1, Name: "str", Type: iceberg.PrimitiveTypes.String},
		iceberg.NestedField{ID: 2, Name: "other_str", Type: iceberg.PrimitiveTypes.String},
		iceberg.NestedField{ID: 3, Name: "int", Type: iceberg.PrimitiveTypes.Int32, Required: true})

	spec := iceberg.NewPartitionSpecID(3,
		iceberg.PartitionField{
			SourceID: 1, FieldID: 1000,
			Transform: iceberg.TruncateTransform{Width: 19}, Name: "my#str%bucket",
		},
		iceberg.PartitionField{
			SourceID: 2, FieldID: 1001,
			Transform: iceberg.IdentityTransform{}, Name: "other str+bucket",
		},
		iceberg.PartitionField{
			SourceID: 3, FieldID: 1002,
			Transform: iceberg.BucketTransform{NumBuckets: 25}, Name: "my!int:bucket",
		})

	record := partitionRecord{"my+str", "( )", int32(10)}
	// both partition field names and values should be URL encoded, with spaces
	// mapping to plus signs, to match Java behavior:
	// https://github.com/apache/iceberg/blob/ca3db931b0f024f0412084751ac85dd4ef2da7e7/api/src/main/java/org/apache/iceberg/PartitionSpec.java#L198-L204
	assert.Equal(t, "my%23str%25bucket=my%2Bstr/other+str%2Bbucket=%28+%29/my%21int%3Abucket=10",
		spec.PartitionToPath(record, schema))
}

func TestGetPartitionFieldName(t *testing.T) {
	schema := iceberg.NewSchema(0,
		iceberg.NestedField{ID: 1, Name: "str", Type: iceberg.PrimitiveTypes.String},
		iceberg.NestedField{ID: 2, Name: "int", Type: iceberg.PrimitiveTypes.Int32, Required: true},
		iceberg.NestedField{ID: 3, Name: "ts", Type: iceberg.PrimitiveTypes.Timestamp})

	tests := []struct {
		field        iceberg.PartitionField
		expectedName string
	}{
		{
			field:        iceberg.PartitionField{SourceID: 1, FieldID: 1000, Transform: iceberg.IdentityTransform{}, Name: "foo"},
			expectedName: "foo",
		},
		{
			field:        iceberg.PartitionField{SourceID: 1, FieldID: 1000, Transform: iceberg.IdentityTransform{}, Name: ""},
			expectedName: "str",
		},
		{
			field:        iceberg.PartitionField{SourceID: 2, FieldID: 1001, Transform: iceberg.BucketTransform{NumBuckets: 7}, Name: ""},
			expectedName: "int_bucket_7",
		},
		{
			field:        iceberg.PartitionField{SourceID: 2, FieldID: 1002, Transform: iceberg.TruncateTransform{Width: 19}, Name: ""},
			expectedName: "int_trunc_19",
		},
		{
			field:        iceberg.PartitionField{SourceID: 3, FieldID: 1003, Transform: iceberg.VoidTransform{}, Name: ""},
			expectedName: "ts_null",
		},
		{
			field:        iceberg.PartitionField{SourceID: 3, FieldID: 1004, Transform: iceberg.YearTransform{}, Name: ""},
			expectedName: "ts_year",
		},
		{
			field:        iceberg.PartitionField{SourceID: 3, FieldID: 1004, Transform: iceberg.MonthTransform{}, Name: ""},
			expectedName: "ts_month",
		},
		{
			field:        iceberg.PartitionField{SourceID: 3, FieldID: 1004, Transform: iceberg.DayTransform{}, Name: ""},
			expectedName: "ts_day",
		},
		{
			field:        iceberg.PartitionField{SourceID: 3, FieldID: 1004, Transform: iceberg.HourTransform{}, Name: ""},
			expectedName: "ts_hour",
		},
	}

	for _, test := range tests {
		t.Run(test.field.Transform.String(), func(t *testing.T) {
			name, err := iceberg.GeneratePartitionFieldName(schema, test.field)
			assert.NoError(t, err)
			assert.Equal(t, test.expectedName, name)
		})
	}
}
