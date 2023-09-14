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
		SourceID: 3, FieldID: 1001, Name: "id", Transform: bucket}
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
		SourceID: 3, FieldID: 1002, Name: "id", Transform: bucket}
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
		iceberg.PartitionField{SourceID: 1, FieldID: 1000,
			Transform: iceberg.TruncateTransform{Width: 19}, Name: "str_truncate"},
		iceberg.PartitionField{SourceID: 2, FieldID: 1001,
			Transform: iceberg.BucketTransform{NumBuckets: 25}, Name: "int_bucket"},
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
		iceberg.PartitionField{SourceID: 1, FieldID: 1000,
			Transform: iceberg.TruncateTransform{Width: 19}, Name: "str_truncate"},
		iceberg.PartitionField{SourceID: 2, FieldID: 1001,
			Transform: iceberg.BucketTransform{NumBuckets: 25}, Name: "int_bucket"},
		iceberg.PartitionField{SourceID: 3, FieldID: 1002,
			Transform: iceberg.IdentityTransform{}, Name: "bool_identity"},
		iceberg.PartitionField{SourceID: 1, FieldID: 1003,
			Transform: iceberg.VoidTransform{}, Name: "str_void"},
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
