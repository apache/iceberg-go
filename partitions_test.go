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

func TestParseTransform(t *testing.T) {
	tests := []struct {
		toparse  string
		expected iceberg.Transform
	}{
		{"identity", iceberg.IdentityTransform{}},
		{"void", iceberg.VoidTransform{}},
		{"year", iceberg.YearTransform{}},
		{"month", iceberg.MonthTransform{}},
		{"day", iceberg.DayTransform{}},
		{"hour", iceberg.HourTransform{}},
		{"bucket[5]", iceberg.BucketTransform{N: 5}},
		{"bucket[100]", iceberg.BucketTransform{N: 100}},
		{"truncate[10]", iceberg.TruncateTransform{W: 10}},
		{"truncate[255]", iceberg.TruncateTransform{W: 255}},
	}

	for _, tt := range tests {
		t.Run(tt.toparse, func(t *testing.T) {
			transform, err := iceberg.ParseTransform(tt.toparse)
			require.NoError(t, err)
			assert.Equal(t, tt.expected, transform)

			txt, err := transform.MarshalText()
			assert.NoError(t, err)
			assert.Equal(t, tt.toparse, string(txt))
		})
	}

	errorTests := []struct {
		name    string
		toparse string
	}{
		{"foobar", "foobar"},
		{"bucket no brackets", "bucket"},
		{"truncate no brackets", "truncate"},
		{"bucket no val", "bucket[]"},
		{"truncate no val", "truncate[]"},
		{"bucket neg", "bucket[-1]"},
		{"truncate neg", "truncate[-1]"},
	}

	for _, tt := range errorTests {
		t.Run(tt.name, func(t *testing.T) {
			tr, err := iceberg.ParseTransform(tt.toparse)
			assert.Nil(t, tr)
			assert.ErrorIs(t, err, iceberg.ErrInvalidTransform)
			assert.ErrorContains(t, err, tt.toparse)
		})
	}
}

func TestPartitionSpec(t *testing.T) {
	assert.Equal(t, 1000, iceberg.UnpartitionedPartitionSpec.LastAssignedFieldID())

	bucket := iceberg.BucketTransform{N: 4}
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

func TestSerializeUnpartitionedSpec(t *testing.T) {
	data, err := json.Marshal(iceberg.UnpartitionedPartitionSpec)
	require.NoError(t, err)

	assert.JSONEq(t, `{"spec-id": 0, "fields": []}`, string(data))
}

func TestSerializePartitionSpec(t *testing.T) {
	spec := iceberg.NewPartitionSpecID(3,
		iceberg.PartitionField{SourceID: 1, FieldID: 1000,
			Transform: iceberg.TruncateTransform{W: 19}, Name: "str_truncate"},
		iceberg.PartitionField{SourceID: 2, FieldID: 1001,
			Transform: iceberg.BucketTransform{N: 25}, Name: "int_bucket"},
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
			Transform: iceberg.TruncateTransform{W: 19}, Name: "str_truncate"},
		iceberg.PartitionField{SourceID: 2, FieldID: 1001,
			Transform: iceberg.BucketTransform{N: 25}, Name: "int_bucket"},
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
