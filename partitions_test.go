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
	"slices"
	"testing"

	"github.com/apache/iceberg-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type nonComparableTransform struct {
	iceberg.IdentityTransform
	// values makes this transform non-comparable, which would have panicked with ==.
	values []int
}

func (t nonComparableTransform) Equals(other iceberg.Transform) bool {
	o, ok := other.(nonComparableTransform)

	return ok && slices.Equal(t.values, o.values)
}

func TestPartitionSpec(t *testing.T) {
	assert.Equal(t, 999, iceberg.UnpartitionedSpec.LastAssignedFieldID())

	bucket := iceberg.BucketTransform{NumBuckets: 4}
	idField1 := iceberg.PartitionField{
		SourceIDs: []int{3}, FieldID: 1001, Name: "id", Transform: bucket,
	}
	spec1 := iceberg.NewPartitionSpec(idField1)

	assert.Zero(t, spec1.ID())
	assert.Equal(t, 1, spec1.NumFields())
	assert.True(t, idField1.Equals(spec1.Field(0)))
	assert.NotEqual(t, idField1, spec1)
	assert.False(t, spec1.IsUnpartitioned())
	assert.True(t, spec1.CompatibleWith(&spec1))
	assert.True(t, spec1.Equals(spec1))
	assert.Equal(t, 1001, spec1.LastAssignedFieldID())
	assert.Equal(t, "[\n\t1001: id: bucket[4](3)\n]", spec1.String())

	// only differs by PartitionField FieldID
	idField2 := iceberg.PartitionField{
		SourceIDs: []int{3}, FieldID: 1002, Name: "id", Transform: bucket,
	}
	spec2 := iceberg.NewPartitionSpec(idField2)

	assert.False(t, spec1.Equals(spec2))
	assert.True(t, spec1.CompatibleWith(&spec2))
	assert.True(t, idField1.Equals(spec1.FieldsBySourceID(3)[0]))
	assert.Empty(t, spec1.FieldsBySourceID(1925))

	spec3 := iceberg.NewPartitionSpec(idField1, idField2)
	assert.False(t, spec1.CompatibleWith(&spec3))
	assert.Equal(t, 1002, spec3.LastAssignedFieldID())
}

func TestNewPartitionSpecIDCopiesFields(t *testing.T) {
	sourceIDs := []int{1}
	fields := make([]iceberg.PartitionField, 1)
	fields[0] = iceberg.PartitionField{
		SourceIDs: sourceIDs,
		FieldID:   1000,
		Name:      "id",
		Transform: iceberg.IdentityTransform{},
	}

	spec := iceberg.NewPartitionSpecID(7, fields...)

	fields[0].FieldID = 2000
	fields[0].Name = "updated"
	fields[0].SourceIDs[0] = 2

	restored := spec.Field(0)
	assert.Equal(t, 1000, restored.FieldID)
	assert.Equal(t, "id", restored.Name)
	assert.Equal(t, []int{1}, restored.SourceIDs)
}

func TestPartitionSpecCompatibleWithUsesTransformEquals(t *testing.T) {
	spec := iceberg.NewPartitionSpec(iceberg.PartitionField{
		SourceIDs: []int{1}, FieldID: 1001, Name: "id",
		Transform: nonComparableTransform{values: []int{1, 2}},
	})
	sameTransformSpec := iceberg.NewPartitionSpec(iceberg.PartitionField{
		SourceIDs: []int{1}, FieldID: 1002, Name: "id",
		Transform: nonComparableTransform{values: []int{1, 2}},
	})
	differentTransformSpec := iceberg.NewPartitionSpec(iceberg.PartitionField{
		SourceIDs: []int{1}, FieldID: 1003, Name: "id",
		Transform: nonComparableTransform{values: []int{2, 3}},
	})

	require.NotPanics(t, func() {
		assert.True(t, spec.CompatibleWith(&sameTransformSpec))
		assert.False(t, spec.CompatibleWith(&differentTransformSpec))
	})

	tests := []struct {
		name       string
		left       iceberg.Transform
		right      iceberg.Transform
		compatible bool
	}{
		{
			name:       "identical bucket transforms are compatible",
			left:       iceberg.BucketTransform{NumBuckets: 16},
			right:      iceberg.BucketTransform{NumBuckets: 16},
			compatible: true,
		},
		{
			name:       "different bucket transforms are incompatible",
			left:       iceberg.BucketTransform{NumBuckets: 16},
			right:      iceberg.BucketTransform{NumBuckets: 32},
			compatible: false,
		},
		{
			name:       "identical truncate transforms are compatible",
			left:       iceberg.TruncateTransform{Width: 4},
			right:      iceberg.TruncateTransform{Width: 4},
			compatible: true,
		},
		{
			name:       "different truncate transforms are incompatible",
			left:       iceberg.TruncateTransform{Width: 4},
			right:      iceberg.TruncateTransform{Width: 8},
			compatible: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			left := iceberg.NewPartitionSpec(iceberg.PartitionField{
				SourceIDs: []int{1}, FieldID: 1001, Name: "id", Transform: tt.left,
			})
			right := iceberg.NewPartitionSpec(iceberg.PartitionField{
				SourceIDs: []int{1}, FieldID: 1002, Name: "id", Transform: tt.right,
			})

			assert.Equal(t, tt.compatible, left.CompatibleWith(&right))
		})
	}
}

func TestPartitionSpecRejectsInvalidBucketTransform(t *testing.T) {
	schema := iceberg.NewSchema(1, iceberg.NestedField{
		ID:   1,
		Name: "id",
		Type: iceberg.PrimitiveTypes.Int32,
	})

	_, err := iceberg.NewPartitionSpecOpts(
		iceberg.AddPartitionFieldBySourceID(1, "id_bucket", iceberg.BucketTransform{NumBuckets: 0}, schema, nil),
	)

	require.ErrorIs(t, err, iceberg.ErrInvalidArgument)
	require.ErrorContains(t, err, "numBuckets > 0")
}

func TestPartitionSpecRejectsNegativeSpecID(t *testing.T) {
	_, err := iceberg.NewPartitionSpecOpts(iceberg.WithSpecID(-1))

	require.ErrorIs(t, err, iceberg.ErrInvalidPartitionSpec)
	require.ErrorContains(t, err, "spec id must be non-negative: -1")
}

func TestPartitionSpec_MarshalTextRejectsInvalidBucketTransform(t *testing.T) {
	spec := iceberg.NewPartitionSpecID(3,
		iceberg.PartitionField{
			SourceIDs: []int{1},
			FieldID:   1000,
			Name:      "bad_bucket",
			Transform: iceberg.BucketTransform{NumBuckets: 0},
		},
	)

	_, err := json.Marshal(spec)
	require.ErrorIs(t, err, iceberg.ErrInvalidArgument)
	require.ErrorContains(t, err, "numBuckets > 0")
}

func TestUnpartitionedWithVoidField(t *testing.T) {
	spec := iceberg.NewPartitionSpec(iceberg.PartitionField{
		SourceIDs: []int{3}, FieldID: 1001, Name: "void", Transform: iceberg.VoidTransform{},
	})

	assert.True(t, spec.IsUnpartitioned())

	spec2 := iceberg.NewPartitionSpec(iceberg.PartitionField{
		SourceIDs: []int{3}, FieldID: 1001, Name: "void", Transform: iceberg.VoidTransform{},
	}, iceberg.PartitionField{
		SourceIDs: []int{3}, FieldID: 1002, Name: "bucket", Transform: iceberg.BucketTransform{NumBuckets: 2},
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
			SourceIDs: []int{1}, FieldID: 1000,
			Transform: iceberg.TruncateTransform{Width: 19}, Name: "str_truncate",
		},
		iceberg.PartitionField{
			SourceIDs: []int{2}, FieldID: 1001,
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

func TestDeserializePartitionSpecWithoutFieldIDs(t *testing.T) {
	data := []byte(`{
		"spec-id": 3,
		"fields": [
			{"source-id": 1, "transform": "identity", "name": "id"},
			{"source-id": 2, "transform": "bucket[16]", "name": "data_bucket"}
		]
	}`)

	var spec iceberg.PartitionSpec
	require.NoError(t, json.Unmarshal(data, &spec))
	require.Equal(t, 1000, spec.Field(0).FieldID)
	require.Equal(t, 1001, spec.Field(1).FieldID)
}

func TestDeserializePartitionSpecWithPartiallyMissingFieldIDs(t *testing.T) {
	data := []byte(`{
		"spec-id": 3,
		"fields": [
			{"source-id": 1, "field-id": 1000, "transform": "identity", "name": "id"},
			{"source-id": 2, "transform": "bucket[16]", "name": "data_bucket"}
		]
	}`)

	var spec iceberg.PartitionSpec
	require.NoError(t, json.Unmarshal(data, &spec))
	require.Equal(t, 1000, spec.Field(0).FieldID)
	require.Equal(t, 1001, spec.Field(1).FieldID)
}

func TestDeserializePartitionSpecAssignsAfterExistingFieldIDs(t *testing.T) {
	data := []byte(`{
		"spec-id": 3,
		"fields": [
			{"source-id": 1, "transform": "identity", "name": "id"},
			{"source-id": 2, "field-id": 1001, "transform": "bucket[16]", "name": "data_bucket"}
		]
	}`)

	var spec iceberg.PartitionSpec
	require.NoError(t, json.Unmarshal(data, &spec))
	require.Equal(t, 1002, spec.Field(0).FieldID)
	require.Equal(t, 1001, spec.Field(1).FieldID)
}

func TestDeserializePartitionSpecWithNullFieldID(t *testing.T) {
	data := []byte(`{
		"spec-id": 3,
		"fields": [
			{"source-id": 1, "field-id": null, "transform": "identity", "name": "id"}
		]
	}`)

	var spec iceberg.PartitionSpec
	err := json.Unmarshal(data, &spec)
	require.ErrorIs(t, err, iceberg.ErrInvalidPartitionSpec)
	require.ErrorContains(t, err, "partition field ID cannot be null")
}

func TestPartitionType(t *testing.T) {
	spec := iceberg.NewPartitionSpecID(3,
		iceberg.PartitionField{
			SourceIDs: []int{1}, FieldID: 1000,
			Transform: iceberg.TruncateTransform{Width: 19}, Name: "str_truncate",
		},
		iceberg.PartitionField{
			SourceIDs: []int{2}, FieldID: 1001,
			Transform: iceberg.BucketTransform{NumBuckets: 25}, Name: "int_bucket",
		},
		iceberg.PartitionField{
			SourceIDs: []int{3}, FieldID: 1002,
			Transform: iceberg.IdentityTransform{}, Name: "bool_identity",
		},
		iceberg.PartitionField{
			SourceIDs: []int{1}, FieldID: 1003,
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
			SourceIDs: []int{1}, FieldID: 1000,
			Transform: iceberg.TruncateTransform{Width: 19}, Name: "my#str%bucket",
		},
		iceberg.PartitionField{
			SourceIDs: []int{2}, FieldID: 1001,
			Transform: iceberg.IdentityTransform{}, Name: "other str+bucket",
		},
		iceberg.PartitionField{
			SourceIDs: []int{3}, FieldID: 1002,
			Transform: iceberg.BucketTransform{NumBuckets: 25}, Name: "my!int:bucket",
		})

	record := partitionRecord{"my+str", "( )", int32(10)}
	// both partition field names and values should be URL encoded, with spaces
	// mapping to plus signs, to match Java behavior:
	// https://github.com/apache/iceberg/blob/ca3db931b0f024f0412084751ac85dd4ef2da7e7/api/src/main/java/org/apache/iceberg/PartitionSpec.java#L198-L204
	assert.Equal(t, "my%23str%25bucket=my%2Bstr/other+str%2Bbucket=%28+%29/my%21int%3Abucket=10",
		spec.PartitionToPath(record, schema))
}

func TestPartitionSpecToPathWithDroppedLeadingSourceColumn(t *testing.T) {
	schema := iceberg.NewSchema(0,
		iceberg.NestedField{ID: 2, Name: "bar", Type: iceberg.PrimitiveTypes.Int32, Required: true},
		iceberg.NestedField{ID: 3, Name: "baz", Type: iceberg.PrimitiveTypes.Bool},
	)

	spec := iceberg.NewPartitionSpecID(3,
		iceberg.PartitionField{
			SourceIDs: []int{1}, FieldID: 1000,
			Transform: iceberg.IdentityTransform{}, Name: "foo",
		},
		iceberg.PartitionField{
			SourceIDs: []int{2}, FieldID: 1001,
			Transform: iceberg.IdentityTransform{}, Name: "bar",
		},
		iceberg.PartitionField{
			SourceIDs: []int{3}, FieldID: 1002,
			Transform: iceberg.IdentityTransform{}, Name: "baz",
		},
	)

	record := partitionRecord{int32(7), true}
	assert.Equal(t, "bar=7/baz=true", spec.PartitionToPath(record, schema))
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
			field:        iceberg.PartitionField{SourceIDs: []int{1}, FieldID: 1000, Transform: iceberg.IdentityTransform{}, Name: "foo"},
			expectedName: "foo",
		},
		{
			field:        iceberg.PartitionField{SourceIDs: []int{1}, FieldID: 1000, Transform: iceberg.IdentityTransform{}, Name: ""},
			expectedName: "str",
		},
		{
			field:        iceberg.PartitionField{SourceIDs: []int{2}, FieldID: 1001, Transform: iceberg.BucketTransform{NumBuckets: 7}, Name: ""},
			expectedName: "int_bucket_7",
		},
		{
			field:        iceberg.PartitionField{SourceIDs: []int{2}, FieldID: 1002, Transform: iceberg.TruncateTransform{Width: 19}, Name: ""},
			expectedName: "int_trunc_19",
		},
		{
			field:        iceberg.PartitionField{SourceIDs: []int{3}, FieldID: 1003, Transform: iceberg.VoidTransform{}, Name: ""},
			expectedName: "ts_null",
		},
		{
			field:        iceberg.PartitionField{SourceIDs: []int{3}, FieldID: 1004, Transform: iceberg.YearTransform{}, Name: ""},
			expectedName: "ts_year",
		},
		{
			field:        iceberg.PartitionField{SourceIDs: []int{3}, FieldID: 1004, Transform: iceberg.MonthTransform{}, Name: ""},
			expectedName: "ts_month",
		},
		{
			field:        iceberg.PartitionField{SourceIDs: []int{3}, FieldID: 1004, Transform: iceberg.DayTransform{}, Name: ""},
			expectedName: "ts_day",
		},
		{
			field:        iceberg.PartitionField{SourceIDs: []int{3}, FieldID: 1004, Transform: iceberg.HourTransform{}, Name: ""},
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

func TestPartitionFieldUnmarshalJSON(t *testing.T) {
	t.Run("unmarshal with source-id", func(t *testing.T) {
		jsonData := `
		{
			"source-id": 1,
			"field-id": 1000,
			"transform": "truncate[19]",
			"name": "str_truncate"
		}`
		var field iceberg.PartitionField
		err := json.Unmarshal([]byte(jsonData), &field)
		require.NoError(t, err)
		assert.Equal(t, 1, field.SourceID())
		assert.Equal(t, 1000, field.FieldID)
		assert.Equal(t, "str_truncate", field.Name)
		assert.Equal(t, iceberg.TruncateTransform{Width: 19}, field.Transform)
	})

	t.Run("unmarshal with source-ids", func(t *testing.T) {
		jsonData := `
		{
			"source-ids": [2],
			"field-id": 1001,
			"transform": "bucket[25]",
			"name": "int_bucket"
		}`
		var field iceberg.PartitionField
		err := json.Unmarshal([]byte(jsonData), &field)
		require.NoError(t, err)
		assert.Equal(t, 2, field.SourceID())
		assert.Equal(t, 1001, field.FieldID)
		assert.Equal(t, "int_bucket", field.Name)
		assert.Equal(t, iceberg.BucketTransform{NumBuckets: 25}, field.Transform)
	})

	t.Run("unmarshal with multiple source-ids", func(t *testing.T) {
		jsonData := `
		{
			"source-ids": [2, 3],
			"field-id": 1001,
			"transform": "bucket[25]",
			"name": "int_bucket"
		}`
		var field iceberg.PartitionField
		err := json.Unmarshal([]byte(jsonData), &field)
		require.NoError(t, err)
		assert.Equal(t, 2, field.SourceID(), "SourceID should be first element")
		assert.Equal(t, []int{2, 3}, field.SourceIDs, "SourceIDs should contain all elements")
		assert.Equal(t, 1001, field.FieldID)
		assert.Equal(t, "int_bucket", field.Name)
	})

	t.Run("marshal multi-arg round-trip", func(t *testing.T) {
		field := iceberg.PartitionField{
			SourceIDs: []int{2, 3},
			FieldID:   1001,
			Name:      "multi_arg",
			Transform: iceberg.BucketTransform{NumBuckets: 25},
		}
		data, err := json.Marshal(field)
		require.NoError(t, err)
		assert.Contains(t, string(data), `"source-ids"`)
		assert.NotContains(t, string(data), `"source-id"`)

		var decoded iceberg.PartitionField
		err = json.Unmarshal(data, &decoded)
		require.NoError(t, err)
		assert.Equal(t, 2, decoded.SourceID())
		assert.Equal(t, []int{2, 3}, decoded.SourceIDs)
	})

	t.Run("marshal single-arg uses source-id", func(t *testing.T) {
		field := iceberg.PartitionField{
			SourceIDs: []int{1},
			FieldID:   1000,
			Name:      "single_arg",
			Transform: iceberg.IdentityTransform{},
		}
		data, err := json.Marshal(field)
		require.NoError(t, err)
		assert.Contains(t, string(data), `"source-id"`)
		assert.NotContains(t, string(data), `"source-ids"`)
	})

	t.Run("unmarshal with both source-id and source-ids", func(t *testing.T) {
		jsonData := `
		{
			"source-id": 1,
			"source-ids": [2],
			"field-id": 1002,
			"transform": "identity",
			"name": "identity"
		}`
		var field iceberg.PartitionField
		err := json.Unmarshal([]byte(jsonData), &field)
		require.Error(t, err)
		assert.EqualError(t, err, "partition field cannot contain both source-id and source-ids")
	})

	t.Run("unmarshal with no source id", func(t *testing.T) {
		jsonData := `
		{
			"field-id": 1003,
			"transform": "void",
			"name": "void"
		}`
		var field iceberg.PartitionField
		err := json.Unmarshal([]byte(jsonData), &field)
		require.NoError(t, err)
		assert.Zero(t, field.SourceID())
		assert.Equal(t, 1003, field.FieldID)
		assert.Equal(t, "void", field.Name)
		assert.Equal(t, iceberg.VoidTransform{}, field.Transform)
	})
}
