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
	"strconv"
	"strings"
	"testing"

	"github.com/apache/iceberg-go/internal"
	"github.com/stretchr/testify/require"
)

func TestPartitionTypeFingerprintMatchesAvroConversion(t *testing.T) {
	// Include currently unsupported types as well: adding support in the Avro
	// converter must also make the fingerprint path accept the same type.
	types := []Type{
		BooleanType{},
		Int32Type{},
		Int64Type{},
		Float32Type{},
		Float64Type{},
		DateType{},
		TimeType{},
		TimestampType{},
		TimestampTzType{},
		TimestampNsType{},
		TimestampTzNsType{},
		StringType{},
		UUIDType{},
		BinaryType{},
		UnknownType{},
		VariantType{},
		GeometryType{},
		GeographyType{},
		FixedTypeOf(8),
		FixedTypeOf(16),
		DecimalTypeOf(10, 2),
		DecimalTypeOf(11, 2),
		DecimalTypeOf(10, 3),
		&StructType{FieldList: []NestedField{{ID: 2, Name: "child", Type: StringType{}}}},
		&ListType{ElementID: 2, Element: StringType{}},
		&MapType{KeyID: 2, KeyType: StringType{}, ValueID: 3, ValueType: Int64Type{}},
	}
	shapes := make(map[string]string, len(types))
	for _, typ := range types {
		t.Run(typ.String(), func(t *testing.T) {
			var key strings.Builder
			fingerprintErr := writePartitionTypeFingerprint(&key, typ)
			partition, avroErr := partitionTypeToAvroSchema(&StructType{FieldList: []NestedField{
				{ID: 1000, Name: "partition", Type: typ},
			}})
			if avroErr != nil {
				require.EqualError(t, fingerprintErr, avroErr.Error())

				return
			}
			require.NoError(t, fingerprintErr)
			if previous, ok := shapes[key.String()]; ok {
				require.Equal(t, previous, partition.String(), "fingerprint collision for %s", typ)
			} else {
				shapes[key.String()] = partition.String()
			}
		})
	}
}

func TestManifestEntrySchemaForMultipleParameterizedFields(t *testing.T) {
	price := PartitionField{SourceIDs: []int{1}, FieldID: 1000, Name: "price", Transform: IdentityTransform{}}
	code := PartitionField{SourceIDs: []int{2}, FieldID: 1001, Name: "code", Transform: IdentityTransform{}}
	for _, version := range []int{1, 2, 3} {
		for _, tc := range []struct {
			name    string
			spec    PartitionSpec
			decimal Type
			fixed   Type
		}{
			{"ordered", NewPartitionSpec(price, code), DecimalTypeOf(10, 2), FixedTypeOf(16)},
			{"reversed", NewPartitionSpec(code, price), DecimalTypeOf(10, 2), FixedTypeOf(16)},
			{"precision", NewPartitionSpec(price, code), DecimalTypeOf(11, 2), FixedTypeOf(16)},
			{"scale", NewPartitionSpec(price, code), DecimalTypeOf(10, 3), FixedTypeOf(16)},
			{"fixed_size", NewPartitionSpec(price, code), DecimalTypeOf(10, 2), FixedTypeOf(8)},
		} {
			t.Run(tc.name+"/v"+strconv.Itoa(version), func(t *testing.T) {
				t.Parallel()

				schema := NewSchema(1,
					NestedField{ID: 1, Name: "source_price", Type: tc.decimal},
					NestedField{ID: 2, Name: "source_code", Type: tc.fixed},
				)
				partition, err := partitionTypeToAvroSchema(tc.spec.PartitionType(schema))
				require.NoError(t, err)
				want, err := internal.NewManifestEntrySchema(partition, version)
				require.NoError(t, err)
				for range 2 {
					got, maps, err := manifestEntrySchemaFor(tc.spec, schema, version)
					require.NoError(t, err)
					require.Equal(t, want.String(), got.String())
					require.Equal(t, getFieldIDMap(want), maps)
				}
			})
		}
	}
}

func TestPartitionSchemaFingerprintNameBoundaries(t *testing.T) {
	schema := NewSchema(1,
		NestedField{ID: 1, Name: "source_price", Type: DecimalTypeOf(10, 2)},
		NestedField{ID: 2, Name: "source_code", Type: FixedTypeOf(16)},
	)
	// Exercise the key's length prefixes independently of Avro name validation.
	names := []string{"a", "ab", "1:a", "a:q10:2:", "f16:", "λ"}
	keys := make(map[string][2]string, len(names)*len(names))
	for _, first := range names {
		for _, second := range names {
			spec := NewPartitionSpec(
				PartitionField{SourceIDs: []int{1}, FieldID: 1000, Name: first, Transform: IdentityTransform{}},
				PartitionField{SourceIDs: []int{2}, FieldID: 1001, Name: second, Transform: IdentityTransform{}},
			)
			key, err := partitionSchemaFingerprint(spec, schema)
			require.NoError(t, err)
			require.NotContains(t, keys, key, "names %q/%q collide with %q", first, second, keys[key])
			keys[key] = [2]string{first, second}
		}
	}
}
