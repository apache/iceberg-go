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
	"github.com/apache/iceberg-go/collation"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestCollatedStringTypeEquals(t *testing.T) {
	plain := iceberg.StringType{}
	ciA := iceberg.StringTypeWithCollation(collation.MustParse("en_US-ci"))
	ciB := iceberg.StringTypeWithCollation(collation.MustParse("en_US-ci"))
	ai := iceberg.StringTypeWithCollation(collation.MustParse("en_US-ci-ai"))
	binary := iceberg.StringTypeWithCollation(collation.MustParse("utf8"))

	assert.True(t, ciA.Equals(ciB))
	assert.False(t, plain.Equals(ciA))
	assert.False(t, ciA.Equals(ai))
	// An explicit utf8 collation is equivalent to the default string type.
	assert.True(t, plain.Equals(binary))
	assert.True(t, binary.Equals(plain))
}

func TestCollatedStringTypeString(t *testing.T) {
	assert.Equal(t, "string", iceberg.StringType{}.String())
	assert.Equal(t, "string collate en_US-ci",
		iceberg.StringTypeWithCollation(collation.MustParse("en_US-ci")).String())
}

func TestCollatedStringTypeComparator(t *testing.T) {
	cmp := iceberg.StringTypeWithCollation(collation.MustParse("en_US-ci")).Comparator()
	assert.Equal(t, 0, cmp("APPLE", "apple"))

	plain := iceberg.StringType{}.Comparator()
	assert.NotEqual(t, 0, plain("APPLE", "apple"))
}

func TestCollationSpecJSONRoundTrip(t *testing.T) {
	field := iceberg.NestedField{
		ID:       3,
		Name:     "product_name",
		Required: true,
		Type:     iceberg.StringTypeWithCollation(collation.MustParse("en_US-ci")),
	}

	data, err := json.Marshal(field)
	require.NoError(t, err)
	// type stays "string"; collation rides alongside as collation_spec.
	assert.JSONEq(t, `{
		"id": 3,
		"name": "product_name",
		"required": true,
		"type": "string",
		"collation_spec": {"name": "en_US-ci"}
	}`, string(data))

	var got iceberg.NestedField
	require.NoError(t, json.Unmarshal(data, &got))
	require.True(t, got.Type.Equals(field.Type))

	st, ok := got.Type.(iceberg.StringType)
	require.True(t, ok)
	require.NotNil(t, st.Collation())
	assert.Equal(t, "en_US-ci", st.Collation().String())
}

func TestCollationSpecJSONWithVersion(t *testing.T) {
	field := iceberg.NestedField{
		ID:   1,
		Name: "c",
		Type: iceberg.StringTypeWithCollation(
			collation.MustParse("de_DE-ci").WithVersion("153.88.33.0"),
		),
	}

	data, err := json.Marshal(field)
	require.NoError(t, err)
	assert.Contains(t, string(data), `"icu_collator_version":"153.88.33.0"`)

	var got iceberg.NestedField
	require.NoError(t, json.Unmarshal(data, &got))
	st := got.Type.(iceberg.StringType)
	assert.Equal(t, "153.88.33.0", st.Collation().Version())
}

func TestPlainStringHasNoCollationSpec(t *testing.T) {
	field := iceberg.NestedField{ID: 1, Name: "c", Type: iceberg.PrimitiveTypes.String}
	data, err := json.Marshal(field)
	require.NoError(t, err)
	assert.NotContains(t, string(data), "collation_spec")
}

func TestCollationSpecOnNonStringErrors(t *testing.T) {
	const j = `{"id":1,"name":"c","required":false,"type":"long","collation_spec":{"name":"en_US-ci"}}`
	var got iceberg.NestedField
	err := json.Unmarshal([]byte(j), &got)
	assert.Error(t, err)
}

func TestComputeCollatedBounds(t *testing.T) {
	spec := collation.MustParse("en_US-ci").WithVersion("v1")

	// Byte-order min/max of these is "Banana"/"apple"; collation-order (ci)
	// min/max is "apple"/"Cherry".
	entry, ok := iceberg.ComputeCollatedBounds(spec, []string{"Banana", "apple", "Cherry"})
	require.True(t, ok)
	assert.Equal(t, "v1", entry.Version)
	assert.Equal(t, "en_US-ci", entry.Collation)

	lower, err := iceberg.LiteralFromBytes(iceberg.PrimitiveTypes.String, entry.Lower)
	require.NoError(t, err)
	upper, err := iceberg.LiteralFromBytes(iceberg.PrimitiveTypes.String, entry.Upper)
	require.NoError(t, err)
	assert.Equal(t, iceberg.StringLiteral("apple"), lower)
	assert.Equal(t, iceberg.StringLiteral("Cherry"), upper)

	// Bounds carry the original values, not ICU sort keys.
	assert.Equal(t, []byte("apple"), entry.Lower)

	// A binary spec has no collation bounds.
	_, ok = iceberg.ComputeCollatedBounds(collation.MustParse("utf8"), []string{"a", "b"})
	assert.False(t, ok)
}

func TestCollationBoundEntryValidFor(t *testing.T) {
	v1 := iceberg.CollationBoundEntry{Collation: "en_US-ci", Version: "v1", Lower: []byte("a"), Upper: []byte("z")}
	assert.True(t, v1.ValidFor(collation.MustParse("en_US-ci").WithVersion("v1")))
	assert.False(t, v1.ValidFor(collation.MustParse("en_US-ci").WithVersion("v2")), "version mismatch")
	assert.False(t, v1.ValidFor(collation.MustParse("en_US-ci")), "unversioned reader cannot use bound")
	assert.False(t, v1.ValidFor(collation.MustParse("de_DE-ci").WithVersion("v1")), "collation mismatch")

	unversioned := iceberg.CollationBoundEntry{Collation: "en_US-ci", Lower: []byte("a"), Upper: []byte("z")}
	assert.False(t, unversioned.ValidFor(collation.MustParse("en_US-ci").WithVersion("v1")))
}

func TestCollatedStringInSchemaRoundTrip(t *testing.T) {
	sc := iceberg.NewSchema(
		0,
		iceberg.NestedField{ID: 1, Name: "id", Type: iceberg.PrimitiveTypes.Int64, Required: true},
		iceberg.NestedField{
			ID: 2, Name: "name", Required: true,
			Type: iceberg.StringTypeWithCollation(collation.MustParse("en_US-ci")),
		},
	)

	data, err := json.Marshal(sc)
	require.NoError(t, err)

	var got iceberg.Schema
	require.NoError(t, json.Unmarshal(data, &got))

	f, ok := got.FindFieldByName("name")
	require.True(t, ok)
	st, ok := f.Type.(iceberg.StringType)
	require.True(t, ok)
	require.NotNil(t, st.Collation())
	assert.Equal(t, "en_US-ci", st.Collation().String())
}
