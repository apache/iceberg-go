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
	"math"
	"testing"

	"github.com/apache/iceberg-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestLongDefaultsPreserveJSONPrecision(t *testing.T) {
	const input = `{"id":1,"name":"value","type":"long","required":true,"initial-default":9007199254740993,"write-default":-9007199254740993}`

	var field iceberg.NestedField
	require.NoError(t, json.Unmarshal([]byte(input), &field))
	assert.Equal(t, int64(9007199254740993), field.InitialDefault)
	assert.Equal(t, int64(-9007199254740993), field.WriteDefault)

	encoded, err := json.Marshal(field)
	require.NoError(t, err)
	assert.JSONEq(t, input, string(encoded))
}

func TestLongDefaultsSurviveSchemaJSONRoundTrip(t *testing.T) {
	original := iceberg.NewSchema(1,
		iceberg.NestedField{
			ID: 1, Name: "max", Type: iceberg.PrimitiveTypes.Int64,
			InitialDefault: int64(math.MaxInt64),
			WriteDefault:   int64(math.MinInt64),
		},
		iceberg.NestedField{
			ID: 2, Name: "small", Type: iceberg.PrimitiveTypes.Int64,
			InitialDefault: int64(7),
			WriteDefault:   int64(-7),
		},
	)

	data, err := json.Marshal(original)
	require.NoError(t, err)

	var decoded iceberg.Schema
	require.NoError(t, json.Unmarshal(data, &decoded))
	assert.Equal(t, int64(math.MaxInt64), decoded.Field(0).InitialDefault)
	assert.Equal(t, int64(math.MinInt64), decoded.Field(0).WriteDefault)
	assert.Equal(t, int64(7), decoded.Field(1).InitialDefault)
	assert.Equal(t, int64(-7), decoded.Field(1).WriteDefault)
	assert.True(t, original.Equals(&decoded))
}

func TestLongDefaultsRejectNonIntegralAndOutOfRangeJSON(t *testing.T) {
	invalid := []string{
		`1.5`,
		`1e2`,
		`9223372036854775808`,
		`-9223372036854775809`,
		`"42"`,
		`true`,
	}

	for _, value := range invalid {
		t.Run(value, func(t *testing.T) {
			for _, key := range []string{"initial-default", "write-default"} {
				input := `{"id":1,"name":"value","type":"long","required":false,"` + key + `":` + value + `}`
				var field iceberg.NestedField
				err := json.Unmarshal([]byte(input), &field)
				assert.ErrorIs(t, err, iceberg.ErrInvalidSchema, key)
			}
		})
	}
}

func TestLongDefaultsKeepNullAndMissingValues(t *testing.T) {
	for _, input := range []string{
		`{"id":1,"name":"value","type":"long","required":false}`,
		`{"id":1,"name":"value","type":"long","required":false,"initial-default":null,"write-default":null}`,
	} {
		var field iceberg.NestedField
		require.NoError(t, json.Unmarshal([]byte(input), &field))
		assert.Nil(t, field.InitialDefault)
		assert.Nil(t, field.WriteDefault)
	}
}

func TestLongWriteDefaultExpressionPreservesJSONNumbers(t *testing.T) {
	const input = `{"id":1,"name":"value","type":"long","required":false,"write-default":{"type":"literal","value":9007199254740993,"data-type":"long"}}`

	var field iceberg.NestedField
	require.NoError(t, json.Unmarshal([]byte(input), &field))

	expression, ok := field.WriteDefault.(map[string]any)
	require.True(t, ok)
	assert.Equal(t, json.Number("9007199254740993"), expression["value"])

	encoded, err := json.Marshal(field)
	require.NoError(t, err)
	assert.JSONEq(t, input, string(encoded))
}
