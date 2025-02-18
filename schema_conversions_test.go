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
	_ "embed"
	"encoding/json"
	"testing"

	"github.com/hamba/avro/v2"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

//go:embed schema_conversions_iceberg_to_avro_testdata.json
var schemaConversionsIcebergToAvroTestsData []byte

//go:embed schema_conversions_avro_to_iceberg_testdata.json
var schemaConversionsAvroToIcebergTestsData []byte

func TestConvertIcebergSchemaToAvro(t *testing.T) {
	var items []struct {
		Name          string          `json:"name"`
		IcebergSchema json.RawMessage `json:"iceberg_schema"`
		AvroSchema    json.RawMessage `json:"avro_schema"`
	}

	require.NoError(t, json.Unmarshal(schemaConversionsIcebergToAvroTestsData, &items))

	for _, item := range items {
		t.Run(item.Name, func(t *testing.T) {
			var (
				iceSch  StructType
				avroSch struct {
					Name string `json:"name"`
				}
			)

			require.NoError(t, json.Unmarshal(item.AvroSchema, &avroSch))
			require.NoError(t, json.Unmarshal(item.IcebergSchema, &iceSch))
			avrSch, err := TypeToAvroSchema(avroSch.Name, &iceSch)
			require.NoError(t, err)
			js, err := json.Marshal(avrSch)
			require.NoError(t, err)
			jsonEqual(t, item.AvroSchema, js)
		})
	}
}

func TestConvertAvroSchemaToIceberg(t *testing.T) {
	var items []struct {
		Name          string          `json:"name"`
		IcebergSchema json.RawMessage `json:"iceberg_schema"`
		AvroSchema    json.RawMessage `json:"avro_schema"`
	}

	require.NoError(t, json.Unmarshal(schemaConversionsAvroToIcebergTestsData, &items))

	for _, item := range items {
		t.Run(item.Name, func(t *testing.T) {
			avroSch, err := avro.Parse(string(item.AvroSchema))
			require.NoError(t, err)
			iceType, err := AvroSchemaToType(avroSch)
			require.NoError(t, err)
			js, err := json.Marshal(iceType)
			require.NoError(t, err)
			jsonEqual(t, item.IcebergSchema, js)
		})
	}
}

func jsonEqual(t *testing.T, expected json.RawMessage, actual json.RawMessage) {
	var (
		expectedMap map[string]any
		actualMap   map[string]any
	)

	require.NoError(t, json.Unmarshal(expected, &expectedMap))
	require.NoError(t, json.Unmarshal(actual, &actualMap))
	assert.Equal(t, expectedMap, actualMap)
}
