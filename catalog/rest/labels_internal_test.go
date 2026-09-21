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

package rest

import (
	"encoding/json"
	"testing"

	iceberg "github.com/apache/iceberg-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const labelsTestTableMetadata = `{
	"format-version": 1,
	"table-uuid": "b55d9dda-6561-423a-8bfc-787980ce421f",
	"location": "s3://warehouse/database/table",
	"last-updated-ms": 1646787054459,
	"last-column-id": 2,
	"schema": {"type":"struct","schema-id":0,"fields":[{"id":1,"name":"id","required":false,"type":"int"},{"id":2,"name":"data","required":false,"type":"string"}]},
	"current-schema-id": 0,
	"schemas": [{"type":"struct","schema-id":0,"fields":[{"id":1,"name":"id","required":false,"type":"int"},{"id":2,"name":"data","required":false,"type":"string"}]}],
	"partition-spec": [],
	"default-spec-id": 0,
	"partition-specs": [{"spec-id":0,"fields":[]}],
	"last-partition-id": 999,
	"default-sort-order-id": 0,
	"sort-orders": [{"order-id":0,"fields":[]}],
	"properties": {}
}`

const labelsTestBlock = `{"object-labels":{"owner":"data-eng"},"fields":[{"field-id":2,"labels":{"classification":"pii"}}]}`

func TestLoadTableResponseLabels(t *testing.T) {
	body := `{"metadata-location":"s3://x","metadata":` + labelsTestTableMetadata + `,"config":{},"labels":` + labelsTestBlock + `}`

	var resp loadTableResponse
	require.NoError(t, json.Unmarshal([]byte(body), &resp))
	require.NotNil(t, resp.Labels)
	assert.Equal(t, "data-eng", resp.Labels.Object()["owner"])
	assert.Equal(t, iceberg.Properties{"classification": "pii"}, resp.Labels.Field(2))
}

func TestLoadViewResponseLabels(t *testing.T) {
	body := `{"metadata-location":"s3://x","metadata":{},"config":{},"labels":` + labelsTestBlock + `}`

	var resp loadViewResponse
	require.NoError(t, json.Unmarshal([]byte(body), &resp))
	require.NotNil(t, resp.Labels)
	assert.Equal(t, "data-eng", resp.Labels.Object()["owner"])
	assert.Equal(t, iceberg.Properties{"classification": "pii"}, resp.Labels.Field(2))
}

func TestLoadResponseLabelsAbsent(t *testing.T) {
	var viewResp loadViewResponse
	require.NoError(t, json.Unmarshal([]byte(`{"metadata-location":"s3://x","metadata":{},"config":{}}`), &viewResp))
	assert.Nil(t, viewResp.Labels)

	// loadTableResponse decodes through a custom UnmarshalJSON (alias pattern),
	// a mechanically different path, so cover its absent case too.
	var tableResp loadTableResponse
	require.NoError(t, json.Unmarshal([]byte(
		`{"metadata-location":"s3://x","metadata":`+labelsTestTableMetadata+`,"config":{}}`), &tableResp))
	assert.Nil(t, tableResp.Labels)
}
