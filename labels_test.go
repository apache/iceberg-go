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

	iceberg "github.com/apache/iceberg-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestLabelsUnmarshal(t *testing.T) {
	const body = `{
		"object-labels": {"owner": "data-eng", "cost-center": "42"},
		"fields": [
			{"field-id": 2, "labels": {"classification": "pii"}},
			{"field-id": 5, "labels": {"classification": "internal"}}
		]
	}`

	var l iceberg.Labels
	require.NoError(t, json.Unmarshal([]byte(body), &l))

	assert.Equal(t, iceberg.Properties{"owner": "data-eng", "cost-center": "42"}, l.Object())
	assert.Equal(t, iceberg.Properties{"classification": "pii"}, l.Field(2))
	assert.Equal(t, iceberg.Properties{"classification": "internal"}, l.Field(5))
	assert.Nil(t, l.Field(999)) // unknown or dropped field id
}

func TestLabelsRoundTrip(t *testing.T) {
	l := iceberg.Labels{
		ObjectLabels: iceberg.Properties{"owner": "data-eng"},
		Fields:       []iceberg.FieldLabel{{FieldID: 3, Labels: iceberg.Properties{"classification": "pii"}}},
	}

	b, err := json.Marshal(l)
	require.NoError(t, err)
	assert.JSONEq(t, `{"object-labels":{"owner":"data-eng"},"fields":[{"field-id":3,"labels":{"classification":"pii"}}]}`, string(b))

	var got iceberg.Labels
	require.NoError(t, json.Unmarshal(b, &got))
	assert.Equal(t, l, got)
}

func TestLabelsEmptyOmitted(t *testing.T) {
	b, err := json.Marshal(iceberg.Labels{})
	require.NoError(t, err)
	assert.JSONEq(t, `{}`, string(b))
}

func TestLabelsNilReceiver(t *testing.T) {
	var l *iceberg.Labels
	assert.Nil(t, l.Object())
	assert.Nil(t, l.Field(1))
	assert.True(t, l.IsEmpty())
}

func TestLabelsIsEmpty(t *testing.T) {
	assert.True(t, (&iceberg.Labels{}).IsEmpty())
	assert.False(t, (&iceberg.Labels{ObjectLabels: iceberg.Properties{"owner": "x"}}).IsEmpty())
	assert.False(t, (&iceberg.Labels{Fields: []iceberg.FieldLabel{{FieldID: 1, Labels: iceberg.Properties{"a": "b"}}}}).IsEmpty())
}
