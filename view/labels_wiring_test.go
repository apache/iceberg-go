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

package view

import (
	"testing"

	"github.com/apache/iceberg-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestViewEqualsIgnoresLabels pins that labels are transient and do not
// participate in View.Equals; two otherwise-identical views compare equal
// regardless of labels.
func TestViewEqualsIgnoresLabels(t *testing.T) {
	meta, err := ParseMetadataString(exampleViewJSON)
	require.NoError(t, err)

	ident := []string{"foo"}
	loc := "s3://bucket/test/location/uuid.metadata.json"
	withLabels := New(ident, meta, loc,
		WithLabels(&iceberg.Labels{ObjectLabels: iceberg.Properties{"owner": "analytics"}}))
	without := New(ident, meta, loc)

	assert.True(t, withLabels.Equals(*without), "labels are transient and must not affect Equals")
	assert.True(t, without.Equals(*withLabels))
}
