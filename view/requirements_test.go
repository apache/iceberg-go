// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package view_test

import (
	"encoding/json"
	"testing"

	"github.com/apache/iceberg-go/view"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
)

func TestParseRequirementList(t *testing.T) {
	t.Run("should parse a list of requirements", func(t *testing.T) {
		jsonData := []byte(`[
			{"type": "assert-view-uuid", "uuid": "550e8400-e29b-41d4-a716-446655440000"}
		]`)

		expected := view.Requirements{
			view.AssertViewUUID(uuid.MustParse("550e8400-e29b-41d4-a716-446655440000")),
		}

		var actual view.Requirements
		err := json.Unmarshal(jsonData, &actual)

		assert.NoError(t, err)
		assert.Equal(t, expected, actual)
	})

	t.Run("should handle an empty list", func(t *testing.T) {
		jsonData := []byte(`[]`)
		var actual view.Requirements
		err := json.Unmarshal(jsonData, &actual)
		assert.NoError(t, err)
		assert.Empty(t, actual)
	})

	t.Run("should return an error for an unknown requirement type in the list", func(t *testing.T) {
		jsonData := []byte(`[
			{"type": "assert-view-uuid", "uuid": "550e8400-e29b-41d4-a716-446655440000"},
			{"type": "assert-foo-bar"}
		]`)

		var actual view.Requirements
		err := json.Unmarshal(jsonData, &actual)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "unknown requirement type: assert-foo-bar")
	})

	t.Run("should return an error for invalid json", func(t *testing.T) {
		jsonData := []byte(`[{"type": "assert-view-uuid", "uuid": "550e8400-e29b-41d4-a716-446655440000"},]`) // trailing comma
		var actual view.Requirements
		err := json.Unmarshal(jsonData, &actual)
		assert.Error(t, err)
	})
}
