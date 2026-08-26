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
	"testing"

	iceberg "github.com/apache/iceberg-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestCloneDefaultValue(t *testing.T) {
	original := map[string]any{
		"binary":  []byte{1, 2},
		"literal": iceberg.BinaryLiteral{3, 4},
		"nested": []any{
			map[string]any{"fixed": iceberg.FixedLiteral{5, 6}},
		},
	}

	cloned := iceberg.CloneDefaultValue(original).(map[string]any)
	cloned["binary"].([]byte)[0] = 9
	cloned["literal"].(iceberg.BinaryLiteral)[0] = 9
	nested, ok := cloned["nested"].([]any)
	require.True(t, ok)
	nestedMap := nested[0].(map[string]any)
	nestedMap["fixed"].(iceberg.FixedLiteral)[0] = 9
	cloned["added"] = true
	nestedMap["added"] = true
	nested[0] = "changed"

	assert.Equal(t, []byte{1, 2}, original["binary"])
	assert.Equal(t, iceberg.BinaryLiteral{3, 4}, original["literal"])
	assert.Equal(t, iceberg.FixedLiteral{5, 6}, original["nested"].([]any)[0].(map[string]any)["fixed"])
	assert.NotContains(t, original, "added")
	assert.NotContains(t, original["nested"].([]any)[0], "added")
}
