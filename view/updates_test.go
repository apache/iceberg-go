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

package view_test

import (
	"encoding/json"
	"testing"

	"github.com/apache/iceberg-go/view"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestViewUpdatesUnmarshalJSON(t *testing.T) {
	jsonData := `[
		{
			"action": "assign-uuid",
			"uuid": "550e8400-e29b-41d4-a716-446655440000"
		},
		{
			"action": "upgrade-format-version",
			"format-version": 2
		},
		{
			"action": "set-location",
			"location": "s3://bucket/warehouse/view"
		},
		{
			"action": "set-properties",
			"updates": {
				"key1": "value1",
				"key2": "value2"
			}
		},
		{
			"action": "remove-properties",
			"removals": ["old-key"]
		}
	]`

	var updates view.ViewUpdates
	err := json.Unmarshal([]byte(jsonData), &updates)
	require.NoError(t, err)
	require.Len(t, updates, 5)

	assignUUID, ok := updates[0].(*view.AssignUUIDUpdate)
	require.True(t, ok)
	assert.Equal(t, "assign-uuid", assignUUID.Action())
	assert.Equal(t, "550e8400-e29b-41d4-a716-446655440000", assignUUID.UUID)

	upgradeVersion, ok := updates[1].(*view.UpgradeFormatVersionUpdate)
	require.True(t, ok)
	assert.Equal(t, "upgrade-format-version", upgradeVersion.Action())
	assert.Equal(t, 2, upgradeVersion.FormatVersion)

	setLocation, ok := updates[2].(*view.SetLocationUpdate)
	require.True(t, ok)
	assert.Equal(t, "set-location", setLocation.Action())
	assert.Equal(t, "s3://bucket/warehouse/view", setLocation.Location)

	setProps, ok := updates[3].(*view.SetPropertiesUpdate)
	require.True(t, ok)
	assert.Equal(t, "set-properties", setProps.Action())
	assert.Equal(t, map[string]string{"key1": "value1", "key2": "value2"}, setProps.Updates)

	removeProps, ok := updates[4].(*view.RemovePropertiesUpdate)
	require.True(t, ok)
	assert.Equal(t, "remove-properties", removeProps.Action())
	assert.Equal(t, []string{"old-key"}, removeProps.Removals)
}

func TestViewUpdatesUnmarshalJSONUnknownAction(t *testing.T) {
	jsonData := `[
		{
			"action": "unknown-action",
			"field": "value"
		}
	]`

	var updates view.ViewUpdates
	err := json.Unmarshal([]byte(jsonData), &updates)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "unknown update action: unknown-action")
}

func TestViewUpdatesUnmarshalJSONInvalidJSON(t *testing.T) {
	jsonData := `invalid json`

	var updates view.ViewUpdates
	err := json.Unmarshal([]byte(jsonData), &updates)
	require.Error(t, err)
}
