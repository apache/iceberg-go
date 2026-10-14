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

package internal

import (
	"maps"
	"testing"

	"github.com/apache/iceberg-go"
	"github.com/apache/iceberg-go/catalog"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestGetUpdatedPropsAndUpdateSummary(t *testing.T) {
	tests := []struct {
		name            string
		current         iceberg.Properties
		removals        []string
		updates         iceberg.Properties
		wantErrContains string
		wantProps       iceberg.Properties
		wantRemoved     []string
		wantUpdated     []string
		wantMissing     []string
	}{
		{
			name:            "removal and update of the same key conflicts",
			current:         iceberg.Properties{"a": "1"},
			removals:        []string{"a"},
			updates:         iceberg.Properties{"a": "2"},
			wantErrContains: "conflict between removals and updates for keys: [a]",
		},
		{
			name:        "existing key is removed",
			current:     iceberg.Properties{"a": "1", "b": "2"},
			removals:    []string{"a"},
			wantProps:   iceberg.Properties{"b": "2"},
			wantRemoved: []string{"a"},
			wantUpdated: []string{},
			wantMissing: []string{},
		},
		{
			name:        "removal of absent key is reported as missing",
			current:     iceberg.Properties{"a": "1"},
			removals:    []string{"ghost"},
			wantProps:   iceberg.Properties{"a": "1"},
			wantRemoved: []string{},
			wantUpdated: []string{},
			wantMissing: []string{"ghost"},
		},
		{
			name:        "existing key gets a new value",
			current:     iceberg.Properties{"a": "old"},
			updates:     iceberg.Properties{"a": "new"},
			wantProps:   iceberg.Properties{"a": "new"},
			wantRemoved: []string{},
			wantUpdated: []string{"a"},
			wantMissing: []string{},
		},
		{
			name:        "new key is added",
			current:     iceberg.Properties{"a": "1"},
			updates:     iceberg.Properties{"b": "2"},
			wantProps:   iceberg.Properties{"a": "1", "b": "2"},
			wantRemoved: []string{},
			wantUpdated: []string{"b"},
			wantMissing: []string{},
		},
		{
			name:        "update with unchanged value is not reported",
			current:     iceberg.Properties{"a": "1"},
			updates:     iceberg.Properties{"a": "1"},
			wantProps:   iceberg.Properties{"a": "1"},
			wantRemoved: []string{},
			wantUpdated: []string{},
			wantMissing: []string{},
		},
		{
			name:        "no removals or updates is a no-op",
			current:     iceberg.Properties{"a": "1"},
			wantProps:   iceberg.Properties{"a": "1"},
			wantRemoved: []string{},
			wantUpdated: []string{},
			wantMissing: []string{},
		},
		{
			name:        "add, update, remove and missing in one call",
			current:     iceberg.Properties{"keep": "v", "drop": "v", "upd": "old"},
			removals:    []string{"drop", "ghost"},
			updates:     iceberg.Properties{"upd": "new", "add": "v", "keep": "v"},
			wantProps:   iceberg.Properties{"keep": "v", "upd": "new", "add": "v"},
			wantRemoved: []string{"drop"},
			wantUpdated: []string{"upd", "add"},
			wantMissing: []string{"ghost"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			before := maps.Clone(tt.current)

			props, summary, err := GetUpdatedPropsAndUpdateSummary(tt.current, tt.removals, tt.updates)

			assert.Equal(t, before, tt.current, "currentProps must not be mutated")

			if tt.wantErrContains != "" {
				require.ErrorContains(t, err, tt.wantErrContains)
				assert.Nil(t, props)
				assert.Equal(t, catalog.PropertiesUpdateSummary{}, summary)

				return
			}

			require.NoError(t, err)
			assert.Equal(t, tt.wantProps, props)
			assert.ElementsMatch(t, tt.wantRemoved, summary.Removed)
			assert.ElementsMatch(t, tt.wantUpdated, summary.Updated)
			assert.ElementsMatch(t, tt.wantMissing, summary.Missing)
		})
	}
}
