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

package table

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestSetSnapshotRefUpdateOmitsNonPositiveRetentionFromJSON(t *testing.T) {
	tests := []struct {
		name               string
		maxRefAgeMs        int64
		maxSnapshotAgeMs   int64
		minSnapshotsToKeep int
		want               string
	}{
		{
			name:               "negative sentinels",
			maxRefAgeMs:        -1,
			maxSnapshotAgeMs:   -1,
			minSnapshotsToKeep: -1,
			want:               `[{"action":"set-snapshot-ref","ref-name":"dev","snapshot-id":123,"type":"branch"}]`,
		},
		{
			name: "zero sentinels",
			want: `[{"action":"set-snapshot-ref","ref-name":"dev","snapshot-id":123,"type":"branch"}]`,
		},
		{
			name:               "mixed omitted and positive",
			maxRefAgeMs:        -1,
			maxSnapshotAgeMs:   200,
			minSnapshotsToKeep: 3,
			want:               `[{"action":"set-snapshot-ref","ref-name":"dev","snapshot-id":123,"type":"branch","max-snapshot-age-ms":200,"min-snapshots-to-keep":3}]`,
		},
		{
			name:               "positive retention",
			maxRefAgeMs:        100,
			maxSnapshotAgeMs:   200,
			minSnapshotsToKeep: 3,
			want:               `[{"action":"set-snapshot-ref","ref-name":"dev","snapshot-id":123,"type":"branch","max-ref-age-ms":100,"max-snapshot-age-ms":200,"min-snapshots-to-keep":3}]`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			update := NewSetSnapshotRefUpdate(
				"dev", 123, BranchRef,
				tt.maxRefAgeMs, tt.maxSnapshotAgeMs, tt.minSnapshotsToKeep,
			)
			encoded, err := json.Marshal(Updates{update})
			require.NoError(t, err)
			require.JSONEq(t, tt.want, string(encoded))

			var decoded Updates
			require.NoError(t, json.Unmarshal(encoded, &decoded))
			require.Len(t, decoded, 1)
			require.Equal(t, UpdateSetSnapshotRef, decoded[0].Action())
		})
	}
}
