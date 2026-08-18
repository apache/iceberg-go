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
	"fmt"
	"testing"
)

var benchmarkUpdates Updates

func BenchmarkUnmarshalUpdates(b *testing.B) {
	for _, legacy := range []bool{false, true} {
		label := "canonical"
		if legacy {
			label = "legacy-properties"
		}
		for _, count := range []int{1, 10, 100, 1000} {
			b.Run(fmt.Sprintf("%s/%d", label, count), func(b *testing.B) {
				payload, err := benchmarkUpdatesPayload(count, legacy)
				if err != nil {
					b.Fatal(err)
				}

				b.ReportAllocs()
				b.SetBytes(int64(len(payload)))
				b.ResetTimer()
				for b.Loop() {
					var updates Updates
					if err := json.Unmarshal(payload, &updates); err != nil {
						b.Fatal(err)
					}
					if len(updates) != count {
						b.Fatalf("decoded %d updates, want %d", len(updates), count)
					}
					benchmarkUpdates = updates
				}
			})
		}
	}
}

func benchmarkUpdatesPayload(count int, legacyProperties bool) ([]byte, error) {
	updates := make([]map[string]any, count)
	for i := range updates {
		switch i % 8 {
		case 0:
			updates[i] = map[string]any{
				"action": "assign-uuid",
				"uuid":   "550e8400-e29b-41d4-a716-446655440000",
			}
		case 1:
			updates[i] = map[string]any{
				"action":         "upgrade-format-version",
				"format-version": 2,
			}
		case 2:
			updates[i] = map[string]any{
				"action":   "set-location",
				"location": "s3://bucket/table",
			}
		case 3:
			field := "updates"
			if legacyProperties {
				field = "updated"
			}
			updates[i] = map[string]any{
				"action": "set-properties",
				field:    map[string]string{"key": "value"},
			}
		case 4:
			field := "removals"
			if legacyProperties {
				field = "removed"
			}
			updates[i] = map[string]any{
				"action": "remove-properties",
				field:    []string{"key"},
			}
		case 5:
			updates[i] = map[string]any{
				"action":     "remove-schemas",
				"schema-ids": []int{1, 2, 3},
			}
		case 6:
			updates[i] = map[string]any{
				"action":   "remove-partition-specs",
				"spec-ids": []int{1, 2, 3},
			}
		default:
			updates[i] = map[string]any{
				"action":      "set-snapshot-ref",
				"ref-name":    "main",
				"type":        "branch",
				"snapshot-id": 1,
			}
		}
	}

	return json.Marshal(updates)
}
