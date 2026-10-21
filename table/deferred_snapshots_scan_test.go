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
	"math/rand"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestScanJSONStringEscapes(t *testing.T) {
	legacyScan := func(raw []byte, i int) int {
		for i++; i < len(raw); i++ {
			switch raw[i] {
			case '\\':
				i++
			case '"':
				return i + 1
			}
		}

		return len(raw)
	}

	testCases := []struct {
		name  string
		value string
	}{
		{"empty", ""},
		{"single character", "a"},
		{"backslash", "\\"},
		{"quote", "\""},
		{"backslash and quote", "\\\""},
		{"mixed escapes", "a\"b\\c"},
		{"unicode", "東京"},
		{"long manifest path", strings.Repeat("s3://bucket/long/manifest-location/", 32)},
		{"long escaped quote", strings.Repeat("x", 256) + `"tail`},
		{"long backslashes and quote", strings.Repeat("x", 256) + `\\"tail`},
		{"repeated escaped quotes", strings.Repeat(`a"b`, 80)},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			raw, err := json.Marshal(tc.value)
			require.NoError(t, err)
			assert.Equal(t, len(raw), scanJSONString(raw, 0))
		})
	}

	t.Run("random parity", func(t *testing.T) {
		// Verify parity for escaped quotes, escaped backslashes, missing closing
		// quotes and other malformed inputs. scanJSONString is a scanner, not
		// a JSON validator; its behavior for truncated data must not change.
		rng := rand.New(rand.NewSource(1))
		alphabet := []byte{'"', '\\', 'a', 0, '\n', ',', '[', ']', '{', '}'}
		for caseID := range 5000 {
			raw := make([]byte, rng.Intn(1024)+1)
			raw[0] = '"'
			for i := 1; i < len(raw); i++ {
				raw[i] = alphabet[rng.Intn(len(alphabet))]
			}
			want := legacyScan(raw, 0)
			require.Equal(t, want, scanJSONString(raw, 0), "case %d for %q", caseID, raw)
		}
	})
}

var benchmarkDeferredSnapshotJSONSpans []rawJSONSpan

func BenchmarkSplitJSONArrayDeferredSnapshots(b *testing.B) {
	for _, snapshotCount := range []int{1000, 10000} {
		for _, pathSuffixLen := range []int{32, 256} {
			b.Run(fmt.Sprintf("snapshots=%d/path_suffix=%d", snapshotCount, pathSuffixLen), func(b *testing.B) {
				location := "s3://bucket/manifests/" + strings.Repeat("a", pathSuffixLen) + ".avro"
				snapshots := make([]map[string]any, snapshotCount)
				for i := range snapshots {
					snapshots[i] = map[string]any{
						"snapshot-id":     i + 1,
						"timestamp-ms":    int64(i+1) * 60000,
						"sequence-number": i + 1,
						"summary":         map[string]string{"operation": "append"},
						"manifest-list":   location,
					}
				}
				raw, err := json.Marshal(snapshots)
				if err != nil {
					b.Fatal(err)
				}
				spans, err := splitJSONArray(raw)
				if err != nil || len(spans) != snapshotCount {
					b.Fatalf("split %d snapshots: got %d spans, err=%v", snapshotCount, len(spans), err)
				}
				b.SetBytes(int64(len(raw)))
				b.ReportAllocs()
				b.ResetTimer()
				for b.Loop() {
					benchmarkDeferredSnapshotJSONSpans, err = splitJSONArray(raw)
					if err != nil {
						b.Fatal(err)
					}
				}
			})
		}
	}
}
