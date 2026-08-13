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

package table_test

import (
	"encoding/json"
	"testing"

	"github.com/apache/iceberg-go/table"
)

var benchmarkSortField table.SortField

func BenchmarkSortFieldUnmarshalJSON(b *testing.B) {
	for _, tt := range []struct {
		name    string
		payload []byte
	}{
		{
			name:    "single-source",
			payload: []byte(`{"source-id": 1, "transform": "bucket[16]", "direction": "asc", "null-order": "nulls-first"}`),
		},
		{
			name:    "multi-source",
			payload: []byte(`{"source-ids": [1, 2], "transform": "bucket[16]", "direction": "asc", "null-order": "nulls-first"}`),
		},
	} {
		b.Run(tt.name, func(b *testing.B) {
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				var field table.SortField
				if err := json.Unmarshal(tt.payload, &field); err != nil {
					b.Fatal(err)
				}
				benchmarkSortField = field
			}
		})
	}
}
