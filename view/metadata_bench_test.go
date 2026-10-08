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
	"fmt"
	"testing"

	"github.com/apache/iceberg-go"
)

var (
	benchmarkVersionIndexSink map[int64]*Version
	benchmarkSchemaIndexSink  map[int]*iceberg.Schema
)

func BenchmarkIndexBy(b *testing.B) {
	for _, entries := range []int{0, 1, 10, 32, 128} {
		versions := make([]*Version, entries)
		schemas := make([]*iceberg.Schema, entries)
		for i := range entries {
			versions[i] = &Version{VersionID: int64(i)}
			schemas[i] = iceberg.NewSchema(i)
		}

		b.Run(fmt.Sprintf("entries=%d", entries), func(b *testing.B) {
			b.Run("versions", func(b *testing.B) {
				b.ReportAllocs()
				for b.Loop() {
					benchmarkVersionIndexSink = indexBy(versions, func(v *Version) int64 { return v.VersionID })
				}
			})
			b.Run("schemas", func(b *testing.B) {
				b.ReportAllocs()
				for b.Loop() {
					benchmarkSchemaIndexSink = indexBy(schemas, func(s *iceberg.Schema) int { return s.ID })
				}
			})
		})
	}
}
