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

import "testing"

var computeHashBenchmarkSink string

func BenchmarkComputeHash(b *testing.B) {
	const dataFileName = "part-00000-8f4c2e1d-2d6b-4e38-9f7f-2bca3d8e6a10-c000.snappy.parquet"

	b.ReportAllocs()
	for range b.N {
		computeHashBenchmarkSink = computeHash(dataFileName)
	}
}

var locationPathBenchmarkSink string

func BenchmarkJoinLocationPath(b *testing.B) {
	for _, tc := range []struct {
		name, location string
		parts          []string
	}{
		{"one", "s3://bucket/table/data", []string{"file-000-uuid.parquet"}},
		{"two", "s3://bucket/table/data/", []string{"0001/0010/0011", "file-000-uuid.parquet"}},
		{"three", "s3://bucket/table/data//", []string{"part=x", "sha-hash", "file-000-uuid.parquet"}},
	} {
		b.Run(tc.name, func(b *testing.B) {
			b.ReportAllocs()
			for b.Loop() {
				locationPathBenchmarkSink = joinLocationPath(tc.location, tc.parts...)
			}
		})
	}
}
