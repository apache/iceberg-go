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

package io_test

import (
	"fmt"
	"testing"

	icebergio "github.com/apache/iceberg-go/io"
)

func BenchmarkMemFSOpen(b *testing.B) {
	for _, sizeMiB := range []int{1, 16} {
		b.Run(fmt.Sprintf("%dMiB", sizeMiB), func(b *testing.B) {
			memFS := icebergio.NewMemFS()
			name := "mem://bucket/data.parquet"
			size := sizeMiB << 20
			if err := memFS.WriteFile(name, make([]byte, size)); err != nil {
				b.Fatal(err)
			}

			var lastByte [1]byte
			b.ReportAllocs()
			b.ResetTimer()
			for b.Loop() {
				file, err := memFS.Open(b.Context(), name)
				if err != nil {
					b.Fatal(err)
				}
				if _, err := file.ReadAt(lastByte[:], int64(size-1)); err != nil {
					b.Fatal(err)
				}
				if err := file.Close(); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}
