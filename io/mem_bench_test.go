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
	"io/fs"
	"testing"

	icebergio "github.com/apache/iceberg-go/io"
)

func BenchmarkMemFSWalkDir(b *testing.B) {
	for _, fileCount := range []int{100, 1_000, 5_000} {
		b.Run(fmt.Sprintf("files=%d", fileCount), func(b *testing.B) {
			memIO := icebergio.NewMemFS()
			for i := range fileCount {
				name := fmt.Sprintf("mem://bucket/root/dir-%03d/file-%05d.parquet", i%128, i)
				if err := memIO.WriteFile(name, nil); err != nil {
					b.Fatal(err)
				}
			}

			b.ReportAllocs()
			b.ResetTimer()
			for b.Loop() {
				if err := memIO.WalkDir("mem://bucket/root", func(_ string, _ fs.DirEntry, err error) error {
					return err
				}); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}
