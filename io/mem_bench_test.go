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
	"context"
	"fmt"
	"io/fs"
	"strings"
	"testing"

	icebergio "github.com/apache/iceberg-go/io"
)

func BenchmarkMemFSWalkDir(b *testing.B) {
	benchmarkMemFSWalkDir(b, func(i int) string {
		return fmt.Sprintf("mem://bucket/root/dir-%03d/file-%05d.parquet", i%128, i)
	})
}

func BenchmarkMemFSWalkDirDeep(b *testing.B) {
	benchmarkMemFSWalkDir(b, func(i int) string {
		parts := make([]string, 8)
		for j := range parts {
			parts[j] = fmt.Sprintf("dir-%03d", (i+j*13)%128)
		}

		return fmt.Sprintf("mem://bucket/root/%s/file-%05d.parquet", strings.Join(parts, "/"), i)
	})
}

func benchmarkMemFSWalkDir(b *testing.B, nameForFile func(int) string) {
	b.Helper()
	for _, fileCount := range []int{100, 1_000, 5_000} {
		b.Run(fmt.Sprintf("files=%d", fileCount), func(b *testing.B) {
			memIO := icebergio.NewMemFS()
			for i := range fileCount {
				if err := memIO.WriteFile(context.Background(), nameForFile(i), nil); err != nil {
					b.Fatal(err)
				}
			}

			b.ReportAllocs()
			for b.Loop() {
				if err := memIO.WalkDir(context.Background(), "mem://bucket/root", func(_ string, _ fs.DirEntry, err error) error {
					return err
				}); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}
