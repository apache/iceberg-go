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

package fileuri

import (
	"strings"
	"testing"
)

var parsedFileURISink FileURI

// BenchmarkParseFileURI covers simple local paths and the fallback forms used
// by FileIO. Compare the baseline and candidate commits with the same command.
func BenchmarkParseFileURI(b *testing.B) {
	for _, tc := range []struct {
		name string
		uri  string
	}{
		{"short-local", "file:///tmp/data.parquet"},
		{"warehouse-path", "file:///srv/warehouse/database/table/data/2026/10/part-00000001.parquet"},
		{"long-local", "file:///srv/" + strings.Repeat("partition_2026/", 20) + "data.parquet"},
		{"escaped-local", "file:///srv/warehouse/data/a%20b.parquet"},
		{"authority", "file://localhost/a/b.parquet"},
		{"windows-drive", "file:///C:/Warehouse/data.parquet"},
		{"opaque", "file:C:/Warehouse/data.parquet"},
	} {
		b.Run(tc.name, func(b *testing.B) {
			b.ReportAllocs()
			for range b.N {
				parsed, err := Parse(tc.uri)
				if err != nil {
					b.Fatal(err)
				}
				parsedFileURISink = parsed
			}
		})
	}
}
