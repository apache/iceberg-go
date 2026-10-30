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

func TestJoinLocationPath(t *testing.T) {
	for _, tc := range []struct {
		name, location, want string
		parts                []string
	}{
		{"one", "s3://bucket/table", "s3://bucket/table/file.parquet", []string{"file.parquet"}},
		{"two", "s3://bucket/table/", "s3://bucket/table/part=x/file.parquet", []string{"part=x", "file.parquet"}},
		{"three", "s3://bucket/table//", "s3://bucket/table/part=x/hash/file.parquet", []string{"part=x", "hash", "file.parquet"}},
		{"empty-base", "////", "/data.parquet", []string{"data.parquet"}},
		{"no-components", "s3://bucket/table", "s3://bucket/table/", nil},
		{"empty-components", "table", "table//file/", []string{"", "file", ""}},
		{"literal-components", "s3://bucket/a/../b//", "s3://bucket/a/../b/x%20/foo#?", []string{"x%20", "foo#?"}},
		{"leading-slash-component", "table", "table//file.parquet", []string{"/file.parquet"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := joinLocationPath(tc.location, tc.parts...); got != tc.want {
				t.Errorf("joinLocationPath(%q, %q) = %q, want %q", tc.location, tc.parts, got, tc.want)
			}
		})
	}
}
