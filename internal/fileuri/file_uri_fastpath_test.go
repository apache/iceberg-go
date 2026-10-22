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
	"net/url"
	"reflect"
	"strings"
	"testing"
)

// Compare all URL fields, not just the decoded path. JoinPath depends on the
// original hierarchical/opaque URL representation.
func TestParseFileURIEquivalentToURLParser(t *testing.T) {
	for _, uri := range []string{
		"file:///", "file:////", "file:///tmp/data.parquet",
		"file:///srv/data/a-b_1.2~3", "file:///srv//data/../file",
		"file:///" + strings.Repeat("abc_0123456789/", 20),
		"file:///C:/Warehouse/data.parquet", "file:///a%20b",
		"file:///data?version=1", "file:///data#fragment",
		"file:///data with spaces", "file:///café", "file:///data\\next",
		"file:///data\x00nul", "file://localhost/a.parquet",
		"file:C:/Warehouse/data.parquet", "file:///a%ZZ",
		"FILE:///tmp/data.parquet", "s3:///tmp/data.parquet",
	} {
		got, gotErr := Parse(uri)
		want, wantErr := url.Parse(uri)
		valid := wantErr == nil && strings.EqualFold(want.Scheme, "file")
		if (gotErr == nil) != valid {
			t.Errorf("Parse(%q): got error %v; url.Parse error %v; valid=%v", uri, gotErr, wantErr, valid)

			continue
		}
		if !valid {
			continue
		}
		if !reflect.DeepEqual(got.parsed, *want) {
			t.Errorf("Parse(%q): URL=%#v, want %#v", uri, got.parsed, *want)
		}

		wantPath := want.Path
		if want.Opaque != "" {
			var err error
			wantPath, err = url.PathUnescape(want.Opaque)
			if err != nil {
				t.Fatal(err)
			}
		}
		if got.LocalPath(false) != wantPath {
			t.Errorf("Parse(%q): path=%q, want %q", uri, got.path, wantPath)
		}
		if want.Opaque == "" && got.JoinPath("metadata", "v1.json") != want.JoinPath("metadata", "v1.json").String() {
			t.Errorf("Parse(%q): JoinPath differs from net/url", uri)
		}
	}
}

func TestPlainFilePathAlphabet(t *testing.T) {
	const safe = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789/-_.~"
	const unsafe = "%?#\\ :@!$\x00é"
	if !isPlainFilePath(safe) {
		t.Fatal("simple URI alphabet rejected")
	}
	for _, c := range unsafe {
		if isPlainFilePath("/tmp/" + string(c)) {
			t.Errorf("unsupported character %q accepted", c)
		}
	}
}
