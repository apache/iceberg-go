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

package maintenance

import (
	"slices"
	"strings"
	"testing"
)

func TestFindOrphanFilesLazyIndex(t *testing.T) {
	tests := []struct {
		name       string
		options    []OrphanCleanupOption
		references map[string]bool
		scanned    []scannedFile
		want       []OrphanFile
		wantError  string
	}{
		{
			name: "all exact references",
			references: map[string]bool{
				"s3://bucket/table/a.parquet": false,
				"s3://bucket/table/b.parquet": true,
			},
			scanned: []scannedFile{
				{path: "s3://bucket/table/a.parquet", size: 10},
				{path: "s3://bucket/table/b.parquet", size: 20},
			},
		},
		{
			name: "scheme alias and real orphan",
			options: []OrphanCleanupOption{
				WithEqualSchemes(map[string]string{"s3,s3a": "s3"}),
			},
			references: map[string]bool{
				"s3://bucket/table/a.parquet": true,
				"s3://bucket/table/b.parquet": true,
			},
			scanned: []scannedFile{
				{path: "s3://bucket/table/a.parquet", size: 10},
				{path: "s3a://bucket/table/b.parquet", size: 20},
				{path: "s3://bucket/table/c.parquet", size: 30},
			},
			want: []OrphanFile{{Path: "s3://bucket/table/c.parquet", SizeBytes: 30}},
		},
		{
			name: "prefix mismatch ignored",
			options: []OrphanCleanupOption{
				WithPrefixMismatchMode(PrefixMismatchIgnore),
			},
			references: map[string]bool{
				"s3://bucket/table/a.parquet": true,
			},
			scanned: []scannedFile{{path: "s3://other/table/a.parquet", size: 50}},
		},
		{
			name: "prefix mismatch deleted",
			options: []OrphanCleanupOption{
				WithPrefixMismatchMode(PrefixMismatchDelete),
			},
			references: map[string]bool{
				"s3://bucket/table/a.parquet": true,
			},
			scanned: []scannedFile{{path: "s3://other/table/a.parquet", size: 50}},
			want:    []OrphanFile{{Path: "s3://other/table/a.parquet", SizeBytes: 50}},
		},
		{
			name: "prefix mismatch error",
			references: map[string]bool{
				"s3://bucket/table/a.parquet": true,
			},
			scanned:   []scannedFile{{path: "s3://other/table/a.parquet", size: 50}},
			wantError: "prefix mismatch detected",
		},
		{
			name:    "no references",
			scanned: []scannedFile{{path: "s3://bucket/table/new.parquet", size: 75}},
			want:    []OrphanFile{{Path: "s3://bucket/table/new.parquet", SizeBytes: 75}},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := newOrphanCleanupConfig(tt.options...)
			got, err := findOrphanFiles(tt.scanned, tt.references, cfg)
			if tt.wantError != "" {
				if err == nil || !strings.Contains(err.Error(), tt.wantError) {
					t.Fatalf("expected error containing %q, got %v", tt.wantError, err)
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			if !slices.Equal(got.OrphanFiles(), tt.want) {
				t.Fatalf("orphan files = %v, want %v", got.OrphanFiles(), tt.want)
			}
			var wantSize int64
			for i, file := range tt.want {
				if got.Files()[i] != file.Path {
					t.Fatalf("orphan paths = %v, want %v", got.Files(), tt.want)
				}
				wantSize += file.SizeBytes
			}
			if got.TotalSizeBytes() != wantSize {
				t.Fatalf("total size = %d, want %d", got.TotalSizeBytes(), wantSize)
			}
		})
	}
}
