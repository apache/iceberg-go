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

package fileuri_test

import (
	"path/filepath"
	"testing"

	"github.com/apache/iceberg-go/internal/fileuri"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestParseFileURI(t *testing.T) {
	tests := []struct {
		name        string
		uri         string
		host        string
		windowsPath string
		posixPath   string
	}{
		{
			name:        "opaque Windows path",
			uri:         "file:C:/Warehouse%20Space/data.parquet",
			windowsPath: "C:/Warehouse Space/data.parquet",
			posixPath:   "C:/Warehouse Space/data.parquet",
		},
		{
			name:        "Windows drive authority",
			uri:         "file://C:/Warehouse/data.parquet",
			host:        "C:",
			windowsPath: "C:/Warehouse/data.parquet",
			posixPath:   "/Warehouse/data.parquet",
		},
		{
			name:        "hierarchical Windows path",
			uri:         "file:///C:/Warehouse/data.parquet",
			windowsPath: "C:/Warehouse/data.parquet",
			posixPath:   "/C:/Warehouse/data.parquet",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			fileURI, err := fileuri.Parse(tt.uri)
			require.NoError(t, err)
			assert.Equal(t, tt.host, fileURI.Host())
			assert.Equal(t, tt.windowsPath, fileURI.LocalPath(true))
			assert.Equal(t, tt.posixPath, fileURI.LocalPath(false))

			nativePath := tt.posixPath
			if filepath.Separator == '\\' {
				nativePath = tt.windowsPath
			}
			assert.Equal(t, nativePath, fileURI.LocalPathForOS())
		})
	}
}

func TestFileURIJoinPath(t *testing.T) {
	tests := []struct {
		name     string
		uri      string
		expected string
	}{
		{
			name:     "opaque Windows path",
			uri:      "file:C:/warehouse/table",
			expected: "file:C:/warehouse/table/metadata/version-hint.text",
		},
		{
			name:     "hierarchical path",
			uri:      "file:///tmp/table",
			expected: "file:///tmp/table/metadata/version-hint.text",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			fileURI, err := fileuri.Parse(tt.uri)
			require.NoError(t, err)
			assert.Equal(t, tt.expected, fileURI.JoinPath("metadata", "version-hint.text"))
		})
	}
}
