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

package iceberg

import (
	"runtime/debug"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestVersionFromBuildInfo(t *testing.T) {
	tests := []struct {
		name     string
		info     debug.BuildInfo
		expected string
	}{
		{
			name: "main module",
			info: debug.BuildInfo{
				Main: debug.Module{Path: icebergModulePath, Version: "v1.2.3"},
			},
			expected: "v1.2.3",
		},
		{
			name: "dependency module",
			info: debug.BuildInfo{
				Main: debug.Module{Path: "example.com/application", Version: "v9.8.7"},
				Deps: []*debug.Module{{Path: icebergModulePath, Version: "v1.2.3"}},
			},
			expected: "v1.2.3",
		},
		{
			name: "replacement module",
			info: debug.BuildInfo{
				Main: debug.Module{Path: "example.com/application", Version: "v9.8.7"},
				Deps: []*debug.Module{{
					Path:    icebergModulePath,
					Version: "v1.2.3",
					Replace: &debug.Module{Path: "example.com/iceberg-go-fork", Version: "v1.3.0"},
				}},
			},
			expected: "v1.3.0",
		},
		{
			name: "local replacement",
			info: debug.BuildInfo{
				Main: debug.Module{Path: "example.com/application", Version: "v9.8.7"},
				Deps: []*debug.Module{{
					Path:    icebergModulePath,
					Version: "v1.2.3",
					Replace: &debug.Module{Path: "../iceberg-go", Version: "(devel)"},
				}},
			},
			expected: unknownVersion,
		},
		{
			name: "unrelated main module",
			info: debug.BuildInfo{
				Main: debug.Module{Path: "example.com/application", Version: "v9.8.7"},
			},
			expected: unknownVersion,
		},
		{
			name: "similarly named dependency",
			info: debug.BuildInfo{
				Main: debug.Module{Path: "example.com/application", Version: "v9.8.7"},
				Deps: []*debug.Module{{Path: icebergModulePath + "-fork", Version: "v1.2.3"}},
			},
			expected: unknownVersion,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.expected, versionFromBuildInfo(&tt.info))
		})
	}
}
