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

package gocloud_test

import (
	"os/exec"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

var cloudSDKPrefixes = map[string][]string{
	"aws":   {"github.com/aws/aws-sdk-go-v2/"},
	"gcp":   {"cloud.google.com/go/storage", "gocloud.dev/blob/gcsblob"},
	"azure": {"github.com/Azure/"},
}

// A package that pulls in a cloud's SDK still registers only its own schemes,
// thus, the registry cannot catch it. Inspect the build graph instead.
//
// catalog/hadoop is listed because it needs only the bucket-backed FileIO type;
// importing io/gocloud for that assertion would link all three SDKs into every
// binary that uses the Hadoop catalog.
func TestPackagesLinkOnlyTheirOwnCloudSDK(t *testing.T) {
	for _, tt := range []struct {
		pkg   string
		cloud string
	}{
		{"./io/gocloud/blobfs", ""},
		{"./io/gocloud/s3", "aws"},
		{"./io/gocloud/gcs", "gcp"},
		{"./io/gocloud/azure", "azure"},
		{"./catalog/hadoop", ""},
	} {
		t.Run(tt.pkg, func(t *testing.T) {
			deps := packageDeps(t, tt.pkg)

			for cloud, prefixes := range cloudSDKPrefixes {
				linked := depsWithAnyPrefix(deps, prefixes)
				if cloud == tt.cloud {
					assert.NotEmpty(t, linked, "%s should link the %s SDK", tt.pkg, cloud)

					continue
				}

				assert.Empty(t, linked, "%s must not link the %s SDK", tt.pkg, cloud)
			}
		})
	}
}

func packageDeps(t *testing.T, pkg string) []string {
	t.Helper()

	goBin, err := exec.LookPath("go")
	if err != nil {
		t.Skipf("go toolchain not available: %v", err)
	}

	cmd := exec.Command(goBin, "list", "-deps", pkg)
	cmd.Dir = "../.."

	out, err := cmd.Output()
	require.NoError(t, err, "go list -deps %s", pkg)

	return strings.Fields(string(out))
}

func depsWithAnyPrefix(deps, prefixes []string) []string {
	var matched []string
	for _, dep := range deps {
		for _, prefix := range prefixes {
			if strings.HasPrefix(dep, prefix) {
				matched = append(matched, dep)

				break
			}
		}
	}

	return matched
}
