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

package metrics

import (
	"testing"

	"github.com/apache/iceberg-go"
	"github.com/stretchr/testify/assert"
)

func TestEnvironmentContextCopiesValues(t *testing.T) {
	SetEnvironmentContext(map[string]string{"engine": "go"})
	t.Cleanup(func() { SetEnvironmentContext(nil) })

	context := EnvironmentContext()
	context["engine"] = "changed"
	assert.Equal(t, "go", EnvironmentContext()["engine"])
	assert.Equal(t, iceberg.Version(), EnvironmentContext()["iceberg-version"])

	SetEnvironmentProperty("version", "1")
	assert.Equal(t, "1", EnvironmentContext()["version"])
	RemoveEnvironmentProperty("version")
	assert.NotContains(t, EnvironmentContext(), "version")
}

func TestEnvironmentContextKeepsIcebergVersion(t *testing.T) {
	SetEnvironmentContext(map[string]string{"iceberg-version": "spoofed"})
	t.Cleanup(func() { SetEnvironmentContext(nil) })

	assert.Equal(t, iceberg.Version(), EnvironmentContext()["iceberg-version"])
	SetEnvironmentProperty("iceberg-version", "spoofed-again")
	RemoveEnvironmentProperty("iceberg-version")
	assert.Equal(t, iceberg.Version(), EnvironmentContext()["iceberg-version"])
}
