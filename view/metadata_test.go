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

package view

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestLoadMetadata(t *testing.T) {
	props := make(map[string]string)
	m, err := LoadMetadata(t.Context(), props, "testdata/view-metadata.json", "test", "test")
	require.NoError(t, err)

	require.Equal(t, "fa6506c3-7681-40c8-86dc-e36561f83385", m.ViewUUID())
	require.Equal(t, 1, m.FormatVersion())
	require.Equal(t, 2, len(m.(*metadata).VersionList))
	require.Equal(t, "Daily event counts", m.Properties()["comment"])
}
