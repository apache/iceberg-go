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

package internal

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestParseMetadataVersionRejectsTrailingJunk(t *testing.T) {
	for _, name := range []string{
		"00001-a1b2c3d4-e5f6-7890-abcd-ef1234567890.metadata.json.tmp",
		"00001-a1b2c3d4-e5f6-7890-abcd-ef1234567890.metadata.json.garbage",
		"00001-a1b2c3d4-e5f6-7890-abcd-ef1234567890.metadata.json/",
	} {
		t.Run(name, func(t *testing.T) {
			require.Equal(t, -1, ParseMetadataVersion(name))
		})
	}
}
