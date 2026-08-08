// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package hive

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestNewHiveClientRejectsMissingHost(t *testing.T) {
	for _, uri := range []string{"thrift://", "thrift://:9083", "localhost:9083"} {
		t.Run(uri, func(t *testing.T) {
			client, err := newHiveClient(uri, nil)
			require.Error(t, err)
			require.Nil(t, client)
			require.ErrorContains(t, err, "host is required")
		})
	}
}
