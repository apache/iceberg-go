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

package table_test

import (
	"encoding/json"
	"testing"

	"github.com/apache/iceberg-go/table"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestSerializeEmbeddedSnapshotPreservesNonzeroSequenceNumber(t *testing.T) {
	snapshot := table.Snapshot{
		SnapshotID:        25,
		SequenceNumber:    7,
		TimestampMs:       1602638573590,
		ManifestLocations: []string{"s3:/a/b/manifest.avro"},
	}

	data, err := json.Marshal(snapshot)
	require.NoError(t, err)

	assert.JSONEq(t, `{
		"snapshot-id": 25,
		"sequence-number": 7,
		"timestamp-ms": 1602638573590,
		"manifests": ["s3:/a/b/manifest.avro"]
	}`, string(data))
}
