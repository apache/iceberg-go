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

	"github.com/apache/iceberg-go"
	"github.com/apache/iceberg-go/table"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestInvalidSnapshotRef(t *testing.T) {
	ref := `{
		"snapshot-id": "3051729675574597004",
		"type": "foobar"
	}`

	var snapRef table.SnapshotRef
	err := json.Unmarshal([]byte(ref), &snapRef)
	assert.Error(t, err)
}

func TestInvalidSnapshotRefType(t *testing.T) {
	ref := `{
		"snapshot-id": 3051729675574597004,
		"type": "foobar"
	}`

	var snapRef table.SnapshotRef
	err := json.Unmarshal([]byte(ref), &snapRef)
	assert.ErrorIs(t, err, table.ErrInvalidRefType)
}

func TestSnapshotBranchRef(t *testing.T) {
	ref := `{
		"snapshot-id": 3051729675574597004,
		"type": "branch"
	}`

	var snapRef table.SnapshotRef
	err := json.Unmarshal([]byte(ref), &snapRef)
	assert.NoError(t, err)

	assert.Equal(t, table.BranchRef, snapRef.SnapshotRefType)
	assert.Equal(t, int64(3051729675574597004), snapRef.SnapshotID)
	assert.Nil(t, snapRef.MinSnapshotsToKeep)
	assert.Nil(t, snapRef.MaxRefAgeMs)
	assert.Nil(t, snapRef.MaxSnapshotAgeMs)
}

func TestSnapshotTagRef(t *testing.T) {
	ref := `{
		"snapshot-id": 3051729675574597004,
		"type": "tag",
		"max-ref-age-ms": 10
	}`

	var snapRef table.SnapshotRef
	err := json.Unmarshal([]byte(ref), &snapRef)
	assert.NoError(t, err)

	assert.Equal(t, table.TagRef, snapRef.SnapshotRefType)
	assert.Equal(t, int64(3051729675574597004), snapRef.SnapshotID)
	assert.Nil(t, snapRef.MinSnapshotsToKeep)
	assert.Equal(t, int64(10), *snapRef.MaxRefAgeMs)
	assert.Nil(t, snapRef.MaxSnapshotAgeMs)
}

func TestSnapshotRefRequiresFields(t *testing.T) {
	for _, ref := range []string{
		`{"type":"branch"}`,
		`{"snapshot-id":1}`,
	} {
		var snapRef table.SnapshotRef
		err := json.Unmarshal([]byte(ref), &snapRef)
		require.ErrorIs(t, err, iceberg.ErrInvalidArgument)
	}
}

func TestSnapshotRefRejectsInvalidRetention(t *testing.T) {
	tests := []string{
		`{"snapshot-id":1,"type":"branch","min-snapshots-to-keep":0}`,
		`{"snapshot-id":1,"type":"branch","max-snapshot-age-ms":-1}`,
		`{"snapshot-id":1,"type":"branch","max-ref-age-ms":0}`,
		`{"snapshot-id":1,"type":"tag","min-snapshots-to-keep":1}`,
		`{"snapshot-id":1,"type":"tag","max-snapshot-age-ms":1}`,
	}

	for _, ref := range tests {
		var snapRef table.SnapshotRef
		err := json.Unmarshal([]byte(ref), &snapRef)
		require.ErrorIs(t, err, iceberg.ErrInvalidArgument)
	}
}
