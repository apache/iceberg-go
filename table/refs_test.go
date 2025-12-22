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
		"min-snapshots-to-keep": 10
	}`

	var snapRef table.SnapshotRef
	err := json.Unmarshal([]byte(ref), &snapRef)
	assert.NoError(t, err)

	assert.Equal(t, table.TagRef, snapRef.SnapshotRefType)
	assert.Equal(t, int64(3051729675574597004), snapRef.SnapshotID)
	assert.Equal(t, 10, *snapRef.MinSnapshotsToKeep)
	assert.Nil(t, snapRef.MaxRefAgeMs)
	assert.Nil(t, snapRef.MaxSnapshotAgeMs)
}
