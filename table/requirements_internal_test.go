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

package table

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestAssertLastAssignedPartitionIDValidate(t *testing.T) {
	t.Run("missing last partition id returns requirement failure", func(t *testing.T) {
		meta := &metadataV1{
			commonMetadata: commonMetadata{},
		}
		req := AssertLastAssignedPartitionID(0)

		err := req.Validate(meta)
		require.Error(t, err)
		assert.EqualError(t, err, "requirement failed: last assigned partition id is missing")
		assert.NotErrorIs(t, err, ErrCommitFailed, "malformed metadata is not a commit conflict")
	})

	t.Run("matches metadata last partition id", func(t *testing.T) {
		meta, err := ParseMetadataBytes([]byte(ExampleTableMetadataV2))
		require.NoError(t, err)
		req := AssertLastAssignedPartitionID(*meta.LastPartitionSpecID())
		assert.NoError(t, req.Validate(meta))
	})

	t.Run("mismatched last partition id is rejected", func(t *testing.T) {
		meta, err := ParseMetadataBytes([]byte(ExampleTableMetadataV2))
		require.NoError(t, err)
		req := AssertLastAssignedPartitionID(*meta.LastPartitionSpecID() + 1)
		err = req.Validate(meta)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "last assigned partition id has changed")
	})
}

func TestAssertBranchRefSnapshotIDRejectsTagWithoutRetry(t *testing.T) {
	meta, err := ParseMetadataBytes([]byte(ExampleTableMetadataV2))
	require.NoError(t, err)

	// "test" is a tag in ExampleTableMetadataV2. Targeting a tag is a usage
	// error that a refresh cannot fix, so it must not look retryable.
	err = assertBranchRefSnapshotID("test", new(int64(3051729675574597004))).Validate(meta)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "tags cannot be transaction targets")
	assert.NotErrorIs(t, err, ErrCommitFailed)
}
