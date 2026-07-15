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
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestDeleteOrphanFilesRejectsNegativeAgeBeforeScan(t *testing.T) {
	_, err := (Table{}).DeleteOrphanFiles(context.Background(), WithFilesOlderThan(-time.Nanosecond))
	require.EqualError(t, err, "orphan cleanup age must be non-negative")
}

func TestExpireSnapshotsRejectsInvalidRetentionBeforeMetadataAccess(t *testing.T) {
	tests := []struct {
		name string
		opt  ExpireSnapshotsOpt
		err  string
	}{
		{name: "negative age", opt: WithOlderThan(-time.Nanosecond), err: "snapshot age must be non-negative"},
		{name: "zero retain last", opt: WithRetainLast(0), err: "retain-last must be at least 1"},
		{name: "negative retain last", opt: WithRetainLast(-1), err: "retain-last must be at least 1"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// A nil metadata builder proves validation happens before any metadata access.
			err := (&Transaction{}).ExpireSnapshots(tt.opt)
			require.EqualError(t, err, tt.err)
		})
	}
}

func TestRetentionOptionsAcceptZeroAgeAndOneSnapshot(t *testing.T) {
	cleanupCfg := &orphanCleanupConfig{}
	WithFilesOlderThan(0)(cleanupCfg)
	require.NoError(t, cleanupCfg.validationErr)

	expireCfg := &expireSnapshotsCfg{}
	WithOlderThan(0)(expireCfg)
	WithRetainLast(1)(expireCfg)
	require.NoError(t, expireCfg.validationErr)
}
