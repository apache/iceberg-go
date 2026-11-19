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
	"testing/synctest"

	"github.com/apache/iceberg-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// An operation that checked its base snapshot must not replay an addition
// against a concurrently added file with the same path.
func TestDirectDataFileWritesRejectConcurrentDuplicatePaths(t *testing.T) {
	cases := []struct {
		name  string
		stage func(*testing.T, *Transaction) error
	}{
		{
			name: "AddDataFiles",
			stage: func(t *testing.T, tx *Transaction) error {
				return tx.AddDataFiles(t.Context(), []iceberg.DataFile{retryCleanupFile(t, "duplicate")}, nil)
			},
		},
		{
			name: "ReplaceDataFiles",
			stage: func(t *testing.T, tx *Transaction) error {
				return tx.ReplaceDataFiles(t.Context(),
					[]iceberg.DataFile{retryCleanupFile(t, "base")},
					[]iceberg.DataFile{retryCleanupFile(t, "duplicate")}, nil)
			},
		},
		{
			name: "ReplaceFiles",
			stage: func(t *testing.T, tx *Transaction) error {
				return tx.ReplaceFiles(t.Context(),
					[]iceberg.DataFile{retryCleanupFile(t, "base")},
					[]iceberg.DataFile{retryCleanupFile(t, "duplicate")}, nil, nil)
			},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				fs, cat, tbl := newExpireReplayTable(t, iceberg.Properties{
					WriteUpdateIsolationLevelKey: string(IsolationSnapshot),
				})
				tbl, _ = appendOnBranch(t, tbl, MainBranch, "base")
				tx := tbl.NewTransaction()
				require.NoError(t, tc.stage(t, tx))
				peerHead := peerAppend(t, fs, cat, MainBranch, "duplicate")
				attemptsBefore := cat.attempts.Load()

				_, err := tx.Commit(t.Context())

				require.ErrorIs(t, err, ErrDuplicateDataFiles)
				require.ErrorIs(t, err, ErrTransactionUnusable)
				require.NotErrorIs(t, err, ErrCommitFailed)
				assert.Equal(t, attemptsBefore+1, cat.attempts.Load(),
					"the duplicate must fail before a second catalog commit")
				assert.Equal(t, peerHead, refHead(t, cat.metadata, MainBranch))
			})
		})
	}
}

// Unrelated appends must remain commutative on the retry path.
func TestDirectDataFileWritesAllowUnrelatedConcurrentAppends(t *testing.T) {
	for _, replace := range []bool{false, true} {
		name := "AddDataFiles"
		if replace {
			name = "ReplaceDataFiles"
		}
		t.Run(name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				fs, cat, tbl := newExpireReplayTable(t, iceberg.Properties{
					WriteUpdateIsolationLevelKey: string(IsolationSnapshot),
				})
				tbl, _ = appendOnBranch(t, tbl, MainBranch, "base")
				tx := tbl.NewTransaction()
				writer := retryCleanupFile(t, "writer")
				var err error
				if replace {
					err = tx.ReplaceDataFiles(t.Context(),
						[]iceberg.DataFile{retryCleanupFile(t, "base")}, []iceberg.DataFile{writer}, nil)
				} else {
					err = tx.AddDataFiles(t.Context(), []iceberg.DataFile{writer}, nil)
				}
				require.NoError(t, err)
				peerAppend(t, fs, cat, MainBranch, "peer")

				committed, err := tx.Commit(t.Context())
				require.NoError(t, err)
				want := []string{"base", "peer", "writer"}
				if replace {
					want = []string{"peer", "writer"}
				}
				assert.Equal(t, want, branchFiles(t, fs, committed.Metadata(), MainBranch))
			})
		})
	}
}

// The latest concurrent status controls uniqueness, not an earlier add.
func TestDirectDataFileWritesAllowConcurrentlyRemovedPath(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		fs, cat, tbl := newExpireReplayTable(t, nil)
		tbl, _ = appendOnBranch(t, tbl, MainBranch, "base")
		tx := tbl.NewTransaction()
		file := retryCleanupFile(t, "reused")
		require.NoError(t, tx.AddDataFiles(t.Context(), []iceberg.DataFile{file}, nil))
		peerAppend(t, fs, cat, MainBranch, "reused")

		peerTx := newOCCTable(t, cat.metadata, fs, cat).NewTransaction()
		require.NoError(t, peerTx.ReplaceDataFilesByPath(t.Context(), []string{file.FilePath()}, nil, nil))
		_, err := peerTx.Commit(t.Context())
		require.NoError(t, err)

		committed, err := tx.Commit(t.Context())
		require.NoError(t, err)
		assert.Equal(t, []string{"base", "reused"},
			branchFiles(t, fs, committed.Metadata(), MainBranch))
	})
}
