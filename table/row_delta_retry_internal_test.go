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
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package table

import (
	"testing"
	"testing/synctest"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestRowDeltaChecksDuplicateDataPathsOnRetry(t *testing.T) {
	t.Run("rejects a path added by a concurrent snapshot", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			fs, cat, tbl := newExpireReplayTable(t, nil)
			tbl, _ = appendOnBranch(t, tbl, MainBranch, "base")

			file := retryCleanupFile(t, "duplicate")
			tx := tbl.NewTransaction()
			require.NoError(t, tx.NewRowDelta(nil).AddRows(file).Commit(t.Context()))

			peerHead := peerAppend(t, fs, cat, MainBranch, "duplicate")
			attemptsBefore := cat.attempts.Load()
			_, err := tx.Commit(t.Context())

			require.ErrorIs(t, err, ErrTransactionUnusable)
			require.ErrorIs(t, err, ErrConflictingDataFiles)
			assert.Contains(t, err.Error(), file.FilePath())
			assert.Equal(t, attemptsBefore+1, cat.attempts.Load(),
				"the retry validator must reject before a second catalog commit")
			assert.Equal(t, peerHead, refHead(t, cat.metadata, MainBranch))
		})
	})

	t.Run("allows a different path added concurrently", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			fs, cat, tbl := newExpireReplayTable(t, nil)
			tbl, _ = appendOnBranch(t, tbl, MainBranch, "base")

			file := retryCleanupFile(t, "writer")
			tx := tbl.NewTransaction()
			require.NoError(t, tx.NewRowDelta(nil).AddRows(file).Commit(t.Context()))
			peerAppend(t, fs, cat, MainBranch, "peer")

			committed, err := tx.Commit(t.Context())
			require.NoError(t, err)
			assert.Equal(t, []string{"base", "peer", "writer"},
				branchFiles(t, fs, committed.Metadata(), MainBranch))
		})
	})

	t.Run("allows the path after a later concurrent deletion", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			fs, cat, tbl := newExpireReplayTable(t, nil)
			tbl, _ = appendOnBranch(t, tbl, MainBranch, "base")

			file := retryCleanupFile(t, "reused")
			tx := tbl.NewTransaction()
			require.NoError(t, tx.NewRowDelta(nil).AddRows(file).Commit(t.Context()))
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
	})
}
