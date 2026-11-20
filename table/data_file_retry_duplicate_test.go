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

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/arrow-go/v18/parquet/pqarrow"
	"github.com/apache/iceberg-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Path-based APIs need a real Parquet file so they can read its metadata.
func writeDuplicateRetryParquet(t *testing.T, fs *memIO, path string) {
	t.Helper()

	schema := arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int32, Nullable: false},
	}, nil)
	builder := array.NewInt32Builder(memory.DefaultAllocator)
	builder.Append(42)
	col := builder.NewArray()
	builder.Release()
	defer col.Release()

	rec := array.NewRecordBatch(schema, []arrow.Array{col}, 1)
	defer rec.Release()
	table := array.NewTableFromRecords(schema, []arrow.RecordBatch{rec})
	defer table.Release()

	out, err := fs.Create(t.Context(), path)
	require.NoError(t, err)
	require.NoError(t, pqarrow.WriteTable(table, out, table.NumRows(), nil, pqarrow.DefaultWriterProps()))
	require.NoError(t, out.Close())
}

// An operation that checked its base snapshot must not replay an addition
// against a concurrently added file with the same path.
func TestDirectDataFileWritesRejectConcurrentDuplicatePaths(t *testing.T) {
	cases := []struct {
		name  string
		stage func(*testing.T, *Transaction, *memIO) error
	}{
		{
			name: "AddDataFiles",
			stage: func(t *testing.T, tx *Transaction, _ *memIO) error {
				return tx.AddDataFiles(t.Context(), []iceberg.DataFile{retryCleanupFile(t, "duplicate")}, nil)
			},
		},
		{
			name: "ReplaceDataFiles",
			stage: func(t *testing.T, tx *Transaction, _ *memIO) error {
				return tx.ReplaceDataFiles(t.Context(),
					[]iceberg.DataFile{retryCleanupFile(t, "base")},
					[]iceberg.DataFile{retryCleanupFile(t, "duplicate")}, nil)
			},
		},

		{
			name: "AddDataFilesByPath",
			stage: func(t *testing.T, tx *Transaction, fs *memIO) error {
				path := retryCleanupFile(t, "duplicate").FilePath()
				writeDuplicateRetryParquet(t, fs, path)

				return tx.AddDataFilesByPath(t.Context(), []string{path}, nil, false)
			},
		},
		{
			name: "ReplaceDataFilesByPath",
			stage: func(t *testing.T, tx *Transaction, fs *memIO) error {
				path := retryCleanupFile(t, "duplicate").FilePath()
				writeDuplicateRetryParquet(t, fs, path)

				return tx.ReplaceDataFilesByPath(t.Context(),
					[]string{retryCleanupFile(t, "base").FilePath()}, []string{path}, nil)
			},
		},
		{
			name: "ReplaceFiles",
			stage: func(t *testing.T, tx *Transaction, _ *memIO) error {
				return tx.ReplaceFiles(t.Context(),
					[]iceberg.DataFile{retryCleanupFile(t, "base")},
					[]iceberg.DataFile{retryCleanupFile(t, "duplicate")}, nil, nil)
			},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			for _, level := range []IsolationLevel{WriteUpdateIsolationLevelDefault, IsolationSnapshot} {
				t.Run(string(level), func(t *testing.T) {
					synctest.Test(t, func(t *testing.T) {
						fs, cat, tbl := newExpireReplayTable(t, iceberg.Properties{
							WriteUpdateIsolationLevelKey: string(level),
						})
						tbl, _ = appendOnBranch(t, tbl, MainBranch, "base")
						tx := tbl.NewTransaction()
						require.NoError(t, tc.stage(t, tx, fs))
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

func TestDirectDataFileWritesRejectDuplicateOnFirstConcurrentSnapshot(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		fs, cat, tbl := newExpireReplayTable(t, nil)
		tx := tbl.NewTransaction()
		require.NoError(t, tx.AddDataFiles(t.Context(),
			[]iceberg.DataFile{retryCleanupFile(t, "duplicate")}, nil))

		// The writer has no base snapshot. The peer creates the first one.
		peerHead := peerAppend(t, fs, cat, MainBranch, "duplicate")
		attemptsBefore := cat.attempts.Load()

		_, err := tx.Commit(t.Context())
		require.ErrorIs(t, err, ErrDuplicateDataFiles)
		require.ErrorIs(t, err, ErrTransactionUnusable)
		require.NotErrorIs(t, err, ErrCommitFailed)
		require.Equal(t, attemptsBefore+1, cat.attempts.Load())
		require.Equal(t, peerHead, refHead(t, cat.metadata, MainBranch))
	})
}

func TestDirectDataFileWritesRejectConcurrentlyReaddedPath(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		fs, cat, tbl := newExpireReplayTable(t, nil)
		tbl, _ = appendOnBranch(t, tbl, MainBranch, "base")
		tx := tbl.NewTransaction()
		reused := retryCleanupFile(t, "reused")
		require.NoError(t, tx.AddDataFiles(t.Context(), []iceberg.DataFile{reused}, nil))

		// An intermediate removal does not make the path available if the
		// latest snapshot has added it again.
		peerAppend(t, fs, cat, MainBranch, "reused")
		peerTx := newOCCTable(t, cat.metadata, fs, cat).NewTransaction()
		require.NoError(t, peerTx.ReplaceDataFilesByPath(t.Context(),
			[]string{reused.FilePath()}, nil, nil))
		_, err := peerTx.Commit(t.Context())
		require.NoError(t, err)
		peerHead := peerAppend(t, fs, cat, MainBranch, "reused")

		attemptsBefore := cat.attempts.Load()
		_, err = tx.Commit(t.Context())
		require.ErrorIs(t, err, ErrDuplicateDataFiles)
		require.ErrorIs(t, err, ErrTransactionUnusable)
		require.NotErrorIs(t, err, ErrCommitFailed)
		require.Equal(t, attemptsBefore+1, cat.attempts.Load())
		require.Equal(t, peerHead, refHead(t, cat.metadata, MainBranch))
		require.Equal(t, []string{"base", "reused"}, branchFiles(t, fs, cat.metadata, MainBranch))
	})
}

func TestDirectDataFileWritesRetryNewBranchAgainstMain(t *testing.T) {
	for _, peerPath := range []string{"staged", "peer"} {
		t.Run(peerPath, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				const branch = "new-branch"
				fs, cat, tbl := newExpireReplayTable(t, nil)
				tbl, _ = appendOnBranch(t, tbl, MainBranch, "base")
				tx, err := tbl.NewTransactionOnBranch(branch)
				require.NoError(t, err)
				stageFiles(t, tx, "staged")
				mainHead := peerAppend(t, fs, cat, MainBranch, peerPath)
				attemptsBefore := cat.attempts.Load()

				committed, err := tx.Commit(t.Context())
				if peerPath == "staged" {
					require.ErrorIs(t, err, ErrDuplicateDataFiles)
					require.Equal(t, attemptsBefore+1, cat.attempts.Load())
					require.Nil(t, cat.metadata.SnapshotByName(branch))

					return
				}
				require.NoError(t, err)
				require.Equal(t, mainHead, parentOf(t, committed.Metadata(), refHead(t, committed.Metadata(), branch)))
				require.Equal(t, []string{"base", "peer", "staged"},
					branchFiles(t, fs, committed.Metadata(), branch))
			})
		})
	}
}

func TestDirectDataFileWritesRetryWithRefsOnlyHistory(t *testing.T) {
	for _, peerPath := range []string{"staged", "peer"} {
		t.Run(peerPath, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				fs, cat, tbl := newExpireReplayTable(t, nil)
				tbl, _ = appendOnBranch(t, tbl, MainBranch, "base")
				refs := &refsOnlyReloadCatalog{reqValidatingCatalog: cat}
				tx := newOCCTable(t, tbl.Metadata(), fs, refs).NewTransaction()
				stageFiles(t, tx, "staged")
				peerAppend(t, fs, cat, MainBranch, peerPath)
				head := peerAppend(t, fs, cat, MainBranch, "head")
				refs.partial = refsOnlySnapshotMetadata(t, cat.metadata, head)

				committed, err := tx.Commit(t.Context())
				require.Equal(t, 1, refs.loads)
				if peerPath == "staged" {
					require.ErrorIs(t, err, ErrDuplicateDataFiles)
					require.Equal(t, head, refHead(t, cat.metadata, MainBranch))

					return
				}
				require.NoError(t, err)
				require.Equal(t, []string{"base", "head", "peer", "staged"},
					branchFiles(t, fs, committed.Metadata(), MainBranch))
			})
		})
	}
}

func TestDirectDataFileRetryUsesLiveManifestsWithoutOriginalList(t *testing.T) {
	for _, peerPath := range []string{"duplicate", "unrelated"} {
		t.Run(peerPath, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				fs, cat, tbl := newExpireReplayTable(t, nil)
				tbl, _ = appendOnBranch(t, tbl, MainBranch, "base")
				originalList := tbl.CurrentSnapshot().ManifestList
				txn := tbl.NewTransaction()
				stageFiles(t, txn, "duplicate")
				peerHead := peerAppend(t, fs, cat, MainBranch, peerPath)
				require.NoError(t, fs.Remove(t.Context(), originalList))

				committed, err := txn.Commit(t.Context())
				if peerPath == "duplicate" {
					require.ErrorIs(t, err, ErrDuplicateDataFiles)
					require.Equal(t, peerHead, refHead(t, cat.metadata, MainBranch))

					return
				}

				require.NoError(t, err)
				require.Equal(t, []string{"base", "duplicate", "unrelated"},
					branchFiles(t, fs, committed.Metadata(), MainBranch))
			})
		})
	}
}

func TestDirectDataFileRetrySkipsInheritedManifests(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		fs, cat, tbl := newExpireReplayTable(t, nil)
		tbl, _ = appendOnBranch(t, tbl, MainBranch, "base")
		inherited, err := tbl.CurrentSnapshot().Manifests(t.Context(), fs)
		require.NoError(t, err)
		require.Len(t, inherited, 1)

		tracked := &unreadableListIO{memIO: fs}
		tx := newOCCTable(t, tbl.Metadata(), tracked, cat).NewTransaction()
		stageFiles(t, tx, "staged")
		peerAppend(t, fs, cat, MainBranch, "peer")
		tracked.unreadable = inherited[0].FilePath()

		committed, err := tx.Commit(t.Context())
		require.NoError(t, err)
		require.Equal(t, []string{"base", "peer", "staged"},
			branchFiles(t, fs, committed.Metadata(), MainBranch))
	})
}

func TestDirectDataFileRetryChecksEveryReplayParent(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		fs, peer, tbl := newExpireReplayTable(t, nil)
		tbl, _ = appendOnBranch(t, tbl, MainBranch, "base")
		peerAppend(t, fs, peer, MainBranch, "unrelated")
		firstHead := peer.metadata
		peerAppend(t, fs, peer, MainBranch, "duplicate")
		nextHead := peer.metadata

		cat := &peerHeadsCatalog{
			heads: []Metadata{firstHead, nextHead},
			errs:  []error{ErrCommitFailed, ErrCommitFailed},
		}
		txn := newOCCTable(t, tbl.Metadata(), fs, cat).NewTransaction()
		stageFiles(t, txn, "duplicate")
		_, err := txn.Commit(t.Context())
		require.ErrorIs(t, err, ErrDuplicateDataFiles)
		require.ErrorIs(t, err, ErrTransactionUnusable)
		require.Equal(t, 2, cat.loads)
		require.Equal(t, 2, cat.attempts)
		require.Equal(t, []string{"base", "duplicate", "unrelated"},
			branchFiles(t, fs, cat.current, MainBranch))
	})
}
