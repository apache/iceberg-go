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
	"context"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/apache/iceberg-go"
	iceio "github.com/apache/iceberg-go/io"
	"github.com/apache/iceberg-go/table"
	"github.com/apache/iceberg-go/table/compaction"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestCompactionPreservesSplitFileRows(t *testing.T) {
	for _, split := range []bool{false, true} {
		for _, partial := range []bool{false, true} {
			for _, filtered := range []bool{false, true} {
				t.Run(fmt.Sprintf("split=%t/partial=%t/filtered=%t", split, partial, filtered), func(t *testing.T) {
					tbl := newRewriteTestTable(t)
					sc, err := table.SchemaToArrowSchema(tbl.Schema(), nil, false, false)
					require.NoError(t, err)
					var rows []string
					var want []int64
					for i := range 12 {
						rows = append(rows, fmt.Sprintf(`{"id":%d,"data":"a"}`, i+1))
						want = append(want, int64(i+1))
					}
					path := tbl.Location() + "/data/split.parquet"
					writeParquetFileWithProperties(t, path, sc, "["+strings.Join(rows, ",")+"]", 1, nil)
					splitSize := "999999999"
					if split {
						splitSize = "1"
					}
					tx := tbl.NewTransaction()
					require.NoError(t, tx.SetProperties(iceberg.Properties{table.ReadSplitTargetSizeKey: splitSize}))
					require.NoError(t, tx.AddFiles(t.Context(), []string{path}, nil, false))
					tbl, err = tx.Commit(t.Context())
					require.NoError(t, err)
					scan := tbl.Scan()
					filter := iceberg.GreaterThan(iceberg.Reference("id"), int64(6))
					if filtered {
						scan = tbl.Scan(table.WithRowFilter(filter))
					}
					tasks, err := scan.PlanFiles(t.Context())
					require.NoError(t, err)
					if filtered {
						// Exercise a residual a remote planner may leave after scan planning.
						for i := range tasks {
							tasks[i].Residual = filter
						}
						require.NotNil(t, tasks[0].Residual)
						require.False(t, tasks[0].Residual.Equals(iceberg.AlwaysTrue{}))
					}
					if split {
						require.Len(t, tasks, 12)
					} else {
						require.Len(t, tasks, 1)
					}
					size := tasks[0].File.FileSizeBytes()
					cfg := defaultTestCompactionCfg
					cfg.TargetFileSizeBytes, cfg.MinFileSizeBytes, cfg.MaxFileSizeBytes = size+1, size+1, size*2
					cfg.MinInputFiles = 1
					var groups []compaction.Group
					if filtered {
						plan, planErr := cfg.PlanCompaction(tasks)
						require.NoError(t, planErr)
						groups = plan.Groups
					} else {
						plan, planErr := compaction.Analyze(t.Context(), tbl, cfg)
						require.NoError(t, planErr)
						groups = plan.Groups
					}
					result, err := tbl.RewriteDataFiles(t.Context(), toTaskGroups(groups), table.RewriteDataFilesOptions{
						PartialProgress: partial, MaxCommits: 12,
					})
					assert.NoError(t, err)
					require.NotNil(t, result)
					require.NotNil(t, result.Table)
					require.Equal(t, 1, result.RewrittenGroups)
					require.ElementsMatch(t, want, scanIDs(t, result.Table), "a later batch failure must not lose unread source ranges")
				})
			}
		}
	}
}

func TestExecuteCompactionGroupRejectsIncompleteFilesBeforeIO(t *testing.T) {
	base := newRewriteTestTable(t)
	builder, err := iceberg.NewDataFileBuilder(*iceberg.UnpartitionedSpec, iceberg.EntryContentData,
		"missing.parquet", iceberg.ParquetFile, nil, nil, nil, 1, 100)
	require.NoError(t, err)
	file := builder.Build()
	whole := table.FileScanTask{File: file, Length: 100}
	filtered := whole
	filtered.Residual = iceberg.EqualTo(iceberg.Reference("id"), int64(1))
	cases := []struct {
		name  string
		tasks []table.FileScanTask
	}{
		{"nil file", []table.FileScanTask{{}}},
		{"nonzero start", []table.FileScanTask{{File: file, Start: 1, Length: 99}}},
		{"negative start", []table.FileScanTask{{File: file, Start: -1, Length: 100}}},
		{"short range", []table.FileScanTask{{File: file, Length: 99}}},
		{"long range", []table.FileScanTask{{File: file, Length: 101}}},
		{"negative length", []table.FileScanTask{{File: file, Length: -1}}},
		{"duplicate path", []table.FileScanTask{whole, whole}},
		{"residual filter", []table.FileScanTask{filtered}},
		{"false residual", []table.FileScanTask{{File: file, Length: 100, Residual: iceberg.AlwaysFalse{}}}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var calls atomic.Int32
			tbl := table.New(base.Identifier(), base.Metadata(), base.MetadataLocation(),
				func(context.Context) (iceio.IO, error) {
					calls.Add(1)

					return nil, errors.New("unexpected IO")
				}, nil)
			result, err := table.ExecuteCompactionGroup(t.Context(), tbl, table.CompactionTaskGroup{Tasks: tc.tasks})
			require.ErrorIs(t, err, table.ErrInvalidOperation)
			require.Zero(t, calls.Load(), "invalid groups must fail before opening input or output files")
			require.Empty(t, result.NewDataFiles)
		})
	}
}

func TestExecuteCompactionGroupAcceptsWholeFileRanges(t *testing.T) {
	base := newRewriteTestTable(t)
	builder, err := iceberg.NewDataFileBuilder(*iceberg.UnpartitionedSpec, iceberg.EntryContentData,
		"missing.parquet", iceberg.ParquetFile, nil, nil, nil, 1, 100)
	require.NoError(t, err)
	file := builder.Build()
	for _, length := range []int64{0, 100} {
		t.Run(strconv.FormatInt(length, 10), func(t *testing.T) {
			var calls atomic.Int32
			ioErr := errors.New("input reached")
			tbl := table.New(base.Identifier(), base.Metadata(), base.MetadataLocation(),
				func(context.Context) (iceio.IO, error) {
					calls.Add(1)

					return nil, ioErr
				}, nil)
			_, err := table.ExecuteCompactionGroup(t.Context(), tbl, table.CompactionTaskGroup{Tasks: []table.FileScanTask{
				{File: file, Length: length, Residual: iceberg.AlwaysTrue{}},
			}})
			require.ErrorIs(t, err, ioErr)
			require.Equal(t, int32(1), calls.Load())
		})
	}
}
