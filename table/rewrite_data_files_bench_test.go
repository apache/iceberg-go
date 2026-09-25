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
	"fmt"
	"io/fs"
	"math/rand/v2"
	"os"
	"path/filepath"
	"slices"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/arrow-go/v18/parquet"
	"github.com/apache/arrow-go/v18/parquet/pqarrow"
	"github.com/apache/iceberg-go"
	iceio "github.com/apache/iceberg-go/io"
	"github.com/apache/iceberg-go/table"
	"github.com/stretchr/testify/require"
)

const (
	groupConcPartitions     = 8
	groupConcFilesPerPart   = 8
	groupConcRowsPerFile    = 30000
	groupConcPayloadWords   = 6
	groupConcPartitionField = 1000
)

func newGroupConcTable(tb testing.TB) *table.Table {
	tb.Helper()

	location := filepath.ToSlash(tb.TempDir())
	schema := iceberg.NewSchema(0,
		iceberg.NestedField{ID: 1, Name: "id", Type: iceberg.PrimitiveTypes.Int64, Required: true},
		iceberg.NestedField{ID: 2, Name: "data", Type: iceberg.PrimitiveTypes.String, Required: false},
		iceberg.NestedField{ID: 3, Name: "payload", Type: iceberg.PrimitiveTypes.String, Required: false},
		iceberg.NestedField{ID: 4, Name: "score", Type: iceberg.PrimitiveTypes.Float64, Required: false},
	)
	spec := iceberg.NewPartitionSpec(iceberg.PartitionField{
		SourceIDs: []int{2}, FieldID: groupConcPartitionField, Transform: iceberg.IdentityTransform{}, Name: "data",
	})
	meta, err := table.NewMetadata(schema, &spec, table.UnsortedSortOrder, location,
		iceberg.Properties{table.PropertyFormatVersion: "2"})
	require.NoError(tb, err)

	cat := &partialProgressCatalog{metadata: meta}

	return table.New(
		table.Identifier{"db", "group_concurrency_bench"},
		meta, location+"/metadata/v1.metadata.json",
		func(context.Context) (iceio.IO, error) { return iceio.LocalFS{}, nil },
		cat,
	)
}

func writeGroupConcFile(tb testing.TB, path string, sc *arrow.Schema, partition string, fileIdx int) int64 {
	tb.Helper()

	mem := memory.DefaultAllocator
	rng := rand.New(rand.NewPCG(uint64(fileIdx), 0x9e3779b97f4a7c15))

	idB := array.NewInt64Builder(mem)
	dataB := array.NewStringBuilder(mem)
	payloadB := array.NewStringBuilder(mem)
	scoreB := array.NewFloat64Builder(mem)
	defer idB.Release()
	defer dataB.Release()
	defer payloadB.Release()
	defer scoreB.Release()

	idB.Reserve(groupConcRowsPerFile)
	dataB.Reserve(groupConcRowsPerFile)
	payloadB.Reserve(groupConcRowsPerFile)
	scoreB.Reserve(groupConcRowsPerFile)

	var scratch [groupConcPayloadWords]uint64
	for i := range groupConcRowsPerFile {
		id := int64(fileIdx*groupConcRowsPerFile + i)
		idB.Append(id)
		dataB.Append(partition)
		for w := range groupConcPayloadWords {
			scratch[w] = rng.Uint64()
		}
		payloadB.Append(fmt.Sprintf("%016x%016x%016x%016x%016x%016x",
			scratch[0], scratch[1], scratch[2], scratch[3], scratch[4], scratch[5]))
		scoreB.Append(float64(id) * 0.5)
	}

	rec := array.NewRecordBatch(sc, []arrow.Array{
		idB.NewArray(), dataB.NewArray(), payloadB.NewArray(), scoreB.NewArray(),
	}, int64(groupConcRowsPerFile))
	defer rec.Release()

	fs := iceio.LocalFS{}
	fw, err := fs.Create(path)
	require.NoError(tb, err)
	defer fw.Close()

	arrTable := array.NewTableFromRecords(sc, []arrow.RecordBatch{rec})
	defer arrTable.Release()

	props := parquet.NewWriterProperties(parquet.WithStats(true))
	require.NoError(tb, pqarrow.WriteTable(arrTable, fw, int64(groupConcRowsPerFile), props, pqarrow.DefaultWriterProps()))

	info, err := os.Stat(path)
	require.NoError(tb, err)

	return info.Size()
}

func planGroupConcGroups(tb testing.TB, tbl *table.Table) []table.CompactionTaskGroup {
	tb.Helper()

	tasks, err := tbl.Scan().PlanFiles(context.Background())
	require.NoError(tb, err)
	require.Len(tb, tasks, groupConcPartitions*groupConcFilesPerPart)

	byPart := make(map[string][]table.FileScanTask, groupConcPartitions)
	for _, task := range tasks {
		part, ok := task.File.Partition()[groupConcPartitionField].(string)
		require.True(tb, ok)
		byPart[part] = append(byPart[part], task)
	}

	keys := make([]string, 0, len(byPart))
	for k := range byPart {
		keys = append(keys, k)
	}
	slices.Sort(keys)
	require.Len(tb, keys, groupConcPartitions)

	groups := make([]table.CompactionTaskGroup, 0, len(keys))
	for _, k := range keys {
		require.Len(tb, byPart[k], groupConcFilesPerPart)
		var total int64
		for _, task := range byPart[k] {
			total += task.File.FileSizeBytes()
		}
		groups = append(groups, table.CompactionTaskGroup{
			PartitionKey:   k,
			Tasks:          byPart[k],
			TotalSizeBytes: total,
		})
	}

	return groups
}

func groupConcOutputPaths(tb testing.TB, location string, before map[string]struct{}) []string {
	tb.Helper()

	var out []string
	err := filepath.WalkDir(filepath.Join(location, "data"), func(path string, entry fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if entry.IsDir() || filepath.Ext(path) != ".parquet" {
			return nil
		}
		if _, ok := before[path]; !ok {
			out = append(out, path)
		}

		return nil
	})
	require.NoError(tb, err)

	return out
}

func BenchmarkRewriteDataFilesGroupConcurrency(b *testing.B) {
	tbl := newGroupConcTable(b)

	arrowSc, err := table.SchemaToArrowSchema(tbl.Schema(), nil, false, false)
	require.NoError(b, err)

	ctx := context.Background()
	for p := range groupConcPartitions {
		partition := fmt.Sprintf("p%d", p)
		files := make([]iceberg.DataFile, 0, groupConcFilesPerPart)
		for f := range groupConcFilesPerPart {
			fileIdx := p*groupConcFilesPerPart + f
			dataPath := tbl.Location() + "/data/" + fmt.Sprintf("p%d-file-%d.parquet", p, f)
			size := writeGroupConcFile(b, dataPath, arrowSc, partition, fileIdx)
			builder, err := iceberg.NewDataFileBuilder(
				tbl.Spec(), iceberg.EntryContentData, dataPath, iceberg.ParquetFile,
				map[int]any{groupConcPartitionField: partition}, nil, nil, groupConcRowsPerFile, size)
			require.NoError(b, err)
			files = append(files, builder.Build())
		}
		txn := tbl.NewTransaction()
		require.NoError(b, txn.AddDataFiles(ctx, files, nil))
		tbl, err = txn.Commit(ctx)
		require.NoError(b, err)
	}

	groups := planGroupConcGroups(b, tbl)
	totalRows := int64(groupConcPartitions * groupConcFilesPerPart * groupConcRowsPerFile)

	for _, maxConcurrentGroups := range []int{1, 2, 4, 8} {
		b.Run(fmt.Sprintf("MaxConcurrentGroups=%d", maxConcurrentGroups), func(b *testing.B) {
			before, err := filepath.Glob(filepath.Join(tbl.Location(), "data", "*.parquet"))
			require.NoError(b, err)
			inputs := make(map[string]struct{}, len(before))
			for _, p := range before {
				inputs[p] = struct{}{}
			}
			opts := table.RewriteDataFilesOptions{MaxConcurrentGroups: maxConcurrentGroups}

			b.ReportAllocs()
			b.ResetTimer()
			var completed int64
			for b.Loop() {
				tx := tbl.NewTransaction()
				result, err := tx.RewriteDataFiles(ctx, groups, opts)
				require.NoError(b, err)
				require.Equal(b, groupConcPartitions, result.RewrittenGroups)
				completed++

				b.StopTimer()
				for _, p := range groupConcOutputPaths(b, tbl.Location(), inputs) {
					require.NoError(b, os.Remove(p))
				}
				b.StartTimer()
			}
			b.StopTimer()
			b.ReportMetric(float64(totalRows*completed)/b.Elapsed().Seconds(), "rows/s")
		})
	}
}
