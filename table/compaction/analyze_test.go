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

package compaction_test

import (
	"context"
	"fmt"
	"path/filepath"
	"strings"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/arrow-go/v18/parquet"
	"github.com/apache/arrow-go/v18/parquet/pqarrow"
	"github.com/apache/iceberg-go"
	iceio "github.com/apache/iceberg-go/io"
	"github.com/apache/iceberg-go/table"
	"github.com/apache/iceberg-go/table/compaction"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// analyzeCatalog is a minimal catalog for local e2e tests.
type analyzeCatalog struct {
	metadata table.Metadata
}

func (m *analyzeCatalog) LoadTable(ctx context.Context, ident table.Identifier) (*table.Table, error) {
	return nil, nil
}

func (m *analyzeCatalog) CommitTable(ctx context.Context, ident table.Identifier, reqs []table.Requirement, updates []table.Update) (table.Metadata, string, error) {
	meta, err := table.UpdateTableMetadata(m.metadata, updates, "")
	if err != nil {
		return nil, "", err
	}

	m.metadata = meta

	return meta, "", nil
}

func newAnalyzeTestTable(t *testing.T) *table.Table {
	t.Helper()

	location := filepath.ToSlash(t.TempDir())

	schema := iceberg.NewSchema(0,
		iceberg.NestedField{ID: 1, Name: "id", Type: iceberg.PrimitiveTypes.Int64, Required: true},
		iceberg.NestedField{ID: 2, Name: "data", Type: iceberg.PrimitiveTypes.String, Required: false},
	)

	meta, err := table.NewMetadata(schema, iceberg.UnpartitionedSpec,
		table.UnsortedSortOrder, location,
		iceberg.Properties{table.PropertyFormatVersion: "2"})
	require.NoError(t, err)

	return table.New(
		table.Identifier{"db", "analyze_test"},
		meta, location+"/metadata/v1.metadata.json",
		func(ctx context.Context) (iceio.IO, error) {
			return iceio.LocalFS{}, nil
		},
		&analyzeCatalog{metadata: meta},
	)
}

func writeTestParquetFile(t testing.TB, path string, sc *arrow.Schema, jsonData string) {
	t.Helper()

	rec, _, err := array.RecordFromJSON(memory.DefaultAllocator, sc, strings.NewReader(jsonData))
	require.NoError(t, err)
	defer rec.Release()

	fs := iceio.LocalFS{}
	fw, err := fs.Create(path)
	require.NoError(t, err)

	tbl := array.NewTableFromRecords(sc, []arrow.RecordBatch{rec})
	defer tbl.Release()

	require.NoError(t, pqarrow.WriteTable(tbl, fw, rec.NumRows(),
		parquet.NewWriterProperties(parquet.WithStats(true)),
		pqarrow.DefaultWriterProps()))
}

func TestAnalyze_SmallFiles(t *testing.T) {
	tbl := newAnalyzeTestTable(t)

	arrowSc, err := table.SchemaToArrowSchema(tbl.Schema(), nil, false, false)
	require.NoError(t, err)

	for i := range 5 {
		dataPath := tbl.Location() + fmt.Sprintf("/data/file-%d.parquet", i)
		writeTestParquetFile(t, dataPath, arrowSc,
			fmt.Sprintf(`[{"id": %d, "data": "row-%d"}]`, i+1, i+1))

		tx := tbl.NewTransaction()
		require.NoError(t, tx.AddFiles(t.Context(), []string{dataPath}, nil, false))
		tbl, err = tx.Commit(t.Context())
		require.NoError(t, err)
	}

	cfg := compaction.Config{
		TargetFileSizeBytes: 10 * 1024 * 1024,
		MinFileSizeBytes:    5 * 1024 * 1024,
		MaxFileSizeBytes:    20 * 1024 * 1024,
		MinInputFiles:       2,
		DeleteFileThreshold: 5,
		PackingLookback:     compaction.DefaultPackingLookback,
	}

	plan, err := compaction.Analyze(t.Context(), tbl, cfg)
	require.NoError(t, err)

	assert.Equal(t, 5, plan.TotalInputFiles)
	assert.Greater(t, plan.TotalInputBytes, int64(0))
	require.NotEmpty(t, plan.Groups)
	assert.Equal(t, 0, plan.SkippedFiles)
	assert.Greater(t, plan.EstOutputFiles, 0)
}

func TestAnalyze_AllOptimal(t *testing.T) {
	tbl := newAnalyzeTestTable(t)

	arrowSc, err := table.SchemaToArrowSchema(tbl.Schema(), nil, false, false)
	require.NoError(t, err)

	dataPath := tbl.Location() + "/data/file-0.parquet"
	writeTestParquetFile(t, dataPath, arrowSc, `[{"id": 1, "data": "hello"}]`)

	tx := tbl.NewTransaction()
	require.NoError(t, tx.AddFiles(t.Context(), []string{dataPath}, nil, false))
	tbl, err = tx.Commit(t.Context())
	require.NoError(t, err)

	cfg := compaction.Config{
		TargetFileSizeBytes: 100,
		MinFileSizeBytes:    1,
		MaxFileSizeBytes:    1024 * 1024 * 1024,
		MinInputFiles:       5,
		DeleteFileThreshold: 5,
		PackingLookback:     compaction.DefaultPackingLookback,
	}

	plan, err := compaction.Analyze(t.Context(), tbl, cfg)
	require.NoError(t, err)

	assert.Equal(t, 1, plan.TotalInputFiles)
	assert.Empty(t, plan.Groups)
}

func TestAnalyze_EmptyTable(t *testing.T) {
	tbl := newAnalyzeTestTable(t)

	cfg := compaction.DefaultConfig()
	plan, err := compaction.Analyze(t.Context(), tbl, cfg)
	require.NoError(t, err)

	assert.Equal(t, 0, plan.TotalInputFiles)
	assert.Empty(t, plan.Groups)
}

func TestAnalyze_InvalidConfig(t *testing.T) {
	tbl := newAnalyzeTestTable(t)

	cfg := compaction.Config{TargetFileSizeBytes: 0}
	_, err := compaction.Analyze(t.Context(), tbl, cfg)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "target file size must be positive")
}
