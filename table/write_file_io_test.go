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
	"strings"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/iceberg-go"
	iceio "github.com/apache/iceberg-go/io"
	"github.com/stretchr/testify/require"
)

func newReadOnlyWriteTable(t *testing.T, overrides ...iceberg.Properties) *Table {
	t.Helper()

	props := iceberg.Properties{PropertyFormatVersion: "2"}
	for _, override := range overrides {
		for k, v := range override {
			props[k] = v
		}
	}

	schema := simpleSchema()
	meta, err := NewMetadata(schema, iceberg.UnpartitionedSpec, UnsortedSortOrder, "file:///tmp/read-only-write",
		props)
	require.NoError(t, err)

	cat := &flakyCatalog{metadata: meta}

	return New(
		Identifier{"db", "read-only-write"},
		meta,
		"file:///tmp/read-only-write/metadata/v1.metadata.json",
		func(context.Context) (iceio.IO, error) { return readOnlyIO{}, nil },
		cat,
	)
}

func newSingleRowReader(t *testing.T, schema *iceberg.Schema) array.RecordReader {
	t.Helper()

	arrowSchema, err := SchemaToArrowSchema(schema, nil, true, false)
	require.NoError(t, err)

	rec, _, err := array.RecordFromJSON(memory.DefaultAllocator, arrowSchema, strings.NewReader(`[{"id": 1}]`))
	require.NoError(t, err)
	defer rec.Release()

	tbl := array.NewTableFromRecords(arrowSchema, []arrow.RecordBatch{rec})
	rdr := array.NewTableReader(tbl, -1)
	t.Cleanup(func() {
		rdr.Release()
		tbl.Release()
	})

	return rdr
}

func newWriteIORequiredDataFileWithPath(t *testing.T, path string) iceberg.DataFile {
	t.Helper()

	builder, err := iceberg.NewDataFileBuilder(
		*iceberg.UnpartitionedSpec,
		iceberg.EntryContentData,
		path,
		iceberg.ParquetFile,
		nil,
		nil,
		nil,
		1,
		1,
	)
	require.NoError(t, err)

	return builder.Build()
}

func newWriteIORequiredDataFile(t *testing.T) iceberg.DataFile {
	t.Helper()

	return newWriteIORequiredDataFileWithPath(t, "file:///tmp/read-only-write/data.parquet")
}

func TestWritePathsReturnErrorForReadOnlyIO(t *testing.T) {
	for _, tc := range []struct {
		name  string
		props iceberg.Properties
		run   func(context.Context, *Table) error
	}{
		{
			name: "append",
			run: func(ctx context.Context, tbl *Table) error {
				return tbl.NewTransaction().Append(ctx, newSingleRowReader(t, tbl.Schema()), nil)
			},
		},
		{
			name: "overwrite",
			run: func(ctx context.Context, tbl *Table) error {
				return tbl.NewTransaction().Overwrite(ctx, newSingleRowReader(t, tbl.Schema()), nil)
			},
		},
		{
			name: "copy-on-write delete",
			run: func(ctx context.Context, tbl *Table) error {
				return tbl.NewTransaction().Delete(ctx, iceberg.AlwaysTrue{}, nil)
			},
		},
		{
			name:  "merge-on-read delete",
			props: iceberg.Properties{WriteDeleteModeKey: WriteModeMergeOnRead},
			run: func(ctx context.Context, tbl *Table) error {
				return tbl.NewTransaction().Delete(ctx, iceberg.AlwaysTrue{}, nil)
			},
		},
		{
			name: "row delta",
			run: func(ctx context.Context, tbl *Table) error {
				return tbl.NewTransaction().NewRowDelta(nil).
					AddRows(newWriteIORequiredDataFile(t)).
					Commit(ctx)
			},
		},
		{
			name: "add data files",
			run: func(ctx context.Context, tbl *Table) error {
				return tbl.NewTransaction().AddDataFiles(ctx, []iceberg.DataFile{
					newWriteIORequiredDataFileWithPath(t, "file:///tmp/read-only-write/add-data-files.parquet"),
				}, nil)
			},
		},
		{
			name: "replace data files with data files add-only path",
			run: func(ctx context.Context, tbl *Table) error {
				return tbl.NewTransaction().ReplaceDataFilesWithDataFiles(ctx, nil, []iceberg.DataFile{
					newWriteIORequiredDataFileWithPath(t, "file:///tmp/read-only-write/replace-data-files-add.parquet"),
				}, nil)
			},
		},
		{
			name: "add files",
			run: func(ctx context.Context, tbl *Table) error {
				return tbl.NewTransaction().AddFiles(ctx, []string{
					"file:///tmp/read-only-write/add-files.parquet",
				}, nil, false)
			},
		},
		{
			name: "replace files add-only path",
			run: func(ctx context.Context, tbl *Table) error {
				return tbl.NewTransaction().ReplaceFiles(ctx, nil, []iceberg.DataFile{
					newWriteIORequiredDataFileWithPath(t, "file:///tmp/read-only-write/replace-files-add.parquet"),
				}, nil, nil)
			},
		},
		{
			name: "write equality deletes",
			run: func(ctx context.Context, tbl *Table) error {
				records := func(yield func(arrow.RecordBatch, error) bool) {}
				_, err := tbl.NewTransaction().WriteEqualityDeletes(ctx, []int{1}, records)

				return err
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			tbl := newReadOnlyWriteTable(t, tc.props)
			var err error
			// NotPanics guards against the pre-fix unsafe fs.(io.WriteFileIO) assertions.
			require.NotPanics(t, func() {
				err = tc.run(t.Context(), tbl)
			})
			require.ErrorIs(t, err, ErrWriteIORequired)
			require.ErrorIs(t, err, iceberg.ErrNotImplemented)
		})
	}
}
