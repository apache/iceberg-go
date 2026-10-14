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
	"bytes"
	"fmt"
	"slices"
	"testing"

	"github.com/apache/iceberg-go"
	iceio "github.com/apache/iceberg-go/io"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type nonComparableEqualityMetadataFile struct {
	iceberg.DataFile
	fieldIDs []int
}

func (f nonComparableEqualityMetadataFile) EqualityFieldIDs() []int {
	return slices.Clone(f.fieldIDs)
}

func TestEqualityDeleteMetadataAcceptsDistinctSamePathFiles(t *testing.T) {
	t.Parallel()

	schema := iceberg.NewSchema(0,
		iceberg.NestedField{ID: 1, Name: "id", Type: iceberg.PrimitiveTypes.Int64, Required: true},
	)
	wrappers := []struct {
		name string
		wrap func(iceberg.DataFile) iceberg.DataFile
	}{
		{
			name: "borrowed metadata",
			wrap: func(file iceberg.DataFile) iceberg.DataFile { return file },
		},
		{
			name: "public getter with non-comparable interface value",
			wrap: func(file iceberg.DataFile) iceberg.DataFile {
				// The outer struct is comparable, but its interface value is not.
				return struct{ iceberg.DataFile }{nonComparableEqualityMetadataFile{
					DataFile: file,
					fieldIDs: file.EqualityFieldIDs(),
				}}
			},
		},
	}

	for _, wrapper := range wrappers {
		for _, useMap := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/map=%t", wrapper.name, useMap), func(t *testing.T) {
				fs := &countingEqualityDeleteOpenFS{MemFS: iceio.NewMemFS()}
				path := "mem://metadata-accept/delete.parquet"
				writeEqualityDeleteParquetToMemFS(t, fs.MemFS, path, `[{"id": 1}]`)
				first := newEqualityDeleteSetAssemblyTestFile(t, path, []int{1})
				second := newEqualityDeleteSetAssemblyTestFile(t, path, []int{1})
				require.NotSame(t, first, second)
				tasks := []FileScanTask{{EqualityDeleteFiles: []iceberg.DataFile{wrapper.wrap(first)}}}
				uniqueFiles := 1
				if useMap {
					otherPath := "mem://metadata-accept/other.parquet"
					writeEqualityDeleteParquetToMemFS(t, fs.MemFS, otherPath, `[{"id": 2}]`)
					other := newEqualityDeleteSetAssemblyTestFile(t, otherPath, []int{1})
					tasks = append(tasks, FileScanTask{EqualityDeleteFiles: []iceberg.DataFile{other}})
					uniqueFiles++
				}
				tasks = append(tasks, FileScanTask{EqualityDeleteFiles: []iceberg.DataFile{wrapper.wrap(second)}})

				loader, err := newLazyEqualityDeleteLoader(fs, schema, nil, nil, tasks)
				require.NoError(t, err)
				require.NotNil(t, loader)
				assert.Len(t, loader.files, uniqueFiles)
				assert.Zero(t, fs.attempts.Load())
				var firstSet *equalityDeleteSet
				for i, task := range tasks {
					sets, err := loader.load(t.Context(), task)
					require.NoError(t, err)
					require.Len(t, sets, 1)
					if i == 0 {
						firstSet = sets[0]
					} else if i == len(tasks)-1 {
						assert.Same(t, firstSet, sets[0])
					}
				}
				assert.Equal(t, int64(uniqueFiles), fs.attempts.Load())
				assert.Equal(t, int64(uniqueFiles), fs.opens.Load())
				var key bytes.Buffer
				key.WriteByte(1)
				bufPutUint64(&key, 1)
				assert.Equal(t, set[string]{key.String(): {}}, firstSet.keys)
				assert.Equal(t, []int{1}, firstSet.fieldIDs)

				fs.attempts.Store(0)
				fs.opens.Store(0)
				perTask, err := readAllEqualityDeleteFiles(t.Context(), fs, schema, nil, tasks, 1)
				require.NoError(t, err)
				require.Len(t, perTask, len(tasks))
				require.Len(t, perTask[0], 1)
				require.Len(t, perTask[len(tasks)-1], 1)
				assert.Same(t, perTask[0][0], perTask[len(tasks)-1][0])
				assert.Equal(t, firstSet, perTask[0][0])
				assert.Equal(t, int64(uniqueFiles), fs.attempts.Load())
				assert.Equal(t, int64(uniqueFiles), fs.opens.Load())
			})
		}
	}
}

func TestEqualityDeleteMetadataConflictErrors(t *testing.T) {
	t.Parallel()

	schema := iceberg.NewSchema(0,
		iceberg.NestedField{ID: 1, Name: "id", Type: iceberg.PrimitiveTypes.Int64, Required: true},
		iceberg.NestedField{ID: 2, Name: "data", Type: iceberg.PrimitiveTypes.Int64, Required: true},
	)
	path := "mem://metadata-conflict/delete.parquet"
	first := newEqualityDeleteSetAssemblyTestFile(t, path, []int{1, 2})
	for _, tt := range []struct {
		name     string
		fieldIDs []int
		format   iceberg.FileFormat
		wantErr  error
	}{
		{"different IDs", []int{1, 3}, iceberg.ParquetFile, ErrConflictingEqualityDeleteMetadata},
		{"reordered IDs", []int{2, 1}, iceberg.ParquetFile, nil},
		{"different format", []int{1, 2}, iceberg.AvroFile, ErrConflictingEqualityDeleteMetadata},
		{"empty IDs", nil, iceberg.ParquetFile, ErrEmptyEqualityFieldIDs},
	} {
		t.Run(tt.name, func(t *testing.T) {
			builder, err := iceberg.NewDataFileBuilder(
				*iceberg.UnpartitionedSpec, iceberg.EntryContentEqDeletes, path,
				tt.format, nil, nil, nil, 1, 128)
			require.NoError(t, err)
			second := builder.EqualityFieldIDs(tt.fieldIDs).Build()
			tasks := []FileScanTask{
				{EqualityDeleteFiles: []iceberg.DataFile{first}},
				{EqualityDeleteFiles: []iceberg.DataFile{second}},
			}
			fs := &countingEqualityDeleteOpenFS{MemFS: iceio.NewMemFS()}
			loader, err := newLazyEqualityDeleteLoader(fs, schema, nil, nil, tasks)
			if tt.wantErr == nil {
				require.NoError(t, err)
				require.NotNil(t, loader)
				assert.Len(t, loader.files, 1)
				assert.Zero(t, fs.attempts.Load())

				return
			}

			require.ErrorIs(t, err, tt.wantErr)
			require.ErrorContains(t, err, path)
			_, err = readAllEqualityDeleteFiles(t.Context(), fs, schema, nil, tasks, 1)
			require.ErrorIs(t, err, tt.wantErr)
			require.ErrorContains(t, err, path)
			assert.Zero(t, fs.attempts.Load(), "reject conflicting metadata before I/O")
		})
	}
}
