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

package compaction

import (
	"testing"

	"github.com/apache/iceberg-go"
	"github.com/stretchr/testify/require"
)

func TestReferencedDataFilePath(t *testing.T) {
	filePathID := filePathFieldID

	t.Run("explicit referenced_data_file", func(t *testing.T) {
		df := newPosDelete(t, "del-a.parquet").ReferencedDataFile("data-a.parquet").Build()
		require.Equal(t, "data-a.parquet", referencedDataFilePath(df))
	})

	t.Run("empty referenced_data_file falls through to bounds", func(t *testing.T) {
		df := newPosDelete(t, "del-a.parquet").ReferencedDataFile("").Build()
		require.Equal(t, "", referencedDataFilePath(df))
	})

	t.Run("equal file_path bounds resolve the single target", func(t *testing.T) {
		df := newPosDelete(t, "del-a.parquet").
			LowerBoundValues(map[int][]byte{filePathID: []byte("data-a.parquet")}).
			UpperBoundValues(map[int][]byte{filePathID: []byte("data-a.parquet")}).
			Build()
		require.Equal(t, "data-a.parquet", referencedDataFilePath(df))
	})

	t.Run("unequal file_path bounds are partition-scoped", func(t *testing.T) {
		df := newPosDelete(t, "del-a.parquet").
			LowerBoundValues(map[int][]byte{filePathID: []byte("data-a.parquet")}).
			UpperBoundValues(map[int][]byte{filePathID: []byte("data-b.parquet")}).
			Build()
		require.Equal(t, "", referencedDataFilePath(df))
	})

	t.Run("no bounds and no ref is partition-scoped", func(t *testing.T) {
		df := newPosDelete(t, "del-a.parquet").Build()
		require.Equal(t, "", referencedDataFilePath(df))
	})
}

func TestIsFileScoped(t *testing.T) {
	t.Run("non-empty referenced_data_file", func(t *testing.T) {
		df := newPosDelete(t, "del-a.parquet").ReferencedDataFile("data-a.parquet").Build()
		require.True(t, isFileScoped(df))
	})

	t.Run("empty referenced_data_file without bounds is not file-scoped", func(t *testing.T) {
		df := newPosDelete(t, "del-a.parquet").ReferencedDataFile("").Build()
		require.False(t, isFileScoped(df))
	})

	t.Run("equality delete is never file-scoped", func(t *testing.T) {
		b, err := iceberg.NewDataFileBuilder(
			*iceberg.UnpartitionedSpec, iceberg.EntryContentEqDeletes,
			"eq-del.parquet", iceberg.ParquetFile, nil, nil, nil, 1, 128)
		require.NoError(t, err)
		require.False(t, isFileScoped(b.Build()))
	})
}

func newPosDelete(t *testing.T, path string) *iceberg.DataFileBuilder {
	t.Helper()

	b, err := iceberg.NewDataFileBuilder(
		*iceberg.UnpartitionedSpec, iceberg.EntryContentPosDeletes,
		path, iceberg.ParquetFile, nil, nil, nil, 1, 128)
	require.NoError(t, err)

	return b
}
