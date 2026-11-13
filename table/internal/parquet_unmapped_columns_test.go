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

package internal_test

import (
	"bytes"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/arrow-go/v18/parquet"
	"github.com/apache/arrow-go/v18/parquet/file"
	"github.com/apache/arrow-go/v18/parquet/pqarrow"
	"github.com/apache/arrow-go/v18/parquet/schema"
	"github.com/apache/iceberg-go"
	"github.com/apache/iceberg-go/table/internal"
	"github.com/stretchr/testify/require"
)

// A Parquet file may contain imported columns that have neither an embedded
// field ID nor an entry in the table's name mapping. Such columns must not
// prevent the reader from projecting known Iceberg columns.
func TestPruneParquetSkipsUnmappedIDLessColumns(t *testing.T) {
	for _, tt := range []struct {
		name   string
		nested bool
	}{
		{name: "top-level column"},
		{name: "nested struct column", nested: true},
	} {
		t.Run(tt.name, func(t *testing.T) {
			reader := openParquetWithUnmappedColumn(t, tt.nested)
			defer reader.Close()

			knownID := 2
			mapping := iceberg.NameMapping{
				{Names: []string{"known"}, FieldID: &knownID},
			}
			if tt.nested {
				payloadID := 1
				mapping = iceberg.NameMapping{{
					Names:   []string{"payload"},
					FieldID: &payloadID,
					Fields:  []iceberg.MappedField{{Names: []string{"known"}, FieldID: &knownID}},
				}}
			}

			projected, indices, err := reader.PrunedSchema(
				map[int]struct{}{knownID: {}}, mapping)
			require.NoError(t, err)
			require.Equal(t, []int{0}, indices)
			require.Equal(t, 1, projected.NumFields())

			if tt.nested {
				require.Equal(t, "payload", projected.Field(0).Name)
				st, ok := projected.Field(0).Type.(*arrow.StructType)
				require.True(t, ok)
				require.Len(t, st.Fields(), 1)
				require.Equal(t, "known", st.Fields()[0].Name)
			} else {
				require.Equal(t, "known", projected.Field(0).Name)
			}
		})
	}

	// Keep the existing error when the file has ID-less columns but the table
	// does not provide any name mapping.
	t.Run("no name mapping", func(t *testing.T) {
		reader := openParquetWithUnmappedColumn(t, false)
		defer reader.Close()

		_, _, err := reader.PrunedSchema(map[int]struct{}{2: {}}, nil)
		require.ErrorContains(t, err, "missing field_id")
	})
}

func openParquetWithUnmappedColumn(t *testing.T, nested bool) internal.FileReader {
	t.Helper()

	leafFields := schema.FieldList{
		schema.NewInt32Node("known", parquet.Repetitions.Required, 2),
		schema.NewInt32Node("external", parquet.Repetitions.Required, -1),
	}
	fields := leafFields
	if nested {
		payload, err := schema.NewGroupNode("payload", parquet.Repetitions.Required, leafFields, 1)
		require.NoError(t, err)
		fields = schema.FieldList{payload}
	}

	root, err := schema.NewGroupNode("schema", parquet.Repetitions.Required, fields, -1)
	require.NoError(t, err)

	var buf bytes.Buffer
	writer := file.NewParquetWriter(&buf, root)
	rowGroup, err := writer.AppendRowGroupChecked()
	require.NoError(t, err)
	for _, value := range []int32{7, 99} {
		column, err := rowGroup.NextColumn()
		require.NoError(t, err)

		_, err = column.(*file.Int32ColumnChunkWriter).WriteBatch([]int32{value}, nil, nil)
		require.NoError(t, err)
		require.NoError(t, column.Close())
	}
	require.NoError(t, rowGroup.Close())
	require.NoError(t, writer.Close())

	parquetReader, err := file.NewParquetReader(bytes.NewReader(buf.Bytes()))
	require.NoError(t, err)

	arrowReader, err := pqarrow.NewFileReader(
		parquetReader, pqarrow.ArrowReadProperties{}, memory.DefaultAllocator)
	require.NoError(t, err)

	return internal.WrapParquetFileReader(arrowReader)
}
