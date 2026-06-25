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

	"github.com/apache/iceberg-go"
	iceio "github.com/apache/iceberg-go/io"
	"github.com/stretchr/testify/require"
)

func TestReadAllEqualityDeleteFilesRejectsEmptyEqualityFieldIDs(t *testing.T) {
	schema := iceberg.NewSchema(0,
		iceberg.NestedField{ID: 1, Name: "id", Type: iceberg.PrimitiveTypes.Int64, Required: true},
	)

	builder, err := iceberg.NewDataFileBuilder(
		*iceberg.UnpartitionedSpec, iceberg.EntryContentEqDeletes,
		"mem://default/table/delete/empty-equality-fields.parquet",
		iceberg.ParquetFile, nil, nil, nil, 1, 128,
	)
	require.NoError(t, err)
	deleteFile := builder.EqualityFieldIDs(nil).Build()

	_, err = readAllEqualityDeleteFiles(
		t.Context(),
		iceio.NewMemFS(),
		schema,
		[]FileScanTask{{EqualityDeleteFiles: []iceberg.DataFile{deleteFile}}},
		1,
	)
	require.ErrorIs(t, err, ErrEmptyEqualityFieldIDs)
	require.ErrorContains(t, err, "empty-equality-fields.parquet")
}
