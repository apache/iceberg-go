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
	"encoding/json"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/arrow-go/v18/parquet"
	"github.com/apache/arrow-go/v18/parquet/pqarrow"
	"github.com/apache/iceberg-go"
	"github.com/apache/iceberg-go/catalog"
	"github.com/apache/iceberg-go/catalog/hadoop"
	iceio "github.com/apache/iceberg-go/io"
	"github.com/apache/iceberg-go/table"
	"github.com/stretchr/testify/require"
)

func TestScanReadsIDLessFieldsAddedAfterImport(t *testing.T) {
	ctx := t.Context()
	cat, err := hadoop.NewCatalog(ctx, "local", t.TempDir(), nil)
	require.NoError(t, err)
	require.NoError(t, cat.CreateNamespace(ctx, catalog.ToIdentifier("db"), nil))

	schema := iceberg.NewSchema(0,
		iceberg.NestedField{ID: 1, Name: "id", Type: iceberg.PrimitiveTypes.Int64, Required: true},
		iceberg.NestedField{ID: 2, Name: "payload", Type: iceberg.NewStructType(
			iceberg.NestedField{ID: 3, Name: "known", Type: iceberg.PrimitiveTypes.String, Required: true},
		), Required: true},
	)
	mapping := iceberg.NameMapping{
		{FieldID: new(1), Names: []string{"id"}},
		{FieldID: new(2), Names: []string{"payload"}, Fields: []iceberg.MappedField{
			{FieldID: new(3), Names: []string{"known"}},
		}},
		{Names: []string{"external_top"}},
	}
	mappingJSON, err := json.Marshal(mapping)
	require.NoError(t, err)
	tbl, err := cat.CreateTable(ctx, catalog.ToIdentifier("db", "events"), schema,
		catalog.WithProperties(iceberg.Properties{
			table.DefaultNameMappingKey: string(mappingJSON),
		}))
	require.NoError(t, err)

	fileSchema := arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int64, Nullable: false},
		{Name: "payload", Type: arrow.StructOf(
			arrow.Field{Name: "known", Type: arrow.BinaryTypes.String, Nullable: false},
			arrow.Field{Name: "external_nested", Type: arrow.BinaryTypes.String, Nullable: true},
		), Nullable: false},
		{Name: "external_top", Type: arrow.BinaryTypes.String, Nullable: true},
	}, nil)
	data, err := array.TableFromJSON(memory.DefaultAllocator, fileSchema, []string{
		`[{"id":1,"payload":{"known":"known","external_nested":"nested"},"external_top":"top"}]`,
	})
	require.NoError(t, err)
	defer data.Release()

	filePath := tbl.Location() + "/data/idless.parquet"
	writer, err := (iceio.LocalFS{}).Create(ctx, filePath)
	require.NoError(t, err)
	require.NoError(t, pqarrow.WriteTable(data, writer, 1,
		parquet.NewWriterProperties(), pqarrow.DefaultWriterProps()))

	dataFileBuilder, err := iceberg.NewDataFileBuilder(
		*iceberg.UnpartitionedSpec, iceberg.EntryContentData, filePath, iceberg.ParquetFile,
		nil, nil, nil, 1, 1024,
	)
	require.NoError(t, err)
	tx := tbl.NewTransaction()
	require.NoError(t, tx.AddDataFiles(ctx,
		[]iceberg.DataFile{dataFileBuilder.Build()}, nil, table.WithoutAutoNameMapping()))
	tbl, err = tx.Commit(ctx)
	require.NoError(t, err)

	tx = tbl.NewTransaction()
	require.NoError(t, tx.UpdateSchema(true, false).
		AddColumn([]string{"payload", "external_nested"}, iceberg.PrimitiveTypes.String, "", false, nil).
		AddColumn([]string{"external_top"}, iceberg.PrimitiveTypes.String, "", false, nil).
		Commit())
	tbl, err = tx.Commit(ctx)
	require.NoError(t, err)

	result, err := tbl.Scan().ToArrowTable(ctx)
	require.NoError(t, err)
	defer result.Release()

	require.EqualValues(t, 1, result.NumRows())
	require.Equal(t, "external_top", result.Schema().Field(2).Name)
	require.Equal(t, "top", result.Column(2).Data().Chunk(0).(*array.String).Value(0))
	payload := result.Column(1).Data().Chunk(0).(*array.Struct)
	require.Equal(t, "nested", payload.Field(1).(*array.String).Value(0))
}
