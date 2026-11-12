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
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package table_test

import (
	"context"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/iceberg-go"
	"github.com/apache/iceberg-go/catalog"
	"github.com/apache/iceberg-go/catalog/hadoop"
	"github.com/apache/iceberg-go/table"
	"github.com/stretchr/testify/require"
)

func TestEmptyStringIdentityPartitionScans(t *testing.T) {
	ctx := context.Background()
	cat, err := hadoop.NewCatalog(ctx, "local", t.TempDir(), nil)
	require.NoError(t, err)
	require.NoError(t, cat.CreateNamespace(ctx, catalog.ToIdentifier("db"), nil))

	schema := iceberg.NewSchema(0,
		iceberg.NestedField{ID: 1, Name: "id", Type: iceberg.PrimitiveTypes.Int64, Required: true},
		iceberg.NestedField{ID: 2, Name: "region", Type: iceberg.PrimitiveTypes.String},
	)
	spec, err := iceberg.NewPartitionSpecOpts(
		iceberg.AddPartitionFieldByName("region", "region", iceberg.IdentityTransform{}, schema, nil))
	require.NoError(t, err)
	tbl, err := cat.CreateTable(ctx, catalog.ToIdentifier("db", "events"), schema,
		catalog.WithPartitionSpec(&spec))
	require.NoError(t, err)

	arrowSchema, err := table.SchemaToArrowSchema(schema, nil, false, false)
	require.NoError(t, err)
	builder := array.NewRecordBuilder(memory.DefaultAllocator, arrowSchema)
	defer builder.Release()
	builder.Field(0).(*array.Int64Builder).AppendValues([]int64{1, 2}, nil)
	builder.Field(1).(*array.StringBuilder).AppendValues([]string{"", "us"}, nil)

	record := builder.NewRecordBatch()
	defer record.Release()
	reader, err := array.NewRecordReader(arrowSchema, []arrow.RecordBatch{record})
	require.NoError(t, err)
	defer reader.Release()
	tbl, err = tbl.Append(ctx, reader, nil)
	require.NoError(t, err)

	for _, tc := range []struct {
		name   string
		region string
		id     int64
	}{
		{name: "empty string", region: "", id: 1},
		{name: "us", region: "us", id: 2},
	} {
		t.Run(tc.name, func(t *testing.T) {
			result, err := tbl.Scan(
				table.WithRowFilter(iceberg.EqualTo(iceberg.Reference("region"), tc.region)),
				table.WithSelectedFields("id"),
			).ToArrowTable(ctx)
			require.NoError(t, err)
			defer result.Release()

			require.EqualValues(t, 1, result.NumRows())
			require.Equal(t, tc.id, result.Column(0).Data().Chunk(0).(*array.Int64).Value(0))
		})
	}
}
