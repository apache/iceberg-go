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
	"path/filepath"
	"strings"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/compute"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/iceberg-go"
	icebergio "github.com/apache/iceberg-go/io"
	"github.com/stretchr/testify/require"
)

func TestWriteRecordsRecoversExactProjJSONCRSOnRead(t *testing.T) {
	const projjson = `{"type":"GeographicCRS","name":"Custom WGS 84"}`

	mem := memory.NewCheckedAllocator(memory.DefaultAllocator)
	defer mem.AssertSize(t, 0)
	ctx := compute.WithAllocator(context.Background(), mem)

	geom, err := iceberg.GeometryTypeOf("projjson:geo.crs")
	require.NoError(t, err)

	iceSchema := iceberg.NewSchema(1,
		iceberg.NestedField{ID: 1, Name: "geom", Type: geom, Required: false},
	)
	spec := iceberg.NewPartitionSpec()
	loc := filepath.ToSlash(t.TempDir())
	meta, err := NewMetadata(iceSchema, &spec, UnsortedSortOrder, loc, iceberg.Properties{
		PropertyFormatVersion: "3",
		"geo.crs":             projjson,
	})
	require.NoError(t, err)

	tbl := New(
		Identifier{"test", "projjson_exact_round_trip"},
		meta,
		filepath.Join(loc, "metadata", "v1.metadata.json"),
		func(context.Context) (icebergio.IO, error) { return icebergio.LocalFS{}, nil },
		nil,
	)

	arrowSchema, err := SchemaToArrowSchemaWithOptions(iceSchema, ArrowSchemaOptions{
		IncludeFieldIDs: true,
		TableProperties: meta.Properties(),
	})
	require.NoError(t, err)

	records := func(yield func(arrow.RecordBatch, error) bool) {
		rec, _, err := array.RecordFromJSON(mem, arrowSchema, strings.NewReader(`[
			{"geom": "01010000000000000000003e400000000000002440"}
		]`))
		require.NoError(t, err)
		yield(rec, nil)
	}

	var dataFiles []iceberg.DataFile
	for df, err := range WriteRecords(ctx, tbl, arrowSchema, records) {
		require.NoError(t, err)
		dataFiles = append(dataFiles, df)
	}
	require.Len(t, dataFiles, 1)

	fs, err := tbl.fsF(ctx)
	require.NoError(t, err)

	readSchema, _, rdr, err := (&arrowScan{
		metadata:        tbl.Metadata(),
		fs:              fs,
		projectedSchema: tbl.Schema(),
		concurrency:     1,
		nameMapping:     tbl.Metadata().NameMapping(),
	}).prepareToRead(ctx, dataFiles[0])
	require.NoError(t, err)
	defer rdr.Close()

	readField, ok := readSchema.FindFieldByID(1)
	require.True(t, ok)
	readGeom, ok := readField.Type.(iceberg.GeometryType)
	require.True(t, ok)
	require.Equal(t, "projjson:geo.crs", readGeom.CRS())
	require.True(t, iceSchema.Equals(readSchema), "expected %s, got %s", iceSchema, readSchema)
}
