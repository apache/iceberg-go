// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package table

import (
	"bytes"
	"context"
	"encoding/binary"
	"os"
	"strconv"
	"testing"
	"time"

	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/extensions"
	"github.com/apache/iceberg-go"
	iceio "github.com/apache/iceberg-go/io"
	"github.com/scritchley/orc"
	orcpb "github.com/scritchley/orc/proto"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/protoadapt"
)

func orcTestAttr(key, value string) []byte {
	var inner []byte
	inner = binary.AppendUvarint(inner, 10)
	inner = binary.AppendUvarint(inner, uint64(len(key)))
	inner = append(inner, key...)
	inner = binary.AppendUvarint(inner, 18)
	inner = binary.AppendUvarint(inner, uint64(len(value)))
	inner = append(inner, value...)
	result := binary.AppendUvarint(nil, 58)
	result = binary.AppendUvarint(result, uint64(len(inner)))

	return append(result, inner...)
}

func orcScanFixture(t *testing.T, fs *iceio.MemFS, schema string, rows [][]any) iceberg.DataFile {
	t.Helper()
	sc, err := orc.ParseSchema(schema)
	require.NoError(t, err)
	var data bytes.Buffer
	writer, err := orc.NewWriter(&data, orc.SetSchema(sc), orc.SetCompression(orc.CompressionNone{}))
	require.NoError(t, err)
	for i, row := range rows {
		require.NoError(t, writer.Write(row...))
		if i == 1 && len(rows) > 2 {
			require.NoError(t, writer.Flush())
		}
	}
	require.NoError(t, writer.Close())
	raw := data.Bytes()
	psLen := int(raw[len(raw)-1])
	psStart := len(raw) - 1 - psLen
	var postscript orcpb.PostScript
	require.NoError(t, proto.Unmarshal(raw[psStart:len(raw)-1], protoadapt.MessageV2Of(&postscript)))
	footerStart := psStart - int(postscript.GetFooterLength())
	var footer orcpb.Footer
	require.NoError(t, proto.Unmarshal(raw[footerStart:psStart], protoadapt.MessageV2Of(&footer)))
	if len(rows) > 2 {
		require.GreaterOrEqual(t, len(footer.Stripes), 2)
	}
	for i := 1; i < len(footer.Types); i++ {
		footer.Types[i].XXX_unrecognized = append(footer.Types[i].XXX_unrecognized, orcTestAttr("iceberg.id", strconv.Itoa(i))...)
	}
	encodedFooter, err := proto.Marshal(protoadapt.MessageV2Of(&footer))
	require.NoError(t, err)
	postscript.FooterLength = new(uint64(len(encodedFooter)))
	encodedPostscript, err := proto.Marshal(protoadapt.MessageV2Of(&postscript))
	require.NoError(t, err)
	require.LessOrEqual(t, len(encodedPostscript), 255)
	output := append([]byte{}, raw[:footerStart]...)
	output = append(output, encodedFooter...)
	output = append(output, encodedPostscript...)
	output = append(output, byte(len(encodedPostscript)))
	const path = "mem://orc/data.orc"
	require.NoError(t, fs.WriteFile(t.Context(), path, output))
	b, err := iceberg.NewDataFileBuilder(*iceberg.UnpartitionedSpec, iceberg.EntryContentData, path, iceberg.OrcFile, nil, nil, nil, int64(len(rows)), int64(len(output)))
	require.NoError(t, err)

	return b.Build()
}

func TestORCScannerMultipleStripesFiltersAndDeletes(t *testing.T) {
	fs := iceio.NewMemFS()
	file := orcScanFixture(t, fs, "struct<old_id:bigint,category:string>", [][]any{{int64(1), "keep"}, {int64(2), "keep"}, {int64(3), "drop"}, {int64(4), "keep"}})
	schema := iceberg.NewSchema(0,
		iceberg.NestedField{ID: 1, Name: "id", Type: iceberg.PrimitiveTypes.Int64},
		iceberg.NestedField{ID: 2, Name: "category", Type: iceberg.PrimitiveTypes.String},
	)
	projection := iceberg.NewSchema(0, iceberg.NestedField{ID: 1, Name: "id", Type: iceberg.PrimitiveTypes.Int64})
	metadata, err := NewMetadata(schema, iceberg.UnpartitionedSpec, UnsortedSortOrder, "mem://orc/table", iceberg.Properties{PropertyFormatVersion: "2"})
	require.NoError(t, err)
	filter := iceberg.EqualTo(iceberg.Reference("category"), "keep")
	bound, err := iceberg.BindExpr(schema, filter, true)
	require.NoError(t, err)
	const deletePath = "mem://orc/delete.parquet"
	writePosDeleteParquetToMemFS(t, fs, deletePath, `[{"file_path":"mem://orc/data.orc","pos":1}]`)
	del := newPosDeleteFile(t, deletePath, 1, 128)
	scan := &arrowScan{fs: fs, metadata: metadata, scanSchema: schema, projectedSchema: projection, filterSchema: schema, boundRowFilter: bound, caseSensitive: true, rowLimit: ScanNoLimit, concurrency: 1}
	task := FileScanTask{File: file, DeleteFiles: []iceberg.DataFile{del}, Residual: iceberg.NewAnd(filter, iceberg.GreaterThan(iceberg.Reference("id"), int64(1)))}
	_, records, err := scan.GetRecords(context.Background(), []FileScanTask{task})
	require.NoError(t, err)
	var ids []int64
	for record, err := range records {
		require.NoError(t, err)
		values := record.Column(0).(*array.Int64)
		for i := range values.Len() {
			ids = append(ids, values.Value(i))
		}
		record.Release()
	}
	require.Equal(t, []int64{4}, ids)
}

func TestORCScannerRejectsNullableDates(t *testing.T) {
	fs := iceio.NewMemFS()
	file := orcScanFixture(t, fs, "struct<d:date>", [][]any{{time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)}, {time.Date(2024, 1, 2, 0, 0, 0, 0, time.UTC)}, {nil}})
	schema := iceberg.NewSchema(0, iceberg.NestedField{ID: 1, Name: "d", Type: iceberg.PrimitiveTypes.Date})
	metadata, err := NewMetadata(schema, iceberg.UnpartitionedSpec, UnsortedSortOrder, "mem://orc/table", nil)
	require.NoError(t, err)
	scan := &arrowScan{fs: fs, metadata: metadata, scanSchema: schema, projectedSchema: schema, filterSchema: schema, boundRowFilter: iceberg.AlwaysTrue{}, caseSensitive: true, rowLimit: ScanNoLimit, concurrency: 1}
	_, records, err := scan.GetRecords(context.Background(), []FileScanTask{{File: file}})
	require.NoError(t, err)
	var scanErr error
	for record, err := range records {
		if record != nil {
			record.Release()
		}
		if err != nil {
			scanErr = err
		}
	}
	require.ErrorIs(t, scanErr, iceberg.ErrNotImplemented)
}

func TestORCScannerUUIDProjectionAndFilter(t *testing.T) {
	raw, err := os.ReadFile("internal/testdata/orc/uuid.orc")
	require.NoError(t, err)
	psLen := int(raw[len(raw)-1])
	psStart := len(raw) - 1 - psLen
	var ps orcpb.PostScript
	require.NoError(t, proto.Unmarshal(raw[psStart:len(raw)-1], protoadapt.MessageV2Of(&ps)))
	footerStart := psStart - int(ps.GetFooterLength())
	var footer orcpb.Footer
	require.NoError(t, proto.Unmarshal(raw[footerStart:psStart], protoadapt.MessageV2Of(&footer)))
	footer.Types[1].XXX_unrecognized = append(footer.Types[1].XXX_unrecognized, orcTestAttr("iceberg.id", "7")...)
	footer.Types[1].XXX_unrecognized = append(footer.Types[1].XXX_unrecognized, orcTestAttr("iceberg.binary-type", "UUID")...)
	encoded, err := proto.Marshal(protoadapt.MessageV2Of(&footer))
	require.NoError(t, err)
	ps.FooterLength = new(uint64(len(encoded)))
	encodedPS, err := proto.Marshal(protoadapt.MessageV2Of(&ps))
	require.NoError(t, err)
	output := append([]byte{}, raw[:footerStart]...)
	output = append(output, encoded...)
	output = append(output, encodedPS...)
	output = append(output, byte(len(encodedPS)))
	fs := iceio.NewMemFS()
	const path = "mem://orc/uuid.orc"
	require.NoError(t, fs.WriteFile(t.Context(), path, output))
	file, err := iceberg.NewDataFileBuilder(*iceberg.UnpartitionedSpec, iceberg.EntryContentData, path, iceberg.OrcFile, nil, nil, nil, 3, int64(len(output)))
	require.NoError(t, err)
	schema := iceberg.NewSchema(0, iceberg.NestedField{ID: 7, Name: "renamed", Type: iceberg.UUIDType{}})
	meta, err := NewMetadata(schema, iceberg.UnpartitionedSpec, UnsortedSortOrder, "mem://orc/table", nil)
	require.NoError(t, err)
	filter, err := iceberg.BindExpr(schema, iceberg.NotNull(iceberg.Reference("renamed")), true)
	require.NoError(t, err)
	scan := &arrowScan{fs: fs, metadata: meta, scanSchema: schema, projectedSchema: schema, filterSchema: schema, boundRowFilter: filter, caseSensitive: true, rowLimit: ScanNoLimit, concurrency: 1}
	_, records, err := scan.GetRecords(t.Context(), []FileScanTask{{File: file.Build(), Residual: iceberg.NotNull(iceberg.Reference("renamed"))}})
	require.NoError(t, err)
	var values [][]byte
	for batch, err := range records {
		require.NoError(t, err)
		col := batch.Column(0).(*extensions.UUIDArray)
		for i := range col.Len() {
			values = append(values, append([]byte(nil), col.Storage().(*array.FixedSizeBinary).Value(i)...))
		}
		batch.Release()
	}
	require.Equal(t, [][]byte{{0, 17, 34, 51, 68, 85, 102, 119, 136, 153, 170, 187, 204, 221, 238, 255}, bytes.Repeat([]byte{255}, 16)}, values)
}
