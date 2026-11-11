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
	"fmt"
	"maps"
	"strings"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/compute"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/arrow-go/v18/parquet"
	"github.com/apache/arrow-go/v18/parquet/compress"
	"github.com/apache/arrow-go/v18/parquet/file"
	"github.com/apache/iceberg-go"
	"github.com/apache/iceberg-go/internal"
	iceio "github.com/apache/iceberg-go/io"
	tblutils "github.com/apache/iceberg-go/table/internal"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func deleteParquetTestMetadata(t *testing.T, props iceberg.Properties) *MetadataBuilder {
	t.Helper()
	sc := iceberg.NewSchema(0, iceberg.NestedField{ID: 1, Name: "value", Type: iceberg.PrimitiveTypes.String})
	meta, err := NewMetadata(sc, iceberg.UnpartitionedSpec, UnsortedSortOrder, "mem://delete-settings", props)
	require.NoError(t, err)
	builder, err := MetadataBuilderFromBase(meta, "")
	require.NoError(t, err)

	return builder
}

func TestDeleteParquetSettingsOnDisk(t *testing.T) {
	for _, content := range []iceberg.ManifestEntryContent{
		iceberg.EntryContentData, iceberg.EntryContentEqDeletes, iceberg.EntryContentPosDeletes,
	} {
		for _, rolling := range []bool{false, true} {
			t.Run(fmt.Sprintf("content=%d/rolling=%t", content, rolling), func(t *testing.T) {
				mem := memory.NewCheckedAllocator(memory.DefaultAllocator)
				defer mem.AssertSize(t, 0)
				ctx := compute.WithAllocator(t.Context(), mem)
				props := iceberg.Properties{
					ParquetCompressionKey: "snappy", ParquetCompressionLevelKey: "1",
					ParquetRowGroupSizeBytesKey: "1048576", ParquetPageSizeBytesKey: "1048576",
					ParquetPageRowLimitKey: "20000", ParquetDictSizeBytesKey: "2097152",
					ParquetPageVersionKey: "2", tblutils.ParquetDictEnabledKey: "false",
					DeleteParquetCompressionKey: "gzip", DeleteParquetCompressionLevelKey: "9",
					DeleteParquetRowGroupSizeBytesKey: "1", DeleteParquetPageSizeBytesKey: "64",
					DeleteParquetPageRowLimitKey: "1", DeleteParquetDictSizeBytesKey: "64",
					DeleteParquetPageVersionKey: "1", DefaultWriteMetricsModeKey: "none",
					ParquetBloomFilterColumnEnabledKeyPrefix + ".value":     "true",
					ParquetBloomFilterColumnEnabledKeyPrefix + ".file_path": "true",
				}
				original := maps.Clone(props)
				meta := deleteParquetTestMetadata(t, props)
				fileSchema := meta.CurrentSchema()
				var equalityIDs []int
				if content == iceberg.EntryContentEqDeletes {
					equalityIDs = []int{1}
				}
				if content == iceberg.EntryContentPosDeletes {
					fileSchema = iceberg.PositionalDeleteSchema
				}
				arrowSchema, err := SchemaToArrowSchema(fileSchema, nil, true, false)
				require.NoError(t, err)
				var batches []arrow.RecordBatch
				for i := range 2 {
					jsonData := fmt.Sprintf(`[{"value":%q}]`, strings.Repeat(fmt.Sprintf("value-%d", i), 256))
					if content == iceberg.EntryContentPosDeletes {
						jsonData = fmt.Sprintf(`[{"file_path":%q,"pos":%d}]`, "s3://bucket/"+strings.Repeat("a", 1024)+".parquet", i)
					}
					rec, _, err := array.RecordFromJSON(mem, arrowSchema, strings.NewReader(jsonData))
					require.NoError(t, err)
					defer rec.Release()
					batches = append(batches, rec)
				}
				fs := iceio.NewMemFS()
				var df iceberg.DataFile
				var writerProps []parquet.WriterProperty
				var rowGroupBytes int64
				if rolling {
					id := uuid.New()
					factory, err := newWriterFactory("mem://delete-settings", recordWritingArgs{
						fs: fs, writeUUID: &id, counter: internal.Counter(0),
					}, meta, fileSchema, 1048576,
						withContentType(content), withFactoryFileSchema(fileSchema), withFactoryEqualityFieldIDs(equalityIDs))
					require.NoError(t, err)
					defer factory.stopCount()
					writerProps = factory.writeProps.([]parquet.WriterProperty)
					rowGroupBytes = factory.rowGroupBytes
					writer, err := factory.openFileWriter(ctx, "", nil, 0, 1, nil)
					require.NoError(t, err)
					for _, rec := range batches {
						require.NoError(t, writer.Write(rec))
					}
					df, err = writer.Close()
					require.NoError(t, err)
				} else {
					writer, err := newDataFileWriter("mem://delete-settings", fs, meta, props,
						withContent(content), withFileSchema(fileSchema), withEqualityFieldIDs(equalityIDs))
					require.NoError(t, err)
					writerProps = writer.invariants.writeProps.([]parquet.WriterProperty)
					rowGroupBytes = writer.invariants.rowGroupBytes
					for _, rec := range batches {
						rec.Retain()
					}
					df, err = writer.writeFile(ctx, nil, WriteTask{
						Uuid: uuid.New(), Schema: fileSchema, Batches: batches, FileCount: 1,
					})
					require.NoError(t, err)
				}
				wp := parquet.NewWriterProperties(writerProps...)
				wantCodec, wantPage, wantGroups := compress.Codecs.Gzip, file.PageTypeDataPage, 2
				if content == iceberg.EntryContentData {
					wantCodec, wantPage, wantGroups = compress.Codecs.Snappy, file.PageTypeDataPageV2, 1
					assert.EqualValues(t, 1048576, wp.DataPageSize())
					assert.EqualValues(t, 2097152, wp.DictionaryPageSizeLimit())
					assert.EqualValues(t, 20000, wp.WriteBatchSize())
					assert.Equal(t, 1, wp.CompressionLevel())
					assert.EqualValues(t, 1048576, rowGroupBytes)
				} else {
					assert.EqualValues(t, 64, wp.DataPageSize())
					assert.EqualValues(t, 64, wp.DictionaryPageSizeLimit())
					assert.EqualValues(t, 1, wp.WriteBatchSize())
					assert.Equal(t, 9, wp.CompressionLevel())
					assert.EqualValues(t, 1, rowGroupBytes)
				}
				assert.Equal(t, original, props, "delete overrides must not mutate shared table properties")
				assert.Equal(t, content, df.ContentType())
				assert.EqualValues(t, 2, df.Count())
				if content == iceberg.EntryContentPosDeletes {
					assert.Len(t, df.LowerBoundValues(), 2, "position-delete bounds ignore the data metrics mode")
				} else {
					assert.Empty(t, df.LowerBoundValues(), "data and equality deletes retain the table metrics mode")
				}
				input, err := fs.Open(ctx, df.FilePath())
				require.NoError(t, err)
				reader, err := file.NewParquetReader(input)
				require.NoError(t, err)
				defer reader.Close()
				require.Equal(t, wantGroups, reader.MetaData().NumRowGroups())
				for rg := range reader.MetaData().NumRowGroups() {
					for col := range reader.MetaData().Schema.NumColumns() {
						column, err := reader.MetaData().RowGroup(rg).ColumnChunk(col)
						require.NoError(t, err)
						assert.Equal(t, wantCodec, column.Compression())
						if content != iceberg.EntryContentData {
							assert.Zero(t, column.BloomFilterOffset())
						}
						pages, err := reader.RowGroup(rg).GetColumnPageReader(col)
						require.NoError(t, err)
						found := false
						for pages.Next() {
							if pages.Page().Type() == file.PageTypeDictionaryPage {
								continue
							}
							assert.Equal(t, wantPage, pages.Page().Type())
							found = true
						}
						require.NoError(t, pages.Err())
						require.True(t, found)
					}
				}
				read, err := tblutils.GetFileFormat(iceberg.ParquetFile).Open(ctx, fs, df.FilePath())
				require.NoError(t, err)
				defer read.Close()
				actual, err := read.ReadTable(ctx)
				require.NoError(t, err)
				defer actual.Release()
				expected := array.NewTableFromRecords(arrowSchema, batches)
				defer expected.Release()
				assert.True(t, array.TableEqual(expected, actual))
			})
		}
	}
}

func TestDeleteParquetSettingsInheritance(t *testing.T) {
	props := iceberg.Properties{
		ParquetCompressionKey: "gzip", ParquetCompressionLevelKey: "3",
		ParquetRowGroupSizeBytesKey: "4096", ParquetPageSizeBytesKey: "256",
		ParquetPageRowLimitKey: "7", ParquetDictSizeBytesKey: "512", ParquetPageVersionKey: "1",
		ParquetBloomFilterColumnEnabledKeyPrefix + ".value":           "true",
		tblutils.ParquetDictEncodingColumnEnabledKeyPrefix + ".value": "false",
	}
	for _, content := range []iceberg.ManifestEntryContent{iceberg.EntryContentEqDeletes, iceberg.EntryContentPosDeletes} {
		t.Run(fmt.Sprint(content), func(t *testing.T) {
			effective, err := parquetPropsFor(content, props)
			require.NoError(t, err)
			wp := parquet.NewWriterProperties(tblutils.GetFileFormat(iceberg.ParquetFile).GetWriteProperties(effective).([]parquet.WriterProperty)...)
			assert.Equal(t, compress.Codecs.Gzip, wp.Compression())
			assert.Equal(t, 3, wp.CompressionLevel())
			assert.EqualValues(t, 256, wp.DataPageSize())
			assert.EqualValues(t, 7, wp.WriteBatchSize())
			assert.EqualValues(t, 512, wp.DictionaryPageSizeLimit())
			assert.Equal(t, parquet.DataPageV1, wp.DataPageVersion())
			size, err := tblutils.ParquetRowGroupTargetSizeBytes(effective)
			require.NoError(t, err)
			assert.EqualValues(t, 4096, size)
			assert.False(t, wp.BloomFilterEnabledFor("value"))
			assert.True(t, wp.DictionaryEnabledFor("value"), "data-column dictionary overrides do not apply to deletes")
		})
	}
	effective, err := parquetPropsFor(iceberg.EntryContentData, props)
	require.NoError(t, err)
	assert.Equal(t, props, effective)
}

func TestDeleteParquetSettingsValidation(t *testing.T) {
	for _, tt := range []struct{ key, value string }{
		{DeleteParquetRowGroupSizeBytesKey, "0"},
		{DeleteParquetRowGroupSizeBytesKey, "bad"},
		{DeleteParquetPageSizeBytesKey, "-1"},
		{DeleteParquetPageSizeBytesKey, ""},
		{DeleteParquetPageRowLimitKey, "0"},
		{DeleteParquetDictSizeBytesKey, "0"},
		{DeleteParquetCompressionLevelKey, "bad"},
		{DeleteParquetCompressionLevelKey, "10"},
	} {
		t.Run(tt.key+"="+tt.value, func(t *testing.T) {
			props := iceberg.Properties{DeleteParquetCompressionKey: "gzip", tt.key: tt.value}
			meta := deleteParquetTestMetadata(t, props)
			fs := iceio.NewMemFS()
			_, err := newDataFileWriter("mem://delete-settings", fs, meta, props, withContent(iceberg.EntryContentEqDeletes))
			require.ErrorIs(t, err, iceberg.ErrInvalidArgument)
			require.ErrorContains(t, err, tt.key)
			id := uuid.New()
			_, err = newWriterFactory("mem://delete-settings", recordWritingArgs{
				fs: fs, writeUUID: &id, counter: internal.Counter(0),
			}, meta, meta.CurrentSchema(), 1048576, withContentType(iceberg.EntryContentEqDeletes))
			require.ErrorIs(t, err, iceberg.ErrInvalidArgument)
			require.ErrorContains(t, err, tt.key)
			// An invalid delete override must not prevent a regular data write.
			_, err = newDataFileWriter("mem://delete-settings", fs, meta, props)
			require.NoError(t, err)
		})
	}
}
