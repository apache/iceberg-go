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
	"encoding/binary"
	"fmt"
	"maps"
	"strings"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/arrow-go/v18/parquet/compress"
	"github.com/apache/arrow-go/v18/parquet/file"
	"github.com/apache/arrow-go/v18/parquet/metadata"
	"github.com/apache/iceberg-go"
	"github.com/apache/iceberg-go/internal"
	iceio "github.com/apache/iceberg-go/io"
	tblutils "github.com/apache/iceberg-go/table/internal"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
)

func TestParquetBloomLogicalPathsOnDisk(t *testing.T) {
	schema := iceberg.NewSchema(0,
		iceberg.NestedField{ID: 1, Name: "items", Type: iceberg.NewListType(2, iceberg.PrimitiveTypes.Int32, false)},
		iceberg.NestedField{ID: 3, Name: "value space", Type: iceberg.PrimitiveTypes.Int32},
		iceberg.NestedField{ID: 4, Name: "lookup", Type: iceberg.NewMapType(5, iceberg.PrimitiveTypes.String, 6, iceberg.PrimitiveTypes.Int32, false)},
	)
	// Table creation assigns fresh IDs; derive the file schema from those IDs.
	initial, err := NewMetadata(schema, iceberg.UnpartitionedSpec, UnsortedSortOrder, "mem://bloom-paths", nil)
	require.NoError(t, err)
	schema = initial.CurrentSchema()
	fileSchema, err := iceberg.SanitizeColumnNames(schema)
	require.NoError(t, err)
	arrowSchema, err := SchemaToArrowSchema(fileSchema, nil, true, false)
	require.NoError(t, err)
	batch, _, err := array.RecordFromJSON(memory.DefaultAllocator, arrowSchema,
		strings.NewReader(fmt.Sprintf(`[{"items":[7,7],%q:7,"lookup":[{"key":"k","value":7}]}]`, fileSchema.Field(1).Name)))
	require.NoError(t, err)
	defer batch.Release()
	props := iceberg.Properties{}
	for _, name := range []string{"items.element", "value space", "lookup.value"} {
		props[ParquetBloomFilterColumnEnabledKeyPrefix+"."+name] = "true"
		props[ParquetBloomFilterColumnFPPKeyPrefix+"."+name] = "0.01"
		props[ParquetBloomFilterColumnNDVKeyPrefix+"."+name] = "1"
	}
	// Logical settings take precedence over the legacy physical spelling.
	props[ParquetBloomFilterColumnNDVKeyPrefix+".items.list.element"] = "100000"
	props[ParquetCompressionColumnCodecKeyPrefix+".items.element"] = "gzip"
	original := maps.Clone(props)
	for _, rolling := range []bool{false, true} {
		t.Run(fmt.Sprintf("rolling=%t", rolling), func(t *testing.T) {
			meta, err := NewMetadata(schema, iceberg.UnpartitionedSpec, UnsortedSortOrder, "mem://bloom-paths", props)
			require.NoError(t, err)
			builder, err := MetadataBuilderFromBase(meta, "")
			require.NoError(t, err)
			fs := iceio.NewMemFS()
			var df iceberg.DataFile
			if rolling {
				id := uuid.New()
				factory, err := newWriterFactory("mem://bloom-paths", recordWritingArgs{
					fs: fs, writeUUID: &id, counter: internal.Counter(0),
				}, builder, fileSchema, 1048576, withFactoryFileSchema(fileSchema))
				require.NoError(t, err)
				defer factory.stopCount()
				writer, err := factory.openFileWriter(t.Context(), "", nil, 0, 1, nil)
				require.NoError(t, err)
				require.NoError(t, writer.Write(batch))
				df, err = writer.Close()
				require.NoError(t, err)
			} else {
				writer, err := newDataFileWriter("mem://bloom-paths", fs, builder, props, withFileSchema(fileSchema))
				require.NoError(t, err)
				batch.Retain()
				df, err = writer.writeFile(t.Context(), nil, WriteTask{
					Uuid: uuid.New(), Schema: fileSchema, Batches: []arrow.RecordBatch{batch}, FileCount: 1,
				})
				require.NoError(t, err)
			}
			input, err := fs.Open(t.Context(), df.FilePath())
			require.NoError(t, err)
			reader, err := file.NewParquetReader(input)
			require.NoError(t, err)
			defer reader.Close()
			column, err := reader.MetaData().RowGroup(0).ColumnChunk(0)
			require.NoError(t, err)
			require.Equal(t, compress.Codecs.Gzip, column.Compression())
			blooms, err := reader.GetBloomFilterReader().RowGroup(0)
			require.NoError(t, err)
			for _, col := range []int{0, 1, 3} {
				visited := false
				require.NoError(t, blooms.VisitColumnBloomFilter(col, func(bf metadata.BloomFilter) error {
					visited = true
					require.NotNil(t, bf)
					require.EqualValues(t, 32, bf.Size())
					require.True(t, bf.CheckHash(bf.Hasher().Sum64(binary.LittleEndian.AppendUint32(nil, 7))))

					return nil
				}))
				require.True(t, visited, "column %d: %s", col, reader.MetaData().Schema.Column(col).Path())
			}
			require.Equal(t, original, props)
		})
	}
	mapping, err := tblutils.GetFileFormat(iceberg.ParquetFile).PathToIDMapping(fileSchema)
	require.NoError(t, err)
	for _, content := range []iceberg.ManifestEntryContent{iceberg.EntryContentEqDeletes, iceberg.EntryContentPosDeletes} {
		effective := tblutils.ResolveParquetBloomColumnPaths(props, schema, fileSchema, mapping, content)
		for key := range effective {
			require.False(t, strings.HasPrefix(key, ParquetBloomFilterColumnEnabledKeyPrefix+"."))
			require.False(t, strings.HasPrefix(key, ParquetBloomFilterColumnFPPKeyPrefix+"."))
			require.False(t, strings.HasPrefix(key, ParquetBloomFilterColumnNDVKeyPrefix+"."))
		}
	}
}

func TestParquetBloomPropertiesFollowSchemaChanges(t *testing.T) {
	for _, setting := range []struct{ prefix, value string }{
		{ParquetBloomFilterColumnEnabledKeyPrefix + ".", "false"},
		{ParquetBloomFilterColumnFPPKeyPrefix + ".", "0.01"},
		{ParquetBloomFilterColumnNDVKeyPrefix + ".", "1"},
	} {
		for _, tc := range []struct {
			name, before, after string
			change              func(*UpdateSchema) *UpdateSchema
		}{
			{"leaf rename", "name", "full_name", func(u *UpdateSchema) *UpdateSchema { return u.RenameColumn([]string{"name"}, "full_name") }},
			{"parent rename", "address.city", "location.city", func(u *UpdateSchema) *UpdateSchema { return u.RenameColumn([]string{"address"}, "location") }},
			{"list rename", "tags.element", "labels.element", func(u *UpdateSchema) *UpdateSchema { return u.RenameColumn([]string{"tags"}, "labels") }},
			{"map rename", "properties.value", "attributes.value", func(u *UpdateSchema) *UpdateSchema { return u.RenameColumn([]string{"properties"}, "attributes") }},
			{"parent delete", "address.city", "", func(u *UpdateSchema) *UpdateSchema { return u.DeleteColumn([]string{"address"}) }},
			{"delete and reuse", "name", "", func(u *UpdateSchema) *UpdateSchema {
				return u.DeleteColumn([]string{"name"}).AddColumn([]string{"name"}, iceberg.PrimitiveTypes.String, "", false, nil)
			}},
		} {
			t.Run(setting.prefix+tc.name, func(t *testing.T) {
				meta, err := NewMetadata(originalSchema, iceberg.UnpartitionedSpec, UnsortedSortOrder, "", iceberg.Properties{
					setting.prefix + tc.before: setting.value, "unrelated": "kept",
				})
				require.NoError(t, err)
				tx := New([]string{"table"}, meta, "", nil, nil).NewTransaction()
				require.NoError(t, tc.change(tx.UpdateSchema(true, true)).Commit())
				staged, err := tx.txnMeta()
				require.NoError(t, err)
				require.NotContains(t, staged.props, setting.prefix+tc.before)
				if tc.after != "" {
					require.Equal(t, setting.value, staged.props[setting.prefix+tc.after])
				}
				require.Equal(t, "kept", staged.props["unrelated"])
			})
		}
	}
}

func TestParquetColumnPropertiesMoveTogether(t *testing.T) {
	settings := []struct{ prefix, value string }{
		{MetricsModeColumnConfPrefix, "full"},
		{ParquetBloomFilterColumnEnabledKeyPrefix, "true"},
		{ParquetBloomFilterColumnFPPKeyPrefix, "0.01"},
		{ParquetBloomFilterColumnNDVKeyPrefix, "100"},
		{ParquetCompressionColumnCodecKeyPrefix, "gzip"},
		{ParquetCompressionColumnLevelKeyPrefix, "4"},
	}
	props := iceberg.Properties{}
	for _, setting := range settings {
		props[setting.prefix+".name"] = setting.value
	}
	meta, err := NewMetadata(originalSchema, iceberg.UnpartitionedSpec, UnsortedSortOrder, "", props)
	require.NoError(t, err)
	tx := New([]string{"table"}, meta, "", nil, nil).NewTransaction()
	require.NoError(t, tx.UpdateSchema(true, true).RenameColumn([]string{"name"}, "full_name").Commit())
	staged, err := tx.txnMeta()
	require.NoError(t, err)
	for _, setting := range settings {
		require.NotContains(t, staged.props, setting.prefix+".name")
		require.Equal(t, setting.value, staged.props[setting.prefix+".full_name"])
	}
}

func TestParquetBloomCanonicalNameShadowsPhysicalName(t *testing.T) {
	schema := iceberg.NewSchema(0,
		iceberg.NestedField{ID: 1, Name: "items", Type: iceberg.NewListType(2, iceberg.PrimitiveTypes.Int32, false)},
		iceberg.NestedField{ID: 3, Name: "items.list.element", Type: iceberg.PrimitiveTypes.Int32},
	)
	fileSchema, err := iceberg.SanitizeColumnNames(schema)
	require.NoError(t, err)
	mapping, err := tblutils.GetFileFormat(iceberg.ParquetFile).PathToIDMapping(fileSchema)
	require.NoError(t, err)
	prefix := ParquetBloomFilterColumnEnabledKeyPrefix + "."
	effective := tblutils.ResolveParquetBloomColumnPaths(iceberg.Properties{prefix + "items.list.element": "true"}, schema, fileSchema, mapping, iceberg.EntryContentData)
	require.NotContains(t, effective, prefix+"items.list.element")
	for path, id := range mapping {
		if id == 3 {
			require.Equal(t, "true", effective[prefix+path])
		}
	}
}

func TestParquetBloomResolvedAdaptiveValidation(t *testing.T) {
	enabled := ParquetBloomFilterColumnEnabledKeyPrefix + "."
	fpp := ParquetBloomFilterColumnFPPKeyPrefix + "."
	ndv := ParquetBloomFilterColumnNDVKeyPrefix + "."
	schema := iceberg.NewSchema(0, iceberg.NestedField{ID: 1, Name: "items", Type: iceberg.NewListType(2, iceberg.PrimitiveTypes.Int32, false)})
	for _, tc := range []struct {
		name    string
		props   iceberg.Properties
		invalid bool
	}{
		{"legacy enabled", iceberg.Properties{enabled + "items.list.element": "true", fpp + "items.element": "0.9999999999999999"}, true},
		{"legacy fpp", iceberg.Properties{enabled + "items.element": "true", fpp + "items.list.element": "0.9999999999999999"}, true},
		{"explicit ndv", iceberg.Properties{enabled + "items.element": "true", fpp + "items.element": "0.9999999999999999", ndv + "items.list.element": "1"}, false},
		{"missing field settings", iceberg.Properties{enabled + "items.element": "true", fpp + "missing": "invalid", ndv + "missing": "invalid"}, false},
		{"shadowed physical settings", iceberg.Properties{
			enabled + "items.element": "true", fpp + "items.element": "0.01", ndv + "items.element": "10",
			fpp + "items.list.element": "invalid", ndv + "items.list.element": "invalid",
		}, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			tc.props[ParquetBloomFilterAdaptiveEnabledKey] = "true"
			meta, err := NewMetadata(schema, iceberg.UnpartitionedSpec, UnsortedSortOrder, "mem://adaptive-validation", tc.props)
			require.NoError(t, err)
			builder, err := MetadataBuilderFromBase(meta, "")
			require.NoError(t, err)
			_, err = newDataFileWriter("mem://adaptive-validation", iceio.NewMemFS(), builder, tc.props)
			if tc.invalid {
				require.ErrorIs(t, err, iceberg.ErrInvalidArgument)
			} else {
				require.NoError(t, err)
			}
			id := uuid.New()
			factory, err := newWriterFactory("mem://adaptive-validation", recordWritingArgs{fs: iceio.NewMemFS(), writeUUID: &id, counter: internal.Counter(0)}, builder, schema, 1048576)
			if tc.invalid {
				require.ErrorIs(t, err, iceberg.ErrInvalidArgument)
			} else {
				require.NoError(t, err)
				factory.stopCount()
			}
		})
	}
}
