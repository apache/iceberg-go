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
	"maps"
	"testing"

	"github.com/apache/arrow-go/v18/parquet"
	"github.com/apache/iceberg-go"
	"github.com/apache/iceberg-go/table/internal"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestGetWritePropertiesColumnStatistics(t *testing.T) {
	format := internal.GetFileFormat(iceberg.ParquetFile)
	writerProps := func(props iceberg.Properties) *parquet.WriterProperties {
		return parquet.NewWriterProperties(format.GetWriteProperties(props).([]parquet.WriterProperty)...)
	}
	prefix := internal.ParquetColumnStatsEnabledKeyPrefix

	defaults := writerProps(nil)
	assert.True(t, defaults.StatisticsEnabled())
	assert.True(t, defaults.StatisticsEnabledFor("id"))

	props := iceberg.Properties{
		prefix + ".id":           "false",
		prefix + ".enabled":      "TrUe",
		prefix + ".nested.child": "FALSE",
		prefix + ".number":       "1",
		prefix + ".empty":        "",
		prefix + ".space":        " true ",
		prefix + ".":             "false",
		prefix:                   "false",
	}
	wp := writerProps(props)
	assert.True(t, wp.StatisticsEnabled(), "the global default must remain enabled")
	for _, name := range []string{"id", "nested.child", "number", "empty", "space"} {
		assert.False(t, wp.StatisticsEnabledFor(name), name)
	}
	for _, name := range []string{"enabled", "unmentioned", "nested", ""} {
		assert.True(t, wp.StatisticsEnabledFor(name), name)
	}
}

func TestResolveParquetStatsColumnPaths(t *testing.T) {
	schema := iceberg.NewSchema(0,
		iceberg.NestedField{ID: 1, Name: "id", Type: iceberg.PrimitiveTypes.Int32},
		iceberg.NestedField{ID: 2, Name: "display name", Type: iceberg.PrimitiveTypes.String},
		iceberg.NestedField{ID: 3, Name: "parent", Type: &iceberg.StructType{FieldList: []iceberg.NestedField{
			{ID: 4, Name: "child", Type: iceberg.PrimitiveTypes.Int32},
		}}},
		iceberg.NestedField{ID: 5, Name: "items", Type: iceberg.NewListType(6, iceberg.PrimitiveTypes.Int32, false)},
		iceberg.NestedField{ID: 7, Name: "properties", Type: iceberg.NewMapType(8, iceberg.PrimitiveTypes.String, 9, iceberg.PrimitiveTypes.Int32, false)},
		iceberg.NestedField{ID: 10, Name: "entries", Type: iceberg.NewListType(11, &iceberg.StructType{FieldList: []iceberg.NestedField{
			{ID: 12, Name: "price", Type: iceberg.PrimitiveTypes.Int32},
		}}, false)},
		iceberg.NestedField{ID: 13, Name: "lookup", Type: iceberg.NewMapType(14, iceberg.PrimitiveTypes.String, 15, &iceberg.StructType{FieldList: []iceberg.NestedField{
			{ID: 16, Name: "price", Type: iceberg.PrimitiveTypes.Int32},
		}}, false)},
	)
	fileSchema, err := iceberg.SanitizeColumnNames(schema)
	require.NoError(t, err)
	mapping, err := internal.GetFileFormat(iceberg.ParquetFile).PathToIDMapping(fileSchema)
	require.NoError(t, err)
	prefix := internal.ParquetColumnStatsEnabledKeyPrefix + "."
	props := iceberg.Properties{
		prefix + "id":                    "false",
		prefix + "display name":          "false",
		prefix + "parent.child":          "TRUE",
		prefix + "items.element":         "false",
		prefix + "properties.key":        "false",
		prefix + "properties.value":      "false",
		prefix + "entries.element.price": "false",
		prefix + "lookup.value.price":    "false",
		prefix + "items.list.element":    "true", // Not an Iceberg field name.
		prefix + "missing":               "false",
		prefix:                           "false",
		"unrelated":                      "kept",
	}
	original := maps.Clone(props)
	mapped := internal.ResolveParquetStatsColumnPaths(props, schema, fileSchema, mapping, iceberg.EntryContentData)
	assert.Equal(t, original, props, "the table properties must not be modified")
	assert.Equal(t, iceberg.Properties{
		prefix + "id":                           "false",
		prefix + "display_x20name":              "false",
		prefix + "parent.child":                 "TRUE",
		prefix + "items.list.element":           "false",
		prefix + "properties.key_value.key":     "false",
		prefix + "properties.key_value.value":   "false",
		prefix + "entries.list.element.price":   "false",
		prefix + "lookup.key_value.value.price": "false",
		"unrelated":                             "kept",
	}, mapped)

	wp := parquet.NewWriterProperties(internal.GetFileFormat(iceberg.ParquetFile).
		GetWriteProperties(mapped).([]parquet.WriterProperty)...)
	assert.False(t, wp.StatisticsEnabledFor("items.list.element"))
	assert.True(t, wp.StatisticsEnabledFor("parent.child"))
	assert.True(t, wp.StatisticsEnabledFor("unmentioned"))
}

func TestResolveParquetStatsColumnPathsForFileOnlyFields(t *testing.T) {
	tableSchema := iceberg.NewSchema(0, iceberg.NestedField{ID: 1, Name: "id", Type: iceberg.PrimitiveTypes.Int32})
	fileSchema := iceberg.NewSchema(0, iceberg.NestedField{ID: 2, Name: "file_path", Type: iceberg.PrimitiveTypes.String})
	prefix := internal.ParquetColumnStatsEnabledKeyPrefix + "."
	props := iceberg.Properties{prefix + "file_path": "false", prefix + "missing": "false"}
	mapping := map[string]int{"file_path": 2, "unknown": 99}
	assert.Equal(t, iceberg.Properties{prefix + "file_path": "false"},
		internal.ResolveParquetStatsColumnPaths(props, tableSchema, fileSchema, mapping, iceberg.EntryContentData))
	assert.Empty(t, internal.ResolveParquetStatsColumnPaths(props, nil, nil, mapping, iceberg.EntryContentData))
	for _, content := range []iceberg.ManifestEntryContent{iceberg.EntryContentPosDeletes, iceberg.EntryContentEqDeletes} {
		assert.Empty(t, internal.ResolveParquetStatsColumnPaths(props, tableSchema, fileSchema, mapping, content))
	}
}
