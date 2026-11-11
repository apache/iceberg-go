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
	"strconv"
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

func BenchmarkResolveParquetStatsColumnPaths(b *testing.B) {
	for _, columnCount := range []int{32, 1024} {
		fields := make([]iceberg.NestedField, columnCount)
		for i := range fields {
			fields[i] = iceberg.NestedField{
				ID:   i + 1,
				Name: "col_" + strconv.Itoa(i),
				Type: iceberg.PrimitiveTypes.Int32,
			}
		}
		schema := iceberg.NewSchema(0, fields...)
		mapping, err := internal.GetFileFormat(iceberg.ParquetFile).PathToIDMapping(schema)
		if err != nil {
			b.Fatal(err)
		}

		for _, configured := range []bool{false, true} {
			props := iceberg.Properties{"unrelated": "kept"}
			name := "default"
			if configured {
				props[internal.ParquetColumnStatsEnabledKeyPrefix+".col_0"] = "false"
				name = "one-override"
			}
			b.Run(strconv.Itoa(columnCount)+"/"+name, func(b *testing.B) {
				b.ReportAllocs()
				for range b.N {
					benchmarkMappedStatsProps = internal.ResolveParquetStatsColumnPaths(
						props, schema, schema, mapping, iceberg.EntryContentData)
				}
			})
		}
	}
}

var benchmarkMappedStatsProps iceberg.Properties

func TestGetWritePropertiesColumnStatisticsBooleanParsing(t *testing.T) {
	format := internal.GetFileFormat(iceberg.ParquetFile)
	prefix := internal.ParquetColumnStatsEnabledKeyPrefix
	cases := []struct {
		name    string
		value   string
		enabled bool
	}{
		{"lowercase-true", "true", true},
		{"uppercase-true", "TRUE", true},
		{"mixed-case-true", "tRuE", true},
		{"false", "false", false},
		{"numeric-one", "1", false},
		{"yes", "yes", false},
		{"leading-space", " true", false},
		{"trailing-space", "true ", false},
		{"empty", "", false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			props := iceberg.Properties{}
			props[prefix+".chosen"] = tc.value
			props[prefix+".chosen.child"] = "false"
			props[prefix+"."] = "false"
			props[prefix] = "false"
			wp := parquet.NewWriterProperties(format.GetWriteProperties(props).([]parquet.WriterProperty)...)
			assert.True(t, wp.StatisticsEnabled(), "column settings cannot change the default")
			assert.Equal(t, tc.enabled, wp.StatisticsEnabledFor("chosen"))
			assert.False(t, wp.StatisticsEnabledFor("chosen.child"))
			assert.True(t, wp.StatisticsEnabledFor("unmentioned"))
		})
	}
}

func TestResolveParquetStatsColumnPathsRenamedProjection(t *testing.T) {
	tableSchema := iceberg.NewSchema(0,
		iceberg.NestedField{ID: 1, Name: "renamed", Type: iceberg.PrimitiveTypes.Int32},
		iceberg.NestedField{ID: 2, Name: "only in table", Type: iceberg.PrimitiveTypes.Int32},
		iceberg.NestedField{ID: 4, Name: "parent", Type: &iceberg.StructType{FieldList: []iceberg.NestedField{
			{ID: 5, Name: "renamed child", Type: iceberg.PrimitiveTypes.Int32},
		}}},
	)
	fileSchema := iceberg.NewSchema(0,
		iceberg.NestedField{ID: 1, Name: "old_name", Type: iceberg.PrimitiveTypes.Int32},
		iceberg.NestedField{ID: 3, Name: "file_only", Type: iceberg.PrimitiveTypes.Int32},
		iceberg.NestedField{ID: 4, Name: "parent", Type: &iceberg.StructType{FieldList: []iceberg.NestedField{
			{ID: 5, Name: "original_child", Type: iceberg.PrimitiveTypes.Int32},
		}}},
	)
	mapping, err := internal.GetFileFormat(iceberg.ParquetFile).PathToIDMapping(fileSchema)
	require.NoError(t, err)
	prefix := internal.ParquetColumnStatsEnabledKeyPrefix + "."
	props := iceberg.Properties{"custom": "kept"}
	props[prefix+"renamed"] = "false"
	props[prefix+"old_name"] = "true"
	props[prefix+"only in table"] = "false"
	props[prefix+"file_only"] = "FALSE"
	props[prefix+"parent.renamed child"] = "false"
	props[prefix+"parent.original_child"] = "true"
	props[prefix+"unknown"] = "false"
	original := maps.Clone(props)

	mapped := internal.ResolveParquetStatsColumnPaths(props, tableSchema, fileSchema, mapping, iceberg.EntryContentData)
	assert.Equal(t, original, props)
	expected := iceberg.Properties{"custom": "kept"}
	expected[prefix+"old_name"] = "false"
	expected[prefix+"file_only"] = "FALSE"
	expected[prefix+"parent.original_child"] = "false"
	require.Equal(t, expected, mapped)

	wp := parquet.NewWriterProperties(internal.GetFileFormat(iceberg.ParquetFile).
		GetWriteProperties(mapped).([]parquet.WriterProperty)...)
	for _, name := range []string{"old_name", "file_only", "parent.original_child"} {
		assert.False(t, wp.StatisticsEnabledFor(name), name)
	}
	assert.True(t, wp.StatisticsEnabledFor("unmentioned"))
}

func TestResolveParquetStatsColumnPathsPreservesOtherProperties(t *testing.T) {
	prefix := internal.ParquetColumnStatsEnabledKeyPrefix + "."
	other := iceberg.Properties{"custom": "kept"}
	other[internal.ParquetColumnStatsEnabledKeyPrefix] = "false"
	other[internal.ParquetBloomFilterColumnEnabledKeyPrefix+".id"] = "false"
	other["write.parquet.dict-encoding-enabled.column.id"] = "false"
	assert.Nil(t, internal.ResolveParquetStatsColumnPaths(nil, nil, nil, nil, iceberg.EntryContentData))
	assert.Equal(t, other, internal.ResolveParquetStatsColumnPaths(other, nil, nil, nil, iceberg.EntryContentData))

	props := maps.Clone(other)
	props[prefix+"id"] = "false"
	props[prefix] = "false"
	props[prefix+"missing"] = "true"
	for _, content := range []iceberg.ManifestEntryContent{
		iceberg.EntryContentPosDeletes, iceberg.EntryContentEqDeletes,
	} {
		assert.Equal(t, other, internal.ResolveParquetStatsColumnPaths(props, nil, nil, nil, content))
	}
	assert.Equal(t, other, internal.ResolveParquetStatsColumnPaths(props, nil, nil,
		map[string]int{"id": 1}, iceberg.EntryContentData))
	assert.Equal(t, "false", props[prefix+"id"], "resolving properties must not modify the input")
}
