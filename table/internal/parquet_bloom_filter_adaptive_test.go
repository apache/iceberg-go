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

package internal_test

import (
	"bytes"
	"fmt"
	"testing"

	"github.com/apache/arrow-go/v18/parquet"
	"github.com/apache/arrow-go/v18/parquet/file"
	"github.com/apache/arrow-go/v18/parquet/metadata"
	"github.com/apache/arrow-go/v18/parquet/schema"
	"github.com/apache/iceberg-go"
	"github.com/apache/iceberg-go/table/internal"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestAdaptiveParquetBloomFilterColumnIsolation(t *testing.T) {
	const rows = 4096
	const maxBytes = 16384
	columns := []struct {
		name     string
		distinct int
		enabled  bool
	}{
		{"small", 32, true},
		{"large", 2048, true},
		{"disabled", 16, false},
	}
	props := iceberg.Properties{
		internal.ParquetBloomFilterMaxBytesKey:                          "16384",
		internal.ParquetBloomFilterAdaptiveEnabledKey:                   "true",
		internal.ParquetDictEnabledKey:                                  "false",
		internal.ParquetBloomFilterColumnEnabledKeyPrefix + ".small":    "true",
		internal.ParquetBloomFilterColumnEnabledKeyPrefix + ".large":    "true",
		internal.ParquetBloomFilterColumnEnabledKeyPrefix + ".disabled": "false",
		internal.ParquetBloomFilterColumnNDVKeyPrefix + ".small":        "32",
		internal.ParquetBloomFilterColumnFPPKeyPrefix + ".small":        "0.01",
		internal.ParquetBloomFilterColumnFPPKeyPrefix + ".large":        "0.001",
		internal.ParquetBloomFilterColumnNDVKeyPrefix + ".disabled":     "1",
		internal.ParquetBloomFilterColumnFPPKeyPrefix + ".disabled":     "0.0001",
	}
	require.NoError(t, internal.ValidateParquetWriteProperties(props))
	format := internal.GetFileFormat(iceberg.ParquetFile)
	wp := parquet.NewWriterProperties(format.GetWriteProperties(props).([]parquet.WriterProperty)...)
	assert.True(t, wp.AdaptiveBloomFilterEnabled())
	assert.Equal(t, int64(32), wp.BloomFilterNDVFor("small"))
	assert.Zero(t, wp.BloomFilterNDVFor("large"))
	assert.InDelta(t, 0.001, wp.BloomFilterFPPFor("large"), 1e-10)
	assert.Equal(t, int64(1), wp.BloomFilterNDVFor("disabled"))
	assert.False(t, wp.BloomFilterEnabledFor("disabled"))

	fields := make(schema.FieldList, len(columns))
	for i, col := range columns {
		fields[i] = schema.NewInt32Node(col.name, parquet.Repetitions.Required, int32(i+1))
	}
	root, err := schema.NewGroupNode("schema", parquet.Repetitions.Required, fields, -1)
	require.NoError(t, err)

	var buf bytes.Buffer
	pw := file.NewParquetWriter(&buf, root, file.WithWriterProps(wp))
	rgw, err := pw.AppendRowGroupChecked()
	require.NoError(t, err)
	for _, col := range columns {
		cw, err := rgw.NextColumn()
		require.NoError(t, err)
		values := make([]int32, rows)
		for i := range values {
			values[i] = int32(i % col.distinct)
		}
		_, err = cw.(*file.Int32ColumnChunkWriter).WriteBatch(values, nil, nil)
		require.NoError(t, err)
		require.NoError(t, cw.Close())
	}
	require.NoError(t, rgw.Close())
	require.NoError(t, pw.Close())

	reader, err := file.NewParquetReader(bytes.NewReader(buf.Bytes()))
	require.NoError(t, err)
	defer reader.Close()
	blooms, err := reader.GetBloomFilterReader().RowGroup(0)
	require.NoError(t, err)
	sizes := make([]int64, len(columns))
	for i, col := range columns {
		visited := false
		err := blooms.VisitColumnBloomFilter(i, func(bf metadata.BloomFilter) error {
			visited = true
			if bf == nil {
				return fmt.Errorf("nil bloom filter for %s", col.name)
			}
			sizes[i] = bf.Size()
			hasher := bf.Hasher()
			for value := 0; value < col.distinct; value++ {
				if !bf.CheckHash(hasher.Sum64(int32PhysBytes(int32(value)))) {
					return fmt.Errorf("bloom filter for %s omitted inserted value %d", col.name, value)
				}
			}
			return nil
		})
		require.NoError(t, err)
		assert.Equal(t, col.enabled, visited, "%s bloom filter presence", col.name)
	}
	assert.Positive(t, sizes[0])
	assert.Less(t, sizes[0], sizes[1], "explicit small NDV must not change the other column's adaptive sizing")
	assert.LessOrEqual(t, sizes[1], int64(maxBytes))
	assert.Zero(t, sizes[2], "disabled column must not write a filter, even with FPP and NDV")
}

func TestAdaptiveParquetBloomFilterRowGroupIsolation(t *testing.T) {
	const rowsPerGroup = 4096
	cardinalities := []int{16, 2048}
	props := iceberg.Properties{
		internal.ParquetBloomFilterMaxBytesKey:                     "16384",
		internal.ParquetBloomFilterAdaptiveEnabledKey:              "true",
		internal.ParquetBloomFilterColumnEnabledKeyPrefix + ".id": "true",
		internal.ParquetDictEnabledKey:                              "false",
	}
	require.NoError(t, internal.ValidateParquetWriteProperties(props))
	format := internal.GetFileFormat(iceberg.ParquetFile)
	wp := parquet.NewWriterProperties(format.GetWriteProperties(props).([]parquet.WriterProperty)...)
	root, err := schema.NewGroupNode("schema", parquet.Repetitions.Required, schema.FieldList{
		schema.NewInt32Node("id", parquet.Repetitions.Required, 1),
	}, -1)
	require.NoError(t, err)

	var buf bytes.Buffer
	pw := file.NewParquetWriter(&buf, root, file.WithWriterProps(wp))
	for group, distinct := range cardinalities {
		rgw, err := pw.AppendRowGroupChecked()
		require.NoError(t, err)
		cw, err := rgw.NextColumn()
		require.NoError(t, err)
		values := make([]int32, rowsPerGroup)
		for i := range values {
			values[i] = int32(group*10000 + i%distinct)
		}
		_, err = cw.(*file.Int32ColumnChunkWriter).WriteBatch(values, nil, nil)
		require.NoError(t, err)
		require.NoError(t, cw.Close())
		require.NoError(t, rgw.Close())
	}
	require.NoError(t, pw.Close())

	reader, err := file.NewParquetReader(bytes.NewReader(buf.Bytes()))
	require.NoError(t, err)
	defer reader.Close()
	sizes := make([]int64, len(cardinalities))
	for group, distinct := range cardinalities {
		blooms, err := reader.GetBloomFilterReader().RowGroup(group)
		require.NoError(t, err)
		visited := false
		err = blooms.VisitColumnBloomFilter(0, func(bf metadata.BloomFilter) error {
			visited = true
			if bf == nil {
				return fmt.Errorf("nil bloom filter in row group %d", group)
			}
			sizes[group] = bf.Size()
			hasher := bf.Hasher()
			for value := 0; value < distinct; value++ {
				written := int32(group*10000 + value)
				if !bf.CheckHash(hasher.Sum64(int32PhysBytes(written))) {
					return fmt.Errorf("row group %d omitted inserted value %d", group, written)
				}
			}
			return nil
		})
		require.NoError(t, err)
		require.True(t, visited, "row group %d must have a bloom filter", group)
	}
	assert.Less(t, sizes[0], sizes[1], "adaptive sizing must reset for each row group")
}

func TestAdaptiveParquetBloomFilterColumnPathProperties(t *testing.T) {
	props := iceberg.Properties{
		internal.ParquetBloomFilterColumnEnabledKeyPrefix + ".nested.id": "true",
		internal.ParquetBloomFilterColumnFPPKeyPrefix + ".nested.id":     "0.002",
		internal.ParquetBloomFilterColumnNDVKeyPrefix + ".nested.id":     "42",
		internal.ParquetBloomFilterColumnFPPKeyPrefix + ".nested":        "0.1",
	}
	require.NoError(t, internal.ValidateParquetWriteProperties(props))
	format := internal.GetFileFormat(iceberg.ParquetFile)
	wp := parquet.NewWriterProperties(format.GetWriteProperties(props).([]parquet.WriterProperty)...)
	assert.True(t, wp.BloomFilterEnabledFor("nested.id"))
	assert.False(t, wp.BloomFilterEnabledFor("nested"))
	assert.Equal(t, int64(42), wp.BloomFilterNDVFor("nested.id"))
	assert.InDelta(t, 0.002, wp.BloomFilterFPPFor("nested.id"), 1e-10)
	assert.InDelta(t, 0.1, wp.BloomFilterFPPFor("nested"), 1e-10)
	assert.Zero(t, wp.BloomFilterNDVFor("nested"))
}
