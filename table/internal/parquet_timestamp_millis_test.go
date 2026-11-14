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
	"bytes"
	"context"
	"encoding/binary"
	"math"
	"testing"

	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/arrow-go/v18/parquet"
	"github.com/apache/arrow-go/v18/parquet/file"
	"github.com/apache/arrow-go/v18/parquet/metadata"
	"github.com/apache/arrow-go/v18/parquet/schema"
	"github.com/apache/iceberg-go/table/internal"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func timestampTestNode(t testing.TB, unit schema.TimeUnitType, convertedOnly bool) *schema.PrimitiveNode {
	t.Helper()

	var (
		node *schema.PrimitiveNode
		err  error
	)
	switch {
	case convertedOnly && unit == schema.TimeUnitMillis:
		node, err = schema.NewPrimitiveNodeConverted("ts", parquet.Repetitions.Required,
			parquet.Types.Int64, schema.ConvertedTypes.TimestampMillis, 0, 0, 0, 1)
	default:
		node, err = schema.NewPrimitiveNodeLogical("ts", parquet.Repetitions.Required,
			schema.NewTimestampLogicalType(false, unit), parquet.Types.Int64, -1, 1)
	}
	require.NoError(t, err)

	return node
}

func buildTimestampTestParquet(
	t testing.TB, node *schema.PrimitiveNode, props *parquet.WriterProperties, values []int64,
) []byte {
	t.Helper()

	root, err := schema.NewGroupNode("schema", parquet.Repetitions.Required, schema.FieldList{node}, -1)
	require.NoError(t, err)

	var buf bytes.Buffer
	pw := file.NewParquetWriter(&buf, root, file.WithWriterProps(props))
	rgw, err := pw.AppendRowGroupChecked()
	require.NoError(t, err)
	cw, err := rgw.NextColumn()
	require.NoError(t, err)
	_, err = cw.(*file.Int64ColumnChunkWriter).WriteBatch(values, nil, nil)
	require.NoError(t, err)
	require.NoError(t, cw.Close())
	require.NoError(t, rgw.Close())
	require.NoError(t, pw.Close())

	return buf.Bytes()
}

func int64PhysBytes(v int64) []byte {
	return binary.LittleEndian.AppendUint64(nil, uint64(v))
}

// A Parquet TIMESTAMP(MILLIS) column is read as an Iceberg microsecond
// timestamp, so the predicate bytes built from a microsecond literal never
// occur in the file's dictionary or bloom filter. Row groups must be kept for
// such columns. The microsecond rows prove the same predicates still prune
// when the units agree.
func TestTimestampMillisSkipsDictionaryAndBloomPruning(t *testing.T) {
	const (
		rows       = 1000
		firstMilli = int64(1_700_000_000_000)
		otherMilli = firstMilli + 1_500
	)

	dictionaryProps := parquet.NewWriterProperties(
		parquet.WithStats(false),
		parquet.WithDictionaryDefault(true),
		parquet.WithDictionaryCostFallbackFor("ts", false),
	)
	bloomProps := parquet.NewWriterProperties(
		parquet.WithStats(false),
		parquet.WithDictionaryDefault(false),
		parquet.WithBloomFilterEnabledFor("ts", true),
	)

	tests := []struct {
		name          string
		unit          schema.TimeUnitType
		convertedOnly bool
		perMilli      int64
	}{
		{"millis logical type", schema.TimeUnitMillis, false, 1},
		{"millis converted type only", schema.TimeUnitMillis, true, 1},
		{"micros control", schema.TimeUnitMicros, false, 1000},
	}
	filters := []struct {
		name     string
		props    *parquet.WriterProperties
		hasIndex func(*metadata.ColumnChunkMetaData) bool
		tester   func(predBytes []byte) *internal.ParquetRowGroupTester
	}{
		{
			name:  "dictionary only",
			props: dictionaryProps,
			hasIndex: func(cc *metadata.ColumnChunkMetaData) bool {
				return cc.HasDictionaryPage()
			},
			tester: func(b []byte) *internal.ParquetRowGroupTester {
				return &internal.ParquetRowGroupTester{
					DictionaryPreds: []internal.RowGroupDictionaryPred{{FieldID: 1, PhysBytes: [][]byte{b}}},
				}
			},
		},
		{
			name:  "bloom filter only",
			props: bloomProps,
			hasIndex: func(cc *metadata.ColumnChunkMetaData) bool {
				return cc.BloomFilterOffset() > 0
			},
			tester: func(b []byte) *internal.ParquetRowGroupTester {
				return &internal.ParquetRowGroupTester{
					BloomPreds: []internal.RowGroupBloomPred{{FieldID: 1, PhysBytes: [][]byte{b}}},
				}
			},
		},
	}

	values := func(perMilli int64) []int64 {
		out := make([]int64, rows)
		for i := range out {
			out[i] = firstMilli * perMilli
			if i%2 == 1 {
				out[i] = otherMilli * perMilli
			}
		}

		return out
	}

	for _, filter := range filters {
		for _, tt := range tests {
			t.Run(filter.name+"/"+tt.name, func(t *testing.T) {
				data := buildTimestampTestParquet(t,
					timestampTestNode(t, tt.unit, tt.convertedOnly), filter.props, values(tt.perMilli))

				pqRdr, err := file.NewParquetReader(bytes.NewReader(data))
				require.NoError(t, err)
				cc, err := pqRdr.MetaData().RowGroup(0).ColumnChunk(0)
				require.NoError(t, err)
				require.True(t, filter.hasIndex(cc), "the writer did not produce the structure under test")
				require.NoError(t, pqRdr.Close())

				countKept := func(microsLiteral int64) int64 {
					rdr := openBloomTestReader(t, data)
					defer rdr.Close()
					rr, err := rdr.GetRecords(context.Background(), []int{0}, filter.tester(int64PhysBytes(microsLiteral)))
					require.NoError(t, err)

					return countRecords(t, rr)
				}

				assert.Equal(t, int64(rows), countKept(firstMilli*1000),
					"pruned a row group that holds the queried instant")
				assert.Equal(t, int64(rows), countKept(otherMilli*1000),
					"pruned a row group that holds the queried instant")
				if tt.perMilli == 1000 {
					assert.Zero(t, countKept((otherMilli+1)*1000),
						"control: an absent microsecond value should still prune")
				}
			})
		}
	}
}

func int64Stats(t testing.TB, node *schema.PrimitiveNode, minV, maxV int64) metadata.TypedStatistics {
	t.Helper()

	root, err := schema.NewGroupNode("schema", parquet.Repetitions.Required, schema.FieldList{node}, -1)
	require.NoError(t, err)
	stats := metadata.NewInt64Statistics(schema.NewSchema(root).Column(0), memory.DefaultAllocator)
	stats.SetMinMax(minV, maxV)

	return stats
}

func TestTimestampMillisAsMicros(t *testing.T) {
	millis := timestampTestNode(t, schema.TimeUnitMillis, false)
	convertedOnly := timestampTestNode(t, schema.TimeUnitMillis, true)
	micros := timestampTestNode(t, schema.TimeUnitMicros, false)

	t.Run("scales min and max to microseconds", func(t *testing.T) {
		for _, node := range []*schema.PrimitiveNode{millis, convertedOnly} {
			stats := internal.TimestampMillisAsMicros(int64Stats(t, node, -1_500, 1_700_000_000_123))
			require.True(t, stats.HasMinMax())
			assert.Equal(t, int64PhysBytes(-1_500_000), stats.EncodeMin())
			assert.Equal(t, int64PhysBytes(1_700_000_000_123_000), stats.EncodeMax())
		}
	})

	t.Run("leaves microsecond statistics alone", func(t *testing.T) {
		stats := internal.TimestampMillisAsMicros(int64Stats(t, micros, 5, 9))
		assert.Equal(t, int64PhysBytes(5), stats.EncodeMin())
		assert.Equal(t, int64PhysBytes(9), stats.EncodeMax())
	})

	t.Run("reports no min or max when scaling overflows", func(t *testing.T) {
		const limit = math.MaxInt64 / 1000
		tests := []struct {
			name     string
			min, max int64
			wantBoth bool
		}{
			{"largest exact", -limit, limit, true},
			{"max overflows", 0, limit + 1, false},
			{"min overflows", math.MinInt64/1000 - 1, 0, false},
		}
		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				stats := internal.TimestampMillisAsMicros(int64Stats(t, millis, tt.min, tt.max))
				assert.Equal(t, tt.wantBoth, stats.HasMinMax())
				if !tt.wantBoth {
					assert.Nil(t, stats.EncodeMin())
					assert.Nil(t, stats.EncodeMax())
				}
			})
		}
	})
}
