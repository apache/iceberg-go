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
	"fmt"
	"math"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/bitutil"
	"github.com/apache/arrow-go/v18/arrow/compute"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/arrow-go/v18/parquet"
	"github.com/apache/arrow-go/v18/parquet/file"
	"github.com/apache/arrow-go/v18/parquet/metadata"
	"github.com/apache/arrow-go/v18/parquet/pqarrow"
	"github.com/apache/arrow-go/v18/parquet/schema"
	"github.com/apache/iceberg-go"
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

func buildMultiRGTimestampParquet(
	t testing.TB, nodes schema.FieldList, props *parquet.WriterProperties, rowGroups [][][]int64,
) []byte {
	t.Helper()

	root, err := schema.NewGroupNode("schema", parquet.Repetitions.Required, nodes, -1)
	require.NoError(t, err)

	var buf bytes.Buffer
	pw := file.NewParquetWriter(&buf, root, file.WithWriterProps(props))
	for _, rgCols := range rowGroups {
		require.Equal(t, len(nodes), len(rgCols))
		rgw, err := pw.AppendRowGroupChecked()
		require.NoError(t, err)
		for _, colVals := range rgCols {
			cw, err := rgw.NextColumn()
			require.NoError(t, err)
			_, err = cw.(*file.Int64ColumnChunkWriter).WriteBatch(colVals, nil, nil)
			require.NoError(t, err)
			require.NoError(t, cw.Close())
		}
		require.NoError(t, rgw.Close())
	}
	require.NoError(t, pw.Close())

	return buf.Bytes()
}

func TestTimestampMillisDictionaryAndBloomPruning(t *testing.T) {
	const (
		rows       = 1000
		negMilli   = int64(-1_500)
		firstMilli = int64(1_700_000_000_000)
		otherMilli = firstMilli + 1_500
		maxDivUs   = (math.MaxInt64 / 1000) * 1000
		minDivUs   = -(math.MaxInt64 / 1000) * 1000
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
	plainProps := parquet.NewWriterProperties(
		parquet.WithStats(false),
		parquet.WithDictionaryDefault(false),
	)

	tests := []struct {
		name          string
		unit          schema.TimeUnitType
		convertedOnly bool
		perMilli      int64
		fieldType     iceberg.Type
	}{
		{"millis logical timestamp", schema.TimeUnitMillis, false, 1, iceberg.PrimitiveTypes.Timestamp},
		{"millis logical timestamptz", schema.TimeUnitMillis, false, 1, iceberg.PrimitiveTypes.TimestampTz},
		{"millis converted type only", schema.TimeUnitMillis, true, 1, iceberg.PrimitiveTypes.Timestamp},
		{"micros control", schema.TimeUnitMicros, false, 1000, iceberg.PrimitiveTypes.Timestamp},
	}
	filters := []struct {
		name     string
		props    *parquet.WriterProperties
		hasIndex func(*metadata.ColumnChunkMetaData) bool
		tester   func(fieldType iceberg.Type, preds [][]byte) *internal.ParquetRowGroupTester
	}{
		{
			name:  "dictionary only",
			props: dictionaryProps,
			hasIndex: func(cc *metadata.ColumnChunkMetaData) bool {
				return cc.HasDictionaryPage()
			},
			tester: func(fieldType iceberg.Type, preds [][]byte) *internal.ParquetRowGroupTester {
				return &internal.ParquetRowGroupTester{
					DictionaryPreds: []internal.RowGroupDictionaryPred{{
						FieldID:   1,
						FieldType: fieldType,
						PhysBytes: preds,
					}},
				}
			},
		},
		{
			name:  "bloom filter only",
			props: bloomProps,
			hasIndex: func(cc *metadata.ColumnChunkMetaData) bool {
				return cc.BloomFilterOffset() > 0
			},
			tester: func(fieldType iceberg.Type, preds [][]byte) *internal.ParquetRowGroupTester {
				return &internal.ParquetRowGroupTester{
					BloomPreds: []internal.RowGroupBloomPred{{
						FieldID:   1,
						FieldType: fieldType,
						PhysBytes: preds,
					}},
				}
			},
		},
	}

	values := func(perMilli int64) []int64 {
		out := make([]int64, rows)
		for i := range out {
			switch i % 3 {
			case 0:
				out[i] = negMilli * perMilli
			case 1:
				out[i] = firstMilli * perMilli
			default:
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

				countKept := func(microsLiterals ...int64) int64 {
					phys := make([][]byte, len(microsLiterals))
					for i, v := range microsLiterals {
						phys[i] = int64PhysBytes(v)
					}
					rdr := openBloomTestReader(t, data)
					defer rdr.Close()
					rr, err := rdr.GetRecords(context.Background(), []int{0}, filter.tester(tt.fieldType, phys))
					require.NoError(t, err)

					return countRecords(t, rr)
				}

				assert.Equal(t, int64(rows), countKept(negMilli*1000), "present negative pre-1970 instant")
				assert.Equal(t, int64(rows), countKept(firstMilli*1000), "present positive instant")
				assert.Equal(t, int64(rows), countKept(otherMilli*1000), "present positive instant")
				assert.Zero(t, countKept((otherMilli+1)*1000), "absent divisible instant should prune")
				assert.Zero(t, countKept(maxDivUs), "absent max boundary divisible instant should prune")
				assert.Zero(t, countKept(minDivUs), "absent min boundary divisible instant should prune")

				// In predicate with mixed divisible + sub-ms literals.
				assert.Equal(t, int64(rows), countKept(firstMilli*1000+500, otherMilli*1000),
					"In with sub-ms and present divisible literal should keep")
				assert.Zero(t, countKept(firstMilli*1000+500, (otherMilli+1)*1000),
					"In with sub-ms and absent divisible literal should prune")

				// Sub-ms literals cannot match TIMESTAMP(MILLIS) and are absent in micros control.
				assert.Zero(t, countKept(firstMilli*1000+500))
				assert.Zero(t, countKept(-500))
				assert.Zero(t, countKept(math.MaxInt64))
				assert.Zero(t, countKept(math.MinInt64))
				assert.Zero(t, countKept(firstMilli*1000+1, -500))
			})
		}

		t.Run(filter.name+"/multi-row-group survivors", func(t *testing.T) {
			data := buildMultiRGTimestampParquet(t,
				schema.FieldList{timestampTestNode(t, schema.TimeUnitMillis, false)},
				filter.props,
				[][][]int64{
					{{negMilli, firstMilli}},
					{{otherMilli, otherMilli + 10, otherMilli + 20}},
				},
			)

			run := func(microsLiterals ...int64) (int64, []internal.RowGroupSpan) {
				phys := make([][]byte, len(microsLiterals))
				for i, v := range microsLiterals {
					phys[i] = int64PhysBytes(v)
				}
				var survivors []internal.RowGroupSpan
				tester := filter.tester(iceberg.PrimitiveTypes.Timestamp, phys)
				tester.Survivors = &survivors

				rdr := openBloomTestReader(t, data)
				defer rdr.Close()
				rr, err := rdr.GetRecords(context.Background(), []int{0}, tester)
				require.NoError(t, err)

				return countRecords(t, rr), survivors
			}

			n, survivors := run(negMilli * 1000)
			assert.Equal(t, int64(2), n)
			assert.Equal(t, []internal.RowGroupSpan{{FirstRowPos: 0, NumRows: 2}}, survivors)

			n, survivors = run(otherMilli * 1000)
			assert.Equal(t, int64(3), n)
			assert.Equal(t, []internal.RowGroupSpan{{FirstRowPos: 2, NumRows: 3}}, survivors)

			n, survivors = run((otherMilli + 999) * 1000)
			assert.Zero(t, n)
			assert.Empty(t, survivors)

			n, survivors = run(firstMilli*1000 + 500)
			assert.Zero(t, n)
			assert.Empty(t, survivors)
		})

		t.Run(filter.name+"/sub-ms prunes plain-encoded file without indexes", func(t *testing.T) {
			data := buildTimestampTestParquet(t,
				timestampTestNode(t, schema.TimeUnitMillis, false), plainProps, []int64{firstMilli, otherMilli})

			pqRdr, err := file.NewParquetReader(bytes.NewReader(data))
			require.NoError(t, err)
			cc, err := pqRdr.MetaData().RowGroup(0).ColumnChunk(0)
			require.NoError(t, err)
			require.False(t, cc.HasDictionaryPage())
			require.LessOrEqual(t, cc.BloomFilterOffset(), int64(0))
			require.NoError(t, pqRdr.Close())

			var survivors []internal.RowGroupSpan
			tester := filter.tester(iceberg.PrimitiveTypes.Timestamp, [][]byte{
				int64PhysBytes(firstMilli*1000 + 500),
				int64PhysBytes(-500),
			})
			tester.Survivors = &survivors

			rdr := openBloomTestReader(t, data)
			defer rdr.Close()
			rr, err := rdr.GetRecords(context.Background(), []int{0}, tester)
			require.NoError(t, err)
			assert.Zero(t, countRecords(t, rr))
			assert.Empty(t, survivors)
		})

		t.Run(filter.name+"/zero-row-group file with unsatisfiable sub-ms resets survivors to nil", func(t *testing.T) {
			data := buildMultiRGTimestampParquet(t,
				schema.FieldList{timestampTestNode(t, schema.TimeUnitMillis, false)},
				plainProps,
				nil,
			)

			survivors := []internal.RowGroupSpan{{FirstRowPos: 0, NumRows: 1}}
			tester := filter.tester(iceberg.PrimitiveTypes.Timestamp, [][]byte{int64PhysBytes(firstMilli*1000 + 500)})
			tester.Survivors = &survivors

			rdr := openBloomTestReader(t, data)
			defer rdr.Close()
			rr, err := rdr.GetRecords(context.Background(), []int{0}, tester)
			require.NoError(t, err)
			assert.Zero(t, countRecords(t, rr))
			assert.Nil(t, survivors)
		})

		t.Run(filter.name+"/multiple predicates on same timestamp millis column", func(t *testing.T) {
			data := buildTimestampTestParquet(t,
				timestampTestNode(t, schema.TimeUnitMillis, false), filter.props, []int64{firstMilli, otherMilli})

			makeTwoPredTester := func(firstUs, secondUs int64) *internal.ParquetRowGroupTester {
				tester := filter.tester(iceberg.PrimitiveTypes.Timestamp, [][]byte{int64PhysBytes(firstUs)})
				if len(tester.DictionaryPreds) > 0 {
					tester.DictionaryPreds = append(tester.DictionaryPreds, internal.RowGroupDictionaryPred{
						FieldID:   1,
						FieldType: iceberg.PrimitiveTypes.Timestamp,
						PhysBytes: [][]byte{int64PhysBytes(secondUs)},
					})
				} else {
					tester.BloomPreds = append(tester.BloomPreds, internal.RowGroupBloomPred{
						FieldID:   1,
						FieldType: iceberg.PrimitiveTypes.Timestamp,
						PhysBytes: [][]byte{int64PhysBytes(secondUs)},
					})
				}

				return tester
			}

			run := func(firstUs, secondUs int64) int64 {
				rdr := openBloomTestReader(t, data)
				defer rdr.Close()
				rr, err := rdr.GetRecords(context.Background(), []int{0}, makeTwoPredTester(firstUs, secondUs))
				require.NoError(t, err)

				return countRecords(t, rr)
			}

			assert.Equal(t, int64(2), run(firstMilli*1000, otherMilli*1000), "both divisible predicates present")
			assert.Zero(t, run(firstMilli*1000, (otherMilli+1)*1000), "second divisible predicate absent")
			assert.Zero(t, run(firstMilli*1000, firstMilli*1000+500), "sub-ms predicate after allocated adapted slice")
		})

		t.Run(filter.name+"/safety guards drop predicate conservatively", func(t *testing.T) {
			idNode, err := schema.NewPrimitiveNode("id", parquet.Repetitions.Required, parquet.Types.Int64, 2, -1)
			require.NoError(t, err)
			props := parquet.NewWriterProperties(
				parquet.WithStats(false),
				parquet.WithDictionaryDefault(filter.name == "dictionary only"),
				parquet.WithDictionaryCostFallbackFor("ts", false),
				parquet.WithDictionaryCostFallbackFor("id", false),
				parquet.WithBloomFilterEnabledFor("ts", filter.name == "bloom filter only"),
				parquet.WithBloomFilterEnabledFor("id", filter.name == "bloom filter only"),
			)
			data := buildMultiRGTimestampParquet(t,
				schema.FieldList{timestampTestNode(t, schema.TimeUnitMillis, false), idNode},
				props,
				[][][]int64{
					{{firstMilli, otherMilli}, {10, 20}},
				},
			)
			pqRdr, err := file.NewParquetReader(bytes.NewReader(data))
			require.NoError(t, err)
			tsCC, err := pqRdr.MetaData().RowGroup(0).ColumnChunk(0)
			require.NoError(t, err)
			require.True(t, filter.hasIndex(tsCC), "ts column missing expected index")
			idCC, err := pqRdr.MetaData().RowGroup(0).ColumnChunk(1)
			require.NoError(t, err)
			require.True(t, filter.hasIndex(idCC), "id column missing expected index")
			require.NoError(t, pqRdr.Close())

			guardCases := []struct {
				name      string
				fieldType iceberg.Type
				physBytes [][]byte
			}{
				{"nil field type divisible", nil, [][]byte{int64PhysBytes((otherMilli + 1) * 1000)}},
				{"nil field type sub-ms", nil, [][]byte{int64PhysBytes(firstMilli*1000 + 500)}},
				{"TimestampNs divisible", iceberg.PrimitiveTypes.TimestampNs, [][]byte{int64PhysBytes((otherMilli + 1) * 1000)}},
				{"TimestampNs sub-ms", iceberg.PrimitiveTypes.TimestampNs, [][]byte{int64PhysBytes(firstMilli*1000 + 500)}},
				{"TimestampTzNs divisible", iceberg.PrimitiveTypes.TimestampTzNs, [][]byte{int64PhysBytes((otherMilli + 1) * 1000)}},
				{"TimestampTzNs sub-ms", iceberg.PrimitiveTypes.TimestampTzNs, [][]byte{int64PhysBytes(firstMilli*1000 + 500)}},
				{"Int64 divisible", iceberg.PrimitiveTypes.Int64, [][]byte{int64PhysBytes((otherMilli + 1) * 1000)}},
				{"Int64 sub-ms", iceberg.PrimitiveTypes.Int64, [][]byte{int64PhysBytes(firstMilli*1000 + 500)}},
				{"malformed width", iceberg.PrimitiveTypes.Timestamp, [][]byte{{1, 2, 3, 4}}},
				{"mixed sub-ms and malformed width", iceberg.PrimitiveTypes.Timestamp, [][]byte{int64PhysBytes(firstMilli*1000 + 500), {1, 2, 3, 4}}},
				{"mixed divisible and malformed width", iceberg.PrimitiveTypes.Timestamp, [][]byte{int64PhysBytes((otherMilli + 1) * 1000), {1, 2, 3, 4}}},
			}

			for _, gc := range guardCases {
				t.Run(gc.name, func(t *testing.T) {
					// Without a companion predicate, the guarded predicate must not prune.
					rdr := openBloomTestReader(t, data)
					rr, err := rdr.GetRecords(context.Background(), []int{0, 1}, filter.tester(gc.fieldType, gc.physBytes))
					require.NoError(t, err)
					assert.Equal(t, int64(2), countRecords(t, rr))
					require.NoError(t, rdr.Close())

					// With an absent companion predicate on id, pruning still happens.
					tester := filter.tester(gc.fieldType, gc.physBytes)
					if len(tester.DictionaryPreds) > 0 {
						tester.DictionaryPreds = append(tester.DictionaryPreds, internal.RowGroupDictionaryPred{
							FieldID:   2,
							FieldType: iceberg.PrimitiveTypes.Int64,
							PhysBytes: [][]byte{int64PhysBytes(999)},
						})
					} else {
						tester.BloomPreds = append(tester.BloomPreds, internal.RowGroupBloomPred{
							FieldID:   2,
							FieldType: iceberg.PrimitiveTypes.Int64,
							PhysBytes: [][]byte{int64PhysBytes(999)},
						})
					}
					rdr2 := openBloomTestReader(t, data)
					rr2, err := rdr2.GetRecords(context.Background(), []int{0, 1}, tester)
					require.NoError(t, err)
					assert.Zero(t, countRecords(t, rr2))
					require.NoError(t, rdr2.Close())
				})
			}
		})

		t.Run(filter.name+"/shared tester immutability across millis and micros files", func(t *testing.T) {
			millisData := buildTimestampTestParquet(t,
				timestampTestNode(t, schema.TimeUnitMillis, false), filter.props, []int64{firstMilli, otherMilli})
			microsData := buildTimestampTestParquet(t,
				timestampTestNode(t, schema.TimeUnitMicros, false), filter.props, []int64{firstMilli * 1000, otherMilli * 1000})

			subMsBytes := int64PhysBytes(firstMilli*1000 + 500)
			divBytes := int64PhysBytes(firstMilli * 1000)
			wantOrig := [][]byte{
				append([]byte(nil), subMsBytes...),
				append([]byte(nil), divBytes...),
			}
			tester := filter.tester(iceberg.PrimitiveTypes.Timestamp, [][]byte{subMsBytes, divBytes})

			// Scan TIMESTAMP(MILLIS) file first.
			rdr1 := openBloomTestReader(t, millisData)
			rr1, err := rdr1.GetRecords(context.Background(), []int{0}, tester)
			require.NoError(t, err)
			assert.Equal(t, int64(2), countRecords(t, rr1))
			require.NoError(t, rdr1.Close())

			// Caller's predicate slice length and bytes must be untouched.
			if len(tester.DictionaryPreds) > 0 {
				assert.Equal(t, wantOrig, tester.DictionaryPreds[0].PhysBytes)
			} else {
				assert.Equal(t, wantOrig, tester.BloomPreds[0].PhysBytes)
			}

			// Reuse the exact same tester on TIMESTAMP(MICROS) file.
			rdr2 := openBloomTestReader(t, microsData)
			rr2, err := rdr2.GetRecords(context.Background(), []int{0}, tester)
			require.NoError(t, err)
			assert.Equal(t, int64(2), countRecords(t, rr2))
			require.NoError(t, rdr2.Close())
		})
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

func openCheckedParquetReader(t testing.TB, alloc memory.Allocator, data []byte) internal.FileReader {
	t.Helper()

	pqRdr, err := file.NewParquetReader(bytes.NewReader(data),
		file.WithReadProps(parquet.NewReaderProperties(alloc)))
	require.NoError(t, err)

	arrRdr, err := pqarrow.NewFileReader(pqRdr, pqarrow.ArrowReadProperties{}, alloc)
	require.NoError(t, err)

	return internal.WrapParquetFileReader(arrRdr)
}

func TestTimestampMillisGetRecordsOverflowReturnsError(t *testing.T) {
	tests := []struct {
		name string
		val  int64
	}{
		{"positive overflow", math.MaxInt64/1000 + 1},
		{"negative overflow", math.MinInt64/1000 - 1},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			alloc := memory.NewCheckedAllocator(memory.DefaultAllocator)
			defer alloc.AssertSize(t, 0)

			data := buildTimestampTestParquet(t,
				timestampTestNode(t, schema.TimeUnitMillis, false),
				parquet.NewWriterProperties(parquet.WithStats(false)),
				[]int64{1_000, tt.val},
			)

			ctx := compute.WithAllocator(t.Context(), alloc)
			rdr := openCheckedParquetReader(t, alloc, data)
			defer func() { require.NoError(t, rdr.Close()) }()

			sc, err := rdr.Schema()
			require.NoError(t, err)
			require.Equal(t, arrow.Microsecond, sc.Field(0).Type.(*arrow.TimestampType).Unit)

			rr, err := rdr.GetRecords(ctx, []int{0}, nil)
			require.NoError(t, err)
			defer rr.Release()

			assert.False(t, rr.Next())
			require.ErrorIs(t, rr.Err(), iceberg.ErrInvalidArgument)
		})
	}
}

func TestTimestampMillisNestedAndNullsNormalizeWithoutLeak(t *testing.T) {
	alloc := memory.NewCheckedAllocator(memory.DefaultAllocator)
	defer alloc.AssertSize(t, 0)

	fieldIDMeta := func(id string) arrow.Metadata {
		return arrow.MetadataFrom(map[string]string{"PARQUET:field_id": id})
	}
	msType := &arrow.TimestampType{Unit: arrow.Millisecond}
	msTzType := &arrow.TimestampType{Unit: arrow.Millisecond, TimeZone: "UTC"}

	arrowSc := arrow.NewSchema([]arrow.Field{
		{Name: "ts", Type: msType, Nullable: true, Metadata: fieldIDMeta("1")},
		{Name: "st", Type: arrow.StructOf(
			arrow.Field{Name: "inner_ts", Type: msTzType, Nullable: true, Metadata: fieldIDMeta("3")},
		), Nullable: true, Metadata: fieldIDMeta("2")},
		{Name: "lst", Type: arrow.ListOfField(
			arrow.Field{Name: "element", Type: msType, Nullable: true, Metadata: fieldIDMeta("5")},
		), Nullable: true, Metadata: fieldIDMeta("4")},
		{Name: "mp", Type: arrow.MapOfFields(
			arrow.Field{Name: "key", Type: arrow.BinaryTypes.String, Nullable: false, Metadata: fieldIDMeta("7")},
			arrow.Field{Name: "value", Type: msType, Nullable: true, Metadata: fieldIDMeta("8")},
		), Nullable: true, Metadata: fieldIDMeta("6")},
	}, nil)

	rb := array.NewRecordBuilder(alloc, arrowSc)
	defer rb.Release()

	// Row 0: non-null across all columns
	rb.Field(0).(*array.TimestampBuilder).Append(1_500)
	sb := rb.Field(1).(*array.StructBuilder)
	sb.Append(true)
	sb.FieldBuilder(0).(*array.TimestampBuilder).Append(2_500)
	lb := rb.Field(2).(*array.ListBuilder)
	lb.Append(true)
	lb.ValueBuilder().(*array.TimestampBuilder).Append(3_500)
	lb.ValueBuilder().(*array.TimestampBuilder).AppendNull()
	mb := rb.Field(3).(*array.MapBuilder)
	mb.Append(true)
	mb.KeyBuilder().(*array.StringBuilder).Append("k")
	mb.ItemBuilder().(*array.TimestampBuilder).Append(4_500)

	// Row 1: nulls across all columns
	rb.Field(0).(*array.TimestampBuilder).AppendNull()
	sb.AppendNull()
	lb.AppendNull()
	mb.AppendNull()

	rec := rb.NewRecordBatch()
	defer rec.Release()
	tbl := array.NewTableFromRecords(arrowSc, []arrow.RecordBatch{rec})
	defer tbl.Release()

	var buf bytes.Buffer
	require.NoError(t, pqarrow.WriteTable(tbl, &buf, 2,
		parquet.NewWriterProperties(parquet.WithStats(false)),
		pqarrow.NewArrowWriterProperties(pqarrow.WithAllocator(alloc))))

	ctx := compute.WithAllocator(t.Context(), alloc)
	rdr := openCheckedParquetReader(t, alloc, buf.Bytes())
	defer func() { require.NoError(t, rdr.Close()) }()

	pruned, cols, err := rdr.PrunedSchema(map[int]struct{}{1: {}, 3: {}, 5: {}, 7: {}, 8: {}}, nil)
	require.NoError(t, err)
	assert.Equal(t, arrow.Microsecond, pruned.Field(0).Type.(*arrow.TimestampType).Unit)
	assert.Equal(t, arrow.Microsecond, pruned.Field(1).Type.(*arrow.StructType).Field(0).Type.(*arrow.TimestampType).Unit)
	assert.Equal(t, "UTC", pruned.Field(1).Type.(*arrow.StructType).Field(0).Type.(*arrow.TimestampType).TimeZone)
	assert.Equal(t, arrow.Microsecond, pruned.Field(2).Type.(*arrow.ListType).Elem().(*arrow.TimestampType).Unit)
	assert.Equal(t, arrow.Microsecond, pruned.Field(3).Type.(*arrow.MapType).ItemType().(*arrow.TimestampType).Unit)

	rr, err := rdr.GetRecords(ctx, cols, nil)
	require.NoError(t, err)
	defer rr.Release()

	require.True(t, rr.Next())
	got := rr.RecordBatch()
	require.Equal(t, int64(2), got.NumRows())

	tsCol := got.Column(0).(*array.Timestamp)
	assert.Equal(t, arrow.Timestamp(1_500_000), tsCol.Value(0))
	assert.True(t, tsCol.IsNull(1))

	stCol := got.Column(1).(*array.Struct)
	assert.Equal(t, arrow.Timestamp(2_500_000), stCol.Field(0).(*array.Timestamp).Value(0))
	assert.True(t, stCol.IsNull(1))

	lstCol := got.Column(2).(*array.List)
	lstVals := lstCol.ListValues().(*array.Timestamp)
	assert.Equal(t, arrow.Timestamp(3_500_000), lstVals.Value(0))
	assert.True(t, lstVals.IsNull(1))
	assert.True(t, lstCol.IsNull(1))

	mpCol := got.Column(3).(*array.Map)
	mpVals := mpCol.Items().(*array.Timestamp)
	assert.Equal(t, arrow.Timestamp(4_500_000), mpVals.Value(0))
	assert.True(t, mpCol.IsNull(1))

	assert.False(t, rr.Next())
	require.NoError(t, rr.Err())
}

func TestTimestampMillisDictionaryEncodedNormalizesWithoutLeak(t *testing.T) {
	alloc := memory.NewCheckedAllocator(memory.DefaultAllocator)
	defer alloc.AssertSize(t, 0)

	data := buildTimestampTestParquet(t,
		timestampTestNode(t, schema.TimeUnitMillis, false),
		parquet.NewWriterProperties(
			parquet.WithStats(false),
			parquet.WithDictionaryDefault(true),
			parquet.WithDictionaryCostFallbackFor("ts", false),
		),
		[]int64{1_000, 2_000, 1_000, 2_000},
	)

	pqRdr, err := file.NewParquetReader(bytes.NewReader(data),
		file.WithReadProps(parquet.NewReaderProperties(alloc)))
	require.NoError(t, err)

	var arrProps pqarrow.ArrowReadProperties
	arrProps.SetReadDict(0, true)
	arrRdr, err := pqarrow.NewFileReader(pqRdr, arrProps, alloc)
	require.NoError(t, err)

	rdr := internal.WrapParquetFileReader(arrRdr)
	defer func() { require.NoError(t, rdr.Close()) }()

	ctx := compute.WithAllocator(t.Context(), alloc)
	rr, err := rdr.GetRecords(ctx, []int{0}, nil)
	require.NoError(t, err)
	defer rr.Release()

	require.True(t, rr.Next())
	got := rr.RecordBatch()
	tsCol := got.Column(0).(*array.Timestamp)
	assert.Equal(t, arrow.Microsecond, tsCol.DataType().(*arrow.TimestampType).Unit)
	require.Equal(t, 4, tsCol.Len())
	assert.Equal(t, arrow.Timestamp(1_000_000), tsCol.Value(0))
	assert.Equal(t, arrow.Timestamp(2_000_000), tsCol.Value(1))
	assert.Equal(t, arrow.Timestamp(1_000_000), tsCol.Value(2))
	assert.Equal(t, arrow.Timestamp(2_000_000), tsCol.Value(3))

	assert.False(t, rr.Next())
	require.NoError(t, rr.Err())
}

func TestWrapStatsForTypeMillisNonMicrosOmitsBounds(t *testing.T) {
	node, err := schema.NewPrimitiveNodeLogical("ts", parquet.Repetitions.Optional,
		schema.NewTimestampLogicalType(false, schema.TimeUnitMillis), parquet.Types.Int64, -1, 1)
	require.NoError(t, err)

	col := schema.NewColumn(node, 1, 0)
	stats := metadata.NewInt64Statistics(col, memory.DefaultAllocator)
	stats.Update([]int64{1_700_000_000_000, 1_700_000_001_500}, 2)

	for _, typ := range []iceberg.PrimitiveType{
		iceberg.PrimitiveTypes.TimestampNs,
		iceberg.PrimitiveTypes.TimestampTzNs,
		iceberg.PrimitiveTypes.Int64,
	} {
		t.Run(typ.String(), func(t *testing.T) {
			wrapped := internal.WrapStatsForType(stats, typ)
			assert.False(t, wrapped.HasMinMax(), "min/max bounds must be omitted for %s", typ)
			assert.True(t, wrapped.HasNullCount())
			assert.Equal(t, int64(2), wrapped.NullCount())
		})
	}
}

func TestDataFileStatsFromMetaPreservesNullCountsWhenMillisBoundsOmitted(t *testing.T) {
	writeTwoRowGroupMillis := func(t *testing.T, utc bool, rg0Vals, rg1Vals []int64, rg0Def, rg1Def []int16) *metadata.FileMetaData {
		t.Helper()

		node, err := schema.NewPrimitiveNodeLogical("ts", parquet.Repetitions.Optional,
			schema.NewTimestampLogicalType(utc, schema.TimeUnitMillis), parquet.Types.Int64, -1, 1)
		require.NoError(t, err)
		root, err := schema.NewGroupNode("schema", parquet.Repetitions.Required, schema.FieldList{node}, -1)
		require.NoError(t, err)

		var buf bytes.Buffer
		pw := file.NewParquetWriter(&buf, root,
			file.WithWriterProps(parquet.NewWriterProperties(parquet.WithStats(true))))

		writeRG := func(vals []int64, defs []int16) {
			rgw, err := pw.AppendRowGroupChecked()
			require.NoError(t, err)
			cw, err := rgw.NextColumn()
			require.NoError(t, err)
			_, err = cw.(*file.Int64ColumnChunkWriter).WriteBatch(vals, defs, nil)
			require.NoError(t, err)
			require.NoError(t, cw.Close())
			require.NoError(t, rgw.Close())
		}

		writeRG(rg0Vals, rg0Def)
		writeRG(rg1Vals, rg1Def)
		require.NoError(t, pw.Close())

		pqRdr, err := file.NewParquetReader(bytes.NewReader(buf.Bytes()))
		require.NoError(t, err)
		defer func() { require.NoError(t, pqRdr.Close()) }()

		return pqRdr.MetaData()
	}

	const overflowMillis = math.MaxInt64/1000 + 1

	tests := []struct {
		name      string
		utc       bool
		icebergTy iceberg.PrimitiveType
		rg0Vals   []int64
		rg1Vals   []int64
	}{
		{
			name:      "timestamp_ns",
			utc:       false,
			icebergTy: iceberg.PrimitiveTypes.TimestampNs,
			rg0Vals:   []int64{1_700_000_000_000, 1_700_000_001_000},
			rg1Vals:   []int64{1_700_000_002_000},
		},
		{
			name:      "timestamptz_ns",
			utc:       true,
			icebergTy: iceberg.PrimitiveTypes.TimestampTzNs,
			rg0Vals:   []int64{1_700_000_000_000, 1_700_000_001_000},
			rg1Vals:   []int64{1_700_000_002_000},
		},
		{
			name:      "overflowing_timestamp_micros",
			utc:       false,
			icebergTy: iceberg.PrimitiveTypes.Timestamp,
			rg0Vals:   []int64{0, overflowMillis},
			rg1Vals:   []int64{1_000},
		},
		{
			name:      "overflowing_timestamp_micros_second_rg",
			utc:       false,
			icebergTy: iceberg.PrimitiveTypes.Timestamp,
			rg0Vals:   []int64{0, 1_000},
			rg1Vals:   []int64{overflowMillis},
		},
	}

	format := internal.GetFileFormat(iceberg.ParquetFile)
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// RG0: 2 non-null, 1 null. RG1: 1 non-null, 2 nulls. Total: 6 values, 3 nulls.
			meta := writeTwoRowGroupMillis(t, tt.utc,
				tt.rg0Vals, tt.rg1Vals,
				[]int16{1, 0, 1},
				[]int16{0, 1, 0},
			)
			collectors := map[int]internal.StatisticsCollector{
				1: {
					FieldID:    1,
					IcebergTyp: tt.icebergTy,
					Mode:       internal.MetricsMode{Typ: internal.MetricModeFull},
				},
			}
			got := format.DataFileStatsFromMeta(internal.Metadata(meta), collectors, map[string]int{"ts": 1}, nil, nil)
			_, hasAgg := got.ColAggs[1]
			assert.False(t, hasAgg, "min/max aggregator must be omitted")
			assert.Equal(t, int64(6), got.ValueCounts[1])
			assert.Equal(t, int64(3), got.NullValueCounts[1])
		})
	}
}

func TestNormalizeTimestampMillisArrayEdgeCases(t *testing.T) {
	msType := &arrow.TimestampType{Unit: arrow.Millisecond, TimeZone: "UTC"}

	t.Run("sliced with nulls", func(t *testing.T) {
		for _, offset := range []int64{3, 8} {
			t.Run(fmt.Sprintf("offset_%d", offset), func(t *testing.T) {
				alloc := memory.NewCheckedAllocator(memory.DefaultAllocator)
				defer alloc.AssertSize(t, 0)

				b := array.NewTimestampBuilder(alloc, msType)
				for i := range 20 {
					if i%3 == 0 {
						b.AppendNull()
					} else {
						b.Append(arrow.Timestamp(1_000 + int64(i)))
					}
				}
				full := b.NewArray()
				b.Release()

				sliced := array.NewSlice(full, offset, offset+9)
				full.Release()

				out, err := internal.NormalizeTimestampMillisArray(alloc, sliced)
				require.NoError(t, err)
				// Release the input before reading output to verify buffer ownership.
				sliced.Release()
				defer out.Release()

				ts := out.(*array.Timestamp)
				assert.Equal(t, arrow.Microsecond, ts.DataType().(*arrow.TimestampType).Unit)
				assert.Equal(t, "UTC", ts.DataType().(*arrow.TimestampType).TimeZone)
				require.Equal(t, 9, ts.Len())
				for i := range 9 {
					origIdx := int(offset) + i
					if origIdx%3 == 0 {
						assert.True(t, ts.IsNull(i), "index %d", i)
					} else {
						assert.False(t, ts.IsNull(i), "index %d", i)
						assert.Equal(t, arrow.Timestamp((1_000+int64(origIdx))*1000), ts.Value(i), "index %d", i)
					}
				}
			})
		}
	})

	t.Run("out-of-range garbage in null slots", func(t *testing.T) {
		alloc := memory.NewCheckedAllocator(memory.DefaultAllocator)
		defer alloc.AssertSize(t, 0)

		valBuf := memory.NewResizableBuffer(alloc)
		valBuf.Resize(arrow.TimestampTraits.BytesRequired(4))
		vals := arrow.TimestampTraits.CastFromBytes(valBuf.Bytes())
		vals[0] = math.MaxInt64
		vals[1] = 1_500
		vals[2] = math.MinInt64
		vals[3] = -2_500

		nullBuf := memory.NewResizableBuffer(alloc)
		nullBuf.Resize(int(bitutil.BytesForBits(4)))
		bitutil.SetBitTo(nullBuf.Bytes(), 0, false)
		bitutil.SetBitTo(nullBuf.Bytes(), 1, true)
		bitutil.SetBitTo(nullBuf.Bytes(), 2, false)
		bitutil.SetBitTo(nullBuf.Bytes(), 3, true)

		data := array.NewData(msType, 4, []*memory.Buffer{nullBuf, valBuf}, nil, 2, 0)
		valBuf.Release()
		nullBuf.Release()
		in := array.NewTimestampData(data)
		data.Release()

		out, err := internal.NormalizeTimestampMillisArray(alloc, in)
		require.NoError(t, err)
		in.Release()
		defer out.Release()

		ts := out.(*array.Timestamp)
		require.Equal(t, 4, ts.Len())
		assert.Equal(t, 2, ts.NullN())
		assert.True(t, ts.IsNull(0))
		assert.Equal(t, arrow.Timestamp(1_500_000), ts.Value(1))
		assert.True(t, ts.IsNull(2))
		assert.Equal(t, arrow.Timestamp(-2_500_000), ts.Value(3))
	})

	t.Run("all nulls", func(t *testing.T) {
		alloc := memory.NewCheckedAllocator(memory.DefaultAllocator)
		defer alloc.AssertSize(t, 0)

		b := array.NewTimestampBuilder(alloc, msType)
		for range 5 {
			b.AppendNull()
		}
		in := b.NewArray()
		b.Release()

		out, err := internal.NormalizeTimestampMillisArray(alloc, in)
		require.NoError(t, err)
		in.Release()
		defer out.Release()

		ts := out.(*array.Timestamp)
		assert.Equal(t, 5, ts.Len())
		assert.Equal(t, 5, ts.NullN())
		for i := range 5 {
			assert.True(t, ts.IsNull(i))
		}
	})

	t.Run("zero length", func(t *testing.T) {
		alloc := memory.NewCheckedAllocator(memory.DefaultAllocator)
		defer alloc.AssertSize(t, 0)

		b := array.NewTimestampBuilder(alloc, msType)
		in := b.NewArray()
		b.Release()

		out, err := internal.NormalizeTimestampMillisArray(alloc, in)
		require.NoError(t, err)
		in.Release()
		defer out.Release()

		ts := out.(*array.Timestamp)
		assert.Equal(t, arrow.Microsecond, ts.DataType().(*arrow.TimestampType).Unit)
		assert.Zero(t, ts.Len())
		assert.Zero(t, ts.NullN())
	})
}

func BenchmarkNormalizeTimestampMillisArray(b *testing.B) {
	alloc := memory.DefaultAllocator
	msType := &arrow.TimestampType{Unit: arrow.Millisecond, TimeZone: "UTC"}

	makeArr := func(n int, kind string) arrow.Array {
		switch kind {
		case "no_nulls":
			tb := array.NewTimestampBuilder(alloc, msType)
			defer tb.Release()
			tb.Reserve(n)
			for i := range n {
				tb.Append(arrow.Timestamp(1_700_000_000_000 + int64(i)))
			}

			return tb.NewArray()
		case "nulls_10pct":
			tb := array.NewTimestampBuilder(alloc, msType)
			defer tb.Release()
			tb.Reserve(n)
			for i := range n {
				if i%10 == 0 {
					tb.AppendNull()
				} else {
					tb.Append(arrow.Timestamp(1_700_000_000_000 + int64(i)))
				}
			}

			return tb.NewArray()
		case "sliced_offset_3":
			tb := array.NewTimestampBuilder(alloc, msType)
			defer tb.Release()
			tb.Reserve(n + 3)
			for i := range n + 3 {
				if i%10 == 0 {
					tb.AppendNull()
				} else {
					tb.Append(arrow.Timestamp(1_700_000_000_000 + int64(i)))
				}
			}
			full := tb.NewArray()
			defer full.Release()

			return array.NewSlice(full, 3, int64(n+3))
		case "all_nulls":
			tb := array.NewTimestampBuilder(alloc, msType)
			defer tb.Release()
			tb.Reserve(n)
			for range n {
				tb.AppendNull()
			}

			return tb.NewArray()
		case "dictionary":
			dictType := &arrow.DictionaryType{IndexType: arrow.PrimitiveTypes.Int32, ValueType: msType}
			vb := array.NewTimestampBuilder(alloc, msType)
			defer vb.Release()
			for i := range 64 {
				vb.Append(arrow.Timestamp(1_700_000_000_000 + int64(i)*1000))
			}
			dictVals := vb.NewArray()
			defer dictVals.Release()

			ib := array.NewInt32Builder(alloc)
			defer ib.Release()
			ib.Reserve(n)
			for i := range n {
				ib.Append(int32(i % 64))
			}
			indices := ib.NewArray()
			defer indices.Release()

			data := array.NewDataWithDictionary(
				dictType, indices.Len(), indices.Data().Buffers(), indices.NullN(), indices.Data().Offset(),
				dictVals.Data().(*array.Data),
			)
			defer data.Release()

			return array.NewDictionaryData(data)
		default:
			b.Fatalf("unknown case %q", kind)

			return nil
		}
	}

	for _, c := range []string{"no_nulls", "nulls_10pct", "sliced_offset_3", "all_nulls", "dictionary"} {
		for _, n := range []int{128, 4096, 131072} {
			arr := makeArr(n, c)
			b.Run(fmt.Sprintf("%s/%d", c, n), func(b *testing.B) {
				b.ReportAllocs()
				b.SetBytes(int64(n * 8))
				b.ResetTimer()
				for range b.N {
					out, err := internal.NormalizeTimestampMillisArray(alloc, arr)
					if err != nil {
						b.Fatal(err)
					}
					out.Release()
				}
			})
			arr.Release()
		}
	}
}
