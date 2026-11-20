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

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
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
