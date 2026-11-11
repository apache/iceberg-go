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
	"fmt"
	"strings"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/decimal128"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/arrow-go/v18/parquet"
	"github.com/apache/arrow-go/v18/parquet/file"
	"github.com/apache/arrow-go/v18/parquet/schema"
	"github.com/apache/iceberg-go"
	iceio "github.com/apache/iceberg-go/io"
	"github.com/apache/iceberg-go/table"
	"github.com/apache/iceberg-go/table/internal"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestParquetDecimalWriterPhysicalTypes(t *testing.T) {
	for _, tt := range []struct {
		precision int
		scale     int
		physical  parquet.Type
		byteWidth int
	}{
		{1, 0, parquet.Types.Int32, 0},
		{9, 2, parquet.Types.Int32, 0},
		{10, 2, parquet.Types.Int64, 0},
		{18, 2, parquet.Types.Int64, 0},
		{19, 2, parquet.Types.FixedLenByteArray, 9},
		{38, 2, parquet.Types.FixedLenByteArray, 16},
	} {
		t.Run(fmt.Sprintf("decimal(%d,%d)", tt.precision, tt.scale), func(t *testing.T) {
			mem := memory.NewCheckedAllocator(memory.DefaultAllocator)
			defer mem.AssertSize(t, 0)

			decimalType := iceberg.MustDecimalTypeOf(tt.precision, tt.scale)
			iceSchema := iceberg.NewSchema(0,
				iceberg.NestedField{ID: 1, Name: "value", Type: decimalType},
				iceberg.NestedField{ID: 2, Name: "nested", Type: &iceberg.StructType{FieldList: []iceberg.NestedField{
					{ID: 3, Name: "amount", Type: decimalType},
				}}},
				iceberg.NestedField{ID: 4, Name: "items", Type: iceberg.NewListType(5, decimalType, false)},
				iceberg.NestedField{ID: 6, Name: "mapping", Type: &iceberg.MapType{
					KeyID: 7, KeyType: iceberg.PrimitiveTypes.String, ValueID: 8, ValueType: decimalType,
				}},
			)
			arrowSchema, err := table.SchemaToArrowSchema(iceSchema, nil, true, false)
			require.NoError(t, err)

			maxValue := strings.Repeat("9", tt.precision)
			if tt.scale > 0 {
				maxValue = maxValue[:tt.precision-tt.scale] + "." + maxValue[tt.precision-tt.scale:]
			}
			var rows []string
			for _, value := range []string{maxValue, "-" + maxValue, "0"} {
				rows = append(rows, fmt.Sprintf(`{"value":%q,"nested":{"amount":%q},"items":[%q,null],"mapping":[{"key":"a","value":%q}]}`,
					value, value, value, value))
			}
			rows = append(rows,
				`{"value":null,"nested":{"amount":null},"items":[null],"mapping":[{"key":"a","value":null}]}`,
				`{"value":null,"nested":null,"items":[],"mapping":[]}`,
				`{"value":null,"nested":null,"items":null,"mapping":null}`)
			record, _, err := array.RecordFromJSON(mem, arrowSchema, strings.NewReader("["+strings.Join(rows, ",")+"]"))
			require.NoError(t, err)
			defer record.Release()

			format := internal.GetFileFormat(iceberg.ParquetFile)
			statsCols := make(map[int]internal.StatisticsCollector)
			for id, name := range map[int]string{
				1: "value", 3: "nested.amount", 5: "items.element", 7: "mapping.key", 8: "mapping.value",
			} {
				var typ iceberg.PrimitiveType = decimalType
				if id == 7 {
					typ = iceberg.PrimitiveTypes.String
				}
				statsCols[id] = internal.StatisticsCollector{
					FieldID: id, ColName: name, IcebergTyp: typ,
					Mode: internal.MetricsMode{Typ: internal.MetricModeFull},
				}
			}
			fs := iceio.NewMemFS()
			df, err := format.WriteDataFile(t.Context(), fs, nil, internal.WriteFileInfo{
				FileSchema: iceSchema, Spec: *iceberg.UnpartitionedSpec,
				FileName: "decimals.parquet", Content: iceberg.EntryContentData,
				WriteProps: format.GetWriteProperties(iceberg.Properties{}), StatsCols: statsCols,
			}, []arrow.RecordBatch{record})
			require.NoError(t, err)
			assert.EqualValues(t, len(rows), df.Count())

			input, err := fs.Open(t.Context(), df.FilePath())
			require.NoError(t, err)
			reader, err := file.NewParquetReader(input)
			require.NoError(t, err)
			defer reader.Close()
			for _, col := range []int{0, 1, 2, 4} {
				column := reader.MetaData().Schema.Column(col)
				assert.Equal(t, tt.physical, column.PhysicalType(), column.Path())
				assert.Equal(t, schema.NewDecimalLogicalType(int32(tt.precision), int32(tt.scale)), column.LogicalType())
				if tt.byteWidth > 0 {
					assert.EqualValues(t, tt.byteWidth, column.TypeLength())
				}
			}

			for _, id := range []int{1, 3} {
				assert.EqualValues(t, 3, df.NullValueCounts()[id])
				for _, bound := range []struct {
					encoded []byte
					value   string
				}{
					{df.LowerBoundValues()[id], "-" + maxValue},
					{df.UpperBoundValues()[id], maxValue},
				} {
					literal, err := iceberg.LiteralFromBytes(decimalType, bound.encoded)
					require.NoError(t, err)
					want, err := decimal128.FromString(bound.value, int32(tt.precision), int32(tt.scale))
					require.NoError(t, err)
					assert.Equal(t, iceberg.Decimal{Val: want, Scale: tt.scale}, literal.(iceberg.TypedLiteral[iceberg.Decimal]).Value())
				}
			}

			read, err := format.Open(t.Context(), fs, df.FilePath())
			require.NoError(t, err)
			defer read.Close()
			actual, err := read.ReadTable(t.Context())
			require.NoError(t, err)
			defer actual.Release()
			expected := array.NewTableFromRecords(arrowSchema, []arrow.RecordBatch{record})
			defer expected.Release()
			assert.True(t, array.TableEqual(expected, actual), "decimal values and nested nulls must round trip")
		})
	}
}
