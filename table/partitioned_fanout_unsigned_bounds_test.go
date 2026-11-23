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

package table

import (
	"context"
	"errors"
	"math"
	"path/filepath"
	"strings"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/iceberg-go"
	iceio "github.com/apache/iceberg-go/io"
	"github.com/stretchr/testify/require"
)

// Arrow's unsigned physical types can represent values that Iceberg's signed
// int and long types cannot. Partition transforms must reject those values
// rather than silently wrapping them to a different, negative partition.
func TestUnsignedArrowPartitionBounds(t *testing.T) {
	tests := []struct {
		name       string
		wideArrow  bool
		value      uint64
		sourceType iceberg.Type
		want       any
		wantError  bool
	}{
		{
			name:       "uint32 zero as int",
			value:      0,
			sourceType: iceberg.PrimitiveTypes.Int32,
			want:       int32(0),
		},
		{
			name:       "uint32 zero as long",
			value:      0,
			sourceType: iceberg.PrimitiveTypes.Int64,
			want:       int64(0),
		},
		{
			name:       "uint32 just above int maximum promoted to long",
			value:      uint64(math.MaxInt32) + 1,
			sourceType: iceberg.PrimitiveTypes.Int64,
			want:       int64(math.MaxInt32) + 1,
		},
		{
			name:       "uint64 zero as long",
			wideArrow:  true,
			value:      0,
			sourceType: iceberg.PrimitiveTypes.Int64,
			want:       int64(0),
		},
		{
			name:       "uint32 signed maximum",
			value:      math.MaxInt32,
			sourceType: iceberg.PrimitiveTypes.Int32,
			want:       int32(math.MaxInt32),
		},
		{
			name:       "uint32 above signed maximum",
			value:      uint64(math.MaxInt32) + 1,
			sourceType: iceberg.PrimitiveTypes.Int32,
			wantError:  true,
		},
		{
			name:       "uint32 maximum",
			value:      math.MaxUint32,
			sourceType: iceberg.PrimitiveTypes.Int32,
			wantError:  true,
		},
		{
			name:       "uint32 promoted to long",
			value:      math.MaxUint32,
			sourceType: iceberg.PrimitiveTypes.Int64,
			want:       int64(math.MaxUint32),
		},
		{
			name:       "uint64 signed maximum",
			wideArrow:  true,
			value:      math.MaxInt64,
			sourceType: iceberg.PrimitiveTypes.Int64,
			want:       int64(math.MaxInt64),
		},
		{
			name:       "uint64 above signed maximum",
			wideArrow:  true,
			value:      uint64(math.MaxInt64) + 1,
			sourceType: iceberg.PrimitiveTypes.Int64,
			wantError:  true,
		},
		{
			name:       "uint64 maximum",
			wideArrow:  true,
			value:      math.MaxUint64,
			sourceType: iceberg.PrimitiveTypes.Int64,
			wantError:  true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var column arrow.Array
			var arrowType arrow.DataType
			if tt.wideArrow {
				builder := array.NewUint64Builder(memory.NewGoAllocator())
				builder.Append(tt.value)
				column = builder.NewArray()
				builder.Release()
				arrowType = arrow.PrimitiveTypes.Uint64
			} else {
				builder := array.NewUint32Builder(memory.NewGoAllocator())
				builder.Append(uint32(tt.value))
				column = builder.NewArray()
				builder.Release()
				arrowType = arrow.PrimitiveTypes.Uint32
			}
			defer column.Release()

			got, err := getArrowValueAsIcebergValue(column, 0, tt.sourceType)
			if tt.wantError {
				if !errors.Is(err, iceberg.ErrInvalidArgument) {
					t.Fatalf("expected an invalid argument for %d, got %v", tt.value, err)
				}
				if got != nil {
					t.Fatalf("expected no converted value on overflow, got %v", got)
				}
			} else {
				if err != nil {
					t.Fatalf("convert %d: %v", tt.value, err)
				}
				if got != tt.want {
					t.Fatalf("converted %d as %T(%v), want %T(%v)", tt.value, got, got, tt.want, tt.want)
				}
			}

			record := array.NewRecordBatch(
				arrow.NewSchema([]arrow.Field{{Name: "id", Type: arrowType}}, nil),
				[]arrow.Array{column}, 1,
			)
			defer record.Release()

			schema := iceberg.NewSchema(0, iceberg.NestedField{
				ID: 1, Name: "id", Type: tt.sourceType,
			})
			for _, transform := range []iceberg.Transform{
				iceberg.IdentityTransform{},
				iceberg.BucketTransform{NumBuckets: 16},
			} {
				spec := iceberg.NewPartitionSpec(iceberg.PartitionField{
					SourceIDs: []int{1}, FieldID: 1000,
					Name: "id", Transform: transform,
				})
				partitions, err := getRecordPartitions(spec, schema, record)
				if tt.wantError {
					if !errors.Is(err, iceberg.ErrInvalidArgument) {
						t.Fatalf("%s: expected partition extraction to reject overflow, got %v", transform, err)
					}
					if len(partitions) != 0 {
						t.Fatalf("%s: returned partitions after overflow: %v", transform, partitions)
					}

					continue
				}

				if err != nil {
					t.Fatalf("%s: extract partition: %v", transform, err)
				}
				if len(partitions) != 1 {
					t.Fatalf("%s: got %d partitions, want 1", transform, len(partitions))
				}
				if _, ok := transform.(iceberg.IdentityTransform); ok {
					if value := partitions[0].partitionRec.Get(0); value != tt.want {
						t.Fatalf("identity partition value %T(%v), want %T(%v)", value, value, tt.want, tt.want)
					}
				}
			}
		})
	}
}

func TestWriteRecordsUnsignedPartitionBounds(t *testing.T) {
	for _, mode := range []struct {
		name string
		opts []WriteRecordOption
	}{
		{name: "fanout"},
		{name: "clustered", opts: []WriteRecordOption{WithClusteredWrite()}},
	} {
		t.Run(mode.name, func(t *testing.T) {
			for _, tc := range []struct {
				name       string
				sourceType iceberg.Type
				inputType  arrow.DataType
				rows       string
				overflow   bool
			}{
				{
					"uint32 promoted to long", iceberg.PrimitiveTypes.Int64, arrow.PrimitiveTypes.Uint32,
					`[{"id":4294967295}]`, false,
				},
				{
					"uint32 over int range", iceberg.PrimitiveTypes.Int32, arrow.PrimitiveTypes.Uint32,
					`[{"id":2147483648}]`, true,
				},
				{
					"uint64 over long range", iceberg.PrimitiveTypes.Int64, arrow.PrimitiveTypes.Uint64,
					`[{"id":9223372036854775808}]`, true,
				},
			} {
				t.Run(tc.name, func(t *testing.T) {
					ctx := t.Context()
					location := t.TempDir()
					schema := iceberg.NewSchema(0, iceberg.NestedField{ID: 1, Name: "id", Type: tc.sourceType, Required: true})
					spec := partitionedSpec()
					meta, err := NewMetadata(schema, &spec, UnsortedSortOrder, location, nil)
					require.NoError(t, err)
					tbl := New(Identifier{"test", "unsigned_partition"}, meta, filepath.Join(location, "metadata", "v1.metadata.json"),
						func(context.Context) (iceio.IO, error) { return iceio.LocalFS{}, nil }, &sequentialCatalog{metadata: meta})
					arrowSchema := arrow.NewSchema([]arrow.Field{{
						Name: "id", Type: tc.inputType, Metadata: writerFieldIDMeta("1"),
					}}, nil)
					records := func(yield func(arrow.RecordBatch, error) bool) {
						record, _, err := array.RecordFromJSON(memory.DefaultAllocator, arrowSchema, strings.NewReader(tc.rows))
						yield(record, err)
					}

					var files []iceberg.DataFile
					for file, err := range WriteRecords(ctx, tbl, arrowSchema, records, mode.opts...) {
						if tc.overflow {
							require.ErrorIs(t, err, iceberg.ErrInvalidArgument)
							require.Empty(t, files)

							return
						}
						require.NoError(t, err)
						files = append(files, file)
					}
					require.False(t, tc.overflow, "overflow must return an error")
					require.Len(t, files, 1)
					require.Equal(t, map[int]any{1000: int64(math.MaxUint32)}, files[0].Partition())

					txn := tbl.NewTransaction()
					require.NoError(t, txn.AddDataFiles(ctx, files, nil))
					tbl, err = txn.Commit(ctx)
					require.NoError(t, err)
					scanned, err := tbl.Scan(WithRowFilter(iceberg.EqualTo(iceberg.Reference("id"), int64(math.MaxUint32)))).ToArrowTable(ctx)
					require.NoError(t, err)
					defer scanned.Release()
					require.EqualValues(t, 1, scanned.NumRows())
					require.EqualValues(t, math.MaxUint32, scanned.Column(0).Data().Chunk(0).(*array.Int64).Value(0))
				})
			}
		})
	}
}

func TestUnsignedPartitionOverflowAfterValidRows(t *testing.T) {
	for _, tc := range []struct {
		name       string
		wideArrow  bool
		sourceType iceberg.Type
	}{
		{name: "uint32 exceeds int", sourceType: iceberg.PrimitiveTypes.Int32},
		{name: "uint64 exceeds long", wideArrow: true, sourceType: iceberg.PrimitiveTypes.Int64},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var column arrow.Array
			var dataType arrow.DataType
			if tc.wideArrow {
				builder := array.NewUint64Builder(memory.NewGoAllocator())
				builder.AppendValues([]uint64{0, math.MaxInt64, uint64(math.MaxInt64) + 1}, nil)
				column = builder.NewArray()
				builder.Release()
				dataType = arrow.PrimitiveTypes.Uint64
			} else {
				builder := array.NewUint32Builder(memory.NewGoAllocator())
				builder.AppendValues([]uint32{0, math.MaxInt32, uint32(math.MaxInt32) + 1}, nil)
				column = builder.NewArray()
				builder.Release()
				dataType = arrow.PrimitiveTypes.Uint32
			}
			defer column.Release()

			record := array.NewRecordBatch(
				arrow.NewSchema([]arrow.Field{{Name: "id", Type: dataType}}, nil),
				[]arrow.Array{column}, int64(column.Len()),
			)
			defer record.Release()

			schema := iceberg.NewSchema(0, iceberg.NestedField{
				ID: 1, Name: "id", Type: tc.sourceType,
			})
			for _, transform := range []iceberg.Transform{
				iceberg.IdentityTransform{},
				iceberg.BucketTransform{NumBuckets: 16},
			} {
				spec := iceberg.NewPartitionSpec(iceberg.PartitionField{
					SourceIDs: []int{1}, FieldID: 1000,
					Name: "id", Transform: transform,
				})
				partitions, err := getRecordPartitions(spec, schema, record)
				require.ErrorIs(t, err, iceberg.ErrInvalidArgument, "%s must reject a later overflowing row", transform)
				require.Empty(t, partitions, "%s must not return partial partitions", transform)
			}
		})
	}
}
