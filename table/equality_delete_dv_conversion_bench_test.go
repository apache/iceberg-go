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

package table_test

import (
	"context"
	"fmt"
	"strings"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/iceberg-go"
	"github.com/apache/iceberg-go/table"
)

func BenchmarkEqualityDeleteDVConversionRead(b *testing.B) {
	const rowCount = 10000
	const deleteEvery = 10

	before, after := equalityDeleteDVBenchmarkTables(b, rowCount, deleteEvery)
	for _, benchmark := range []struct {
		name  string
		table *table.Table
	}{
		{name: "equality_deletes", table: before},
		{name: "deletion_vectors", table: after},
	} {
		b.Run(benchmark.name, func(b *testing.B) {
			b.ReportAllocs()
			b.ReportMetric(float64(rowCount-rowCount/deleteEvery), "rows/scan")
			b.ResetTimer()
			for range b.N {
				var rows int64
				_, records, err := benchmark.table.Scan().ToArrowRecords(context.Background())
				if err != nil {
					b.Fatal(err)
				}
				for record, err := range records {
					if err != nil {
						b.Fatal(err)
					}
					rows += record.NumRows()
					record.Release()
				}
				if want := int64(rowCount - rowCount/deleteEvery); rows != want {
					b.Fatalf("scan returned %d rows, want %d", rows, want)
				}
			}
		})
	}
}

func equalityDeleteDVBenchmarkTables(b *testing.B, rowCount, deleteEvery int) (*table.Table, *table.Table) {
	b.Helper()
	ctx := context.Background()
	tbl := newMergeOnReadTestTableVersion(b, "3")
	propertiesTx := tbl.NewTransaction()
	if err := propertiesTx.SetProperties(iceberg.Properties{table.ParquetRowGroupLimitKey: "100000"}); err != nil {
		b.Fatal(err)
	}
	tbl, err := propertiesTx.Commit(ctx)
	if err != nil {
		b.Fatal(err)
	}
	dataSchema, err := table.SchemaToArrowSchema(tbl.Metadata().CurrentSchema(), nil, false, false)
	if err != nil {
		b.Fatal(err)
	}
	rows := strings.Builder{}
	rows.WriteByte('[')
	for id := 1; id <= rowCount; id++ {
		if id > 1 {
			rows.WriteByte(',')
		}
		fmt.Fprintf(&rows, `{"id":%d,"data":"row-%d"}`, id, id)
	}
	rows.WriteByte(']')
	data, err := array.TableFromJSON(memory.DefaultAllocator, dataSchema, []string{rows.String()})
	if err != nil {
		b.Fatal(err)
	}
	reader := array.NewTableReader(data, -1)
	tbl, err = tbl.Append(ctx, reader, nil)
	reader.Release()
	data.Release()
	if err != nil {
		b.Fatal(err)
	}
	deleteSchema, err := table.SchemaToArrowSchema(
		iceberg.NewSchema(0, iceberg.NestedField{
			ID: 1, Name: "id", Type: iceberg.PrimitiveTypes.Int64, Required: true,
		}), nil, true, false)
	if err != nil {
		b.Fatal(err)
	}
	deleteRows := strings.Builder{}
	deleteRows.WriteByte('[')
	deleteCount := 0
	for id := deleteEvery; id <= rowCount; id += deleteEvery {
		if deleteCount > 0 {
			deleteRows.WriteByte(',')
		}
		fmt.Fprintf(&deleteRows, `{"id":%d}`, id)
		deleteCount++
	}
	deleteRows.WriteByte(']')
	deleteRecord, _, err := array.RecordFromJSON(memory.DefaultAllocator, deleteSchema, strings.NewReader(deleteRows.String()))
	if err != nil {
		b.Fatal(err)
	}
	tx := tbl.NewTransaction()
	deleteFiles, err := tx.WriteEqualityDeletes(ctx, []int{1}, func(yield func(arrow.RecordBatch, error) bool) {
		yield(deleteRecord, nil)
	})
	deleteRecord.Release()
	if err != nil {
		b.Fatal(err)
	}
	if err := tx.NewRowDelta(nil).AddDeletes(deleteFiles...).Commit(ctx); err != nil {
		b.Fatal(err)
	}
	tbl, err = tx.Commit(ctx)
	if err != nil {
		b.Fatal(err)
	}
	before := tbl
	result, err := tbl.ConvertEqualityDeletesToDeletionVectors(ctx)
	if err != nil {
		b.Fatal(err)
	}
	if result.AddedDeletionVectors != 1 {
		b.Fatalf("conversion wrote %d vectors, want 1", result.AddedDeletionVectors)
	}

	return before, result.Table
}
