// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package table

import (
	"fmt"
	"strings"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/parquet"
	"github.com/apache/arrow-go/v18/parquet/pqarrow"
	"github.com/apache/iceberg-go"
	iceio "github.com/apache/iceberg-go/io"
)

func benchmarkScanSchema(fieldCount int) *iceberg.Schema {
	fields := make([]iceberg.NestedField, fieldCount)
	for i := range fields {
		baseID := i*3 + 1
		fields[i] = iceberg.NestedField{
			ID:   baseID,
			Name: fmt.Sprintf("field_%d", i),
			Type: &iceberg.StructType{FieldList: []iceberg.NestedField{
				{ID: baseID + 1, Name: "value", Type: iceberg.PrimitiveTypes.String},
				{ID: baseID + 2, Name: "count", Type: iceberg.PrimitiveTypes.Int64},
			}},
		}
	}

	return iceberg.NewSchema(1, fields...)
}

func benchmarkScanMetadata(b *testing.B, fieldCount, propertyCount int) Metadata {
	b.Helper()

	props := make(iceberg.Properties, propertyCount)
	for i := range propertyCount {
		props[fmt.Sprintf("property_%d", i)] = fmt.Sprintf("value_%d", i)
	}

	metadata, err := NewMetadata(
		benchmarkScanSchema(fieldCount),
		iceberg.UnpartitionedSpec,
		UnsortedSortOrder,
		"mem://benchmark/table",
		props,
	)
	if err != nil {
		b.Fatal(err)
	}

	return metadata
}

func benchmarkScanJSON(rowCount, fieldCount int) string {
	var result strings.Builder
	result.WriteByte('[')
	for row := range rowCount {
		if row > 0 {
			result.WriteByte(',')
		}
		result.WriteByte('{')
		for field := range fieldCount {
			if field > 0 {
				result.WriteByte(',')
			}
			fmt.Fprintf(&result, `"field_%d":{"value":"value_%d","count":%d}`,
				field, field, row)
		}
		result.WriteByte('}')
	}
	result.WriteByte(']')

	return result.String()
}

func BenchmarkArrowScanManyFilesAndBatches(b *testing.B) {
	const (
		fieldCount = 16
		fileCount  = 32
		batchCount = 32
	)

	for _, propertyCount := range []int{0, 16, 64} {
		b.Run(fmt.Sprintf("extra_properties_%d", propertyCount), func(b *testing.B) {
			metadata := benchmarkScanMetadata(b, fieldCount, propertyCount)
			properties := metadata.Properties()
			properties[ParquetBatchSizeKey] = "1"
			metadata, err := NewMetadata(
				metadata.CurrentSchema(),
				iceberg.UnpartitionedSpec,
				UnsortedSortOrder,
				"mem://benchmark/table",
				properties,
			)
			if err != nil {
				b.Fatal(err)
			}

			projectedSchema := metadata.CurrentSchema()
			arrowSchema, err := SchemaToArrowSchemaWithOptions(projectedSchema, ArrowSchemaOptions{IncludeFieldIDs: true})
			if err != nil {
				b.Fatal(err)
			}
			record := mustLoadRecordBatchFromJSON(arrowSchema, benchmarkScanJSON(batchCount, fieldCount))
			defer record.Release()
			table := array.NewTableFromRecords(arrowSchema, []arrow.RecordBatch{record})
			defer table.Release()

			const dataPath = "mem://benchmark/table/data.parquet"
			fs := iceio.NewMemFS()
			writer, err := fs.Create(dataPath)
			if err != nil {
				b.Fatal(err)
			}
			if err := pqarrow.WriteTable(table, writer, record.NumRows(),
				parquet.NewWriterProperties(parquet.WithStats(true)),
				pqarrow.DefaultWriterProps()); err != nil {
				b.Fatal(err)
			}
			if err := writer.Close(); err != nil {
				b.Fatal(err)
			}

			dataFile, err := iceberg.NewDataFileBuilder(
				*iceberg.UnpartitionedSpec,
				iceberg.EntryContentData,
				dataPath,
				iceberg.ParquetFile,
				nil,
				nil,
				nil,
				record.NumRows(),
				1,
			)
			if err != nil {
				b.Fatal(err)
			}
			tasks := make([]FileScanTask, fileCount)
			for i := range tasks {
				tasks[i].File = dataFile.Build()
			}

			b.ReportAllocs()
			b.ResetTimer()
			for b.Loop() {
				scan := &arrowScan{
					metadata:        metadata,
					fs:              fs,
					projectedSchema: projectedSchema,
					boundRowFilter:  iceberg.AlwaysTrue{},
					rowLimit:        -1,
					concurrency:     4,
				}
				_, records, err := scan.GetRecords(b.Context(), tasks)
				if err != nil {
					b.Fatal(err)
				}
				batches := 0
				for record, err := range records {
					if err != nil {
						b.Fatal(err)
					}
					record.Release()
					batches++
				}
				if batches != fileCount*batchCount {
					b.Fatalf("unexpected batch count: %d", batches)
				}
			}
			b.ReportMetric(fileCount, "files/op")
			b.ReportMetric(fileCount*batchCount, "batches/op")
		})
	}
}

func benchmarkComplexBoundFilter(b *testing.B, schema *iceberg.Schema) iceberg.BooleanExpression {
	b.Helper()

	filter := iceberg.NewAnd(
		iceberg.EqualTo(iceberg.Reference("field_0.value"), "value_0"),
		iceberg.NewOr(
			iceberg.GreaterThan(iceberg.Reference("field_1.count"), int64(10)),
			iceberg.LessThan(iceberg.Reference("field_2.count"), int64(20)),
		),
		iceberg.NewNot(iceberg.IsNull(iceberg.Reference("field_3.value"))),
		iceberg.IsIn(iceberg.Reference("field_4.count"), int64(1), int64(2), int64(3)),
		iceberg.NotEqualTo(iceberg.Reference("field_5.value"), "value_5"),
	)

	bound, err := iceberg.BindExpr(schema, filter, true)
	if err != nil {
		b.Fatal(err)
	}

	return bound
}

var benchmarkCompiledFilterPlansSink *compiledFileFilterPlans

func BenchmarkArrowScanFilterPlanSetup(b *testing.B) {
	schema := benchmarkScanSchema(8)
	filter := benchmarkComplexBoundFilter(b, schema)

	b.Run("recompile_each_file", func(b *testing.B) {
		b.ReportAllocs()
		b.ResetTimer()
		for b.Loop() {
			recordPlan, err := compileFileFilterPlan(schema, filter, true, true, false)
			if err != nil {
				b.Fatal(err)
			}
			pruningPlan, err := compileFileFilterPlan(schema, filter, true, false, true)
			if err != nil {
				b.Fatal(err)
			}
			benchmarkCompiledFilterPlansSink = &compiledFileFilterPlans{
				record:  recordPlan,
				pruning: pruningPlan,
			}
		}
	})

	b.Run("cache_hit", func(b *testing.B) {
		scan := &arrowScan{
			boundRowFilter:  filter,
			rowGroupFilter:  filter,
			filterSchema:    schema,
			projectedSchema: schema,
			caseSensitive:   true,
		}
		if _, err := scan.cachedFileFilterPlans(schema, true); err != nil {
			b.Fatal(err)
		}

		b.ReportAllocs()
		b.ResetTimer()
		for b.Loop() {
			plans, err := scan.cachedFileFilterPlans(schema, true)
			if err != nil {
				b.Fatal(err)
			}
			benchmarkCompiledFilterPlansSink = plans
		}
	})
}

func BenchmarkArrowScanAddTaskProjectedFieldIDs(b *testing.B) {
	schema := benchmarkScanSchema(8)
	filter := benchmarkComplexBoundFilter(b, schema)
	baseIDs, err := iceberg.ExtractFieldIDs(filter)
	if err != nil {
		b.Fatal(err)
	}

	for _, taskCount := range []int{1_000, 10_000, 100_000} {
		b.Run(fmt.Sprintf("tasks=%d/residual=nil", taskCount), func(b *testing.B) {
			tasks := make([]FileScanTask, taskCount)
			scanner := &arrowScan{
				scanSchema:     schema,
				boundRowFilter: filter,
			}
			invariants := &arrowScanInvariants{projectedIDs: make(set[int], len(baseIDs))}
			for _, id := range baseIDs {
				invariants.projectedIDs[id] = struct{}{}
			}

			b.ReportAllocs()
			b.ResetTimer()
			for b.Loop() {
				if err := scanner.addTaskProjectedFieldIDs(invariants, tasks); err != nil {
					b.Fatal(err)
				}
			}

			b.StopTimer()
			if len(invariants.projectedIDs) != len(baseIDs) {
				b.Fatalf("projected field IDs changed: got %d, want %d", len(invariants.projectedIDs), len(baseIDs))
			}
			b.ReportMetric(float64(taskCount), "tasks/op")
		})
	}
}

var benchmarkTaskResidualRows int64

func BenchmarkArrowScanTaskResidual(b *testing.B) {
	const rowCount = 32_768

	schema := iceberg.NewSchema(1,
		iceberg.NestedField{ID: 1, Name: "tenant_id", Type: iceberg.PrimitiveTypes.String},
		iceberg.NestedField{ID: 2, Name: "amount", Type: iceberg.PrimitiveTypes.Int64},
		iceberg.NestedField{ID: 3, Name: "payload", Type: iceberg.PrimitiveTypes.String},
	)
	projectedSchema, err := schema.Select(true, "payload")
	if err != nil {
		b.Fatal(err)
	}
	metadata, err := NewMetadata(
		schema, iceberg.UnpartitionedSpec, UnsortedSortOrder, "mem://benchmark/identity-residual", nil,
	)
	if err != nil {
		b.Fatal(err)
	}

	arrowSchema, err := SchemaToArrowSchema(schema, nil, true, false)
	if err != nil {
		b.Fatal(err)
	}
	record := mustLoadRecordBatchFromJSON(arrowSchema, benchmarkTaskResidualJSON(rowCount))
	defer record.Release()
	arrowTable := array.NewTableFromRecords(arrowSchema, []arrow.RecordBatch{record})
	defer arrowTable.Release()

	dataPath := "mem://benchmark/identity-residual/data.parquet"
	fs := iceio.NewMemFS()
	writer, err := fs.Create(dataPath)
	if err != nil {
		b.Fatal(err)
	}
	if err := pqarrow.WriteTable(arrowTable, writer, record.NumRows(),
		parquet.NewWriterProperties(parquet.WithStats(true)), pqarrow.DefaultWriterProps()); err != nil {
		b.Fatal(err)
	}
	if err := writer.Close(); err != nil {
		b.Fatal(err)
	}
	file, err := fs.Open(dataPath)
	if err != nil {
		b.Fatal(err)
	}
	fileInfo, err := file.Stat()
	if closeErr := file.Close(); err != nil {
		b.Fatal(err)
	} else if closeErr != nil {
		b.Fatal(closeErr)
	}

	dataFileBuilder, err := iceberg.NewDataFileBuilder(
		*iceberg.UnpartitionedSpec,
		iceberg.EntryContentData,
		dataPath,
		iceberg.ParquetFile,
		nil,
		nil,
		nil,
		rowCount,
		fileInfo.Size(),
	)
	if err != nil {
		b.Fatal(err)
	}
	task := FileScanTask{
		File:   dataFileBuilder.Build(),
		Start:  0,
		Length: fileInfo.Size(),
	}
	tasks := []FileScanTask{task}

	identityFilter := iceberg.EqualTo(iceberg.Reference("tenant_id"), "acme")
	mixedFilter := iceberg.NewAnd(
		identityFilter,
		iceberg.GreaterThan(iceberg.Reference("amount"), int64(100)),
	)
	mixedResidual := iceberg.GreaterThan(iceberg.Reference("amount"), int64(100))
	for _, tc := range []struct {
		name         string
		filter       iceberg.BooleanExpression
		residual     iceberg.BooleanExpression
		expectedRows int64
	}{
		{name: "identity_only/original_filter", filter: identityFilter, expectedRows: rowCount},
		{name: "identity_only/always_true_residual", filter: identityFilter, residual: iceberg.AlwaysTrue{}, expectedRows: rowCount},
		{name: "mixed/original_filter", filter: mixedFilter, expectedRows: rowCount / 2},
		{name: "mixed/amount_residual", filter: mixedFilter, residual: mixedResidual, expectedRows: rowCount / 2},
	} {
		boundFilter, err := iceberg.BindExpr(schema, tc.filter, true)
		if err != nil {
			b.Fatal(err)
		}

		b.Run(tc.name, func(b *testing.B) {
			tasks[0].Residual = tc.residual
			b.ReportAllocs()
			b.ReportMetric(float64(rowCount), "rows/op")
			b.ResetTimer()
			for b.Loop() {
				scan := &arrowScan{
					metadata:        metadata,
					fs:              fs,
					scanSchema:      schema,
					projectedSchema: projectedSchema,
					boundRowFilter:  boundFilter,
					caseSensitive:   true,
					rowLimit:        -1,
					concurrency:     1,
				}
				_, records, err := scan.GetRecords(b.Context(), tasks)
				if err != nil {
					b.Fatal(err)
				}

				var rows int64
				for record, err := range records {
					if err != nil {
						b.Fatal(err)
					}
					rows += int64(record.NumRows())
					record.Release()
				}
				if rows != tc.expectedRows {
					b.Fatalf("unexpected row count: got %d, want %d", rows, tc.expectedRows)
				}
				benchmarkTaskResidualRows = rows
			}
		})
	}
}

func benchmarkTaskResidualJSON(rowCount int) string {
	var result strings.Builder
	result.Grow(rowCount * 64)
	result.WriteByte('[')
	for i := range rowCount {
		if i > 0 {
			result.WriteByte(',')
		}
		amount := 50
		if i%2 != 0 {
			amount = 150
		}
		fmt.Fprintf(&result, `{"tenant_id":"acme","amount":%d,"payload":"payload-%d"}`,
			amount, i)
	}
	result.WriteByte(']')

	return result.String()
}
