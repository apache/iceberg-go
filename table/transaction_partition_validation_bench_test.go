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

package table

import (
	"fmt"
	"testing"

	"github.com/apache/iceberg-go"
)

var partitionValidationBenchmarkSink int

func BenchmarkPartitionValidationPlans(b *testing.B) {
	const fileCount = 128

	for _, content := range []struct {
		name string
		kind string
	}{
		{name: "data", kind: "data"},
		{name: "delete", kind: "delete"},
	} {
		for _, specCount := range []int{1, 4} {
			for _, fieldCount := range []int{3, 8, 16} {
				b.Run(fmt.Sprintf("%s/specs=%d/fields=%d/files=%d", content.name, specCount, fieldCount, fileCount), func(b *testing.B) {
					txn := newPartitionValidationBenchmarkTransaction(b, specCount, fieldCount)
					dataFiles, deleteFiles := newPartitionValidationBenchmarkFiles(b, txn, content.kind, specCount, fieldCount, fileCount)

					b.ReportAllocs()
					b.ResetTimer()
					for range b.N {
						if content.kind == "data" {
							files, err := txn.validateDataFilesToAdd(dataFiles, "BenchmarkPartitionValidationPlans", false)
							if err != nil {
								b.Fatal(err)
							}
							partitionValidationBenchmarkSink = len(files)
							continue
						}

						files, err := txn.validateDeleteFilesToAdd(deleteFiles, "BenchmarkPartitionValidationPlans")
						if err != nil {
							b.Fatal(err)
						}
						partitionValidationBenchmarkSink = len(files.paths)
					}
				})
			}
		}
	}
}

func newPartitionValidationBenchmarkTransaction(b *testing.B, specCount, fieldCount int) *Transaction {
	b.Helper()

	schemaFields := make([]iceberg.NestedField, fieldCount)
	partitionFields := make([]iceberg.PartitionField, fieldCount)
	for i := range fieldCount {
		fieldID := i + 1
		fieldName := fmt.Sprintf("field_%d", i)
		schemaFields[i] = iceberg.NestedField{
			ID:       fieldID,
			Name:     fieldName,
			Type:     iceberg.PrimitiveTypes.Int32,
			Required: true,
		}
		partitionFields[i] = iceberg.PartitionField{
			SourceIDs: []int{fieldID},
			FieldID:   1000 + i,
			Name:      fieldName,
			Transform: iceberg.IdentityTransform{},
		}
	}

	schema := iceberg.NewSchema(0, schemaFields...)
	specs := make([]iceberg.PartitionSpec, specCount)
	for i := range specs {
		specs[i] = iceberg.NewPartitionSpecID(i, partitionFields...)
	}

	builder, err := NewMetadataBuilder(2)
	if err != nil {
		b.Fatal(err)
	}
	if err = builder.SetLoc("mem://partition-validation-benchmark"); err != nil {
		b.Fatal(err)
	}
	if err = builder.AddSchema(schema); err != nil {
		b.Fatal(err)
	}
	if err = builder.SetCurrentSchemaID(-1); err != nil {
		b.Fatal(err)
	}
	if err = builder.AddSortOrder(&UnsortedSortOrder); err != nil {
		b.Fatal(err)
	}
	if err = builder.SetDefaultSortOrderID(-1); err != nil {
		b.Fatal(err)
	}
	if err = builder.AddPartitionSpec(&specs[0], true); err != nil {
		b.Fatal(err)
	}
	for i := 1; i < len(specs); i++ {
		if err = builder.AddPartitionSpec(&specs[i], false); err != nil {
			b.Fatal(err)
		}
	}
	if err = builder.SetDefaultSpecID(-1); err != nil {
		b.Fatal(err)
	}
	meta, err := builder.Build()
	if err != nil {
		b.Fatal(err)
	}

	return New(Identifier{"db", "partition-validation-benchmark"}, meta, "metadata.json", nil, nil).NewTransaction()
}

func newPartitionValidationBenchmarkFiles(
	b *testing.B,
	txn *Transaction,
	kind string,
	specCount, fieldCount, fileCount int,
) ([]iceberg.DataFile, []rewriteDeleteFileAddition) {
	b.Helper()

	specs := make([]iceberg.PartitionSpec, specCount)
	for i := range specs {
		spec, err := txn.meta.GetSpecByID(i)
		if err != nil {
			b.Fatal(err)
		}
		specs[i] = *spec
	}

	dataFiles := make([]iceberg.DataFile, 0, fileCount)
	deleteFiles := make([]rewriteDeleteFileAddition, 0, fileCount)
	for i := range fileCount {
		specID := i % specCount
		partition := make(map[int]any, fieldCount)
		for field := range fieldCount {
			partition[1000+field] = int32(i + field)
		}

		content := iceberg.EntryContentData
		if kind == "delete" {
			content = iceberg.EntryContentPosDeletes
		}
		file, err := iceberg.NewDataFileBuilder(
			specs[specID], content,
			fmt.Sprintf("mem://partition-validation-benchmark/%s-%d.parquet", kind, i),
			iceberg.ParquetFile, partition, nil, nil, 1, 1,
		)
		if err != nil {
			b.Fatal(err)
		}
		dataFile := file.Build()
		if kind == "data" {
			dataFiles = append(dataFiles, dataFile)
		} else {
			deleteFiles = append(deleteFiles, rewriteDeleteFileAddition{file: dataFile})
		}
	}

	return dataFiles, deleteFiles
}
