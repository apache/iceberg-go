// Licensed to the Apache Software Foundation (ASF) under one or more
// contributor license agreements.  See the NOTICE file distributed with
// this work for additional information regarding copyright ownership.
// The ASF licenses this file to you under the Apache License, Version 2.0
// (the "License"); you may not use this file except in compliance with
// the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package internal

import (
	"fmt"
	"strconv"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/iceberg-go"
	iceio "github.com/apache/iceberg-go/io"
)

func BenchmarkParquetFileWriterSchemaMapping(b *testing.B) {
	format := parquetFormat{}

	for _, fieldCount := range []int{8, 64, 256} {
		b.Run(fmt.Sprintf("fields_%d", fieldCount), func(b *testing.B) {
			fileSchema, arrowSchema := parquetSchemaBenchmarkSchemas(fieldCount)
			writeProps := format.GetWriteProperties(iceberg.Properties{})
			cachedMapping, err := format.PathToIDMapping(fileSchema)
			if err != nil {
				b.Fatal(err)
			}
			cachedVariantFieldIDs := VariantFieldIDsFromSchema(fileSchema)

			for _, cached := range []bool{false, true} {
				name := "rebuild"
				if cached {
					name = "cached"
				}

				b.Run(name, func(b *testing.B) {
					fs := iceio.NewMemFS()
					b.ReportAllocs()
					b.ResetTimer()
					for i := 0; b.Loop(); i++ {
						info := WriteFileInfo{
							FileSchema: fileSchema,
							FileName:   "file-" + strconv.Itoa(i) + ".parquet",
							WriteProps: writeProps,
						}
						if cached {
							info.ColMapping = cachedMapping
							info.VariantFieldIDs = cachedVariantFieldIDs
						}

						writer, err := format.NewFileWriter(b.Context(), fs, nil, info, arrowSchema)
						if err != nil {
							b.Fatal(err)
						}
						if err := writer.Abort(); err != nil {
							b.Fatal(err)
						}
					}
				})
			}
		})
	}
}

func parquetSchemaBenchmarkSchemas(fieldCount int) (*iceberg.Schema, *arrow.Schema) {
	icebergFields := make([]iceberg.NestedField, fieldCount)
	arrowFields := make([]arrow.Field, fieldCount)
	for i := range fieldCount {
		name := fmt.Sprintf("field_%d", i)
		icebergFields[i] = iceberg.NestedField{
			ID:       i + 1,
			Name:     name,
			Type:     iceberg.PrimitiveTypes.Int64,
			Required: true,
		}
		arrowFields[i] = arrow.Field{Name: name, Type: arrow.PrimitiveTypes.Int64, Nullable: false}
	}

	return iceberg.NewSchema(0, icebergFields...), arrow.NewSchema(arrowFields, nil)
}
