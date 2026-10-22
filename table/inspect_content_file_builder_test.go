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
	"testing"

	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/iceberg-go"
	"github.com/stretchr/testify/require"
)

// The cached appender must bind by field ID again when the record builder
// changes, even when the next batch presents the fields in a different order.
func TestInspectContentFileAppenderRebindsAcrossRecordBuilders(t *testing.T) {
	partitionType := &iceberg.StructType{}
	fields := inspectContentFileFields(partitionType)
	reordered := append(append([]iceberg.NestedField(nil), fields[1:]...), fields[0])
	file := newTestDataFile(t, *iceberg.UnpartitionedSpec,
		"mem://default/table-location/data/rebound.parquet", nil)
	appendFile := newInspectContentFileAppender(partitionType)

	for _, tc := range []struct {
		name   string
		schema *iceberg.Schema
	}{
		{name: "normal", schema: iceberg.NewSchema(0, fields...)},
		{name: "reordered", schema: iceberg.NewSchema(0, reordered...)},
	} {
		t.Run(tc.name, func(t *testing.T) {
			arrowSchema, err := SchemaToArrowSchema(tc.schema, nil, true, false)
			require.NoError(t, err)

			builder := array.NewRecordBuilder(memory.DefaultAllocator, arrowSchema)
			defer builder.Release()
			require.NoError(t, appendFile(builder, file))
			require.NoError(t, appendFile(builder, file))

			record := builder.NewRecordBatch()
			defer record.Release()
			require.EqualValues(t, 2, record.NumRows())

			pathIdx := record.Schema().FieldIndices("file_path")[0]
			countIdx := record.Schema().FieldIndices("record_count")[0]
			for row := range 2 {
				require.Equal(t, file.FilePath(), record.Column(pathIdx).(*array.String).Value(row))
				require.Equal(t, file.Count(), record.Column(countIdx).(*array.Int64).Value(row))
			}
		})
	}
}
