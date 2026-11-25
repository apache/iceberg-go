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

package internal_test

import (
	"bytes"
	"strings"
	"testing"

	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/arrow-go/v18/parquet"
	"github.com/apache/arrow-go/v18/parquet/file"
	"github.com/apache/arrow-go/v18/parquet/pqarrow"
	"github.com/apache/iceberg-go"
	"github.com/apache/iceberg-go/table"
	"github.com/apache/iceberg-go/table/internal"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// An empty string or binary value serializes to zero bytes. It is still a
// bound and must be written, as Iceberg Java does.
func TestEmptyStringAndBinaryBounds(t *testing.T) {
	const maxRune = "\U0010FFFF"

	truncate16 := internal.MetricsMode{Typ: internal.MetricModeTruncate, Len: 16}

	tests := []struct {
		name string
		mode internal.MetricsMode
		// rows is a JSON array of {"s": ..., "b": ...} objects; b is base64.
		rows string
		// Field 1 is s (string), field 2 is b (binary). A missing key means no bound.
		lower, upper map[int][]byte
	}{
		{
			name:  "all empty",
			mode:  truncate16,
			rows:  `[{"s": "", "b": ""}, {"s": "", "b": ""}]`,
			lower: map[int][]byte{1: {}, 2: {}},
			upper: map[int][]byte{1: {}, 2: {}},
		},
		{
			name:  "empty minimum",
			mode:  truncate16,
			rows:  `[{"s": "", "b": ""}, {"s": "z", "b": "eg=="}]`,
			lower: map[int][]byte{1: {}, 2: {}},
			upper: map[int][]byte{1: []byte("z"), 2: []byte("z")},
		},
		{
			name:  "all empty, full mode",
			mode:  internal.MetricsMode{Typ: internal.MetricModeFull},
			rows:  `[{"s": "", "b": ""}]`,
			lower: map[int][]byte{1: {}, 2: {}},
			upper: map[int][]byte{1: {}, 2: {}},
		},
		{
			// A truncated maximum with no greater prefix still has no upper bound.
			name:  "no upper bound after truncation",
			mode:  internal.MetricsMode{Typ: internal.MetricModeTruncate, Len: 1},
			rows:  `[{"s": "", "b": ""}, {"s": "` + maxRune + maxRune + `", "b": "//8="}]`,
			lower: map[int][]byte{1: {}, 2: {}},
			upper: nil,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			df := emptyBoundsDataFile(t, tt.mode, tt.rows)

			assert.Equal(t, tt.lower, df.LowerBoundValues())
			if tt.upper == nil {
				assert.Empty(t, df.UpperBoundValues())
			} else {
				assert.Equal(t, tt.upper, df.UpperBoundValues())
			}
		})
	}
}

func emptyBoundsDataFile(t *testing.T, mode internal.MetricsMode, rows string) iceberg.DataFile {
	t.Helper()

	sc := iceberg.NewSchema(0,
		iceberg.NestedField{ID: 1, Name: "s", Type: iceberg.PrimitiveTypes.String},
		iceberg.NestedField{ID: 2, Name: "b", Type: iceberg.PrimitiveTypes.Binary},
	)
	arrowSchema, err := table.SchemaToArrowSchema(sc, nil, true, false)
	require.NoError(t, err)

	rec, _, err := array.RecordFromJSON(memory.DefaultAllocator, arrowSchema, strings.NewReader(rows))
	require.NoError(t, err)
	defer rec.Release()

	var buf bytes.Buffer
	wr, err := pqarrow.NewFileWriter(arrowSchema, &buf,
		parquet.NewWriterProperties(parquet.WithStats(true)), pqarrow.DefaultWriterProps())
	require.NoError(t, err)
	require.NoError(t, wr.Write(rec))
	require.NoError(t, wr.Close())

	rdr, err := file.NewParquetReader(bytes.NewReader(buf.Bytes()))
	require.NoError(t, err)
	defer rdr.Close()

	format := internal.GetFileFormat(iceberg.ParquetFile)
	mapping, err := format.PathToIDMapping(sc)
	require.NoError(t, err)

	collector := map[int]internal.StatisticsCollector{
		1: {FieldID: 1, Mode: mode, ColName: "s", IcebergTyp: iceberg.PrimitiveTypes.String},
		2: {FieldID: 2, Mode: mode, ColName: "b", IcebergTyp: iceberg.PrimitiveTypes.Binary},
	}
	stats := format.DataFileStatsFromMeta(internal.Metadata(rdr.MetaData()), collector, mapping, nil, nil)

	return stats.ToDataFile(internal.DataFileOpts{
		Schema:   sc,
		Spec:     *iceberg.UnpartitionedSpec,
		Path:     "empty-bounds.parquet",
		Format:   iceberg.ParquetFile,
		Content:  iceberg.EntryContentData,
		FileSize: int64(buf.Len()),
	})
}
