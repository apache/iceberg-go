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
	"context"
	"path/filepath"
	"strconv"
	"testing"

	"github.com/apache/iceberg-go"
	iceio "github.com/apache/iceberg-go/io"
	tblutils "github.com/apache/iceberg-go/table/internal"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// readPropertiesRowsPerBatch scans tasks and returns the row count of each
// record batch the reader emits. Batch boundaries are the observable proof of
// which batch-size setting actually reached the Parquet reader.
func readPropertiesRowsPerBatch(t *testing.T, scan *Scan, tasks []FileScanTask) []int64 {
	t.Helper()

	_, recs, err := scan.ReadTasks(context.Background(), tasks)
	require.NoError(t, err)

	var rows []int64
	for rec, err := range recs {
		require.NoError(t, err)
		rows = append(rows, rec.NumRows())
		rec.Release()
	}

	return rows
}

// buildReadPropertiesTestTable builds a single-binary-column v3 table carrying
// the supplied table properties, so a test can vary the table-level side of the
// property precedence independently of the scan-level side.
func buildReadPropertiesTestTable(t *testing.T, fs iceio.IO, location string, props iceberg.Properties) *Table {
	t.Helper()

	sc := iceberg.NewSchema(0, iceberg.NestedField{
		ID:       dvBinaryFieldID,
		Name:     "payload",
		Type:     iceberg.PrimitiveTypes.Binary,
		Required: false,
	})

	all := iceberg.Properties{PropertyFormatVersion: "3"}
	for k, v := range props {
		all[k] = v
	}

	meta, err := NewMetadata(sc, iceberg.UnpartitionedSpec, UnsortedSortOrder, location, all)
	require.NoError(t, err)

	fsF := func(_ context.Context) (iceio.IO, error) { return fs, nil }

	return New(Identifier{"default", "read_properties"}, meta, "", fsF, nil)
}

// TestScanOptionsReachParquetReader pins that read.parquet.batch-size passed as
// a scan option actually reaches the Parquet reader.
//
// It previously did not. GetRecords put only the table's metadata properties on
// the context, and (*ParquetFileSource).GetReader reads its batch size from
// exactly that context, so a per-scan override was dropped without an error and
// the reader fell back to ParquetBatchSizeDefault (131072 rows).
//
// That silence had teeth in production: a caller sizing batches down to bound
// its own memory got 131072-row batches regardless, and any per-batch bug scaled
// with the payload of a batch 8x larger than the one it had been reasoned about.
//
// The file is written as one row group so the batch boundaries observed here can
// only come from the batch-size setting, not from row-group layout.
func TestScanOptionsReachParquetReader(t *testing.T) {
	fs := iceio.LocalFS{}
	tmp := t.TempDir()

	const totalRows = 300
	payloads := make([][]byte, totalRows)
	for i := range payloads {
		payloads[i] = []byte(strconv.Itoa(i))
	}

	df := writeBinaryParquetWithFieldID(t, fs, filepath.Join(tmp, "batched.parquet"), payloads)
	tasks := []FileScanTask{{File: df}}

	t.Run("scan option bounds the batch size", func(t *testing.T) {
		tbl := buildReadPropertiesTestTable(t, fs, tmp, nil)
		rows := readPropertiesRowsPerBatch(t, tbl.Scan(WithOptions(iceberg.Properties{
			tblutils.ParquetBatchSizeKey: "100",
		})), tasks)

		assert.Equal(t, []int64{100, 100, 100}, rows,
			"a scan-option batch size must reach the parquet reader")
	})

	t.Run("scan option overrides the table property", func(t *testing.T) {
		tbl := buildReadPropertiesTestTable(t, fs, tmp, iceberg.Properties{
			tblutils.ParquetBatchSizeKey: "250",
		})
		rows := readPropertiesRowsPerBatch(t, tbl.Scan(WithOptions(iceberg.Properties{
			tblutils.ParquetBatchSizeKey: "75",
		})), tasks)

		assert.Equal(t, []int64{75, 75, 75, 75}, rows,
			"the scan option is the more specific setting and must win over the table property")
	})

	t.Run("table property still applies with no scan option", func(t *testing.T) {
		tbl := buildReadPropertiesTestTable(t, fs, tmp, iceberg.Properties{
			tblutils.ParquetBatchSizeKey: "120",
		})
		rows := readPropertiesRowsPerBatch(t, tbl.Scan(), tasks)

		assert.Equal(t, []int64{120, 120, 60}, rows,
			"the table property must keep working when no scan option overrides it")
	})

	t.Run("unset on both sides falls back to the library default", func(t *testing.T) {
		tbl := buildReadPropertiesTestTable(t, fs, tmp, nil)
		rows := readPropertiesRowsPerBatch(t, tbl.Scan(), tasks)

		assert.Equal(t, []int64{totalRows}, rows,
			"the default batch size is larger than the file, so it arrives as one batch")
	})
}

// TestReadPropertiesFor covers the precedence and aliasing contract directly,
// including the cases the scan-level test cannot reach (nil inputs, and the
// guarantee that neither caller's map is mutated).
func TestReadPropertiesFor(t *testing.T) {
	t.Run("scan options win per key, others are inherited", func(t *testing.T) {
		tableProps := iceberg.Properties{"a": "table", "b": "table"}
		scanOpts := iceberg.Properties{"a": "scan", "c": "scan"}

		got := readPropertiesFor(tableProps, scanOpts)

		assert.Equal(t, iceberg.Properties{"a": "scan", "b": "table", "c": "scan"}, got)
		assert.Equal(t, iceberg.Properties{"a": "table", "b": "table"}, tableProps,
			"the table's properties must not be mutated")
		assert.Equal(t, iceberg.Properties{"a": "scan", "c": "scan"}, scanOpts,
			"the caller's scan options must not be mutated")
	})

	t.Run("no scan options yields the table properties unchanged", func(t *testing.T) {
		tableProps := iceberg.Properties{"a": "table"}

		assert.Equal(t, tableProps, readPropertiesFor(tableProps, nil))
		assert.Equal(t, tableProps, readPropertiesFor(tableProps, iceberg.Properties{}))
	})

	t.Run("no table properties yields the scan options", func(t *testing.T) {
		scanOpts := iceberg.Properties{"a": "scan"}

		assert.Equal(t, scanOpts, readPropertiesFor(nil, scanOpts))
	})
}
