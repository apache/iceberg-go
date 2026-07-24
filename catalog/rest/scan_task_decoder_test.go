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

package rest

import (
	"encoding/json"
	"testing"

	"github.com/apache/iceberg-go"
	"github.com/apache/iceberg-go/table"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestDecodeScanTasksFullPayload(t *testing.T) {
	t.Parallel()

	metadata := newScanTaskDecoderMetadata()
	var wire ScanTasks
	require.NoError(t, json.Unmarshal([]byte(`{
		"file-scan-tasks": [{
			"data-file": {
				"spec-id": 7,
				"partition": [34, "2026-07-17", "78797A21"],
				"content": "data",
				"file-path": "s3://bucket/table/data.parquet",
				"file-format": "parquet",
				"file-size-in-bytes": 4096,
				"record-count": 100,
				"key-metadata": "0A0B",
				"split-offsets": [4, 128],
				"sort-order-id": 3,
				"first-row-id": 99,
				"column-sizes": {"keys": [1, 2], "values": [800, 1200]},
				"value-counts": {"keys": [1, 2], "values": [100, 100]},
				"null-value-counts": {"keys": [1, 2], "values": [0, 1]},
				"nan-value-counts": {"keys": [7], "values": [2]},
				"lower-bounds": {
					"keys": [8, 9],
					"values": ["01000000", "02000000"]
				},
				"upper-bounds": {
					"keys": [8, 9],
					"values": ["05000000", "0A000000"]
				}
			},
			"delete-file-references": [0, 1, 2],
			"residual-filter": {"type": "eq", "term": "id", "value": 34}
		}],
		"delete-files": [
			{
				"spec-id": 7,
				"partition": [34, "2026-07-17", "78797A21"],
				"content": "position-deletes",
				"file-path": "s3://bucket/table/pos-delete.parquet",
				"file-format": "parquet",
				"file-size-in-bytes": 512,
				"record-count": 5
			},
			{
				"spec-id": 7,
				"partition": [34, "2026-07-17", "78797A21"],
				"content": "equality-deletes",
				"file-path": "s3://bucket/table/eq-delete.parquet",
				"file-format": "parquet",
				"file-size-in-bytes": 256,
				"record-count": 3,
				"equality-ids": [1, 2]
			},
			{
				"spec-id": 7,
				"partition": [34, "2026-07-17", "78797A21"],
				"content": "position-deletes",
				"file-path": "s3://bucket/table/deletes.puffin",
				"file-format": "puffin",
				"file-size-in-bytes": 1024,
				"record-count": 7,
				"referenced-data-file": "s3://bucket/table/data.parquet",
				"content-offset": 25,
				"content-size-in-bytes": 50
			}
		]
	}`), &wire))

	tasks, err := DecodeScanTasks(wire, metadata, metadata.schema, iceberg.AlwaysTrue{})
	require.NoError(t, err)
	require.Len(t, tasks, 1)

	task := tasks[0]
	assert.Equal(t, "s3://bucket/table/data.parquet", task.File.FilePath())
	assert.Equal(t, int64(4096), task.Length)
	assert.Zero(t, task.Start)
	assert.Equal(t, map[int]any{
		1000: int64(34),
		1001: iceberg.Date(20651),
		1002: []byte("xyz!"),
	}, task.File.Partition())
	assert.Equal(t, []byte{0x0a, 0x0b}, task.File.KeyMetadata())
	assert.Equal(t, []int64{4, 128}, task.File.SplitOffsets())
	assert.Equal(t, intPtr(3), task.File.SortOrderID())
	assert.Equal(t, int64Ptr(99), task.FirstRowID)
	assert.Equal(t, map[int]int64{1: 800, 2: 1200}, task.File.ColumnSizes())
	assert.Equal(t, map[int]int64{1: 100, 2: 100}, task.File.ValueCounts())
	assert.Equal(t, map[int]int64{1: 0, 2: 1}, task.File.NullValueCounts())
	assert.Equal(t, map[int]int64{7: 2}, task.File.NaNValueCounts())

	// These vectors mirror Java TestContentFileParser: bounds are hexadecimal
	// encodings of raw Iceberg binary values, not typed JSON single-values.
	assert.Equal(t, []byte{1, 0, 0, 0}, task.File.LowerBoundValues()[8])
	assert.Equal(t, []byte{2, 0, 0, 0}, task.File.LowerBoundValues()[9])
	assert.Equal(t, []byte{5, 0, 0, 0}, task.File.UpperBoundValues()[8])
	assert.Equal(t, []byte{10, 0, 0, 0}, task.File.UpperBoundValues()[9])

	require.NotNil(t, task.Residual)
	assert.True(t, task.Residual.Equals(iceberg.EqualTo(iceberg.Reference("id"), int64(34))))
	require.Len(t, task.DeleteFiles, 1)
	assert.Equal(t, "s3://bucket/table/pos-delete.parquet", task.DeleteFiles[0].FilePath())
	require.Len(t, task.EqualityDeleteFiles, 1)
	assert.Equal(t, []int{1, 2}, task.EqualityDeleteFiles[0].EqualityFieldIDs())
	require.Len(t, task.DeletionVectorFiles, 1)
	dv := task.DeletionVectorFiles[0]
	assert.Equal(t, iceberg.PuffinFile, dv.FileFormat())
	assert.Equal(t, stringPtr("s3://bucket/table/data.parquet"), dv.ReferencedDataFile())
	assert.Equal(t, int64Ptr(25), dv.ContentOffset())
	assert.Equal(t, int64Ptr(50), dv.ContentSizeInBytes())
}

func TestDecodeScanTasksUsesFallbackAndAcceptsSpecConstants(t *testing.T) {
	t.Parallel()

	metadata := newScanTaskDecoderMetadata()
	fallback := iceberg.GreaterThan(iceberg.Reference("id"), int64(10))
	wire := validScanTasksWire()

	tasks, err := DecodeScanTasks(wire, metadata, metadata.schema, fallback)
	require.NoError(t, err)
	require.Len(t, tasks, 1)
	assert.Same(t, fallback, tasks[0].Residual)

	wire.FileScanTasks[0].ResidualFilter = json.RawMessage(`{"type":"false"}`)
	tasks, err = DecodeScanTasks(wire, metadata, metadata.schema, fallback)
	require.NoError(t, err)
	assert.True(t, tasks[0].Residual.Equals(iceberg.AlwaysFalse{}))
}

func TestDecodeScanTasksKeepsDeleteReferencesEnvelopeLocal(t *testing.T) {
	t.Parallel()

	metadata := newScanTaskDecoderMetadata()
	first := validScanTasksWire()
	first.FileScanTasks[0].DeleteFileReferences = []int{0}
	first.DeleteFiles[0].FilePath = "s3://bucket/table/first-delete.parquet"
	second := validScanTasksWire()
	second.FileScanTasks[0].DataFile.FilePath = "s3://bucket/table/second-data.parquet"
	second.FileScanTasks[0].DeleteFileReferences = []int{0}
	second.DeleteFiles[0].FilePath = "s3://bucket/table/second-delete.parquet"

	firstTasks, err := DecodeScanTasks(first, metadata, metadata.schema, nil)
	require.NoError(t, err)
	secondTasks, err := DecodeScanTasks(second, metadata, metadata.schema, nil)
	require.NoError(t, err)

	require.Len(t, firstTasks, 1)
	require.Len(t, firstTasks[0].DeleteFiles, 1)
	assert.Equal(t, "s3://bucket/table/first-delete.parquet", firstTasks[0].DeleteFiles[0].FilePath())
	require.Len(t, secondTasks, 1)
	require.Len(t, secondTasks[0].DeleteFiles, 1)
	assert.Equal(t, "s3://bucket/table/second-delete.parquet", secondTasks[0].DeleteFiles[0].FilePath())
}

func TestDecodeScanTasksDerivesDeletionVectorTargetWhenOmitted(t *testing.T) {
	t.Parallel()

	metadata := newScanTaskDecoderMetadata()
	wire := validScanTasksWire()
	wire.FileScanTasks[0].DeleteFileReferences = []int{0}
	wire.DeleteFiles[0].FileFormat = "puffin"
	wire.DeleteFiles[0].ContentOffset = int64Ptr(10)
	wire.DeleteFiles[0].ContentSizeInBytes = int64Ptr(20)

	tasks, err := DecodeScanTasks(wire, metadata, metadata.schema, nil)
	require.NoError(t, err)
	require.Len(t, tasks, 1)
	require.Len(t, tasks[0].DeletionVectorFiles, 1)
	assert.Equal(t, stringPtr("s3://bucket/table/data.parquet"), tasks[0].DeletionVectorFiles[0].ReferencedDataFile())
}

func TestRESTValueMapMatchesJavaContentFileParser(t *testing.T) {
	t.Parallel()

	var bounds RESTValueMap
	require.NoError(t, json.Unmarshal(
		[]byte(`{"keys":[3,4],"values":["01000000","02000000"]}`),
		&bounds,
	))
	decoded, err := decodeValueMap("lower-bounds", &bounds)
	require.NoError(t, err)
	assert.Equal(t, map[int][]byte{
		3: {1, 0, 0, 0},
		4: {2, 0, 0, 0},
	}, decoded)

	err = json.Unmarshal([]byte(`{"keys":[3],"values":[1]}`), &bounds)
	require.Error(t, err, "bounds are binary hex strings, not typed JSON values")
}

func TestRESTValueMapUsesIcebergBinaryEncoding(t *testing.T) {
	t.Parallel()

	var bounds RESTValueMap
	require.NoError(t, json.Unmarshal(
		[]byte(`{
			"keys": [1, 2, 3],
			"values": ["22000000", "04D2", "15CD5B0700000000"]
		}`),
		&bounds,
	))
	decoded, err := decodeValueMap("lower-bounds", &bounds)
	require.NoError(t, err)

	decimalType := iceberg.DecimalTypeOf(9, 2)
	tests := []struct {
		name    string
		fieldID int
		typ     iceberg.Type
		want    iceberg.Literal
	}{
		{
			name:    "int little-endian",
			fieldID: 1,
			typ:     iceberg.PrimitiveTypes.Int32,
			want:    iceberg.Int32Literal(34),
		},
		{
			name:    "decimal big-endian",
			fieldID: 2,
			typ:     decimalType,
			want:    mustLiteral(t, "12.34", decimalType),
		},
		{
			name:    "timestamp little-endian",
			fieldID: 3,
			typ:     iceberg.PrimitiveTypes.Timestamp,
			want:    iceberg.TimestampLiteral(123456789),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			literal, err := iceberg.LiteralFromBytes(tt.typ, decoded[tt.fieldID])
			require.NoError(t, err)
			assert.Truef(t, tt.want.Equals(literal), "got %s, want %s", literal, tt.want)
		})
	}
}

func TestDecodeScanTasksKeyMetadataMatchesJavaContentFileParser(t *testing.T) {
	t.Parallel()

	metadata := newScanTaskDecoderMetadata()
	wire := validScanTasksWire()
	// This value mirrors Java TestContentFileParser's all-optional data-file
	// fixture. SingleValueParser encodes binary values as hexadecimal strings.
	wire.FileScanTasks[0].DataFile.KeyMetadata = stringPtr("00000000000000000000000000000000")

	tasks, err := DecodeScanTasks(wire, metadata, metadata.schema, nil)
	require.NoError(t, err)
	require.Len(t, tasks, 1)
	assert.Equal(t, make([]byte, 16), tasks[0].File.KeyMetadata())
}

func TestDecodeScanTasksRejectsMalformedPayloads(t *testing.T) {
	t.Parallel()

	metadata := newScanTaskDecoderMetadata()
	tests := []struct {
		name   string
		mutate func(*ScanTasks)
		want   string
	}{
		{
			name: "missing data file",
			mutate: func(w *ScanTasks) {
				w.FileScanTasks[0].DataFile = nil
			},
			want: "missing data-file",
		},
		{
			name: "unknown spec",
			mutate: func(w *ScanTasks) {
				w.FileScanTasks[0].DataFile.SpecID = 99
			},
			want: "unknown partition spec ID 99",
		},
		{
			name: "wrong partition width",
			mutate: func(w *ScanTasks) {
				w.FileScanTasks[0].DataFile.Partition = nil
			},
			want: "has 0 values, want 3",
		},
		{
			name: "negative delete reference",
			mutate: func(w *ScanTasks) {
				w.FileScanTasks[0].DeleteFileReferences = []int{-1}
			},
			want: "want 0 <= index",
		},
		{
			name: "out of range delete reference",
			mutate: func(w *ScanTasks) {
				w.FileScanTasks[0].DeleteFileReferences = []int{1}
			},
			want: "want 0 <= index < 1",
		},
		{
			name: "duplicate delete reference",
			mutate: func(w *ScanTasks) {
				w.FileScanTasks[0].DeleteFileReferences = []int{0, 0}
			},
			want: "repeats delete-file reference 0",
		},
		{
			name: "unreferenced delete file",
			mutate: func(w *ScanTasks) {
				w.FileScanTasks[0].DeleteFileReferences = nil
			},
			want: "delete-files[0] is not referenced by any file scan task",
		},
		{
			name: "count map length mismatch",
			mutate: func(w *ScanTasks) {
				w.FileScanTasks[0].DataFile.ValueCounts = &RESTCountMap{Keys: []int{1}, Values: []int64{}}
			},
			want: "value-counts has 1 keys and 0 values",
		},
		{
			name: "non-hexadecimal bound",
			mutate: func(w *ScanTasks) {
				w.FileScanTasks[0].DataFile.LowerBounds = &RESTValueMap{
					Keys: []int{1}, Values: []string{"not-hex"},
				}
			},
			want: "is not hexadecimal",
		},
		{
			name: "bad residual",
			mutate: func(w *ScanTasks) {
				w.FileScanTasks[0].ResidualFilter = json.RawMessage(`{"type":"wat"}`)
			},
			want: "unknown expression type",
		},
		{
			name: "missing equality ids",
			mutate: func(w *ScanTasks) {
				w.DeleteFiles[0].Content = "equality-deletes"
				w.DeleteFiles[0].EqualityIDs = nil
			},
			want: "missing equality-ids",
		},
		{
			name: "equality delete with blob range",
			mutate: func(w *ScanTasks) {
				w.DeleteFiles[0].Content = "equality-deletes"
				w.DeleteFiles[0].EqualityIDs = []int{1}
				w.DeleteFiles[0].ContentOffset = int64Ptr(10)
				w.DeleteFiles[0].ContentSizeInBytes = int64Ptr(20)
			},
			want: "must not carry position-delete reference or blob offsets",
		},
		{
			name: "position delete with equality ids",
			mutate: func(w *ScanTasks) {
				w.DeleteFiles[0].EqualityIDs = []int{1}
			},
			want: "must not carry equality-ids",
		},
		{
			name: "puffin missing blob range",
			mutate: func(w *ScanTasks) {
				w.DeleteFiles[0].FileFormat = "puffin"
			},
			want: "requires content-offset and content-size-in-bytes",
		},
		{
			name: "negative record count",
			mutate: func(w *ScanTasks) {
				w.FileScanTasks[0].DataFile.RecordCount = -1
			},
			want: "record-count must be positive",
		},
		{
			name: "zero record count",
			mutate: func(w *ScanTasks) {
				w.FileScanTasks[0].DataFile.RecordCount = 0
			},
			want: "record-count must be positive",
		},
		{
			name: "negative file size",
			mutate: func(w *ScanTasks) {
				w.FileScanTasks[0].DataFile.FileSizeInBytes = -1
			},
			want: "file-size-in-bytes must be positive",
		},
		{
			name: "zero file size",
			mutate: func(w *ScanTasks) {
				w.FileScanTasks[0].DataFile.FileSizeInBytes = 0
			},
			want: "file-size-in-bytes must be positive",
		},
		{
			name: "referenced data file disagrees with task",
			mutate: func(w *ScanTasks) {
				w.DeleteFiles[0].ReferencedDataFile = stringPtr("s3://bucket/table/other.parquet")
				w.FileScanTasks[0].DeleteFileReferences = []int{0}
			},
			want: "task data-file",
		},
		{
			name: "deletion vector referenced by different data files",
			mutate: func(w *ScanTasks) {
				w.DeleteFiles[0].FileFormat = "puffin"
				w.DeleteFiles[0].ContentOffset = int64Ptr(10)
				w.DeleteFiles[0].ContentSizeInBytes = int64Ptr(20)
				w.FileScanTasks[0].DeleteFileReferences = []int{0}
				secondDataFile := *w.FileScanTasks[0].DataFile
				secondDataFile.FilePath = "s3://bucket/table/other.parquet"
				w.FileScanTasks = append(w.FileScanTasks, RESTFileScanTask{
					DataFile:             &secondDataFile,
					DeleteFileReferences: []int{0},
				})
			},
			want: "is referenced by both",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			wire := validScanTasksWire()
			tt.mutate(&wire)
			_, err := DecodeScanTasks(wire, metadata, metadata.schema, iceberg.AlwaysTrue{})
			require.ErrorIs(t, err, ErrRESTError)
			assert.ErrorContains(t, err, tt.want)
		})
	}
}

func TestDecodeScanTasksRejectsDeleteFilesWithoutTasks(t *testing.T) {
	t.Parallel()

	metadata := newScanTaskDecoderMetadata()
	wire := validScanTasksWire()
	wire.FileScanTasks = nil

	_, err := DecodeScanTasks(wire, metadata, metadata.schema, nil)
	require.ErrorIs(t, err, ErrRESTError)
	assert.ErrorContains(t, err, "delete files have no file scan tasks")
}

func TestDecodePartitionLiteralCoversPrimitiveWireTypes(t *testing.T) {
	t.Parallel()

	decimalType := iceberg.DecimalTypeOf(9, 2)
	fixedType := iceberg.FixedTypeOf(4)
	tests := []struct {
		name string
		raw  string
		typ  iceberg.Type
		want iceberg.Literal
	}{
		{"boolean", `true`, iceberg.PrimitiveTypes.Bool, iceberg.BoolLiteral(true)},
		{"int", `2147483647`, iceberg.PrimitiveTypes.Int32, iceberg.Int32Literal(2147483647)},
		{"long above JSON exact float range", `9007199254740993`, iceberg.PrimitiveTypes.Int64, iceberg.Int64Literal(9007199254740993)},
		{"float", `1.25`, iceberg.PrimitiveTypes.Float32, iceberg.Float32Literal(1.25)},
		{"double", `1.25`, iceberg.PrimitiveTypes.Float64, iceberg.Float64Literal(1.25)},
		{"string", `"hello"`, iceberg.PrimitiveTypes.String, iceberg.StringLiteral("hello")},
		{"date", `"2026-07-17"`, iceberg.PrimitiveTypes.Date, mustLiteral(t, "2026-07-17", iceberg.PrimitiveTypes.Date)},
		{"time", `"10:15:30.123456"`, iceberg.PrimitiveTypes.Time, mustLiteral(t, "10:15:30.123456", iceberg.PrimitiveTypes.Time)},
		{"timestamp", `"2026-07-17T10:15:30.123456"`, iceberg.PrimitiveTypes.Timestamp, mustLiteral(t, "2026-07-17T10:15:30.123456", iceberg.PrimitiveTypes.Timestamp)},
		{"timestamptz", `"2026-07-17T10:15:30.123456+00:00"`, iceberg.PrimitiveTypes.TimestampTz, mustLiteral(t, "2026-07-17T10:15:30.123456+00:00", iceberg.PrimitiveTypes.TimestampTz)},
		{"timestamp nanos", `"2026-07-17T10:15:30.123456789"`, iceberg.PrimitiveTypes.TimestampNs, mustLiteral(t, "2026-07-17T10:15:30.123456789", iceberg.PrimitiveTypes.TimestampNs)},
		{"timestamptz nanos", `"2026-07-17T10:15:30.123456789+00:00"`, iceberg.PrimitiveTypes.TimestampTzNs, mustLiteral(t, "2026-07-17T10:15:30.123456789+00:00", iceberg.PrimitiveTypes.TimestampTzNs)},
		{"decimal", `"12.34"`, decimalType, mustLiteral(t, "12.34", decimalType)},
		{"uuid", `"f79c3e09-677c-4bbd-a479-3f349cb785e7"`, iceberg.PrimitiveTypes.UUID, mustLiteral(t, "f79c3e09-677c-4bbd-a479-3f349cb785e7", iceberg.PrimitiveTypes.UUID)},
		{"fixed", `"78797A21"`, fixedType, iceberg.FixedLiteral([]byte("xyz!"))},
		{"binary", `"00FF10"`, iceberg.PrimitiveTypes.Binary, iceberg.BinaryLiteral([]byte{0, 255, 16})},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			got, err := decodePartitionLiteral(json.RawMessage(tt.raw), tt.typ)
			require.NoError(t, err)
			assert.Truef(t, got.Equals(tt.want), "got %s (%T), want %s (%T)", got, got, tt.want, tt.want)
		})
	}
}

func TestDecodePartitionLiteralRejectsInvalidValues(t *testing.T) {
	t.Parallel()

	for _, tt := range []struct {
		name string
		raw  string
		typ  iceberg.Type
		want string
	}{
		{"null", `null`, iceberg.PrimitiveTypes.Int64, "null is not valid"},
		{"int32 overflow", `2147483648`, iceberg.PrimitiveTypes.Int32, "outside int32 range"},
		{"long fraction", `1.5`, iceberg.PrimitiveTypes.Int64, "invalid integer value"},
		{"wrong fixed width", `"00FF"`, iceberg.FixedTypeOf(4), "different length"},
		{"invalid binary hex", `"XYZ"`, iceberg.PrimitiveTypes.Binary, "hexadecimal"},
		{"nested value", `{"x":1}`, iceberg.PrimitiveTypes.String, "invalid string value"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			_, err := decodePartitionLiteral(json.RawMessage(tt.raw), tt.typ)
			require.ErrorContains(t, err, tt.want)
		})
	}
}

func validScanTasksWire() ScanTasks {
	data := RESTContentFile{
		SpecID:          7,
		Partition:       []json.RawMessage{json.RawMessage(`34`), json.RawMessage(`"2026-07-17"`), json.RawMessage(`"78797A21"`)},
		Content:         "data",
		FilePath:        "s3://bucket/table/data.parquet",
		FileFormat:      "parquet",
		FileSizeInBytes: 100,
		RecordCount:     10,
	}
	deleteFile := data
	deleteFile.Content = "position-deletes"
	deleteFile.FilePath = "s3://bucket/table/delete.parquet"

	return ScanTasks{
		FileScanTasks: []RESTFileScanTask{{
			DataFile:             &RESTDataFile{RESTContentFile: data},
			DeleteFileReferences: []int{0},
		}},
		DeleteFiles: []RESTDeleteFile{{RESTContentFile: deleteFile}},
	}
}

type scanTaskDecoderMetadata struct {
	schema *iceberg.Schema
	spec   iceberg.PartitionSpec
}

func newScanTaskDecoderMetadata() *scanTaskDecoderMetadata {
	schema := iceberg.NewSchema(10,
		iceberg.NestedField{ID: 1, Name: "id", Type: iceberg.PrimitiveTypes.Int64, Required: true},
		iceberg.NestedField{ID: 2, Name: "category", Type: iceberg.PrimitiveTypes.String},
		iceberg.NestedField{ID: 3, Name: "event_date", Type: iceberg.PrimitiveTypes.Date},
		iceberg.NestedField{ID: 4, Name: "amount", Type: iceberg.DecimalTypeOf(9, 2)},
		iceberg.NestedField{ID: 5, Name: "code", Type: iceberg.FixedTypeOf(4)},
		iceberg.NestedField{ID: 6, Name: "event_time", Type: iceberg.PrimitiveTypes.TimestampTzNs},
		iceberg.NestedField{ID: 7, Name: "score", Type: iceberg.PrimitiveTypes.Float64},
		iceberg.NestedField{ID: 8, Name: "lower_int", Type: iceberg.PrimitiveTypes.Int32},
		iceberg.NestedField{ID: 9, Name: "upper_int", Type: iceberg.PrimitiveTypes.Int32},
	)
	spec := iceberg.NewPartitionSpecID(7,
		iceberg.PartitionField{SourceIDs: []int{1}, FieldID: 1000, Name: "id_part", Transform: iceberg.IdentityTransform{}},
		iceberg.PartitionField{SourceIDs: []int{3}, FieldID: 1001, Name: "date_part", Transform: iceberg.IdentityTransform{}},
		iceberg.PartitionField{SourceIDs: []int{5}, FieldID: 1002, Name: "code_part", Transform: iceberg.IdentityTransform{}},
	)

	return &scanTaskDecoderMetadata{schema: schema, spec: spec}
}

func (m *scanTaskDecoderMetadata) CurrentSchema() *iceberg.Schema { return m.schema }
func (m *scanTaskDecoderMetadata) Schemas() []*iceberg.Schema     { return []*iceberg.Schema{m.schema} }
func (m *scanTaskDecoderMetadata) PartitionSpec() iceberg.PartitionSpec {
	return m.spec
}

func (m *scanTaskDecoderMetadata) PartitionSpecByID(id int) *iceberg.PartitionSpec {
	if id != m.spec.ID() {
		return nil
	}

	return &m.spec
}
func (*scanTaskDecoderMetadata) CurrentSnapshot() *table.Snapshot   { return nil }
func (*scanTaskDecoderMetadata) SnapshotByID(int64) *table.Snapshot { return nil }
func (*scanTaskDecoderMetadata) Properties() iceberg.Properties     { return nil }

func mustLiteral(t *testing.T, value string, typ iceberg.Type) iceberg.Literal {
	t.Helper()
	literal, err := iceberg.StringLiteral(value).To(typ)
	require.NoError(t, err)

	return literal
}

func intPtr(value int) *int       { return &value }
func int64Ptr(value int64) *int64 { return &value }
