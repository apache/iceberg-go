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

package codec

import (
	"encoding/binary"
	"math"
	"testing"

	"github.com/DataDog/iceberg-go"
	"github.com/stretchr/testify/require"
)

func TestValidateScanRange(t *testing.T) {
	for _, tt := range []struct {
		name              string
		start, length     int64
		fileSize          int64
		shouldReturnError bool
	}{
		{name: "full file", length: 100, fileSize: 100},
		{name: "empty range at EOF", start: 100, fileSize: 100},
		{name: "empty file", fileSize: 0},
		{name: "one byte at EOF", start: 100, length: 1, fileSize: 100, shouldReturnError: true},
		{name: "maximum file size with empty range", start: math.MaxInt64, fileSize: math.MaxInt64},
		{name: "maximum file size boundary", start: math.MaxInt64 - 1, length: 1, fileSize: math.MaxInt64},
		{name: "start after EOF", start: 101, fileSize: 100, shouldReturnError: true},
		{name: "end after EOF", start: 99, length: 2, fileSize: 100, shouldReturnError: true},
		{name: "start plus length overflows", start: math.MaxInt64 - 1, length: 2, fileSize: math.MaxInt64, shouldReturnError: true},
		{name: "negative file size", fileSize: -1, shouldReturnError: true},
	} {
		t.Run(tt.name, func(t *testing.T) {
			err := validateScanRange(tt.start, tt.length, tt.fileSize)
			if tt.shouldReturnError {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
			}
		})
	}
}

// TestDecodeFileScanTaskInnerErrorCarriesMarker covers an inner decode path:
// the outer Avro envelope decodes fine, but the framed primary-file blob is not
// a valid DataFile encoding. The wrapped error must still carry the
// "codec: DecodeFileScanTask: file:" marker. Building the malformed envelope
// needs the unexported fileScanTaskEnvelope/fileScanTaskSchema, so this lives
// in an internal test rather than the codec_test package.
func TestDecodeFileScanTaskInnerErrorCarriesMarker(t *testing.T) {
	env := fileScanTaskEnvelope{File: []byte("not a valid data file blob")}
	data, err := fileScanTaskSchema.Encode(&env)
	require.NoError(t, err)

	spec := iceberg.NewPartitionSpecID(7,
		iceberg.PartitionField{SourceIDs: []int{1}, FieldID: 1000, Name: "id_part", Transform: iceberg.IdentityTransform{}},
	)
	schema := iceberg.NewSchema(123,
		iceberg.NestedField{ID: 1, Name: "id", Type: iceberg.Int64Type{}, Required: true},
	)

	_, err = DecodeFileScanTask(data, spec, schema, 2)
	require.Error(t, err)
	require.Contains(t, err.Error(), "codec: DecodeFileScanTask: file:",
		"an inner (primary-file) decode error must carry the function + sub-path marker")
}

func TestDecodeFileScanTaskResidualExtension(t *testing.T) {
	schema := iceberg.NewSchema(123,
		iceberg.NestedField{ID: 1, Name: "id", Type: iceberg.Int64Type{}, Required: true},
	)
	payload := []byte(`{"type":"eq","term":"id","value":42}`)
	extension := append([]byte(nil), fileScanTaskResidualMagic...)
	extension = binary.AppendUvarint(extension, uint64(len(payload)))
	extension = append(extension, payload...)

	residual, err := decodeFileScanTaskResidual(extension, schema)
	require.NoError(t, err)
	require.True(t, residual.Equals(iceberg.EqualTo(iceberg.Reference("id"), int64(42))))

	legacy, err := decodeFileScanTaskResidual(nil, schema)
	require.NoError(t, err)
	require.Nil(t, legacy)

	_, err = decodeFileScanTaskResidual([]byte("unknown"), schema)
	require.ErrorContains(t, err, "unknown trailing extension")

	_, err = decodeFileScanTaskResidual(extension[:len(extension)-1], schema)
	require.ErrorContains(t, err, "residual length")
}

func TestDecodeFileScanTaskRejectsNegativeScanRanges(t *testing.T) {
	// Range checks are intentionally validated before attempting to decode nested
	// data-file payloads, so malformed start/length are rejected at the
	// envelope layer without decoding heavy blob fields.
	spec := iceberg.NewPartitionSpecID(7,
		iceberg.PartitionField{SourceIDs: []int{1}, FieldID: 1000, Name: "id_part", Transform: iceberg.IdentityTransform{}},
	)
	schema := iceberg.NewSchema(123,
		iceberg.NestedField{ID: 1, Name: "id", Type: iceberg.Int64Type{}, Required: true},
	)

	t.Run("start", func(t *testing.T) {
		data, err := fileScanTaskSchema.Encode(&fileScanTaskEnvelope{
			Start:  -1,
			Length: 1,
		})
		require.NoError(t, err)

		_, err = DecodeFileScanTask(data, spec, schema, 2)
		require.Error(t, err)
		require.Contains(t, err.Error(), "codec: DecodeFileScanTask:")
		require.Contains(t, err.Error(), "start must be non-negative")
	})

	t.Run("length", func(t *testing.T) {
		data, err := fileScanTaskSchema.Encode(&fileScanTaskEnvelope{
			Start:  0,
			Length: -1,
		})
		require.NoError(t, err)

		_, err = DecodeFileScanTask(data, spec, schema, 2)
		require.Error(t, err)
		require.Contains(t, err.Error(), "codec: DecodeFileScanTask:")
		require.Contains(t, err.Error(), "length must be non-negative")
	})
}

func TestDecodeFileScanTaskAllowsRangeBeyondFileSize(t *testing.T) {
	spec := *iceberg.UnpartitionedSpec
	builder, err := iceberg.NewDataFileBuilder(spec, iceberg.EntryContentData,
		"data.parquet", iceberg.ParquetFile, nil, nil, nil, 1, 100)
	require.NoError(t, err)
	file, err := EncodeDataFile(builder.Build(), spec, nil, 2)
	require.NoError(t, err)
	envelope, err := fileScanTaskSchema.Encode(&fileScanTaskEnvelope{
		File: file, Start: 90, Length: 11,
	})
	require.NoError(t, err)

	decoded, err := DecodeFileScanTask(envelope, spec, nil, 2)
	require.NoError(t, err)
	require.Equal(t, int64(90), decoded.Start)
	require.Equal(t, int64(11), decoded.Length)
}
