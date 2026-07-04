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
	"testing"

	"github.com/apache/iceberg-go"
	"github.com/stretchr/testify/require"
)

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

func TestDecodeFileScanTaskRejectsNegativeScanRanges(t *testing.T) {
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
