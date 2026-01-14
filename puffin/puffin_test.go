// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package puffin_test

import (
	"bytes"
	"testing"

	"github.com/apache/iceberg-go/puffin"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestWriterEmptyFile(t *testing.T) {
	var buf bytes.Buffer

	w, err := puffin.NewWriter(&buf)
	require.NoError(t, err)

	err = w.Finish()
	require.NoError(t, err)

	// Verify minimum structure: header magic + footer magic + JSON + trailer
	assert.GreaterOrEqual(t, buf.Len(), puffin.MagicSize*2+12)

	// Verify header magic
	assert.Equal(t, []byte{'P', 'F', 'A', '1'}, buf.Bytes()[:4])

	// Verify trailing magic
	assert.Equal(t, []byte{'P', 'F', 'A', '1'}, buf.Bytes()[buf.Len()-4:])
}

func TestWriterSingleBlob(t *testing.T) {
	var buf bytes.Buffer
	blobData := []byte("hello world")

	w, err := puffin.NewWriter(&buf)
	require.NoError(t, err)

	meta, err := w.AddBlob(puffin.BlobMetadataInput{
		Type:           puffin.BlobTypeDataSketchesTheta,
		SnapshotID:     123,
		SequenceNumber: 1,
		Fields:         []int32{1, 2},
	}, blobData)
	require.NoError(t, err)

	assert.Equal(t, puffin.BlobTypeDataSketchesTheta, meta.Type)
	assert.Equal(t, int64(123), meta.SnapshotID)
	assert.Equal(t, int64(1), meta.SequenceNumber)
	assert.Equal(t, []int32{1, 2}, meta.Fields)
	assert.Equal(t, int64(puffin.MagicSize), meta.Offset) // blob starts after header magic
	assert.Equal(t, int64(len(blobData)), meta.Length)

	err = w.Finish()
	require.NoError(t, err)
}

func TestWriterMultipleBlobs(t *testing.T) {
	var buf bytes.Buffer
	blob1 := []byte("first blob")
	blob2 := []byte("second blob data")

	w, err := puffin.NewWriter(&buf)
	require.NoError(t, err)

	meta1, err := w.AddBlob(puffin.BlobMetadataInput{
		Type:       puffin.BlobTypeDataSketchesTheta,
		SnapshotID: 100,
		Fields:     []int32{1},
	}, blob1)
	require.NoError(t, err)

	meta2, err := w.AddBlob(puffin.BlobMetadataInput{
		Type:       puffin.BlobTypeDeletionVector,
		SnapshotID: 200,
		Fields:     []int32{2},
	}, blob2)
	require.NoError(t, err)

	// Second blob should start after first blob
	assert.Equal(t, meta1.Offset+meta1.Length, meta2.Offset)

	err = w.Finish()
	require.NoError(t, err)
}

func TestWriterSetCreatedBy(t *testing.T) {
	var buf bytes.Buffer

	w, err := puffin.NewWriter(&buf)
	require.NoError(t, err)

	err = w.SetCreatedBy("test-app v1.0")
	require.NoError(t, err)

	err = w.Finish()
	require.NoError(t, err)

	// Read back and verify
	r, err := puffin.NewReader(bytes.NewReader(buf.Bytes()), int64(buf.Len()))
	require.NoError(t, err)

	footer, err := r.ReadFooter()
	require.NoError(t, err)

	assert.Equal(t, "test-app v1.0", footer.Properties[puffin.CreatedBy])
}

func TestWriterSetProperties(t *testing.T) {
	var buf bytes.Buffer

	w, err := puffin.NewWriter(&buf)
	require.NoError(t, err)

	err = w.AddProperties(map[string]string{
		"custom-key": "custom-value",
	})
	require.NoError(t, err)

	err = w.Finish()
	require.NoError(t, err)

	// Read back and verify
	r, err := puffin.NewReader(bytes.NewReader(buf.Bytes()), int64(buf.Len()))
	require.NoError(t, err)

	footer, err := r.ReadFooter()
	require.NoError(t, err)

	assert.Equal(t, "custom-value", footer.Properties["custom-key"])
}

func TestWriterErrors(t *testing.T) {
	t.Run("nil writer", func(t *testing.T) {
		_, err := puffin.NewWriter(nil)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "writer is nil")
	})

	t.Run("empty blob type", func(t *testing.T) {
		var buf bytes.Buffer
		w, err := puffin.NewWriter(&buf)
		require.NoError(t, err)

		_, err = w.AddBlob(puffin.BlobMetadataInput{
			Type: "", // empty type
		}, []byte("data"))
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "type is required")
	})

	t.Run("add after finish", func(t *testing.T) {
		var buf bytes.Buffer
		w, err := puffin.NewWriter(&buf)
		require.NoError(t, err)

		err = w.Finish()
		require.NoError(t, err)

		_, err = w.AddBlob(puffin.BlobMetadataInput{
			Type:   puffin.BlobTypeDataSketchesTheta,
			Fields: []int32{1},
		}, []byte("data"))
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "finalized")
	})

	t.Run("double finish", func(t *testing.T) {
		var buf bytes.Buffer
		w, err := puffin.NewWriter(&buf)
		require.NoError(t, err)

		err = w.Finish()
		require.NoError(t, err)

		err = w.Finish()
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "finalized")
	})
}

func TestReaderErrors(t *testing.T) {
	t.Run("nil reader", func(t *testing.T) {
		_, err := puffin.NewReader(nil, 100)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "reader is nil")
	})

	t.Run("file too small", func(t *testing.T) {
		data := make([]byte, 10) // too small
		_, err := puffin.NewReader(bytes.NewReader(data), int64(len(data)))
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "too small")
	})

	t.Run("invalid header magic", func(t *testing.T) {
		data := make([]byte, 100)
		copy(data[0:4], []byte("XXXX")) // wrong magic
		copy(data[96:100], []byte{'P', 'F', 'A', '1'})

		_, err := puffin.NewReader(bytes.NewReader(data), int64(len(data)))
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "invalid header magic")
	})

	t.Run("invalid trailing magic", func(t *testing.T) {
		data := make([]byte, 100)
		copy(data[0:4], []byte{'P', 'F', 'A', '1'})
		copy(data[96:100], []byte("XXXX")) // wrong trailing magic

		_, err := puffin.NewReader(bytes.NewReader(data), int64(len(data)))
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "invalid trailing magic")
	})

	t.Run("blob index out of range", func(t *testing.T) {
		var buf bytes.Buffer
		w, err := puffin.NewWriter(&buf)
		require.NoError(t, err)
		err = w.Finish()
		require.NoError(t, err)

		r, err := puffin.NewReader(bytes.NewReader(buf.Bytes()), int64(buf.Len()))
		require.NoError(t, err)

		_, err = r.ReadBlob(0) // no blobs, index 0 is out of range
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "out of range")
	})
}

func TestRoundTrip(t *testing.T) {
	tests := []struct {
		name  string
		blobs []struct {
			data  []byte
			input puffin.BlobMetadataInput
		}
	}{
		{
			name:  "empty file",
			blobs: nil,
		},
		{
			name: "single blob",
			blobs: []struct {
				data  []byte
				input puffin.BlobMetadataInput
			}{
				{
					data: []byte("test data"),
					input: puffin.BlobMetadataInput{
						Type:           puffin.BlobTypeDataSketchesTheta,
						SnapshotID:     1,
						SequenceNumber: 1,
						Fields:         []int32{1},
					},
				},
			},
		},
		{
			name: "multiple blobs",
			blobs: []struct {
				data  []byte
				input puffin.BlobMetadataInput
			}{
				{
					data: []byte("first blob data"),
					input: puffin.BlobMetadataInput{
						Type:           puffin.BlobTypeDataSketchesTheta,
						SnapshotID:     100,
						SequenceNumber: 1,
						Fields:         []int32{1, 2},
					},
				},
				{
					data: []byte("second blob with more data"),
					input: puffin.BlobMetadataInput{
						Type:           puffin.BlobTypeDeletionVector,
						SnapshotID:     200,
						SequenceNumber: 2,
						Fields:         []int32{3},
						Properties:     map[string]string{"key": "value"},
					},
				},
				{
					data: []byte("third"),
					input: puffin.BlobMetadataInput{
						Type:           puffin.BlobTypeDataSketchesTheta,
						SnapshotID:     300,
						SequenceNumber: 3,
						Fields:         []int32{4, 5, 6},
					},
				},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Write
			var buf bytes.Buffer
			w, err := puffin.NewWriter(&buf)
			require.NoError(t, err)

			for _, blob := range tt.blobs {
				_, err := w.AddBlob(blob.input, blob.data)
				require.NoError(t, err)
			}

			err = w.Finish()
			require.NoError(t, err)

			// Read
			r, err := puffin.NewReader(bytes.NewReader(buf.Bytes()), int64(buf.Len()))
			require.NoError(t, err)

			footer, err := r.ReadFooter()
			require.NoError(t, err)

			// Verify blob count
			assert.Len(t, footer.Blobs, len(tt.blobs))

			// Verify each blob
			for i, expected := range tt.blobs {
				blobData, err := r.ReadBlob(i)
				require.NoError(t, err)

				assert.Equal(t, expected.data, blobData.Data, "blob %d data mismatch", i)
				assert.Equal(t, expected.input.Type, blobData.Metadata.Type, "blob %d type mismatch", i)
				assert.Equal(t, expected.input.SnapshotID, blobData.Metadata.SnapshotID, "blob %d snapshot-id mismatch", i)
				assert.Equal(t, expected.input.SequenceNumber, blobData.Metadata.SequenceNumber, "blob %d sequence-number mismatch", i)
				assert.Equal(t, expected.input.Fields, blobData.Metadata.Fields, "blob %d fields mismatch", i)

				if expected.input.Properties != nil {
					assert.Equal(t, expected.input.Properties, blobData.Metadata.Properties, "blob %d properties mismatch", i)
				}
			}

			// Also test ReadAllBlobs
			if len(tt.blobs) > 0 {
				allBlobs, err := r.ReadAllBlobs()
				require.NoError(t, err)
				assert.Len(t, allBlobs, len(tt.blobs))

				for i, expected := range tt.blobs {
					assert.Equal(t, expected.data, allBlobs[i].Data)
				}
			}
		})
	}
}

func TestReadBlobByMetadata(t *testing.T) {
	// Write a file with a blob
	var buf bytes.Buffer
	blobData := []byte("metadata test blob")

	w, err := puffin.NewWriter(&buf)
	require.NoError(t, err)

	meta, err := w.AddBlob(puffin.BlobMetadataInput{
		Type:       puffin.BlobTypeDataSketchesTheta,
		SnapshotID: 42,
		Fields:     []int32{1},
	}, blobData)
	require.NoError(t, err)

	err = w.Finish()
	require.NoError(t, err)

	// Read using metadata directly
	r, err := puffin.NewReader(bytes.NewReader(buf.Bytes()), int64(buf.Len()))
	require.NoError(t, err)

	// Must read footer first
	_, err = r.ReadFooter()
	require.NoError(t, err)

	data, err := r.ReadBlobByMetadata(meta)
	require.NoError(t, err)

	assert.Equal(t, blobData, data)
}

func TestReadRange(t *testing.T) {
	// Write a file with a known blob
	var buf bytes.Buffer
	blobData := []byte("0123456789")

	w, err := puffin.NewWriter(&buf)
	require.NoError(t, err)

	meta, err := w.AddBlob(puffin.BlobMetadataInput{
		Type:       puffin.BlobTypeDataSketchesTheta,
		SnapshotID: 1,
		Fields:     []int32{1},
	}, blobData)
	require.NoError(t, err)

	err = w.Finish()
	require.NoError(t, err)

	// Read a range
	r, err := puffin.NewReader(bytes.NewReader(buf.Bytes()), int64(buf.Len()))
	require.NoError(t, err)

	_, err = r.ReadFooter()
	require.NoError(t, err)

	// Read middle portion of blob
	data, err := r.ReadRange(meta.Offset+2, 5)
	require.NoError(t, err)

	assert.Equal(t, []byte("23456"), data)
}
