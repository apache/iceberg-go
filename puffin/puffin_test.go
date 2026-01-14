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

package puffin_test

import (
	"bytes"
	"testing"

	"github.com/apache/iceberg-go/puffin"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestRoundTrip verifies that data written by Writer can be read back by Reader
// with all metadata and blob data preserved exactly.
func TestRoundTrip(t *testing.T) {
	// Test data
	blob1Data := []byte("theta sketch data here")
	blob2Data := []byte("another blob with different content")

	// Write puffin file to buffer
	var buf bytes.Buffer
	w, err := puffin.NewWriter(&buf)
	require.NoError(t, err)

	// Add file-level properties
	err = w.AddProperties(map[string]string{
		"test-property": "test-value",
	})
	require.NoError(t, err)

	// Add first blob (DataSketches type)
	meta1, err := w.AddBlob(puffin.BlobMetadataInput{
		Type:           puffin.BlobTypeDataSketchesTheta,
		SnapshotID:     123,
		SequenceNumber: 1,
		Fields:         []int32{1, 2, 3},
		Properties:     map[string]string{"ndv": "1000"},
	}, blob1Data)
	require.NoError(t, err)

	// Add second blob (DeletionVector type)
	meta2, err := w.AddBlob(puffin.BlobMetadataInput{
		Type:           puffin.BlobTypeDeletionVector,
		SnapshotID:     -1,
		SequenceNumber: -1,
		Fields:         []int32{},
		Properties: map[string]string{
			"referenced-data-file": "data/file.parquet",
			"cardinality":          "42",
		},
	}, blob2Data)
	require.NoError(t, err)

	err = w.Finish()
	require.NoError(t, err)

	// Read puffin file back
	data := buf.Bytes()
	r, err := puffin.NewReader(bytes.NewReader(data), int64(len(data)))
	require.NoError(t, err)

	// Verify footer
	footer := r.Footer()
	require.NotNil(t, footer)
	assert.Len(t, footer.Blobs, 2)

	// Verify file properties
	assert.Equal(t, "test-value", footer.Properties["test-property"])
	assert.Contains(t, footer.Properties[puffin.CreatedBy], "iceberg-go")

	// Verify first blob metadata
	assert.Equal(t, puffin.BlobTypeDataSketchesTheta, footer.Blobs[0].Type)
	assert.Equal(t, int64(123), footer.Blobs[0].SnapshotID)
	assert.Equal(t, int64(1), footer.Blobs[0].SequenceNumber)
	assert.Equal(t, []int32{1, 2, 3}, footer.Blobs[0].Fields)
	assert.Equal(t, meta1.Offset, footer.Blobs[0].Offset)
	assert.Equal(t, meta1.Length, footer.Blobs[0].Length)
	assert.Equal(t, "1000", footer.Blobs[0].Properties["ndv"])

	// Verify second blob metadata
	assert.Equal(t, puffin.BlobTypeDeletionVector, footer.Blobs[1].Type)
	assert.Equal(t, int64(-1), footer.Blobs[1].SnapshotID)
	assert.Equal(t, int64(-1), footer.Blobs[1].SequenceNumber)
	assert.Equal(t, []int32{}, footer.Blobs[1].Fields)
	assert.Equal(t, meta2.Offset, footer.Blobs[1].Offset)
	assert.Equal(t, meta2.Length, footer.Blobs[1].Length)

	// Verify blob data via ReadBlob
	blobData1, err := r.ReadBlob(0)
	require.NoError(t, err)
	assert.Equal(t, blob1Data, blobData1.Data)

	blobData2, err := r.ReadBlob(1)
	require.NoError(t, err)
	assert.Equal(t, blob2Data, blobData2.Data)

	// Verify ReadAllBlobs
	allBlobs, err := r.ReadAllBlobs()
	require.NoError(t, err)
	assert.Len(t, allBlobs, 2)
	assert.Equal(t, blob1Data, allBlobs[0].Data)
	assert.Equal(t, blob2Data, allBlobs[1].Data)
}

// TestEmptyFile verifies that a puffin file with no blobs is valid.
func TestEmptyFile(t *testing.T) {
	var buf bytes.Buffer
	w, err := puffin.NewWriter(&buf)
	require.NoError(t, err)

	err = w.Finish()
	require.NoError(t, err)

	data := buf.Bytes()
	r, err := puffin.NewReader(bytes.NewReader(data), int64(len(data)))
	require.NoError(t, err)

	footer := r.Footer()
	assert.NotNil(t, footer)
	assert.Len(t, footer.Blobs, 0)
	assert.Contains(t, footer.Properties[puffin.CreatedBy], "iceberg-go")

	// ReadAllBlobs should return nil for empty file
	blobs, err := r.ReadAllBlobs()
	require.NoError(t, err)
	assert.Nil(t, blobs)
}

// TestWriterValidation verifies that Writer rejects invalid input.
func TestWriterValidation(t *testing.T) {
	t.Run("nil writer", func(t *testing.T) {
		_, err := puffin.NewWriter(nil)
		assert.Error(t, err)
		assert.ErrorContains(t, err, "nil")
	})

	t.Run("missing type", func(t *testing.T) {
		var buf bytes.Buffer
		w, err := puffin.NewWriter(&buf)
		require.NoError(t, err)

		_, err = w.AddBlob(puffin.BlobMetadataInput{
			Type:   "", // missing
			Fields: []int32{1},
		}, []byte("data"))
		assert.Error(t, err)
		assert.ErrorContains(t, err, "type")
	})

	t.Run("nil fields", func(t *testing.T) {
		var buf bytes.Buffer
		w, err := puffin.NewWriter(&buf)
		require.NoError(t, err)

		_, err = w.AddBlob(puffin.BlobMetadataInput{
			Type:   puffin.BlobTypeDataSketchesTheta,
			Fields: nil, // nil not allowed
		}, []byte("data"))
		assert.Error(t, err)
		assert.ErrorContains(t, err, "fields")
	})

	t.Run("add blob after finish", func(t *testing.T) {
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
		assert.ErrorContains(t, err, "finalized")
	})

	t.Run("double finish", func(t *testing.T) {
		var buf bytes.Buffer
		w, err := puffin.NewWriter(&buf)
		require.NoError(t, err)

		err = w.Finish()
		require.NoError(t, err)

		err = w.Finish()
		assert.Error(t, err)
		assert.ErrorContains(t, err, "finalized")
	})

	t.Run("deletion vector with invalid snapshot-id", func(t *testing.T) {
		var buf bytes.Buffer
		w, err := puffin.NewWriter(&buf)
		require.NoError(t, err)

		_, err = w.AddBlob(puffin.BlobMetadataInput{
			Type:           puffin.BlobTypeDeletionVector,
			SnapshotID:     123, // must be -1
			SequenceNumber: -1,
			Fields:         []int32{},
		}, []byte("data"))
		assert.Error(t, err)
		assert.ErrorContains(t, err, "snapshot-id")
	})

	t.Run("deletion vector with invalid sequence-number", func(t *testing.T) {
		var buf bytes.Buffer
		w, err := puffin.NewWriter(&buf)
		require.NoError(t, err)

		_, err = w.AddBlob(puffin.BlobMetadataInput{
			Type:           puffin.BlobTypeDeletionVector,
			SnapshotID:     -1,
			SequenceNumber: 5, // must be -1
			Fields:         []int32{},
		}, []byte("data"))
		assert.Error(t, err)
		assert.ErrorContains(t, err, "sequence-number")
	})
}

// TestReaderInvalidFile verifies that Reader rejects invalid/corrupt files.
func TestReaderInvalidFile(t *testing.T) {
	t.Run("nil reader", func(t *testing.T) {
		_, err := puffin.NewReader(nil, 100)
		assert.Error(t, err)
		assert.ErrorContains(t, err, "nil")
	})

	t.Run("file too small", func(t *testing.T) {
		data := []byte("tiny")
		_, err := puffin.NewReader(bytes.NewReader(data), int64(len(data)))
		assert.Error(t, err)
		assert.ErrorContains(t, err, "too small")
	})

	t.Run("invalid header magic", func(t *testing.T) {
		// Create valid file first
		var buf bytes.Buffer
		w, _ := puffin.NewWriter(&buf)
		w.Finish()
		data := buf.Bytes()

		// Corrupt header magic
		data[0] = 'X'

		_, err := puffin.NewReader(bytes.NewReader(data), int64(len(data)))
		assert.Error(t, err)
		assert.ErrorContains(t, err, "magic")
	})

	t.Run("invalid trailing magic", func(t *testing.T) {
		// Create valid file first
		var buf bytes.Buffer
		w, _ := puffin.NewWriter(&buf)
		w.Finish()
		data := buf.Bytes()

		// Corrupt trailing magic (last 4 bytes)
		data[len(data)-1] = 'X'

		_, err := puffin.NewReader(bytes.NewReader(data), int64(len(data)))
		assert.Error(t, err)
		assert.ErrorContains(t, err, "magic")
	})
}

// TestReaderBlobAccess verifies blob access methods work correctly.
func TestReaderBlobAccess(t *testing.T) {
	// Create file with multiple blobs
	var buf bytes.Buffer
	w, err := puffin.NewWriter(&buf)
	require.NoError(t, err)

	blobs := [][]byte{
		[]byte("first"),
		[]byte("second"),
		[]byte("third"),
	}

	for _, blob := range blobs {
		_, err := w.AddBlob(puffin.BlobMetadataInput{
			Type:   puffin.BlobTypeDataSketchesTheta,
			Fields: []int32{},
		}, blob)
		require.NoError(t, err)
	}
	require.NoError(t, w.Finish())

	data := buf.Bytes()
	r, err := puffin.NewReader(bytes.NewReader(data), int64(len(data)))
	require.NoError(t, err)

	t.Run("read by index", func(t *testing.T) {
		for i, expected := range blobs {
			blobData, err := r.ReadBlob(i)
			require.NoError(t, err)
			assert.Equal(t, expected, blobData.Data)
		}
	})

	t.Run("index out of range", func(t *testing.T) {
		_, err := r.ReadBlob(-1)
		assert.Error(t, err)

		_, err = r.ReadBlob(100)
		assert.Error(t, err)
	})

	t.Run("read by metadata", func(t *testing.T) {
		meta := r.Footer().Blobs[1]
		data, err := r.ReadBlobByMetadata(meta)
		require.NoError(t, err)
		assert.Equal(t, blobs[1], data)
	})

	t.Run("read all preserves order", func(t *testing.T) {
		allBlobs, err := r.ReadAllBlobs()
		require.NoError(t, err)
		require.Len(t, allBlobs, 3)

		for i, expected := range blobs {
			assert.Equal(t, expected, allBlobs[i].Data)
		}
	})
}
