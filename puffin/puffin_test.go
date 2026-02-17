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
	"math"
	"testing"

	"github.com/apache/iceberg-go/puffin"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// --- Test Helpers ---

func newWriter() (*puffin.Writer, *bytes.Buffer) {
	buf := &bytes.Buffer{}
	w, _ := puffin.NewWriter(buf)

	return w, buf
}

func newReader(t *testing.T, buf *bytes.Buffer) *puffin.Reader {
	r, err := puffin.NewReader(bytes.NewReader(buf.Bytes()))
	require.NoError(t, err)

	return r
}

func defaultBlobInput() puffin.BlobMetadataInput {
	return puffin.BlobMetadataInput{
		Type:   puffin.BlobTypeDataSketchesTheta,
		Fields: []int32{},
	}
}

func validFile() []byte {
	w, buf := newWriter()
	w.Finish()

	return buf.Bytes()
}

func validFileWithBlob() []byte {
	w, buf := newWriter()
	w.AddBlob(defaultBlobInput(), []byte("test data"))
	w.Finish()

	return buf.Bytes()
}

// --- Tests ---

// TestRoundTrip verifies that data written by Writer can be read back by Reader.
// This is the core integration test ensuring the puffin format is correctly implemented.
func TestRoundTrip(t *testing.T) {
	blob1Data := []byte("theta sketch data here")
	blob2Data := []byte("another blob with different content")

	w, buf := newWriter()
	w.AddProperties(map[string]string{"test-property": "test-value"})

	meta1, err := w.AddBlob(puffin.BlobMetadataInput{
		Type:           puffin.BlobTypeDataSketchesTheta,
		SnapshotID:     123,
		SequenceNumber: 1,
		Fields:         []int32{1, 2, 3},
		Properties:     map[string]string{"ndv": "1000"},
	}, blob1Data)
	require.NoError(t, err)

	meta2, err := w.AddBlob(puffin.BlobMetadataInput{
		Type:           puffin.BlobTypeDeletionVector,
		SnapshotID:     -1,
		SequenceNumber: -1,
		Fields:         []int32{},
		Properties:     map[string]string{"referenced-data-file": "data/file.parquet"},
	}, blob2Data)
	require.NoError(t, err)
	require.NoError(t, w.Finish())

	r := newReader(t, buf)
	blobs := r.Blobs()
	props := r.Properties()

	assert.Len(t, blobs, 2)
	assert.Equal(t, "test-value", props["test-property"])
	assert.Contains(t, props[puffin.CreatedBy], "iceberg-go")

	// Verify blob 1
	assert.Equal(t, puffin.BlobTypeDataSketchesTheta, blobs[0].Type)
	assert.Equal(t, int64(123), blobs[0].SnapshotID)
	assert.Equal(t, int64(1), blobs[0].SequenceNumber)
	assert.Equal(t, []int32{1, 2, 3}, blobs[0].Fields)
	assert.Equal(t, meta1.Offset, blobs[0].Offset)
	assert.Equal(t, meta1.Length, blobs[0].Length)
	assert.Equal(t, "1000", blobs[0].Properties["ndv"])

	// Verify blob 2
	assert.Equal(t, puffin.BlobTypeDeletionVector, blobs[1].Type)
	assert.Equal(t, int64(-1), blobs[1].SnapshotID)
	assert.Equal(t, int64(-1), blobs[1].SequenceNumber)
	assert.Equal(t, meta2.Offset, blobs[1].Offset)
	assert.Equal(t, meta2.Length, blobs[1].Length)

	// Verify data
	blobData1, _ := r.ReadBlob(0)
	assert.Equal(t, blob1Data, blobData1.Data)

	blobData2, _ := r.ReadBlob(1)
	assert.Equal(t, blob2Data, blobData2.Data)

	allBlobs, _ := r.ReadAllBlobs()
	assert.Len(t, allBlobs, 2)
	assert.Equal(t, blob1Data, allBlobs[0].Data)
	assert.Equal(t, blob2Data, allBlobs[1].Data)
}

// TestEmptyFile verifies that a puffin file with no blobs is valid.
// Empty files are valid per spec and used when no statistics exist yet.
func TestEmptyFile(t *testing.T) {
	w, buf := newWriter()
	require.NoError(t, w.Finish())

	r := newReader(t, buf)
	assert.Len(t, r.Blobs(), 0)
	assert.Contains(t, r.Properties()[puffin.CreatedBy], "iceberg-go")

	blobs, err := r.ReadAllBlobs()
	require.NoError(t, err)
	assert.Nil(t, blobs)
}

// TestEmptyBlobData verifies that a zero-length blob is valid.
// Some blob types may legitimately have empty content.
func TestEmptyBlobData(t *testing.T) {
	w, buf := newWriter()
	meta, err := w.AddBlob(defaultBlobInput(), []byte{})
	require.NoError(t, err)
	assert.Equal(t, int64(0), meta.Length)
	w.Finish()

	r := newReader(t, buf)
	blob, _ := r.ReadBlob(0)
	assert.Empty(t, blob.Data)
}

// TestLargeFooter verifies reading works when footer exceeds initial 8KB read buffer.
// This exercises the code path where footer requires a second read from storage.
func TestLargeFooter(t *testing.T) {
	w, buf := newWriter()
	numBlobs := 200
	for i := range numBlobs {
		w.AddBlob(puffin.BlobMetadataInput{
			Type:       puffin.BlobTypeDataSketchesTheta,
			SnapshotID: int64(i),
			Fields:     []int32{1, 2, 3, 4, 5},
			Properties: map[string]string{"key": "value-to-increase-footer-size"},
		}, []byte("blob"))
	}
	w.Finish()

	r := newReader(t, buf)
	assert.Len(t, r.Blobs(), numBlobs)
}

// TestWriterValidation verifies that Writer rejects invalid input.
// Proper validation prevents corrupt files and provides clear error messages.
func TestWriterValidation(t *testing.T) {
	// nil writer: Ensures graceful failure when no underlying writer is provided.
	t.Run("nil writer", func(t *testing.T) {
		_, err := puffin.NewWriter(nil)
		assert.ErrorContains(t, err, "nil")
	})

	// missing type: Blob type is required per spec to identify the blob format.
	t.Run("missing type", func(t *testing.T) {
		w, _ := newWriter()
		_, err := w.AddBlob(puffin.BlobMetadataInput{Type: "", Fields: []int32{}}, []byte("x"))
		assert.ErrorContains(t, err, "type")
	})

	// nil fields: Fields slice must be non-nil per spec (empty slice is valid).
	t.Run("nil fields", func(t *testing.T) {
		w, _ := newWriter()
		_, err := w.AddBlob(puffin.BlobMetadataInput{Type: puffin.BlobTypeDataSketchesTheta, Fields: nil}, []byte("x"))
		assert.ErrorContains(t, err, "fields")
	})

	// add blob after finish: Enforces writer state machine - no writes after finalization.
	t.Run("add blob after finish", func(t *testing.T) {
		w, _ := newWriter()
		w.Finish()
		_, err := w.AddBlob(defaultBlobInput(), []byte("x"))
		assert.ErrorContains(t, err, "finalized")
	})

	// double finish: Prevents accidental double-write of footer corrupting the file.
	t.Run("double finish", func(t *testing.T) {
		w, _ := newWriter()
		w.Finish()
		assert.ErrorContains(t, w.Finish(), "finalized")
	})

	// deletion vector invalid snapshot-id: Spec requires snapshot-id=-1 for deletion vectors.
	t.Run("deletion vector invalid snapshot-id", func(t *testing.T) {
		w, _ := newWriter()
		_, err := w.AddBlob(puffin.BlobMetadataInput{
			Type: puffin.BlobTypeDeletionVector, SnapshotID: 123, SequenceNumber: -1, Fields: []int32{},
		}, []byte("x"))
		assert.ErrorContains(t, err, "snapshot-id")
	})

	// deletion vector invalid sequence-number: Spec requires sequence-number=-1 for deletion vectors.
	t.Run("deletion vector invalid sequence-number", func(t *testing.T) {
		w, _ := newWriter()
		_, err := w.AddBlob(puffin.BlobMetadataInput{
			Type: puffin.BlobTypeDeletionVector, SnapshotID: -1, SequenceNumber: 5, Fields: []int32{},
		}, []byte("x"))
		assert.ErrorContains(t, err, "sequence-number")
	})
}

// TestSetCreatedBy verifies the SetCreatedBy method.
// Allows applications to identify themselves in puffin files for debugging.
func TestSetCreatedBy(t *testing.T) {
	// custom value: Verifies custom application identifiers are preserved in footer.
	t.Run("custom value", func(t *testing.T) {
		w, buf := newWriter()
		require.NoError(t, w.SetCreatedBy("MyApp 1.0"))
		w.Finish()
		r := newReader(t, buf)
		assert.Equal(t, "MyApp 1.0", r.Properties()[puffin.CreatedBy])
	})

	// empty rejected: Empty identifier provides no value and likely indicates a bug.
	t.Run("empty rejected", func(t *testing.T) {
		w, _ := newWriter()
		assert.ErrorContains(t, w.SetCreatedBy(""), "empty")
	})

	// after finish rejected: Enforces writer state machine consistency.
	t.Run("after finish rejected", func(t *testing.T) {
		w, _ := newWriter()
		w.Finish()
		assert.ErrorContains(t, w.SetCreatedBy("x"), "finalized")
	})
}

// TestClearProperties verifies the ClearProperties method.
// Allows resetting properties before finishing if initial values were wrong.
func TestClearProperties(t *testing.T) {
	w, buf := newWriter()
	w.AddProperties(map[string]string{"key": "value"})
	w.ClearProperties()
	w.Finish()

	r := newReader(t, buf)
	_, exists := r.Properties()["key"]
	assert.False(t, exists)
}

// TestAddPropertiesAfterFinish verifies AddProperties rejects calls after finish.
// Enforces writer state machine - properties cannot be added after footer is written.
func TestAddPropertiesAfterFinish(t *testing.T) {
	w, _ := newWriter()
	w.Finish()
	assert.ErrorContains(t, w.AddProperties(map[string]string{"k": "v"}), "finalized")
}

// TestReaderInvalidFile verifies that Reader rejects invalid/corrupt files.
// Early detection of corruption prevents silent data loss or security issues.
func TestReaderInvalidFile(t *testing.T) {
	tests := []struct {
		name    string
		data    func() []byte
		wantErr string
	}{
		// file too small: Minimum valid puffin file has header magic + footer, rejects truncated files.
		{"file too small", func() []byte { return []byte("tiny") }, "too small"},
		// invalid header magic: First 4 bytes must be 'PFA1' to identify puffin format.
		{"invalid header magic", func() []byte {
			d := validFile()
			d[0] = 'X'

			return d
		}, "magic"},
		// invalid trailing magic: Last 4 bytes must be 'PFA1' to detect truncation.
		{"invalid trailing magic", func() []byte {
			d := validFile()
			d[len(d)-1] = 'X'

			return d
		}, "magic"},
		// invalid footer start magic: Footer section must start with 'PFA1' for integrity.
		{"invalid footer start magic", func() []byte {
			d := validFile()
			d[4] = 'X'

			return d
		}, "magic"},
		// unknown flags: Reject files with unsupported features to avoid misinterpretation.
		{"unknown flags", func() []byte {
			d := validFile()
			d[len(d)-8] = 0x80

			return d
		}, "unknown"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			data := tt.data()
			_, err := puffin.NewReader(bytes.NewReader(data))
			assert.ErrorContains(t, err, tt.wantErr)
		})
	}

	// nil reader: Ensures graceful failure when no underlying reader is provided.
	t.Run("nil reader", func(t *testing.T) {
		_, err := puffin.NewReader(nil)
		assert.ErrorContains(t, err, "nil")
	})
}

// TestReaderBlobAccess verifies blob access methods work correctly.
// Tests the primary API for retrieving blob data from puffin files.
func TestReaderBlobAccess(t *testing.T) {
	w, buf := newWriter()
	blobs := [][]byte{[]byte("first"), []byte("second"), []byte("third")}
	for _, b := range blobs {
		w.AddBlob(defaultBlobInput(), b)
	}
	w.Finish()
	r := newReader(t, buf)

	// read by index: Primary access method for retrieving blobs sequentially.
	t.Run("read by index", func(t *testing.T) {
		for i, expected := range blobs {
			blob, _ := r.ReadBlob(i)
			assert.Equal(t, expected, blob.Data)
		}
	})

	// index out of range: Prevents panic and provides clear error for invalid indices.
	t.Run("index out of range", func(t *testing.T) {
		_, err := r.ReadBlob(-1)
		assert.Error(t, err)
		_, err = r.ReadBlob(100)
		assert.Error(t, err)
	})

	// read by metadata: Allows direct access when caller has metadata from external source.
	t.Run("read by metadata", func(t *testing.T) {
		data, _ := r.ReadBlobByMetadata(r.Blobs()[1])
		assert.Equal(t, blobs[1], data)
	})

	// read all preserves order: Ensures blobs are returned in same order as written.
	t.Run("read all preserves order", func(t *testing.T) {
		all, _ := r.ReadAllBlobs()
		for i, expected := range blobs {
			assert.Equal(t, expected, all[i].Data)
		}
	})
}

// TestReadBlobByMetadataValidation verifies validation of blob metadata.
// Prevents reading garbage data when metadata is corrupted or crafted maliciously.
func TestReadBlobByMetadataValidation(t *testing.T) {
	data := validFileWithBlob()
	r, _ := puffin.NewReader(bytes.NewReader(data))

	tests := []struct {
		name    string
		meta    puffin.BlobMetadata
		wantErr string
	}{
		// empty type: Type is required to interpret blob content correctly.
		{"empty type", puffin.BlobMetadata{Type: "", Offset: 4, Length: 1}, "type"},
		// offset before header: Prevents reading magic bytes as blob data.
		{"offset before header", puffin.BlobMetadata{Type: "t", Offset: 0, Length: 1}, "header"},
		// negative length: Invalid length could cause allocation issues.
		{"negative length", puffin.BlobMetadata{Type: "t", Offset: 4, Length: -1}, "length"},
		// extends into footer: Prevents reading footer JSON as blob data.
		{"extends into footer", puffin.BlobMetadata{Type: "t", Offset: 4, Length: 9999}, "footer"},
		// overflow: Prevents integer overflow attacks in offset+length calculation.
		{"overflow", puffin.BlobMetadata{Type: "t", Offset: math.MaxInt64, Length: 1}, "overflow"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := r.ReadBlobByMetadata(tt.meta)
			assert.ErrorContains(t, err, tt.wantErr)
		})
	}
}

// TestReadAt verifies the ReadAt method (io.ReaderAt implementation).
// Enables partial reads for streaming or when only specific byte ranges are needed.
func TestReadAt(t *testing.T) {
	w, buf := newWriter()
	meta, _ := w.AddBlob(defaultBlobInput(), []byte("hello world"))
	w.Finish()
	r := newReader(t, buf)

	// valid range: Verifies partial blob reads work correctly.
	t.Run("valid range", func(t *testing.T) {
		data := make([]byte, 5)
		n, err := r.ReadAt(data, meta.Offset)
		require.NoError(t, err)
		assert.Equal(t, 5, n)
		assert.Equal(t, []byte("hello"), data)
	})

	// extends into footer: Prevents reading footer metadata as blob content.
	t.Run("extends into footer", func(t *testing.T) {
		data := make([]byte, buf.Len())
		_, err := r.ReadAt(data, 4)
		assert.ErrorContains(t, err, "footer")
	})

	// offset before header: Prevents reading file magic as blob content.
	t.Run("offset before header", func(t *testing.T) {
		data := make([]byte, 4)
		_, err := r.ReadAt(data, 0)
		assert.ErrorContains(t, err, "header")
	})
}
