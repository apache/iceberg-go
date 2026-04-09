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
	"encoding/binary"
	"errors"
	"hash/crc32"
	"os"
	"path/filepath"
	"testing"

	"github.com/apache/iceberg-go"
	iceio "github.com/apache/iceberg-go/io"
	"github.com/apache/iceberg-go/puffin"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func readDVTestData(t *testing.T, name string) []byte {
	t.Helper()
	data, err := os.ReadFile(filepath.Join("testdata", "deletes", name))
	require.NoError(t, err)

	return data
}

func wrapDVPayloadForTest(bitmapBytes []byte) []byte {
	bitmapDataLength := dvMagicSize + len(bitmapBytes)
	totalSize := dvLengthSize + bitmapDataLength + dvCRCSize
	out := make([]byte, totalSize)

	binary.BigEndian.PutUint32(out[0:dvLengthSize], uint32(bitmapDataLength))
	binary.LittleEndian.PutUint32(out[dvLengthSize:dvLengthSize+dvMagicSize], DVMagicNumber)
	copy(out[dvLengthSize+dvMagicSize:], bitmapBytes)

	crc := crc32.ChecksumIEEE(out[dvLengthSize : totalSize-dvCRCSize])
	binary.BigEndian.PutUint32(out[totalSize-dvCRCSize:], crc)

	return out
}

func writePuffinWithDVBlob(t *testing.T, dir string, dvBlobBytes []byte) (string, puffin.BlobMetadata) {
	t.Helper()

	path := filepath.Join(dir, "test-dv.puffin")
	f, err := os.Create(path)
	require.NoError(t, err)
	defer f.Close()

	w, err := puffin.NewWriter(f)
	require.NoError(t, err)

	meta, err := w.AddBlob(puffin.BlobMetadataInput{
		Type:           puffin.BlobTypeDeletionVector,
		SnapshotID:     -1,
		SequenceNumber: -1,
		Fields:         []int32{2147483546},
		Properties: map[string]string{
			"referenced-data-file": "s3://bucket/data/data-001.parquet",
		},
	}, dvBlobBytes)
	require.NoError(t, err)
	require.NoError(t, w.Finish())

	return path, meta
}

func newDVTestFile(path string, count int64, offset, size *int64) *dvMockDataFile {
	return &dvMockDataFile{
		mockDataFile: mockDataFile{
			path:   path,
			format: iceberg.PuffinFile,
			count:  count,
		},
		referencedDataFile: strPtr("s3://bucket/data/data-001.parquet"),
		contentOffset:      offset,
		contentSizeInBytes: size,
	}
}

type failingOpenFS struct {
	err error
}

func (f failingOpenFS) Open(string) (iceio.File, error) { return nil, f.err }
func (f failingOpenFS) Remove(string) error             { return nil }

// Why: proves the DV envelope can successfully decode a representative valid blob.
// Condition: Java-produced DV with deleted positions [1, 3, 5, 7, 9] and cardinality validation disabled.
// Assertion: no error, cardinality is 5, expected positions are present, and an adjacent unset position is absent.
func TestDeserializeDV(t *testing.T) {
	data := readDVTestData(t, "small-alternating-values-position-index.bin")

	bm, err := DeserializeDV(data, -1)
	require.NoError(t, err)

	assert.Equal(t, int64(5), bm.Cardinality())
	assert.True(t, bm.Contains(1))
	assert.True(t, bm.Contains(9))
	assert.False(t, bm.Contains(2))
}

// Why: truncated payloads should fail cleanly before any slicing or decoding happens.
// Condition: fewer than the minimum 12 bytes required for length, magic, and CRC.
// Assertion: returns an error containing "too short".
func TestDeserializeDVTooShort(t *testing.T) {
	_, err := DeserializeDV([]byte{0, 1, 2}, -1)
	assert.ErrorContains(t, err, "too short")
}

// Why: the DV envelope owns the outer length field and must reject mismatches.
// Condition: payload has only the minimum bytes, but the encoded length says 99.
// Assertion: returns an error containing "length mismatch".
func TestDeserializeDVBadLength(t *testing.T) {
	data := make([]byte, dvMinSize)
	binary.BigEndian.PutUint32(data[0:dvLengthSize], 99)

	_, err := DeserializeDV(data, -1)
	assert.ErrorContains(t, err, "length mismatch")
}

// Why: the reader must reject non-DV payloads even if the framing size looks valid.
// Condition: encoded length is correct for a magic-only payload, but the magic value is not DVMagicNumber.
// Assertion: returns an error containing "invalid deletion vector magic".
func TestDeserializeDVBadMagic(t *testing.T) {
	data := make([]byte, dvMinSize)
	binary.BigEndian.PutUint32(data[0:dvLengthSize], dvMagicSize)
	binary.LittleEndian.PutUint32(data[dvLengthSize:dvLengthSize+dvMagicSize], 0xFFFFFFFF)

	_, err := DeserializeDV(data, -1)
	assert.ErrorContains(t, err, "invalid deletion vector magic")
}

// Why: CRC is the DV format's corruption check and should fail before bitmap decoding.
// Condition: start from a valid golden DV blob, then flip one byte in the stored CRC.
// Assertion: returns an error containing "CRC mismatch".
func TestDeserializeDVBadCRC(t *testing.T) {
	data := append([]byte(nil), readDVTestData(t, "small-alternating-values-position-index.bin")...)
	data[len(data)-1] ^= 0xFF

	_, err := DeserializeDV(data, -1)
	assert.ErrorContains(t, err, "CRC mismatch")
}

// Why: DV tests only need to prove that inner roaring decode failures are surfaced with DV context.
// Condition: valid DV envelope containing invalid roaring bitmap bytes and a matching CRC.
// Assertion: returns an error containing "deserialize deletion vector bitmap".
func TestDeserializeDVWrapsBitmapDecodeError(t *testing.T) {
	data := wrapDVPayloadForTest([]byte{0x00, 0x01, 0x02})

	_, err := DeserializeDV(data, -1)
	assert.ErrorContains(t, err, "deserialize deletion vector bitmap")
}

// Why: manifest metadata and DV content should agree on the number of deleted rows.
// Condition: valid DV blob with 5 positions, but expected cardinality is set to 999.
// Assertion: returns an error containing "cardinality mismatch".
func TestDeserializeDVCardinalityMismatch(t *testing.T) {
	data := readDVTestData(t, "small-alternating-values-position-index.bin")

	_, err := DeserializeDV(data, 999)
	assert.ErrorContains(t, err, "cardinality mismatch")
}

// Why: this is the production path that ties together Puffin I/O, blob range selection, and DV parsing.
// Condition: real Puffin file containing one valid DV blob, with offset and size taken from Puffin metadata.
// Assertion: no error, cardinality is 5, expected positions are present, and an unset position is absent.
func TestReadDV(t *testing.T) {
	dvBlobBytes := readDVTestData(t, "small-alternating-values-position-index.bin")

	dir := t.TempDir()
	path, meta := writePuffinWithDVBlob(t, dir, dvBlobBytes)

	offset, size := meta.Offset, meta.Length
	bm, err := ReadDV(iceio.LocalFS{}, newDVTestFile(path, 5, &offset, &size))
	require.NoError(t, err)

	assert.Equal(t, int64(5), bm.Cardinality())
	assert.True(t, bm.Contains(1))
	assert.True(t, bm.Contains(9))
	assert.False(t, bm.Contains(2))
}

// Why: ReadDV should reject callers that pass the wrong file type before doing any I/O.
// Condition: DataFile reports Parquet format instead of Puffin.
// Assertion: returns an error containing "expected PUFFIN format".
func TestReadDVWrongFormat(t *testing.T) {
	_, err := ReadDV(iceio.LocalFS{}, &dvMockDataFile{
		mockDataFile: mockDataFile{
			path:   "s3://bucket/data/pos-del.parquet",
			format: iceberg.ParquetFile,
		},
	})
	assert.ErrorContains(t, err, "expected PUFFIN format")
}

// Why: DV manifest entries require both content offset and content size to find the blob.
// Condition: one subtest omits offset and another omits size.
// Assertion: each case returns an error containing "missing ContentOffset/ContentSizeInBytes".
func TestReadDVMissingContentMetadata(t *testing.T) {
	t.Run("nil offset", func(t *testing.T) {
		size := int64(50)

		_, err := ReadDV(iceio.LocalFS{}, newDVTestFile("s3://bucket/data/dv.puffin", 0, nil, &size))
		assert.ErrorContains(t, err, "missing ContentOffset/ContentSizeInBytes")
	})

	t.Run("nil size", func(t *testing.T) {
		offset := int64(4)

		_, err := ReadDV(iceio.LocalFS{}, newDVTestFile("s3://bucket/data/dv.puffin", 0, &offset, nil))
		assert.ErrorContains(t, err, "missing ContentOffset/ContentSizeInBytes")
	})
}

// Why: storage open failures should be wrapped with file-path context by ReadDV.
// Condition: custom IO implementation returns a fixed error from Open.
// Assertion: error contains both "open DV file missing.puffin" and the underlying "boom" message.
func TestReadDVOpenError(t *testing.T) {
	offset, size := int64(4), int64(16)

	_, err := ReadDV(failingOpenFS{err: errors.New("boom")}, newDVTestFile("missing.puffin", 0, &offset, &size))
	assert.ErrorContains(t, err, "open DV file missing.puffin")
	assert.ErrorContains(t, err, "boom")
}

// Why: ReadDV should surface Puffin container parse failures distinctly from DV parse failures.
// Condition: file exists and can be opened, but its contents are not a valid Puffin file.
// Assertion: returns an error containing "create puffin reader".
func TestReadDVInvalidPuffin(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "invalid.puffin")
	require.NoError(t, os.WriteFile(path, []byte("not a puffin file"), 0o644))

	offset, size := int64(4), int64(16)
	_, err := ReadDV(iceio.LocalFS{}, newDVTestFile(path, 0, &offset, &size))
	assert.ErrorContains(t, err, "create puffin reader")
}

// Why: manifest-provided blob ranges must point into the Puffin blob area, not arbitrary offsets.
// Condition: valid Puffin file, but content offset is forced to 0, which points before the blob region.
// Assertion: returns an error containing "read DV blob at offset 0".
func TestReadDVInvalidBlobRange(t *testing.T) {
	dvBlobBytes := readDVTestData(t, "small-alternating-values-position-index.bin")

	dir := t.TempDir()
	path, meta := writePuffinWithDVBlob(t, dir, dvBlobBytes)

	offset, size := int64(0), meta.Length
	_, err := ReadDV(iceio.LocalFS{}, newDVTestFile(path, 5, &offset, &size))
	assert.ErrorContains(t, err, "read DV blob at offset 0")
}

// Why: ReadDV is responsible for passing dvFile.Count through to DV cardinality validation.
// Condition: valid Puffin blob with 5 deleted positions, but DataFile count is set to 4.
// Assertion: returns an error containing "cardinality mismatch".
func TestReadDVCardinalityMismatch(t *testing.T) {
	dvBlobBytes := readDVTestData(t, "small-alternating-values-position-index.bin")

	dir := t.TempDir()
	path, meta := writePuffinWithDVBlob(t, dir, dvBlobBytes)

	offset, size := meta.Offset, meta.Length
	_, err := ReadDV(iceio.LocalFS{}, newDVTestFile(path, 4, &offset, &size))
	assert.ErrorContains(t, err, "cardinality mismatch")
}
