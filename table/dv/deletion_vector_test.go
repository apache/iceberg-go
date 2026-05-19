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

package dv

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

// mockDVFile is a minimal iceberg.DataFile implementation with only the
// fields exercised by the DV reader: path, format, count, and the DV-specific
// offset/size/referenced-data-file pointers.
type mockDVFile struct {
	path               string
	format             iceberg.FileFormat
	count              int64
	referencedDataFile *string
	contentOffset      *int64
	contentSizeInBytes *int64
}

func (m *mockDVFile) FilePath() string                        { return m.path }
func (m *mockDVFile) FileFormat() iceberg.FileFormat          { return m.format }
func (m *mockDVFile) Count() int64                            { return m.count }
func (m *mockDVFile) ReferencedDataFile() *string             { return m.referencedDataFile }
func (m *mockDVFile) ContentOffset() *int64                   { return m.contentOffset }
func (m *mockDVFile) ContentSizeInBytes() *int64              { return m.contentSizeInBytes }
func (*mockDVFile) ContentType() iceberg.ManifestEntryContent { return iceberg.EntryContentPosDeletes }
func (*mockDVFile) Partition() map[int]any                    { return nil }
func (*mockDVFile) FileSizeBytes() int64                      { return 0 }
func (*mockDVFile) ColumnSizes() map[int]int64                { return nil }
func (*mockDVFile) ValueCounts() map[int]int64                { return nil }
func (*mockDVFile) NullValueCounts() map[int]int64            { return nil }
func (*mockDVFile) NaNValueCounts() map[int]int64             { return nil }
func (*mockDVFile) DistinctValueCounts() map[int]int64        { return nil }
func (*mockDVFile) LowerBoundValues() map[int][]byte          { return nil }
func (*mockDVFile) UpperBoundValues() map[int][]byte          { return nil }
func (*mockDVFile) KeyMetadata() []byte                       { return nil }
func (*mockDVFile) SplitOffsets() []int64                     { return nil }
func (*mockDVFile) EqualityFieldIDs() []int                   { return nil }
func (*mockDVFile) SortOrderID() *int                         { return nil }
func (*mockDVFile) SpecID() int32                             { return 0 }
func (*mockDVFile) FirstRowID() *int64                        { return nil }

func strPtr(s string) *string { return &s }

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
	// Callers of this wrapper all use the canonical 5-position fixture
	// (small-alternating-values-position-index.bin), so hardcoding "5" is
	// safe. Tests that exercise other bitmap sizes call
	// writePuffinWithDVBlobAndProps directly with their own cardinality.
	return writePuffinWithDVBlobAndProps(t, dir, dvBlobBytes, map[string]string{
		"referenced-data-file": "s3://bucket/data/data-001.parquet",
		"cardinality":          "5",
	})
}

// writePuffinWithDVBlobAndProps is the same as writePuffinWithDVBlob but with
// caller-supplied blob properties — used by the cardinality-validation tests
// to inject the spec-mandated `cardinality` property (or omit it).
func writePuffinWithDVBlobAndProps(t *testing.T, dir string, dvBlobBytes []byte, props map[string]string) (string, puffin.BlobMetadata) {
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
		Properties:     props,
	}, dvBlobBytes)
	require.NoError(t, err)
	require.NoError(t, w.Finish())

	return path, meta
}

func newDVTestFile(path string, count int64, offset, size *int64) *mockDVFile {
	return &mockDVFile{
		path:               path,
		format:             iceberg.PuffinFile,
		count:              count,
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

// Why: validates the DV envelope for the empty case — zero deleted positions.
// Condition: Java-produced DV with no positions.
// Assertion: no error, bitmap is empty.
func TestDeserializeDVEmpty(t *testing.T) {
	data := readDVTestData(t, "empty-position-index.bin")

	bm, err := DeserializeDV(data, -1)
	require.NoError(t, err)

	assert.True(t, bm.IsEmpty())
	assert.Equal(t, int64(0), bm.Cardinality())
}

// Why: exercises all three roaring container types (array, run, bitmap) through the DV envelope,
// which is the main cross-library bug source.
// Condition: Java-produced DV with 132,561 positions across 2 keys (0 and 1), each with 3 containers.
// Assertion: cardinality matches, representative positions from each container type are present.
func TestDeserializeDVAllContainerTypes(t *testing.T) {
	data := readDVTestData(t, "all-container-types-position-index.bin")

	bm, err := DeserializeDV(data, -1)
	require.NoError(t, err)

	assert.Equal(t, int64(132561), bm.Cardinality())

	// Key 0, array container: positions 5 and 7
	assert.True(t, bm.Contains(5))
	assert.True(t, bm.Contains(7))
	assert.False(t, bm.Contains(6))

	// Key 0, run container: positions 65537..66535 (container 1, 999 values)
	assert.True(t, bm.Contains(65537))
	assert.True(t, bm.Contains(66535))
	assert.False(t, bm.Contains(65536))
	assert.False(t, bm.Contains(66536))

	// Key 0, bitmap container: positions 131073..196606 (container 2, 65534 values)
	assert.True(t, bm.Contains(131073))
	assert.True(t, bm.Contains(196606))
	assert.False(t, bm.Contains(131072))

	// Key 1, array container: positions (1<<32)|10 and (1<<32)|20
	assert.True(t, bm.Contains((uint64(1)<<32)|10))
	assert.True(t, bm.Contains((uint64(1)<<32)|20))

	// Key 1, run container: starts at (1<<32)|65546
	assert.True(t, bm.Contains((uint64(1)<<32)|65546))

	// Key 1, bitmap container: starts at (1<<32)|131073
	assert.True(t, bm.Contains((uint64(1)<<32)|131073))
}

// Why: validates DV decoding with values that span both small and large 32-bit ranges in a single key.
// Condition: Java-produced DV with positions [100, 101, 2147483747, 2147483748].
// Assertion: cardinality is 4, all positions present, adjacent unset positions absent.
func TestDeserializeDVSmallAndLargeValues(t *testing.T) {
	data := readDVTestData(t, "small-and-large-values-position-index.bin")

	bm, err := DeserializeDV(data, -1)
	require.NoError(t, err)

	assert.Equal(t, int64(4), bm.Cardinality())
	assert.True(t, bm.Contains(100))
	assert.True(t, bm.Contains(101))
	assert.True(t, bm.Contains(2147483747)) // Integer.MAX_VALUE + 100
	assert.True(t, bm.Contains(2147483748)) // Integer.MAX_VALUE + 101
	assert.False(t, bm.Contains(99))
	assert.False(t, bm.Contains(102))
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
	_, err := ReadDV(iceio.LocalFS{}, &mockDVFile{
		path:   "s3://bucket/data/pos-del.parquet",
		format: iceberg.ParquetFile,
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

// Why: negative or absurdly large blob sizes should be rejected before allocation.
// Condition: ContentSizeInBytes set to -1.
// Assertion: returns an error containing "out of valid range".
func TestReadDVInvalidBlobSize(t *testing.T) {
	offset := int64(4)
	negSize := int64(-1)

	_, err := ReadDV(iceio.LocalFS{}, newDVTestFile("s3://bucket/data/dv.puffin", 0, &offset, &negSize))
	assert.ErrorContains(t, err, "out of valid range")
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

// TestReadDVCardinalityValidation pins the spec-mandated check that the
// puffin blob's `cardinality` property matches the bitmap's actual decoded
// cardinality. Catches truncated/partially-overwritten blobs whose CRC still
// validates over the bytes that are present (5 spec positions in the
// reference fixture).
func TestReadDVCardinalityValidation(t *testing.T) {
	dvBlobBytes := readDVTestData(t, "small-alternating-values-position-index.bin")

	t.Run("matching cardinality passes", func(t *testing.T) {
		dir := t.TempDir()
		path, meta := writePuffinWithDVBlobAndProps(t, dir, dvBlobBytes, map[string]string{
			"referenced-data-file": "s3://bucket/data/data-001.parquet",
			"cardinality":          "5",
		})
		offset, size := meta.Offset, meta.Length
		bm, err := ReadDV(iceio.LocalFS{}, newDVTestFile(path, 5, &offset, &size))
		require.NoError(t, err)
		assert.Equal(t, int64(5), bm.Cardinality())
	})

	t.Run("mismatched cardinality is rejected", func(t *testing.T) {
		dir := t.TempDir()
		path, meta := writePuffinWithDVBlobAndProps(t, dir, dvBlobBytes, map[string]string{
			"referenced-data-file": "s3://bucket/data/data-001.parquet",
			// Bitmap actually has 5 positions; claim 99.
			"cardinality": "99",
		})
		offset, size := meta.Offset, meta.Length
		_, err := ReadDV(iceio.LocalFS{}, newDVTestFile(path, 5, &offset, &size))
		require.Error(t, err)
		// Pin the specific error path: DeserializeDV's "cardinality
		// mismatch", not the helper's "invalid cardinality" parse error.
		assert.Contains(t, err.Error(), "cardinality mismatch")
	})

	t.Run("missing property is tolerated with a warning", func(t *testing.T) {
		// No `cardinality` property — spec deviation tolerated for read
		// compatibility with third-party writers that omit it. ReadDV logs
		// a warning (see slog.Warn) and falls back to CRC-only validation
		// inside DeserializeDV. Strict enforcement is deferred until the
		// Go writer-side coverage lands.
		dir := t.TempDir()
		path, meta := writePuffinWithDVBlob(t, dir, dvBlobBytes)
		offset, size := meta.Offset, meta.Length
		bm, err := ReadDV(iceio.LocalFS{}, newDVTestFile(path, 5, &offset, &size))
		require.NoError(t, err)
		assert.Equal(t, int64(5), bm.Cardinality())
	})

	// Note: the previous "malformed property is rejected" and "negative
	// cardinality rejected on read" subtests were removed once the puffin
	// writer started rejecting non-numeric and negative cardinality values
	// at AddBlob time (via ParseUint). The reader-side checks in
	// blobCardinality stay as belt-and-suspenders for third-party-written
	// files but are no longer reachable from any in-tree writer path used
	// to construct test fixtures. Writer-side coverage:
	//   TestWriterValidation/deletion_vector_non-numeric_cardinality
	//   TestWriterValidation/deletion_vector_negative_cardinality
	// (both in puffin_test.go).

	t.Run("manifest offset not found in footer is rejected with a precise error", func(t *testing.T) {
		// Manifest entry points at an in-bounds offset that doesn't match
		// any footer blob's starting position. Surfaces as the helper's
		// "no blob in puffin footer" error rather than the deeper CRC
		// error from DeserializeDV — clearer signal for diagnosing
		// manifest corruption.
		dir := t.TempDir()
		path, meta := writePuffinWithDVBlob(t, dir, dvBlobBytes)
		// Offset shifted by 1 keeps the ReadAt range inside the blob
		// region (no "extends into footer" error) while guaranteeing no
		// footer blob starts at that exact offset.
		wrongOffset, smallSize := meta.Offset+1, int64(8)
		_, err := ReadDV(iceio.LocalFS{}, newDVTestFile(path, 5, &wrongOffset, &smallSize))
		require.Error(t, err)
		assert.Contains(t, err.Error(), "no blob in puffin footer")
	})

	t.Run("manifest size disagrees with footer length at matching offset", func(t *testing.T) {
		// Same starting offset but a different length — a distinct writer
		// bug from "no blob at offset". The helper surfaces a precise
		// "blob at offset N has length M, manifest says K" message rather
		// than the broader "no blob" path.
		dir := t.TempDir()
		path, meta := writePuffinWithDVBlob(t, dir, dvBlobBytes)
		offset := meta.Offset
		wrongSize := meta.Length - 1
		_, err := ReadDV(iceio.LocalFS{}, newDVTestFile(path, 5, &offset, &wrongSize))
		require.Error(t, err)
		assert.Contains(t, err.Error(), "manifest says")
	})
}
