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

package encryption_test

import (
	"bytes"
	"crypto/aes"
	"crypto/cipher"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"math"
	mrand "math/rand/v2"
	"slices"
	"testing"
	"time"

	"github.com/apache/iceberg-go/encryption"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"golang.org/x/sync/errgroup"
)

// AES GCM Stream layout constants, restated here from the spec rather than
// imported, so the tests do not share them with the code under test.
const (
	plainBlockSize = 1 << 20 // the one block size Java and iceberg-rust accept
	blockOverhead  = 12 + 16 // nonce + GCM tag
	headerLength   = 4 + 4   // "AGS1" magic + little-endian block length
	testKeyID      = "table-key"
)

func newTestManager(opts ...encryption.StandardManagerOption) *encryption.StandardEncryptionManager {
	return encryption.NewStandardEncryptionManager(opts...)
}

// testData returns n deterministic pseudo-random bytes, so that blocks differ
// from one another and a swapped or misplaced block is detectable.
func testData(n int) []byte {
	r := mrand.New(mrand.NewPCG(1, 2))
	b := make([]byte, n)
	for i := range b {
		b[i] = byte(r.Uint32())
	}

	return b
}

func encryptAll(t *testing.T, mgr *encryption.StandardEncryptionManager, plaintext []byte) (ciphertext []byte, keyMetadata encryption.EncryptionKeyMetadata) {
	t.Helper()
	fw := &memFileWriter{}
	out, err := mgr.NewEncryptedOutputFile(t.Context(), fw, testKeyID)
	require.NoError(t, err)
	_, err = out.Write(plaintext)
	require.NoError(t, err)
	require.NoError(t, out.Close())

	return fw.Bytes(), out.KeyMetadata()
}

func decryptAll(t *testing.T, mgr *encryption.StandardEncryptionManager, ciphertext []byte, keyMetadata encryption.EncryptionKeyMetadata) []byte {
	t.Helper()
	in, err := mgr.NewDecryptedInputFile(t.Context(), newMemFile(ciphertext), keyMetadata)
	require.NoError(t, err)
	data, err := io.ReadAll(in)
	require.NoError(t, err)

	return data
}

// expectedStreamLength is the length of the encrypted stream for n plaintext
// bytes: the header, then ceil(n/block) blocks of nonce, ciphertext and tag,
// and always at least block 0 (an empty file is a header plus one empty block).
func expectedStreamLength(n int) int {
	blocks := max(1, (n+plainBlockSize-1)/plainBlockSize)

	return headerLength + n + blocks*blockOverhead
}

// ---------------------------------------------------------------------------
// Independent AGS1 / StandardKeyMetadata codecs, built only from the standard
// library and the spec text so that they share no code, and no bugs, with the
// package under test.
// ---------------------------------------------------------------------------

func avroLong(v int64) []byte { return binary.AppendVarint(nil, v) }

func avroBytes(b []byte) []byte { return append(avroLong(int64(len(b))), b...) }

// avroKeyMetadata hand-encodes StandardKeyMetadata with both optional fields
// present: version byte 1, then the Avro record
// {encryption_key: bytes, aad_prefix: ["null", bytes], file_length: ["null", long]}.
func avroKeyMetadata(dek, aadPrefix []byte, fileLength int64) []byte {
	return slices.Concat([]byte{1}, avroBytes(dek), avroLong(1), avroBytes(aadPrefix), avroLong(1), avroLong(fileLength))
}

type parsedKeyMetadata struct {
	dek, aadPrefix []byte
	fileLength     int64
}

func parseKeyMetadata(t *testing.T, km []byte) parsedKeyMetadata {
	t.Helper()
	require.NotEmpty(t, km)
	require.Equal(t, byte(1), km[0], "version byte")
	rest := km[1:]

	readLong := func() int64 {
		v, n := binary.Varint(rest)
		require.Positive(t, n, "malformed varint")
		rest = rest[n:]

		return v
	}
	readBytes := func() []byte {
		n := readLong()
		require.GreaterOrEqual(t, n, int64(0))
		require.LessOrEqual(t, n, int64(len(rest)))
		b := rest[:n]
		rest = rest[n:]

		return b
	}

	var p parsedKeyMetadata
	p.dek = readBytes()
	require.EqualValues(t, 1, readLong(), "aad_prefix must be present (union branch 1)")
	p.aadPrefix = readBytes()
	require.EqualValues(t, 1, readLong(), "file_length must be present (union branch 1)")
	p.fileLength = readLong()
	require.Empty(t, rest, "trailing bytes after the record")

	return p
}

func newGCM(t *testing.T, dek []byte) cipher.AEAD {
	t.Helper()
	block, err := aes.NewCipher(dek)
	require.NoError(t, err)
	gcm, err := cipher.NewGCM(block)
	require.NoError(t, err)

	return gcm
}

// blockAAD builds the spec's AAD, aadPrefix || little-endian uint32 block index.
func blockAAD(aadPrefix []byte, index uint32) []byte {
	return binary.LittleEndian.AppendUint32(slices.Clone(aadPrefix), index)
}

// decodeAGS1 reads an AGS1 stream the way the spec describes.
func decodeAGS1(t *testing.T, stream, dek, aadPrefix []byte) []byte {
	t.Helper()
	require.GreaterOrEqual(t, len(stream), headerLength)
	require.Equal(t, "AGS1", string(stream[:4]), "magic")
	require.EqualValues(t, plainBlockSize, binary.LittleEndian.Uint32(stream[4:8]), "plain block length")

	gcm := newGCM(t, dek)
	var plaintext []byte
	body := stream[headerLength:]
	for index := uint32(0); len(body) > 0; index++ {
		block := body[:min(len(body), plainBlockSize+blockOverhead)]
		body = body[len(block):]
		require.GreaterOrEqual(t, len(block), blockOverhead, "block %d", index)

		nonce, sealed := block[:12], block[12:]
		opened, err := gcm.Open(nil, nonce, sealed, blockAAD(aadPrefix, index))
		require.NoError(t, err, "block %d", index)
		plaintext = append(plaintext, opened...)
	}

	return plaintext
}

// encodeAGS1 writes an AGS1 stream the way Java does: block 0 is always
// written, even for empty plaintext. The real writer draws a random nonce per
// block; this one derives each block's nonce from its index (so it still
// differs from block to block) to make the output reproducible. It is only
// fit for tests.
func encodeAGS1(t *testing.T, plaintext, dek, aadPrefix []byte) []byte {
	t.Helper()
	gcm := newGCM(t, dek)

	stream := append([]byte("AGS1"), binary.LittleEndian.AppendUint32(nil, plainBlockSize)...)
	for index := uint32(0); index == 0 || len(plaintext) > 0; index++ {
		n := min(len(plaintext), plainBlockSize)
		nonce := bytes.Repeat([]byte{byte(index) + 1}, 12)
		stream = gcm.Seal(append(stream, nonce...), nonce, plaintext[:n], blockAAD(aadPrefix, index))
		plaintext = plaintext[n:]
	}

	return stream
}

// ---------------------------------------------------------------------------
// Round trips and layout
// ---------------------------------------------------------------------------

func TestStandardEncryptionManager_RoundTrip(t *testing.T) {
	sizes := []int{
		0,
		1,
		13,
		plainBlockSize - 1,
		plainBlockSize,
		plainBlockSize + 1,
		2 * plainBlockSize,
		2*plainBlockSize + 12345,
	}
	for _, size := range sizes {
		t.Run(fmt.Sprintf("%d bytes", size), func(t *testing.T) {
			mgr := newTestManager()
			plaintext := testData(size)

			ciphertext, keyMetadata := encryptAll(t, mgr, plaintext)
			assert.Len(t, ciphertext, expectedStreamLength(size), "no empty trailing block, and block 0 even when empty")
			assert.EqualValues(t, len(ciphertext), parseKeyMetadata(t, keyMetadata).fileLength, "file_length is the encrypted length")

			got := decryptAll(t, mgr, ciphertext, keyMetadata)
			assert.True(t, bytes.Equal(plaintext, got), "decrypted bytes differ from the plaintext")
		})
	}
}

// Java writes block 0 even for an empty file, and iceberg-rust rejects any
// stream shorter than the resulting 8 + 28 = 36 bytes.
func TestStandardEncryptionManager_EmptyFileUsesJavaLayout(t *testing.T) {
	mgr := newTestManager()
	ciphertext, keyMetadata := encryptAll(t, mgr, nil)

	assert.Len(t, ciphertext, 36)
	assert.EqualValues(t, 36, parseKeyMetadata(t, keyMetadata).fileLength)

	in, err := mgr.NewDecryptedInputFile(t.Context(), newMemFile(ciphertext), keyMetadata)
	require.NoError(t, err)
	info, err := in.Stat()
	require.NoError(t, err)
	assert.EqualValues(t, 0, info.Size())
	got, err := io.ReadAll(in)
	require.NoError(t, err)
	assert.Empty(t, got)
}

func TestStandardEncryptionManager_RandomAccess(t *testing.T) {
	mgr := newTestManager()
	plaintext := testData(2*plainBlockSize + 100)

	ciphertext, keyMetadata := encryptAll(t, mgr, plaintext)
	in, err := mgr.NewDecryptedInputFile(t.Context(), newMemFile(ciphertext), keyMetadata)
	require.NoError(t, err)

	// Read a slice spanning a block boundary.
	buf := make([]byte, 20)
	n, err := in.ReadAt(buf, plainBlockSize-10)
	require.NoError(t, err)
	assert.Equal(t, plaintext[plainBlockSize-10:plainBlockSize+10], buf[:n])

	// A read running off the end returns what is left, with io.EOF.
	tail := make([]byte, 200)
	n, err = in.ReadAt(tail, 2*plainBlockSize)
	assert.Equal(t, io.EOF, err)
	assert.Equal(t, plaintext[2*plainBlockSize:], tail[:n])

	// Seek + Read.
	pos, err := in.Seek(5, io.SeekStart)
	require.NoError(t, err)
	assert.Equal(t, int64(5), pos)

	rest, err := io.ReadAll(in)
	require.NoError(t, err)
	assert.True(t, bytes.Equal(plaintext[5:], rest))
}

func TestStandardEncryptionManager_EmptyFile_ZeroLengthReadAt(t *testing.T) {
	mgr := newTestManager()
	ciphertext, keyMetadata := encryptAll(t, mgr, nil)

	in, err := mgr.NewDecryptedInputFile(t.Context(), newMemFile(ciphertext), keyMetadata)
	require.NoError(t, err)

	n, err := in.ReadAt(nil, 0)
	assert.NoError(t, err, "a zero-length ReadAt must return (0, nil), like os.File")
	assert.Equal(t, 0, n)
}

func TestStandardEncryptionManager_ReadFrom(t *testing.T) {
	mgr := newTestManager()
	fw := &memFileWriter{}
	out, err := mgr.NewEncryptedOutputFile(t.Context(), fw, testKeyID)
	require.NoError(t, err)

	plaintext := testData(2*plainBlockSize + 7)
	n, err := out.ReadFrom(bytes.NewReader(plaintext))
	require.NoError(t, err)
	assert.EqualValues(t, len(plaintext), n)
	require.NoError(t, out.Close())

	got := decryptAll(t, mgr, fw.Bytes(), out.KeyMetadata())
	assert.True(t, bytes.Equal(plaintext, got))
}

func TestStandardEncryptionManager_Stat(t *testing.T) {
	mgr := newTestManager()
	plaintext := []byte("hello iceberg world, this is a stat test")
	ciphertext, keyMetadata := encryptAll(t, mgr, plaintext)

	in, err := mgr.NewDecryptedInputFile(t.Context(), newMemFile(ciphertext), keyMetadata)
	require.NoError(t, err)

	info, err := in.Stat()
	require.NoError(t, err)
	assert.EqualValues(t, len(plaintext), info.Size(), "Stat must report the plaintext length, not the on-disk ciphertext length")
	assert.Less(t, info.Size(), int64(len(ciphertext)), "ciphertext must be larger than plaintext due to the header and per-block nonce/tag overhead")
}

// ---------------------------------------------------------------------------
// Conformance with the AGS1 spec and StandardKeyMetadata
// ---------------------------------------------------------------------------

// Round trips through the package can't catch a wrong AAD encoding (say, a
// big-endian block index), because writer and reader share gcmStreamBlockAAD.
// This decodes with crypto/aes and crypto/cipher alone, so such an encoding
// fails at block 1.
func TestStandardEncryptionManager_OutputDecodesWithSpecDecoder(t *testing.T) {
	for _, size := range []int{0, 1, plainBlockSize, 2*plainBlockSize + 17} {
		t.Run(fmt.Sprintf("%d bytes", size), func(t *testing.T) {
			plaintext := testData(size)
			ciphertext, keyMetadata := encryptAll(t, newTestManager(), plaintext)

			meta := parseKeyMetadata(t, keyMetadata)
			assert.Len(t, meta.dek, 16, "the default data key length is 128-bit")
			assert.Len(t, meta.aadPrefix, 16)
			assert.EqualValues(t, len(ciphertext), meta.fileLength)

			got := decodeAGS1(t, ciphertext, meta.dek, meta.aadPrefix)
			assert.True(t, bytes.Equal(plaintext, got))
		})
	}
}

// The reverse direction: a stream and key metadata produced by the
// independent codecs above must be readable by the manager.
func TestStandardEncryptionManager_ReadsSpecBuiltStream(t *testing.T) {
	dek, err := hex.DecodeString("000102030405060708090a0b0c0d0e0f")
	require.NoError(t, err)
	aadPrefix, err := hex.DecodeString("a0a1a2a3a4a5a6a7a8a9aaabacadaeaf")
	require.NoError(t, err)

	// Known-answer vector: StandardKeyMetadata for an empty file, written out
	// by hand from the Avro binary encoding. 0x01 is the version, 0x20 the
	// zig-zag length 16, each 0x02 selects the non-null union branch, and 0x48
	// is the zig-zag encoding of the 36-byte file length.
	const wantEmpty = "01" + "20" + "000102030405060708090a0b0c0d0e0f" + "02" + "20" + "a0a1a2a3a4a5a6a7a8a9aaabacadaeaf" + "02" + "48"
	assert.Equal(t, wantEmpty, hex.EncodeToString(avroKeyMetadata(dek, aadPrefix, 36)), "the test encoder must match the hand-written vector")

	for _, size := range []int{0, 1, plainBlockSize, 2*plainBlockSize + 17} {
		t.Run(fmt.Sprintf("%d bytes", size), func(t *testing.T) {
			plaintext := testData(size)
			stream := encodeAGS1(t, plaintext, dek, aadPrefix)
			require.Len(t, stream, expectedStreamLength(size))

			got := decryptAll(t, newTestManager(), stream, avroKeyMetadata(dek, aadPrefix, int64(len(stream))))
			assert.True(t, bytes.Equal(plaintext, got))
		})
	}
}

// TestStandardEncryptionManager_BlockNoncesAreUnique pins the one GCM
// property whose loss is catastrophic and silent: reusing a nonce under a
// single key leaks the plaintext XOR of the two blocks and the GHASH
// authentication key, yet a file sealed with a reused (or constant) nonce
// still round-trips and decrypts correctly, so no round-trip, tamper, or
// reorder test would ever catch a regression here. Sealing identical
// plaintext in every block makes any nonce collision directly visible as
// matching ciphertext bytes.
func TestStandardEncryptionManager_BlockNoncesAreUnique(t *testing.T) {
	const numBlocks = 4
	// Every block carries byte-for-byte identical plaintext.
	plaintext := bytes.Repeat([]byte("0123456789abcdef"), numBlocks*plainBlockSize/16)

	ciphertext, _ := encryptAll(t, newTestManager(), plaintext)
	require.Len(t, ciphertext, headerLength+numBlocks*(plainBlockSize+blockOverhead))

	seenNonceAt := make(map[string]int, numBlocks)
	cipherBlocks := make([][]byte, numBlocks)
	for i := range numBlocks {
		offset := headerLength + i*(plainBlockSize+blockOverhead)
		block := ciphertext[offset : offset+plainBlockSize+blockOverhead]
		nonce := string(block[:12])

		if prev, ok := seenNonceAt[nonce]; ok {
			t.Fatalf("block %d reused the nonce from block %d: GCM nonce reuse under a single key leaks the plaintext XOR and the GHASH authentication key", i, prev)
		}
		seenNonceAt[nonce] = i
		cipherBlocks[i] = block
	}

	for i := 1; i < numBlocks; i++ {
		assert.False(t, bytes.Equal(cipherBlocks[0], cipherBlocks[i]), "identical plaintext in blocks 0 and %d must not produce identical ciphertext", i)
	}
}

func TestStandardEncryptionManager_DataKeyLength(t *testing.T) {
	t.Run("valid lengths", func(t *testing.T) {
		for _, length := range []int{16, 24, 32} {
			mgr := newTestManager(encryption.WithStandardDataKeyLength(length))
			plaintext := testData(1000)

			ciphertext, keyMetadata := encryptAll(t, mgr, plaintext)
			assert.Len(t, parseKeyMetadata(t, keyMetadata).dek, length)
			assert.Equal(t, plaintext, decryptAll(t, mgr, ciphertext, keyMetadata))
		}
	})

	t.Run("invalid lengths", func(t *testing.T) {
		for _, length := range []int{-1, 0, 15, 17, 33} {
			mgr := newTestManager(encryption.WithStandardDataKeyLength(length))
			_, err := mgr.NewEncryptedOutputFile(t.Context(), &memFileWriter{}, testKeyID)
			require.Error(t, err, "length %d", length)
			assert.ErrorIs(t, err, encryption.ErrInvalidKeyLength)
		}
	})
}

// ---------------------------------------------------------------------------
// Fail-closed behaviour and untrusted input
// ---------------------------------------------------------------------------

func TestStandardEncryptionManager_OutputFile_EmptyKeyIDRejected(t *testing.T) {
	_, err := newTestManager().NewEncryptedOutputFile(t.Context(), &memFileWriter{}, "")
	require.Error(t, err)
	assert.ErrorIs(t, err, encryption.ErrKeyIDRequired)
}

func TestStandardEncryptionManager_InputFile_EmptyKeyMetadataRejected(t *testing.T) {
	_, err := newTestManager().NewDecryptedInputFile(t.Context(), newMemFile(nil), nil)
	require.Error(t, err)
	assert.ErrorIs(t, err, encryption.ErrKeyMetadataRequired)
}

func TestStandardEncryptionManager_TamperedBlockFailsAuthentication(t *testing.T) {
	mgr := newTestManager()
	ciphertext, keyMetadata := encryptAll(t, mgr, testData(2*plainBlockSize))
	// Flip a bit inside the first block's ciphertext, past the 8-byte AES
	// GCM Stream header and 12-byte nonce; flipping the header instead
	// would fail earlier, at stream-header validation.
	ciphertext[headerLength+12] ^= 0xFF

	in, err := mgr.NewDecryptedInputFile(t.Context(), newMemFile(ciphertext), keyMetadata)
	require.NoError(t, err)

	_, err = io.ReadAll(in)
	require.Error(t, err)
	assert.ErrorIs(t, err, encryption.ErrAuthenticationFailed)
}

func TestStandardEncryptionManager_ReorderedBlocksFailAuthentication(t *testing.T) {
	mgr := newTestManager()
	ciphertext, keyMetadata := encryptAll(t, mgr, testData(2*plainBlockSize)) // exactly two full blocks

	// Swap the two blocks: with a random nonce per block, only the AAD (which
	// binds a block to its position) can catch this.
	const physical = plainBlockSize + blockOverhead
	block0 := slices.Clone(ciphertext[headerLength : headerLength+physical])
	block1 := slices.Clone(ciphertext[headerLength+physical : headerLength+2*physical])
	copy(ciphertext[headerLength:], block1)
	copy(ciphertext[headerLength+physical:], block0)

	in, err := mgr.NewDecryptedInputFile(t.Context(), newMemFile(ciphertext), keyMetadata)
	require.NoError(t, err)

	_, err = io.ReadAll(in)
	require.Error(t, err)
	assert.ErrorIs(t, err, encryption.ErrAuthenticationFailed, "swapping blocks must be detected: the AAD binds each block to its position")
}

func TestStandardEncryptionManager_WrongKeyMaterialFailsAuthentication(t *testing.T) {
	mgr := newTestManager()
	ciphertext, keyMetadata := encryptAll(t, mgr, []byte("hello iceberg"))
	good := parseKeyMetadata(t, keyMetadata)

	tests := map[string][]byte{
		"wrong DEK":        avroKeyMetadata(bytes.Repeat([]byte{0x55}, len(good.dek)), good.aadPrefix, good.fileLength),
		"wrong AAD prefix": avroKeyMetadata(good.dek, bytes.Repeat([]byte{0x55}, len(good.aadPrefix)), good.fileLength),
	}
	for name, meta := range tests {
		t.Run(name, func(t *testing.T) {
			in, err := mgr.NewDecryptedInputFile(t.Context(), newMemFile(ciphertext), meta)
			require.NoError(t, err)

			_, err = io.ReadAll(in)
			require.Error(t, err)
			assert.ErrorIs(t, err, encryption.ErrAuthenticationFailed)
		})
	}
}

func TestStandardEncryptionManager_InvalidStreamHeaderRejected(t *testing.T) {
	mgr := newTestManager()
	ciphertext, keyMetadata := encryptAll(t, mgr, []byte("hello iceberg"))

	setBlockLength := func(n uint32) func([]byte) {
		return func(b []byte) { binary.LittleEndian.PutUint32(b[4:8], n) }
	}
	tests := map[string]func([]byte){
		"magic mismatch":              func(b []byte) { copy(b[:4], "XXXX") },
		"zero block length":           setBlockLength(0),
		"64 KiB block length":         setBlockLength(64 * 1024),
		"2 MiB block length":          setBlockLength(2 * plainBlockSize),
		"maximum uint32 block length": setBlockLength(math.MaxUint32),
	}
	for name, mutate := range tests {
		t.Run(name, func(t *testing.T) {
			stream := slices.Clone(ciphertext)
			mutate(stream)

			_, err := mgr.NewDecryptedInputFile(t.Context(), newMemFile(stream), keyMetadata)
			require.Error(t, err)
			assert.ErrorIs(t, err, encryption.ErrInvalidStreamHeader)
		})
	}

	t.Run("truncated header", func(t *testing.T) {
		_, err := mgr.NewDecryptedInputFile(t.Context(), newMemFile(ciphertext[:5]), keyMetadata)
		require.Error(t, err)
		assert.ErrorIs(t, err, encryption.ErrInvalidStreamHeader)
	})
}

func TestStandardEncryptionManager_InvalidKeyMetadataRejectedOnRead(t *testing.T) {
	dek := bytes.Repeat([]byte{0x11}, 16)
	aad := bytes.Repeat([]byte{0x22}, 16)
	valid := func(fileLength int64) []byte { return avroKeyMetadata(dek, aad, fileLength) }
	const physical = plainBlockSize + blockOverhead

	tests := []struct {
		name    string
		meta    []byte
		wantErr error
		wantMsg string
	}{
		{"unsupported version", slices.Concat([]byte{2}, valid(36)[1:]), encryption.ErrUnsupportedKeyMetadataVersion, ""},
		{"version byte only", []byte{1}, encryption.ErrInvalidKeyMetadata, "varint"},
		{"truncated mid-record", valid(36)[:20], encryption.ErrInvalidKeyMetadata, "out of range"},
		{"trailing bytes", append(valid(36), 0), encryption.ErrInvalidKeyMetadata, "trailing"},
		{"oversized varint", slices.Concat([]byte{1}, bytes.Repeat([]byte{0xff}, 11)), encryption.ErrInvalidKeyMetadata, "varint"},
		{"negative bytes length", slices.Concat([]byte{1}, avroLong(-1)), encryption.ErrInvalidKeyMetadata, "out of range"},
		{"bytes length beyond the record", slices.Concat([]byte{1}, avroLong(1<<40)), encryption.ErrInvalidKeyMetadata, "out of range"},
		{"unknown union branch", slices.Concat([]byte{1}, avroBytes(dek), avroLong(2)), encryption.ErrInvalidKeyMetadata, "union branch"},
		{"missing aad prefix", slices.Concat([]byte{1}, avroBytes(dek), avroLong(0), avroLong(1), avroLong(36)), encryption.ErrInvalidKeyMetadata, "aad-prefix must be exactly 16 bytes, got 0"},
		{"empty aad prefix", avroKeyMetadata(dek, nil, 36), encryption.ErrInvalidKeyMetadata, "aad-prefix must be exactly 16 bytes, got 0"},
		{"short aad prefix", avroKeyMetadata(dek, aad[:15], 36), encryption.ErrInvalidKeyMetadata, "aad-prefix must be exactly 16 bytes, got 15"},
		{"long aad prefix", avroKeyMetadata(dek, append(slices.Clone(aad), 0), 36), encryption.ErrInvalidKeyMetadata, "aad-prefix must be exactly 16 bytes, got 17"},
		{"missing file length", slices.Concat([]byte{1}, avroBytes(dek), avroLong(1), avroBytes(aad), avroLong(0)), encryption.ErrInvalidKeyMetadata, "file-length is required"},
		{"negative file length", valid(-1), encryption.ErrInvalidKeyMetadata, "shorter than"},
		{"file length below the 36-byte minimum", valid(35), encryption.ErrInvalidKeyMetadata, "shorter than"},
		{"file length ends inside a nonce", valid(headerLength + physical + 5), encryption.ErrInvalidKeyMetadata, "nonce and tag"},
		{"invalid DEK length", avroKeyMetadata(dek[:15], aad, 36), encryption.ErrInvalidKeyLength, ""},
		{"empty DEK", avroKeyMetadata(nil, aad, 36), encryption.ErrInvalidKeyLength, ""},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := newTestManager().NewDecryptedInputFile(t.Context(), newMemFile(nil), tt.meta)
			require.Error(t, err)
			assert.ErrorIs(t, err, tt.wantErr)
			if tt.wantMsg != "" {
				assert.ErrorContains(t, err, tt.wantMsg)
			}
		})
	}
}

// A file length near math.MaxInt64 is well-formed, so construction succeeds,
// but reading must fail closed without overflowing or allocating more than
// one block.
func TestStandardEncryptionManager_HugeFileLengthFailsClosedOnRead(t *testing.T) {
	mgr := newTestManager()
	dek := bytes.Repeat([]byte{0x11}, 16)
	aad := bytes.Repeat([]byte{0x22}, 16)
	header := encodeAGS1(t, nil, dek, aad)[:headerLength]

	in, err := mgr.NewDecryptedInputFile(t.Context(), newMemFile(header), avroKeyMetadata(dek, aad, math.MaxInt64))
	require.NoError(t, err)

	buf := make([]byte, 16)
	_, err = in.ReadAt(buf, 0)
	assert.ErrorIs(t, err, encryption.ErrBlockTruncated, "block 0 lies beyond the real file")

	_, err = in.ReadAt(buf, (math.MaxUint32+1)*plainBlockSize)
	assert.ErrorIs(t, err, encryption.ErrInvalidKeyMetadata, "the AAD only has 32 bits for the block index")
}

// shortReadFile is an icebergio.File stub whose ReadAt returns fewer bytes
// than requested along with io.EOF once truncateAt is reached, mimicking a
// real backend (S3, local fs) reading a genuinely truncated file. A plain
// bytes.Reader always fills the requested slice, which is why the round-trip
// tests never exercise this path.
type shortReadFile struct {
	*memFile
	truncateAt int64
}

func (f *shortReadFile) ReadAt(p []byte, off int64) (int, error) {
	if off >= f.truncateAt {
		return 0, io.EOF
	}
	if off+int64(len(p)) > f.truncateAt {
		p = p[:f.truncateAt-off]
	}
	n, err := f.memFile.ReadAt(p, off)
	if err == nil && int64(n) < int64(len(p)) {
		err = io.EOF
	}

	return n, err
}

func TestStandardEncryptionManager_TruncatedBackendReadReportsBlockTruncated(t *testing.T) {
	mgr := newTestManager()
	ciphertext, keyMetadata := encryptAll(t, mgr, testData(100))
	truncated := &shortReadFile{memFile: newMemFile(ciphertext), truncateAt: int64(len(ciphertext) - 5)}

	in, err := mgr.NewDecryptedInputFile(t.Context(), truncated, keyMetadata)
	require.NoError(t, err)

	_, err = io.ReadAll(in)
	require.Error(t, err)
	assert.ErrorIs(t, err, encryption.ErrBlockTruncated)
	assert.NotErrorIs(t, err, encryption.ErrCiphertextTooShort, "a truncated block read is distinct from a too-short KMS-wrapped key/payload")
	assert.NotErrorIs(t, err, encryption.ErrAuthenticationFailed, "a truncated read must not be misreported as tampering")
}

// The plaintext length comes from the file length in key metadata, not from
// the storage size, so bytes appended after the last block are ignored, as
// the AES GCM Stream spec's "File length" rule implies.
func TestStandardEncryptionManager_TrailingBytesAfterLastBlockIgnored(t *testing.T) {
	mgr := newTestManager()
	plaintext := testData(plainBlockSize + 10)
	ciphertext, keyMetadata := encryptAll(t, mgr, plaintext)

	got := decryptAll(t, mgr, append(ciphertext, []byte("trailing garbage")...), keyMetadata)
	assert.True(t, bytes.Equal(plaintext, got))
}

// ---------------------------------------------------------------------------
// Write and close failures
// ---------------------------------------------------------------------------

// failAfterNWriter is an icebergio.FileWriter stub that fails the Nth call
// to Write, simulating a mid-stream flush failure on the underlying storage.
// It also counts Close calls, since memFileWriter.Close is a no-op and would
// otherwise mask a leaked/never-closed underlying writer.
type failAfterNWriter struct {
	memFileWriter
	failAt int
	writes int
	closes int
}

func (w *failAfterNWriter) Write(p []byte) (int, error) {
	w.writes++
	if w.writes == w.failAt {
		return 0, errors.New("simulated write failure")
	}

	return w.memFileWriter.Write(p)
}

func (w *failAfterNWriter) Close() error {
	w.closes++

	return w.memFileWriter.Close()
}

func TestStandardEncryptionManager_FlushFailurePoisonsWriterAndClose(t *testing.T) {
	mgr := newTestManager()
	// Write call 1 is the AES GCM Stream header written by
	// NewEncryptedOutputFile; call 2 flushes the first block; call 3 flushes
	// the second block and fails.
	fw := &failAfterNWriter{failAt: 3}
	out, err := mgr.NewEncryptedOutputFile(t.Context(), fw, testKeyID)
	require.NoError(t, err)

	// The first block flushes fine; the second block's flush fails.
	n, err := out.Write(make([]byte, 2*plainBlockSize))
	require.Error(t, err)
	assert.Equal(t, plainBlockSize, n, "only the first block reached storage")
	assert.Equal(t, 1, fw.closes, "a poisoned flush must close the underlying writer, not leak it")

	// A subsequent Write must return the same sticky error, not attempt more
	// I/O, and not the post-Close error: the writer was poisoned, not closed.
	_, err2 := out.Write([]byte("c"))
	require.Error(t, err2)
	assert.ErrorIs(t, err2, err)
	assert.NotErrorIs(t, err2, encryption.ErrOutputFileClosed)
	assert.NotErrorIs(t, err2, fs.ErrClosed)

	// Close must report the failure rather than silently succeeding.
	closeErr := out.Close()
	require.Error(t, closeErr)
	assert.Nil(t, out.KeyMetadata(), "key metadata must not be finalized when the file failed to write")

	// A retried Close must keep reporting the same error, not nil.
	closeErr2 := out.Close()
	require.Error(t, closeErr2)
	assert.Equal(t, 1, fw.closes, "a retried Close must not close the underlying writer again")
}

func TestStandardEncryptionManager_WriteAfterCloseRejected(t *testing.T) {
	out, err := newTestManager().NewEncryptedOutputFile(t.Context(), &memFileWriter{}, testKeyID)
	require.NoError(t, err)
	require.NoError(t, out.Close())

	_, err = out.Write([]byte("late"))
	require.Error(t, err)
	assert.ErrorIs(t, err, encryption.ErrOutputFileClosed)
	assert.ErrorIs(t, err, fs.ErrClosed)
}

// closeFailWriter is an icebergio.FileWriter stub whose Close always fails
// even though every Write succeeds, simulating a backend where the final
// flush/commit (e.g. an S3 multipart completion) fails independently of the
// preceding writes.
type closeFailWriter struct {
	memFileWriter
	closes int
}

func (w *closeFailWriter) Close() error {
	w.closes++

	return errors.New("simulated close failure")
}

func TestStandardEncryptionManager_UnderlyingCloseFailurePoisonsWriter(t *testing.T) {
	fw := &closeFailWriter{}
	out, err := newTestManager().NewEncryptedOutputFile(t.Context(), fw, testKeyID)
	require.NoError(t, err)

	_, err = out.Write([]byte("hello"))
	require.NoError(t, err)

	closeErr := out.Close()
	require.Error(t, closeErr)
	assert.Nil(t, out.KeyMetadata(), "key metadata must not be finalized when the underlying Close fails")

	// A retried Close must keep reporting the same error, not nil, and must
	// not attempt to close the underlying writer again.
	closeErr2 := out.Close()
	require.Error(t, closeErr2)
	assert.ErrorIs(t, closeErr2, closeErr)
	assert.Equal(t, 1, fw.closes, "a retried Close must not re-close the underlying writer")

	_, err = out.Write([]byte("late"))
	require.Error(t, err)
	assert.ErrorIs(t, err, closeErr, "a subsequent Write must report the same sticky error")
}

// ---------------------------------------------------------------------------
// Block cache and concurrency
// ---------------------------------------------------------------------------

// countingReadAtFile wraps a memFile and counts ReadAt calls, to verify that
// repeated small reads landing in the same block reuse the decrypted-block
// cache instead of re-decrypting on every call.
type countingReadAtFile struct {
	*memFile
	reads int
}

func (f *countingReadAtFile) ReadAt(p []byte, off int64) (int, error) {
	f.reads++

	return f.memFile.ReadAt(p, off)
}

func TestStandardEncryptionManager_RepeatedSmallReadsReuseDecryptedBlockCache(t *testing.T) {
	mgr := newTestManager()
	plaintext := testData(plainBlockSize + 5000) // a full block, then a 5000-byte tail block

	ciphertext, keyMetadata := encryptAll(t, mgr, plaintext)
	counting := &countingReadAtFile{memFile: newMemFile(ciphertext)}

	in, err := mgr.NewDecryptedInputFile(t.Context(), counting, keyMetadata)
	require.NoError(t, err)
	baseline := counting.reads // the stream header was already read once above

	buf := make([]byte, 1)
	readOneByteAt := func(off int64) {
		_, err := in.Seek(off, io.SeekStart)
		require.NoError(t, err)
		n, err := in.Read(buf)
		require.NoError(t, err)
		require.Equal(t, 1, n)
	}
	for off := range int64(5000) {
		readOneByteAt(off) // all in block 0
	}
	for off := range int64(5000) {
		readOneByteAt(plainBlockSize + off) // all in block 1
	}

	assert.LessOrEqual(t, counting.reads-baseline, 2, "10000 one-byte reads across two blocks must decrypt each block at most once")
}

// slowReadFile wraps a memFile and sleeps before every ReadAt, simulating a
// remote backend (e.g. S3) with real network latency. It is used to check
// that concurrent ReadAt calls for distinct blocks overlap instead of queuing
// behind readBlock's cache lock.
type slowReadFile struct {
	*memFile
	delay time.Duration
}

func (f *slowReadFile) ReadAt(p []byte, off int64) (int, error) {
	time.Sleep(f.delay)

	return f.memFile.ReadAt(p, off)
}

func TestStandardEncryptionManager_ConcurrentReadAtOverlaps(t *testing.T) {
	const (
		numBlocks = 8
		delay     = 20 * time.Millisecond
	)
	mgr := newTestManager()
	ciphertext, keyMetadata := encryptAll(t, mgr, testData(numBlocks*plainBlockSize))

	slow := &slowReadFile{memFile: newMemFile(ciphertext), delay: delay}
	in, err := mgr.NewDecryptedInputFile(t.Context(), slow, keyMetadata) // reads the header; not timed below
	require.NoError(t, err)

	start := time.Now()
	var g errgroup.Group
	for i := range numBlocks {
		g.Go(func() error {
			buf := make([]byte, 16)
			_, err := in.ReadAt(buf, int64(i)*plainBlockSize)

			return err
		})
	}
	require.NoError(t, g.Wait())
	elapsed := time.Since(start)

	// Serial execution (for example, holding cacheMu across ReadAt) would take
	// roughly numBlocks*delay. True concurrency keeps it close to a single
	// delay; the threshold below is generous to stay robust on slow or loaded
	// CI machines while still failing on a serialized implementation.
	assert.Less(t, elapsed, numBlocks/2*delay, "concurrent ReadAt calls for distinct blocks must overlap, not serialize")
}
