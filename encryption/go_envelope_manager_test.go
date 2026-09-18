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
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"math"
	"testing"
	"time"

	"github.com/apache/iceberg-go/encryption"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"golang.org/x/sync/errgroup"
)

func newTestGoEnvelopeManager(t *testing.T, opts ...encryption.GoEnvelopeManagerOption) (*encryption.GoEnvelopeEncryptionManager, *encryption.InMemoryKeyManagementClient) {
	t.Helper()
	kms := encryption.NewInMemoryKeyManagementClient()
	require.NoError(t, kms.AddKey("kek-1", bytes.Repeat([]byte{0x42}, 32)))

	return encryption.NewGoEnvelopeEncryptionManager(kms, opts...), kms
}

func encryptAll(t *testing.T, mgr *encryption.GoEnvelopeEncryptionManager, keyID string, plaintext []byte) (ciphertext []byte, keyMetadata encryption.EncryptionKeyMetadata) {
	t.Helper()
	fw := &memFileWriter{}
	out, err := mgr.NewEncryptedOutputFile(t.Context(), fw, keyID)
	require.NoError(t, err)
	_, err = out.Write(plaintext)
	require.NoError(t, err)
	require.NoError(t, out.Close())

	return fw.Bytes(), out.KeyMetadata()
}

func decryptAll(t *testing.T, mgr *encryption.GoEnvelopeEncryptionManager, ciphertext []byte, keyMetadata encryption.EncryptionKeyMetadata) []byte {
	t.Helper()
	in, err := mgr.NewDecryptedInputFile(t.Context(), newMemFile(ciphertext), keyMetadata)
	require.NoError(t, err)
	data, err := io.ReadAll(in)
	require.NoError(t, err)

	return data
}

func TestGoEnvelopeEncryptionManager_RoundTrip_SmallerThanBlock(t *testing.T) {
	mgr, _ := newTestGoEnvelopeManager(t, encryption.WithBlockSize(16))
	plaintext := []byte("hello iceberg")

	ciphertext, keyMetadata := encryptAll(t, mgr, "kek-1", plaintext)
	assert.NotEmpty(t, keyMetadata)
	assert.NotEqual(t, plaintext, ciphertext, "ciphertext must differ from plaintext")

	got := decryptAll(t, mgr, ciphertext, keyMetadata)
	assert.Equal(t, plaintext, got)
}

func TestGoEnvelopeEncryptionManager_RoundTrip_MultiBlock(t *testing.T) {
	mgr, _ := newTestGoEnvelopeManager(t, encryption.WithBlockSize(16))
	plaintext := bytes.Repeat([]byte("0123456789abcdef"), 10)
	plaintext = append(plaintext, []byte("partial-tail")...)

	ciphertext, keyMetadata := encryptAll(t, mgr, "kek-1", plaintext)
	got := decryptAll(t, mgr, ciphertext, keyMetadata)
	assert.Equal(t, plaintext, got)
}

func TestGoEnvelopeEncryptionManager_RoundTrip_ExactBlockMultiple(t *testing.T) {
	mgr, _ := newTestGoEnvelopeManager(t, encryption.WithBlockSize(16))
	plaintext := bytes.Repeat([]byte("0123456789abcdef"), 4)

	ciphertext, keyMetadata := encryptAll(t, mgr, "kek-1", plaintext)
	got := decryptAll(t, mgr, ciphertext, keyMetadata)
	assert.Equal(t, plaintext, got)
}

func TestGoEnvelopeEncryptionManager_RoundTrip_Empty(t *testing.T) {
	mgr, _ := newTestGoEnvelopeManager(t, encryption.WithBlockSize(16))

	ciphertext, keyMetadata := encryptAll(t, mgr, "kek-1", nil)
	got := decryptAll(t, mgr, ciphertext, keyMetadata)
	assert.Empty(t, got)
}

func TestGoEnvelopeEncryptionManager_RandomAccess(t *testing.T) {
	mgr, _ := newTestGoEnvelopeManager(t, encryption.WithBlockSize(16))
	plaintext := bytes.Repeat([]byte("0123456789abcdef"), 10)
	plaintext = append(plaintext, []byte("tail-bytes")...)

	ciphertext, keyMetadata := encryptAll(t, mgr, "kek-1", plaintext)
	in, err := mgr.NewDecryptedInputFile(t.Context(), newMemFile(ciphertext), keyMetadata)
	require.NoError(t, err)

	// Read a slice spanning a block boundary.
	buf := make([]byte, 20)
	n, err := in.ReadAt(buf, 10)
	require.NoError(t, err)
	assert.Equal(t, plaintext[10:30], buf[:n])

	// Seek + Read.
	pos, err := in.Seek(5, io.SeekStart)
	require.NoError(t, err)
	assert.Equal(t, int64(5), pos)

	rest, err := io.ReadAll(in)
	require.NoError(t, err)
	assert.Equal(t, plaintext[5:], rest)
}

func TestGoEnvelopeEncryptionManager_TamperedBlockFailsAuthentication(t *testing.T) {
	mgr, _ := newTestGoEnvelopeManager(t, encryption.WithBlockSize(16))
	plaintext := bytes.Repeat([]byte("0123456789abcdef"), 4)

	ciphertext, keyMetadata := encryptAll(t, mgr, "kek-1", plaintext)
	// Flip a bit inside the first block's ciphertext, past the 8-byte AES
	// GCM Stream header and 12-byte nonce; flipping the header instead
	// would fail earlier, at stream-header validation.
	ciphertext[8+12] ^= 0xFF

	in, err := mgr.NewDecryptedInputFile(t.Context(), newMemFile(ciphertext), keyMetadata)
	require.NoError(t, err)

	_, err = io.ReadAll(in)
	require.Error(t, err)
	assert.True(t, errors.Is(err, encryption.ErrAuthenticationFailed))
}

func TestGoEnvelopeEncryptionManager_ReorderedBlocksFailAuthentication(t *testing.T) {
	mgr, _ := newTestGoEnvelopeManager(t, encryption.WithBlockSize(16))
	plaintext := bytes.Repeat([]byte("0123456789abcdef"), 2) // exactly two full 16-byte blocks

	ciphertext, keyMetadata := encryptAll(t, mgr, "kek-1", plaintext)

	// The AES GCM Stream header (magic + block length) is 8 bytes; each
	// cipher block here is 16 (block size) + 12 (nonce) + 16 (tag) = 44
	// bytes. Swap the two blocks: with a random nonce per block, only the
	// AAD (which binds a block to its position) can catch this.
	const headerLen, blockPhysicalLen = 8, 44
	block1 := append([]byte(nil), ciphertext[headerLen:headerLen+blockPhysicalLen]...)
	block2 := append([]byte(nil), ciphertext[headerLen+blockPhysicalLen:headerLen+2*blockPhysicalLen]...)
	copy(ciphertext[headerLen:headerLen+blockPhysicalLen], block2)
	copy(ciphertext[headerLen+blockPhysicalLen:headerLen+2*blockPhysicalLen], block1)

	in, err := mgr.NewDecryptedInputFile(t.Context(), newMemFile(ciphertext), keyMetadata)
	require.NoError(t, err)

	_, err = io.ReadAll(in)
	require.Error(t, err)
	assert.True(t, errors.Is(err, encryption.ErrAuthenticationFailed), "swapping blocks must be detected: the AAD binds each block to its position")
}

func TestGoEnvelopeEncryptionManager_StreamHeaderMagicMismatchRejected(t *testing.T) {
	mgr, _ := newTestGoEnvelopeManager(t, encryption.WithBlockSize(16))
	ciphertext, keyMetadata := encryptAll(t, mgr, "kek-1", []byte("hello iceberg"))
	copy(ciphertext[:4], "XXXX")

	_, err := mgr.NewDecryptedInputFile(t.Context(), newMemFile(ciphertext), keyMetadata)
	require.Error(t, err)
	assert.True(t, errors.Is(err, encryption.ErrInvalidStreamHeader))
}

func TestGoEnvelopeEncryptionManager_StreamHeaderZeroBlockLengthRejected(t *testing.T) {
	mgr, _ := newTestGoEnvelopeManager(t, encryption.WithBlockSize(16))
	ciphertext, keyMetadata := encryptAll(t, mgr, "kek-1", []byte("hello iceberg"))
	binary.LittleEndian.PutUint32(ciphertext[4:8], 0)

	_, err := mgr.NewDecryptedInputFile(t.Context(), newMemFile(ciphertext), keyMetadata)
	require.Error(t, err)
	assert.True(t, errors.Is(err, encryption.ErrInvalidStreamHeader))
}

func TestGoEnvelopeEncryptionManager_StreamHeaderBlockLengthExceedsMaxRejected(t *testing.T) {
	mgr, _ := newTestGoEnvelopeManager(t, encryption.WithBlockSize(16))
	ciphertext, keyMetadata := encryptAll(t, mgr, "kek-1", []byte("hello iceberg"))
	binary.LittleEndian.PutUint32(ciphertext[4:8], math.MaxUint32)

	_, err := mgr.NewDecryptedInputFile(t.Context(), newMemFile(ciphertext), keyMetadata)
	require.Error(t, err)
	assert.True(t, errors.Is(err, encryption.ErrInvalidStreamHeader))
}

func TestGoEnvelopeEncryptionManager_OutputFile_EmptyKeyIDRejected(t *testing.T) {
	mgr, _ := newTestGoEnvelopeManager(t)
	_, err := mgr.NewEncryptedOutputFile(t.Context(), &memFileWriter{}, "")
	require.Error(t, err)
	assert.True(t, errors.Is(err, encryption.ErrKeyIDRequired))
}

func TestGoEnvelopeEncryptionManager_InputFile_EmptyKeyMetadataRejected(t *testing.T) {
	mgr, _ := newTestGoEnvelopeManager(t)
	_, err := mgr.NewDecryptedInputFile(t.Context(), newMemFile(nil), nil)
	require.Error(t, err)
	assert.True(t, errors.Is(err, encryption.ErrKeyMetadataRequired))
}

func TestGoEnvelopeEncryptionManager_UnknownKeyIDPropagatesFromKMS(t *testing.T) {
	mgr, _ := newTestGoEnvelopeManager(t)
	_, err := mgr.NewEncryptedOutputFile(t.Context(), &memFileWriter{}, "does-not-exist")
	require.Error(t, err)
	assert.True(t, errors.Is(err, encryption.ErrUnknownKeyID))
}

func TestGoEnvelopeEncryptionManager_MalformedKeyMetadataRejected(t *testing.T) {
	mgr, _ := newTestGoEnvelopeManager(t)
	_, err := mgr.NewDecryptedInputFile(t.Context(), newMemFile(nil), encryption.EncryptionKeyMetadata("not json"))
	require.Error(t, err)
}

func TestGoEnvelopeEncryptionManager_ZeroBlockSizeRejectedOnWrite(t *testing.T) {
	mgr, _ := newTestGoEnvelopeManager(t, encryption.WithBlockSize(0))
	_, err := mgr.NewEncryptedOutputFile(t.Context(), &memFileWriter{}, "kek-1")
	require.Error(t, err)
	assert.True(t, errors.Is(err, encryption.ErrInvalidBlockSize))
}

func TestGoEnvelopeEncryptionManager_NegativeBlockSizeRejectedOnWrite(t *testing.T) {
	mgr, _ := newTestGoEnvelopeManager(t, encryption.WithBlockSize(-1))
	_, err := mgr.NewEncryptedOutputFile(t.Context(), &memFileWriter{}, "kek-1")
	require.Error(t, err)
	assert.True(t, errors.Is(err, encryption.ErrInvalidBlockSize))
}

func TestGoEnvelopeEncryptionManager_BlockSizeExceedingMaxRejectedOnWrite(t *testing.T) {
	mgr, _ := newTestGoEnvelopeManager(t, encryption.WithBlockSize(encryption.GoEnvelopeMaxBlockSize+1))
	_, err := mgr.NewEncryptedOutputFile(t.Context(), &memFileWriter{}, "kek-1")
	require.Error(t, err)
	assert.True(t, errors.Is(err, encryption.ErrInvalidBlockSize))
}

func TestGoEnvelopeEncryptionManager_NegativePlaintextLengthMetadataRejectedOnRead(t *testing.T) {
	mgr, _ := newTestGoEnvelopeManager(t)
	meta := []byte(`{"v":1,"key-id":"kek-1","wrapped-key":"AA==","block-size":16,"aad-prefix":"MTIzNDU2Nzg5MDEyMzQ1Ng==","plaintext-length":-1}`)
	_, err := mgr.NewDecryptedInputFile(t.Context(), newMemFile(nil), meta)
	require.Error(t, err)
	assert.True(t, errors.Is(err, encryption.ErrInvalidKeyMetadata))
	assert.ErrorContains(t, err, "plaintext-length must be non-negative")
}

func TestGoEnvelopeEncryptionManager_EmptyAADPrefixMetadataRejectedOnRead(t *testing.T) {
	mgr, _ := newTestGoEnvelopeManager(t)
	meta := []byte(`{"v":1,"key-id":"kek-1","wrapped-key":"AA==","block-size":16,"aad-prefix":"","plaintext-length":10}`)
	_, err := mgr.NewDecryptedInputFile(t.Context(), newMemFile(nil), meta)
	require.Error(t, err)
	assert.True(t, errors.Is(err, encryption.ErrInvalidKeyMetadata))
	assert.ErrorContains(t, err, "aad-prefix must not be empty")
}

func TestGoEnvelopeEncryptionManager_ZeroBlockSizeMetadataRejectedOnRead(t *testing.T) {
	mgr, _ := newTestGoEnvelopeManager(t)
	meta := []byte(`{"v":1,"key-id":"kek-1","wrapped-key":"AA==","block-size":0,"aad-prefix":"MTIzNDU2Nzg5MDEyMzQ1Ng==","plaintext-length":10}`)
	_, err := mgr.NewDecryptedInputFile(t.Context(), newMemFile(nil), meta)
	require.Error(t, err)
	assert.True(t, errors.Is(err, encryption.ErrInvalidKeyMetadata))
	assert.ErrorContains(t, err, "block-size must be positive")
}

func TestGoEnvelopeEncryptionManager_BlockSizeExceedingMaxMetadataRejectedOnRead(t *testing.T) {
	mgr, _ := newTestGoEnvelopeManager(t)
	meta := []byte(fmt.Sprintf(`{"v":1,"key-id":"kek-1","wrapped-key":"AA==","block-size":%d,"aad-prefix":"MTIzNDU2Nzg5MDEyMzQ1Ng==","plaintext-length":10}`, encryption.GoEnvelopeMaxBlockSize+1))
	_, err := mgr.NewDecryptedInputFile(t.Context(), newMemFile(nil), meta)
	require.Error(t, err)
	assert.True(t, errors.Is(err, encryption.ErrInvalidKeyMetadata))
	assert.ErrorContains(t, err, "block-size must be positive")
}

func TestGoEnvelopeEncryptionManager_InvalidDEKLengthRejected(t *testing.T) {
	mgr, _ := newTestGoEnvelopeManager(t, encryption.WithDEKLength(15))
	_, err := mgr.NewEncryptedOutputFile(t.Context(), &memFileWriter{}, "kek-1")
	require.Error(t, err)
	assert.True(t, errors.Is(err, encryption.ErrInvalidKeyLength))
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

func TestGoEnvelopeEncryptionManager_TruncatedBackendReadReportsBlockTruncated(t *testing.T) {
	mgr, _ := newTestGoEnvelopeManager(t, encryption.WithBlockSize(16))
	plaintext := bytes.Repeat([]byte("0123456789abcdef"), 4)

	ciphertext, keyMetadata := encryptAll(t, mgr, "kek-1", plaintext)
	truncated := &shortReadFile{memFile: newMemFile(ciphertext), truncateAt: int64(len(ciphertext) - 5)}

	in, err := mgr.NewDecryptedInputFile(t.Context(), truncated, keyMetadata)
	require.NoError(t, err)

	_, err = io.ReadAll(in)
	require.Error(t, err)
	assert.True(t, errors.Is(err, encryption.ErrBlockTruncated))
	assert.False(t, errors.Is(err, encryption.ErrCiphertextTooShort), "a truncated block read is distinct from a too-short KMS-wrapped key/payload")
	assert.False(t, errors.Is(err, encryption.ErrAuthenticationFailed), "a truncated read must not be misreported as tampering")
}

func TestGoEnvelopeEncryptionManager_EmptyFile_ZeroLengthReadAt(t *testing.T) {
	mgr, _ := newTestGoEnvelopeManager(t, encryption.WithBlockSize(16))
	ciphertext, keyMetadata := encryptAll(t, mgr, "kek-1", nil)

	in, err := mgr.NewDecryptedInputFile(t.Context(), newMemFile(ciphertext), keyMetadata)
	require.NoError(t, err)

	n, err := in.ReadAt(nil, 0)
	assert.NoError(t, err, "a zero-length ReadAt on an empty file should behave like bytes.Reader, not report io.EOF")
	assert.Equal(t, 0, n)
}

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

func TestGoEnvelopeEncryptionManager_FlushFailurePoisonsWriterAndClose(t *testing.T) {
	mgr, _ := newTestGoEnvelopeManager(t, encryption.WithBlockSize(4))
	// Write call 1 is the AES GCM Stream header written by
	// NewEncryptedOutputFile; call 2 flushes the first (4-byte) block;
	// call 3 flushes the second block and fails.
	fw := &failAfterNWriter{failAt: 3}
	out, err := mgr.NewEncryptedOutputFile(t.Context(), fw, "kek-1")
	require.NoError(t, err)

	// First block (4 bytes) flushes fine; the second block's flush fails.
	n, err := out.Write([]byte("aaaabbbb"))
	require.Error(t, err)
	assert.Equal(t, 4, n, "only the first block reached storage")
	assert.Equal(t, 1, fw.closes, "a poisoned flush must close the underlying writer, not leak it")

	// A subsequent Write must return the same sticky error, not attempt more I/O.
	_, err2 := out.Write([]byte("c"))
	require.Error(t, err2)
	assert.ErrorIs(t, err2, err)

	// Close must report the failure rather than silently succeeding.
	closeErr := out.Close()
	require.Error(t, closeErr)
	assert.Nil(t, out.KeyMetadata(), "key metadata must not be finalized when the file failed to write")

	// A retried Close must keep reporting the same error, not nil.
	closeErr2 := out.Close()
	require.Error(t, closeErr2)
	assert.Equal(t, 1, fw.closes, "a retried Close must not close the underlying writer again")
}

func TestGoEnvelopeEncryptionManager_WriteAfterCloseRejected(t *testing.T) {
	mgr, _ := newTestGoEnvelopeManager(t)
	out, err := mgr.NewEncryptedOutputFile(t.Context(), &memFileWriter{}, "kek-1")
	require.NoError(t, err)
	require.NoError(t, out.Close())

	_, err = out.Write([]byte("late"))
	require.Error(t, err)
	assert.True(t, errors.Is(err, encryption.ErrOutputFileClosed))
	assert.True(t, errors.Is(err, fs.ErrClosed))
}

func TestNewGoEnvelopeEncryptionManager_NilKMSPanics(t *testing.T) {
	assert.PanicsWithValue(t, "encryption: NewGoEnvelopeEncryptionManager: kms must not be nil", func() {
		encryption.NewGoEnvelopeEncryptionManager(nil)
	})
}

func TestGoEnvelopeEncryptionManager_StreamHeaderBlockLengthMismatchRejected(t *testing.T) {
	mgr, _ := newTestGoEnvelopeManager(t, encryption.WithBlockSize(16))
	ciphertext, keyMetadata := encryptAll(t, mgr, "kek-1", []byte("hello iceberg"))
	// A header block length that is itself in-range, but does not match the
	// (trusted) block-size recorded in key metadata.
	binary.LittleEndian.PutUint32(ciphertext[4:8], 32)

	_, err := mgr.NewDecryptedInputFile(t.Context(), newMemFile(ciphertext), keyMetadata)
	require.Error(t, err)
	assert.True(t, errors.Is(err, encryption.ErrInvalidStreamHeader))
}

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

func TestGoEnvelopeEncryptionManager_RepeatedSmallReadsReuseDecryptedBlockCache(t *testing.T) {
	mgr, _ := newTestGoEnvelopeManager(t, encryption.WithBlockSize(4096))
	plaintext := bytes.Repeat([]byte("x"), 5000) // spans exactly two blocks (4096 + 904)

	ciphertext, keyMetadata := encryptAll(t, mgr, "kek-1", plaintext)
	counting := &countingReadAtFile{memFile: newMemFile(ciphertext)}

	in, err := mgr.NewDecryptedInputFile(t.Context(), counting, keyMetadata)
	require.NoError(t, err)
	baseline := counting.reads // the stream header was already read once above

	buf := make([]byte, 1)
	for range plaintext {
		n, err := in.Read(buf)
		require.NoError(t, err)
		require.Equal(t, 1, n)
	}

	assert.LessOrEqual(t, counting.reads-baseline, 2, "5000 one-byte reads across two blocks must decrypt each block at most once")
}

func TestGoEnvelopeEncryptionManager_RoundTrip_DefaultBlockSize(t *testing.T) {
	mgr, _ := newTestGoEnvelopeManager(t) // exercises GoEnvelopeDefaultBlockSize (64 KiB), not an explicit small size
	plaintext := bytes.Repeat([]byte("iceberg-default-block-size-test-"), 4000)
	require.Greater(t, len(plaintext), 64*1024, "plaintext must span multiple default-size blocks")

	ciphertext, keyMetadata := encryptAll(t, mgr, "kek-1", plaintext)
	got := decryptAll(t, mgr, ciphertext, keyMetadata)
	assert.Equal(t, plaintext, got)
}

func TestGoEnvelopeEncryptionManager_ReadFrom(t *testing.T) {
	mgr, _ := newTestGoEnvelopeManager(t, encryption.WithBlockSize(4))
	fw := &memFileWriter{}
	out, err := mgr.NewEncryptedOutputFile(t.Context(), fw, "kek-1")
	require.NoError(t, err)

	plaintext := []byte("hello iceberg world")
	n, err := out.ReadFrom(bytes.NewReader(plaintext))
	require.NoError(t, err)
	assert.EqualValues(t, len(plaintext), n)
	require.NoError(t, out.Close())

	got := decryptAll(t, mgr, fw.Bytes(), out.KeyMetadata())
	assert.Equal(t, plaintext, got)
}

func TestGoEnvelopeEncryptionManager_Stat(t *testing.T) {
	mgr, _ := newTestGoEnvelopeManager(t, encryption.WithBlockSize(16))
	plaintext := []byte("hello iceberg world, this is a stat test")
	ciphertext, keyMetadata := encryptAll(t, mgr, "kek-1", plaintext)

	in, err := mgr.NewDecryptedInputFile(t.Context(), newMemFile(ciphertext), keyMetadata)
	require.NoError(t, err)

	info, err := in.Stat()
	require.NoError(t, err)
	assert.EqualValues(t, len(plaintext), info.Size(), "Stat must report the plaintext length, not the on-disk ciphertext length")
	assert.Less(t, info.Size(), int64(len(ciphertext)), "ciphertext must be larger than plaintext due to per-block nonce/tag overhead")
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

func TestGoEnvelopeEncryptionManager_UnderlyingCloseFailurePoisonsWriter(t *testing.T) {
	mgr, _ := newTestGoEnvelopeManager(t)
	fw := &closeFailWriter{}
	out, err := mgr.NewEncryptedOutputFile(t.Context(), fw, "kek-1")
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

// slowReadFile wraps a memFile and sleeps before every ReadAt, simulating a
// remote backend (e.g. S3) with real network latency. It is used to prove
// that concurrent ReadAt calls for distinct blocks actually overlap rather
// than being serialized by readBlock's internal cache lock.
type slowReadFile struct {
	*memFile
	delay time.Duration
}

func (f *slowReadFile) ReadAt(p []byte, off int64) (int, error) {
	time.Sleep(f.delay)

	return f.memFile.ReadAt(p, off)
}

func TestGoEnvelopeEncryptionManager_ConcurrentReadAtOverlaps(t *testing.T) {
	const (
		numBlocks = 8
		blockSize = 16
		delay     = 20 * time.Millisecond
	)
	mgr, _ := newTestGoEnvelopeManager(t, encryption.WithBlockSize(blockSize))
	plaintext := bytes.Repeat([]byte("0123456789abcdef"), numBlocks)
	ciphertext, keyMetadata := encryptAll(t, mgr, "kek-1", plaintext)

	slow := &slowReadFile{memFile: newMemFile(ciphertext), delay: delay}
	in, err := mgr.NewDecryptedInputFile(t.Context(), slow, keyMetadata) // reads the header; not timed below
	require.NoError(t, err)

	start := time.Now()
	var g errgroup.Group
	for i := range numBlocks {
		g.Go(func() error {
			buf := make([]byte, blockSize)
			_, err := in.ReadAt(buf, int64(i*blockSize))

			return err
		})
	}
	require.NoError(t, g.Wait())
	elapsed := time.Since(start)

	// Serial execution (the pre-fix behavior, holding cacheMu across ReadAt)
	// would take roughly numBlocks*delay. True concurrency keeps it close to
	// a single delay; the threshold below is generous to stay robust on
	// slow or loaded CI machines while still failing on a serialized regression.
	assert.Less(t, elapsed, numBlocks/2*delay, "concurrent ReadAt calls for distinct blocks must overlap, not serialize")
}
