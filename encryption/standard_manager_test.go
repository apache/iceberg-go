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
	"errors"
	"io"
	"testing"

	"github.com/apache/iceberg-go/encryption"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func newTestStandardManager(t *testing.T, opts ...encryption.StandardManagerOption) (*encryption.StandardEncryptionManager, *encryption.InMemoryKeyManagementClient) {
	t.Helper()
	kms := encryption.NewInMemoryKeyManagementClient()
	require.NoError(t, kms.AddKey("kek-1", bytes.Repeat([]byte{0x42}, 32)))

	return encryption.NewStandardEncryptionManager(kms, opts...), kms
}

func encryptAll(t *testing.T, mgr *encryption.StandardEncryptionManager, keyID string, plaintext []byte) (ciphertext []byte, keyMetadata encryption.EncryptionKeyMetadata) {
	t.Helper()
	fw := &memFileWriter{}
	out, err := mgr.NewEncryptedOutputFile(t.Context(), fw, keyID)
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

func TestStandardEncryptionManager_RoundTrip_SmallerThanBlock(t *testing.T) {
	mgr, _ := newTestStandardManager(t, encryption.WithBlockSize(16))
	plaintext := []byte("hello iceberg")

	ciphertext, keyMetadata := encryptAll(t, mgr, "kek-1", plaintext)
	assert.NotEmpty(t, keyMetadata)
	assert.NotEqual(t, plaintext, ciphertext, "ciphertext must differ from plaintext")

	got := decryptAll(t, mgr, ciphertext, keyMetadata)
	assert.Equal(t, plaintext, got)
}

func TestStandardEncryptionManager_RoundTrip_MultiBlock(t *testing.T) {
	mgr, _ := newTestStandardManager(t, encryption.WithBlockSize(16))
	plaintext := bytes.Repeat([]byte("0123456789abcdef"), 10)
	plaintext = append(plaintext, []byte("partial-tail")...)

	ciphertext, keyMetadata := encryptAll(t, mgr, "kek-1", plaintext)
	got := decryptAll(t, mgr, ciphertext, keyMetadata)
	assert.Equal(t, plaintext, got)
}

func TestStandardEncryptionManager_RoundTrip_ExactBlockMultiple(t *testing.T) {
	mgr, _ := newTestStandardManager(t, encryption.WithBlockSize(16))
	plaintext := bytes.Repeat([]byte("0123456789abcdef"), 4)

	ciphertext, keyMetadata := encryptAll(t, mgr, "kek-1", plaintext)
	got := decryptAll(t, mgr, ciphertext, keyMetadata)
	assert.Equal(t, plaintext, got)
}

func TestStandardEncryptionManager_RoundTrip_Empty(t *testing.T) {
	mgr, _ := newTestStandardManager(t, encryption.WithBlockSize(16))

	ciphertext, keyMetadata := encryptAll(t, mgr, "kek-1", nil)
	got := decryptAll(t, mgr, ciphertext, keyMetadata)
	assert.Empty(t, got)
}

func TestStandardEncryptionManager_RandomAccess(t *testing.T) {
	mgr, _ := newTestStandardManager(t, encryption.WithBlockSize(16))
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

func TestStandardEncryptionManager_TamperedBlockFailsAuthentication(t *testing.T) {
	mgr, _ := newTestStandardManager(t, encryption.WithBlockSize(16))
	plaintext := bytes.Repeat([]byte("0123456789abcdef"), 4)

	ciphertext, keyMetadata := encryptAll(t, mgr, "kek-1", plaintext)
	ciphertext[0] ^= 0xFF // flip a bit in the first block's ciphertext

	in, err := mgr.NewDecryptedInputFile(t.Context(), newMemFile(ciphertext), keyMetadata)
	require.NoError(t, err)

	_, err = io.ReadAll(in)
	require.Error(t, err)
	assert.True(t, errors.Is(err, encryption.ErrAuthenticationFailed))
}

func TestStandardEncryptionManager_OutputFile_EmptyKeyIDRejected(t *testing.T) {
	mgr, _ := newTestStandardManager(t)
	_, err := mgr.NewEncryptedOutputFile(t.Context(), &memFileWriter{}, "")
	require.Error(t, err)
	assert.True(t, errors.Is(err, encryption.ErrKeyIDRequired))
}

func TestStandardEncryptionManager_InputFile_EmptyKeyMetadataRejected(t *testing.T) {
	mgr, _ := newTestStandardManager(t)
	_, err := mgr.NewDecryptedInputFile(t.Context(), newMemFile(nil), nil)
	require.Error(t, err)
	assert.True(t, errors.Is(err, encryption.ErrKeyMetadataRequired))
}

func TestStandardEncryptionManager_UnknownKeyIDPropagatesFromKMS(t *testing.T) {
	mgr, _ := newTestStandardManager(t)
	_, err := mgr.NewEncryptedOutputFile(t.Context(), &memFileWriter{}, "does-not-exist")
	require.Error(t, err)
	assert.True(t, errors.Is(err, encryption.ErrUnknownKeyID))
}

func TestStandardEncryptionManager_MalformedKeyMetadataRejected(t *testing.T) {
	mgr, _ := newTestStandardManager(t)
	_, err := mgr.NewDecryptedInputFile(t.Context(), newMemFile(nil), encryption.EncryptionKeyMetadata("not json"))
	require.Error(t, err)
}

func TestStandardEncryptionManager_ZeroBlockSizeRejectedOnWrite(t *testing.T) {
	mgr, _ := newTestStandardManager(t, encryption.WithBlockSize(0))
	_, err := mgr.NewEncryptedOutputFile(t.Context(), &memFileWriter{}, "kek-1")
	require.Error(t, err)
	assert.True(t, errors.Is(err, encryption.ErrInvalidBlockSize))
}

func TestStandardEncryptionManager_NegativeBlockSizeRejectedOnWrite(t *testing.T) {
	mgr, _ := newTestStandardManager(t, encryption.WithBlockSize(-1))
	_, err := mgr.NewEncryptedOutputFile(t.Context(), &memFileWriter{}, "kek-1")
	require.Error(t, err)
	assert.True(t, errors.Is(err, encryption.ErrInvalidBlockSize))
}

func TestStandardEncryptionManager_ZeroBlockSizeMetadataRejectedOnRead(t *testing.T) {
	mgr, _ := newTestStandardManager(t)
	meta := []byte(`{"v":1,"key-id":"kek-1","wrapped-key":"AA==","nonce-prefix":"AAAAAA==","block-size":0,"plaintext-length":10}`)
	_, err := mgr.NewDecryptedInputFile(t.Context(), newMemFile(nil), meta)
	require.Error(t, err)
	assert.True(t, errors.Is(err, encryption.ErrInvalidKeyMetadata))
}

func TestStandardEncryptionManager_NegativePlaintextLengthMetadataRejectedOnRead(t *testing.T) {
	mgr, _ := newTestStandardManager(t)
	meta := []byte(`{"v":1,"key-id":"kek-1","wrapped-key":"AA==","nonce-prefix":"AAAAAA==","block-size":16,"plaintext-length":-1}`)
	_, err := mgr.NewDecryptedInputFile(t.Context(), newMemFile(nil), meta)
	require.Error(t, err)
	assert.True(t, errors.Is(err, encryption.ErrInvalidKeyMetadata))
}

func TestStandardEncryptionManager_BadNoncePrefixLengthMetadataRejectedOnRead(t *testing.T) {
	mgr, _ := newTestStandardManager(t)
	meta := []byte(`{"v":1,"key-id":"kek-1","wrapped-key":"AA==","nonce-prefix":"AA==","block-size":16,"plaintext-length":10}`)
	_, err := mgr.NewDecryptedInputFile(t.Context(), newMemFile(nil), meta)
	require.Error(t, err)
	assert.True(t, errors.Is(err, encryption.ErrInvalidKeyMetadata))
}

func TestStandardEncryptionManager_InvalidDEKLengthRejected(t *testing.T) {
	mgr, _ := newTestStandardManager(t, encryption.WithDEKLength(15))
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

func TestStandardEncryptionManager_TruncatedBackendReadReportsCiphertextTooShort(t *testing.T) {
	mgr, _ := newTestStandardManager(t, encryption.WithBlockSize(16))
	plaintext := bytes.Repeat([]byte("0123456789abcdef"), 4)

	ciphertext, keyMetadata := encryptAll(t, mgr, "kek-1", plaintext)
	truncated := &shortReadFile{memFile: newMemFile(ciphertext), truncateAt: int64(len(ciphertext) - 5)}

	in, err := mgr.NewDecryptedInputFile(t.Context(), truncated, keyMetadata)
	require.NoError(t, err)

	_, err = io.ReadAll(in)
	require.Error(t, err)
	assert.True(t, errors.Is(err, encryption.ErrCiphertextTooShort))
	assert.False(t, errors.Is(err, encryption.ErrAuthenticationFailed), "a truncated read must not be misreported as tampering")
}

func TestStandardEncryptionManager_EmptyFile_ZeroLengthReadAt(t *testing.T) {
	mgr, _ := newTestStandardManager(t, encryption.WithBlockSize(16))
	ciphertext, keyMetadata := encryptAll(t, mgr, "kek-1", nil)

	in, err := mgr.NewDecryptedInputFile(t.Context(), newMemFile(ciphertext), keyMetadata)
	require.NoError(t, err)

	n, err := in.ReadAt(nil, 0)
	assert.NoError(t, err, "a zero-length ReadAt on an empty file should behave like bytes.Reader, not report io.EOF")
	assert.Equal(t, 0, n)
}

// failAfterNWriter is an icebergio.FileWriter stub that fails the Nth call
// to Write, simulating a mid-stream flush failure on the underlying storage.
type failAfterNWriter struct {
	memFileWriter
	failAt int
	writes int
}

func (w *failAfterNWriter) Write(p []byte) (int, error) {
	w.writes++
	if w.writes == w.failAt {
		return 0, errors.New("simulated write failure")
	}

	return w.memFileWriter.Write(p)
}

func TestStandardEncryptionManager_FlushFailurePoisonsWriterAndClose(t *testing.T) {
	mgr, _ := newTestStandardManager(t, encryption.WithBlockSize(4))
	fw := &failAfterNWriter{failAt: 2}
	out, err := mgr.NewEncryptedOutputFile(t.Context(), fw, "kek-1")
	require.NoError(t, err)

	// First block (4 bytes) flushes fine; the second block's flush fails.
	_, err = out.Write([]byte("aaaabbbb"))
	require.Error(t, err)

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
}

func TestStandardEncryptionManager_WriteAfterCloseRejected(t *testing.T) {
	mgr, _ := newTestStandardManager(t)
	out, err := mgr.NewEncryptedOutputFile(t.Context(), &memFileWriter{}, "kek-1")
	require.NoError(t, err)
	require.NoError(t, out.Close())

	_, err = out.Write([]byte("late"))
	require.Error(t, err)
	assert.True(t, errors.Is(err, encryption.ErrOutputFileClosed))
}
