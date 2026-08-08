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
