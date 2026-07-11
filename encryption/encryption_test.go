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
	"io/fs"
	"testing"

	"github.com/apache/iceberg-go/encryption"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"golang.org/x/sync/errgroup"
)

// ---------------------------------------------------------------------------
// Minimal in-memory io.File / io.FileWriter stubs for testing
// ---------------------------------------------------------------------------

// memFile is a minimal in-memory implementation of icebergio.File.
type memFile struct {
	r *bytes.Reader
}

func newMemFile(data []byte) *memFile {
	buf := make([]byte, len(data))
	copy(buf, data)

	return &memFile{r: bytes.NewReader(buf)}
}

func (f *memFile) Stat() (fs.FileInfo, error)              { return nil, nil }
func (f *memFile) Read(p []byte) (int, error)              { return f.r.Read(p) }
func (f *memFile) Seek(off int64, w int) (int64, error)    { return f.r.Seek(off, w) }
func (f *memFile) Close() error                            { return nil }
func (f *memFile) ReadAt(p []byte, off int64) (int, error) { return f.r.ReadAt(p, off) }

// memFileWriter is a minimal in-memory implementation of icebergio.FileWriter.
type memFileWriter struct{ buf bytes.Buffer }

func (w *memFileWriter) Write(p []byte) (int, error)         { return w.buf.Write(p) }
func (w *memFileWriter) Close() error                        { return nil }
func (w *memFileWriter) ReadFrom(r io.Reader) (int64, error) { return io.Copy(&w.buf, r) }
func (w *memFileWriter) Bytes() []byte                       { return w.buf.Bytes() }

// ---------------------------------------------------------------------------
// PlaintextEncryptionManager
// ---------------------------------------------------------------------------

func TestPlaintextEncryptionManager_OutputFile(t *testing.T) {
	mgr := encryption.PlaintextEncryptionManager{}
	fw := &memFileWriter{}

	out, err := mgr.NewEncryptedOutputFile(t.Context(), fw, "unused-key-id")
	require.NoError(t, err)

	data := []byte("hello iceberg")
	_, err = out.Write(data)
	require.NoError(t, err)
	require.NoError(t, out.Close())

	assert.Nil(t, out.KeyMetadata(), "plaintext manager should return nil key metadata")
	assert.Equal(t, data, fw.Bytes(), "plaintext manager must not alter data on write")
}

func TestPlaintextEncryptionManager_InputFile(t *testing.T) {
	payload := []byte("plaintext data")
	km := encryption.EncryptionKeyMetadata([]byte("some-metadata"))
	mgr := encryption.PlaintextEncryptionManager{}

	in, err := mgr.NewDecryptedInputFile(t.Context(), newMemFile(payload), km)
	require.NoError(t, err)

	assert.Equal(t, km, in.KeyMetadata(), "key metadata should be passed through unchanged")

	got, err := io.ReadAll(in)
	require.NoError(t, err)
	assert.Equal(t, payload, got, "plaintext manager must not alter data on read")
}

func TestPlaintextEncryptionManager_InputFile_NilMetadata(t *testing.T) {
	mgr := encryption.PlaintextEncryptionManager{}
	in, err := mgr.NewDecryptedInputFile(t.Context(), newMemFile(nil), nil)
	require.NoError(t, err)
	assert.Nil(t, in.KeyMetadata())
}

// ---------------------------------------------------------------------------
// InMemoryKeyManagementClient
// ---------------------------------------------------------------------------

func TestInMemoryKMS_WrapUnwrap(t *testing.T) {
	kms := encryption.NewInMemoryKeyManagementClient()
	key := bytes.Repeat([]byte{0xAB}, 32) // AES-256 master key
	require.NoError(t, kms.AddKey("master-1", key))

	plaintext := []byte("my-dek-bytes-here-16") // 20-byte DEK
	ctx := t.Context()

	wrapped, err := kms.WrapKey(ctx, "master-1", plaintext)
	require.NoError(t, err)
	assert.NotEqual(t, plaintext, wrapped, "wrapped key must differ from plaintext")

	unwrapped, err := kms.UnwrapKey(ctx, "master-1", wrapped)
	require.NoError(t, err)
	assert.Equal(t, plaintext, unwrapped)
}

func TestInMemoryKMS_WrapUnwrap_NonDeterministic(t *testing.T) {
	// Each call to WrapKey must produce a different ciphertext (random nonce).
	kms := encryption.NewInMemoryKeyManagementClient()
	require.NoError(t, kms.AddKey("k", bytes.Repeat([]byte{0x01}, 16)))
	ctx := t.Context()

	dek := []byte("same-dek-bytes!!")
	w1, err := kms.WrapKey(ctx, "k", dek)
	require.NoError(t, err)
	w2, err := kms.WrapKey(ctx, "k", dek)
	require.NoError(t, err)
	assert.NotEqual(t, w1, w2, "each wrap must use a unique nonce")
}

func TestInMemoryKMS_AddKey_InvalidLength(t *testing.T) {
	kms := encryption.NewInMemoryKeyManagementClient()
	err := kms.AddKey("bad", []byte("tooshort")) // 8 bytes — invalid
	require.ErrorIs(t, err, encryption.ErrInvalidKeyLength)
}

func TestInMemoryKMS_UnknownKeyID(t *testing.T) {
	kms := encryption.NewInMemoryKeyManagementClient()
	_, err := kms.WrapKey(t.Context(), "no-such-key", []byte("data"))
	require.ErrorIs(t, err, encryption.ErrUnknownKeyID)
}

func TestInMemoryKMS_SupportsKeyGeneration(t *testing.T) {
	kms := encryption.NewInMemoryKeyManagementClient()
	assert.True(t, kms.SupportsKeyGeneration())
}

func TestInMemoryKMS_GenerateKey(t *testing.T) {
	kms := encryption.NewInMemoryKeyManagementClient()
	require.NoError(t, kms.AddKey("kek", bytes.Repeat([]byte{0xFF}, 32)))
	ctx := t.Context()

	plaintext, wrapped, err := kms.GenerateKey(ctx, "kek", 32)
	require.NoError(t, err)
	assert.Len(t, plaintext, 32)
	assert.NotEmpty(t, wrapped)

	// Round-trip: unwrap should give back the generated DEK.
	recovered, err := kms.UnwrapKey(ctx, "kek", wrapped)
	require.NoError(t, err)
	assert.Equal(t, plaintext, recovered)
}

func TestInMemoryKMS_GenerateKey_InvalidLength(t *testing.T) {
	kms := encryption.NewInMemoryKeyManagementClient()
	require.NoError(t, kms.AddKey("k", bytes.Repeat([]byte{0x00}, 16)))
	_, _, err := kms.GenerateKey(t.Context(), "k", 0)
	require.ErrorIs(t, err, encryption.ErrInvalidKeyLength)
}

func TestInMemoryKMS_TamperDetection(t *testing.T) {
	kms := encryption.NewInMemoryKeyManagementClient()
	require.NoError(t, kms.AddKey("k", bytes.Repeat([]byte{0x42}, 32)))
	ctx := t.Context()

	wrapped, err := kms.WrapKey(ctx, "k", []byte("important-dek!!"))
	require.NoError(t, err)

	// Flip one byte in the ciphertext portion (after the 12-byte nonce).
	tampered := make([]byte, len(wrapped))
	copy(tampered, wrapped)
	tampered[12] ^= 0xFF

	_, err = kms.UnwrapKey(ctx, "k", tampered)
	require.ErrorIs(t, err, encryption.ErrAuthenticationFailed, "tampered ciphertext must fail authentication")
}

func TestInMemoryKMS_ShortCiphertext(t *testing.T) {
	kms := encryption.NewInMemoryKeyManagementClient()
	require.NoError(t, kms.AddKey("k", bytes.Repeat([]byte{0x33}, 16)))

	_, err := kms.UnwrapKey(t.Context(), "k", []byte{0x00, 0x01}) // shorter than AES-GCM nonce
	require.ErrorIs(t, err, encryption.ErrCiphertextTooShort)
}

func TestInMemoryKMS_CrossKeyConfusion(t *testing.T) {
	// Ciphertext produced with one KEK must not decrypt under another KEK,
	// even if both KEKs are the same length.
	kms := encryption.NewInMemoryKeyManagementClient()
	require.NoError(t, kms.AddKey("a", bytes.Repeat([]byte{0x01}, 32)))
	require.NoError(t, kms.AddKey("b", bytes.Repeat([]byte{0x02}, 32)))
	ctx := t.Context()

	wrapped, err := kms.WrapKey(ctx, "a", []byte("dek-under-key-a!"))
	require.NoError(t, err)

	_, err = kms.UnwrapKey(ctx, "b", wrapped)
	require.ErrorIs(t, err, encryption.ErrAuthenticationFailed)
}

func TestInMemoryKMS_ConcurrentAccess(t *testing.T) {
	kms := encryption.NewInMemoryKeyManagementClient()
	require.NoError(t, kms.AddKey("k", bytes.Repeat([]byte{0x11}, 16)))
	ctx := t.Context()
	dek := []byte("concurrent-dek!!")

	var g errgroup.Group
	for range 20 {
		g.Go(func() error {
			wrapped, err := kms.WrapKey(ctx, "k", dek)
			if err != nil {
				return err
			}
			got, err := kms.UnwrapKey(ctx, "k", wrapped)
			if err != nil {
				return err
			}
			if !bytes.Equal(got, dek) {
				return errors.New("round-trip mismatch")
			}

			return nil
		})
	}
	require.NoError(t, g.Wait())
}

func TestInMemoryKMS_AddKey_IsolatesCopy(t *testing.T) {
	kms := encryption.NewInMemoryKeyManagementClient()
	masterKey := bytes.Repeat([]byte{0xAA}, 32)
	require.NoError(t, kms.AddKey("k", masterKey))

	// Mutate the original slice after registration.
	masterKey[0] = 0x00

	// Wrap/unwrap must still work with the original key bytes.
	ctx := t.Context()
	wrapped, err := kms.WrapKey(ctx, "k", []byte("data-dek-12345678"))
	require.NoError(t, err)
	got, err := kms.UnwrapKey(ctx, "k", wrapped)
	require.NoError(t, err)
	assert.Equal(t, []byte("data-dek-12345678"), got)
}

func TestInMemoryKMS_AddKey_Overwrite(t *testing.T) {
	// AddKey documents that re-registering a keyID replaces the KEK. A wrap
	// under the old KEK must no longer decrypt after the replacement.
	kms := encryption.NewInMemoryKeyManagementClient()
	require.NoError(t, kms.AddKey("k", bytes.Repeat([]byte{0x01}, 32)))
	ctx := t.Context()

	wrapped, err := kms.WrapKey(ctx, "k", []byte("before-rotation!"))
	require.NoError(t, err)

	require.NoError(t, kms.AddKey("k", bytes.Repeat([]byte{0x02}, 32)))

	_, err = kms.UnwrapKey(ctx, "k", wrapped)
	require.ErrorIs(t, err, encryption.ErrAuthenticationFailed)
}
