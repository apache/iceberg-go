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

package encryption

import (
	"context"
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/fs"

	icebergio "github.com/apache/iceberg-go/io"
)

// Defaults for [StandardEncryptionManager].
const (
	// StandardDefaultDEKLength is the default length, in bytes, of the
	// per-file data encryption key (DEK) generated for AES-256-GCM.
	StandardDefaultDEKLength = 32

	// StandardDefaultBlockSize is the default plaintext block size, in
	// bytes, used to split a file into independently authenticated AES-GCM
	// blocks. Blocks allow random access (Seek/ReadAt) without buffering or
	// decrypting the whole file.
	StandardDefaultBlockSize = 64 * 1024
)

// Sentinel errors returned by [StandardEncryptionManager].
var (
	// ErrKeyIDRequired is returned by
	// [StandardEncryptionManager.NewEncryptedOutputFile] when keyID is empty.
	// StandardEncryptionManager always encrypts, so it requires a KEK to wrap
	// the generated DEK; use [PlaintextEncryptionManager] for unencrypted
	// tables instead of passing an empty keyID here.
	ErrKeyIDRequired = errors.New("encryption: StandardEncryptionManager requires a non-empty keyID")

	// ErrKeyMetadataRequired is returned by
	// [StandardEncryptionManager.NewDecryptedInputFile] when keyMetadata is
	// empty. StandardEncryptionManager always decrypts, so it requires the
	// per-file key metadata produced by [StandardEncryptionManager.NewEncryptedOutputFile].
	ErrKeyMetadataRequired = errors.New("encryption: StandardEncryptionManager requires non-empty key metadata")

	// ErrUnsupportedKeyMetadataVersion is returned when key metadata was
	// produced by a newer, incompatible encoding version.
	ErrUnsupportedKeyMetadataVersion = errors.New("encryption: unsupported key metadata version")

	// ErrInvalidBlockSize is returned when a configured or decoded block
	// size is not positive.
	ErrInvalidBlockSize = errors.New("encryption: block size must be positive")

	// ErrInvalidKeyMetadata is returned by
	// [StandardEncryptionManager.NewDecryptedInputFile] when decoded key
	// metadata fails basic sanity checks (e.g. a negative plaintext length
	// or a nonce prefix of the wrong size). Key metadata is untrusted input
	// on a crypto read path, so it is validated rather than trusted blindly.
	ErrInvalidKeyMetadata = errors.New("encryption: invalid key metadata")
)

// standardKeyMetadataVersion is the current encoding version written by
// [StandardEncryptionManager]. It is bumped whenever the on-disk layout of
// standardKeyMetadata or the block ciphertext format changes incompatibly.
const standardKeyMetadataVersion = 1

// standardKeyMetadata is the JSON-encoded structure stored as the opaque
// [EncryptionKeyMetadata] for files produced by [StandardEncryptionManager].
type standardKeyMetadata struct {
	Version         int    `json:"v"`
	KeyID           string `json:"key-id"`
	WrappedKey      []byte `json:"wrapped-key"`
	NoncePrefix     []byte `json:"nonce-prefix"`
	BlockSize       int    `json:"block-size"`
	PlaintextLength int64  `json:"plaintext-length"`
}

// StandardEncryptionManager is a generic, format-agnostic [EncryptionManager]
// that provides envelope encryption for arbitrary files (e.g. manifests,
// manifest lists, Puffin statistics) using a [KeyManagementClient] to wrap
// and unwrap a fresh AES-256-GCM data encryption key (DEK) per file.
//
// Each file is split into fixed-size plaintext blocks, and each block is
// sealed independently with AES-GCM using a unique nonce (a per-file random
// prefix combined with the block index). This bounds memory usage and
// supports random access (Seek/ReadAt) on the decrypted file without
// buffering or decrypting more than the requested blocks.
//
// StandardEncryptionManager always encrypts and always decrypts: it fails
// closed, returning [ErrKeyIDRequired] or [ErrKeyMetadataRequired] rather
// than silently falling back to plaintext. Use [PlaintextEncryptionManager]
// for tables or files that are not encrypted.
type StandardEncryptionManager struct {
	kms       KeyManagementClient
	dekLength int
	blockSize int
}

var _ EncryptionManager = (*StandardEncryptionManager)(nil)

// StandardManagerOption configures a [StandardEncryptionManager] created by
// [NewStandardEncryptionManager].
type StandardManagerOption func(*StandardEncryptionManager)

// WithDEKLength overrides the default data encryption key length (in bytes).
// Valid AES key lengths are 16, 24, or 32 bytes.
func WithDEKLength(length int) StandardManagerOption {
	return func(m *StandardEncryptionManager) { m.dekLength = length }
}

// WithBlockSize overrides the default plaintext block size (in bytes) used
// to split files for independent block-level authentication.
func WithBlockSize(size int) StandardManagerOption {
	return func(m *StandardEncryptionManager) { m.blockSize = size }
}

// NewStandardEncryptionManager creates a [StandardEncryptionManager] backed
// by kms. kms must not be nil.
func NewStandardEncryptionManager(kms KeyManagementClient, opts ...StandardManagerOption) *StandardEncryptionManager {
	m := &StandardEncryptionManager{
		kms:       kms,
		dekLength: StandardDefaultDEKLength,
		blockSize: StandardDefaultBlockSize,
	}
	for _, opt := range opts {
		opt(m)
	}

	return m
}

// NewEncryptedOutputFile creates a new AES-GCM block-encrypted output file.
// keyID identifies the KEK used to wrap the freshly generated per-file DEK,
// and must be non-empty; otherwise [ErrKeyIDRequired] is returned.
func (m *StandardEncryptionManager) NewEncryptedOutputFile(ctx context.Context, writer icebergio.FileWriter, keyID string) (EncryptedOutputFile, error) {
	if keyID == "" {
		return nil, ErrKeyIDRequired
	}
	if m.blockSize <= 0 {
		return nil, fmt.Errorf("%w: got %d", ErrInvalidBlockSize, m.blockSize)
	}

	var (
		plainDEK, wrappedDEK []byte
		err                  error
	)
	if m.kms.SupportsKeyGeneration() {
		plainDEK, wrappedDEK, err = m.kms.GenerateKey(ctx, keyID, m.dekLength)
		if err != nil {
			return nil, fmt.Errorf("encryption: failed to generate DEK: %w", err)
		}
	} else {
		plainDEK = make([]byte, m.dekLength)
		if _, err = rand.Read(plainDEK); err != nil {
			return nil, fmt.Errorf("encryption: failed to generate DEK: %w", err)
		}
		if wrappedDEK, err = m.kms.WrapKey(ctx, keyID, plainDEK); err != nil {
			return nil, fmt.Errorf("encryption: failed to wrap DEK: %w", err)
		}
	}

	aead, err := newStandardAEAD(plainDEK)
	if err != nil {
		return nil, err
	}

	noncePrefix := make([]byte, 4)
	if _, err := rand.Read(noncePrefix); err != nil {
		return nil, fmt.Errorf("encryption: failed to generate nonce prefix: %w", err)
	}

	return &standardOutputFile{
		FileWriter:  writer,
		aead:        aead,
		noncePrefix: noncePrefix,
		blockSize:   m.blockSize,
		keyID:       keyID,
		wrappedKey:  wrappedDEK,
	}, nil
}

// NewDecryptedInputFile wraps file for transparent block-level AES-GCM
// decryption. keyMetadata must be the non-empty blob produced by
// [StandardEncryptionManager.NewEncryptedOutputFile]; otherwise
// [ErrKeyMetadataRequired] is returned.
func (m *StandardEncryptionManager) NewDecryptedInputFile(ctx context.Context, file icebergio.File, keyMetadata EncryptionKeyMetadata) (EncryptedInputFile, error) {
	if len(keyMetadata) == 0 {
		return nil, ErrKeyMetadataRequired
	}

	var meta standardKeyMetadata
	if err := json.Unmarshal(keyMetadata, &meta); err != nil {
		return nil, fmt.Errorf("encryption: failed to decode key metadata: %w", err)
	}
	if meta.Version != standardKeyMetadataVersion {
		return nil, fmt.Errorf("%w: %d", ErrUnsupportedKeyMetadataVersion, meta.Version)
	}
	if meta.BlockSize <= 0 {
		return nil, fmt.Errorf("%w: block-size must be positive, got %d", ErrInvalidKeyMetadata, meta.BlockSize)
	}
	if meta.PlaintextLength < 0 {
		return nil, fmt.Errorf("%w: plaintext-length must be non-negative, got %d", ErrInvalidKeyMetadata, meta.PlaintextLength)
	}
	if len(meta.NoncePrefix) != 4 {
		return nil, fmt.Errorf("%w: nonce-prefix must be 4 bytes, got %d", ErrInvalidKeyMetadata, len(meta.NoncePrefix))
	}

	plainDEK, err := m.kms.UnwrapKey(ctx, meta.KeyID, meta.WrappedKey)
	if err != nil {
		return nil, fmt.Errorf("encryption: failed to unwrap DEK: %w", err)
	}

	aead, err := newStandardAEAD(plainDEK)
	if err != nil {
		return nil, err
	}

	return &standardInputFile{
		underlying:      file,
		aead:            aead,
		noncePrefix:     meta.NoncePrefix,
		blockSize:       meta.BlockSize,
		plaintextLength: meta.PlaintextLength,
		keyMetadata:     keyMetadata,
	}, nil
}

func newStandardAEAD(key []byte) (cipher.AEAD, error) {
	block, err := aes.NewCipher(key)
	if err != nil {
		return nil, fmt.Errorf("%w: %v", ErrInvalidKeyLength, err)
	}
	gcm, err := cipher.NewGCM(block)
	if err != nil {
		return nil, fmt.Errorf("encryption: failed to create GCM: %w", err)
	}

	return gcm, nil
}

// standardBlockNonce derives the AES-GCM nonce for blockIndex: the 4-byte
// per-file random prefix followed by the 8-byte big-endian block index. The
// (key, nonce) pair is unique per block since the DEK is fresh per file and
// no two blocks in the same file share an index.
func standardBlockNonce(prefix []byte, blockIndex uint64) []byte {
	nonce := make([]byte, 12)
	copy(nonce, prefix)
	binary.BigEndian.PutUint64(nonce[4:], blockIndex)

	return nonce
}

// standardOutputFile is an [EncryptedOutputFile] that seals fixed-size
// plaintext blocks with AES-GCM as they are written.
type standardOutputFile struct {
	icebergio.FileWriter

	aead        cipher.AEAD
	noncePrefix []byte
	blockSize   int
	keyID       string
	wrappedKey  []byte

	buf        []byte
	blockIndex uint64
	written    int64
	closed     bool

	keyMetadata EncryptionKeyMetadata
}

var _ EncryptedOutputFile = (*standardOutputFile)(nil)

func (f *standardOutputFile) Write(p []byte) (int, error) {
	total := len(p)
	for len(p) > 0 {
		space := f.blockSize - len(f.buf)
		n := min(space, len(p))
		f.buf = append(f.buf, p[:n]...)
		p = p[n:]
		f.written += int64(n)
		if len(f.buf) == f.blockSize {
			if err := f.flushBlock(); err != nil {
				return total - len(p), err
			}
		}
	}

	return total, nil
}

func (f *standardOutputFile) flushBlock() error {
	ciphertext := f.aead.Seal(nil, standardBlockNonce(f.noncePrefix, f.blockIndex), f.buf, nil)
	if _, err := f.FileWriter.Write(ciphertext); err != nil {
		return fmt.Errorf("encryption: failed to write encrypted block: %w", err)
	}
	f.blockIndex++
	f.buf = f.buf[:0]

	return nil
}

// ReadFrom copies from r, encrypting as data is written, satisfying
// io.ReaderFrom (required by [icebergio.FileWriter]).
func (f *standardOutputFile) ReadFrom(r io.Reader) (int64, error) {
	buf := make([]byte, 32*1024)
	var total int64
	for {
		n, err := r.Read(buf)
		if n > 0 {
			wn, werr := f.Write(buf[:n])
			total += int64(wn)
			if werr != nil {
				return total, werr
			}
		}
		if err == io.EOF {
			break
		}
		if err != nil {
			return total, err
		}
	}

	return total, nil
}

func (f *standardOutputFile) Close() error {
	if f.closed {
		return nil
	}
	f.closed = true

	if len(f.buf) > 0 {
		if err := f.flushBlock(); err != nil {
			_ = f.FileWriter.Close()

			return err
		}
	}

	meta := standardKeyMetadata{
		Version:         standardKeyMetadataVersion,
		KeyID:           f.keyID,
		WrappedKey:      f.wrappedKey,
		NoncePrefix:     f.noncePrefix,
		BlockSize:       f.blockSize,
		PlaintextLength: f.written,
	}
	encoded, err := json.Marshal(meta)
	if err != nil {
		_ = f.FileWriter.Close()

		return fmt.Errorf("encryption: failed to encode key metadata: %w", err)
	}
	f.keyMetadata = encoded

	return f.FileWriter.Close()
}

// KeyMetadata returns the finalized per-file key metadata. It is only
// populated after Close has been called.
func (f *standardOutputFile) KeyMetadata() EncryptionKeyMetadata { return f.keyMetadata }

// standardInputFile is an [EncryptedInputFile] that decrypts fixed-size
// AES-GCM blocks on demand, supporting random access via ReadAt/Seek.
type standardInputFile struct {
	underlying      icebergio.File
	aead            cipher.AEAD
	noncePrefix     []byte
	blockSize       int
	plaintextLength int64
	keyMetadata     EncryptionKeyMetadata

	pos int64
}

var _ EncryptedInputFile = (*standardInputFile)(nil)

func (f *standardInputFile) numBlocks() int64 {
	if f.plaintextLength == 0 {
		return 0
	}

	return (f.plaintextLength + int64(f.blockSize) - 1) / int64(f.blockSize)
}

func (f *standardInputFile) blockPlainLen(idx int64) int64 {
	if idx == f.numBlocks()-1 {
		return f.plaintextLength - idx*int64(f.blockSize)
	}

	return int64(f.blockSize)
}

func (f *standardInputFile) physicalOffset(idx int64) int64 {
	return idx * int64(f.blockSize+f.aead.Overhead())
}

func (f *standardInputFile) readBlock(idx int64) ([]byte, error) {
	plainLen := f.blockPlainLen(idx)
	ciphertext := make([]byte, plainLen+int64(f.aead.Overhead()))
	if _, err := f.underlying.ReadAt(ciphertext, f.physicalOffset(idx)); err != nil && !errors.Is(err, io.EOF) {
		return nil, fmt.Errorf("encryption: failed to read block %d: %w", idx, err)
	}

	plaintext, err := f.aead.Open(nil, standardBlockNonce(f.noncePrefix, uint64(idx)), ciphertext, nil)
	if err != nil {
		return nil, fmt.Errorf("%w: block %d: %v", ErrAuthenticationFailed, idx, err)
	}

	return plaintext, nil
}

func (f *standardInputFile) ReadAt(p []byte, off int64) (int, error) {
	if off < 0 {
		return 0, errors.New("encryption: ReadAt: negative offset")
	}
	if off >= f.plaintextLength {
		return 0, io.EOF
	}

	var read int
	for read < len(p) {
		curOff := off + int64(read)
		if curOff >= f.plaintextLength {
			break
		}
		idx := curOff / int64(f.blockSize)
		block, err := f.readBlock(idx)
		if err != nil {
			return read, err
		}
		inBlockOff := curOff - idx*int64(f.blockSize)
		read += copy(p[read:], block[inBlockOff:])
	}

	var err error
	if read < len(p) {
		err = io.EOF
	}

	return read, err
}

func (f *standardInputFile) Read(p []byte) (int, error) {
	n, err := f.ReadAt(p, f.pos)
	f.pos += int64(n)

	return n, err
}

func (f *standardInputFile) Seek(offset int64, whence int) (int64, error) {
	var newPos int64
	switch whence {
	case io.SeekStart:
		newPos = offset
	case io.SeekCurrent:
		newPos = f.pos + offset
	case io.SeekEnd:
		newPos = f.plaintextLength + offset
	default:
		return 0, fmt.Errorf("encryption: Seek: invalid whence %d", whence)
	}
	if newPos < 0 {
		return 0, errors.New("encryption: Seek: negative position")
	}
	f.pos = newPos

	return newPos, nil
}

func (f *standardInputFile) Close() error { return f.underlying.Close() }

func (f *standardInputFile) Stat() (fs.FileInfo, error) {
	info, err := f.underlying.Stat()
	if err != nil {
		return nil, err
	}

	return standardFileInfo{FileInfo: info, size: f.plaintextLength}, nil
}

// KeyMetadata returns the key metadata this file was decrypted with.
func (f *standardInputFile) KeyMetadata() EncryptionKeyMetadata { return f.keyMetadata }

// standardFileInfo overrides Size() to report the plaintext length rather
// than the (larger) on-disk ciphertext length.
type standardFileInfo struct {
	fs.FileInfo
	size int64
}

func (i standardFileInfo) Size() int64 { return i.size }
