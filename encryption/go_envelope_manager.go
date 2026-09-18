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
	"math"
	"sync"

	icebergio "github.com/apache/iceberg-go/io"
)

// Defaults for [GoEnvelopeEncryptionManager].
const (
	// GoEnvelopeDefaultDEKLength is the default length, in bytes, of the
	// per-file data encryption key (DEK) generated for AES-256-GCM.
	GoEnvelopeDefaultDEKLength = 32

	// GoEnvelopeDefaultBlockSize is the default plaintext block size, in
	// bytes, used to split a file into independently authenticated AES-GCM
	// blocks. Blocks allow random access (Seek/ReadAt) without buffering or
	// decrypting the whole file.
	//
	// This is not Java's Ciphers.PLAIN_BLOCK_SIZE (1 MiB, a compile-time
	// constant there); see the [GoEnvelopeEncryptionManager] doc comment.
	GoEnvelopeDefaultBlockSize = 64 * 1024

	// GoEnvelopeMaxBlockSize is the largest plaintext block size accepted
	// from the AES GCM Stream header on read, and the largest that
	// [WithBlockSize] will configure for writing. The header's block-length
	// field is unauthenticated, untrusted data (the Iceberg AES GCM Stream
	// spec's "File length" note applies equally here); without a ceiling, a
	// crafted header claiming e.g. 1<<40 bytes would force a huge
	// allocation (see readBlock) before any authentication check can run.
	GoEnvelopeMaxBlockSize = 128 * 1024 * 1024
)

// Sentinel errors returned by [GoEnvelopeEncryptionManager].
var (
	// ErrKeyIDRequired is returned by
	// [GoEnvelopeEncryptionManager.NewEncryptedOutputFile] when keyID is
	// empty. GoEnvelopeEncryptionManager always encrypts, so it requires a
	// KEK to wrap the generated DEK; use [PlaintextEncryptionManager] for
	// unencrypted tables instead of passing an empty keyID here.
	ErrKeyIDRequired = errors.New("encryption: GoEnvelopeEncryptionManager requires a non-empty keyID")

	// ErrKeyMetadataRequired is returned by
	// [GoEnvelopeEncryptionManager.NewDecryptedInputFile] when keyMetadata
	// is empty. GoEnvelopeEncryptionManager always decrypts, so it requires
	// the per-file key metadata produced by
	// [GoEnvelopeEncryptionManager.NewEncryptedOutputFile].
	ErrKeyMetadataRequired = errors.New("encryption: GoEnvelopeEncryptionManager requires non-empty key metadata")

	// ErrUnsupportedKeyMetadataVersion is returned when key metadata was
	// produced by a newer, incompatible encoding version.
	ErrUnsupportedKeyMetadataVersion = errors.New("encryption: unsupported key metadata version")

	// ErrInvalidBlockSize is returned when a configured block size is not
	// positive or exceeds [GoEnvelopeMaxBlockSize].
	ErrInvalidBlockSize = errors.New("encryption: block size must be positive and at most GoEnvelopeMaxBlockSize")

	// ErrInvalidStreamHeader is returned when the AES GCM Stream header
	// (the "AGS1" magic and little-endian block-length fields at the start
	// of an encrypted file, per the Iceberg AES GCM Stream spec) is
	// missing, truncated, or specifies a block length outside the
	// supported range.
	ErrInvalidStreamHeader = errors.New("encryption: invalid AES GCM Stream header")

	// ErrInvalidKeyMetadata is returned by
	// [GoEnvelopeEncryptionManager.NewDecryptedInputFile] when decoded key
	// metadata fails basic sanity checks (e.g. a negative plaintext length
	// or a missing AAD prefix). Key metadata is untrusted input on a crypto
	// read path, so it is validated rather than trusted blindly.
	ErrInvalidKeyMetadata = errors.New("encryption: invalid key metadata")

	// ErrOutputFileClosed is returned by [goEnvelopeOutputFile.Write] when
	// called after Close, or after a previous flush has poisoned the writer.
	// It wraps [fs.ErrClosed] so callers can test with errors.Is(err, fs.ErrClosed).
	ErrOutputFileClosed = fmt.Errorf("encryption: write to closed GoEnvelopeEncryptionManager output file: %w", fs.ErrClosed)

	// ErrBlockTruncated is returned by [goEnvelopeInputFile.ReadAt] when the
	// underlying storage returns fewer ciphertext bytes for a block than its
	// recorded length requires, indicating the file was truncated at rest.
	// This is distinct from [ErrCiphertextTooShort], which [KeyManagementClient]
	// implementations use for a too-short wrapped key or KMS-encrypted
	// payload; keeping them separate lets a caller tell a malformed KMS blob
	// apart from a short block read.
	ErrBlockTruncated = errors.New("encryption: block truncated: read fewer ciphertext bytes than expected")
)

// Constants describing the Iceberg AES GCM Stream ("AGS1") wire format used
// for the ciphertext produced by [GoEnvelopeEncryptionManager]. See
// https://iceberg.apache.org/gcm-stream-spec/ for the full specification.
const (
	// gcmStreamMagic identifies an AES GCM Stream version 1 file.
	gcmStreamMagic = "AGS1"

	// gcmStreamHeaderLength is the length, in bytes, of the magic plus the
	// little-endian block-length field written at the start of every file.
	gcmStreamHeaderLength = len(gcmStreamMagic) + 4

	// gcmStreamNonceLength is the length, in bytes, of the random AES-GCM
	// nonce stored at the start of every cipher block.
	gcmStreamNonceLength = 12

	// gcmStreamTagLength is the length, in bytes, of the AES-GCM
	// authentication tag appended to every cipher block's ciphertext.
	gcmStreamTagLength = 16

	// gcmStreamBlockOverhead is the number of ciphertext bytes added to
	// each block beyond its plaintext length (nonce + tag).
	gcmStreamBlockOverhead = gcmStreamNonceLength + gcmStreamTagLength

	// gcmStreamAADPrefixLength is the length, in bytes, of the random
	// per-file AAD prefix generated for new output files.
	gcmStreamAADPrefixLength = 16
)

// goEnvelopeKeyMetadataVersion is the current encoding version written by
// [GoEnvelopeEncryptionManager]. It is bumped whenever the on-disk layout of
// goEnvelopeKeyMetadata changes incompatibly.
const goEnvelopeKeyMetadataVersion = 1

// goEnvelopeKeyMetadata is the JSON-encoded structure stored as the opaque
// [EncryptionKeyMetadata] for files produced by [GoEnvelopeEncryptionManager].
//
// This encoding is Go-specific, not Java's Avro-encoded StandardKeyMetadata;
// see the [GoEnvelopeEncryptionManager] doc comment. Per the table spec,
// DataFile/ManifestFile key_metadata is explicitly "implementation-specific";
// what must be Iceberg AES GCM Stream compliant - and is - is the wire
// format of the encrypted byte stream itself (magic, block framing, nonce
// placement, and AAD; see the constants above).
type goEnvelopeKeyMetadata struct {
	Version    int    `json:"v"`
	KeyID      string `json:"key-id"`
	WrappedKey []byte `json:"wrapped-key"`

	// BlockSize is the trusted plaintext block size the file was written
	// with. It is validated bounded before any file I/O, and is what's
	// actually used to size reads; the stream header's block-length field
	// is untrusted (unauthenticated, attacker-influenced storage bytes) and
	// is only compared against this value, never used directly to size an
	// allocation.
	BlockSize int64 `json:"block-size"`

	// AADPrefix is combined with each block's little-endian index to form
	// the AES GCM Stream additional authenticated data, binding every
	// ciphertext block to this file and to its position so that blocks
	// cannot be silently reordered, replayed from another file, or spliced
	// in from elsewhere in the same file. It is not secret.
	AADPrefix []byte `json:"aad-prefix"`

	// PlaintextLength is the trusted total plaintext size used to compute
	// the block count on read. Per the AES GCM Stream spec's "File length"
	// note, a reader must use a length from a trusted source rather than
	// the underlying storage's reported size, since storage size alone
	// cannot distinguish a genuinely short file from one truncated by an
	// attacker who does not also control this metadata.
	PlaintextLength int64 `json:"plaintext-length"`
}

// GoEnvelopeEncryptionManager is a generic, format-agnostic [EncryptionManager]
// that provides envelope encryption for arbitrary files (e.g. manifests,
// manifest lists, Puffin statistics) using a [KeyManagementClient] to wrap
// and unwrap a fresh AES-256-GCM data encryption key (DEK) per file.
//
// Each file is split into fixed-size plaintext blocks and written using the
// Iceberg AES GCM Stream ("AGS1") format: a magic/block-length header
// followed by independently authenticated blocks, each carrying its own
// random nonce and authenticated with an AAD that binds it to the file and
// to its position. This bounds memory usage and supports random access
// (Seek/ReadAt) on the decrypted file without buffering or decrypting more
// than the requested blocks.
//
// GoEnvelopeEncryptionManager always encrypts and always decrypts: it fails
// closed, returning [ErrKeyIDRequired] or [ErrKeyMetadataRequired] rather
// than silently falling back to plaintext. Use [PlaintextEncryptionManager]
// for tables or files that are not encrypted.
//
// Not interoperable with Java's StandardEncryptionManager: the AGS1 byte
// stream framing (magic, block layout, nonce placement, AAD) matches the
// spec, but the two implementations are not cross-readable. Java's reader
// hard-requires a 1 MiB header block size (a compile-time constant there),
// while [GoEnvelopeDefaultBlockSize] is 64 KiB, and Java decodes key
// metadata as a 1-byte version plus Avro-encoded StandardKeyMetadata, while
// this type encodes key metadata as JSON (goEnvelopeKeyMetadata). Files
// written by one are not readable by the other.
type GoEnvelopeEncryptionManager struct {
	kms       KeyManagementClient
	dekLength int
	blockSize int
}

var _ EncryptionManager = (*GoEnvelopeEncryptionManager)(nil)

// GoEnvelopeManagerOption configures a [GoEnvelopeEncryptionManager] created
// by [NewGoEnvelopeEncryptionManager].
type GoEnvelopeManagerOption func(*GoEnvelopeEncryptionManager)

// WithDEKLength overrides the default data encryption key length (in bytes).
// Valid AES key lengths are 16, 24, or 32 bytes.
func WithDEKLength(length int) GoEnvelopeManagerOption {
	return func(m *GoEnvelopeEncryptionManager) { m.dekLength = length }
}

// WithBlockSize overrides the default plaintext block size (in bytes) used
// to split files for independent block-level authentication. size must be
// positive and at most [GoEnvelopeMaxBlockSize].
func WithBlockSize(size int) GoEnvelopeManagerOption {
	return func(m *GoEnvelopeEncryptionManager) { m.blockSize = size }
}

// NewGoEnvelopeEncryptionManager creates a [GoEnvelopeEncryptionManager]
// backed by kms. kms must not be nil; NewGoEnvelopeEncryptionManager panics
// if it is.
func NewGoEnvelopeEncryptionManager(kms KeyManagementClient, opts ...GoEnvelopeManagerOption) *GoEnvelopeEncryptionManager {
	if kms == nil {
		panic("encryption: NewGoEnvelopeEncryptionManager: kms must not be nil")
	}

	m := &GoEnvelopeEncryptionManager{
		kms:       kms,
		dekLength: GoEnvelopeDefaultDEKLength,
		blockSize: GoEnvelopeDefaultBlockSize,
	}
	for _, opt := range opts {
		opt(m)
	}

	return m
}

// NewEncryptedOutputFile creates a new AES-GCM block-encrypted output file.
// keyID identifies the KEK used to wrap the freshly generated per-file DEK,
// and must be non-empty; otherwise [ErrKeyIDRequired] is returned.
func (m *GoEnvelopeEncryptionManager) NewEncryptedOutputFile(ctx context.Context, writer icebergio.FileWriter, keyID string) (EncryptedOutputFile, error) {
	if keyID == "" {
		return nil, ErrKeyIDRequired
	}
	if m.blockSize <= 0 || m.blockSize > GoEnvelopeMaxBlockSize {
		return nil, fmt.Errorf("%w: got %d", ErrInvalidBlockSize, m.blockSize)
	}
	switch m.dekLength {
	case 16, 24, 32:
	default:
		return nil, fmt.Errorf("%w: DEK length must be 16, 24, or 32 bytes; got %d", ErrInvalidKeyLength, m.dekLength)
	}

	// The (key, nonce) uniqueness this block format relies on requires a
	// freshly generated DEK for every file: never cache or reuse
	// plainDEK/wrappedDEK across calls to NewEncryptedOutputFile.
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
		if _, err = io.ReadFull(rand.Reader, plainDEK); err != nil {
			return nil, fmt.Errorf("encryption: failed to generate DEK: %w", err)
		}
		if wrappedDEK, err = m.kms.WrapKey(ctx, keyID, plainDEK); err != nil {
			return nil, fmt.Errorf("encryption: failed to wrap DEK: %w", err)
		}
	}

	aead, err := newGoEnvelopeAEAD(plainDEK)
	if err != nil {
		return nil, err
	}

	aadPrefix := make([]byte, gcmStreamAADPrefixLength)
	if _, err := io.ReadFull(rand.Reader, aadPrefix); err != nil {
		return nil, fmt.Errorf("encryption: failed to generate AAD prefix: %w", err)
	}

	// Write the AES GCM Stream header (magic + little-endian block length)
	// up front, before any ciphertext blocks, per the format spec.
	header := make([]byte, gcmStreamHeaderLength)
	copy(header, gcmStreamMagic)
	binary.LittleEndian.PutUint32(header[len(gcmStreamMagic):], uint32(m.blockSize)) //nolint:gosec // bounded by ErrInvalidBlockSize above
	if _, err := writer.Write(header); err != nil {
		return nil, fmt.Errorf("encryption: failed to write stream header: %w", err)
	}

	return &goEnvelopeOutputFile{
		FileWriter: writer,
		aead:       aead,
		aadPrefix:  aadPrefix,
		blockSize:  m.blockSize,
		keyID:      keyID,
		wrappedKey: wrappedDEK,
	}, nil
}

// NewDecryptedInputFile wraps file for transparent block-level AES-GCM
// decryption. keyMetadata must be the non-empty blob produced by
// [GoEnvelopeEncryptionManager.NewEncryptedOutputFile]; otherwise
// [ErrKeyMetadataRequired] is returned.
func (m *GoEnvelopeEncryptionManager) NewDecryptedInputFile(ctx context.Context, file icebergio.File, keyMetadata EncryptionKeyMetadata) (EncryptedInputFile, error) {
	if len(keyMetadata) == 0 {
		return nil, ErrKeyMetadataRequired
	}

	var meta goEnvelopeKeyMetadata
	if err := json.Unmarshal(keyMetadata, &meta); err != nil {
		return nil, fmt.Errorf("encryption: failed to decode key metadata: %w", err)
	}
	if meta.Version != goEnvelopeKeyMetadataVersion {
		return nil, fmt.Errorf("%w: %d", ErrUnsupportedKeyMetadataVersion, meta.Version)
	}
	if meta.BlockSize <= 0 || meta.BlockSize > GoEnvelopeMaxBlockSize {
		return nil, fmt.Errorf("%w: block-size must be positive and at most %d, got %d", ErrInvalidKeyMetadata, GoEnvelopeMaxBlockSize, meta.BlockSize)
	}
	if meta.PlaintextLength < 0 {
		return nil, fmt.Errorf("%w: plaintext-length must be non-negative, got %d", ErrInvalidKeyMetadata, meta.PlaintextLength)
	}
	if len(meta.AADPrefix) == 0 {
		return nil, fmt.Errorf("%w: aad-prefix must not be empty", ErrInvalidKeyMetadata)
	}

	plainDEK, err := m.kms.UnwrapKey(ctx, meta.KeyID, meta.WrappedKey)
	if err != nil {
		return nil, fmt.Errorf("encryption: failed to unwrap DEK: %w", err)
	}

	aead, err := newGoEnvelopeAEAD(plainDEK)
	if err != nil {
		return nil, err
	}

	// headerBlockSize is untrusted (unauthenticated bytes read from
	// storage): it is only ever compared against the trusted, already
	// bounded meta.BlockSize below, never used to size a read or allocation.
	headerBlockSize, err := readGCMStreamHeader(file)
	if err != nil {
		return nil, err
	}
	if headerBlockSize != meta.BlockSize {
		return nil, fmt.Errorf("%w: stream header block length %d does not match key metadata block-size %d", ErrInvalidStreamHeader, headerBlockSize, meta.BlockSize)
	}

	return &goEnvelopeInputFile{
		underlying:      file,
		aead:            aead,
		aadPrefix:       meta.AADPrefix,
		blockSize:       meta.BlockSize,
		plaintextLength: meta.PlaintextLength,
		keyMetadata:     keyMetadata,
	}, nil
}

func newGoEnvelopeAEAD(key []byte) (cipher.AEAD, error) {
	block, err := aes.NewCipher(key)
	if err != nil {
		return nil, fmt.Errorf("%w: %w", ErrInvalidKeyLength, err)
	}
	gcm, err := cipher.NewGCM(block)
	if err != nil {
		return nil, fmt.Errorf("encryption: failed to create GCM: %w", err)
	}

	return gcm, nil
}

// readGCMStreamHeader reads and validates the AES GCM Stream magic and
// block-length header at the start of file, returning the plaintext block
// length. The header is untrusted, unauthenticated data, so the returned
// length is bounded to [GoEnvelopeMaxBlockSize] before any allocation sized
// by it takes place.
func readGCMStreamHeader(file icebergio.File) (int64, error) {
	header := make([]byte, gcmStreamHeaderLength)
	n, err := file.ReadAt(header, 0)
	if err != nil && !errors.Is(err, io.EOF) {
		return 0, fmt.Errorf("encryption: failed to read stream header: %w", err)
	}
	if n != gcmStreamHeaderLength {
		return 0, fmt.Errorf("%w: expected %d header bytes, got %d", ErrInvalidStreamHeader, gcmStreamHeaderLength, n)
	}
	if string(header[:len(gcmStreamMagic)]) != gcmStreamMagic {
		return 0, fmt.Errorf("%w: missing %q magic", ErrInvalidStreamHeader, gcmStreamMagic)
	}

	blockSize := int64(binary.LittleEndian.Uint32(header[len(gcmStreamMagic):]))
	if blockSize <= 0 || blockSize > GoEnvelopeMaxBlockSize {
		return 0, fmt.Errorf("%w: block length %d out of supported range (0, %d]", ErrInvalidStreamHeader, blockSize, GoEnvelopeMaxBlockSize)
	}

	return blockSize, nil
}

// gcmStreamBlockAAD derives the AES-GCM additional authenticated data for
// blockIndex: the per-file AAD prefix followed by the 4-byte little-endian
// block index, per the Iceberg AES GCM Stream spec. This binds every
// ciphertext block to this file and to its position, which matters because
// blocks carry independent random nonces (rather than a nonce derived from
// the block index): without the index in the AAD, an attacker able to
// tamper with ciphertext at rest could silently reorder or splice blocks.
func gcmStreamBlockAAD(prefix []byte, blockIndex uint32) []byte {
	aad := make([]byte, len(prefix)+4)
	copy(aad, prefix)
	binary.LittleEndian.PutUint32(aad[len(prefix):], blockIndex)

	return aad
}

// checkedMulInt64 returns a*b and true, or (0, false) if the multiplication
// overflows int64.
func checkedMulInt64(a, b int64) (int64, bool) {
	if a == 0 || b == 0 {
		return 0, true
	}
	result := a * b
	if result/b != a {
		return 0, false
	}

	return result, true
}

// checkedAddInt64 returns a+b and true, or (0, false) if the addition
// overflows int64.
func checkedAddInt64(a, b int64) (int64, bool) {
	result := a + b
	if (b > 0 && result < a) || (b < 0 && result > a) {
		return 0, false
	}

	return result, true
}

// goEnvelopeOutputFile is an [EncryptedOutputFile] that seals fixed-size
// plaintext blocks with AES-GCM as they are written, using the Iceberg AES
// GCM Stream ("AGS1") wire format.
type goEnvelopeOutputFile struct {
	icebergio.FileWriter

	aead       cipher.AEAD
	aadPrefix  []byte
	blockSize  int
	keyID      string
	wrappedKey []byte

	buf        []byte
	blockIndex uint32
	written    int64
	closed     bool
	err        error

	// underlyingClosed tracks whether FileWriter.Close has already been
	// attempted, so a poisoned writer is closed exactly once regardless of
	// whether the failure is first observed in Write or in Close.
	underlyingClosed bool

	keyMetadata EncryptionKeyMetadata
}

var _ EncryptedOutputFile = (*goEnvelopeOutputFile)(nil)

// closeUnderlyingIgnoringError closes the underlying writer at most once.
// The error is ignored: the caller is already reporting a more specific
// failure (a flush or encode error), and this is best-effort cleanup so a
// poisoned writer never leaks its underlying file descriptor or connection.
//
// defensive: today every call site sets f.err before calling this, and
// Write/Close both bail out early once f.err is set, so the
// underlyingClosed guard never actually stops a second Close in practice.
// It stays as a hard invariant in case a future call site is added that
// doesn't follow that pattern.
func (f *goEnvelopeOutputFile) closeUnderlyingIgnoringError() {
	if f.underlyingClosed {
		return
	}
	f.underlyingClosed = true
	_ = f.FileWriter.Close()
}

func (f *goEnvelopeOutputFile) Write(p []byte) (int, error) {
	if f.err != nil {
		return 0, f.err
	}
	if f.closed {
		return 0, ErrOutputFileClosed
	}

	total := len(p)
	consumed := 0 // bytes of p appended into f.buf so far in this call
	accepted := 0 // bytes of p known to be durably flushed; reported on failure
	for len(p) > 0 {
		space := f.blockSize - len(f.buf)
		n := min(space, len(p))
		f.buf = append(f.buf, p[:n]...)
		p = p[n:]
		consumed += n
		if len(f.buf) == f.blockSize {
			if err := f.flushBlock(); err != nil {
				f.err = err
				f.closeUnderlyingIgnoringError()

				return accepted, err
			}
			accepted = consumed
		}
	}

	return total, nil
}

// flushBlock seals and writes the currently buffered plaintext block using a
// fresh random nonce, per the Iceberg AES GCM Stream format. f.written is
// only advanced once the ciphertext has actually reached the underlying
// writer, so a failed flush never overcounts PlaintextLength.
func (f *goEnvelopeOutputFile) flushBlock() error {
	if f.blockIndex == math.MaxUint32 {
		return errors.New("encryption: cannot write block: exceeded maximum block count")
	}

	nonce := make([]byte, gcmStreamNonceLength)
	if _, err := io.ReadFull(rand.Reader, nonce); err != nil {
		return fmt.Errorf("encryption: failed to generate block nonce: %w", err)
	}

	aad := gcmStreamBlockAAD(f.aadPrefix, f.blockIndex)
	sealed := f.aead.Seal(nonce, nonce, f.buf, aad)
	if _, err := f.FileWriter.Write(sealed); err != nil {
		return fmt.Errorf("encryption: failed to write encrypted block: %w", err)
	}
	f.written += int64(len(f.buf))
	f.blockIndex++
	f.buf = f.buf[:0]

	return nil
}

// ReadFrom copies from r, encrypting as data is written, satisfying
// io.ReaderFrom (required by [icebergio.FileWriter]).
func (f *goEnvelopeOutputFile) ReadFrom(r io.Reader) (int64, error) {
	buf := make([]byte, max(32*1024, f.blockSize))
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

// Close flushes any buffered partial block and finalizes the key metadata.
// closed is only set, and keyMetadata only published, once everything -
// including the underlying Close - has succeeded; a failed Close poisons the
// writer (via f.err) so a retry reliably reports the same error instead of
// masking the failure as success or exposing metadata for an output that
// never finished.
func (f *goEnvelopeOutputFile) Close() error {
	if f.err != nil {
		return f.err
	}
	if f.closed {
		return nil
	}

	if len(f.buf) > 0 {
		if err := f.flushBlock(); err != nil {
			f.err = err
			f.closeUnderlyingIgnoringError()

			return err
		}
	}

	meta := goEnvelopeKeyMetadata{
		Version:         goEnvelopeKeyMetadataVersion,
		KeyID:           f.keyID,
		WrappedKey:      f.wrappedKey,
		BlockSize:       int64(f.blockSize),
		AADPrefix:       f.aadPrefix,
		PlaintextLength: f.written,
	}
	encoded, err := json.Marshal(meta)
	if err != nil {
		f.err = fmt.Errorf("encryption: failed to encode key metadata: %w", err)
		f.closeUnderlyingIgnoringError()

		return f.err
	}

	if err := f.FileWriter.Close(); err != nil {
		f.underlyingClosed = true
		f.err = fmt.Errorf("encryption: failed to close underlying writer: %w", err)

		return f.err
	}
	f.underlyingClosed = true

	f.keyMetadata = encoded
	f.closed = true

	return nil
}

// KeyMetadata returns the finalized per-file key metadata. It is only
// populated after Close has succeeded.
func (f *goEnvelopeOutputFile) KeyMetadata() EncryptionKeyMetadata { return f.keyMetadata }

// goEnvelopeInputFile is an [EncryptedInputFile] that decrypts fixed-size
// AES-GCM blocks on demand, supporting random access via ReadAt/Seek.
//
// ReadAt is safe, and intended, for concurrent use, matching the io.ReaderAt
// contract: readBlock only holds cacheMu around the cache check/publish, not
// across the underlying ReadAt or AEAD decryption, so concurrent calls for
// distinct blocks actually run in parallel rather than being serialized. A
// lost cache race (two goroutines decrypting the same block) is harmless,
// just wasted work. Read and Seek mutate the shared cursor (pos) and are not
// concurrent-safe; do not call them from multiple goroutines on the same
// instance.
//
// The single-entry cache retains one full decrypted block (up to
// [GoEnvelopeMaxBlockSize], 128 MiB) for the lifetime of the input file, not
// just the lifetime of a single read call.
type goEnvelopeInputFile struct {
	underlying      icebergio.File
	aead            cipher.AEAD
	aadPrefix       []byte
	blockSize       int64
	plaintextLength int64
	keyMetadata     EncryptionKeyMetadata

	pos int64

	// cacheMu guards cacheIdx/cacheBlock/cacheValid below. It is only held
	// around the cache check and the cache publish, never across the
	// underlying ReadAt or AEAD decryption in readBlock.
	cacheMu    sync.Mutex
	cacheIdx   int64
	cacheBlock []byte
	cacheValid bool
}

var _ EncryptedInputFile = (*goEnvelopeInputFile)(nil)

func (f *goEnvelopeInputFile) numBlocks() int64 {
	if f.plaintextLength == 0 {
		return 0
	}

	// Overflow-safe ceiling division: plaintextLength and blockSize both
	// originate from data outside this reader's control (key metadata and
	// the untrusted stream header, respectively), so avoid computing
	// (plaintextLength + blockSize - 1), which can overflow near
	// math.MaxInt64.
	return 1 + (f.plaintextLength-1)/f.blockSize
}

// blockPlainLen returns the plaintext length of block idx, given the total
// numBlocks (passed in rather than recomputed, since every caller has
// already computed it).
func (f *goEnvelopeInputFile) blockPlainLen(idx, numBlocks int64) int64 {
	if idx == numBlocks-1 {
		return f.plaintextLength - idx*f.blockSize
	}

	return f.blockSize
}

// blockPhysicalOffset computes the physical offset of block idx in the
// underlying ciphertext, using overflow-checked arithmetic. idx is derived
// from the untrusted plaintext-length in key metadata, so a small blockSize
// combined with a huge plaintext-length could otherwise overflow int64
// before the resulting (implausible) offset is ever used in a read.
func blockPhysicalOffset(idx, blockSize int64) (int64, error) {
	physicalBlockSize, ok := checkedAddInt64(blockSize, gcmStreamBlockOverhead)
	if !ok {
		return 0, fmt.Errorf("%w: block size %d overflows physical layout", ErrInvalidKeyMetadata, blockSize)
	}
	product, ok := checkedMulInt64(idx, physicalBlockSize)
	if !ok {
		return 0, fmt.Errorf("%w: block %d physical offset overflows", ErrInvalidKeyMetadata, idx)
	}
	offset, ok := checkedAddInt64(product, int64(gcmStreamHeaderLength))
	if !ok {
		return 0, fmt.Errorf("%w: block %d physical offset overflows", ErrInvalidKeyMetadata, idx)
	}

	return offset, nil
}

// readBlock decrypts block idx, or returns it from the single-entry cache
// if it was the most recently decrypted block. It validates idx and the
// computed block length before reading, since metadata (blockSize,
// plaintextLength) can originate from untrusted input. It also honors the
// actual byte count returned by ReadAt: a short, non-EOF-explained read is
// reported as [ErrBlockTruncated] (truncated storage) rather than being
// silently zero-padded into the AEAD, which would otherwise surface as a
// misleading [ErrAuthenticationFailed].
//
// cacheMu is only held around the cache check and the cache publish at the
// end, never across the underlying ReadAt or aead.Open below: holding it
// across I/O would serialize all concurrent readers, including remote
// backends. A lost race between two goroutines decrypting the same block is
// harmless (redundant work, not a correctness issue).
func (f *goEnvelopeInputFile) readBlock(idx int64) ([]byte, error) {
	f.cacheMu.Lock()
	if f.cacheValid && f.cacheIdx == idx {
		block := f.cacheBlock
		f.cacheMu.Unlock()

		return block, nil
	}
	f.cacheMu.Unlock()

	numBlocks := f.numBlocks()
	// defensive: idx is only ever derived from ReadAt's off, which already
	// rejects off >= f.plaintextLength before calling readBlock; kept as a
	// hard boundary check since idx ultimately traces back to untrusted
	// key metadata (plaintextLength, blockSize).
	if idx < 0 || idx >= numBlocks {
		return nil, fmt.Errorf("%w: block index %d out of range [0, %d)", ErrInvalidKeyMetadata, idx, numBlocks)
	}
	if idx > math.MaxUint32 {
		return nil, fmt.Errorf("%w: block index %d exceeds the maximum supported block count", ErrInvalidKeyMetadata, idx)
	}

	plainLen := f.blockPlainLen(idx, numBlocks)
	if plainLen < 0 {
		return nil, fmt.Errorf("%w: negative computed length for block %d", ErrInvalidKeyMetadata, idx)
	}

	offset, err := blockPhysicalOffset(idx, f.blockSize)
	if err != nil {
		return nil, err
	}

	wantLen := plainLen + gcmStreamBlockOverhead
	ciphertext := make([]byte, wantLen)
	n, err := f.underlying.ReadAt(ciphertext, offset)
	if err != nil && !errors.Is(err, io.EOF) {
		return nil, fmt.Errorf("encryption: failed to read block %d: %w", idx, err)
	}
	if int64(n) != wantLen {
		return nil, fmt.Errorf("%w: block %d: read %d of %d expected ciphertext bytes", ErrBlockTruncated, idx, n, wantLen)
	}
	ciphertext = ciphertext[:n]

	nonce, sealed := ciphertext[:gcmStreamNonceLength], ciphertext[gcmStreamNonceLength:]
	aad := gcmStreamBlockAAD(f.aadPrefix, uint32(idx))

	plaintext, err := f.aead.Open(nil, nonce, sealed, aad)
	if err != nil {
		return nil, fmt.Errorf("%w: block %d: %w", ErrAuthenticationFailed, idx, err)
	}

	f.cacheMu.Lock()
	f.cacheIdx = idx
	f.cacheBlock = plaintext
	f.cacheValid = true
	f.cacheMu.Unlock()

	return plaintext, nil
}

func (f *goEnvelopeInputFile) ReadAt(p []byte, off int64) (int, error) {
	if len(p) == 0 {
		return 0, nil
	}
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
		idx := curOff / f.blockSize
		block, err := f.readBlock(idx)
		if err != nil {
			return read, err
		}
		inBlockOff := curOff - idx*f.blockSize
		read += copy(p[read:], block[inBlockOff:])
	}

	var err error
	if read < len(p) {
		err = io.EOF
	}

	return read, err
}

func (f *goEnvelopeInputFile) Read(p []byte) (int, error) {
	n, err := f.ReadAt(p, f.pos)
	f.pos += int64(n)

	return n, err
}

func (f *goEnvelopeInputFile) Seek(offset int64, whence int) (int64, error) {
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

func (f *goEnvelopeInputFile) Close() error { return f.underlying.Close() }

func (f *goEnvelopeInputFile) Stat() (fs.FileInfo, error) {
	info, err := f.underlying.Stat()
	if err != nil {
		return nil, err
	}

	return goEnvelopeFileInfo{FileInfo: info, size: f.plaintextLength}, nil
}

// KeyMetadata returns the key metadata this file was decrypted with.
func (f *goEnvelopeInputFile) KeyMetadata() EncryptionKeyMetadata { return f.keyMetadata }

// goEnvelopeFileInfo overrides Size() to report the plaintext length rather
// than the (larger) on-disk ciphertext length.
type goEnvelopeFileInfo struct {
	fs.FileInfo
	size int64
}

func (i goEnvelopeFileInfo) Size() int64 { return i.size }
