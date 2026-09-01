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

	// StandardMaxBlockSize is the largest plaintext block size accepted
	// from the AES GCM Stream header on read, and the largest that
	// [WithBlockSize] will configure for writing. The header's block-length
	// field is unauthenticated, untrusted data (the Iceberg AES GCM Stream
	// spec's "File length" note applies equally here); without a ceiling, a
	// crafted header claiming e.g. 1<<40 bytes would force a huge
	// allocation (see readBlock) before any authentication check can run.
	StandardMaxBlockSize = 128 * 1024 * 1024
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

	// ErrInvalidBlockSize is returned when a configured block size is not
	// positive or exceeds [StandardMaxBlockSize].
	ErrInvalidBlockSize = errors.New("encryption: block size must be positive and at most StandardMaxBlockSize")

	// ErrInvalidStreamHeader is returned when the AES GCM Stream header
	// (the "AGS1" magic and little-endian block-length fields at the start
	// of an encrypted file, per the Iceberg AES GCM Stream spec) is
	// missing, truncated, or specifies a block length outside the
	// supported range.
	ErrInvalidStreamHeader = errors.New("encryption: invalid AES GCM Stream header")

	// ErrInvalidKeyMetadata is returned by
	// [StandardEncryptionManager.NewDecryptedInputFile] when decoded key
	// metadata fails basic sanity checks (e.g. a negative plaintext length
	// or a missing AAD prefix). Key metadata is untrusted input on a crypto
	// read path, so it is validated rather than trusted blindly.
	ErrInvalidKeyMetadata = errors.New("encryption: invalid key metadata")

	// ErrOutputFileClosed is returned by [standardOutputFile.Write] when
	// called after Close, or after a previous flush has poisoned the writer.
	// It wraps [fs.ErrClosed] so callers can test with errors.Is(err, fs.ErrClosed).
	ErrOutputFileClosed = fmt.Errorf("encryption: write to closed StandardEncryptionManager output file: %w", fs.ErrClosed)
)

// Constants describing the Iceberg AES GCM Stream ("AGS1") wire format used
// for the ciphertext produced by [StandardEncryptionManager]. See
// https://iceberg.apache.org/gcm-stream-spec/ for the full specification.
const (
	// standardStreamMagic identifies an AES GCM Stream version 1 file.
	standardStreamMagic = "AGS1"

	// standardHeaderLength is the length, in bytes, of the magic plus the
	// little-endian block-length field written at the start of every file.
	standardHeaderLength = len(standardStreamMagic) + 4

	// standardNonceLength is the length, in bytes, of the random AES-GCM
	// nonce stored at the start of every cipher block.
	standardNonceLength = 12

	// standardTagLength is the length, in bytes, of the AES-GCM
	// authentication tag appended to every cipher block's ciphertext.
	standardTagLength = 16

	// standardBlockOverhead is the number of ciphertext bytes added to
	// each block beyond its plaintext length (nonce + tag).
	standardBlockOverhead = standardNonceLength + standardTagLength

	// standardAADPrefixLength is the length, in bytes, of the random
	// per-file AAD prefix generated for new output files.
	standardAADPrefixLength = 16
)

// standardKeyMetadataVersion is the current encoding version written by
// [StandardEncryptionManager]. It is bumped whenever the on-disk layout of
// standardKeyMetadata changes incompatibly.
const standardKeyMetadataVersion = 1

// standardKeyMetadata is the JSON-encoded structure stored as the opaque
// [EncryptionKeyMetadata] for files produced by [StandardEncryptionManager].
//
// Per the table spec, DataFile/ManifestFile key_metadata is explicitly
// "implementation-specific"; what must be Iceberg AES GCM Stream compliant -
// and is - is the wire format of the encrypted byte stream itself (magic,
// block framing, nonce placement, and AAD; see the constants above).
type standardKeyMetadata struct {
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

// StandardEncryptionManager is a generic, format-agnostic [EncryptionManager]
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
// to split files for independent block-level authentication. size must be
// positive and at most [StandardMaxBlockSize].
func WithBlockSize(size int) StandardManagerOption {
	return func(m *StandardEncryptionManager) { m.blockSize = size }
}

// NewStandardEncryptionManager creates a [StandardEncryptionManager] backed
// by kms. kms must not be nil; NewStandardEncryptionManager panics if it is.
func NewStandardEncryptionManager(kms KeyManagementClient, opts ...StandardManagerOption) *StandardEncryptionManager {
	if kms == nil {
		panic("encryption: NewStandardEncryptionManager: kms must not be nil")
	}

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
	if m.blockSize <= 0 || m.blockSize > StandardMaxBlockSize {
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

	aead, err := newStandardAEAD(plainDEK)
	if err != nil {
		return nil, err
	}

	aadPrefix := make([]byte, standardAADPrefixLength)
	if _, err := io.ReadFull(rand.Reader, aadPrefix); err != nil {
		return nil, fmt.Errorf("encryption: failed to generate AAD prefix: %w", err)
	}

	// Write the AES GCM Stream header (magic + little-endian block length)
	// up front, before any ciphertext blocks, per the format spec.
	header := make([]byte, standardHeaderLength)
	copy(header, standardStreamMagic)
	binary.LittleEndian.PutUint32(header[len(standardStreamMagic):], uint32(m.blockSize)) //nolint:gosec // bounded by ErrInvalidBlockSize above
	if _, err := writer.Write(header); err != nil {
		return nil, fmt.Errorf("encryption: failed to write stream header: %w", err)
	}

	return &standardOutputFile{
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
	if meta.BlockSize <= 0 || meta.BlockSize > StandardMaxBlockSize {
		return nil, fmt.Errorf("%w: block-size must be positive and at most %d, got %d", ErrInvalidKeyMetadata, StandardMaxBlockSize, meta.BlockSize)
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

	aead, err := newStandardAEAD(plainDEK)
	if err != nil {
		return nil, err
	}

	// headerBlockSize is untrusted (unauthenticated bytes read from
	// storage): it is only ever compared against the trusted, already
	// bounded meta.BlockSize below, never used to size a read or allocation.
	headerBlockSize, err := readStandardStreamHeader(file)
	if err != nil {
		return nil, err
	}
	if headerBlockSize != meta.BlockSize {
		return nil, fmt.Errorf("%w: stream header block length %d does not match key metadata block-size %d", ErrInvalidStreamHeader, headerBlockSize, meta.BlockSize)
	}

	return &standardInputFile{
		underlying:      file,
		aead:            aead,
		aadPrefix:       meta.AADPrefix,
		blockSize:       meta.BlockSize,
		plaintextLength: meta.PlaintextLength,
		keyMetadata:     keyMetadata,
	}, nil
}

func newStandardAEAD(key []byte) (cipher.AEAD, error) {
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

// readStandardStreamHeader reads and validates the AES GCM Stream magic and
// block-length header at the start of file, returning the plaintext block
// length. The header is untrusted, unauthenticated data, so the returned
// length is bounded to [StandardMaxBlockSize] before any allocation sized by
// it takes place.
func readStandardStreamHeader(file icebergio.File) (int64, error) {
	header := make([]byte, standardHeaderLength)
	n, err := file.ReadAt(header, 0)
	if err != nil && !errors.Is(err, io.EOF) {
		return 0, fmt.Errorf("encryption: failed to read stream header: %w", err)
	}
	if n != standardHeaderLength {
		return 0, fmt.Errorf("%w: expected %d header bytes, got %d", ErrInvalidStreamHeader, standardHeaderLength, n)
	}
	if string(header[:len(standardStreamMagic)]) != standardStreamMagic {
		return 0, fmt.Errorf("%w: missing %q magic", ErrInvalidStreamHeader, standardStreamMagic)
	}

	blockSize := int64(binary.LittleEndian.Uint32(header[len(standardStreamMagic):]))
	if blockSize <= 0 || blockSize > StandardMaxBlockSize {
		return 0, fmt.Errorf("%w: block length %d out of supported range (0, %d]", ErrInvalidStreamHeader, blockSize, StandardMaxBlockSize)
	}

	return blockSize, nil
}

// standardBlockAAD derives the AES-GCM additional authenticated data for
// blockIndex: the per-file AAD prefix followed by the 4-byte little-endian
// block index, per the Iceberg AES GCM Stream spec. This binds every
// ciphertext block to this file and to its position, which matters because
// blocks carry independent random nonces (rather than a nonce derived from
// the block index): without the index in the AAD, an attacker able to
// tamper with ciphertext at rest could silently reorder or splice blocks.
func standardBlockAAD(prefix []byte, blockIndex uint32) []byte {
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

// standardOutputFile is an [EncryptedOutputFile] that seals fixed-size
// plaintext blocks with AES-GCM as they are written, using the Iceberg AES
// GCM Stream ("AGS1") wire format.
type standardOutputFile struct {
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

var _ EncryptedOutputFile = (*standardOutputFile)(nil)

// closeUnderlyingIgnoringError closes the underlying writer at most once.
// The error is ignored: the caller is already reporting a more specific
// failure (a flush or encode error), and this is best-effort cleanup so a
// poisoned writer never leaks its underlying file descriptor or connection.
func (f *standardOutputFile) closeUnderlyingIgnoringError() {
	if f.underlyingClosed {
		return
	}
	f.underlyingClosed = true
	_ = f.FileWriter.Close()
}

func (f *standardOutputFile) Write(p []byte) (int, error) {
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
func (f *standardOutputFile) flushBlock() error {
	if f.blockIndex == math.MaxUint32 {
		return errors.New("encryption: cannot write block: exceeded maximum block count")
	}

	nonce := make([]byte, standardNonceLength)
	if _, err := io.ReadFull(rand.Reader, nonce); err != nil {
		return fmt.Errorf("encryption: failed to generate block nonce: %w", err)
	}

	aad := standardBlockAAD(f.aadPrefix, f.blockIndex)
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
func (f *standardOutputFile) ReadFrom(r io.Reader) (int64, error) {
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
func (f *standardOutputFile) Close() error {
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

	meta := standardKeyMetadata{
		Version:         standardKeyMetadataVersion,
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
func (f *standardOutputFile) KeyMetadata() EncryptionKeyMetadata { return f.keyMetadata }

// standardInputFile is an [EncryptedInputFile] that decrypts fixed-size
// AES-GCM blocks on demand, supporting random access via ReadAt/Seek.
//
// ReadAt is stateless and safe for concurrent use, matching the io.ReaderAt
// contract. Read and Seek mutate the shared cursor (pos) and are not
// concurrent-safe; do not call them from multiple goroutines on the same
// instance.
type standardInputFile struct {
	underlying      icebergio.File
	aead            cipher.AEAD
	aadPrefix       []byte
	blockSize       int64
	plaintextLength int64
	keyMetadata     EncryptionKeyMetadata

	pos int64

	// cacheMu guards cacheIdx/cacheBlock/cacheValid below. Serializing
	// readBlock keeps the single-entry cache correct under the concurrent
	// ReadAt usage this type documents, and also avoids re-decrypting the
	// same block for runs of small, sequential reads that land in it.
	cacheMu    sync.Mutex
	cacheIdx   int64
	cacheBlock []byte
	cacheValid bool
}

var _ EncryptedInputFile = (*standardInputFile)(nil)

func (f *standardInputFile) numBlocks() int64 {
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

func (f *standardInputFile) blockPlainLen(idx int64) int64 {
	if idx == f.numBlocks()-1 {
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
	physicalBlockSize, ok := checkedAddInt64(blockSize, standardBlockOverhead)
	if !ok {
		return 0, fmt.Errorf("%w: block size %d overflows physical layout", ErrInvalidKeyMetadata, blockSize)
	}
	product, ok := checkedMulInt64(idx, physicalBlockSize)
	if !ok {
		return 0, fmt.Errorf("%w: block %d physical offset overflows", ErrInvalidKeyMetadata, idx)
	}
	offset, ok := checkedAddInt64(product, int64(standardHeaderLength))
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
// reported as [ErrCiphertextTooShort] (truncated storage) rather than being
// silently zero-padded into the AEAD, which would otherwise surface as a
// misleading [ErrAuthenticationFailed].
func (f *standardInputFile) readBlock(idx int64) ([]byte, error) {
	f.cacheMu.Lock()
	defer f.cacheMu.Unlock()

	if f.cacheValid && f.cacheIdx == idx {
		return f.cacheBlock, nil
	}

	numBlocks := f.numBlocks()
	if idx < 0 || idx >= numBlocks {
		return nil, fmt.Errorf("%w: block index %d out of range [0, %d)", ErrInvalidKeyMetadata, idx, numBlocks)
	}
	if idx > math.MaxUint32 {
		return nil, fmt.Errorf("%w: block index %d exceeds the maximum supported block count", ErrInvalidKeyMetadata, idx)
	}

	plainLen := f.blockPlainLen(idx)
	if plainLen < 0 {
		return nil, fmt.Errorf("%w: negative computed length for block %d", ErrInvalidKeyMetadata, idx)
	}

	offset, err := blockPhysicalOffset(idx, f.blockSize)
	if err != nil {
		return nil, err
	}

	wantLen := plainLen + standardBlockOverhead
	ciphertext := make([]byte, wantLen)
	n, err := f.underlying.ReadAt(ciphertext, offset)
	if err != nil && !errors.Is(err, io.EOF) {
		return nil, fmt.Errorf("encryption: failed to read block %d: %w", idx, err)
	}
	if int64(n) != wantLen {
		return nil, fmt.Errorf("%w: block %d: read %d of %d expected ciphertext bytes", ErrCiphertextTooShort, idx, n, wantLen)
	}
	ciphertext = ciphertext[:n]

	nonce, sealed := ciphertext[:standardNonceLength], ciphertext[standardNonceLength:]
	aad := standardBlockAAD(f.aadPrefix, uint32(idx))

	plaintext, err := f.aead.Open(nil, nonce, sealed, aad)
	if err != nil {
		return nil, fmt.Errorf("%w: block %d: %w", ErrAuthenticationFailed, idx, err)
	}

	f.cacheIdx = idx
	f.cacheBlock = plaintext
	f.cacheValid = true

	return plaintext, nil
}

func (f *standardInputFile) ReadAt(p []byte, off int64) (int, error) {
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
