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
	"errors"
	"fmt"
	"io"
	"io/fs"
	"math"
	"slices"
	"sync"

	icebergio "github.com/apache/iceberg-go/io"
)

// StandardDefaultDataKeyLength is the default length, in bytes, of the
// per-file data encryption key (DEK). It is 128-bit, matching the default of
// the encryption.data-key-length table property in the Java and Rust
// implementations.
const StandardDefaultDataKeyLength = 16

// Sentinel errors returned by [StandardEncryptionManager].
var (
	// ErrKeyIDRequired is returned by
	// [StandardEncryptionManager.NewEncryptedOutputFile] when keyID is empty.
	// StandardEncryptionManager always encrypts, so it requires the table's
	// encryption key ID; use [PlaintextEncryptionManager] for unencrypted
	// tables instead of passing an empty keyID here.
	ErrKeyIDRequired = errors.New("encryption: StandardEncryptionManager requires a non-empty keyID")

	// ErrKeyMetadataRequired is returned by
	// [StandardEncryptionManager.NewDecryptedInputFile] when keyMetadata is
	// empty. StandardEncryptionManager always decrypts, so it requires the
	// per-file key metadata produced by
	// [StandardEncryptionManager.NewEncryptedOutputFile].
	ErrKeyMetadataRequired = errors.New("encryption: StandardEncryptionManager requires non-empty key metadata")

	// ErrUnsupportedKeyMetadataVersion is returned when key metadata starts
	// with a version byte other than the one this package understands.
	ErrUnsupportedKeyMetadataVersion = errors.New("encryption: unsupported key metadata version")

	// ErrInvalidStreamHeader is returned when the AES GCM Stream header (the
	// "AGS1" magic and little-endian plain block length at the start of an
	// encrypted file, per the Iceberg AES GCM Stream spec) is missing,
	// truncated, or declares a block length other than the fixed 1 MiB.
	ErrInvalidStreamHeader = errors.New("encryption: invalid AES GCM Stream header")

	// ErrInvalidKeyMetadata is returned by
	// [StandardEncryptionManager.NewDecryptedInputFile] when key metadata is
	// not a well-formed StandardKeyMetadata record or fails basic sanity
	// checks (e.g. a truncated record, a missing or wrong-sized AAD prefix, or
	// a missing or impossible file length). Key metadata is untrusted input on
	// a crypto read path, so it is validated rather than trusted blindly.
	ErrInvalidKeyMetadata = errors.New("encryption: invalid key metadata")

	// ErrOutputFileClosed is returned by [standardOutputFile.Write] when
	// called after a successful Close. It wraps [fs.ErrClosed] so callers can
	// test with errors.Is(err, fs.ErrClosed). A Write after a failed flush or
	// a failed Close returns that original error instead.
	ErrOutputFileClosed = fmt.Errorf("encryption: write to closed StandardEncryptionManager output file: %w", fs.ErrClosed)

	// ErrBlockTruncated is returned by [standardInputFile.ReadAt] when the
	// underlying storage returns fewer ciphertext bytes for a block than its
	// recorded length requires, indicating the file was truncated at rest.
	// This is distinct from [ErrCiphertextTooShort], which [KeyManagementClient]
	// implementations use for a too-short wrapped key or KMS-encrypted
	// payload; keeping them separate lets a caller tell a malformed KMS blob
	// apart from a short block read.
	ErrBlockTruncated = errors.New("encryption: block truncated: read fewer ciphertext bytes than expected")
)

// Constants describing the Iceberg AES GCM Stream ("AGS1") wire format used
// for the ciphertext produced by [StandardEncryptionManager]. See
// https://iceberg.apache.org/gcm-stream-spec/ for the full specification.
const (
	// gcmStreamMagic identifies an AES GCM Stream version 1 file.
	gcmStreamMagic = "AGS1"

	// gcmStreamHeaderLength is the length, in bytes, of the magic plus the
	// 4-byte little-endian plain block length written at the start of every
	// file.
	gcmStreamHeaderLength = 8

	// gcmStreamPlainBlockSize is the plaintext block size. It is fixed:
	// Java's Ciphers.PLAIN_BLOCK_SIZE and iceberg-rust's PLAIN_BLOCK_SIZE are
	// both 1 MiB, and Java's AesGcmInputStream rejects any other value in the
	// header, so a stream with a different block size is unreadable there.
	gcmStreamPlainBlockSize = 1024 * 1024

	// gcmStreamNonceLength is the length, in bytes, of the random AES-GCM
	// nonce stored at the start of every cipher block.
	gcmStreamNonceLength = 12

	// gcmStreamTagLength is the length, in bytes, of the AES-GCM
	// authentication tag appended to every cipher block's ciphertext.
	gcmStreamTagLength = 16

	// gcmStreamBlockOverhead is the number of ciphertext bytes added to
	// each block beyond its plaintext length (nonce + tag).
	gcmStreamBlockOverhead = gcmStreamNonceLength + gcmStreamTagLength

	// gcmStreamCipherBlockSize is the on-disk length of a full block.
	gcmStreamCipherBlockSize = gcmStreamPlainBlockSize + gcmStreamBlockOverhead

	// gcmStreamMinLength is the length of the smallest valid stream: the
	// header followed by one block holding no plaintext. Java and iceberg-rust
	// always write block 0, even for an empty file, and iceberg-rust rejects
	// anything shorter.
	gcmStreamMinLength = gcmStreamHeaderLength + gcmStreamBlockOverhead

	// gcmStreamAADPrefixLength is the length, in bytes, of the random
	// per-file AAD prefix. It matches Java's ENCRYPTION_AAD_LENGTH_DEFAULT.
	gcmStreamAADPrefixLength = 16
)

// standardKeyMetadataVersion is the leading version byte of Iceberg's
// StandardKeyMetadata encoding.
const standardKeyMetadataVersion = 1

// standardKeyMetadata is the decoded form of Iceberg's StandardKeyMetadata:
// a version byte followed by an Avro binary record
//
//	{encryption_key: bytes, aad_prefix: ["null", bytes], file_length: ["null", long]}
//
// This is the key_metadata encoding Java's StandardEncryptionManager,
// iceberg-rust and pyiceberg read and write. It holds the raw DEK, so it is a
// secret and must be protected by whatever stores it.
type standardKeyMetadata struct {
	encryptionKey []byte

	// aadPrefix is combined with each block's little-endian index to form
	// the AES GCM Stream additional authenticated data, binding every
	// ciphertext block to this file and to its position so that blocks
	// cannot be silently reordered, replayed from another file, or spliced
	// in from elsewhere in the same file. It is not secret. It is nil when
	// the record omits it.
	aadPrefix []byte

	// fileLength is the length of the encrypted stream, header included.
	// Per the AES GCM Stream spec's "File length" note, a reader must take
	// the length from a trusted source rather than the underlying storage's
	// reported size, since storage size alone cannot distinguish a genuinely
	// short file from one truncated by an attacker who does not also control
	// this metadata. hasFileLength is false when the record omits it.
	fileLength    int64
	hasFileLength bool
}

// encodeStandardKeyMetadata encodes the version byte and Avro record for a
// file, with both optional fields present.
func encodeStandardKeyMetadata(encryptionKey, aadPrefix []byte, fileLength int64) EncryptionKeyMetadata {
	b := []byte{standardKeyMetadataVersion}
	b = binary.AppendVarint(b, int64(len(encryptionKey)))
	b = append(b, encryptionKey...)
	b = binary.AppendVarint(b, 1) // union branch 1 (bytes): aad_prefix is present
	b = binary.AppendVarint(b, int64(len(aadPrefix)))
	b = append(b, aadPrefix...)
	b = binary.AppendVarint(b, 1) // union branch 1 (long): file_length is present
	b = binary.AppendVarint(b, fileLength)

	return b
}

// decodeStandardKeyMetadata decodes the version byte and Avro record written
// by [encodeStandardKeyMetadata] or by Java's StandardKeyMetadata.buffer().
func decodeStandardKeyMetadata(b []byte) (standardKeyMetadata, error) {
	if b[0] != standardKeyMetadataVersion {
		return standardKeyMetadata{}, fmt.Errorf("%w: %d", ErrUnsupportedKeyMetadataVersion, b[0])
	}

	r := avroReader{buf: b[1:]}
	var k standardKeyMetadata
	k.encryptionKey = r.bytes()
	if r.present() {
		k.aadPrefix = r.bytes()
	}
	if r.present() {
		k.fileLength = r.long()
		k.hasFileLength = true
	}
	if r.err == nil && len(r.buf) != 0 {
		r.err = fmt.Errorf("%d unexpected trailing bytes", len(r.buf))
	}
	if r.err != nil {
		return standardKeyMetadata{}, fmt.Errorf("%w: %w", ErrInvalidKeyMetadata, r.err)
	}

	return k, nil
}

// avroReader decodes the few Avro binary primitives StandardKeyMetadata uses,
// remembering the first error. Avro longs are zig-zag varints, which is what
// encoding/binary's Varint implements.
type avroReader struct {
	buf []byte
	err error
}

func (r *avroReader) long() int64 {
	if r.err != nil {
		return 0
	}
	v, n := binary.Varint(r.buf)
	if n <= 0 {
		r.err = errors.New("truncated or oversized varint")

		return 0
	}
	r.buf = r.buf[n:]

	return v
}

func (r *avroReader) bytes() []byte {
	n := r.long()
	if r.err != nil {
		return nil
	}
	if n < 0 || n > int64(len(r.buf)) {
		r.err = fmt.Errorf("bytes length %d out of range (%d bytes remain)", n, len(r.buf))

		return nil
	}
	out := slices.Clone(r.buf[:n])
	r.buf = r.buf[n:]

	return out
}

// present reads the branch index of a ["null", T] union and reports whether
// the value is present (branch 1) rather than null (branch 0).
func (r *avroReader) present() bool {
	branch := r.long()
	if r.err != nil {
		return false
	}
	if branch != 0 && branch != 1 {
		r.err = fmt.Errorf("unexpected union branch %d", branch)

		return false
	}

	return branch == 1
}

// plaintextLengthFromStreamLength derives the plaintext length from the
// length of the encrypted stream, the way Java's
// AesGcmInputStream.calculatePlaintextLength does. It accepts the 36-byte
// form of an empty file (header plus one empty block) and rejects lengths no
// writer could have produced.
func plaintextLengthFromStreamLength(streamLength int64) (int64, error) {
	if streamLength < gcmStreamMinLength {
		return 0, fmt.Errorf("%w: file-length %d is shorter than the %d-byte minimum stream", ErrInvalidKeyMetadata, streamLength, gcmStreamMinLength)
	}

	body := streamLength - gcmStreamHeaderLength
	plainLength := body / gcmStreamCipherBlockSize * gcmStreamPlainBlockSize
	if lastBlock := body % gcmStreamCipherBlockSize; lastBlock != 0 {
		if lastBlock < gcmStreamBlockOverhead {
			return 0, fmt.Errorf("%w: file-length %d ends in a %d-byte block, shorter than its %d-byte nonce and tag", ErrInvalidKeyMetadata, streamLength, lastBlock, gcmStreamBlockOverhead)
		}
		plainLength += lastBlock - gcmStreamBlockOverhead
	}

	return plainLength, nil
}

// StandardEncryptionManager is a generic, format-agnostic [EncryptionManager]
// for arbitrary files (e.g. manifests, manifest lists, Puffin statistics). It
// reads and writes the same bytes as Java's StandardEncryptionManager,
// iceberg-rust and pyiceberg: an Iceberg AES GCM Stream ("AGS1") with fixed
// 1 MiB plaintext blocks, and key metadata in Iceberg's StandardKeyMetadata
// encoding.
//
// Each file is encrypted under a fresh random data encryption key (DEK),
// generated locally with no KMS call. Each block carries its own random nonce
// and is authenticated with an AAD that binds it to the file and to its
// position. This bounds memory usage and supports random access
// (Seek/ReadAt) on the decrypted file without buffering or decrypting more
// than the requested blocks.
//
// The key metadata returned by Close contains the raw DEK, so it is a secret:
// persist it only where something protects it, such as a manifest or manifest
// list that is itself encrypted under a key encryption key. Wrapping the DEK
// that way is not done by this type.
//
// StandardEncryptionManager always encrypts and always decrypts: it fails
// closed, returning [ErrKeyIDRequired] or [ErrKeyMetadataRequired] rather
// than silently falling back to plaintext. Use [PlaintextEncryptionManager]
// for tables or files that are not encrypted.
type StandardEncryptionManager struct {
	dataKeyLength int
}

var _ EncryptionManager = (*StandardEncryptionManager)(nil)

// StandardManagerOption configures a [StandardEncryptionManager] created by
// [NewStandardEncryptionManager].
type StandardManagerOption func(*StandardEncryptionManager)

// WithStandardDataKeyLength sets the length, in bytes, of the per-file data
// encryption key. It corresponds to the encryption.data-key-length table
// property. Valid AES key lengths are 16, 24, or 32 bytes; the default is
// [StandardDefaultDataKeyLength].
func WithStandardDataKeyLength(length int) StandardManagerOption {
	return func(m *StandardEncryptionManager) { m.dataKeyLength = length }
}

// NewStandardEncryptionManager creates a [StandardEncryptionManager].
func NewStandardEncryptionManager(opts ...StandardManagerOption) *StandardEncryptionManager {
	m := &StandardEncryptionManager{dataKeyLength: StandardDefaultDataKeyLength}
	for _, opt := range opts {
		opt(m)
	}

	return m
}

// NewEncryptedOutputFile creates a new AES-GCM block-encrypted output file.
// keyID is the table's encryption key ID and must be non-empty; otherwise
// [ErrKeyIDRequired] is returned. It is not recorded in the file's key
// metadata.
func (m *StandardEncryptionManager) NewEncryptedOutputFile(_ context.Context, writer icebergio.FileWriter, keyID string) (EncryptedOutputFile, error) {
	if keyID == "" {
		return nil, ErrKeyIDRequired
	}
	switch m.dataKeyLength {
	case 16, 24, 32:
	default:
		return nil, fmt.Errorf("%w: DEK length must be 16, 24, or 32 bytes; got %d", ErrInvalidKeyLength, m.dataKeyLength)
	}

	// A fresh DEK per file keeps the number of random-nonce encryptions under
	// any one key within the 2^32 limit NIST SP 800-38D sets for random
	// 96-bit nonces: a file holds at most 2^32-1 blocks.
	dek := make([]byte, m.dataKeyLength)
	if _, err := io.ReadFull(rand.Reader, dek); err != nil {
		return nil, fmt.Errorf("encryption: failed to generate DEK: %w", err)
	}

	aead, err := newStandardAEAD(dek)
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
	binary.LittleEndian.PutUint32(header[len(gcmStreamMagic):], gcmStreamPlainBlockSize)
	if _, err := writer.Write(header); err != nil {
		return nil, fmt.Errorf("encryption: failed to write stream header: %w", err)
	}

	return &standardOutputFile{
		FileWriter:      writer,
		aead:            aead,
		dek:             dek,
		aadPrefix:       aadPrefix,
		encryptedLength: gcmStreamHeaderLength,
	}, nil
}

// NewDecryptedInputFile wraps file for transparent block-level AES-GCM
// decryption. keyMetadata must be the non-empty StandardKeyMetadata blob
// produced by [StandardEncryptionManager.NewEncryptedOutputFile] or by another
// Iceberg implementation; otherwise [ErrKeyMetadataRequired] is returned.
//
// The plaintext length is derived from the file length recorded in key
// metadata, never from the underlying file's size. Key metadata without a file
// length is rejected, because there is no other trusted source for it here.
// Key metadata without an AAD prefix, which the encoding permits, is rejected
// too: the prefix is what binds a block to its file.
func (m *StandardEncryptionManager) NewDecryptedInputFile(_ context.Context, file icebergio.File, keyMetadata EncryptionKeyMetadata) (EncryptedInputFile, error) {
	if len(keyMetadata) == 0 {
		return nil, ErrKeyMetadataRequired
	}

	meta, err := decodeStandardKeyMetadata(keyMetadata)
	if err != nil {
		return nil, err
	}
	// Java, iceberg-rust and this writer all emit 16-byte prefixes. Bounding
	// it also keeps the per-block AAD allocation in readBlock small.
	if len(meta.aadPrefix) != gcmStreamAADPrefixLength {
		return nil, fmt.Errorf("%w: aad-prefix must be exactly %d bytes, got %d", ErrInvalidKeyMetadata, gcmStreamAADPrefixLength, len(meta.aadPrefix))
	}
	if !meta.hasFileLength {
		return nil, fmt.Errorf("%w: file-length is required; the plaintext length is not taken from the underlying file's size", ErrInvalidKeyMetadata)
	}
	plaintextLength, err := plaintextLengthFromStreamLength(meta.fileLength)
	if err != nil {
		return nil, err
	}

	aead, err := newStandardAEAD(meta.encryptionKey)
	if err != nil {
		return nil, err
	}

	if err := validateGCMStreamHeader(file); err != nil {
		return nil, err
	}

	return &standardInputFile{
		underlying:      file,
		aead:            aead,
		aadPrefix:       meta.aadPrefix,
		plaintextLength: plaintextLength,
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

// validateGCMStreamHeader reads the AES GCM Stream header at the start of
// file and checks that it is the "AGS1" magic followed by the fixed 1 MiB
// plain block length. The header is unauthenticated storage data, so it is
// only validated here: the block size comes from [gcmStreamPlainBlockSize],
// never from the header.
func validateGCMStreamHeader(file icebergio.File) error {
	header := make([]byte, gcmStreamHeaderLength)
	n, err := file.ReadAt(header, 0)
	if err != nil && !errors.Is(err, io.EOF) {
		return fmt.Errorf("encryption: failed to read stream header: %w", err)
	}
	if n != gcmStreamHeaderLength {
		return fmt.Errorf("%w: expected %d header bytes, got %d", ErrInvalidStreamHeader, gcmStreamHeaderLength, n)
	}
	if string(header[:len(gcmStreamMagic)]) != gcmStreamMagic {
		return fmt.Errorf("%w: missing %q magic", ErrInvalidStreamHeader, gcmStreamMagic)
	}

	if blockSize := binary.LittleEndian.Uint32(header[len(gcmStreamMagic):]); blockSize != gcmStreamPlainBlockSize {
		return fmt.Errorf("%w: block length %d, want %d", ErrInvalidStreamHeader, blockSize, gcmStreamPlainBlockSize)
	}

	return nil
}

// gcmStreamBlockAAD derives the AES-GCM additional authenticated data for
// blockIndex: the per-file AAD prefix followed by the 4-byte little-endian
// block index, per the Iceberg AES GCM Stream spec. GCM authenticates the AAD
// along with the ciphertext, so a block only verifies at its own index in its
// own file: reordered, duplicated, or spliced-in blocks fail authentication.
func gcmStreamBlockAAD(prefix []byte, blockIndex uint32) []byte {
	aad := make([]byte, len(prefix)+4)
	copy(aad, prefix)
	binary.LittleEndian.PutUint32(aad[len(prefix):], blockIndex)

	return aad
}

// standardOutputFile is an [EncryptedOutputFile] that seals fixed-size
// plaintext blocks with AES-GCM as they are written, using the Iceberg AES
// GCM Stream ("AGS1") wire format.
type standardOutputFile struct {
	icebergio.FileWriter

	aead      cipher.AEAD
	dek       []byte
	aadPrefix []byte

	buf        []byte
	blockIndex uint32
	closed     bool
	err        error

	// encryptedLength is the number of bytes written to the underlying
	// writer so far, header included. It becomes file_length in the key
	// metadata, and only advances once a block has been written.
	encryptedLength int64

	// underlyingClosed records that FileWriter.Close has been called, so the
	// underlying writer is closed at most once.
	underlyingClosed bool

	keyMetadata EncryptionKeyMetadata
}

var _ EncryptedOutputFile = (*standardOutputFile)(nil)

// closeUnderlyingIgnoringError closes the underlying writer at most once.
// The error is ignored: the caller is already reporting a more specific
// failure (a flush error), and this is best-effort cleanup so a poisoned
// writer never leaks its underlying file descriptor or connection.
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
		space := gcmStreamPlainBlockSize - len(f.buf)
		n := min(space, len(p))
		f.buf = append(f.buf, p[:n]...)
		p = p[n:]
		consumed += n
		if len(f.buf) == gcmStreamPlainBlockSize {
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
// fresh random nonce, per the Iceberg AES GCM Stream format.
// f.encryptedLength only advances once the ciphertext has reached the
// underlying writer, so file_length never counts a block that failed to write.
func (f *standardOutputFile) flushBlock() error {
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
	f.encryptedLength += int64(len(sealed))
	f.blockIndex++
	f.buf = f.buf[:0]

	return nil
}

// ReadFrom copies from r, encrypting as data is written, satisfying
// io.ReaderFrom (required by [icebergio.FileWriter]).
func (f *standardOutputFile) ReadFrom(r io.Reader) (int64, error) {
	buf := make([]byte, gcmStreamPlainBlockSize)
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

// Close flushes any buffered partial block, writes an empty block 0 if the
// file is empty, and finalizes the key metadata. closed is set, and
// keyMetadata published, only once everything, including the underlying
// Close, has succeeded. A failed Close poisons the writer (via f.err): a retry
// returns the same error, and no key metadata is exposed for an output that
// never finished.
func (f *standardOutputFile) Close() error {
	if f.err != nil {
		return f.err
	}
	if f.closed {
		return nil
	}

	// Java and iceberg-rust write block 0 even when the file is empty, so an
	// empty file is 36 bytes (a header plus one 28-byte block), and
	// iceberg-rust rejects anything shorter. Match them.
	if len(f.buf) > 0 || f.blockIndex == 0 {
		if err := f.flushBlock(); err != nil {
			f.err = err
			f.closeUnderlyingIgnoringError()

			return err
		}
	}

	if err := f.FileWriter.Close(); err != nil {
		f.underlyingClosed = true
		f.err = fmt.Errorf("encryption: failed to close underlying writer: %w", err)

		return f.err
	}
	f.underlyingClosed = true

	f.keyMetadata = encodeStandardKeyMetadata(f.dek, f.aadPrefix, f.encryptedLength)
	f.closed = true

	return nil
}

// KeyMetadata returns the finalized per-file key metadata. It is only
// populated after Close has succeeded. It contains the raw DEK.
func (f *standardOutputFile) KeyMetadata() EncryptionKeyMetadata { return f.keyMetadata }

// standardInputFile is an [EncryptedInputFile] that decrypts fixed-size
// AES-GCM blocks on demand, supporting random access via ReadAt/Seek.
//
// The plaintext length comes from the trusted file length in key metadata, so
// ciphertext appended after the last block is ignored, and truncating the
// ciphertext while also editing that metadata cannot be detected. Both follow
// from the AES GCM Stream spec's "File length" rule.
//
// ReadAt is safe, and intended, for concurrent use, matching the io.ReaderAt
// contract: readBlock only holds cacheMu around the cache check/publish, not
// across the underlying ReadAt or AEAD decryption, so concurrent calls for
// distinct blocks run in parallel. A lost cache race (two goroutines
// decrypting the same block) is harmless, just wasted work. Read and Seek
// mutate the shared cursor (pos) and are not concurrent-safe; do not call them
// from multiple goroutines on the same instance.
//
// The single-entry cache retains one decrypted block (1 MiB) for the lifetime
// of the input file, not just the lifetime of a single read call.
type standardInputFile struct {
	underlying      icebergio.File
	aead            cipher.AEAD
	aadPrefix       []byte
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

var _ EncryptedInputFile = (*standardInputFile)(nil)

func (f *standardInputFile) numBlocks() int64 {
	if f.plaintextLength == 0 {
		return 0
	}

	// plaintextLength is derived from a file length in key metadata, so it
	// can be close to math.MaxInt64; 1 + (n-1)/size avoids the overflow that
	// (n + size - 1)/size would hit there.
	return 1 + (f.plaintextLength-1)/gcmStreamPlainBlockSize
}

// blockPlainLen returns the plaintext length of block idx, given the total
// number of blocks.
func (f *standardInputFile) blockPlainLen(idx, numBlocks int64) int64 {
	if idx == numBlocks-1 {
		return f.plaintextLength - idx*gcmStreamPlainBlockSize
	}

	return gcmStreamPlainBlockSize
}

// readBlock decrypts block idx, or returns it from the single-entry cache
// if it was the most recently decrypted block. It checks the byte count
// returned by ReadAt: a short read is reported as [ErrBlockTruncated]
// (truncated storage), since zero-padding it into the AEAD would surface as a
// misleading [ErrAuthenticationFailed].
//
// cacheMu is only held around the cache check and the cache publish at the
// end, never across the underlying ReadAt or aead.Open below: holding it
// across I/O would serialize all concurrent readers, including remote
// backends. A lost race between two goroutines decrypting the same block is
// harmless (redundant work, not a correctness issue).
func (f *standardInputFile) readBlock(idx int64) ([]byte, error) {
	f.cacheMu.Lock()
	if f.cacheValid && f.cacheIdx == idx {
		block := f.cacheBlock
		f.cacheMu.Unlock()

		return block, nil
	}
	f.cacheMu.Unlock()

	numBlocks := f.numBlocks()
	if idx < 0 || idx >= numBlocks {
		return nil, fmt.Errorf("%w: block index %d out of range [0, %d)", ErrInvalidKeyMetadata, idx, numBlocks)
	}
	// The AAD carries the block index as 32 bits, and a file-length near
	// math.MaxInt64 would otherwise imply more blocks than that.
	if idx > math.MaxUint32 {
		return nil, fmt.Errorf("%w: block index %d exceeds the maximum supported block count", ErrInvalidKeyMetadata, idx)
	}

	// idx < numBlocks and plaintextLength was derived from an int64 stream
	// length, so this offset is no larger than that length and cannot
	// overflow.
	offset := gcmStreamHeaderLength + idx*gcmStreamCipherBlockSize

	wantLen := f.blockPlainLen(idx, numBlocks) + gcmStreamBlockOverhead
	ciphertext := make([]byte, wantLen)
	n, err := f.underlying.ReadAt(ciphertext, offset)
	if err != nil && !errors.Is(err, io.EOF) {
		return nil, fmt.Errorf("encryption: failed to read block %d: %w", idx, err)
	}
	if int64(n) != wantLen {
		return nil, fmt.Errorf("%w: block %d: read %d of %d expected ciphertext bytes", ErrBlockTruncated, idx, n, wantLen)
	}

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
		idx := curOff / gcmStreamPlainBlockSize
		block, err := f.readBlock(idx)
		if err != nil {
			return read, err
		}
		inBlockOff := curOff - idx*gcmStreamPlainBlockSize
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
