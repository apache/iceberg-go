// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package puffin

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"sort"
)

// ReaderAtSeeker combines io.ReaderAt and io.Seeker for reading Puffin files.
// This interface is implemented by *os.File, *bytes.Reader, and similar types.
type ReaderAtSeeker interface {
	io.ReaderAt
	io.Seeker
}

// Reader reads blobs and metadata from a Puffin file.
//
// Usage:
//
//	r, err := puffin.NewReader(file)
//	if err != nil {
//	    return err
//	}
//	for i := range r.Blobs() {
//	    blob, err := r.ReadBlob(i)
//	    // process blob.Data
//	}
type Reader struct {
	r           ReaderAtSeeker
	size        int64
	footer      Footer
	footerStart int64 // cached after ReadFooter
	maxBlobSize int64
}

// BlobData pairs a blob's metadata with its content.
type BlobData struct {
	Metadata BlobMetadata
	Data     []byte
}

// ReaderOption configures a Reader.
type ReaderOption func(*Reader)

// WithMaxBlobSize sets the maximum blob size allowed when reading.
// This prevents OOM attacks from malicious files with huge blob lengths.
// Default is DefaultMaxBlobSize (256 MB).
func WithMaxBlobSize(size int64) ReaderOption {
	return func(r *Reader) {
		r.maxBlobSize = size
	}
}

// NewReader creates a new Puffin file reader.
// The file size is auto-detected using Seek.
// It validates magic bytes and reads the footer eagerly.
// The caller is responsible for closing the underlying reader.
func NewReader(r ReaderAtSeeker, opts ...ReaderOption) (*Reader, error) {
	if r == nil {
		return nil, errors.New("puffin: reader is nil")
	}

	// Auto-detect file size
	size, err := r.Seek(0, io.SeekEnd)
	if err != nil {
		return nil, fmt.Errorf("puffin: detect file size: %w", err)
	}

	// Minimum size: header magic + footer magic + footer trailer
	// [Magic] + zero for blob + [Magic] + [FooterPayloadSize (assuming ~0)] + [Flags] + [Magic]
	minSize := int64(MagicSize + MagicSize + footerTrailerSize)
	if size < minSize {
		return nil, fmt.Errorf("puffin: file too small (%d bytes, minimum %d)", size, minSize)
	}

	// Validate header magic
	var headerMagic [MagicSize]byte
	if _, err := r.ReadAt(headerMagic[:], 0); err != nil {
		return nil, fmt.Errorf("puffin: read header magic: %w", err)
	}
	if !bytes.Equal(headerMagic[:], magic[:]) {
		return nil, errors.New("puffin: invalid header magic")
	}

	pr := &Reader{
		r:           r,
		size:        size,
		maxBlobSize: DefaultMaxBlobSize,
	}

	for _, opt := range opts {
		opt(pr)
	}

	// Read footer
	if err := pr.readFooter(); err != nil {
		return nil, err
	}

	return pr, nil
}

// Blobs returns the blob metadata entries from the footer.
func (r *Reader) Blobs() []BlobMetadata {
	return r.footer.Blobs
}

// Properties returns the file-level properties from the footer.
func (r *Reader) Properties() map[string]string {
	return r.footer.Properties
}

// defaultFooterReadSize is the initial read size when reading the footer.
// We read more than strictly needed to hopefully get the entire footer
// in one read, reducing round-trips on cloud object storage.
const defaultFooterReadSize = 8 * 1024 // 8 KB

// readFooter reads and parses the footer from the Puffin file.
func (r *Reader) readFooter() error {
	// Read a larger chunk from the end to minimize round-trips on cloud storage.
	// This often captures the entire footer in one read.
	readSize := min(int64(defaultFooterReadSize), r.size)
	buf := make([]byte, readSize)
	if _, err := r.r.ReadAt(buf, r.size-readSize); err != nil {
		return fmt.Errorf("puffin: read footer region: %w", err)
	}

	// Parse trailer from end of buffer: PayloadSize(4) + Flags(4) + Magic(4)
	trailer := buf[len(buf)-footerTrailerSize:]

	// Validate trailing magic
	if !bytes.Equal(trailer[8:12], magic[:]) {
		return errors.New("puffin: invalid trailing magic in footer")
	}

	// Extract payload size and flags
	payloadSize := int64(binary.LittleEndian.Uint32(trailer[0:4]))
	flags := binary.LittleEndian.Uint32(trailer[4:8])

	// Check for compressed footer (unsupported)
	if flags&FooterFlagCompressed != 0 {
		return errors.New("puffin: compressed footer not supported")
	}

	// Check for unknown flags
	if flags&^uint32(FooterFlagCompressed) != 0 {
		return fmt.Errorf("puffin: unknown footer flags set: 0x%x", flags)
	}

	// Validate payload size
	if payloadSize < 0 {
		return fmt.Errorf("puffin: invalid footer payload size %d", payloadSize)
	}

	// Calculate footer start position
	// Layout: [header magic (4)] [blobs...] [footer magic (4)] [JSON (payloadSize)] [trailer (12)]
	footerStart := r.size - footerTrailerSize - payloadSize - MagicSize
	if footerStart < MagicSize {
		return fmt.Errorf("puffin: footer payload size %d exceeds available space", payloadSize)
	}

	// Total footer size: magic(4) + payload + trailer(12)
	totalFooterSize := MagicSize + payloadSize + footerTrailerSize

	// Validate footer start magic
	if totalFooterSize <= readSize {
		// We already have the footer magic in buf
		footerOffset := len(buf) - int(totalFooterSize)
		if !bytes.Equal(buf[footerOffset:footerOffset+MagicSize], magic[:]) {
			return errors.New("puffin: invalid footer start magic")
		}
	} else {
		// Footer is larger than our initial read, need to read magic separately
		var footerMagic [MagicSize]byte
		if _, err := r.r.ReadAt(footerMagic[:], footerStart); err != nil {
			return fmt.Errorf("puffin: read footer start magic: %w", err)
		}
		if !bytes.Equal(footerMagic[:], magic[:]) {
			return errors.New("puffin: invalid footer start magic")
		}
	}

	payloadReader := io.NewSectionReader(r.r, footerStart+MagicSize, payloadSize)
	var footer Footer
	if err := json.NewDecoder(payloadReader).Decode(&footer); err != nil {
		return fmt.Errorf("puffin: decode footer JSON: %w", err)
	}

	// Validate blob metadata
	if err := r.validateBlobs(footer.Blobs, footerStart); err != nil {
		return err
	}

	r.footer = footer
	r.footerStart = footerStart

	return nil
}

// ReadBlob reads the content of a specific blob by index.
// The footer is read automatically if not already cached.
func (r *Reader) ReadBlob(index int) (*BlobData, error) {
	footer := r.footer

	if index < 0 || index >= len(footer.Blobs) {
		return nil, fmt.Errorf("puffin: blob index %d out of range [0, %d)", index, len(footer.Blobs))
	}

	meta := footer.Blobs[index]
	data, err := r.readBlobData(meta)
	if err != nil {
		return nil, err
	}

	return &BlobData{Metadata: meta, Data: data}, nil
}

// ReadBlobByMetadata reads a blob using its metadata directly.
// This is useful when you have metadata from an external source.
func (r *Reader) ReadBlobByMetadata(meta BlobMetadata) ([]byte, error) {
	return r.readBlobData(meta)
}

// readBlobData is the internal implementation for reading blob data.
func (r *Reader) readBlobData(meta BlobMetadata) ([]byte, error) {
	// Validate blob type
	if meta.Type == "" {
		return nil, errors.New("puffin: cannot read blob: type is required")
	}

	// Check for compressed blob (unsupported)
	if meta.CompressionCodec != nil && *meta.CompressionCodec != "" {
		return nil, fmt.Errorf("puffin: cannot read blob: compression %q not supported", *meta.CompressionCodec)
	}

	// Validate offset/length
	if err := r.validateRange(meta.Offset, meta.Length); err != nil {
		return nil, fmt.Errorf("puffin: blob: %w", err)
	}

	// Read blob data
	data := make([]byte, meta.Length)
	if _, err := r.r.ReadAt(data, meta.Offset); err != nil {
		return nil, fmt.Errorf("puffin: read blob data: %w", err)
	}

	return data, nil
}

// ReadAllBlobs reads all blobs from the file.
func (r *Reader) ReadAllBlobs() ([]*BlobData, error) {
	footer := r.footer

	if len(footer.Blobs) == 0 {
		return nil, nil
	}

	// Create index mapping to preserve original order
	type indexedBlob struct {
		index int
		meta  BlobMetadata
	}
	indexed := make([]indexedBlob, len(footer.Blobs))
	for i, meta := range footer.Blobs {
		indexed[i] = indexedBlob{index: i, meta: meta}
	}

	// Sort by offset for sequential I/O
	sort.Slice(indexed, func(i, j int) bool {
		return indexed[i].meta.Offset < indexed[j].meta.Offset
	})

	// Read blobs in offset order, store in original order
	results := make([]*BlobData, len(footer.Blobs))
	for _, ib := range indexed {
		data, err := r.readBlobData(ib.meta)
		if err != nil {
			return nil, fmt.Errorf("puffin: read blob %d: %w", ib.index, err)
		}
		results[ib.index] = &BlobData{Metadata: ib.meta, Data: data}
	}

	return results, nil
}

// ReadAt implements io.ReaderAt, reading from the blob data region.
// It validates that the read range is within the blob data region
// This is useful for deletion vector use case.
// offset/length pointing directly into the Puffin file in manifest.
func (r *Reader) ReadAt(p []byte, off int64) (n int, err error) {
	if err := r.validateRange(off, int64(len(p))); err != nil {
		return 0, fmt.Errorf("puffin: %w", err)
	}

	return r.r.ReadAt(p, off)
}

// validateRange validates offset and length for reading from the blob data region.
func (r *Reader) validateRange(offset, length int64) error {
	if length < 0 {
		return fmt.Errorf("invalid length %d", length)
	}
	if length > r.maxBlobSize {
		return fmt.Errorf("size %d exceeds limit %d", length, r.maxBlobSize)
	}
	if offset < MagicSize {
		return fmt.Errorf("invalid offset %d (before header)", offset)
	}

	end := offset + length
	if end < offset {
		return fmt.Errorf("offset+length overflow: offset=%d length=%d", offset, length)
	}
	if end > r.footerStart {
		return fmt.Errorf("extends into footer: offset=%d length=%d footerStart=%d",
			offset, length, r.footerStart)
	}

	return nil
}

// validateBlobs validates all blob metadata entries.
func (r *Reader) validateBlobs(blobs []BlobMetadata, footerStart int64) error {
	for i, blob := range blobs {
		// Type is required
		if blob.Type == "" {
			return fmt.Errorf("puffin: blob %d: type is required", i)
		}

		// Length must be non-negative
		if blob.Length < 0 {
			return fmt.Errorf("puffin: blob %d: invalid length %d", i, blob.Length)
		}

		// Offset must be after header magic
		if blob.Offset < MagicSize {
			return fmt.Errorf("puffin: blob %d: offset %d before header", i, blob.Offset)
		}

		// Check for overflow
		end := blob.Offset + blob.Length
		if end < blob.Offset {
			return fmt.Errorf("puffin: blob %d: offset+length overflow: offset=%d length=%d", i, blob.Offset, blob.Length)
		}

		// Blob must not extend into footer
		if end > footerStart {
			return fmt.Errorf("puffin: blob %d: extends into footer: offset=%d length=%d footerStart=%d",
				i, blob.Offset, blob.Length, footerStart)
		}
	}

	return nil
}
