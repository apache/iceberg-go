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
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
	"sort"
)

// PuffinReader provides random access to blobs and metadata in a Puffin file.
//
// The reader validates magic bytes on construction and caches the footer
// after the first read. Use ReadBlob or ReadAllBlobs to access blob data.
//
// Usage:
//
//	r, err := puffin.NewPuffinReader(file, fileSize)
//	if err != nil {
//	    return err
//	}
//	footer, err := r.ReadFooter()
//	if err != nil {
//	    return err
//	}
//	for i := range footer.Blobs {
//	    blob, err := r.ReadBlob(i)
//	    // process blob.Data
//	}
type PuffinReader struct {
	r           io.ReaderAt
	size        int64
	footer      *Footer
	footerStart int64 // cached after ReadFooter
	maxBlobSize int64
}

// BlobData pairs a blob's metadata with its content.
type BlobData struct {
	Metadata BlobMetadata
	Data     []byte
}

// ReaderOption configures a PuffinReader.
type ReaderOption func(*PuffinReader)

// WithMaxBlobSize sets the maximum blob size allowed when reading.
// This prevents OOM attacks from malicious files with huge blob lengths.
// Default is DefaultMaxBlobSize (256 MB).
func WithMaxBlobSize(size int64) ReaderOption {
	return func(r *PuffinReader) {
		r.maxBlobSize = size
	}
}

// NewPuffinReader creates a new Puffin reader.
// It validates both the header and trailing magic bytes upfront.
// The caller is responsible for closing the underlying io.ReaderAt.
func NewPuffinReader(r io.ReaderAt, size int64, opts ...ReaderOption) (*PuffinReader, error) {
	if r == nil {
		return nil, fmt.Errorf("puffin: reader is nil")
	}

	// Minimum size: header magic + footer magic + footer trailer
	minSize := int64(MagicSize + MagicSize + footerTrailerSize)
	if size < minSize {
		return nil, fmt.Errorf("puffin: file too small (%d bytes, minimum %d)", size, minSize)
	}

	// Validate header magic
	var headerMagic [MagicSize]byte
	if _, err := r.ReadAt(headerMagic[:], 0); err != nil {
		return nil, fmt.Errorf("puffin: read header magic: %w", err)
	}
	if headerMagic != magic {
		return nil, fmt.Errorf("puffin: invalid header magic")
	}

	// Validate trailing magic (fail fast on corrupt/truncated files)
	var trailingMagic [MagicSize]byte
	if _, err := r.ReadAt(trailingMagic[:], size-MagicSize); err != nil {
		return nil, fmt.Errorf("puffin: read trailing magic: %w", err)
	}
	if trailingMagic != magic {
		return nil, fmt.Errorf("puffin: invalid trailing magic")
	}

	pr := &PuffinReader{
		r:           r,
		size:        size,
		maxBlobSize: DefaultMaxBlobSize,
	}

	for _, opt := range opts {
		opt(pr)
	}

	return pr, nil
}

// ReadFooter reads and parses the footer from the Puffin file.
// The footer is cached after the first successful read.
func (r *PuffinReader) ReadFooter() (*Footer, error) {
	if r.footer != nil {
		return r.footer, nil
	}

	// Read trailer (last 12 bytes): PayloadSize(4) + Flags(4) + Magic(4)
	var trailer [footerTrailerSize]byte
	if _, err := r.r.ReadAt(trailer[:], r.size-footerTrailerSize); err != nil {
		return nil, fmt.Errorf("puffin: read footer trailer: %w", err)
	}

	// Validate trailing magic (already checked in constructor, but be defensive)
	if trailer[8] != magic[0] || trailer[9] != magic[1] ||
		trailer[10] != magic[2] || trailer[11] != magic[3] {
		return nil, fmt.Errorf("puffin: invalid trailing magic in footer")
	}

	// Extract payload size and flags
	payloadSize := int64(binary.LittleEndian.Uint32(trailer[0:4]))
	flags := binary.LittleEndian.Uint32(trailer[4:8])

	// Check for compressed footer (unsupported)
	if flags&FooterFlagCompressed != 0 {
		return nil, fmt.Errorf("puffin: compressed footer not supported")
	}

	// Check for unknown flags (future-proofing)
	if flags&^uint32(FooterFlagCompressed) != 0 {
		return nil, fmt.Errorf("puffin: unknown footer flags set: 0x%x", flags)
	}

	// Validate payload size
	if payloadSize < 0 {
		return nil, fmt.Errorf("puffin: invalid footer payload size %d", payloadSize)
	}

	// Calculate footer start position
	// Layout: [header magic (4)] [blobs...] [footer magic (4)] [JSON (payloadSize)] [trailer (12)]
	footerStart := r.size - footerTrailerSize - payloadSize - MagicSize
	if footerStart < MagicSize {
		return nil, fmt.Errorf("puffin: footer payload size %d exceeds available space", payloadSize)
	}

	// Validate footer start magic
	var footerMagic [MagicSize]byte
	if _, err := r.r.ReadAt(footerMagic[:], footerStart); err != nil {
		return nil, fmt.Errorf("puffin: read footer start magic: %w", err)
	}
	if footerMagic != magic {
		return nil, fmt.Errorf("puffin: invalid footer start magic")
	}

	// Read footer JSON payload
	payload := make([]byte, payloadSize)
	if _, err := r.r.ReadAt(payload, footerStart+MagicSize); err != nil {
		return nil, fmt.Errorf("puffin: read footer payload: %w", err)
	}

	// Parse footer JSON
	var footer Footer
	if err := json.Unmarshal(payload, &footer); err != nil {
		return nil, fmt.Errorf("puffin: decode footer JSON: %w", err)
	}

	// Validate blob metadata
	if err := r.validateBlobs(footer.Blobs, footerStart); err != nil {
		return nil, err
	}

	// Cache footer and footerStart
	r.footer = &footer
	r.footerStart = footerStart

	return r.footer, nil
}

// ReadBlob reads the content of a specific blob by index.
// The footer is read automatically if not already cached.
func (r *PuffinReader) ReadBlob(index int) (*BlobData, error) {
	footer, err := r.ReadFooter()
	if err != nil {
		return nil, err
	}

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
// The footer must be read first to establish bounds checking.
func (r *PuffinReader) ReadBlobByMetadata(meta BlobMetadata) ([]byte, error) {
	if r.footer == nil {
		return nil, fmt.Errorf("puffin: cannot read blob: footer not read")
	}
	return r.readBlobData(meta)
}

// readBlobData is the internal implementation for reading blob data.
func (r *PuffinReader) readBlobData(meta BlobMetadata) ([]byte, error) {
	// Validate blob type
	if meta.Type == "" {
		return nil, fmt.Errorf("puffin: cannot read blob: type is required")
	}

	// Check for compressed blob (unsupported)
	if meta.CompressionCodec != nil && *meta.CompressionCodec != "" {
		return nil, fmt.Errorf("puffin: cannot read blob: compression %q not supported", *meta.CompressionCodec)
	}

	// Validate length
	if meta.Length < 0 {
		return nil, fmt.Errorf("puffin: invalid blob length %d", meta.Length)
	}
	if meta.Length > r.maxBlobSize {
		return nil, fmt.Errorf("puffin: blob size %d exceeds limit %d", meta.Length, r.maxBlobSize)
	}

	// Validate offset (must be after header magic)
	if meta.Offset < MagicSize {
		return nil, fmt.Errorf("puffin: invalid blob offset %d (before header)", meta.Offset)
	}

	// Check for overflow
	end := meta.Offset + meta.Length
	if end < meta.Offset {
		return nil, fmt.Errorf("puffin: blob offset+length overflow: offset=%d length=%d", meta.Offset, meta.Length)
	}

	// Validate blob doesn't extend into footer
	if end > r.footerStart {
		return nil, fmt.Errorf("puffin: blob extends into footer: offset=%d length=%d footerStart=%d",
			meta.Offset, meta.Length, r.footerStart)
	}

	// Read blob data
	data := make([]byte, meta.Length)
	if _, err := r.r.ReadAt(data, meta.Offset); err != nil {
		return nil, fmt.Errorf("puffin: read blob data: %w", err)
	}

	return data, nil
}

// ReadAllBlobs reads all blobs from the file.
// Blobs are read in offset order for sequential I/O efficiency,
// but results are returned in the original footer order.
func (r *PuffinReader) ReadAllBlobs() ([]*BlobData, error) {
	footer, err := r.ReadFooter()
	if err != nil {
		return nil, err
	}

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

// ReadRange reads a raw byte range from the blob data region.
// The footer must be read first to establish bounds checking.
func (r *PuffinReader) ReadRange(offset, length int64) ([]byte, error) {
	if r.footer == nil {
		return nil, fmt.Errorf("puffin: cannot read range: footer not read")
	}

	// Validate length
	if length < 0 {
		return nil, fmt.Errorf("puffin: invalid range length %d", length)
	}
	if length > r.maxBlobSize {
		return nil, fmt.Errorf("puffin: range size %d exceeds limit %d", length, r.maxBlobSize)
	}

	// Validate offset
	if offset < MagicSize {
		return nil, fmt.Errorf("puffin: invalid range offset %d (before header)", offset)
	}

	// Check for overflow
	end := offset + length
	if end < offset {
		return nil, fmt.Errorf("puffin: range offset+length overflow: offset=%d length=%d", offset, length)
	}

	// Validate range doesn't extend into footer
	if end > r.footerStart {
		return nil, fmt.Errorf("puffin: range extends into footer: offset=%d length=%d footerStart=%d",
			offset, length, r.footerStart)
	}

	// Read data
	buf := make([]byte, length)
	if _, err := r.r.ReadAt(buf, offset); err != nil {
		return nil, fmt.Errorf("puffin: read range: %w", err)
	}

	return buf, nil
}

// validateBlobs validates all blob metadata entries.
func (r *PuffinReader) validateBlobs(blobs []BlobMetadata, footerStart int64) error {
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
