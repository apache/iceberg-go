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
	"bufio"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"math"

	"github.com/pierrec/lz4/v4"
)

const (
	lz4FrameMagic                 uint32 = 0x184d2204
	lz4FrameHeaderPrefixSize             = 6 // magic (4) + FLG (1) + BD (1)
	lz4FrameHeaderSize                   = 7 // prefix + header checksum
	lz4FrameHeaderSizeWithContent        = 15
	lz4FrameContentSizeFlag       byte   = 1 << 3
	lz4FrameContentChecksumFlag   byte   = 1 << 2
	lz4FrameBlockChecksumFlag     byte   = 1 << 4
	lz4FrameVersionMask           byte   = 0xc0
	lz4FrameVersionOne            byte   = 0x40
	lz4FrameReservedFLGFlag       byte   = 1 << 1
	lz4FrameDictionaryIDFlag      byte   = 1
	lz4FrameReservedBDFlags       byte   = 0x8f
	lz4FrameBlockSizeMask         uint32 = 0x7fffffff
	lz4FrameReadBufferSize               = 4 << 20 // LZ4 maximum block size.
)

type countingReader struct {
	reader io.Reader
	count  int64
	err    error
}

func (r *countingReader) Read(p []byte) (int, error) {
	n, err := r.reader.Read(p)
	r.count += int64(n)
	if err != nil && !errors.Is(err, io.EOF) {
		r.err = err
	}

	return n, err
}

type lz4FrameHeader struct {
	flags          byte
	headerSize     int
	contentSize    uint64
	blockMaxSize   int64
	hasContentSize bool
}

func lz4BlockMaxSize(bd byte) (int64, error) {
	switch (bd >> 4) & 0x7 {
	case 4:
		return 64 << 10, nil
	case 5:
		return 256 << 10, nil
	case 6:
		return 1 << 20, nil
	case 7:
		return 4 << 20, nil
	default:
		return 0, errors.New("invalid LZ4 frame block maximum size")
	}
}

// readLZ4FrameHeader reads and validates the LZ4 frame descriptor without
// consuming the payload. The content size is required by the Puffin format
// for compressed footer payloads.
func readLZ4FrameHeader(r io.ReaderAt) (lz4FrameHeader, error) {
	var header [lz4FrameHeaderSizeWithContent]byte
	if _, err := r.ReadAt(header[:lz4FrameHeaderSize], 0); err != nil {
		return lz4FrameHeader{}, err
	}

	if binary.LittleEndian.Uint32(header[:4]) != lz4FrameMagic {
		return lz4FrameHeader{}, errors.New("invalid LZ4 frame magic")
	}

	hasContentSize := header[4]&lz4FrameContentSizeFlag != 0
	if header[4]&lz4FrameVersionMask != lz4FrameVersionOne {
		return lz4FrameHeader{}, errors.New("invalid LZ4 frame version")
	}
	if header[4]&lz4FrameReservedFLGFlag != 0 {
		return lz4FrameHeader{}, errors.New("invalid LZ4 frame reserved flag")
	}
	if header[4]&lz4FrameDictionaryIDFlag != 0 {
		return lz4FrameHeader{}, errors.New("LZ4 frame dictionary ID is not supported")
	}
	if header[5]&lz4FrameReservedBDFlags != 0 {
		return lz4FrameHeader{}, errors.New("invalid LZ4 frame reserved block descriptor flags")
	}
	blockMaxSize, err := lz4BlockMaxSize(header[5])
	if err != nil {
		return lz4FrameHeader{}, err
	}
	if hasContentSize {
		if _, err := r.ReadAt(header[lz4FrameHeaderSize:], lz4FrameHeaderSize); err != nil {
			return lz4FrameHeader{}, err
		}
	}

	headerSize := lz4FrameHeaderSize
	if hasContentSize {
		headerSize = lz4FrameHeaderSizeWithContent
	}
	valid, err := lz4.ValidFrameHeader(header[:headerSize])
	if err != nil {
		return lz4FrameHeader{}, err
	}
	if !valid {
		return lz4FrameHeader{}, errors.New("invalid frame header")
	}

	frameHeader := lz4FrameHeader{
		flags:          header[4],
		headerSize:     headerSize,
		blockMaxSize:   blockMaxSize,
		hasContentSize: hasContentSize,
	}
	if hasContentSize {
		frameHeader.contentSize = binary.LittleEndian.Uint64(header[lz4FrameHeaderPrefixSize : lz4FrameHeaderPrefixSize+8])
	}

	return frameHeader, nil
}

func validateLZ4FrameEnvelope(
	r io.ReaderAt,
	payloadSize int64,
	flags byte,
	headerSize int,
	blockMaxSize int64,
) error {
	if payloadSize < int64(headerSize) {
		return errors.New("LZ4 frame is shorter than its header")
	}
	if payloadSize-int64(headerSize) < 4 {
		return errors.New("LZ4 frame is missing its end mark")
	}

	firstBlock := make([]byte, headerSize+4)
	if _, err := r.ReadAt(firstBlock, 0); err != nil {
		return fmt.Errorf("read LZ4 frame header and block header: %w", err)
	}

	blockSize := binary.LittleEndian.Uint32(firstBlock[headerSize:])
	offset := int64(headerSize + 4)
	reader := bufio.NewReaderSize(
		io.NewSectionReader(r, offset, payloadSize-offset),
		int(min(payloadSize-offset, lz4FrameReadBufferSize)),
	)
	var blockHeader [4]byte

	for blockSize != 0 {
		dataSize := int64(blockSize & lz4FrameBlockSizeMask)
		if dataSize > blockMaxSize {
			return errors.New("LZ4 frame block exceeds declared maximum")
		}
		if dataSize > payloadSize-offset {
			return errors.New("LZ4 frame block exceeds its payload")
		}
		if _, err := io.CopyN(io.Discard, reader, dataSize); err != nil {
			return fmt.Errorf("read LZ4 frame block: %w", err)
		}
		offset += dataSize

		if flags&lz4FrameBlockChecksumFlag != 0 {
			if payloadSize-offset < 4 {
				return errors.New("LZ4 frame block checksum is truncated")
			}
			if _, err := io.CopyN(io.Discard, reader, 4); err != nil {
				return fmt.Errorf("read LZ4 frame block checksum: %w", err)
			}
			offset += 4
		}

		if payloadSize-offset < int64(len(blockHeader)) {
			return errors.New("LZ4 frame is missing its end mark")
		}
		if _, err := io.ReadFull(reader, blockHeader[:]); err != nil {
			return fmt.Errorf("read LZ4 frame block header: %w", err)
		}
		offset += int64(len(blockHeader))

		blockSize = binary.LittleEndian.Uint32(blockHeader[:])
	}

	if flags&lz4FrameContentChecksumFlag != 0 {
		if payloadSize-offset < 4 {
			return errors.New("LZ4 frame content checksum is truncated")
		}
		if _, err := io.CopyN(io.Discard, reader, 4); err != nil {
			return fmt.Errorf("read LZ4 frame content checksum: %w", err)
		}
		offset += 4
	}

	if offset != payloadSize {
		return errors.New("LZ4 frame has trailing data")
	}

	return nil
}

// newCompressedFooterReader validates a compressed footer's LZ4 envelope
// before building a bounded decompression stream.
func newCompressedFooterReader(payloadReader *io.SectionReader, payloadSize, maxFooterSize int64) (*countingReader, *io.LimitedReader, uint64, error) {
	// Java, PyIceberg, and Rust currently reject compressed Puffin footers;
	// keep this flag reader-only until those implementations catch up.
	frameHeader, err := readLZ4FrameHeader(payloadReader)
	if err != nil {
		return nil, nil, 0, fmt.Errorf("puffin: inspect compressed footer LZ4 frame: %w", err)
	}
	if !frameHeader.hasContentSize {
		return nil, nil, 0, errors.New("puffin: compressed footer LZ4 frame is missing content size")
	}
	if frameHeader.contentSize > uint64(maxFooterSize) {
		return nil, nil, 0, fmt.Errorf("puffin: footer exceeds maximum size %d", maxFooterSize)
	}
	if err := validateLZ4FrameEnvelope(
		payloadReader,
		payloadSize,
		frameHeader.flags,
		frameHeader.headerSize,
		frameHeader.blockMaxSize,
	); err != nil {
		return nil, nil, 0, fmt.Errorf("puffin: inspect compressed footer LZ4 frame: %w", err)
	}
	compressedPayloadReader := bufio.NewReaderSize(
		io.NewSectionReader(payloadReader, 0, payloadSize),
		int(min(payloadSize, lz4FrameReadBufferSize)),
	)
	compressed := &countingReader{reader: lz4.NewReader(compressedPayloadReader)}
	limit := maxFooterSize
	if limit < math.MaxInt64 {
		limit++
	}
	limited := &io.LimitedReader{R: compressed, N: limit}

	return compressed, limited, frameHeader.contentSize, nil
}
