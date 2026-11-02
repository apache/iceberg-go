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
	"errors"
	"fmt"
	"io"
	"math"

	"github.com/klauspost/compress/zstd"
	"github.com/pierrec/lz4/v4"
)

// readCompressedBlob decompresses one Puffin blob frame. Puffin requires
// compressed blobs to include their uncompressed content size in the frame.
// Bound both the advertised size and the actual output to maxBlobSize.
func readCompressedBlob(data []byte, codec string, maxBlobSize int64) ([]byte, error) {
	if maxBlobSize <= 0 {
		return nil, fmt.Errorf("puffin: invalid maximum blob size %d", maxBlobSize)
	}

	var (
		decoded      io.Reader
		expectedSize uint64
		closeDecoder func()
	)

	switch codec {
	case "lz4":
		input := bytes.NewReader(data)
		header, err := readLZ4FrameHeader(input)
		if err != nil {
			return nil, fmt.Errorf("puffin: invalid LZ4 blob frame: %w", err)
		}
		if !header.hasContentSize {
			return nil, errors.New("puffin: LZ4 blob frame is missing content size")
		}
		expectedSize = header.contentSize
		if expectedSize > uint64(maxBlobSize) {
			return nil, fmt.Errorf("puffin: decompressed blob exceeds maximum size %d", maxBlobSize)
		}
		if err := validateLZ4FrameEnvelope(
			input, int64(len(data)), header.flags, header.headerSize, header.blockMaxSize,
		); err != nil {
			return nil, fmt.Errorf("puffin: invalid LZ4 blob frame: %w", err)
		}
		decoded = lz4.NewReader(bytes.NewReader(data))

	case "zstd":
		var header zstd.Header
		if err := header.Decode(data); err != nil {
			return nil, fmt.Errorf("puffin: invalid zstd blob frame: %w", err)
		}
		if header.Skippable || !header.HasFCS {
			return nil, errors.New("puffin: zstd blob frame is missing content size")
		}
		expectedSize = header.FrameContentSize
		if expectedSize > uint64(maxBlobSize) {
			return nil, fmt.Errorf("puffin: decompressed blob exceeds maximum size %d", maxBlobSize)
		}
		if err := validateZstdFrameEnvelope(data, header); err != nil {
			return nil, fmt.Errorf("puffin: invalid zstd blob frame: %w", err)
		}
		// The decoder may need a window larger than a tiny caller-set blob
		// limit, but its output is still strictly bounded below.
		decoder, err := zstd.NewReader(
			bytes.NewReader(data),
			zstd.WithDecoderConcurrency(1),
			zstd.WithDecoderMaxMemory(max(uint64(maxBlobSize), uint64(1<<20))),
		)
		if err != nil {
			return nil, fmt.Errorf("puffin: create zstd blob decoder: %w", err)
		}
		decoded = decoder
		closeDecoder = decoder.Close

	default:
		return nil, fmt.Errorf("puffin: unsupported blob compression codec %q", codec)
	}
	if closeDecoder != nil {
		defer closeDecoder()
	}

	// Reading one byte past the limit catches corrupt content-size headers and
	// decompression bombs without allocating unbounded output buffers.
	limit := maxBlobSize
	if limit < math.MaxInt64 {
		limit++
	}
	output, err := io.ReadAll(io.LimitReader(decoded, limit))
	if err != nil {
		return nil, fmt.Errorf("puffin: decompress %s blob: %w", codec, err)
	}
	if int64(len(output)) > maxBlobSize {
		return nil, fmt.Errorf("puffin: decompressed blob exceeds maximum size %d", maxBlobSize)
	}
	if uint64(len(output)) != expectedSize {
		return nil, fmt.Errorf("puffin: decompressed %s blob size %d does not match frame content size %d", codec, len(output), expectedSize)
	}

	return output, nil
}

// validateZstdFrameEnvelope ensures the blob contains exactly one frame.
// The decoder accepts concatenated or skippable frames, which Puffin does not.
func validateZstdFrameEnvelope(data []byte, header zstd.Header) error {
	offset := header.HeaderSize
	for {
		if len(data)-offset < 3 {
			return errors.New("truncated zstd block header")
		}
		block := uint32(data[offset]) | uint32(data[offset+1])<<8 | uint32(data[offset+2])<<16
		offset += 3
		last := block&1 != 0
		kind := (block >> 1) & 3
		blockSize := int(block >> 3)
		switch kind {
		case 0, 2:
		case 1:
			blockSize = 1
		default:
			return errors.New("reserved zstd block type")
		}
		if blockSize > len(data)-offset {
			return errors.New("truncated zstd block")
		}
		offset += blockSize
		if last {
			break
		}
	}
	if header.HasCheckSum {
		if len(data)-offset < 4 {
			return errors.New("truncated zstd content checksum")
		}
		offset += 4
	}
	if offset != len(data) {
		return errors.New("zstd frame has trailing data")
	}

	return nil
}
