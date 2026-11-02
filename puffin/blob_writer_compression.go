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
	"fmt"

	"github.com/klauspost/compress/zstd"
	"github.com/pierrec/lz4/v4"
)

// compressBlob returns one complete frame, including the uncompressed content
// size required by the Puffin format. The caller must not compress DVs.
func compressBlob(codec string, data []byte) ([]byte, error) {
	switch codec {
	case "":
		return data, nil
	case "lz4":
		// The lz4 library intentionally omits the content-size field when
		// SizeOption(0) is used. Write the empty frame explicitly instead.
		if len(data) == 0 {
			return emptyLZ4Frame(), nil
		}
		var buf bytes.Buffer
		writer := lz4.NewWriter(&buf)
		if err := writer.Apply(lz4.SizeOption(uint64(len(data))), lz4.ConcurrencyOption(1)); err != nil {
			return nil, fmt.Errorf("puffin: configure lz4 compression: %w", err)
		}
		if _, err := writer.Write(data); err != nil {
			return nil, fmt.Errorf("puffin: compress lz4 blob: %w", err)
		}
		if err := writer.Close(); err != nil {
			return nil, fmt.Errorf("puffin: finalize lz4 blob: %w", err)
		}

		return buf.Bytes(), nil
	case "zstd":
		options := []zstd.EOption{zstd.WithEncoderConcurrency(1), zstd.WithZeroFrames(true)}
		// Non-single-segment frames omit content size for inputs below 256 bytes.
		if len(data) < 256 {
			options = append(options, zstd.WithSingleSegment(true))
		}
		encoder, err := zstd.NewWriter(nil, options...)
		if err != nil {
			return nil, fmt.Errorf("puffin: configure zstd compression: %w", err)
		}
		result := encoder.EncodeAll(data, nil)
		if err := encoder.Close(); err != nil {
			return nil, fmt.Errorf("puffin: finalize zstd blob: %w", err)
		}

		return result, nil
	default:
		return nil, fmt.Errorf("puffin: unsupported blob compression codec %q", codec)
	}
}

// emptyLZ4Frame is an independent-block frame with an explicit zero content
// size. A conventional lz4.Writer cannot emit this header using SizeOption(0).
func emptyLZ4Frame() []byte {
	frame := make([]byte, 4+2+8+1+4)
	binary.LittleEndian.PutUint32(frame[:4], lz4FrameMagic)
	frame[4] = lz4FrameVersionOne | 0x20 | lz4FrameContentSizeFlag
	frame[5] = 0x70 // 4 MiB maximum block size
	frame[14] = emptyLZ4HeaderChecksum(frame[4:14])

	return frame
}

// XXH32 with seed zero for a 10-byte LZ4 frame descriptor.
func emptyLZ4HeaderChecksum(descriptor []byte) byte {
	const (
		prime1 uint32 = 2654435761
		prime3 uint32 = 3266489917
		prime4 uint32 = 668265263
		prime5 uint32 = 374761393
	)
	rotl := func(x uint32, r uint) uint32 { return x<<r | x>>(32-r) }
	h := prime5 + uint32(len(descriptor))
	remaining := descriptor
	for len(remaining) >= 4 {
		h += binary.LittleEndian.Uint32(remaining[:4]) * prime3
		h = rotl(h, 17) * prime4
		remaining = remaining[4:]
	}
	for _, b := range remaining {
		h += uint32(b) * prime5
		h = rotl(h, 11) * prime1
	}
	h ^= h >> 15
	h *= 2246822519
	h ^= h >> 13
	h *= prime3
	h ^= h >> 16

	return byte(h >> 8)
}
