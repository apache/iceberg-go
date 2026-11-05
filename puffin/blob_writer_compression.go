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
	"fmt"
	"sync"

	"github.com/klauspost/compress/zstd"
	"github.com/pierrec/lz4/v4"
)

// compressBlob returns one complete frame, including the uncompressed content
// size required by the Puffin format. The caller must not compress DVs.
func compressBlob(codec string, data []byte) ([]byte, error) {
	switch codec {
	case "":
		return data, nil
	case CompressionCodecLZ4:
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
	case CompressionCodecZstd:
		// Short Zstd frames need the single-segment flag to include a content size.
		encoder, err := sharedZstdBlobEncoder(len(data) < 256)
		if err != nil {
			return nil, err
		}

		return encoder.EncodeAll(data, nil), nil
	default:
		return nil, fmt.Errorf("puffin: unsupported blob compression codec %q", codec)
	}
}

// emptyLZ4Frame is an independent-block frame with an explicit zero content
// size. LZ4 SizeOption(0) omits that field. The header checksum (0x12) is
// verified by lz4.ValidFrameHeader in the writer tests.
func emptyLZ4Frame() []byte {
	return []byte{
		0x04, 0x22, 0x4d, 0x18, // frame magic
		0x68, 0x70, // frame flags and 4 MiB block size
		0, 0, 0, 0, 0, 0, 0, 0, // content size: zero
		0x12,       // frame descriptor checksum
		0, 0, 0, 0, // end mark
	}
}

var (
	zstdBlobEncoderOnce          sync.Once
	zstdBlobEncoder              *zstd.Encoder
	zstdBlobSingleSegmentEncoder *zstd.Encoder
	zstdBlobEncoderErr           error
)

// sharedZstdBlobEncoder lazily creates two reusable encoders. EncodeAll can be
// called concurrently and does not require resetting or closing the encoder.
func sharedZstdBlobEncoder(singleSegment bool) (*zstd.Encoder, error) {
	zstdBlobEncoderOnce.Do(func() {
		options := []zstd.EOption{zstd.WithEncoderConcurrency(1), zstd.WithZeroFrames(true)}
		zstdBlobEncoder, zstdBlobEncoderErr = zstd.NewWriter(nil, options...)
		if zstdBlobEncoderErr != nil {
			return
		}
		zstdBlobSingleSegmentEncoder, zstdBlobEncoderErr = zstd.NewWriter(
			nil, append(options, zstd.WithSingleSegment(true))...,
		)
		if zstdBlobEncoderErr != nil {
			_ = zstdBlobEncoder.Close()
		}
	})
	if zstdBlobEncoderErr != nil {
		return nil, fmt.Errorf("puffin: configure zstd compression: %w", zstdBlobEncoderErr)
	}
	if singleSegment {
		return zstdBlobSingleSegmentEncoder, nil
	}

	return zstdBlobEncoder, nil
}
