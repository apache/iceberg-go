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

package puffin

import (
	"bytes"
	"encoding/binary"
	"io"
	"testing"

	"github.com/pierrec/lz4/v4"
	"github.com/stretchr/testify/require"
)

type recordingReaderAt struct {
	data        []byte
	readLengths []int
}

func (r *recordingReaderAt) ReadAt(p []byte, offset int64) (int, error) {
	r.readLengths = append(r.readLengths, len(p))

	return bytes.NewReader(r.data).ReadAt(p, offset)
}

func compressedReaderTestFrame(t *testing.T, payload []byte, options ...lz4.Option) []byte {
	t.Helper()

	var compressed bytes.Buffer
	writer := lz4.NewWriter(&compressed)
	options = append([]lz4.Option{lz4.SizeOption(uint64(len(payload)))}, options...)
	require.NoError(t, writer.Apply(options...))
	_, err := writer.Write(payload)
	require.NoError(t, err)
	require.NoError(t, writer.Close())

	return compressed.Bytes()
}

func compressedReaderTestFile(frame []byte) []byte {
	data := append([]byte("PFA1PFA1"), frame...)
	trailer := make([]byte, footerTrailerSize)
	binary.LittleEndian.PutUint32(trailer[:4], uint32(len(frame)))
	binary.LittleEndian.PutUint32(trailer[4:8], FooterFlagCompressed)
	copy(trailer[8:], magic[:])

	return append(data, trailer...)
}

func TestValidateLZ4FrameEnvelopeUsesBufferedReads(t *testing.T) {
	frame := compressedReaderTestFrame(t, []byte(`{"blobs":[]}`), lz4.ChecksumOption(false))
	endMark := frame[len(frame)-4:]
	frame = frame[:len(frame)-4]
	emptyBlock := make([]byte, 4)
	binary.LittleEndian.PutUint32(emptyBlock, 0x80000000)

	const emptyBlockCount = 20_000
	var data bytes.Buffer
	data.Write(frame)
	for range emptyBlockCount {
		data.Write(emptyBlock)
	}
	data.Write(endMark)

	reader := &recordingReaderAt{data: data.Bytes()}
	frameReader := io.NewSectionReader(reader, 0, int64(data.Len()))
	frameHeader, err := readLZ4FrameHeader(bytes.NewReader(data.Bytes()))
	require.NoError(t, err)
	require.NoError(t, validateLZ4FrameEnvelope(
		frameReader,
		int64(data.Len()),
		frameHeader.flags,
		frameHeader.headerSize,
		frameHeader.blockMaxSize,
	))

	require.NotEmpty(t, reader.readLengths)
	require.Less(t, len(reader.readLengths), emptyBlockCount/100)
	for _, length := range reader.readLengths {
		require.Greater(t, length, len(emptyBlock))
	}
}

func TestValidateLZ4FrameEnvelopeRejectsOversizedBlock(t *testing.T) {
	frame := compressedReaderTestFrame(
		t,
		[]byte(`{"blobs":[]}`),
		lz4.BlockSizeOption(lz4.Block64Kb),
		lz4.ChecksumOption(false),
	)

	frame = frame[:lz4FrameHeaderSizeWithContent]
	blockHeader := make([]byte, 4)
	binary.LittleEndian.PutUint32(blockHeader, uint32(64<<10)+1)
	frame = append(frame, blockHeader...)
	frame = append(frame, bytes.Repeat([]byte{0}, (64<<10)+1)...)
	frame = append(frame, []byte{0, 0, 0, 0}...)

	frameHeader, err := readLZ4FrameHeader(bytes.NewReader(frame))
	require.NoError(t, err)
	reader := &recordingReaderAt{data: frame}

	err = validateLZ4FrameEnvelope(
		reader,
		int64(len(frame)),
		frameHeader.flags,
		frameHeader.headerSize,
		frameHeader.blockMaxSize,
	)
	require.ErrorContains(t, err, "exceeds declared maximum")
	require.Equal(t, []int{frameHeader.headerSize + 4}, reader.readLengths)
}

func TestReaderPreservesLZ4BlockChecksumError(t *testing.T) {
	payload := []byte(`{"blobs":[]}`)
	frame := compressedReaderTestFrame(
		t,
		payload,
		lz4.BlockChecksumOption(true),
		lz4.ChecksumOption(false),
	)
	blockSize := binary.LittleEndian.Uint32(frame[lz4FrameHeaderSizeWithContent:]) & lz4FrameBlockSizeMask
	blockChecksumOffset := lz4FrameHeaderSizeWithContent + 4 + int(blockSize)
	frame[blockChecksumOffset] ^= 0xff
	data := compressedReaderTestFile(frame)

	_, err := NewReader(bytes.NewReader(data))
	require.ErrorContains(t, err, "invalid block checksum")
}
