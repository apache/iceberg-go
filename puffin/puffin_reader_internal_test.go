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

func TestValidateLZ4FrameEnvelopeUsesBufferedReads(t *testing.T) {
	payload := []byte(`{"blobs":[]}`)
	var compressed bytes.Buffer
	writer := lz4.NewWriter(&compressed)
	require.NoError(t, writer.Apply(
		lz4.SizeOption(uint64(len(payload))),
		lz4.ChecksumOption(false),
	))
	_, err := writer.Write(payload)
	require.NoError(t, err)
	require.NoError(t, writer.Close())

	frame := compressed.Bytes()
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
	require.NoError(t, validateLZ4FrameEnvelope(
		frameReader,
		int64(data.Len()),
		data.Bytes()[4],
		lz4FrameHeaderSizeWithContent,
	))

	require.NotEmpty(t, reader.readLengths)
	require.Less(t, len(reader.readLengths), emptyBlockCount/100)
	for _, length := range reader.readLengths {
		require.Greater(t, length, len(emptyBlock))
	}
}
