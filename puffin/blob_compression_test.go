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

package puffin_test

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"testing"

	"github.com/apache/iceberg-go/puffin"
	"github.com/klauspost/compress/zstd"
	"github.com/pierrec/lz4/v4"
	"github.com/stretchr/testify/require"
)

func compressedBlobTestFile(t *testing.T, codec string, payload []byte) []byte {
	t.Helper()

	return compressedBlobTestFileWithMetadata(t, payload, puffin.BlobMetadata{
		Type:             puffin.BlobTypeDataSketchesTheta,
		Fields:           []int32{1},
		SnapshotID:       42,
		SequenceNumber:   7,
		Offset:           puffin.MagicSize,
		Length:           int64(len(payload)),
		CompressionCodec: &codec,
	})
}

func compressedBlobTestFileWithMetadata(t *testing.T, payload []byte, metadata puffin.BlobMetadata) []byte {
	t.Helper()
	footer, err := json.Marshal(puffin.Footer{Blobs: []puffin.BlobMetadata{metadata}})
	require.NoError(t, err)

	var data bytes.Buffer
	data.WriteString("PFA1")
	data.Write(payload)
	data.WriteString("PFA1")
	data.Write(footer)
	var trailer [12]byte
	binary.LittleEndian.PutUint32(trailer[:4], uint32(len(footer)))
	copy(trailer[8:], "PFA1")
	data.Write(trailer[:])

	return data.Bytes()
}

func compressedBlobTestPayload(t *testing.T, codec string, data []byte) []byte {
	t.Helper()

	var output bytes.Buffer
	switch codec {
	case "lz4":
		writer := lz4.NewWriter(&output)
		require.NoError(t, writer.Apply(lz4.SizeOption(uint64(len(data)))))
		_, err := writer.Write(data)
		require.NoError(t, err)
		require.NoError(t, writer.Close())

	case "zstd":
		writer, err := zstd.NewWriter(nil)
		require.NoError(t, err)
		defer writer.Close()

		return writer.EncodeAll(data, nil)
	}

	return output.Bytes()
}

func TestReadCompressedPuffinBlobs(t *testing.T) {
	raw := bytes.Repeat([]byte("iceberg blob content "), 256)
	for _, codec := range []string{"lz4", "zstd"} {
		t.Run(codec, func(t *testing.T) {
			stored := compressedBlobTestPayload(t, codec, raw)
			r, err := puffin.NewReader(bytes.NewReader(compressedBlobTestFile(t, codec, stored)))
			require.NoError(t, err)

			blob, err := r.ReadBlob(0)
			require.NoError(t, err)
			require.Equal(t, raw, blob.Data)
			require.Equal(t, int64(len(stored)), blob.Metadata.Length)
			require.Equal(t, codec, *blob.Metadata.CompressionCodec)

			byMetadata, err := r.ReadBlobByMetadata(r.Blobs()[0])
			require.NoError(t, err)
			require.Equal(t, raw, byMetadata)

			all, err := r.ReadAllBlobs()
			require.NoError(t, err)
			require.Len(t, all, 1)
			require.Equal(t, raw, all[0].Data)
		})
	}
}

func TestReadCompressedPuffinBlobHonorsUncompressedLimit(t *testing.T) {
	raw := bytes.Repeat([]byte("a"), 4096)
	for _, codec := range []string{"lz4", "zstd"} {
		t.Run(codec, func(t *testing.T) {
			stored := compressedBlobTestPayload(t, codec, raw)
			require.Less(t, len(stored), 128)
			r, err := puffin.NewReader(
				bytes.NewReader(compressedBlobTestFile(t, codec, stored)),
				puffin.WithMaxBlobSize(128),
			)
			require.NoError(t, err)

			_, err = r.ReadBlob(0)
			require.ErrorContains(t, err, "decompressed blob exceeds maximum size")
		})
	}
}

func TestReadCompressedPuffinBlobRejectsInvalidFrames(t *testing.T) {
	raw := []byte("Puffin blob frame")
	t.Run("unknown codec", func(t *testing.T) {
		r, err := puffin.NewReader(bytes.NewReader(compressedBlobTestFile(t, "gzip", raw)))
		require.NoError(t, err)
		_, err = r.ReadBlob(0)
		require.ErrorContains(t, err, "not supported")
	})

	t.Run("lz4 missing content size", func(t *testing.T) {
		var output bytes.Buffer
		writer := lz4.NewWriter(&output)
		_, err := writer.Write(raw)
		require.NoError(t, err)
		require.NoError(t, writer.Close())

		r, err := puffin.NewReader(bytes.NewReader(compressedBlobTestFile(t, "lz4", output.Bytes())))
		require.NoError(t, err)
		_, err = r.ReadBlob(0)
		require.ErrorContains(t, err, "missing content size")
	})

	t.Run("lz4 trailing frame", func(t *testing.T) {
		frame := compressedBlobTestPayload(t, "lz4", raw)
		frame = append(frame, 0xff)
		r, err := puffin.NewReader(bytes.NewReader(compressedBlobTestFile(t, "lz4", frame)))
		require.NoError(t, err)
		_, err = r.ReadBlob(0)
		require.ErrorContains(t, err, "trailing data")
	})

	t.Run("zstd missing content size", func(t *testing.T) {
		var output bytes.Buffer
		writer, err := zstd.NewWriter(&output)
		require.NoError(t, err)
		_, err = writer.Write(raw)
		require.NoError(t, err)
		require.NoError(t, writer.Close())

		r, err := puffin.NewReader(bytes.NewReader(compressedBlobTestFile(t, "zstd", output.Bytes())))
		require.NoError(t, err)
		_, err = r.ReadBlob(0)
		require.ErrorContains(t, err, "missing content size")
	})
}

func TestReadCompressedPuffinBlobRejectsZstdTrailingFrames(t *testing.T) {
	data := bytes.Repeat([]byte("single frame"), 256)
	frame := compressedBlobTestPayload(t, "zstd", data)
	for _, tc := range []struct {
		name     string
		trailing []byte
	}{
		{"empty frame", []byte{0x28, 0xb5, 0x2f, 0xfd, 0x20, 0x00, 0x01, 0x00, 0x00}},
		{"skippable frame", []byte{0x50, 0x2a, 0x4d, 0x18, 0, 0, 0, 0}},
		{"nonempty frame", compressedBlobTestPayload(t, "zstd", []byte("other"))},
		{"trailing byte", []byte{0}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			payload := append(bytes.Clone(frame), tc.trailing...)
			r, err := puffin.NewReader(bytes.NewReader(compressedBlobTestFile(t, "zstd", payload)))
			require.NoError(t, err)
			_, err = r.ReadBlob(0)
			require.ErrorContains(t, err, "trailing data")
		})
	}
}

func TestReadCompressedPuffinBlobRejectsTruncatedZstd(t *testing.T) {
	frame := compressedBlobTestPayload(t, "zstd", bytes.Repeat([]byte("hello"), 100))
	for n := 1; n < len(frame); n++ {
		r, err := puffin.NewReader(bytes.NewReader(compressedBlobTestFile(t, "zstd", frame[:n])))
		require.NoError(t, err)
		_, err = r.ReadBlob(0)
		require.Error(t, err, "truncated at %d of %d", n, len(frame))
	}
}

func TestReadCompressedPuffinBlobZstdMultipleBlocksAndEmpty(t *testing.T) {
	large := make([]byte, 300000)
	for i := range large {
		large[i] = byte((i * 31) ^ (i >> 7))
	}
	for _, tc := range []struct {
		name        string
		frame, want []byte
	}{
		{"multiple blocks", compressedBlobTestPayload(t, "zstd", large), large},
		{"empty", []byte{0x28, 0xb5, 0x2f, 0xfd, 0x20, 0x00, 0x01, 0x00, 0x00}, []byte{}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			r, err := puffin.NewReader(bytes.NewReader(compressedBlobTestFile(t, "zstd", tc.frame)))
			require.NoError(t, err)
			blob, err := r.ReadBlob(0)
			require.NoError(t, err)
			require.Equal(t, tc.want, blob.Data)
		})
	}
}

func TestReadRejectsCompressedDeletionVectors(t *testing.T) {
	for _, codec := range []string{"lz4", "zstd"} {
		t.Run(codec, func(t *testing.T) {
			stored := compressedBlobTestPayload(t, codec, []byte("invalid compressed deletion vector"))
			meta := puffin.BlobMetadata{
				Type:             puffin.BlobTypeDeletionVector,
				Fields:           []int32{},
				SnapshotID:       -1,
				SequenceNumber:   -1,
				Offset:           puffin.MagicSize,
				Length:           int64(len(stored)),
				CompressionCodec: &codec,
			}
			_, err := puffin.NewReader(bytes.NewReader(compressedBlobTestFileWithMetadata(t, stored, meta)))
			require.ErrorContains(t, err, "deletion-vector-v1 must not be compressed")

			reader, err := puffin.NewReader(bytes.NewReader(compressedBlobTestFile(t, codec, stored)))
			require.NoError(t, err)
			_, err = reader.ReadBlobByMetadata(meta)
			require.ErrorContains(t, err, "deletion-vector-v1 must not be compressed")
		})
	}
}

func TestReadSmallZstdBlobWithLargeWindow(t *testing.T) {
	raw := bytes.Repeat([]byte("abc"), 100)
	encoder, err := zstd.NewWriter(nil, zstd.WithWindowSize(8<<20), zstd.WithSingleSegment(false))
	require.NoError(t, err)
	frame := encoder.EncodeAll(raw, nil)
	encoder.Close()
	var initial zstd.Header
	require.NoError(t, initial.Decode(frame))
	require.False(t, initial.SingleSegment)
	// Set a valid 8 MiB window descriptor while retaining the original blocks.
	frame[5] = 0x68
	var header zstd.Header
	require.NoError(t, header.Decode(frame))
	require.True(t, header.HasFCS)
	require.Equal(t, uint64(8<<20), header.WindowSize)
	require.Equal(t, uint64(len(raw)), header.FrameContentSize)
	r, err := puffin.NewReader(bytes.NewReader(compressedBlobTestFile(t, "zstd", frame)), puffin.WithMaxBlobSize(1024))
	require.NoError(t, err)
	blob, err := r.ReadBlob(0)
	require.NoError(t, err)
	require.Equal(t, raw, blob.Data)
}
