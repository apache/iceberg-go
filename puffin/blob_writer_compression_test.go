// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
package puffin_test

import (
	"bytes"
	"io"
	"strconv"
	"testing"

	"github.com/apache/iceberg-go/puffin"
	"github.com/klauspost/compress/zstd"
	"github.com/pierrec/lz4/v4"
	"github.com/stretchr/testify/require"
)

func TestWriteCompressedBlobs(t *testing.T) {
	for _, codec := range []string{"", "lz4", "zstd"} {
		for _, input := range [][]byte{nil, []byte("hello, Iceberg!"), bytes.Repeat([]byte("Iceberg"), 2000)} {
			t.Run(codec+"/"+strconv.Itoa(len(input)), func(t *testing.T) {
				var file bytes.Buffer
				writer, err := puffin.NewWriter(&file)
				require.NoError(t, err)
				inputMeta := puffin.BlobMetadataInput{
					Type:             puffin.BlobTypeDataSketchesTheta,
					Fields:           []int32{12},
					SnapshotID:       5,
					SequenceNumber:   3,
					CompressionCodec: codec,
				}
				meta, err := writer.AddBlob(inputMeta, input)
				require.NoError(t, err)
				require.NoError(t, writer.Finish())
				require.Equal(t, int64(puffin.MagicSize), meta.Offset)
				if codec != "" {
					require.Greater(t, meta.Length, int64(0), "even an empty compressed frame has bytes")
				}
				if codec == "" {
					require.Nil(t, meta.CompressionCodec)
				} else {
					require.Equal(t, codec, *meta.CompressionCodec)
				}
				reader, err := puffin.NewReader(bytes.NewReader(file.Bytes()))
				require.NoError(t, err)
				blobs := reader.Blobs()
				require.Len(t, blobs, 1)
				require.Equal(t, meta, blobs[0])
				encoded := file.Bytes()[meta.Offset : meta.Offset+meta.Length]
				var got []byte
				switch codec {
				case "":
					got = encoded
				case "lz4":
					got, err = io.ReadAll(lz4.NewReader(bytes.NewReader(encoded)))
					require.NoError(t, err)
				case "zstd":
					var header zstd.Header
					require.NoError(t, header.Decode(encoded))
					require.True(t, header.HasFCS)
					require.Equal(t, uint64(len(input)), header.FrameContentSize)
					decoder, decoderErr := zstd.NewReader(nil)
					require.NoError(t, decoderErr)
					got, err = decoder.DecodeAll(encoded, nil)
					decoder.Close()
					require.NoError(t, err)
				}
				require.Equal(t, string(input), string(got))
			})
		}
	}
}

func TestZstdBlobFrameContentSizeBoundaries(t *testing.T) {
	for _, size := range []int{0, 1, 15, 255, 256, 65535, 65536} {
		t.Run(strconv.Itoa(size), func(t *testing.T) {
			var output bytes.Buffer
			writer, err := puffin.NewWriter(&output)
			require.NoError(t, err)
			meta, err := writer.AddBlob(puffin.BlobMetadataInput{
				Type: puffin.BlobTypeDataSketchesTheta, Fields: []int32{1}, CompressionCodec: "zstd",
			}, bytes.Repeat([]byte{42}, size))
			require.NoError(t, err)
			var header zstd.Header
			require.NoError(t, header.Decode(output.Bytes()[meta.Offset:meta.Offset+meta.Length]))
			require.True(t, header.HasFCS)
			require.Equal(t, uint64(size), header.FrameContentSize)
		})
	}
}

func TestCompressedWriterRejectsInvalidCodecWithoutWriting(t *testing.T) {
	var buf bytes.Buffer
	w, err := puffin.NewWriter(&buf)
	require.NoError(t, err)
	n := buf.Len()
	_, err = w.AddBlob(puffin.BlobMetadataInput{
		Type:             puffin.BlobTypeDataSketchesTheta,
		Fields:           []int32{3},
		CompressionCodec: "gzip",
	}, []byte("payload"))
	require.ErrorContains(t, err, "unsupported blob compression codec")
	require.Equal(t, n, buf.Len())
	_, err = w.AddBlob(puffin.BlobMetadataInput{
		Type:             puffin.BlobTypeDeletionVector,
		Fields:           []int32{},
		SnapshotID:       -1,
		SequenceNumber:   -1,
		CompressionCodec: "lz4",
	}, []byte("payload"))
	require.ErrorContains(t, err, "must not be compressed")
	require.Equal(t, n, buf.Len())
	_, err = w.AddBlob(puffin.BlobMetadataInput{Type: puffin.BlobTypeDataSketchesTheta, Fields: []int32{}}, []byte("ok"))
	require.NoError(t, err)
	require.NoError(t, w.Finish())
}
