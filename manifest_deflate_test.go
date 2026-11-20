// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package iceberg

import (
	"bytes"
	"io"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/twmb/avro/ocf"
)

type countingManifestDeflateCodec struct {
	*manifestDeflateCodec
	decompressions int
}

func (c *countingManifestDeflateCodec) DecompressBounded(src []byte, maxBytes int64) ([]byte, error) {
	c.decompressions++

	return c.manifestDeflateCodec.DecompressBounded(src, maxBytes)
}

func TestManifestReaderKeepsDeflateEntriesFromEarlierBlocks(t *testing.T) {
	schema := NewSchema(1, NestedField{ID: 1, Name: "id", Type: PrimitiveTypes.Int64, Required: true})
	snapshotID := int64(1)
	// Each path exceeds the writer's 64 KiB block threshold.
	paths := []string{strings.Repeat("first", 14<<10), strings.Repeat("other", 14<<10)}
	bounds := [][]byte{{1}, {2}}
	entries := make([]ManifestEntry, len(paths))
	for i, path := range paths {
		builder, err := NewDataFileBuilder(*UnpartitionedSpec, EntryContentData, path, ParquetFile,
			nil, nil, nil, 1, 128)
		require.NoError(t, err)
		builder.LowerBoundValues(map[int][]byte{1: bounds[i]})
		entries[i] = NewManifestEntry(EntryStatusADDED, &snapshotID, nil, nil, builder.Build())
	}

	var encoded bytes.Buffer
	manifest, err := WriteManifest("manifest.avro", &encoded, 2, *UnpartitionedSpec, schema, snapshotID, entries)
	require.NoError(t, err)

	// Prove that the test exercises buffer reuse across two compressed blocks.
	countingCodec := &countingManifestDeflateCodec{manifestDeflateCodec: newManifestDeflateCodec()}
	blockReader, err := ocf.NewReader(bytes.NewReader(encoded.Bytes()), ocf.WithCodec(countingCodec))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, blockReader.Close()) })
	for range entries {
		var decoded any
		require.NoError(t, blockReader.Decode(&decoded))
	}
	var extra any
	require.ErrorIs(t, blockReader.Decode(&extra), io.EOF)
	require.Equal(t, len(entries), countingCodec.decompressions)

	for _, test := range []struct {
		name       string
		projection *ManifestEntryProjection
	}{
		{name: "full"},
		{name: "projected", projection: &ManifestEntryProjection{IncludePruningStats: true}},
	} {
		t.Run(test.name, func(t *testing.T) {
			reader, err := newManifestReader(manifest, bytes.NewReader(encoded.Bytes()), test.projection)
			require.NoError(t, err)
			t.Cleanup(func() { require.NoError(t, reader.Close()) })

			first, err := reader.ReadEntry()
			require.NoError(t, err)
			second, err := reader.ReadEntry()
			require.NoError(t, err)
			_, err = reader.ReadEntry()
			require.ErrorIs(t, err, io.EOF)

			for i, entry := range []ManifestEntry{first, second} {
				require.Equal(t, paths[i], entry.DataFile().FilePath())
				require.Equal(t, bounds[i], entry.DataFile().LowerBoundValues()[1])
			}
		})
	}
}

func TestManifestDeflateCodecRejectsOversizedAndTruncatedBlocks(t *testing.T) {
	codec := newManifestDeflateCodec()
	defer codec.Close()
	data := bytes.Repeat([]byte{42}, 1024)
	encoded, err := codec.Compress(data)
	require.NoError(t, err)

	_, err = codec.DecompressBounded(encoded, int64(len(data)-1))
	require.ErrorContains(t, err, "decompressed block exceeds limit")
	require.Zero(t, codec.output.Cap())
	_, err = codec.DecompressBounded(encoded[:len(encoded)/2], int64(len(data)))
	require.Error(t, err)

	decoded, err := codec.DecompressBounded(encoded, int64(len(data)))
	require.NoError(t, err)
	require.Equal(t, data, decoded)
}

func TestManifestDeflateCodecReleasesLargeBufferAfterSmallBlock(t *testing.T) {
	codec := newManifestDeflateCodec()
	defer codec.Close()
	large := bytes.Repeat([]byte{42}, manifestDeflateShrinkThreshold+1)
	small := []byte("next block")
	largeBlock, err := codec.Compress(large)
	require.NoError(t, err)
	smallBlock, err := codec.Compress(small)
	require.NoError(t, err)

	first, err := codec.Decompress(largeBlock)
	require.NoError(t, err)
	second, err := codec.Decompress(largeBlock)
	require.NoError(t, err)
	require.Same(t, &first[0], &second[0])

	decoded, err := codec.Decompress(smallBlock)
	require.NoError(t, err)
	require.Equal(t, small, decoded)
	require.LessOrEqual(t, codec.output.Cap(), manifestDeflateShrinkThreshold)
}

func TestManifestDeflateCodecAlternatingBlocksAndClose(t *testing.T) {
	codec := newManifestDeflateCodec()
	t.Cleanup(func() { require.NoError(t, codec.Close()) })

	// The inflater must not carry state between empty, small, and large
	// blocks, even after the codec is closed and used again.
	blocks := [][]byte{
		nil,
		[]byte("a"),
		bytes.Repeat([]byte("large block with a distinct payload"), 4096),
		[]byte("a different small block"),
		nil,
		bytes.Repeat([]byte{0, 1, 2, 3, 4, 5}, 2048),
	}
	encoded := make([][]byte, len(blocks))
	for i, block := range blocks {
		var err error
		encoded[i], err = codec.Compress(block)
		require.NoError(t, err)
	}

	for round := 0; round < 3; round++ {
		for i, block := range blocks {
			decoded, err := codec.DecompressBounded(encoded[i], int64(len(block)))
			require.NoError(t, err, "round %d, block %d", round, i)
			require.Len(t, decoded, len(block))
			require.True(t, bytes.Equal(block, decoded), "round %d, block %d", round, i)
		}

		require.NoError(t, codec.Close())
		require.Nil(t, codec.reader)
		require.Zero(t, codec.output.Cap())
	}
}

func TestManifestDeflateCodecRepeatedLimitFailuresRecover(t *testing.T) {
	codec := newManifestDeflateCodec()
	t.Cleanup(func() { require.NoError(t, codec.Close()) })

	for _, plain := range [][]byte{
		[]byte("small block"),
		bytes.Repeat([]byte("longer block"), 1024),
		[]byte("another block"),
	} {
		encoded, err := codec.Compress(plain)
		require.NoError(t, err)

		for attempt := 0; attempt < 2; attempt++ {
			_, err = codec.DecompressBounded(encoded, int64(len(plain)-1))
			require.ErrorContains(t, err, "decompressed block exceeds limit")
			require.Zero(t, codec.output.Cap(), "an unsuccessful block must not retain a buffer")
		}

		decoded, err := codec.DecompressBounded(encoded, int64(len(plain)))
		require.NoError(t, err)
		require.Equal(t, plain, decoded)

		// A zero limit means no bound, not a zero-length block.
		decoded, err = codec.DecompressBounded(encoded, 0)
		require.NoError(t, err)
		require.Equal(t, plain, decoded)
	}
}
