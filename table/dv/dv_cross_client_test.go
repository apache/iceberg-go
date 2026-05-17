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

package dv

import (
	"context"
	"encoding/binary"
	"hash/crc32"
	"testing"

	"github.com/apache/iceberg-go"
	iceio "github.com/apache/iceberg-go/io"
	"github.com/apache/iceberg-go/puffin"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestCrossClientDVWriterProducesSpecCompliantBlob validates that DVWriter
// output conforms to the DV spec envelope format and can be parsed by
// any spec-compliant reader (Java, pyiceberg, iceberg-rust).
//
// Format verified:
//   - Puffin file structure (magic + blobs + footer)
//   - Blob type is "deletion-vector-v1"
//   - snapshot-id = -1, sequence-number = -1
//   - Blob properties include "cardinality" and "referenced-data-file"
//   - Inner DV envelope: [length][magic 0xD1D33964][bitmap][CRC32]
func TestCrossClientDVWriterProducesSpecCompliantBlob(t *testing.T) {
	fs := iceio.NewMemFS()
	w := NewDVWriter(fs)

	dataPath := "s3://warehouse/db/table/data/00000-0-abc.parquet"
	positions := []int64{1, 3, 5, 7, 9}
	w.Add(dataPath, positions)

	location := "mem://xc/dv-test.puffin"
	dataFiles, err := w.Flush(context.Background(), location)
	require.NoError(t, err)
	require.Len(t, dataFiles, 1)

	df := dataFiles[0]
	assert.Equal(t, iceberg.PuffinFile, df.FileFormat())
	assert.Equal(t, iceberg.EntryContentPosDeletes, df.ContentType())
	assert.Equal(t, int64(5), df.Count())
	assert.Equal(t, dataPath, *df.ReferencedDataFile())

	f, err := fs.Open(location)
	require.NoError(t, err)
	defer f.Close()

	reader, err := puffin.NewReader(f)
	require.NoError(t, err)

	blobs := reader.Blobs()
	require.Len(t, blobs, 1)

	blob := blobs[0]
	assert.Equal(t, puffin.BlobTypeDeletionVector, blob.Type)
	assert.Equal(t, int64(-1), blob.SnapshotID)
	assert.Equal(t, int64(-1), blob.SequenceNumber)
	assert.Equal(t, "5", blob.Properties["cardinality"])
	assert.Equal(t, dataPath, blob.Properties["referenced-data-file"])

	blobData, err := reader.ReadBlob(0)
	require.NoError(t, err)

	verifyDVEnvelopeFormat(t, blobData.Data)

	bm, err := DeserializeDV(blobData.Data, 5)
	require.NoError(t, err)
	assert.Equal(t, int64(5), bm.Cardinality())
	for _, pos := range positions {
		assert.True(t, bm.Contains(uint64(pos)), "position %d should be set", pos)
	}
}

// TestCrossClientGoSerializeMatchesJavaFormat verifies that Go's SerializeDV
// produces byte-identical output to a Java-produced DV fixture for the same
// input positions. Byte equivalence is a stronger cross-client guarantee than
// roundtrip-readability: any spec-compliant reader will accept Go's output,
// and Go's output is indistinguishable from Java's at the wire level.
func TestCrossClientGoSerializeMatchesJavaFormat(t *testing.T) {
	javaBytes := readDVTestData(t, "small-alternating-values-position-index.bin")

	bm := NewRoaringPositionBitmap()
	bm.Set(1)
	bm.Set(3)
	bm.Set(5)
	bm.Set(7)
	bm.Set(9)

	goBytes, err := SerializeDV(bm)
	require.NoError(t, err)

	assert.Equal(t, javaBytes, goBytes,
		"Go-serialized DV must be byte-identical to Java fixture for the same positions")
}

// TestCrossClientMultiFileDVPuffin validates that a multi-blob Puffin file
// (one blob per data file) has correct offsets and each blob is independently
// readable — matching how Java writes multi-DV Puffin files.
func TestCrossClientMultiFileDVPuffin(t *testing.T) {
	fs := iceio.NewMemFS()
	w := NewDVWriter(fs)

	path1 := "s3://warehouse/db/table/data/file-001.parquet"
	path2 := "s3://warehouse/db/table/data/file-002.parquet"
	w.Add(path1, []int64{0, 100, 200})
	w.Add(path2, []int64{50, 150})

	location := "mem://xc/multi-dv.puffin"
	dataFiles, err := w.Flush(context.Background(), location)
	require.NoError(t, err)
	require.Len(t, dataFiles, 2)

	for _, df := range dataFiles {
		bm, err := ReadDV(fs, df)
		require.NoError(t, err)
		assert.Equal(t, df.Count(), bm.Cardinality())
	}

	assert.NotEqual(t, *dataFiles[0].ContentOffset(), *dataFiles[1].ContentOffset(),
		"blobs for different files must have different offsets")
}

func verifyDVEnvelopeFormat(t *testing.T, data []byte) {
	t.Helper()
	require.GreaterOrEqual(t, len(data), dvMinSize, "DV payload too short")

	length := binary.BigEndian.Uint32(data[0:dvLengthSize])
	expectedLen := uint32(len(data) - dvLengthSize - dvCRCSize)
	assert.Equal(t, expectedLen, length, "length field mismatch")

	magic := binary.LittleEndian.Uint32(data[dvLengthSize : dvLengthSize+dvMagicSize])
	assert.Equal(t, DVMagicNumber, magic, "magic number mismatch")

	crcStart := dvLengthSize
	crcEnd := len(data) - dvCRCSize
	computedCRC := crc32.ChecksumIEEE(data[crcStart:crcEnd])
	storedCRC := binary.BigEndian.Uint32(data[crcEnd:])
	assert.Equal(t, computedCRC, storedCRC, "CRC-32 mismatch")
}
