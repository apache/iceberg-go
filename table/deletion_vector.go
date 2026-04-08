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

package table

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"hash/crc32"

	"github.com/apache/iceberg-go"
	iceio "github.com/apache/iceberg-go/io"
	"github.com/apache/iceberg-go/puffin"
)

const (
	// DVMagicNumber is the magic number for deletion vectors.
	// Spec bytes: D1 D3 39 64 (big-endian) = 0x6439D3D1 (little-endian uint32)
	DVMagicNumber uint32 = 0x6439D3D1

	dvLengthSize = 4 // length field
	dvMagicSize  = 4 // magic field
	dvCRCSize    = 4 // CRC-32 checksum
	dvMinSize    = dvLengthSize + dvMagicSize + dvCRCSize
)

// --- Low-level serialization (bytes ↔ bitmap) ---

// DeserializeDV parses a deletion vector blob and returns a bitmap of deleted positions.
//
// The DV binary format is:
//   - Length (4 bytes, big-endian): size of magic + bitmap data
//   - Magic  (4 bytes, little-endian): must be 0x6439D3D1
//   - Bitmap (variable): roaring bitmap in Iceberg portable format
//   - CRC-32 (4 bytes, big-endian): checksum over magic + bitmap
//
// If expectedCardinality >= 0, the bitmap's cardinality is validated against it.
func DeserializeDV(data []byte, expectedCardinality int64) (*RoaringPositionBitmap, error) {
	if len(data) < dvMinSize {
		return nil, fmt.Errorf("deletion vector payload too short: %d bytes (minimum %d)", len(data), dvMinSize)
	}

	// 1. Read and validate length
	length := binary.BigEndian.Uint32(data[0:dvLengthSize])
	expectedLength := uint32(len(data) - dvLengthSize - dvCRCSize)
	if length != expectedLength {
		return nil, fmt.Errorf("deletion vector length mismatch: got %d, expected %d", length, expectedLength)
	}

	// 2. Read and validate magic
	magic := binary.LittleEndian.Uint32(data[dvLengthSize : dvLengthSize+dvMagicSize])
	if magic != DVMagicNumber {
		return nil, fmt.Errorf("invalid deletion vector magic: 0x%08x, expected 0x%08x", magic, DVMagicNumber)
	}

	// 3. Verify CRC-32 over magic + bitmap (bytes 4 to len-4)
	bitmapDataStart := dvLengthSize
	bitmapDataEnd := len(data) - dvCRCSize
	computedCRC := crc32.ChecksumIEEE(data[bitmapDataStart:bitmapDataEnd])
	expectedCRC := binary.BigEndian.Uint32(data[bitmapDataEnd:])
	if computedCRC != expectedCRC {
		return nil, fmt.Errorf("deletion vector CRC mismatch: computed 0x%08x, expected 0x%08x", computedCRC, expectedCRC)
	}

	// 4. Deserialize roaring bitmap from the inner bytes (after length + magic, before CRC)
	roaringStart := dvLengthSize + dvMagicSize
	bitmap, err := DeserializeRoaringPositionBitmap(bytes.NewReader(data[roaringStart:bitmapDataEnd]))
	if err != nil {
		return nil, fmt.Errorf("deserialize deletion vector bitmap: %w", err)
	}

	// 5. Validate cardinality if requested
	if expectedCardinality >= 0 {
		actual := bitmap.Cardinality()
		if actual != expectedCardinality {
			return nil, fmt.Errorf("deletion vector cardinality mismatch: got %d, expected %d", actual, expectedCardinality)
		}
	}

	return bitmap, nil
}

// --- Reader: reads DV from a puffin file via manifest metadata ---

// ReadDV reads a deletion vector from a puffin file using the manifest entry metadata.
// It uses ContentOffset/ContentSizeInBytes for a direct read when available,
// otherwise falls back to parsing the puffin footer.
func ReadDV(fs iceio.IO, dvFile iceberg.DataFile) (*RoaringPositionBitmap, error) {
	if dvFile.FileFormat() != iceberg.PuffinFile {
		return nil, fmt.Errorf("expected PUFFIN format for deletion vector, got %s", dvFile.FileFormat())
	}

	f, err := fs.Open(dvFile.FilePath())
	if err != nil {
		return nil, fmt.Errorf("open DV file %s: %w", dvFile.FilePath(), err)
	}
	defer f.Close()

	var blobData []byte

	if dvFile.ContentOffset() != nil && dvFile.ContentSizeInBytes() != nil {
		// Fast path: read blob directly at known offset (skip puffin footer parsing).
		// Note: this assumes uncompressed blobs. The Iceberg spec requires DV blobs
		// to be uncompressed, and the puffin reader does not support compression either.
		offset := *dvFile.ContentOffset()
		size := *dvFile.ContentSizeInBytes()
		blobData = make([]byte, size)
		if _, err := f.ReadAt(blobData, offset); err != nil {
			return nil, fmt.Errorf("read DV blob at offset %d: %w", offset, err)
		}
	} else {
		// Fallback: parse puffin footer to find the DV blob
		reader, err := puffin.NewReader(f)
		if err != nil {
			return nil, fmt.Errorf("create puffin reader: %w", err)
		}

		var found bool
		for i, blob := range reader.Blobs() {
			if blob.Type == puffin.BlobTypeDeletionVector {
				blobObj, err := reader.ReadBlob(i)
				if err != nil {
					return nil, fmt.Errorf("read DV blob: %w", err)
				}
				blobData = blobObj.Data
				found = true
				break
			}
		}
		if !found {
			return nil, fmt.Errorf("no deletion-vector-v1 blob found in %s", dvFile.FilePath())
		}
	}

	return DeserializeDV(blobData, dvFile.Count())
}
