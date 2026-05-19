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
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"log/slog"
	"strconv"

	"github.com/apache/iceberg-go"
	iceio "github.com/apache/iceberg-go/io"
	"github.com/apache/iceberg-go/puffin"
)

// dvCardinalityProperty is the spec-mandated blob property name carrying the
// number of deleted row positions encoded in the inner roaring bitmap. The
// writer attaches it; the reader uses it to detect truncated bitmaps whose
// CRC happens to validate (the CRC covers the bytes that ARE present, not
// the bytes that should have been).
const dvCardinalityProperty = "cardinality"

const (
	// DVMagicNumber is the magic number for deletion vectors.
	// Spec bytes: D1 D3 39 64 (big-endian) = 0x6439D3D1 (little-endian uint32)
	DVMagicNumber uint32 = 0x6439D3D1

	dvLengthSize = 4 // length field
	dvMagicSize  = 4 // magic field
	dvCRCSize    = 4 // CRC-32 checksum
	dvMinSize    = dvLengthSize + dvMagicSize + dvCRCSize
)

// DeserializeDV parses a deletion vector blob and returns a bitmap of deleted positions.
//
// The DV binary format is:
//   - Length (4 bytes, big-endian): size of magic + bitmap data, excluding CRC-32
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
	bitmap, err := DeserializeRoaringPositionBitmap(data[roaringStart:bitmapDataEnd])
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

// ReadDV reads a deletion vector from a puffin file using the manifest entry metadata.
// ContentOffset and ContentSizeInBytes must be set on the DataFile (required by v3 spec).
//
// When the puffin blob carries a `cardinality` property (spec-mandated for
// deletion-vector-v1), its value is parsed and used to validate the decoded
// bitmap — this catches truncated or partially-overwritten blobs whose CRC
// still validates over the bytes that are present.
//
// Java's BitmapPositionDeleteIndex validates against the manifest entry's
// `record_count` field rather than the puffin blob property; the two are
// always set to the same value by Java's writer, so this PR's behavior agrees
// with Java for spec-conformant tables. A future change should cross-validate
// both sources when they're independently available (tracked separately).
//
// Blobs missing the spec-required cardinality property are accepted with a
// slog warning rather than rejected — strict enforcement is deferred until
// the Go DV writer guarantees the property is always present. The per-byte
// CRC check in DeserializeDV still applies, so missing-property is degraded
// integrity, not absent integrity.
func ReadDV(fs iceio.IO, dvFile iceberg.DataFile) (*RoaringPositionBitmap, error) {
	if dvFile.FileFormat() != iceberg.PuffinFile {
		return nil, fmt.Errorf("expected PUFFIN format for deletion vector, got %s", dvFile.FileFormat())
	}

	if dvFile.ContentOffset() == nil || dvFile.ContentSizeInBytes() == nil {
		return nil, fmt.Errorf("DV file %s missing ContentOffset/ContentSizeInBytes", dvFile.FilePath())
	}

	size := *dvFile.ContentSizeInBytes()
	if size < 0 || size > int64(puffin.DefaultMaxBlobSize) {
		return nil, fmt.Errorf("DV blob size %d out of valid range [0, %d]", size, puffin.DefaultMaxBlobSize)
	}

	f, err := fs.Open(dvFile.FilePath())
	if err != nil {
		return nil, fmt.Errorf("open DV file %s: %w", dvFile.FilePath(), err)
	}
	defer f.Close()

	reader, err := puffin.NewReader(f)
	if err != nil {
		return nil, fmt.Errorf("create puffin reader for %s: %w", dvFile.FilePath(), err)
	}

	offset := *dvFile.ContentOffset()
	blobData := make([]byte, size)
	if _, err := reader.ReadAt(blobData, offset); err != nil {
		return nil, fmt.Errorf("read DV blob at offset %d: %w", offset, err)
	}

	cardinality, ok, err := blobCardinality(reader.Blobs(), offset, size)
	if err != nil {
		return nil, fmt.Errorf("DV file %s: %w", dvFile.FilePath(), err)
	}
	if !ok {
		// Spec deviation: deletion-vector-v1 MUST carry a cardinality
		// property. We tolerate the absence to keep reading from third-
		// party writers that emit non-conformant files; flag so operators
		// can identify affected tables.
		slog.Warn("DV blob missing spec-required cardinality property; skipping cardinality validation",
			"dv_file", dvFile.FilePath(), "offset", offset)
		cardinality = -1
	}

	return DeserializeDV(blobData, cardinality)
}

// blobCardinality returns the cardinality declared by the puffin blob at the
// manifest entry's (offset, size). The bool indicates whether the property was
// present:
//
//   - (n, true, nil)  — property found and parsed successfully
//   - (0, false, nil) — matching blob found but no cardinality property
//   - (_, _, err)     — manifest/footer mismatch or property unparseable
//
// Keeping the sentinel out of the int64 return channel avoids leaking
// DeserializeDV's "-1 means skip" convention up the call chain.
func blobCardinality(blobs []puffin.BlobMetadata, offset, size int64) (int64, bool, error) {
	for _, b := range blobs {
		if b.Offset != offset {
			continue
		}
		if b.Length != size {
			// Same starting offset, different length: the manifest entry
			// disagrees with the puffin footer on how big this blob is.
			// Surface that distinct condition rather than rolling it into
			// "no blob at offset" — different writer bug, different fix.
			return 0, false, fmt.Errorf("blob at offset %d has length %d, manifest says %d", offset, b.Length, size)
		}
		v, ok := b.Properties[dvCardinalityProperty]
		if !ok {
			return 0, false, nil
		}
		parsed, err := strconv.ParseInt(v, 10, 64)
		if err != nil {
			return 0, false, fmt.Errorf("invalid %s property %q: %w", dvCardinalityProperty, v, err)
		}
		if parsed < 0 {
			// Negative is meaningless for a count of deleted positions, and
			// `-1` specifically aliases DeserializeDV's skip-validation
			// sentinel — accepting it would silently disable the very check
			// this layer was added to perform.
			return 0, false, fmt.Errorf("%s property must be non-negative, got %d", dvCardinalityProperty, parsed)
		}

		return parsed, true, nil
	}

	return 0, false, fmt.Errorf("no blob in puffin footer at offset %d, size %d", offset, size)
}
