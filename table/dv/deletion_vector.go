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
	"bytes"
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

// SerializeDV produces the spec-format DV binary envelope from a bitmap:
//
//   - Length (4 bytes, big-endian): size of magic + bitmap data, excluding CRC-32
//   - Magic  (4 bytes, little-endian): DVMagicNumber
//   - Bitmap (variable): roaring bitmap in Iceberg portable format
//   - CRC-32 (4 bytes, big-endian): checksum over magic + bitmap
func SerializeDV(bitmap *RoaringPositionBitmap) ([]byte, error) {
	var bitmapBuf bytes.Buffer
	if err := bitmap.Serialize(&bitmapBuf); err != nil {
		return nil, fmt.Errorf("serialize roaring bitmap: %w", err)
	}

	bitmapBytes := bitmapBuf.Bytes()
	innerLen := dvMagicSize + len(bitmapBytes)
	totalSize := dvLengthSize + innerLen + dvCRCSize
	out := make([]byte, totalSize)

	binary.BigEndian.PutUint32(out[0:dvLengthSize], uint32(innerLen))
	binary.LittleEndian.PutUint32(out[dvLengthSize:dvLengthSize+dvMagicSize], DVMagicNumber)
	copy(out[dvLengthSize+dvMagicSize:], bitmapBytes)

	crc := crc32.ChecksumIEEE(out[dvLengthSize : totalSize-dvCRCSize])
	binary.BigEndian.PutUint32(out[totalSize-dvCRCSize:], crc)

	return out, nil
}

// ReadDV reads a deletion vector from a puffin file using the manifest entry metadata.
// ContentOffset and ContentSizeInBytes must be set on the DataFile (required by v3 spec).
//
// The decoded bitmap's cardinality is cross-validated against two independent
// sources, so a truncated or partially-overwritten blob whose CRC still
// validates over the bytes that are present is rejected:
//
//   - The manifest entry's record_count (dvFile.Count()). This is field 103, a
//     required non-nullable long, so it is always available — zero means an
//     empty deletion vector, not "unknown". Java's BitmapPositionDeleteIndex
//     validates against this value, so it is our primary expected cardinality.
//   - The puffin blob's spec-mandated `cardinality` property, when present.
//
// When both sources are available they must agree; a disagreement (e.g. a
// stale manifest record_count against a freshly written blob) is a writer bug
// and fails fast. The bitmap is then validated against the manifest count.
//
// Blobs missing the spec-required cardinality property are still validated
// against the manifest record_count and accepted with a slog warning rather
// than rejected — the Go writer always emits the property, but third-party
// writers may not, and the per-byte CRC check in DeserializeDV still applies.
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

	// Manifest record_count (field 103) is a required, non-nullable long, so it
	// is always present for a DV data file: zero means an empty deletion
	// vector, not "unknown". This is Java's primary expected cardinality.
	manifestCardinality := dvFile.Count()

	// The puffin blob independently declares its cardinality via the spec-
	// mandated property; when present it is a second source to cross-check.
	puffinCardinality, hasPuffinCardinality, err := blobCardinality(reader.Blobs(), offset, size)
	if err != nil {
		return nil, fmt.Errorf("DV file %s: %w", dvFile.FilePath(), err)
	}

	// When both sources are available they must agree. A disagreement means a
	// writer set the manifest record_count and the blob property inconsistently
	// (e.g. a stale record_count after an incremental merge against a freshly
	// written blob) — fail fast rather than silently trusting one over the other.
	if hasPuffinCardinality && manifestCardinality != puffinCardinality {
		return nil, fmt.Errorf("DV file %s: manifest record_count %d disagrees with puffin cardinality property %d",
			dvFile.FilePath(), manifestCardinality, puffinCardinality)
	}

	if !hasPuffinCardinality {
		// Spec deviation: deletion-vector-v1 MUST carry a cardinality property.
		// The manifest record_count still bounds the decoded bitmap below, so
		// this is degraded (not absent) validation; flag it so operators can
		// spot non-conformant third-party writers.
		slog.Warn("DV blob missing spec-required cardinality property; validating against manifest record_count only",
			"dv_file", dvFile.FilePath(), "offset", offset)
	}

	// Validate the decoded bitmap against the manifest record_count (always
	// present, including zero). When the puffin property is present it has
	// already been confirmed to agree with this value above.
	return DeserializeDV(blobData, manifestCardinality)
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
