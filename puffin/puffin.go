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
//
// Package puffin provides reading and writing of Puffin files.
//
// Puffin is a file format designed to store statistics and indexes
// for Iceberg tables. A Puffin file contains blobs (opaque byte sequences)
// with associated metadata, such as Apache DataSketches or deletion vectors.
//
// File structure:
//
//	[Magic] [Blob]* [Magic] [Footer Payload] [Footer Payload Size] [Flags] [Magic]
//
// See the specification at https://iceberg.apache.org/puffin-spec/
package puffin

import (
	"encoding/json"
	"errors"
	"fmt"
)

var magic = [4]byte{'P', 'F', 'A', '1'}

const (
	//[Magic] [FooterPayload] [FooterPayloadSize] [Flags] [Magic]
	// MagicSize is the number of bytes in the magic marker.
	MagicSize = 4

	// footerTrailerSize accounts for footer length (4)+ flags (4) + trailing magic (4).
	footerTrailerSize = 12

	// FooterFlagCompressed indicates that the footer payload uses the Puffin
	// format's LZ4 frame compression.
	FooterFlagCompressed = 1 // bit 0

	// Prevents OOM
	// DefaultMaxBlobSize is the maximum blob size allowed when reading (256 MB).
	// Override with WithMaxBlobSize when creating a reader.
	DefaultMaxBlobSize = 256 << 20

	// DefaultMaxFooterSize is the maximum footer payload size allowed when
	// reading (64 MB). For compressed footer payloads, the limit applies after
	// decompression. Override with WithMaxFooterSize when creating a reader.
	DefaultMaxFooterSize = 64 << 20

	// CreatedBy is a human-readable identification of the application writing the file, along with its version.
	// Example: "Trino version 381".
	CreatedBy = "created-by"
)

type BlobMetadata struct {
	Type             BlobType          `json:"type"`
	Fields           []int32           `json:"fields"`
	SnapshotID       int64             `json:"snapshot-id"`
	SequenceNumber   int64             `json:"sequence-number"`
	Offset           int64             `json:"offset"`
	Length           int64             `json:"length"`
	CompressionCodec *string           `json:"compression-codec,omitempty"`
	Properties       map[string]string `json:"properties,omitempty"`
}

// Footer describes the blobs and file-level properties stored in a Puffin file.
type Footer struct {
	Blobs      []BlobMetadata    `json:"blobs"`
	Properties map[string]string `json:"properties,omitempty"`
}

// decodePuffinProperties rejects non-string property values. Go's JSON decoder
// otherwise converts null map values into empty strings, unlike Iceberg Java.
func decodePuffinProperties(data json.RawMessage) (map[string]string, error) {
	if len(data) == 0 {
		return nil, nil
	}

	var raw map[string]*string
	if err := json.Unmarshal(data, &raw); err != nil {
		return nil, fmt.Errorf("invalid properties: %w", err)
	}
	if raw == nil {
		return nil, errors.New("properties must be a JSON object")
	}

	properties := make(map[string]string, len(raw))
	for key, value := range raw {
		if value == nil {
			return nil, fmt.Errorf("property %q must be a string", key)
		}
		properties[key] = *value
	}

	return properties, nil
}

// UnmarshalJSON rejects missing or null required Puffin blob metadata fields.
// Without this check, encoding/json silently substitutes zero values.
func (b *BlobMetadata) UnmarshalJSON(data []byte) error {
	var wire struct {
		Type             *BlobType       `json:"type"`
		Fields           *[]int32        `json:"fields"`
		SnapshotID       *int64          `json:"snapshot-id"`
		SequenceNumber   *int64          `json:"sequence-number"`
		Offset           *int64          `json:"offset"`
		Length           *int64          `json:"length"`
		CompressionCodec *string         `json:"compression-codec"`
		Properties       json.RawMessage `json:"properties"`
	}
	if err := json.Unmarshal(data, &wire); err != nil {
		return err
	}
	switch {
	case wire.Type == nil:
		return errors.New("blob metadata missing required field type")
	case wire.Fields == nil:
		return errors.New("blob metadata missing required field fields")
	case wire.SnapshotID == nil:
		return errors.New("blob metadata missing required field snapshot-id")
	case wire.SequenceNumber == nil:
		return errors.New("blob metadata missing required field sequence-number")
	case wire.Offset == nil:
		return errors.New("blob metadata missing required field offset")
	case wire.Length == nil:
		return errors.New("blob metadata missing required field length")
	}

	properties, err := decodePuffinProperties(wire.Properties)
	if err != nil {
		return fmt.Errorf("blob metadata: %w", err)
	}

	*b = BlobMetadata{
		Type:             *wire.Type,
		Fields:           *wire.Fields,
		SnapshotID:       *wire.SnapshotID,
		SequenceNumber:   *wire.SequenceNumber,
		Offset:           *wire.Offset,
		Length:           *wire.Length,
		CompressionCodec: wire.CompressionCodec,
		Properties:       properties,
	}

	return nil
}

// UnmarshalJSON requires the blobs list, even when it is empty.
func (f *Footer) UnmarshalJSON(data []byte) error {
	var wire struct {
		Blobs      *[]json.RawMessage `json:"blobs"`
		Properties json.RawMessage    `json:"properties"`
	}
	if err := json.Unmarshal(data, &wire); err != nil {
		return err
	}
	if wire.Blobs == nil {
		return errors.New("footer missing required field blobs")
	}

	blobs := make([]BlobMetadata, len(*wire.Blobs))
	for i, raw := range *wire.Blobs {
		if err := json.Unmarshal(raw, &blobs[i]); err != nil {
			return fmt.Errorf("blob %d: %w", i, err)
		}
	}

	properties, err := decodePuffinProperties(wire.Properties)
	if err != nil {
		return fmt.Errorf("footer: %w", err)
	}

	*f = Footer{Blobs: blobs, Properties: properties}

	return nil
}

// BlobType identifies the format of a Puffin blob. Unknown non-empty names
// are preserved so that callers can round-trip vendor-defined blob types.
type BlobType string

const (
	// BlobTypeDataSketchesTheta is a serialized compact Theta sketch
	// produced by the Apache DataSketches library.
	BlobTypeDataSketchesTheta BlobType = "apache-datasketches-theta-v1"

	// BlobTypeDeletionVector is a serialized deletion vector per the
	// Iceberg spec. Requires snapshot-id and sequence-number to be -1.
	BlobTypeDeletionVector BlobType = "deletion-vector-v1"
)

// IsKnown reports whether the type is one of the standard blob types
// defined by the Puffin spec. Unknown non-empty types are still valid metadata.
func (bt BlobType) IsKnown() bool {
	switch bt {
	case BlobTypeDataSketchesTheta, BlobTypeDeletionVector:
		return true
	default:
		return false
	}
}

// UnmarshalJSON rejects empty and null blob types while preserving unknown
// non-empty type names for forward compatibility.
func (bt *BlobType) UnmarshalJSON(data []byte) error {
	var name string
	if err := json.Unmarshal(data, &name); err != nil {
		return err
	}
	if name == "" {
		return fmt.Errorf("invalid blob type: %s", name)
	}

	*bt = BlobType(name)

	return nil
}
