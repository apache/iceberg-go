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
package puffin

import (
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"

	"github.com/apache/iceberg-go"
)

// PuffinWriter writes blobs and metadata to a Puffin file.
//
// Usage:
//
//	w, err := puffin.NewWriter(file)
//	if err != nil {
//	    return err
//	}
//	_, err = w.AddBlob(puffin.BlobMetadataInput{
//	    Type:       puffin.BlobTypeDataSketchesTheta,
//	    SnapshotID: 123,
//	    Fields:     []int32{1},
//	}, sketchBytes)
//	if err != nil {
//	    return err
//	}
//	return w.Finish()
type Writer struct {
	w         io.Writer
	offset    int64
	blobs     []BlobMetadata
	props     map[string]string
	done      bool
	createdBy string
}

// BlobMetadataInput contains fields the caller provides when adding a blob.
// Offset, Length, and CompressionCodec are set by the writer.
type BlobMetadataInput struct {
	Type           BlobType
	SnapshotID     int64
	SequenceNumber int64
	Fields         []int32
	Properties     map[string]string
}

// NewWriter creates a new PuffinWriter and writes the file header magic.
// The caller is responsible for closing the underlying writer after Finish returns.
func NewWriter(w io.Writer) (*Writer, error) {
	if w == nil {
		return nil, errors.New("puffin: writer is nil")
	}

	// Write header magic bytes
	if err := writeAll(w, magic[:]); err != nil {
		return nil, fmt.Errorf("puffin: write header magic: %w", err)
	}

	return &Writer{
		w:         w,
		offset:    MagicSize,
		props:     make(map[string]string),
		createdBy: fmt.Sprintf("iceberg-go %s", iceberg.Version()),
	}, nil
}

// SetProperties merges the provided properties into the file-level properties
// written to the footer. Can be called multiple times before Finish.
func (w *Writer) AddProperties(props map[string]string) error {
	if w.done {
		return errors.New("puffin: cannot set properties: writer already finalized")
	}
	for k, v := range props {
		w.props[k] = v
	}

	return nil
}

// clear properties
func (w *Writer) ClearProperties() {
	w.props = make(map[string]string)
}

// SetCreatedBy overrides the default "created-by" property written to the footer.
// The default value is "iceberg-go". Example: "MyApp version 1.2.3".
func (w *Writer) SetCreatedBy(createdBy string) error {
	if w.done {
		return errors.New("puffin: cannot set created-by: writer already finalized")
	}
	if createdBy == "" {
		return errors.New("puffin: cannot set created-by: value cannot be empty")
	}
	w.createdBy = createdBy

	return nil
}

// AddBlob writes blob data and records its metadata for the footer.
// Returns the complete BlobMetadata including the computed Offset and Length.
// The input.Type is required; use constants like ApacheDataSketchesThetaV1.
func (w *Writer) AddBlob(input BlobMetadataInput, data []byte) (BlobMetadata, error) {
	if w.done {
		return BlobMetadata{}, errors.New("puffin: cannot add blob: writer already finalized")
	}
	if input.Type == "" {
		return BlobMetadata{}, errors.New("puffin: cannot add blob: type is required")
	}
	if input.Fields == nil {
		return BlobMetadata{}, errors.New("puffin: cannot add blob: fields is required")
	}

	meta := BlobMetadata{
		Type:           input.Type,
		SnapshotID:     input.SnapshotID,
		SequenceNumber: input.SequenceNumber,
		Fields:         input.Fields,
		Offset:         w.offset,
		Length:         int64(len(data)),
		Properties:     input.Properties,
	}

	if err := writeAll(w.w, data); err != nil {
		return BlobMetadata{}, fmt.Errorf("puffin: write blob: %w", err)
	}

	w.offset += meta.Length
	w.blobs = append(w.blobs, meta)

	return meta, nil
}

// Finish writes the footer and completes the Puffin file structure.
// Must be called exactly once after all blobs are written.
// After Finish returns, no further operations are allowed on the writer.
func (w *Writer) Finish() error {
	if w.done {
		return errors.New("puffin: cannot finish: writer already finalized")
	}

	// Build footer
	footer := Footer{
		Blobs:      w.blobs,
		Properties: w.props,
	}
	if footer.Properties == nil {
		footer.Properties = make(map[string]string)
	}
	if w.createdBy != "" {
		footer.Properties[CreatedBy] = w.createdBy
	}

	payload, err := json.Marshal(footer)
	if err != nil {
		return fmt.Errorf("puffin: marshal footer: %w", err)
	}

	// Check footer size fits in int32
	if len(payload) > math.MaxInt32 {
		return fmt.Errorf("puffin: footer too large: %d bytes exceeds 2GB limit", len(payload))
	}

	// Write footer start magic
	if err := writeAll(w.w, magic[:]); err != nil {
		return fmt.Errorf("puffin: write footer magic: %w", err)
	}

	// Write footer payload
	if err := writeAll(w.w, payload); err != nil {
		return fmt.Errorf("puffin: write footer payload: %w", err)
	}

	// Write trailer: PayloadSize(4) + Flags(4) + Magic(4)
	var trailer [footerTrailerSize]byte
	binary.LittleEndian.PutUint32(trailer[0:4], uint32(len(payload)))
	binary.LittleEndian.PutUint32(trailer[4:8], 0) // flags = 0 (uncompressed)
	copy(trailer[8:12], magic[:])

	if err := writeAll(w.w, trailer[:]); err != nil {
		return fmt.Errorf("puffin: write footer trailer: %w", err)
	}

	w.done = true

	return nil
}

// writeAll writes all bytes to w or returns an error.
// Handles the io.Writer contract where Write can return n < len(data) without error.
func writeAll(w io.Writer, data []byte) error {
	n, err := w.Write(data)
	if err != nil {
		return err
	}
	if n != len(data) {
		return fmt.Errorf("short write: wrote %d of %d bytes", n, len(data))
	}

	return nil
}
