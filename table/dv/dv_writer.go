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
	"context"
	"fmt"
	"strconv"

	"github.com/apache/iceberg-go"
	iceio "github.com/apache/iceberg-go/io"
	"github.com/apache/iceberg-go/puffin"
)

// DVWriter accumulates deletion positions per data file and flushes them as
// a single Puffin file containing one deletion-vector-v1 blob per data file.
// The returned DataFile entries are ready for RowDelta.AddDeletes().
type DVWriter struct {
	fs      iceio.WriteFileIO
	entries map[string]*RoaringPositionBitmap
	order   []string
}

// NewDVWriter creates a DVWriter backed by the given writable filesystem.
func NewDVWriter(fs iceio.WriteFileIO) *DVWriter {
	return &DVWriter{
		fs:      fs,
		entries: make(map[string]*RoaringPositionBitmap),
	}
}

// Add accumulates positions to delete for a given data file.
// Positions are deduplicated via the underlying roaring bitmap.
func (w *DVWriter) Add(dataFilePath string, positions []int64) {
	bm, ok := w.entries[dataFilePath]
	if !ok {
		bm = NewRoaringPositionBitmap()
		w.entries[dataFilePath] = bm
		w.order = append(w.order, dataFilePath)
	}

	for _, pos := range positions {
		bm.Set(uint64(pos))
	}
}

// Flush writes one Puffin file containing one blob per data file,
// and returns manifest entries ready for RowDelta.AddDeletes().
//
// The location parameter is the full path (including filename) for the
// Puffin file to create. The caller is responsible for generating a
// unique path within the table's metadata directory.
func (w *DVWriter) Flush(_ context.Context, location string) ([]iceberg.DataFile, error) {
	if len(w.entries) == 0 {
		return nil, nil
	}

	var buf bytes.Buffer

	pw, err := puffin.NewWriter(&buf)
	if err != nil {
		return nil, fmt.Errorf("create puffin writer: %w", err)
	}

	type blobResult struct {
		dataFilePath string
		meta         puffin.BlobMetadata
		cardinality  int64
	}

	results := make([]blobResult, 0, len(w.order))

	for _, dataFilePath := range w.order {
		bm := w.entries[dataFilePath]
		dvBytes, err := SerializeDV(bm)
		if err != nil {
			return nil, fmt.Errorf("serialize DV for %s: %w", dataFilePath, err)
		}

		cardinality := bm.Cardinality()
		meta, err := pw.AddBlob(puffin.BlobMetadataInput{
			Type:           puffin.BlobTypeDeletionVector,
			SnapshotID:     -1,
			SequenceNumber: -1,
			Fields:         []int32{},
			Properties: map[string]string{
				dvCardinalityProperty:  strconv.FormatInt(cardinality, 10),
				"referenced-data-file": dataFilePath,
			},
		}, dvBytes)
		if err != nil {
			return nil, fmt.Errorf("add DV blob for %s: %w", dataFilePath, err)
		}

		results = append(results, blobResult{
			dataFilePath: dataFilePath,
			meta:         meta,
			cardinality:  cardinality,
		})
	}

	if err := pw.Finish(); err != nil {
		return nil, fmt.Errorf("finish puffin file: %w", err)
	}

	fileBytes := buf.Bytes()
	if err := w.fs.WriteFile(location, fileBytes); err != nil {
		return nil, fmt.Errorf("write DV puffin file: %w", err)
	}

	fileSize := int64(len(fileBytes))

	dataFiles := make([]iceberg.DataFile, 0, len(results))
	for _, r := range results {
		builder, err := iceberg.NewDataFileBuilder(
			iceberg.PartitionSpec{},
			iceberg.EntryContentPosDeletes,
			location,
			iceberg.PuffinFile,
			nil, nil, nil,
			r.cardinality,
			fileSize,
		)
		if err != nil {
			return nil, fmt.Errorf("build DataFile for %s: %w", r.dataFilePath, err)
		}

		df := builder.
			ReferencedDataFile(r.dataFilePath).
			ContentOffset(r.meta.Offset).
			ContentSizeInBytes(r.meta.Length).
			Build()

		dataFiles = append(dataFiles, df)
	}

	w.entries = make(map[string]*RoaringPositionBitmap)
	w.order = nil

	return dataFiles, nil
}
