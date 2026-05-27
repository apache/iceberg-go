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
	"maps"
	"strconv"

	"github.com/apache/iceberg-go"
	iceio "github.com/apache/iceberg-go/io"
	"github.com/apache/iceberg-go/puffin"
)

// dvEntry holds the per-data-file state a DV manifest entry needs. Each entry
// is keyed by the referenced data file path; the spec and partition record
// come from the data file itself and are propagated unchanged onto the output
// DV manifest entry — matching Java's BaseDVFileWriter.Deletes shape.
type dvEntry struct {
	bitmap        *RoaringPositionBitmap
	spec          iceberg.PartitionSpec
	partitionData map[int]any
}

// DVWriter accumulates deletion positions per data file and flushes them as
// a single Puffin file containing one deletion-vector-v1 blob per data file.
// The returned DataFile entries are ready for RowDelta.AddDeletes().
type DVWriter struct {
	fs      iceio.WriteFileIO
	entries map[string]*dvEntry
	order   []string
}

// NewDVWriter creates a DVWriter backed by the given writable filesystem.
func NewDVWriter(fs iceio.WriteFileIO) *DVWriter {
	return &DVWriter{
		fs:      fs,
		entries: make(map[string]*dvEntry),
	}
}

// Add accumulates positions to delete for a given data file. The spec and
// partitionData are the data file's own partition metadata (from its manifest
// entry) and are propagated to the output DV manifest entry, so partitioned
// tables produce DV entries with the correct partition record. Positions are
// deduplicated via the underlying roaring bitmap.
//
// partitionData keys must be partition field IDs (PartitionField.FieldID), not
// source column IDs. NewDataFileBuilder iterates spec.Fields() and re-keys by
// field name using these IDs; wrong keys silently produce an empty partition
// record on the output DataFile.
//
// First Add for a given dataFilePath captures spec and partitionData on the
// entry; later Adds for the same path append positions only and ignore the
// new spec/partitionData args. This mirrors Java's BaseDVFileWriter, which
// stores partition metadata via computeIfAbsent on the same key. Callers must
// not pass conflicting partition values across Adds for the same data file —
// the writer trusts the first-Add values for the rest of the writer's life.
func (w *DVWriter) Add(dataFilePath string, positions []int64, spec iceberg.PartitionSpec, partitionData map[int]any) {
	entry, ok := w.entries[dataFilePath]
	if !ok {
		// Defensive copies on capture so a caller that mutates or reuses
		// the spec / partition map between Add and Flush can't silently
		// corrupt the entry. Mirrors Java's StructLikeUtil.copy(partition)
		// in BaseDVFileWriter.Deletes. partitionData=nil round-trips as nil
		// (maps.Clone returns nil on nil input).
		entry = &dvEntry{
			bitmap:        NewRoaringPositionBitmap(),
			spec:          copyPartitionSpec(spec),
			partitionData: maps.Clone(partitionData),
		}
		w.entries[dataFilePath] = entry
		w.order = append(w.order, dataFilePath)
	}

	for _, pos := range positions {
		entry.bitmap.Set(uint64(pos))
	}
}

// copyPartitionSpec returns a fresh PartitionSpec that does not alias the
// caller's slice and map backing arrays. PartitionSpec carries a `fields`
// slice and a `sourceIdToFields` map; assigning by value shares both. The
// reconstructed spec calls initialize() internally, building a private
// sourceIdToFields map.
func copyPartitionSpec(spec iceberg.PartitionSpec) iceberg.PartitionSpec {
	fields := make([]iceberg.PartitionField, 0, spec.NumFields())
	for _, f := range spec.Fields() {
		fields = append(fields, f)
	}

	return iceberg.NewPartitionSpecID(spec.ID(), fields...)
}

// Flush writes one Puffin file containing one blob per data file, and returns
// manifest entries ready for RowDelta.AddDeletes(). Each output DataFile
// carries the partition spec and partition record of the data file it
// references, so partitioned tables get spec-correct manifest entries.
//
// The location parameter is the full path (including filename) for the Puffin
// file to create. The caller is responsible for generating a unique path
// within the table's metadata directory.
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
		entry        *dvEntry
		meta         puffin.BlobMetadata
		cardinality  int64
	}

	results := make([]blobResult, 0, len(w.order))

	for _, dataFilePath := range w.order {
		entry := w.entries[dataFilePath]
		dvBytes, err := SerializeDV(entry.bitmap)
		if err != nil {
			return nil, fmt.Errorf("serialize DV for %s: %w", dataFilePath, err)
		}

		cardinality := entry.bitmap.Cardinality()
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
			entry:        entry,
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
			r.entry.spec,
			iceberg.EntryContentPosDeletes,
			location,
			iceberg.PuffinFile,
			r.entry.partitionData,
			nil, nil,
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

	w.entries = make(map[string]*dvEntry)
	w.order = nil

	return dataFiles, nil
}
