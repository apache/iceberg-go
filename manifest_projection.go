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

package iceberg

import (
	"errors"
	"fmt"
	"iter"
	"slices"

	iceio "github.com/apache/iceberg-go/io"
	lru "github.com/hashicorp/golang-lru/v2"
	"github.com/twmb/avro"
)

// ManifestEntryProjection selects the optional data-file fields decoded while
// reading a manifest. The fields needed to build a scan task are always read.
// Column statistics are read only when IncludeColumnStats is true.
//
// A projected read is intended for planning paths that use statistics
// transiently. Callers that need the complete DataFile metadata should use
// ManifestFile.Entries or ReadManifest instead.
type ManifestEntryProjection struct {
	IncludeColumnStats bool
}

const manifestEntryProjectionCacheSize = 256

type manifestEntryProjectionCacheKey struct {
	writerSchema       string
	includeColumnStats bool
}

var manifestEntryProjectionCache = func() *lru.Cache[manifestEntryProjectionCacheKey, *avro.Schema] {
	c, err := lru.New[manifestEntryProjectionCacheKey, *avro.Schema](manifestEntryProjectionCacheSize)
	if err != nil {
		panic(err)
	}

	return c
}()

// EntriesWithProjection streams manifest entries using a reader-schema
// projection. It is the projected counterpart to ManifestFile.Entries and is
// useful when a caller needs only the fields required for scan planning.
func EntriesWithProjection(
	fs iceio.IO,
	m ManifestFile,
	discardDeleted bool,
	projection ManifestEntryProjection,
) iter.Seq2[ManifestEntry, error] {
	return manifestEntries(fs, m, discardDeleted, &projection)
}

func manifestEntries(
	fs iceio.IO,
	m ManifestFile,
	discardDeleted bool,
	projection *ManifestEntryProjection,
) iter.Seq2[ManifestEntry, error] {
	return func(yield func(ManifestEntry, error) bool) {
		f, err := fs.Open(m.FilePath())
		if err != nil {
			yield(nil, err)

			return
		}
		aborted := false
		defer func() {
			if cerr := f.Close(); cerr != nil && !aborted {
				yield(nil, cerr)
			}
		}()

		for entry, err := range iterManifest(m, f, discardDeleted, projection) {
			if !yield(entry, err) {
				aborted = true

				return
			}
		}
	}
}

func projectedManifestEntrySchema(
	writerSchema *avro.Schema,
	projection ManifestEntryProjection,
) (*avro.Schema, error) {
	key := manifestEntryProjectionCacheKey{
		// avro.Schema.String returns the original header JSON; it is an O(1)
		// accessor, not a serialization. The bounded cache retains at most one
		// copy of each writer-schema string per projection mode.
		writerSchema:       writerSchema.String(),
		includeColumnStats: projection.IncludeColumnStats,
	}
	if cached, ok := manifestEntryProjectionCache.Get(key); ok {
		return cached, nil
	}

	root := writerSchema.Root()
	// SchemaNode.Schema reads the node tree without mutating it, so the shallow
	// copy intentionally shares immutable Props and Aliases with the writer.
	projectedRoot := *root
	projectedRoot.Fields = slices.Clone(root.Fields)
	dataFileFound := false
	for i := range projectedRoot.Fields {
		if projectedRoot.Fields[i].Name != "data_file" {
			continue
		}

		dataFileFound = true
		dataFile := projectedRoot.Fields[i].Type
		if dataFile.Type != "record" {
			return nil, fmt.Errorf("manifest entry data_file has unexpected Avro type %q", dataFile.Type)
		}

		fields := make([]avro.SchemaField, 0, len(dataFile.Fields))
		for _, field := range dataFile.Fields {
			if manifestScanDataFileField(field.Name, projection.IncludeColumnStats) {
				fields = append(fields, field)
			}
		}
		dataFile.Fields = fields
		projectedRoot.Fields[i].Type = dataFile

		break
	}
	if !dataFileFound {
		return nil, errors.New("manifest entry schema does not contain a data_file field")
	}

	projected, err := projectedRoot.Schema()
	if err != nil {
		return nil, fmt.Errorf("build projected manifest entry schema: %w", err)
	}
	manifestEntryProjectionCache.Add(key, projected)

	return projected, nil
}

func manifestScanDataFileField(name string, includeColumnStats bool) bool {
	switch name {
	case "content", "file_path", "file_format", "partition", "record_count",
		"file_size_in_bytes", "block_size_in_bytes", "key_metadata", "split_offsets", "equality_ids",
		"sort_order_id", "first_row_id", "referenced_data_file", "content_offset",
		"content_size_in_bytes":
		return true
	case "value_counts", "null_value_counts", "nan_value_counts", "lower_bounds", "upper_bounds":
		return includeColumnStats
	default:
		// column_sizes and distinct_counts are not needed to build or read a
		// FileScanTask.
		return false
	}
}

// DataFileWithoutColumnStats returns a copy of the built-in DataFile with
// transient column statistics removed. Other DataFile implementations are
// returned unchanged because the package cannot safely clone their private
// state.
func DataFileWithoutColumnStats(file DataFile) DataFile {
	d, ok := file.(*dataFile)
	if !ok {
		return file
	}

	d.initPartitionData()
	out := cloneDataFileAvroFields(d)
	out.ColSizes = nil
	out.ValCounts = nil
	out.NullCounts = nil
	out.NaNCounts = nil
	out.DistinctCounts = nil
	out.LowerBounds = nil
	out.UpperBounds = nil
	out.fieldNameToID = d.fieldNameToID
	out.fieldIDToLogicalType = d.fieldIDToLogicalType
	out.fieldIDToPartitionData = d.fieldIDToPartitionData
	out.fieldIDToDecimalScale = d.fieldIDToDecimalScale
	out.specID = d.specID

	return out
}

// ManifestEntryWithoutColumnStats returns a copy of an entry whose built-in
// DataFile has had transient column statistics removed.
func ManifestEntryWithoutColumnStats(entry ManifestEntry) ManifestEntry {
	m, ok := entry.(*manifestEntry)
	if !ok {
		return entry
	}

	out := *m
	out.Data = DataFileWithoutColumnStats(m.Data)

	return &out
}
