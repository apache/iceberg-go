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
	"fmt"
	"reflect"
	"sync"

	"github.com/apache/iceberg-go/internal"
	"github.com/twmb/avro"
)

// MarshalAvroEntry encodes this DataFile as Avro bytes using the
// manifest-entry encoding for the given partition spec, table schema
// and format version (1, 2, or 3). The wire format is the same one a
// manifest carries for this data file, so adding a field to the
// underlying struct (and its avro tags) automatically extends what
// MarshalAvroEntry transports — there is no separate wire-mirror
// struct to keep in sync.
//
// MarshalAvroEntry is the low-level avro primitive used by the
// [github.com/apache/iceberg-go/codec] package; callers performing
// cross-process transport should use that package's high-level API
// rather than calling this method directly. The receiver MUST decode
// with [UnmarshalAvroDataFileEntry] and the matching
// (spec, schema, version) triple.
//
// MarshalAvroEntry is non-mutating and safe to call concurrently with
// any other reader or encoder of the same DataFile: it encodes a
// shallow copy of df's avro-tagged fields, leaving df untouched.
//
// distinct_counts round-trips on v1 and v2. The v3 manifest-entry
// schema omits the field (deprecated in the v3 spec, see
// apache/iceberg#12182), so it does not survive encode→decode on v3 —
// callers on v3 that need distinct counts must transport them
// separately.
func (d *dataFile) MarshalAvroEntry(spec PartitionSpec, schema *Schema, version int) ([]byte, error) {
	if version < 1 || version > 3 {
		return nil, fmt.Errorf("iceberg: MarshalAvroEntry: unsupported format version %d", version)
	}
	s, maps, err := manifestEntrySchemaFor(spec, schema, version)
	if err != nil {
		return nil, err
	}
	clone := cloneDataFileAvroFields(d)
	clone.PartitionData = avroEncodePartitionData(d.Partition(), maps.nameToID, maps.idToType)

	return s.Encode(newEncodeEntry(version, clone))
}

// UnmarshalAvroDataFileEntry decodes Avro bytes produced by
// [(*dataFile).MarshalAvroEntry] back into a DataFile. The
// (spec, schema, version) triple must match the encoder; passing a
// different spec or version yields a decode error or silently
// mis-typed partition values.
//
// UnmarshalAvroDataFileEntry is the low-level avro primitive used by
// the [github.com/apache/iceberg-go/codec] package; callers performing
// cross-process transport should use that package's high-level API
// rather than calling this function directly.
//
// The returned DataFile carries the partition spec id and the field-id
// lookup tables, so Partition() and the stats accessors return id-keyed
// maps as if the file had been read from a manifest.
func UnmarshalAvroDataFileEntry(data []byte, spec PartitionSpec, schema *Schema, version int) (DataFile, error) {
	if version < 1 || version > 3 {
		return nil, fmt.Errorf("iceberg: UnmarshalAvroDataFileEntry: unsupported format version %d", version)
	}
	s, maps, err := manifestEntrySchemaFor(spec, schema, version)
	if err != nil {
		return nil, err
	}
	entry, df := newDecodeEntry(version)
	if _, err := s.Decode(data, entry); err != nil {
		return nil, fmt.Errorf("iceberg: UnmarshalAvroDataFileEntry: %w", err)
	}
	df.specID = int32(spec.ID())
	df.fieldNameToID = maps.nameToID
	df.fieldIDToLogicalType = maps.idToType
	df.fieldIDToFixedSize = maps.idToFixedSize

	return df, nil
}

// newEncodeEntry returns the right manifest-entry shape for the schema
// version: v1's manifest_entry has a non-nullable snapshot_id and uses
// [fallbackManifestEntry], v2/v3 use [manifestEntry] with nullable
// pointers.
func newEncodeEntry(version int, df *dataFile) any {
	if version == 1 {
		return &fallbackManifestEntry{
			manifestEntry: manifestEntry{EntryStatus: EntryStatusADDED, Data: df},
		}
	}

	return &manifestEntry{EntryStatus: EntryStatusADDED, Data: df}
}

// newDecodeEntry mirrors [newEncodeEntry] for the read side: it returns
// the pointer to pass to avro.Schema.Decode along with the pre-allocated
// *dataFile that will be populated.
func newDecodeEntry(version int) (any, *dataFile) {
	df := &dataFile{}
	if version == 1 {
		return &fallbackManifestEntry{manifestEntry: manifestEntry{Data: df}}, df
	}

	return &manifestEntry{Data: df}, df
}

// cloneDataFileAvroFields returns a fresh *dataFile populated with src's
// avro-tagged fields. Internal state (sync.Once, lazy-init caches,
// specID, the field-id lookup maps) is intentionally left at zero
// values because the avro encoder reads only the avro-tagged fields.
//
// Using reflection over the tag set means a new avro-tagged field
// upstream is auto-copied without an update here — the dataFile struct
// remains the single source of truth for the wire shape. It also
// sidesteps the go-vet copies-lock warning that would fire on a
// struct-literal copy of *dataFile (it embeds sync.Once).
func cloneDataFileAvroFields(src *dataFile) *dataFile {
	out := &dataFile{}
	srcVal := reflect.ValueOf(src).Elem()
	outVal := reflect.ValueOf(out).Elem()
	t := srcVal.Type()
	for i := 0; i < t.NumField(); i++ {
		if _, hasAvroTag := t.Field(i).Tag.Lookup("avro"); hasAvroTag {
			outVal.Field(i).Set(srcVal.Field(i))
		}
	}

	return out
}

// avroEncodePartitionData converts an id-keyed partition tuple (carrying
// iceberg-typed values like Date or Decimal) into the name-keyed
// avro-friendly map the manifest-entry schema expects. Idempotent:
// values already in primitive form pass through unchanged.
func avroEncodePartitionData(idKeyed map[int]any, nameToID map[string]int, logicalTypes map[int]string) map[string]any {
	converted := avroPartitionData(idKeyed, logicalTypes)
	out := make(map[string]any, len(converted))
	for name, id := range nameToID {
		if v, ok := converted[id]; ok {
			out[name] = v
		}
	}

	return out
}

type dataFileFieldMaps struct {
	nameToID      map[string]int
	idToType      map[int]string
	idToFixedSize map[int]int
}

type dataFileSchemaCacheKey struct {
	specID  int
	version int
}

type dataFileSchemaEntry struct {
	schema *avro.Schema
	maps   dataFileFieldMaps
}

var dataFileSchemaCache sync.Map

// manifestEntrySchemaFor returns the cached avro schema and partition
// field-id lookups for (spec, version). The schema is independent of
// the table schema apart from how it shapes the partition struct.
func manifestEntrySchemaFor(spec PartitionSpec, schema *Schema, version int) (*avro.Schema, dataFileFieldMaps, error) {
	key := dataFileSchemaCacheKey{specID: spec.ID(), version: version}
	if cached, ok := dataFileSchemaCache.Load(key); ok {
		e := cached.(*dataFileSchemaEntry)

		return e.schema, e.maps, nil
	}
	partSchema, err := partitionTypeToAvroSchema(spec.PartitionType(schema))
	if err != nil {
		return nil, dataFileFieldMaps{}, err
	}
	fullSchema, err := internal.NewManifestEntrySchema(partSchema, version)
	if err != nil {
		return nil, dataFileFieldMaps{}, err
	}
	n2i, i2t, i2s := getFieldIDMap(fullSchema)
	entry := &dataFileSchemaEntry{
		schema: fullSchema,
		maps: dataFileFieldMaps{
			nameToID:      n2i,
			idToType:      i2t,
			idToFixedSize: i2s,
		},
	}
	if actual, loaded := dataFileSchemaCache.LoadOrStore(key, entry); loaded {
		e := actual.(*dataFileSchemaEntry)

		return e.schema, e.maps, nil
	}

	return entry.schema, entry.maps, nil
}
