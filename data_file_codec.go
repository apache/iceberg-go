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
	"github.com/apache/iceberg-go/internal/datafileavro"
	"github.com/twmb/avro"
)

func init() {
	datafileavro.Unmarshal = func(data []byte, spec, schema any, version int) (any, error) {
		return unmarshalAvroDataFileEntry(data, spec.(PartitionSpec), schema.(*Schema), version)
	}
}

// AvroEntryMarshaler is implemented by DataFile values that can be
// encoded using the manifest-entry Avro encoding. The iceberg
// package's built-in DataFile implementation satisfies it; external
// implementations can also satisfy it to participate in the
// [github.com/apache/iceberg-go/codec] DataFile codec.
//
// The encoded bytes are the same bytes a manifest carries for this
// data file. Implementations must produce output that the iceberg
// package's manifest-entry Avro decoder accepts.
type AvroEntryMarshaler interface {
	MarshalAvroEntry(spec PartitionSpec, schema *Schema, version int) ([]byte, error)
}

// MarshalAvroEntry encodes this DataFile as Avro bytes using the
// manifest-entry encoding for the given partition spec, table schema
// and format version (1, 2, or 3). The wire format is the same one a
// manifest carries for this data file, so adding a field to the
// underlying struct (and its avro tags) automatically extends what
// MarshalAvroEntry transports — there is no separate wire-mirror
// struct to keep in sync.
//
// MarshalAvroEntry is the iceberg-package side of the
// [github.com/apache/iceberg-go/codec] DataFile codec; callers
// performing cross-process transport should prefer that package's
// high-level API.
//
// MarshalAvroEntry is safe to call concurrently with any other
// reader or encoder of the same DataFile: a fresh *dataFile is
// cloned (avro-tagged fields only) and the avro encoder reads, but
// does not mutate, the cloned values. Pointer-typed avro fields like
// ColSizes share their backing storage with the source; the
// thread-safety guarantee relies on the avro encoder being
// non-mutating.
//
// v1 note: the v1 manifest-entry schema has a non-nullable snapshot_id
// field. MarshalAvroEntry writes 0 there, so v1 bytes are not usable
// as a standalone manifest entry — they only round-trip via the
// matching decoder.
//
// distinct_counts (field 111) is deprecated in the spec for all
// versions. MarshalAvroEntry preserves any value already on the
// source for v1 and v2 as a read-compatibility artifact; v3 omits
// the field entirely (apache/iceberg#12182). New DataFiles should
// not set distinct counts.
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

// unmarshalAvroDataFileEntry decodes Avro bytes produced by
// [(*dataFile).MarshalAvroEntry] back into a DataFile. The
// (spec, schema, version) triple must match the encoder; passing a
// different spec or version yields a decode error or silently
// mis-typed partition values.
//
// The returned DataFile carries the partition spec id and the field-id
// lookup tables, so Partition() and the stats accessors return id-keyed
// maps as if the file had been read from a manifest.
//
// It is reachable from the [github.com/apache/iceberg-go/codec]
// package through the [datafileavro] bridge.
func unmarshalAvroDataFileEntry(data []byte, spec PartitionSpec, schema *Schema, version int) (DataFile, error) {
	if version < 1 || version > 3 {
		return nil, fmt.Errorf("iceberg: unmarshalAvroDataFileEntry: unsupported format version %d", version)
	}
	s, maps, err := manifestEntrySchemaFor(spec, schema, version)
	if err != nil {
		return nil, err
	}
	entry, df := newDecodeEntry(version)
	if _, err := s.Decode(data, entry); err != nil {
		return nil, fmt.Errorf("iceberg: unmarshalAvroDataFileEntry: %w", err)
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
//
// Note: this is a shallow copy. Pointer-typed avro fields (ColSizes,
// LowerBounds, etc.) share their backing storage with the source.
// The no-mutation guarantee of MarshalAvroEntry depends on the avro
// encoder being read-only on the values it walks; TestMarshalAvroEntry
// DoesNotMutate asserts this end-to-end across every avro-tagged
// field, so a future regression in the encoder surfaces in tests.
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

// dataFileSchemaCacheKey identifies a cached avro schema by the
// structural fingerprint of its partition type and the format version.
// Using the fingerprint (rather than spec.ID()) avoids cross-table
// collisions: two tables that both rely on InitialPartitionSpecID = 0
// but expose different partition column types receive different
// cached schemas.
type dataFileSchemaCacheKey struct {
	partTypeFingerprint string
	version             int
}

type dataFileSchemaEntry struct {
	schema *avro.Schema
	maps   dataFileFieldMaps
}

var dataFileSchemaCache sync.Map

// manifestEntrySchemaFor returns the cached avro schema and partition
// field-id lookups for the given partition type and format version.
// The cache key uses [StructType.String] as a structural fingerprint
// of the partition type, so two specs that produce different partition
// column types cache under different keys even when they share an id.
func manifestEntrySchemaFor(spec PartitionSpec, schema *Schema, version int) (*avro.Schema, dataFileFieldMaps, error) {
	partType := spec.PartitionType(schema)
	key := dataFileSchemaCacheKey{partTypeFingerprint: partType.String(), version: version}
	if cached, ok := dataFileSchemaCache.Load(key); ok {
		e := cached.(*dataFileSchemaEntry)

		return e.schema, e.maps, nil
	}
	partSchema, err := partitionTypeToAvroSchema(partType)
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
