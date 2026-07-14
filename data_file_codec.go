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

	"github.com/apache/iceberg-go/internal"
	"github.com/apache/iceberg-go/internal/datafileavro"
	lru "github.com/hashicorp/golang-lru/v2"
	"github.com/twmb/avro"
)

// defaultSchemaCacheSize is the initial capacity of dataFileSchemaCache.
// 8192 covers a few thousand active tables (each contributing 1–3 entries
// for its partition spec × format version) which is what a long-running
// consumer like a server-side compaction service typically sees.
// SetSchemaCacheSize tunes this at runtime.
const defaultSchemaCacheSize = 8192

func init() {
	if datafileavro.Unmarshal != nil {
		panic("iceberg: datafileavro.Unmarshal already set")
	}
	datafileavro.Unmarshal = func(data []byte, spec, schema any, version int) (any, error) {
		s, ok := spec.(PartitionSpec)
		if !ok {
			return nil, fmt.Errorf("iceberg: datafileavro.Unmarshal: expected PartitionSpec, got %T", spec)
		}
		sc, ok := schema.(*Schema)
		if !ok {
			return nil, fmt.Errorf("iceberg: datafileavro.Unmarshal: expected *Schema, got %T", schema)
		}

		return unmarshalAvroDataFileEntry(data, s, sc, version)
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
// distinct_counts (field 111) is deprecated in the spec for every
// version (apache/iceberg#12182). MarshalAvroEntry drops the field
// on encode for v1, v2, and v3 alike — values populated on the
// source DataFile are not transported. The Avro tag on the dataFile
// struct is intentionally retained so legacy manifests that already
// carry the field on the wire still decode through the matching
// reader. New DataFiles should not set distinct counts.
func (d *dataFile) MarshalAvroEntry(spec PartitionSpec, schema *Schema, version int) ([]byte, error) {
	if version < 1 || version > 3 {
		return nil, fmt.Errorf("iceberg: MarshalAvroEntry: unsupported format version %d", version)
	}
	s, maps, err := manifestEntrySchemaFor(spec, schema, version)
	if err != nil {
		return nil, err
	}
	clone := cloneDataFileAvroFields(d)
	clone.PartitionData = avroEncodePartitionData(d.Partition(), maps.nameToID, maps.idToType, maps.idToDecimal)

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
	df.fieldIDToDecimal = maps.idToDecimal

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
func avroEncodePartitionData(idKeyed map[int]any, nameToID map[string]int, logicalTypes map[int]string, decimals map[int]decimalMeta) map[string]any {
	converted := avroPartitionData(idKeyed, logicalTypes, decimals)
	out := make(map[string]any, len(converted))
	for name, id := range nameToID {
		if v, ok := converted[id]; ok {
			out[name] = v
		}
	}

	return out
}

type dataFileFieldMaps struct {
	nameToID    map[string]int
	idToType    map[int]string
	idToDecimal map[int]decimalMeta
}

// dataFileSchemaCacheKey identifies a cached avro schema by the
// structural fingerprint of the partition Avro shape and the format
// version. The fingerprint is taken from the avro schema produced by
// [partitionTypeToAvroSchema] rather than [StructType.String]: the
// avro shape ignores doc strings and other metadata that don't change
// the wire format, so structurally identical specs that differ only
// in documentation share a single cache entry.
type dataFileSchemaCacheKey struct {
	partAvroFingerprint string
	version             int
}

type dataFileSchemaEntry struct {
	schema *avro.Schema
	maps   dataFileFieldMaps
}

var dataFileSchemaCache = mustNewSchemaCache(defaultSchemaCacheSize)

func mustNewSchemaCache(size int) *lru.Cache[dataFileSchemaCacheKey, *dataFileSchemaEntry] {
	c, err := lru.New[dataFileSchemaCacheKey, *dataFileSchemaEntry](size)
	if err != nil {
		panic(fmt.Sprintf("iceberg: schema cache size %d invalid: %v", size, err))
	}

	return c
}

// SetSchemaCacheSize resizes the manifest-entry schema cache used by
// the DataFile codec. The default capacity is sized for a few thousand
// active partition specs; long-running consumers with larger working
// sets (e.g. a compaction service touching many tables) should raise
// it. Existing entries are preserved on grow; on shrink, least-recently
// used entries are evicted down to the new size. Safe to call concurrently
// with codec operations: the underlying golang-lru/v2 cache serializes Resize
// against Get/Add through the same mutex.
func SetSchemaCacheSize(size int) error {
	if size <= 0 {
		return fmt.Errorf("iceberg: SetSchemaCacheSize: size must be positive, got %d", size)
	}
	dataFileSchemaCache.Resize(size)

	return nil
}

// manifestEntrySchemaFor returns the cached avro schema and partition
// field-id lookups for the given partition type and format version.
// The cache key fingerprints the partition Avro shape, so specs that
// differ only in field documentation share a single entry.
func manifestEntrySchemaFor(spec PartitionSpec, schema *Schema, version int) (*avro.Schema, dataFileFieldMaps, error) {
	partType := spec.PartitionType(schema)
	partSchema, err := partitionTypeToAvroSchema(partType)
	if err != nil {
		return nil, dataFileFieldMaps{}, err
	}
	key := dataFileSchemaCacheKey{partAvroFingerprint: partSchema.String(), version: version}
	if cached, ok := dataFileSchemaCache.Get(key); ok {
		return cached.schema, cached.maps, nil
	}
	fullSchema, err := internal.NewManifestEntrySchema(partSchema, version)
	if err != nil {
		return nil, dataFileFieldMaps{}, err
	}
	n2i, i2t, i2d := getFieldIDMap(fullSchema)
	entry := &dataFileSchemaEntry{
		schema: fullSchema,
		maps: dataFileFieldMaps{
			nameToID:    n2i,
			idToType:    i2t,
			idToDecimal: i2d,
		},
	}
	dataFileSchemaCache.Add(key, entry)

	return entry.schema, entry.maps, nil
}
