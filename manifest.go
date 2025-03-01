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
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"reflect"
	"slices"
	"strconv"
	"sync"
	"time"

	"github.com/apache/iceberg-go/internal"
	iceio "github.com/apache/iceberg-go/io"
	"github.com/google/uuid"

	"github.com/hamba/avro/v2"
	"github.com/hamba/avro/v2/ocf"
)

// ManifestContent indicates the type of data inside of the files
// described by a manifest. This will indicate whether the data files
// contain active data or deleted rows.
type ManifestContent int32

const (
	ManifestContentData    ManifestContent = 0
	ManifestContentDeletes ManifestContent = 1
)

func (m ManifestContent) String() string {
	switch m {
	case ManifestContentData:
		return "data"
	case ManifestContentDeletes:
		return "deletes"
	default:
		return "UNKNOWN"
	}
}

type FieldSummary struct {
	ContainsNull bool    `avro:"contains_null"`
	ContainsNaN  *bool   `avro:"contains_nan"`
	LowerBound   *[]byte `avro:"lower_bound"`
	UpperBound   *[]byte `avro:"upper_bound"`
}

// ManifestV1Builder is a helper for building a V1 manifest file
// struct which will conform to the ManifestFile interface.
type ManifestV1Builder struct {
	m *manifestFileV1
}

// NewManifestV1Builder is passed all of the required fields and then allows
// all of the optional fields to be set by calling the corresponding methods
// before calling [ManifestV1Builder.Build] to construct the object.
func NewManifestV1Builder(path string, length int64, partitionSpecID int32, addedSnapshotID int64) *ManifestV1Builder {
	return &ManifestV1Builder{
		m: &manifestFileV1{
			Path:            path,
			Len:             length,
			SpecID:          partitionSpecID,
			AddedSnapshotID: addedSnapshotID,
		},
	}
}

func (b *ManifestV1Builder) PartitionType(st *StructType) *ManifestV1Builder {
	b.m.partitionType = st

	return b
}

func (b *ManifestV1Builder) AddedFiles(cnt int32) *ManifestV1Builder {
	b.m.AddedFilesCount = &cnt

	return b
}

func (b *ManifestV1Builder) ExistingFiles(cnt int32) *ManifestV1Builder {
	b.m.ExistingFilesCount = &cnt

	return b
}

func (b *ManifestV1Builder) DeletedFiles(cnt int32) *ManifestV1Builder {
	b.m.DeletedFilesCount = &cnt

	return b
}

func (b *ManifestV1Builder) AddedRows(cnt int64) *ManifestV1Builder {
	b.m.AddedRowsCount = &cnt

	return b
}

func (b *ManifestV1Builder) ExistingRows(cnt int64) *ManifestV1Builder {
	b.m.ExistingRowsCount = &cnt

	return b
}

func (b *ManifestV1Builder) DeletedRows(cnt int64) *ManifestV1Builder {
	b.m.DeletedRowsCount = &cnt

	return b
}

func (b *ManifestV1Builder) Partitions(p []FieldSummary) *ManifestV1Builder {
	b.m.PartitionList = &p

	return b
}

func (b *ManifestV1Builder) KeyMetadata(km []byte) *ManifestV1Builder {
	b.m.Key = km

	return b
}

// Build returns the constructed manifest file, after calling Build this
// builder should not be used further as we avoid copying by just returning
// a pointer to the constructed manifest file. Further calls to the modifier
// methods after calling build would modify the constructed ManifestFile.
func (b *ManifestV1Builder) Build() ManifestFile {
	if b.m.partitionType == nil {
		b.m.partitionType = UnpartitionedSpec.PartitionType(nil)
	}

	return b.m
}

type fallbackManifestFileV1 struct {
	manifestFileV1
	AddedSnapshotID *int64 `avro:"added_snapshot_id"`
}

func (f *fallbackManifestFileV1) toManifest() *manifestFileV1 {
	f.manifestFileV1.AddedSnapshotID = *f.AddedSnapshotID

	return &f.manifestFileV1
}

type manifestFileV1 struct {
	partitionType      *StructType
	Path               string          `avro:"manifest_path"`
	Len                int64           `avro:"manifest_length"`
	SpecID             int32           `avro:"partition_spec_id"`
	AddedSnapshotID    int64           `avro:"added_snapshot_id"`
	AddedFilesCount    *int32          `avro:"added_files_count"`
	ExistingFilesCount *int32          `avro:"existing_files_count"`
	DeletedFilesCount  *int32          `avro:"deleted_files_count"`
	AddedRowsCount     *int64          `avro:"added_rows_count"`
	ExistingRowsCount  *int64          `avro:"existing_rows_count"`
	DeletedRowsCount   *int64          `avro:"deleted_rows_count"`
	PartitionList      *[]FieldSummary `avro:"partitions"`
	Key                []byte          `avro:"key_metadata"`
}

func (*manifestFileV1) Version() int             { return 1 }
func (m *manifestFileV1) FilePath() string       { return m.Path }
func (m *manifestFileV1) Length() int64          { return m.Len }
func (m *manifestFileV1) PartitionSpecID() int32 { return m.SpecID }
func (m *manifestFileV1) ManifestContent() ManifestContent {
	return ManifestContentData
}

func (m *manifestFileV1) SnapshotID() int64 {
	return m.AddedSnapshotID
}

func (m *manifestFileV1) AddedDataFiles() int32 {
	if m.AddedFilesCount == nil {
		return 0
	}

	return *m.AddedFilesCount
}

func (m *manifestFileV1) ExistingDataFiles() int32 {
	if m.ExistingFilesCount == nil {
		return 0
	}

	return *m.ExistingFilesCount
}

func (m *manifestFileV1) DeletedDataFiles() int32 {
	if m.DeletedFilesCount == nil {
		return 0
	}

	return *m.DeletedFilesCount
}

func (m *manifestFileV1) AddedRows() int64 {
	if m.AddedRowsCount == nil {
		return 0
	}

	return *m.AddedRowsCount
}

func (m *manifestFileV1) ExistingRows() int64 {
	if m.ExistingRowsCount == nil {
		return 0
	}

	return *m.ExistingRowsCount
}

func (m *manifestFileV1) DeletedRows() int64 {
	if m.DeletedRowsCount == nil {
		return 0
	}

	return *m.DeletedRowsCount
}

func (m *manifestFileV1) HasAddedFiles() bool {
	return m.AddedFilesCount == nil || *m.AddedFilesCount > 0
}

func (m *manifestFileV1) HasExistingFiles() bool {
	return m.ExistingFilesCount == nil || *m.ExistingFilesCount > 0
}

func (m *manifestFileV1) SequenceNum() int64    { return 0 }
func (m *manifestFileV1) MinSequenceNum() int64 { return 0 }
func (m *manifestFileV1) KeyMetadata() []byte   { return m.Key }
func (m *manifestFileV1) Partitions() []FieldSummary {
	if m.PartitionList == nil {
		return nil
	}

	return *m.PartitionList
}

func (m *manifestFileV1) FetchEntries(fs iceio.IO, discardDeleted bool) ([]ManifestEntry, error) {
	return fetchManifestEntries(m, fs, discardDeleted)
}

// // WriteEntries writes a list of manifest entries to an avro file.
// func (m *manifestFileV1) WriteEntries(out io.Writer, entries []ManifestEntry) error {
// 	return writeManifestEntries(out, m.partitionType, entries, m.Version())
// }

// ManifestV2Builder is a helper for building a V2 manifest file
// struct which will conform to the ManifestFile interface.
type ManifestV2Builder struct {
	m *manifestFileV2
}

// NewManifestV2Builder is constructed with the primary fields, with the remaining
// fields set to their zero value unless modified by calling the corresponding
// methods of the builder. Then calling [ManifestV2Builder.Build] to retrieve the
// constructed ManifestFile.
func NewManifestV2Builder(path string, length int64, partitionSpecID int32, content ManifestContent, addedSnapshotID int64) *ManifestV2Builder {
	return &ManifestV2Builder{
		m: &manifestFileV2{
			Path:            path,
			Len:             length,
			SpecID:          partitionSpecID,
			Content:         content,
			AddedSnapshotID: addedSnapshotID,
		},
	}
}

func (b *ManifestV2Builder) PartitionSchema(st *StructType) *ManifestV2Builder {
	b.m.partitionType = st

	return b
}

func (b *ManifestV2Builder) SequenceNum(num, minSeqNum int64) *ManifestV2Builder {
	b.m.SeqNumber, b.m.MinSeqNumber = num, minSeqNum

	return b
}

func (b *ManifestV2Builder) AddedFiles(cnt int32) *ManifestV2Builder {
	b.m.AddedFilesCount = cnt

	return b
}

func (b *ManifestV2Builder) ExistingFiles(cnt int32) *ManifestV2Builder {
	b.m.ExistingFilesCount = cnt

	return b
}

func (b *ManifestV2Builder) DeletedFiles(cnt int32) *ManifestV2Builder {
	b.m.DeletedFilesCount = cnt

	return b
}

func (b *ManifestV2Builder) AddedRows(cnt int64) *ManifestV2Builder {
	b.m.AddedRowsCount = cnt

	return b
}

func (b *ManifestV2Builder) ExistingRows(cnt int64) *ManifestV2Builder {
	b.m.ExistingRowsCount = cnt

	return b
}

func (b *ManifestV2Builder) DeletedRows(cnt int64) *ManifestV2Builder {
	b.m.DeletedRowsCount = cnt

	return b
}

func (b *ManifestV2Builder) Partitions(p []FieldSummary) *ManifestV2Builder {
	b.m.PartitionList = &p

	return b
}

func (b *ManifestV2Builder) KeyMetadata(km []byte) *ManifestV2Builder {
	b.m.Key = km

	return b
}

// Build returns the constructed manifest file, after calling Build this
// builder should not be used further as we avoid copying by just returning
// a pointer to the constructed manifest file. Further calls to the modifier
// methods after calling build would modify the constructed ManifestFile.
func (b *ManifestV2Builder) Build() ManifestFile {
	if b.m.partitionType == nil {
		b.m.partitionType = UnpartitionedSpec.PartitionType(nil)
	}

	return b.m
}

type manifestFileV2 struct {
	partitionType      *StructType
	Path               string          `avro:"manifest_path"`
	Len                int64           `avro:"manifest_length"`
	SpecID             int32           `avro:"partition_spec_id"`
	Content            ManifestContent `avro:"content"`
	SeqNumber          int64           `avro:"sequence_number"`
	MinSeqNumber       int64           `avro:"min_sequence_number"`
	AddedSnapshotID    int64           `avro:"added_snapshot_id"`
	AddedFilesCount    int32           `avro:"added_files_count"`
	ExistingFilesCount int32           `avro:"existing_files_count"`
	DeletedFilesCount  int32           `avro:"deleted_files_count"`
	AddedRowsCount     int64           `avro:"added_rows_count"`
	ExistingRowsCount  int64           `avro:"existing_rows_count"`
	DeletedRowsCount   int64           `avro:"deleted_rows_count"`
	PartitionList      *[]FieldSummary `avro:"partitions"`
	Key                []byte          `avro:"key_metadata"`
}

func (*manifestFileV2) Version() int { return 2 }

func (m *manifestFileV2) FilePath() string                 { return m.Path }
func (m *manifestFileV2) Length() int64                    { return m.Len }
func (m *manifestFileV2) PartitionSpecID() int32           { return m.SpecID }
func (m *manifestFileV2) ManifestContent() ManifestContent { return m.Content }
func (m *manifestFileV2) SnapshotID() int64 {
	return m.AddedSnapshotID
}

func (m *manifestFileV2) AddedDataFiles() int32 {
	return m.AddedFilesCount
}

func (m *manifestFileV2) ExistingDataFiles() int32 {
	return m.ExistingFilesCount
}

func (m *manifestFileV2) DeletedDataFiles() int32 {
	return m.DeletedFilesCount
}

func (m *manifestFileV2) AddedRows() int64 {
	return m.AddedRowsCount
}

func (m *manifestFileV2) ExistingRows() int64 {
	return m.ExistingRowsCount
}

func (m *manifestFileV2) DeletedRows() int64 {
	return m.DeletedRowsCount
}

func (m *manifestFileV2) SequenceNum() int64    { return m.SeqNumber }
func (m *manifestFileV2) MinSequenceNum() int64 { return m.MinSeqNumber }
func (m *manifestFileV2) KeyMetadata() []byte   { return m.Key }

func (m *manifestFileV2) Partitions() []FieldSummary {
	if m.PartitionList == nil {
		return nil
	}

	return *m.PartitionList
}

func (m *manifestFileV2) HasAddedFiles() bool {
	return m.AddedFilesCount > 0
}

func (m *manifestFileV2) HasExistingFiles() bool {
	return m.ExistingFilesCount > 0
}

func (m *manifestFileV2) FetchEntries(fs iceio.IO, discardDeleted bool) ([]ManifestEntry, error) {
	return fetchManifestEntries(m, fs, discardDeleted)
}

// // WriteEntries writes a list of manifest entries to an avro file.
// func (m *manifestFileV2) WriteEntries(out io.Writer, entries []ManifestEntry) error {
// 	return writeManifestEntries(out, m.partitionType, entries, m.Version())
// }

func getFieldIDMap(sc avro.Schema) (map[string]int, map[int]avro.LogicalType) {
	getField := func(rs *avro.RecordSchema, name string) *avro.Field {
		for _, f := range rs.Fields() {
			if f.Name() == name {
				return f
			}
		}

		return nil
	}

	result := make(map[string]int)
	logicalTypes := make(map[int]avro.LogicalType)
	entryField := getField(sc.(*avro.RecordSchema), "data_file")
	partitionField := getField(entryField.Type().(*avro.RecordSchema), "partition")

	for _, field := range partitionField.Type().(*avro.RecordSchema).Fields() {
		if fid, ok := field.Prop("field-id").(float64); ok {
			result[field.Name()] = int(fid)
			avroTyp := field.Type()
			if us, ok := avroTyp.(*avro.UnionSchema); ok {
				for _, t := range us.Types() {
					avroTyp = t
				}
			}

			if ps, ok := avroTyp.(*avro.PrimitiveSchema); ok && ps.Logical() != nil {
				logicalTypes[int(fid)] = ps.Logical().Type()
			}
		}
	}

	return result, logicalTypes
}

type hasFieldToIDMap interface {
	setFieldNameToIDMap(map[string]int)
	setFieldIDToLogicalTypeMap(map[int]avro.LogicalType)
}

func fetchManifestEntries(m ManifestFile, fs iceio.IO, discardDeleted bool) ([]ManifestEntry, error) {
	f, err := fs.Open(m.FilePath())
	if err != nil {
		return nil, err
	}
	defer f.Close()

	dec, err := ocf.NewDecoder(f)
	if err != nil {
		return nil, err
	}

	metadata := dec.Metadata()
	sc, err := avro.ParseBytes(dec.Metadata()["avro.schema"])
	if err != nil {
		return nil, err
	}

	fieldNameToID, fieldIDToLogicalType := getFieldIDMap(sc)
	isVer1, isFallback := true, false
	if string(metadata["format-version"]) == "2" {
		isVer1 = false
	} else {
		for _, f := range sc.(*avro.RecordSchema).Fields() {
			if f.Name() == "snapshot_id" {
				if f.Type().Type() == avro.Union {
					isFallback = true
				}

				break
			}
		}
	}

	results := make([]ManifestEntry, 0)
	for dec.HasNext() {
		var tmp ManifestEntry
		if isVer1 {
			if isFallback {
				tmp = &fallbackManifestEntryV1{manifestEntryV1: manifestEntryV1{Data: &dataFile{}}}
			} else {
				tmp = &manifestEntryV1{Data: &dataFile{}}
			}
		} else {
			tmp = &manifestEntryV2{Data: &dataFile{}}
		}

		if err := dec.Decode(tmp); err != nil {
			return nil, err
		}

		if isFallback {
			tmp = tmp.(*fallbackManifestEntryV1).toEntry()
		}

		if !discardDeleted || tmp.Status() != EntryStatusDELETED {
			tmp.inheritSeqNum(m)
			if fieldToIDMap, ok := tmp.DataFile().(hasFieldToIDMap); ok {
				fieldToIDMap.setFieldNameToIDMap(fieldNameToID)
				fieldToIDMap.setFieldIDToLogicalTypeMap(fieldIDToLogicalType)
			}
			results = append(results, tmp)
		}
	}

	return results, dec.Error()
}

// ManifestFile is the interface which covers both V1 and V2 manifest files.
type ManifestFile interface {
	// Version returns the version number of this manifest file.
	// It should be 1 or 2.
	Version() int
	// FilePath is the location URI of this manifest file.
	FilePath() string
	// Length is the length in bytes of the manifest file.
	Length() int64
	// PartitionSpecID is the ID of the partition spec used to write
	// this manifest. It must be listed in the table metadata
	// partition-specs.
	PartitionSpecID() int32
	// ManifestContent is the type of files tracked by this manifest,
	// either data or delete files. All v1 manifests track data files.
	ManifestContent() ManifestContent
	// SnapshotID is the ID of the snapshot where this manifest file
	// was added.
	SnapshotID() int64
	// AddedDataFiles returns the number of entries in the manifest that
	// have the status of EntryStatusADDED.
	AddedDataFiles() int32
	// ExistingDataFiles returns the number of entries in the manifest
	// which have the status of EntryStatusEXISTING.
	ExistingDataFiles() int32
	// DeletedDataFiles returns the number of entries in the manifest
	// which have the status of EntryStatusDELETED.
	DeletedDataFiles() int32
	// AddedRows returns the number of rows in all files of the manifest
	// that have status EntryStatusADDED.
	AddedRows() int64
	// ExistingRows returns the number of rows in all files of the manifest
	// which have status EntryStatusEXISTING.
	ExistingRows() int64
	// DeletedRows returns the number of rows in all files of the manifest
	// which have status EntryStatusDELETED.
	DeletedRows() int64
	// SequenceNum returns the sequence number when this manifest was
	// added to the table. Will be 0 for v1 manifest lists.
	SequenceNum() int64
	// MinSequenceNum is the minimum data sequence number of all live data
	// or delete files in the manifest. Will be 0 for v1 manifest lists.
	MinSequenceNum() int64
	// KeyMetadata returns implementation-specific key metadata for encryption
	// if it exists in the manifest list.
	KeyMetadata() []byte
	// Partitions returns a list of field summaries for each partition
	// field in the spec. Each field in the list corresponds to a field in
	// the manifest file's partition spec.
	Partitions() []FieldSummary

	// HasAddedFiles returns true if AddedDataFiles > 0 or if it was null.
	HasAddedFiles() bool
	// HasExistingFiles returns true if ExistingDataFiles > 0 or if it was null.
	HasExistingFiles() bool
	// FetchEntries reads the manifest list file to fetch the list of
	// manifest entries using the provided file system IO interface.
	// If discardDeleted is true, entries for files containing deleted rows
	// will be skipped.
	FetchEntries(fs iceio.IO, discardDeleted bool) ([]ManifestEntry, error)
	// // WriteEntries writes a list of manifest entries to a provided
	// // io.Writer. The version of the manifest file is used to determine the
	// // schema to use for writing the entries.
	// WriteEntries(out io.Writer, entries []ManifestEntry) error
}

// ReadManifestList reads in an avro manifest list file and returns a slice
// of manifest files or an error if one is encountered.
func ReadManifestList(in io.Reader) ([]ManifestFile, error) {
	dec, err := ocf.NewDecoder(in)
	if err != nil {
		return nil, err
	}

	sc, err := avro.ParseBytes(dec.Metadata()["avro.schema"])
	if err != nil {
		return nil, err
	}

	var fallbackAddedSnapshot bool
	for _, f := range sc.(*avro.RecordSchema).Fields() {
		if f.Name() == "added_snapshot_id" {
			if f.Type().Type() == avro.Union {
				fallbackAddedSnapshot = true
			}

			break
		}
	}

	out := make([]ManifestFile, 0)
	for dec.HasNext() {
		var file ManifestFile
		if string(dec.Metadata()["format-version"]) == "2" {
			file = &manifestFileV2{}
		} else {
			if fallbackAddedSnapshot {
				file = &fallbackManifestFileV1{}
			} else {
				file = &manifestFileV1{}
			}
		}

		if err := dec.Decode(file); err != nil {
			return nil, err
		}

		if fallbackAddedSnapshot {
			file = file.(*fallbackManifestFileV1).toManifest()
		}

		out = append(out, file)
	}

	return out, dec.Error()
}

type writerImpl interface {
	content() ManifestContent
	prepareEntry(ManifestEntry, int64) (ManifestEntry, error)
}

type v1writerImpl struct{}

func (v1writerImpl) content() ManifestContent { return ManifestContentData }
func (v1writerImpl) prepareEntry(entry ManifestEntry, _ int64) (ManifestEntry, error) {
	return entry, nil
}

type v2writerImpl struct{}

func (v2writerImpl) content() ManifestContent { return ManifestContentData }
func (v2writerImpl) prepareEntry(entry ManifestEntry, snapshotID int64) (ManifestEntry, error) {
	if entry.SequenceNum() <= 0 {
		if entry.SnapshotID() > 0 && entry.SnapshotID() != snapshotID {
			return nil, fmt.Errorf("found unassigned sequence number for entry from snapshot: %d", entry.SnapshotID())
		}

		if entry.Status() != EntryStatusADDED {
			return nil, errors.New("only entries with status ADDED can be missing a sequence number")
		}
	}

	return entry, nil
}

type fieldStats interface {
	toSummary() FieldSummary
	update(value any)
}

type partitionFieldStats[T LiteralType] struct {
	containsNull bool
	containsNan  bool
	min          *T
	max          *T

	cmp Comparator[T]
}

func newPartitionFieldStat(typ PrimitiveType) fieldStats {
	switch typ.(type) {
	case Int32Type:
		return &partitionFieldStats[int32]{cmp: getComparator[int32]()}
	case Int64Type:
		return &partitionFieldStats[int64]{cmp: getComparator[int64]()}
	case Float32Type:
		return &partitionFieldStats[float32]{cmp: getComparator[float32]()}
	case Float64Type:
		return &partitionFieldStats[float64]{cmp: getComparator[float64]()}
	case StringType:
		return &partitionFieldStats[string]{cmp: getComparator[string]()}
	case DateType:
		return &partitionFieldStats[Date]{cmp: getComparator[Date]()}
	case TimeType:
		return &partitionFieldStats[Time]{cmp: getComparator[Time]()}
	case TimestampType:
		return &partitionFieldStats[Timestamp]{cmp: getComparator[Timestamp]()}
	case UUIDType:
		return &partitionFieldStats[uuid.UUID]{cmp: getComparator[uuid.UUID]()}
	case BinaryType:
		return &partitionFieldStats[[]byte]{cmp: getComparator[[]byte]()}
	case FixedType:
		return &partitionFieldStats[[]byte]{cmp: getComparator[[]byte]()}
	case DecimalType:
		return &partitionFieldStats[Decimal]{cmp: getComparator[Decimal]()}
	default:
		panic(fmt.Sprintf("expected primitive type for partition type: %s", typ))
	}
}

func (p *partitionFieldStats[T]) toSummary() FieldSummary {
	var (
		lowerBound *[]byte
		upperBound *[]byte
		lit        Literal
	)

	if p.min != nil {
		lit = NewLiteral(*p.min)
		lb, _ := lit.MarshalBinary()
		lowerBound = &lb
	}

	if p.max != nil {
		lit = NewLiteral(*p.max)
		ub, _ := lit.MarshalBinary()
		upperBound = &ub
	}

	return FieldSummary{
		ContainsNull: p.containsNull,
		ContainsNaN:  &p.containsNan,
		LowerBound:   lowerBound,
		UpperBound:   upperBound,
	}
}

func (p *partitionFieldStats[T]) update(value any) {
	if value == nil {
		p.containsNull = true

		return
	}

	var actualVal T
	v := reflect.ValueOf(value)
	if !v.CanConvert(reflect.TypeOf(actualVal)) {
		panic(fmt.Sprintf("expected type %T, got %T", actualVal, value))
	}

	actualVal = v.Convert(reflect.TypeOf(actualVal)).Interface().(T)

	switch f := any(actualVal).(type) {
	case float32:
		if math.IsNaN(float64(f)) {
			p.containsNan = true

			return
		}
	case float64:
		if math.IsNaN(f) {
			p.containsNan = true

			return
		}
	}

	if p.min == nil {
		p.min = &actualVal
		p.max = &actualVal
	} else {
		if p.cmp(actualVal, *p.min) < 0 {
			p.min = &actualVal
		}

		if p.cmp(actualVal, *p.max) > 0 {
			p.max = &actualVal
		}
	}
}

func constructPartitionSummaries(spec PartitionSpec, schema *Schema, partitions []map[string]any) ([]FieldSummary, error) {
	partType := spec.PartitionType(schema)
	fieldStats := make([]fieldStats, len(partType.FieldList))
	for i, field := range partType.FieldList {
		pt, ok := field.Type.(PrimitiveType)
		if !ok {
			return nil, fmt.Errorf("expected primitive type for partition field, got %s", field.Type)
		}

		fieldStats[i] = newPartitionFieldStat(pt)
	}

	for _, part := range partitions {
		for i, field := range partType.FieldList {
			fieldStats[i].update(part[field.Name])
		}
	}

	summaries := make([]FieldSummary, len(fieldStats))
	for i, stat := range fieldStats {
		summaries[i] = stat.toSummary()
	}

	return summaries, nil
}

type ManifestWriter struct {
	closed  bool
	version int
	impl    writerImpl

	output io.Writer
	writer *ocf.Encoder

	spec   PartitionSpec
	schema *Schema

	snapshotID    int64
	addedFiles    int32
	addedRows     int64
	existingFiles int32
	existingRows  int64
	deletedFiles  int32
	deletedRows   int64

	partitions  []map[string]any
	minSeqNum   int64
	reusedEntry ManifestEntry
}

func NewManifestWriter(version int, out io.Writer, spec PartitionSpec, schema *Schema, snapshotID int64) (*ManifestWriter, error) {
	var (
		impl        writerImpl
		reusedEntry ManifestEntry
	)

	switch version {
	case 1:
		impl = v1writerImpl{}
		reusedEntry = &manifestEntryV1{}
	case 2:
		impl = v2writerImpl{}
		reusedEntry = &manifestEntryV2{}
	default:
		return nil, fmt.Errorf("unsupported manifest version: %d", version)
	}

	sc, err := partitionTypeToAvroSchema(spec.PartitionType(schema))
	if err != nil {
		return nil, err
	}

	fileSchema, err := internal.NewManifestEntrySchema(sc, version)
	if err != nil {
		return nil, err
	}

	w := &ManifestWriter{
		impl:        impl,
		version:     version,
		output:      out,
		spec:        spec,
		schema:      schema,
		snapshotID:  snapshotID,
		minSeqNum:   -1,
		partitions:  make([]map[string]any, 0),
		reusedEntry: reusedEntry,
	}

	md, err := w.meta()
	if err != nil {
		return nil, err
	}

	enc, err := ocf.NewEncoderWithSchema(fileSchema, out,
		ocf.WithMetadata(md),
		ocf.WithCodec(ocf.Deflate))

	w.writer = enc

	return w, err
}

func (w *ManifestWriter) Close() error {
	if w.closed {
		return nil
	}

	if w.addedFiles+w.existingFiles+w.deletedFiles == 0 {
		return errors.New("empty manifest file has been written")
	}

	w.closed = true

	return w.writer.Close()
}

func (w *ManifestWriter) ToManifestFile(location string, length int64) (ManifestFile, error) {
	if err := w.Close(); err != nil {
		return nil, err
	}

	if w.minSeqNum == 0 {
		w.minSeqNum = -1
	}

	partitions, err := constructPartitionSummaries(w.spec, w.schema, w.partitions)
	if err != nil {
		return nil, err
	}

	switch w.version {
	case 1:
		return &manifestFileV1{
			Path:               location,
			Len:                length,
			SpecID:             int32(w.spec.id),
			AddedSnapshotID:    w.snapshotID,
			AddedFilesCount:    &w.addedFiles,
			ExistingFilesCount: &w.existingFiles,
			DeletedFilesCount:  &w.deletedFiles,
			AddedRowsCount:     &w.addedRows,
			ExistingRowsCount:  &w.existingRows,
			DeletedRowsCount:   &w.deletedRows,
			PartitionList:      &partitions,
			Key:                nil,
		}, nil
	case 2:
		return &manifestFileV2{
			Path:               location,
			Len:                length,
			SpecID:             int32(w.spec.id),
			Content:            ManifestContentData,
			SeqNumber:          -1,
			MinSeqNumber:       w.minSeqNum,
			AddedSnapshotID:    w.snapshotID,
			AddedFilesCount:    w.addedFiles,
			ExistingFilesCount: w.existingFiles,
			DeletedFilesCount:  w.deletedFiles,
			AddedRowsCount:     w.addedRows,
			ExistingRowsCount:  w.existingRows,
			DeletedRowsCount:   w.deletedRows,
			PartitionList:      &partitions,
			Key:                nil,
		}, nil
	default:
		return nil, fmt.Errorf("unsupported manifest version: %d", w.version)
	}
}

func (w *ManifestWriter) meta() (map[string][]byte, error) {
	schemaJson, err := json.Marshal(w.schema)
	if err != nil {
		return nil, err
	}

	specFieldsJson, err := json.Marshal(w.spec.fields)
	if err != nil {
		return nil, err
	}

	return map[string][]byte{
		"schema":            schemaJson,
		"schema-id":         []byte(strconv.Itoa(w.schema.ID)),
		"partition-spec":    specFieldsJson,
		"partition-spec-id": []byte(strconv.Itoa(w.spec.ID())),
		"format-version":    []byte(strconv.Itoa(w.version)),
		"content":           []byte(w.impl.content().String()),
	}, nil
}

func (w *ManifestWriter) addEntry(entry ManifestEntry) error {
	if w.closed {
		return errors.New("cannot add entry to closed manifest writer")
	}

	switch entry.Status() {
	case EntryStatusADDED:
		w.addedFiles++
		w.addedRows += entry.DataFile().Count()
	case EntryStatusEXISTING:
		w.existingFiles++
		w.existingRows += entry.DataFile().Count()
	case EntryStatusDELETED:
		w.deletedFiles++
		w.deletedRows += entry.DataFile().Count()
	default:
		return fmt.Errorf("unknown entry status: %v", entry.Status())
	}

	w.partitions = append(w.partitions, entry.DataFile().Partition())
	if (entry.Status() == EntryStatusADDED || entry.Status() == EntryStatusEXISTING) &&
		entry.SequenceNum() > 0 && (w.minSeqNum < 0 || entry.SequenceNum() < w.minSeqNum) {
		w.minSeqNum = entry.SequenceNum()
	}

	entry, err := w.impl.prepareEntry(entry, w.snapshotID)
	if err != nil {
		return err
	}

	return w.writer.Encode(entry)
}

func (w *ManifestWriter) Add(entry ManifestEntry) error {
	w.reusedEntry.wrap(EntryStatusADDED, w.snapshotID, entry.SequenceNum(), nil, entry.DataFile())

	return w.addEntry(w.reusedEntry)
}

func (w *ManifestWriter) Delete(entry ManifestEntry) error {
	w.reusedEntry.wrap(EntryStatusDELETED, w.snapshotID, entry.SequenceNum(), entry.FileSequenceNum(), entry.DataFile())

	return w.addEntry(w.reusedEntry)
}

func (w *ManifestWriter) Existing(entry ManifestEntry) error {
	w.reusedEntry.wrap(EntryStatusEXISTING, w.snapshotID, entry.SequenceNum(), entry.FileSequenceNum(), entry.DataFile())

	return w.addEntry(w.reusedEntry)
}

type ManifestListWriter struct {
	version          int
	out              io.Writer
	commitSnapshotID int64
	sequenceNumber   int64
	writer           *ocf.Encoder
}

func NewManifestListWriterV1(out io.Writer, snapshotID int64, parentSnapshot *int64) (*ManifestListWriter, error) {
	m := &ManifestListWriter{
		version:          1,
		out:              out,
		commitSnapshotID: snapshotID,
		sequenceNumber:   -1,
	}

	parentSnapshotStr := "null"
	if parentSnapshot != nil {
		parentSnapshotStr = strconv.Itoa(int(*parentSnapshot))
	}

	return m, m.init(map[string][]byte{
		"format-version":     []byte(strconv.Itoa(m.version)),
		"snapshot-id":        []byte(strconv.Itoa(int(snapshotID))),
		"parent-snapshot-id": []byte(parentSnapshotStr),
	})
}

func NewManifestListWriterV2(out io.Writer, snapshotID, sequenceNumber int64, parentSnapshot *int64) (*ManifestListWriter, error) {
	m := &ManifestListWriter{
		version:          2,
		out:              out,
		commitSnapshotID: snapshotID,
		sequenceNumber:   sequenceNumber,
	}

	parentSnapshotStr := "null"
	if parentSnapshot != nil {
		parentSnapshotStr = strconv.Itoa(int(*parentSnapshot))
	}

	return m, m.init(map[string][]byte{
		"format-version":     []byte(strconv.Itoa(m.version)),
		"snapshot-id":        []byte(strconv.Itoa(int(snapshotID))),
		"sequence-number":    []byte(strconv.Itoa(int(sequenceNumber))),
		"parent-snapshot-id": []byte(parentSnapshotStr),
	})
}

func (m *ManifestListWriter) init(meta map[string][]byte) error {
	fileSchema, err := internal.NewManifestFileSchema(m.version)
	if err != nil {
		return err
	}

	enc, err := ocf.NewEncoderWithSchema(fileSchema, m.out,
		ocf.WithMetadata(meta),
		ocf.WithCodec(ocf.Deflate))
	if err != nil {
		return err
	}

	m.writer = enc

	return nil
}

func (m *ManifestListWriter) Close() error {
	if m.writer == nil {
		return nil
	}

	return m.writer.Close()
}

func (m *ManifestListWriter) AddManifests(files []ManifestFile) error {
	if len(files) == 0 {
		return nil
	}

	switch m.version {
	case 1:
		if slices.ContainsFunc(files, func(f ManifestFile) bool {
			return f.Version() != 1
		}) {
			return fmt.Errorf("%w: ManifestListWriter only supports version 1 manifest files", ErrInvalidArgument)
		}

		for _, file := range files {
			if err := m.writer.Encode(file); err != nil {
				return err
			}
		}

	case 2:
		for _, file := range files {
			if file.Version() != 2 {
				return fmt.Errorf("%w: ManifestListWriter only supports version 2 manifest files", ErrInvalidArgument)
			}

			wrapped := *(file.(*manifestFileV2))
			if wrapped.SeqNumber == -1 {
				// if the sequence number is being assigned here,
				// then the manifest must be created by the current
				// operation.
				// to validate this, check the snapshot id matches the current commmit
				if m.commitSnapshotID != wrapped.AddedSnapshotID {
					return fmt.Errorf("found unassigned sequence number for a manifest from snapshot %d != %d",
						m.commitSnapshotID, wrapped.AddedSnapshotID)
				}
				wrapped.SeqNumber = m.sequenceNumber
			}

			if wrapped.MinSeqNumber == -1 {
				if m.commitSnapshotID != wrapped.AddedSnapshotID {
					return fmt.Errorf("found unassigned sequence number for a manifest from snapshot: %d", wrapped.AddedSnapshotID)
				}
				// if the min sequence number is not determined, then there was no assigned sequence number
				// for any file written to the wrapped manifest. replace the unassigned sequence number with
				// the one for this commit
				wrapped.MinSeqNumber = m.sequenceNumber
			}
			if err := m.writer.Encode(wrapped); err != nil {
				return err
			}
		}
	default:
		return fmt.Errorf("unsupported manifest version: %d", m.version)
	}

	return nil
}

// WriteManifestList writes a list of manifest files to an avro file.
func WriteManifestList(version int, out io.Writer, snapshotID int64, parentSnapshotID, sequenceNumber *int64, files []ManifestFile) error {
	var (
		writer *ManifestListWriter
		err    error
	)

	switch version {
	case 1:
		writer, err = NewManifestListWriterV1(out, snapshotID, parentSnapshotID)
	case 2:
		if sequenceNumber == nil {
			return errors.New("sequence number is required for V2 tables")
		}
		writer, err = NewManifestListWriterV2(out, snapshotID, *sequenceNumber, parentSnapshotID)
	default:
		return fmt.Errorf("unsupported manifest version: %d", version)
	}

	if err != nil {
		return err
	}

	if err = writer.AddManifests(files); err != nil {
		return err
	}

	return writer.Close()
}

func WriteManifest(
	out io.Writer,
	version int,
	spec PartitionSpec,
	schema *Schema,
	snapshotID int64,
	entries []ManifestEntry,
) (func(string, int64) (ManifestFile, error), error) {
	w, err := NewManifestWriter(version, out, spec, schema, snapshotID)
	if err != nil {
		return nil, err
	}

	for _, entry := range entries {
		if err := w.addEntry(entry); err != nil {
			return nil, err
		}
	}

	return w.ToManifestFile, w.Close()
}

// ManifestEntryStatus defines constants for the entry status of
// existing, added or deleted.
type ManifestEntryStatus int8

const (
	EntryStatusEXISTING ManifestEntryStatus = 0
	EntryStatusADDED    ManifestEntryStatus = 1
	EntryStatusDELETED  ManifestEntryStatus = 2
)

// ManifestEntryContent defines constants for the type of file contents
// in the file entries. Data, Position based deletes and equality based
// deletes.
type ManifestEntryContent int8

const (
	EntryContentData       ManifestEntryContent = 0
	EntryContentPosDeletes ManifestEntryContent = 1
	EntryContentEqDeletes  ManifestEntryContent = 2
)

func (m ManifestEntryContent) String() string {
	switch m {
	case EntryContentData:
		return "Data"
	case EntryContentPosDeletes:
		return "Positional_Deletes"
	case EntryContentEqDeletes:
		return "Equality_Deletes"
	default:
		return "UNKNOWN"
	}
}

// FileFormat defines constants for the format of data files.
type FileFormat string

const (
	AvroFile    FileFormat = "AVRO"
	OrcFile     FileFormat = "ORC"
	ParquetFile FileFormat = "PARQUET"
)

type colMap[K, V any] struct {
	Key   K `avro:"key"`
	Value V `avro:"value"`
}

func avroColMapToMap[K comparable, V any](c *[]colMap[K, V]) map[K]V {
	if c == nil {
		return nil
	}

	out := make(map[K]V)
	for _, data := range *c {
		out[data.Key] = data.Value
	}

	return out
}

func mapToAvroColMap[K comparable, V any](m map[K]V) *[]colMap[K, V] {
	if m == nil {
		return nil
	}

	out := make([]colMap[K, V], 0, len(m))
	for k, v := range m {
		out = append(out, colMap[K, V]{Key: k, Value: v})
	}

	return &out
}

func avroPartitionData(input map[string]any, nameToID map[string]int, logicalTypes map[int]avro.LogicalType) map[string]any {
	out := make(map[string]any)
	for k, v := range input {
		if id, ok := nameToID[k]; ok {
			if logical, ok := logicalTypes[id]; ok {
				switch logical {
				case avro.Date:
					out[k] = Date(v.(time.Time).Truncate(24*time.Hour).Unix() / int64((time.Hour * 24).Seconds()))
				case avro.TimeMillis:
					out[k] = Time(v.(time.Duration).Milliseconds())
				case avro.TimeMicros:
					out[k] = Time(v.(time.Duration).Microseconds())
				case avro.TimestampMillis:
					out[k] = Timestamp(v.(time.Time).UTC().UnixMilli())
				case avro.TimestampMicros:
					out[k] = Timestamp(v.(time.Time).UTC().UnixMicro())
				default:
					out[k] = v
				}

				continue
			}
		}
		out[k] = v
	}

	return out
}

type dataFile struct {
	Content          ManifestEntryContent   `avro:"content"`
	Path             string                 `avro:"file_path"`
	Format           FileFormat             `avro:"file_format"`
	PartitionData    map[string]any         `avro:"partition"`
	RecordCount      int64                  `avro:"record_count"`
	FileSize         int64                  `avro:"file_size_in_bytes"`
	BlockSizeInBytes int64                  `avro:"block_size_in_bytes"`
	ColSizes         *[]colMap[int, int64]  `avro:"column_sizes"`
	ValCounts        *[]colMap[int, int64]  `avro:"value_counts"`
	NullCounts       *[]colMap[int, int64]  `avro:"null_value_counts"`
	NaNCounts        *[]colMap[int, int64]  `avro:"nan_value_counts"`
	DistinctCounts   *[]colMap[int, int64]  `avro:"distinct_counts"`
	LowerBounds      *[]colMap[int, []byte] `avro:"lower_bounds"`
	UpperBounds      *[]colMap[int, []byte] `avro:"upper_bounds"`
	Key              *[]byte                `avro:"key_metadata"`
	Splits           *[]int64               `avro:"split_offsets"`
	EqualityIDs      *[]int                 `avro:"equality_ids"`
	SortOrder        *int                   `avro:"sort_order_id"`

	colSizeMap     map[int]int64
	valCntMap      map[int]int64
	nullCntMap     map[int]int64
	nanCntMap      map[int]int64
	distinctCntMap map[int]int64
	lowerBoundMap  map[int][]byte
	upperBoundMap  map[int][]byte

	// not used for anything yet, but important to maintain the information
	// for future development and updates such as when we get to writes,
	// and scan planning
	fieldNameToID        map[string]int
	fieldIDToLogicalType map[int]avro.LogicalType

	initMaps sync.Once
}

func (d *dataFile) initializeMapData() {
	d.initMaps.Do(func() {
		d.colSizeMap = avroColMapToMap(d.ColSizes)
		d.valCntMap = avroColMapToMap(d.ValCounts)
		d.nullCntMap = avroColMapToMap(d.NullCounts)
		d.nanCntMap = avroColMapToMap(d.NaNCounts)
		d.distinctCntMap = avroColMapToMap(d.DistinctCounts)
		d.lowerBoundMap = avroColMapToMap(d.LowerBounds)
		d.upperBoundMap = avroColMapToMap(d.UpperBounds)
		d.PartitionData = avroPartitionData(d.PartitionData, d.fieldNameToID, d.fieldIDToLogicalType)
	})
}

func (d *dataFile) setFieldNameToIDMap(m map[string]int) { d.fieldNameToID = m }
func (d *dataFile) setFieldIDToLogicalTypeMap(m map[int]avro.LogicalType) {
	d.fieldIDToLogicalType = m
}

func (d *dataFile) ContentType() ManifestEntryContent { return d.Content }
func (d *dataFile) FilePath() string                  { return d.Path }
func (d *dataFile) FileFormat() FileFormat            { return d.Format }
func (d *dataFile) Partition() map[string]any {
	d.initializeMapData()

	return d.PartitionData
}

func (d *dataFile) Count() int64         { return d.RecordCount }
func (d *dataFile) FileSizeBytes() int64 { return d.FileSize }

func (d *dataFile) ColumnSizes() map[int]int64 {
	d.initializeMapData()

	return d.colSizeMap
}

func (d *dataFile) ValueCounts() map[int]int64 {
	d.initializeMapData()

	return d.valCntMap
}

func (d *dataFile) NullValueCounts() map[int]int64 {
	d.initializeMapData()

	return d.nullCntMap
}

func (d *dataFile) NaNValueCounts() map[int]int64 {
	d.initializeMapData()

	return d.nanCntMap
}

func (d *dataFile) DistinctValueCounts() map[int]int64 {
	d.initializeMapData()

	return d.distinctCntMap
}

func (d *dataFile) LowerBoundValues() map[int][]byte {
	d.initializeMapData()

	return d.lowerBoundMap
}

func (d *dataFile) UpperBoundValues() map[int][]byte {
	d.initializeMapData()

	return d.upperBoundMap
}

func (d *dataFile) KeyMetadata() []byte {
	if d.Key == nil {
		return nil
	}

	return *d.Key
}

func (d *dataFile) SplitOffsets() []int64 {
	if d.Splits == nil {
		return nil
	}

	return *d.Splits
}

func (d *dataFile) EqualityFieldIDs() []int {
	if d.EqualityIDs == nil {
		return nil
	}

	return *d.EqualityIDs
}

func (d *dataFile) SortOrderID() *int { return d.SortOrder }

// ManifestEntryV1Builder is a helper for building a V1 manifest entry
// struct which will conform to the ManifestEntry interface.
type ManifestEntryV1Builder struct {
	m *manifestEntryV1
}

// NewManifestEntryV1Builder is passed all of the required fields and then allows
// all of the optional fields to be set by calling the corresponding methods
// before calling [ManifestEntryV1Builder.Build] to construct the object.
func NewManifestEntryV1Builder(status ManifestEntryStatus, snapshotID int64, data DataFile) (*ManifestEntryV1Builder, error) {
	return &ManifestEntryV1Builder{
		m: &manifestEntryV1{
			EntryStatus: status,
			Snapshot:    snapshotID,
			Data:        data,
		},
	}, nil
}

func (b *ManifestEntryV1Builder) Build() ManifestEntry {
	return b.m
}

type manifestEntryV1 struct {
	EntryStatus ManifestEntryStatus `avro:"status"`
	Snapshot    int64               `avro:"snapshot_id"`
	SeqNum      *int64
	FileSeqNum  *int64
	Data        DataFile `avro:"data_file"`
}

type fallbackManifestEntryV1 struct {
	manifestEntryV1
	Snapshot *int64 `avro:"snapshot_id"`
}

func (f *fallbackManifestEntryV1) toEntry() *manifestEntryV1 {
	f.manifestEntryV1.Snapshot = *f.Snapshot

	return &f.manifestEntryV1
}

func (m *manifestEntryV1) inheritSeqNum(manifest ManifestFile) {}

func (m *manifestEntryV1) Status() ManifestEntryStatus { return m.EntryStatus }
func (m *manifestEntryV1) SnapshotID() int64           { return m.Snapshot }

func (m *manifestEntryV1) SequenceNum() int64 {
	if m.SeqNum == nil {
		return 0
	}

	return *m.SeqNum
}

func (m *manifestEntryV1) FileSequenceNum() *int64 {
	return m.FileSeqNum
}

func (m *manifestEntryV1) DataFile() DataFile { return m.Data }

func (m *manifestEntryV1) wrap(status ManifestEntryStatus, snapshotID int64, seqNum int64, fileSeqNum *int64, datafile DataFile) ManifestEntry {
	m.EntryStatus = status
	m.Snapshot = snapshotID
	if seqNum > 0 {
		m.SeqNum = &seqNum
	}

	m.FileSeqNum = fileSeqNum

	m.Data = datafile

	return m
}

// ManifestEntryV2Builder is a helper for building a V2 manifest entry
// struct which will conform to the ManifestEntry interface.
type ManifestEntryV2Builder struct {
	m *manifestEntryV2
}

// NewManifestEntryV2Builder is passed all of the required fields and then allows
// all of the optional fields to be set by calling the corresponding methods
// before calling [ManifestEntryV2Builder.Build] to construct the object.
func NewManifestEntryV2Builder(status ManifestEntryStatus, snapshotID int64, data DataFile) *ManifestEntryV2Builder {
	return &ManifestEntryV2Builder{
		m: &manifestEntryV2{
			EntryStatus: status,
			Snapshot:    &snapshotID,
			Data:        data,
		},
	}
}

// SequenceNum sets the sequence number for the manifest entry.
func (b *ManifestEntryV2Builder) SequenceNum(num int64) *ManifestEntryV2Builder {
	b.m.SeqNum = &num

	return b
}

// FileSequenceNum sets the file sequence number for the manifest entry.
func (b *ManifestEntryV2Builder) FileSequenceNum(num int64) *ManifestEntryV2Builder {
	b.m.FileSeqNum = &num

	return b
}

// Build returns the constructed manifest entry, after calling Build this
// builder should not be used further as we avoid copying by just returning
// a pointer to the constructed manifest entry. Further calls to the modifier
// methods after calling build would modify the constructed ManifestEntry.
func (b *ManifestEntryV2Builder) Build() ManifestEntry {
	return b.m
}

type manifestEntryV2 struct {
	EntryStatus ManifestEntryStatus `avro:"status"`
	Snapshot    *int64              `avro:"snapshot_id"`
	SeqNum      *int64              `avro:"sequence_number"`
	FileSeqNum  *int64              `avro:"file_sequence_number"`
	Data        DataFile            `avro:"data_file"`
}

func (m *manifestEntryV2) inheritSeqNum(manifest ManifestFile) {
	if m.Snapshot == nil {
		snap := manifest.SnapshotID()
		m.Snapshot = &snap
	}

	manifestSequenceNum := manifest.SequenceNum()
	if m.SeqNum == nil && (manifestSequenceNum == 0 || m.EntryStatus == EntryStatusADDED) {
		m.SeqNum = &manifestSequenceNum
	}

	if m.FileSeqNum == nil && (manifestSequenceNum == 0 || m.EntryStatus == EntryStatusADDED) {
		m.FileSeqNum = &manifestSequenceNum
	}
}

func (m *manifestEntryV2) Status() ManifestEntryStatus { return m.EntryStatus }
func (m *manifestEntryV2) SnapshotID() int64 {
	if m.Snapshot == nil {
		return 0
	}

	return *m.Snapshot
}

func (m *manifestEntryV2) SequenceNum() int64 {
	if m.SeqNum == nil {
		return 0
	}

	return *m.SeqNum
}

func (m *manifestEntryV2) FileSequenceNum() *int64 {
	return m.FileSeqNum
}

func (m *manifestEntryV2) DataFile() DataFile { return m.Data }

func (m *manifestEntryV2) wrap(status ManifestEntryStatus, snapshotID int64, seqNum int64, fileSeqNum *int64, datafile DataFile) ManifestEntry {
	m.EntryStatus = status
	if snapshotID > 0 {
		m.Snapshot = &snapshotID
	}
	if seqNum > 0 {
		m.SeqNum = &seqNum
	}
	m.FileSeqNum = fileSeqNum

	m.Data = datafile

	return m
}

// DataFileBuilder is a helper for building a data file struct which will
// conform to the DataFile interface.
type DataFileBuilder struct {
	d *dataFile
}

// NewDataFileBuilder is passed all of the required fields and then allows
// all of the optional fields to be set by calling the corresponding methods
// before calling [DataFileBuilder.Build] to construct the object.
func NewDataFileBuilder(
	content ManifestEntryContent,
	path string,
	format FileFormat,
	partitionData map[string]any,
	recordCount int64,
	fileSize int64,
) (*DataFileBuilder, error) {
	if content != EntryContentData && content != EntryContentPosDeletes && content != EntryContentEqDeletes {
		return nil, fmt.Errorf(
			"%w: content must be one of %s, %s, or %s",
			ErrInvalidArgument, EntryContentData, EntryContentPosDeletes, EntryContentEqDeletes,
		)
	}

	if path == "" {
		return nil, fmt.Errorf("%w: path cannot be empty", ErrInvalidArgument)
	}

	if format != AvroFile && format != OrcFile && format != ParquetFile {
		return nil, fmt.Errorf(
			"%w: format must be one of %s, %s, or %s",
			ErrInvalidArgument, AvroFile, OrcFile, ParquetFile,
		)
	}

	if recordCount <= 0 {
		return nil, fmt.Errorf("%w: record count must be greater than 0", ErrInvalidArgument)
	}

	if fileSize <= 0 {
		return nil, fmt.Errorf("%w: file size must be greater than 0", ErrInvalidArgument)
	}

	return &DataFileBuilder{
		d: &dataFile{
			Content:       content,
			Path:          path,
			Format:        format,
			PartitionData: partitionData,
			RecordCount:   recordCount,
			FileSize:      fileSize,
		},
	}, nil
}

// BlockSizeInBytes sets the block size in bytes for the data file. Deprecated in v2.
func (b *DataFileBuilder) BlockSizeInBytes(size int64) *DataFileBuilder {
	b.d.BlockSizeInBytes = size

	return b
}

// ColumnSizes sets the column sizes for the data file.
func (b *DataFileBuilder) ColumnSizes(sizes map[int]int64) *DataFileBuilder {
	b.d.ColSizes = mapToAvroColMap(sizes)

	return b
}

// ValueCounts sets the value counts for the data file.
func (b *DataFileBuilder) ValueCounts(counts map[int]int64) *DataFileBuilder {
	b.d.ValCounts = mapToAvroColMap(counts)

	return b
}

// NullValueCounts sets the null value counts for the data file.
func (b *DataFileBuilder) NullValueCounts(counts map[int]int64) *DataFileBuilder {
	b.d.NullCounts = mapToAvroColMap(counts)

	return b
}

// NaNValueCounts sets the NaN value counts for the data file.
func (b *DataFileBuilder) NaNValueCounts(counts map[int]int64) *DataFileBuilder {
	b.d.NaNCounts = mapToAvroColMap(counts)

	return b
}

// DistinctValueCounts sets the distinct value counts for the data file.
func (b *DataFileBuilder) DistinctValueCounts(counts map[int]int64) *DataFileBuilder {
	b.d.DistinctCounts = mapToAvroColMap(counts)

	return b
}

// LowerBoundValues sets the lower bound values for the data file.
func (b *DataFileBuilder) LowerBoundValues(bounds map[int][]byte) *DataFileBuilder {
	b.d.LowerBounds = mapToAvroColMap(bounds)

	return b
}

// UpperBoundValues sets the upper bound values for the data file.
func (b *DataFileBuilder) UpperBoundValues(bounds map[int][]byte) *DataFileBuilder {
	b.d.UpperBounds = mapToAvroColMap(bounds)

	return b
}

// KeyMetadata sets the key metadata for the data file.
func (b *DataFileBuilder) KeyMetadata(key []byte) *DataFileBuilder {
	b.d.Key = &key

	return b
}

// SplitOffsets sets the split offsets for the data file.
func (b *DataFileBuilder) SplitOffsets(offsets []int64) *DataFileBuilder {
	b.d.Splits = &offsets

	return b
}

// EqualityFieldIDs sets the equality field ids for the data file.
func (b *DataFileBuilder) EqualityFieldIDs(ids []int) *DataFileBuilder {
	b.d.EqualityIDs = &ids

	return b
}

// SortOrderID sets the sort order id for the data file.
func (b *DataFileBuilder) SortOrderID(id int) *DataFileBuilder {
	b.d.SortOrder = &id

	return b
}

func (b *DataFileBuilder) Build() DataFile {
	return b.d
}

// DataFile is the interface for reading the information about a
// given data file indicated by an entry in a manifest list.
type DataFile interface {
	// ContentType is the type of the content stored by the data file,
	// either Data, Equality deletes, or Position deletes. All v1 files
	// are Data files.
	ContentType() ManifestEntryContent
	// FilePath is the full URI for the file, complete with FS scheme.
	FilePath() string
	// FileFormat is the format of the data file, AVRO, Orc, or Parquet.
	FileFormat() FileFormat
	// Partition returns a mapping of field name to partition value for
	// each of the partition spec's fields.
	Partition() map[string]any
	// Count returns the number of records in this file.
	Count() int64
	// FileSizeBytes is the total file size in bytes.
	FileSizeBytes() int64
	// ColumnSizes is a mapping from column id to the total size on disk
	// of all regions that store the column. Does not include bytes
	// necessary to read other columns, like footers. Map will be nil for
	// row-oriented formats (avro).
	ColumnSizes() map[int]int64
	// ValueCounts is a mapping from column id to the number of values
	// in the column, including null and NaN values.
	ValueCounts() map[int]int64
	// NullValueCounts is a mapping from column id to the number of
	// null values in the column.
	NullValueCounts() map[int]int64
	// NaNValueCounts is a mapping from column id to the number of NaN
	// values in the column.
	NaNValueCounts() map[int]int64
	// DistictValueCounts is a mapping from column id to the number of
	// distinct values in the column. Distinct counts must be derived
	// using values in the file by counting or using sketches, but not
	// using methods like merging existing distinct counts.
	DistinctValueCounts() map[int]int64
	// LowerBoundValues is a mapping from column id to the lower bounded
	// value of the column, serialized as binary. Each value in the column
	// must be less than or requal to all non-null, non-NaN values in the
	// column for the file.
	LowerBoundValues() map[int][]byte
	// UpperBoundValues is a mapping from column id to the upper bounded
	// value of the column, serialized as binary. Each value in the column
	// must be greater than or equal to all non-null, non-NaN values in
	// the column for the file.
	UpperBoundValues() map[int][]byte
	// KeyMetadata is implementation-specific key metadata for encryption.
	KeyMetadata() []byte
	// SplitOffsets are the split offsets for the data file. For example,
	// all row group offsets in a Parquet file. Must be sorted ascending.
	SplitOffsets() []int64
	// EqualityFieldIDs are used to determine row equality in equality
	// delete files. It is required when the content type is
	// EntryContentEqDeletes.
	EqualityFieldIDs() []int
	// SortOrderID returns the id representing the sort order for this
	// file, or nil if there is no sort order.
	SortOrderID() *int
}

// ManifestEntry is an interface for both v1 and v2 manifest entries.
type ManifestEntry interface {
	// Status returns the type of the file tracked by this entry.
	// Deletes are informational only and not used in scans.
	Status() ManifestEntryStatus
	// SnapshotID is the id where the file was added, or deleted,
	// if null it is inherited from the manifest list.
	SnapshotID() int64
	// SequenceNum returns the data sequence number of the file.
	// If it was null and the status is EntryStatusADDED then it
	// is inherited from the manifest list.
	SequenceNum() int64
	// FileSequenceNum returns the file sequence number indicating
	// when the file was added. If it was null and the status is
	// EntryStatusADDED then it is inherited from the manifest list.
	FileSequenceNum() *int64
	// DataFile provides the information about the data file indicated
	// by this manifest entry.
	DataFile() DataFile

	inheritSeqNum(manifest ManifestFile)
	wrap(status ManifestEntryStatus, snapshotID int64, seqNum int64, fileSeqNum *int64, datafile DataFile) ManifestEntry
}

var PositionalDeleteSchema = NewSchema(0,
	NestedField{ID: 2147483546, Type: PrimitiveTypes.String, Name: "file_path", Required: true},
	NestedField{ID: 2147483545, Type: PrimitiveTypes.Int32, Name: "pos", Required: true},
)
