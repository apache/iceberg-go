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
	"context"
	"encoding/json"
	"fmt"
	"io"
	"sync"

	"github.com/hamba/avro/v2"
	"github.com/hamba/avro/v2/ocf"
	"github.com/thanos-io/objstore"
)

// ManifestContent indicates the type of data inside of the files
// described by a manifest. This will indicate whether the data files
// contain active data or deleted rows.
type ManifestContent int32

const (
	ManifestContentData    ManifestContent = 0
	ManifestContentDeletes ManifestContent = 1
)

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
	Path               string          `avro:"manifest_path"`
	Len                int64           `avro:"manifest_length"`
	SpecID             int32           `avro:"partition_spec_id"`
	AddedSnapshotID    int64           `avro:"added_snapshot_id"`
	AddedFilesCount    *int32          `avro:"added_data_files_count"`
	ExistingFilesCount *int32          `avro:"existing_data_files_count"`
	DeletedFilesCount  *int32          `avro:"deleted_data_files_count"`
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

func (m *manifestFileV1) FetchEntries(bucket objstore.Bucket, discardDeleted bool) ([]ManifestEntry, *Schema, error) {
	return fetchManifestEntries(m, bucket, discardDeleted)
}

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
	return b.m
}

type manifestFileV2 struct {
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

func (m *manifestFileV2) FetchEntries(bucket objstore.Bucket, discardDeleted bool) ([]ManifestEntry, *Schema, error) {
	return fetchManifestEntries(m, bucket, discardDeleted)
}

func fetchManifestEntries(m ManifestFile, bucket objstore.Bucket, discardDeleted bool) ([]ManifestEntry, *Schema, error) {
	f, err := bucket.Get(context.TODO(), m.FilePath())
	if err != nil {
		return nil, nil, err
	}
	defer f.Close()

	dec, err := ocf.NewDecoder(f)
	if err != nil {
		return nil, nil, err
	}

	// Extract the table shcmema from the metadata
	schema := &Schema{}
	metadata := dec.Metadata()
	if err := json.Unmarshal(metadata["schema"], schema); err != nil {
		return nil, nil, fmt.Errorf("failed to unmarshal schema: %w", err)
	}

	isVer1, isFallback := true, false
	if string(metadata["format-version"]) == "2" {
		isVer1 = false
	} else {
		sc, err := avro.ParseBytes(dec.Metadata()["avro.schema"])
		if err != nil {
			return nil, nil, err
		}

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
				tmp = &fallbackManifestEntryV1{
					manifestEntryV1: manifestEntryV1{
						Data: &dataFile{},
					},
				}
			} else {
				tmp = &manifestEntryV1{
					Data: &dataFile{},
				}
			}
		} else {
			tmp = &manifestEntryV2{
				Data: &dataFile{},
			}
		}

		if err := dec.Decode(tmp); err != nil {
			return nil, nil, err
		}

		if isFallback {
			tmp = tmp.(*fallbackManifestEntryV1).toEntry()
		}

		if !discardDeleted || tmp.Status() != EntryStatusDELETED {
			tmp.inheritSeqNum(m)
			results = append(results, tmp)
		}
	}

	return results, schema, dec.Error()
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
	// manifest entries using the provided bucket. It will return the schema of the table
	// when the manifest was written.
	// If discardDeleted is true, entries for files containing deleted rows
	// will be skipped.
	FetchEntries(bucket objstore.Bucket, discardDeleted bool) ([]ManifestEntry, *Schema, error)
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

// TODO(thor) revisit this I had copilot write it
func avroColMapFromMap[K comparable, V any](m map[K]V) *[]colMap[K, V] {
	if m == nil {
		return nil
	}

	out := make([]colMap[K, V], 0, len(m))
	for k, v := range m {
		out = append(out, colMap[K, V]{Key: k, Value: v})
	}
	return &out
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
	})
}

func (d *dataFile) ContentType() ManifestEntryContent { return d.Content }
func (d *dataFile) FilePath() string                  { return d.Path }
func (d *dataFile) FileFormat() FileFormat            { return d.Format }
func (d *dataFile) Partition() map[string]any         { return d.PartitionData }
func (d *dataFile) Count() int64                      { return d.RecordCount }
func (d *dataFile) FileSizeBytes() int64              { return d.FileSize }

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
	return d.EqualityFieldIDs()
}

func (d *dataFile) SortOrderID() *int { return d.SortOrder }

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
}

var PositionalDeleteSchema = NewSchema(0,
	NestedField{ID: 2147483546, Type: PrimitiveTypes.String, Name: "file_path", Required: true},
	NestedField{ID: 2147483545, Type: PrimitiveTypes.Int32, Name: "pos", Required: true},
)
