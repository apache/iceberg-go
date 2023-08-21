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
	"io"
	"sync"

	iceio "github.com/apache/iceberg-go/io"

	"github.com/hamba/avro/v2/ocf"
)

type ManifestContent int32

const (
	ManifestContentData    ManifestContent = 0
	ManifestContentDeletes ManifestContent = 1
)

type fieldSummary struct {
	ContainsNull bool    `avro:"contains_null"`
	ContainsNaN  *bool   `avro:"contains_nan"`
	LowerBound   *[]byte `avro:"lower_bound"`
	UpperBound   *[]byte `avro:"upper_bound"`
}

type manifestFileV1 struct {
	Path               string          `avro:"manifest_path"`
	Len                int64           `avro:"manifest_length"`
	PartitionSpecID    int32           `avro:"partition_spec_id"`
	Content            ManifestContent `avro:"content"`
	AddedSnapshotID    *int64          `avro:"added_snapshot_id"`
	AddedFilesCount    *int32          `avro:"added_data_files_count"`
	ExistingFilesCount *int32          `avro:"existing_data_files_count"`
	DeletedFilesCount  *int32          `avro:"deleted_data_files_count"`
	AddedRowsCount     *int64          `avro:"added_rows_count"`
	ExistingRowsCount  *int64          `avro:"existing_rows_count"`
	DeletedRowsCount   *int64          `avro:"deleted_rows_count"`
	Partitions         *[]fieldSummary `avro:"partitions"`
	KeyMetadata        []byte          `avro:"key_metadata"`
}

func (*manifestFileV1) Version() int                       { return 1 }
func (m *manifestFileV1) FilePath() string                 { return m.Path }
func (m *manifestFileV1) Length() int64                    { return m.Len }
func (m *manifestFileV1) PartitionID() int32               { return m.PartitionSpecID }
func (m *manifestFileV1) ManifestContent() ManifestContent { return m.Content }
func (m *manifestFileV1) SnapshotID() int64 {
	if m.AddedSnapshotID == nil {
		return 0
	}
	return *m.AddedSnapshotID
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
func (m *manifestFileV1) Metadata() []byte      { return m.KeyMetadata }
func (m *manifestFileV1) PartitionList() []fieldSummary {
	if m.Partitions == nil {
		return nil
	}
	return *m.Partitions
}

func (m *manifestFileV1) FetchEntries(fs iceio.IO, discardDeleted bool) ([]ManifestEntry, error) {
	return fetchManifestEntries(m, fs, discardDeleted)
}

type manifestFileV2 struct {
	Path               string          `avro:"manifest_path"`
	Len                int64           `avro:"manifest_length"`
	PartitionSpecID    int32           `avro:"partition_spec_id"`
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
	Partitions         *[]fieldSummary `avro:"partitions"`
	KeyMetadata        []byte          `avro:"key_metadata"`
}

func (*manifestFileV2) Version() int { return 2 }

func (m *manifestFileV2) FilePath() string                 { return m.Path }
func (m *manifestFileV2) Length() int64                    { return m.Len }
func (m *manifestFileV2) PartitionID() int32               { return m.PartitionSpecID }
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
func (m *manifestFileV2) Metadata() []byte      { return m.KeyMetadata }

func (m *manifestFileV2) PartitionList() []fieldSummary {
	if m.Partitions == nil {
		return nil
	}
	return *m.Partitions
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
	isVer1 := true
	if string(metadata["format-version"]) == "2" {
		isVer1 = false
	}

	results := make([]ManifestEntry, 0)
	for dec.HasNext() {
		var tmp ManifestEntry
		if isVer1 {
			tmp = &manifestEntryV1{}
		} else {
			tmp = &manifestEntryV2{}
		}

		if err := dec.Decode(tmp); err != nil {
			return nil, err
		}

		if !discardDeleted || tmp.Status() != EntryStatusDELETED {
			tmp.inheritSeqNum(m)
			results = append(results, tmp)
		}
	}

	return results, dec.Error()
}

type ManifestFile interface {
	Version() int
	FilePath() string
	Length() int64
	PartitionID() int32
	ManifestContent() ManifestContent
	SnapshotID() int64
	AddedDataFiles() int32
	ExistingDataFiles() int32
	DeletedDataFiles() int32
	AddedRows() int64
	ExistingRows() int64
	DeletedRows() int64
	SequenceNum() int64
	MinSequenceNum() int64
	Metadata() []byte
	PartitionList() []fieldSummary

	HasAddedFiles() bool
	HasExistingFiles() bool
	FetchEntries(fs iceio.IO, discardDeleted bool) ([]ManifestEntry, error)
}

func ReadManifestList(in io.Reader) ([]ManifestFile, error) {
	dec, err := ocf.NewDecoder(in)
	if err != nil {
		return nil, err
	}

	out := make([]ManifestFile, 0)

	for dec.HasNext() {
		var file ManifestFile
		if string(dec.Metadata()["format-version"]) == "2" {
			file = &manifestFileV2{}
		} else {
			file = &manifestFileV1{}
		}

		if err := dec.Decode(file); err != nil {
			return nil, err
		}
		out = append(out, file)
	}

	return out, dec.Error()
}

type ManifestEntryStatus int8

const (
	EntryStatusEXISTING ManifestEntryStatus = 0
	EntryStatusADDED    ManifestEntryStatus = 1
	EntryStatusDELETED  ManifestEntryStatus = 2
)

type ManifestEntryContent int8

const (
	EntryContentData       ManifestEntryContent = 0
	EntryContentPosDeletes ManifestEntryContent = 1
	EntryContentEqDeletes  ManifestEntryContent = 2
)

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
	KeyMetadata      *[]byte                `avro:"key_metadata"`
	SplitOffsets     *[]int64               `avro:"split_offsets"`
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

func (d *dataFile) MetadataKey() []byte {
	if d.KeyMetadata == nil {
		return nil
	}
	return *d.KeyMetadata
}

func (d *dataFile) Splits() []int64 {
	if d.SplitOffsets == nil {
		return nil
	}
	return *d.SplitOffsets
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
	Data        dataFile `avro:"data_file"`
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

func (m *manifestEntryV1) DataFile() DataFile { return &m.Data }

type manifestEntryV2 struct {
	EntryStatus ManifestEntryStatus `avro:"status"`
	Snapshot    *int64              `avro:"snapshot_id"`
	SeqNum      *int64              `avro:"sequence_number"`
	FileSeqNum  *int64              `avro:"file_sequence_number"`
	Data        dataFile            `avro:"data_file"`
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

func (m *manifestEntryV2) DataFile() DataFile { return &m.Data }

type DataFile interface {
	ContentType() ManifestEntryContent
	FilePath() string
	FileFormat() FileFormat
	Partition() map[string]any
	Count() int64
	FileSizeBytes() int64
	ColumnSizes() map[int]int64
	ValueCounts() map[int]int64
	NullValueCounts() map[int]int64
	NaNValueCounts() map[int]int64
	DistinctValueCounts() map[int]int64
	LowerBoundValues() map[int][]byte
	UpperBoundValues() map[int][]byte
	MetadataKey() []byte
	Splits() []int64
	EqualityFieldIDs() []int
	SortOrderID() *int
}

type ManifestEntry interface {
	Status() ManifestEntryStatus
	SnapshotID() int64
	SequenceNum() int64
	FileSequenceNum() *int64
	DataFile() DataFile

	inheritSeqNum(manifest ManifestFile)
}

var PositionalDeleteSchema = NewSchema(0,
	NestedField{ID: 2147483546, Type: PrimitiveTypes.String, Name: "file_path", Required: true},
	NestedField{ID: 2147483545, Type: PrimitiveTypes.Int32, Name: "pos", Required: true},
)
