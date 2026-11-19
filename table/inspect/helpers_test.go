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

package inspect

import (
	"context"
	"encoding/json"
	"strings"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/arrow-go/v18/parquet"
	"github.com/apache/arrow-go/v18/parquet/pqarrow"
	"github.com/apache/iceberg-go"
	iceio "github.com/apache/iceberg-go/io"
	"github.com/apache/iceberg-go/table"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
)

// Test fixtures. Several mirror helpers in package table's tests, rebuilt on
// the public table API.

// testMetadata mirrors the JSON form of table metadata so tests can describe a
// table literally, including states the metadata builder would not produce
// (for example, a snapshot-log entry for an expired snapshot).
type testMetadata struct {
	FormatVersion      int                             `json:"format-version"`
	UUID               uuid.UUID                       `json:"table-uuid"`
	Loc                string                          `json:"location"`
	LastUpdatedMS      int64                           `json:"last-updated-ms"`
	LastColumnId       int                             `json:"last-column-id"`
	SchemaList         []*iceberg.Schema               `json:"schemas"`
	CurrentSchemaID    int                             `json:"current-schema-id"`
	Specs              []iceberg.PartitionSpec         `json:"partition-specs"`
	DefaultSpecID      int                             `json:"default-spec-id"`
	LastPartitionID    *int                            `json:"last-partition-id,omitempty"`
	Props              iceberg.Properties              `json:"properties,omitempty"`
	SnapshotList       []table.Snapshot                `json:"snapshots,omitempty"`
	CurrentSnapshotID  *int64                          `json:"current-snapshot-id,omitempty"`
	SnapshotLog        []table.SnapshotLogEntry        `json:"snapshot-log,omitempty"`
	MetadataLog        []table.MetadataLogEntry        `json:"metadata-log,omitempty"`
	SortOrderList      []table.SortOrder               `json:"sort-orders"`
	DefaultSortOrderID int                             `json:"default-sort-order-id"`
	SnapshotRefs       map[string]table.SnapshotRef    `json:"refs,omitempty"`
	StatisticsList     []table.StatisticsFile          `json:"statistics,omitempty"`
	PartitionStatsList []table.PartitionStatisticsFile `json:"partition-statistics,omitempty"`
	LastSequenceNumber *int64                          `json:"last-sequence-number,omitempty"`
	NextRowID          *int64                          `json:"next-row-id,omitempty"`
}

// newTestMetadata parses m as table metadata. Unset required fields default
// to a v2 table with an empty schema, no partitioning, and no sort order. When
// the last sequence number is unset, it uses the highest snapshot sequence
// number.
func newTestMetadata(m testMetadata) table.Metadata {
	if m.FormatVersion == 0 {
		m.FormatVersion = 2
	}
	if m.UUID == uuid.Nil {
		m.UUID = uuid.New()
	}
	if m.Loc == "" {
		m.Loc = "s3://test/table"
	}
	if m.SchemaList == nil {
		m.SchemaList = []*iceberg.Schema{iceberg.NewSchema(0)}
	}
	if m.Specs == nil {
		m.Specs = []iceberg.PartitionSpec{*iceberg.UnpartitionedSpec}
	}
	if m.LastPartitionID == nil {
		lastPartitionID := 999
		m.LastPartitionID = &lastPartitionID
	}
	if m.SortOrderList == nil {
		m.SortOrderList = []table.SortOrder{table.UnsortedSortOrder}
	}
	if m.FormatVersion > 1 && m.LastSequenceNumber == nil {
		var last int64
		for _, snapshot := range m.SnapshotList {
			last = max(last, snapshot.SequenceNumber)
		}
		m.LastSequenceNumber = &last
	}
	raw, err := json.Marshal(m)
	if err != nil {
		panic(err)
	}
	meta, err := table.ParseMetadataBytes(raw)
	if err != nil {
		panic(err)
	}

	return meta
}

// withSnapshots returns base with its snapshot list replaced by snapshots and
// its current snapshot set to current.
func withSnapshots(t testing.TB, base table.Metadata, snapshots []table.Snapshot, current *int64) table.Metadata {
	t.Helper()

	raw, err := json.Marshal(base)
	require.NoError(t, err)
	var m testMetadata
	require.NoError(t, json.Unmarshal(raw, &m))
	m.SnapshotList = snapshots
	m.CurrentSnapshotID = current
	m.SnapshotRefs = nil
	m.LastSequenceNumber = nil

	return newTestMetadata(m)
}

// withFS returns tbl with its FileIO factory replaced by fsF.
func withFS(tbl *table.Table, fsF table.FSysF) *table.Table {
	return table.New(tbl.Identifier(), tbl.Metadata(), tbl.MetadataLocation(), fsF, nil)
}

// newMemTable returns an empty table with the given spec and simpleSchema,
// backed by an in-memory FileIO.
func newMemTable(t *testing.T, spec iceberg.PartitionSpec) (*table.Table, iceio.WriteFileIO) {
	t.Helper()
	ctx := context.Background()
	fs, err := iceio.LoadFS(ctx, nil, "mem://default/table-location")
	require.NoError(t, err, "LoadFS mem")
	wfs := fs.(iceio.WriteFileIO)
	meta, err := table.NewMetadata(simpleSchema(), &spec, table.UnsortedSortOrder, "mem://default/table-location", nil)
	require.NoError(t, err, "new metadata")
	tbl := table.New(table.Identifier{"db", "tbl"}, meta, "metadata.json",
		func(context.Context) (iceio.IO, error) { return fs, nil }, nil)

	return tbl, wfs
}

func simpleSchema() *iceberg.Schema {
	return iceberg.NewSchema(0, iceberg.NestedField{
		ID: 1, Name: "id", Type: iceberg.PrimitiveTypes.Int32, Required: true,
	})
}

func partitionedSpec() iceberg.PartitionSpec {
	return iceberg.NewPartitionSpec(iceberg.PartitionField{
		SourceIDs: []int{1}, FieldID: 1000, Name: "id", Transform: iceberg.IdentityTransform{},
	})
}

func newTestDataFile(t testing.TB, spec iceberg.PartitionSpec, path string, partition map[int]any) iceberg.DataFile {
	return newTestDataFileWithCount(t, spec, path, partition, 1)
}

func newTestDataFileWithCount(t testing.TB, spec iceberg.PartitionSpec, path string, partition map[int]any, count int64) iceberg.DataFile {
	t.Helper()

	builder, err := iceberg.NewDataFileBuilder(
		spec,
		iceberg.EntryContentData,
		path,
		iceberg.ParquetFile,
		partition,
		nil,
		nil,
		count,
		count,
	)
	require.NoError(t, err, "new data file builder")

	return builder.Build()
}

func newTestPosDeleteFileForSpec(
	t *testing.T,
	spec iceberg.PartitionSpec,
	path string,
	partition map[int]any,
	referencedDataFile string,
) iceberg.DataFile {
	t.Helper()

	builder, err := iceberg.NewDataFileBuilder(
		spec,
		iceberg.EntryContentPosDeletes,
		path,
		iceberg.ParquetFile,
		partition,
		nil,
		nil,
		1,
		1,
	)
	require.NoError(t, err, "new position delete file builder")

	return builder.ReferencedDataFile(referencedDataFile).Build()
}

func newPosDeleteFile(t *testing.T, path string, count, size int64) iceberg.DataFile {
	t.Helper()

	builder, err := iceberg.NewDataFileBuilder(
		*iceberg.UnpartitionedSpec, iceberg.EntryContentPosDeletes,
		path, iceberg.ParquetFile, nil, nil, nil, count, size)
	require.NoError(t, err)

	return builder.Build()
}

func addUnsortedSortOrder(t *testing.T, mb *table.MetadataBuilder) {
	t.Helper()
	so := table.UnsortedSortOrder
	require.NoError(t, mb.AddSortOrder(&so))
	require.NoError(t, mb.SetDefaultSortOrderID(0))
}

func writePosDeleteParquetToMemFS(t *testing.T, memFS *iceio.MemFS, path, content string) {
	t.Helper()

	schema := table.PositionalDeleteArrowSchema
	rec, _, err := array.RecordFromJSON(memory.NewGoAllocator(), schema, strings.NewReader(content))
	require.NoError(t, err)
	defer rec.Release()

	tbl := array.NewTableFromRecords(schema, []arrow.RecordBatch{rec})
	defer tbl.Release()

	fw, err := memFS.Create(context.Background(), path)
	require.NoError(t, err)
	require.NoError(t, pqarrow.WriteTable(tbl, fw, rec.NumRows(),
		parquet.NewWriterProperties(parquet.WithDictionaryDefault(true), parquet.WithStats(true)),
		pqarrow.DefaultWriterProps()))
	require.NoError(t, fw.Close())
}

// mockDataFile is a DataFile whose fields tests set directly, including
// values a DataFileBuilder would reject.
type mockDataFile struct {
	path        string
	contentType iceberg.ManifestEntryContent
	format      iceberg.FileFormat
	partition   map[int]any
	count       int64
	filesize    int64
	specid      int32
}

func (m *mockDataFile) ContentType() iceberg.ManifestEntryContent { return m.contentType }
func (m *mockDataFile) FilePath() string                          { return m.path }
func (m *mockDataFile) FileFormat() iceberg.FileFormat            { return m.format }
func (m *mockDataFile) Partition() map[int]any                    { return m.partition }
func (m *mockDataFile) Count() int64                              { return m.count }
func (m *mockDataFile) FileSizeBytes() int64                      { return m.filesize }
func (*mockDataFile) ColumnSizes() map[int]int64                  { return nil }
func (*mockDataFile) ValueCounts() map[int]int64                  { return nil }
func (*mockDataFile) NullValueCounts() map[int]int64              { return nil }
func (*mockDataFile) NaNValueCounts() map[int]int64               { return nil }
func (*mockDataFile) LowerBoundValues() map[int][]byte            { return nil }
func (*mockDataFile) UpperBoundValues() map[int][]byte            { return nil }
func (*mockDataFile) KeyMetadata() []byte                         { return nil }
func (*mockDataFile) SplitOffsets() []int64                       { return nil }
func (*mockDataFile) EqualityFieldIDs() []int                     { return nil }
func (*mockDataFile) SortOrderID() *int                           { return nil }
func (m *mockDataFile) SpecID() int32                             { return m.specid }
func (*mockDataFile) FirstRowID() *int64                          { return nil }
func (*mockDataFile) ReferencedDataFile() *string                 { return nil }
func (*mockDataFile) ContentOffset() *int64                       { return nil }
func (*mockDataFile) ContentSizeInBytes() *int64                  { return nil }

// publicStatsDataFile exposes statistics only through the public DataFile
// getters and counts the calls.
type publicStatsDataFile struct {
	iceberg.DataFile
	getterCalls int
}

func (f *publicStatsDataFile) ValueCounts() map[int]int64 {
	f.getterCalls++

	return f.DataFile.ValueCounts()
}

func (f *publicStatsDataFile) NullValueCounts() map[int]int64 {
	f.getterCalls++

	return f.DataFile.NullValueCounts()
}

func (f *publicStatsDataFile) NaNValueCounts() map[int]int64 {
	f.getterCalls++

	return f.DataFile.NaNValueCounts()
}

func (f *publicStatsDataFile) LowerBoundValues() map[int][]byte {
	f.getterCalls++

	return f.DataFile.LowerBoundValues()
}

func (f *publicStatsDataFile) UpperBoundValues() map[int][]byte {
	f.getterCalls++

	return f.DataFile.UpperBoundValues()
}

func (f *publicStatsDataFile) ColumnSizes() map[int]int64 {
	f.getterCalls++

	return f.DataFile.ColumnSizes()
}

func (f *publicStatsDataFile) KeyMetadata() []byte {
	f.getterCalls++

	return f.DataFile.KeyMetadata()
}

func (f *publicStatsDataFile) SplitOffsets() []int64 {
	f.getterCalls++

	return f.DataFile.SplitOffsets()
}

func (f *publicStatsDataFile) EqualityFieldIDs() []int {
	f.getterCalls++

	return f.DataFile.EqualityFieldIDs()
}

func testDataFileWithStats(t *testing.T) iceberg.DataFile {
	t.Helper()

	spec := iceberg.NewPartitionSpec(iceberg.PartitionField{
		SourceIDs: []int{1},
		FieldID:   1000,
		Name:      "part",
		Transform: iceberg.IdentityTransform{},
	})
	builder, err := iceberg.NewDataFileBuilder(
		spec,
		iceberg.EntryContentData,
		"s3://bucket/file.parquet",
		iceberg.ParquetFile,
		map[int]any{1000: "partition"},
		nil,
		nil,
		2,
		10,
	)
	require.NoError(t, err)

	return builder.
		ColumnSizes(map[int]int64{1: 10}).
		ValueCounts(map[int]int64{1: 2}).
		NullValueCounts(map[int]int64{1: 0}).
		NaNValueCounts(map[int]int64{1: 0}).
		LowerBoundValues(map[int][]byte{1: {1, 2}}).
		UpperBoundValues(map[int][]byte{1: {3, 4}}).
		KeyMetadata([]byte{5, 6}).
		SplitOffsets([]int64{0, 64}).
		EqualityFieldIDs([]int{1}).
		Build()
}
