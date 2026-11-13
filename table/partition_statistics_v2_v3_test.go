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

package table

import (
	"bytes"
	"context"
	"errors"
	"io"
	"math"
	"slices"
	"strconv"
	"strings"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/arrow-go/v18/parquet/file"
	"github.com/apache/arrow-go/v18/parquet/pqarrow"
	"github.com/apache/iceberg-go"
	iceio "github.com/apache/iceberg-go/io"
	"github.com/stretchr/testify/require"
)

type partitionStatisticsTestManifest struct {
	spec    iceberg.PartitionSpec
	content iceberg.ManifestContent
	entries []iceberg.ManifestEntry
	version int
}

func partitionStatisticsTestTable(t *testing.T, version int, manifests ...partitionStatisticsTestManifest) *Table {
	t.Helper()
	// Store only manifests. Successful computation must not open data files,
	// classic delete files, or the Puffin containers referenced below.
	spec := partitionedSpec()
	fs := iceio.NewMemFS()
	meta, err := NewMetadata(simpleSchema(), &spec, UnsortedSortOrder, "mem://stats/table", iceberg.Properties{
		PropertyFormatVersion: strconv.Itoa(version), CommitNumRetriesKey: "0",
	})
	require.NoError(t, err)
	builder, err := MetadataBuilderFromBase(meta, "mem://stats/table/metadata/base.json")
	require.NoError(t, err)
	knownSpecs := map[int]bool{spec.ID(): true}
	var manifestFiles []iceberg.ManifestFile
	for index, manifest := range manifests {
		if !knownSpecs[manifest.spec.ID()] {
			builder.specs = append(builder.specs, manifest.spec)
			knownSpecs[manifest.spec.ID()] = true
		}
		manifestVersion := manifest.version
		if manifestVersion == 0 {
			manifestVersion = version
		}
		path := "mem://stats/table/metadata/manifest-" + strconv.Itoa(index) + ".avro"
		var buf bytes.Buffer
		writer, err := iceberg.NewManifestWriter(manifestVersion, &buf, manifest.spec, simpleSchema(), 10,
			iceberg.WithManifestWriterContent(manifest.content))
		require.NoError(t, err)
		for _, entry := range manifest.entries {
			switch entry.Status() {
			case iceberg.EntryStatusDELETED:
				require.NoError(t, writer.Delete(entry))
			case iceberg.EntryStatusEXISTING:
				require.NoError(t, writer.Existing(entry))
			default:
				require.NoError(t, writer.Add(entry))
			}
		}
		require.NoError(t, writer.Close())
		descriptor, err := writer.ToManifestFile(path, int64(buf.Len()), iceberg.WithManifestFileContent(manifest.content))
		require.NoError(t, err)
		require.NoError(t, fs.WriteFile(t.Context(), path, buf.Bytes()))
		manifestFiles = append(manifestFiles, descriptor)
	}
	listPath := "mem://stats/table/metadata/snapshot.avro"
	var buf bytes.Buffer
	sequence := int64(3)
	require.NoError(t, iceberg.WriteManifestList(version, &buf, 10, nil, &sequence, 0, manifestFiles))
	require.NoError(t, fs.WriteFile(t.Context(), listPath, buf.Bytes()))
	builder.snapshotList = []Snapshot{{
		SnapshotID: 10, SequenceNumber: sequence, TimestampMs: 1000, ManifestList: listPath,
		Summary: &Summary{Operation: OpAppend},
	}}
	builder.currentSnapshotID = new(int64(10))
	builder.lastSequenceNumber = new(sequence)
	builder.refs = map[string]SnapshotRef{MainBranch: {SnapshotID: 10, Type: BranchRef}}
	meta, err = builder.Build()
	require.NoError(t, err)

	return New(Identifier{"db", "partition_stats"}, meta, "mem://stats/table/metadata/base.json",
		func(context.Context) (iceio.IO, error) { return fs, nil }, nil)
}

func partitionStatsFile(t *testing.T, spec iceberg.PartitionSpec, content iceberg.ManifestEntryContent, path string, partition map[int]any, records int64) *iceberg.DataFileBuilder {
	t.Helper()
	format := iceberg.ParquetFile
	if strings.HasSuffix(path, ".puffin") {
		format = iceberg.PuffinFile
	}
	builder, err := iceberg.NewDataFileBuilder(spec, content, path, format, partition, nil, nil, records, 1024)
	require.NoError(t, err)
	if content == iceberg.EntryContentEqDeletes {
		builder.EqualityFieldIDs([]int{1})
	}

	return builder
}

func partitionStatsEntry(file iceberg.DataFile, status iceberg.ManifestEntryStatus, sequence int64) iceberg.ManifestEntry {
	return iceberg.NewManifestEntry(status, new(int64(10)), new(sequence), new(sequence), file)
}

func partitionStatsReadTable(t *testing.T, tbl *Table, stats *PartitionStatisticsFile) arrow.Table {
	t.Helper()
	fs, err := tbl.FS(t.Context())
	require.NoError(t, err)
	f, err := fs.Open(t.Context(), stats.StatisticsPath)
	require.NoError(t, err)
	data, err := io.ReadAll(f)
	require.NoError(t, err)
	require.NoError(t, f.Close())
	require.EqualValues(t, len(data), stats.FileSizeInBytes)
	reader, err := file.NewParquetReader(bytes.NewReader(data))
	require.NoError(t, err)
	defer reader.Close()
	arrowReader, err := pqarrow.NewFileReader(reader, pqarrow.ArrowReadProperties{}, memory.DefaultAllocator)
	require.NoError(t, err)
	out, err := arrowReader.ReadTable(t.Context())
	require.NoError(t, err)
	t.Cleanup(out.Release)

	return out
}

func partitionStatsCompute(t *testing.T, tbl *Table) arrow.Table {
	t.Helper()
	tx := tbl.NewTransaction()
	stats, err := tx.ComputePartitionStatistics(t.Context())
	require.NoError(t, err)
	staged, err := tx.meta.Build()
	require.NoError(t, err)
	require.Equal(t, []PartitionStatisticsFile{*stats}, slices.Collect(staged.PartitionStatistics()))
	require.Equal(t, tbl.CurrentSnapshot().SnapshotID, stats.SnapshotID)

	return partitionStatsReadTable(t, tbl, stats)
}

func partitionStatsInt64(t *testing.T, rows arrow.Table, name string) *array.Int64 {
	t.Helper()
	indices := rows.Schema().FieldIndices(name)
	require.Len(t, indices, 1)

	return rows.Column(indices[0]).Data().Chunk(0).(*array.Int64)
}

func partitionStatsInt32(t *testing.T, rows arrow.Table, name string) *array.Int32 {
	t.Helper()
	indices := rows.Schema().FieldIndices(name)
	require.Len(t, indices, 1)

	return rows.Column(indices[0]).Data().Chunk(0).(*array.Int32)
}

func TestPartitionStatisticsVersionedSchemas(t *testing.T) {
	for _, version := range []int{1, 2, 3} {
		schema, err := PartitionStatisticsSchema(statsTestPartitionType(), version)
		require.NoError(t, err)
		if version == 1 {
			require.True(t, schema.Equals(PartitionStatisticsSchemaV1(statsTestPartitionType())))

			continue
		}
		wantIDs := []int{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12}
		if version == 3 {
			wantIDs = append(wantIDs, 13)
		}
		var ids []int
		for _, field := range schema.Fields() {
			ids = append(ids, field.ID)
		}
		require.Equal(t, wantIDs, ids)
		for id := 6; id <= 9; id++ {
			field, found := schema.FindFieldByID(id)
			require.True(t, found)
			require.Equal(t, version == 3, field.Required)
		}
		if version == 3 {
			field, _ := schema.FindFieldByID(13)
			require.True(t, field.Required)
			require.Equal(t, int32(0), field.InitialDefault)
			require.Equal(t, int32(0), field.WriteDefault)
		}
	}
	for _, version := range []int{0, -1, 4} {
		_, err := PartitionStatisticsSchema(statsTestPartitionType(), version)
		require.ErrorIs(t, err, iceberg.ErrInvalidArgument)
	}
	_, err := PartitionStatisticsSchema(nil, 2)
	require.ErrorIs(t, err, iceberg.ErrInvalidArgument)
	_, err = PartitionStatisticsSchema(iceberg.NewStructType(), 3)
	require.ErrorIs(t, err, iceberg.ErrInvalidArgument)
}

func TestPartitionStatisticsV2DeleteMetrics(t *testing.T) {
	spec := partitionedSpec()
	entry := func(content iceberg.ManifestEntryContent, path string, part int32, records int64, status iceberg.ManifestEntryStatus) iceberg.ManifestEntry {
		return partitionStatsEntry(partitionStatsFile(t, spec, content, path, map[int]any{1000: part}, records).Build(), status, 1)
	}
	tbl := partitionStatisticsTestTable(t, 2,
		partitionStatisticsTestManifest{spec: spec, content: iceberg.ManifestContentData, entries: []iceberg.ManifestEntry{
			entry(iceberg.EntryContentData, "a.parquet", 2, 10, iceberg.EntryStatusADDED),
			entry(iceberg.EntryContentData, "b.parquet", 2, 5, iceberg.EntryStatusEXISTING),
			entry(iceberg.EntryContentData, "c.parquet", 10, 7, iceberg.EntryStatusADDED),
			entry(iceberg.EntryContentData, "removed.parquet", 2, 100, iceberg.EntryStatusDELETED),
		}},
		partitionStatisticsTestManifest{spec: spec, content: iceberg.ManifestContentDeletes, entries: []iceberg.ManifestEntry{
			entry(iceberg.EntryContentPosDeletes, "pos.parquet", 2, 3, iceberg.EntryStatusADDED),
			entry(iceberg.EntryContentEqDeletes, "eq.parquet", 2, 20, iceberg.EntryStatusADDED),
			entry(iceberg.EntryContentPosDeletes, "delete-only.parquet", 20, 4, iceberg.EntryStatusADDED),
			entry(iceberg.EntryContentPosDeletes, "removed-delete.parquet", 2, 100, iceberg.EntryStatusDELETED),
		}},
	)
	rows := partitionStatsCompute(t, tbl)
	require.EqualValues(t, 3, rows.NumRows())
	require.Equal(t, []int64{15, 7, 0}, partitionStatsInt64(t, rows, "data_record_count").Int64Values())
	require.Equal(t, []int32{2, 1, 0}, partitionStatsInt32(t, rows, "data_file_count").Int32Values())
	require.Equal(t, []int64{2048, 1024, 0}, partitionStatsInt64(t, rows, "total_data_file_size_in_bytes").Int64Values())
	require.Equal(t, []int64{3, 0, 4}, partitionStatsInt64(t, rows, "position_delete_record_count").Int64Values())
	require.Equal(t, []int32{1, 0, 1}, partitionStatsInt32(t, rows, "position_delete_file_count").Int32Values())
	require.Equal(t, []int64{20, 0, 0}, partitionStatsInt64(t, rows, "equality_delete_record_count").Int64Values())
	require.Equal(t, []int32{1, 0, 0}, partitionStatsInt32(t, rows, "equality_delete_file_count").Int32Values())
	require.Equal(t, 3, partitionStatsInt64(t, rows, "total_record_count").NullN())
	require.Empty(t, rows.Schema().FieldIndices("dv_count"))
	require.Equal(t, []int64{1000, 1000, 1000}, partitionStatsInt64(t, rows, "last_updated_at").Int64Values())
	require.Equal(t, []int64{10, 10, 10}, partitionStatsInt64(t, rows, "last_updated_snapshot_id").Int64Values())
}

func TestPartitionStatisticsLastUpdated(t *testing.T) {
	for _, version := range []int{2, 3} {
		t.Run(strconv.Itoa(version), func(t *testing.T) {
			spec := partitionedSpec()
			existing := func(path string, partition int32, snapshot int64) iceberg.ManifestEntry {
				file := partitionStatsFile(t, spec, iceberg.EntryContentData, path, map[int]any{1000: partition}, 1).Build()

				return iceberg.NewManifestEntry(iceberg.EntryStatusEXISTING, new(snapshot), new(int64(1)), new(int64(1)), file)
			}
			deleted := func(path string, partition int32) iceberg.ManifestEntry {
				file := partitionStatsFile(t, spec, iceberg.EntryContentData, path, map[int]any{1000: partition}, 1).Build()

				return partitionStatsEntry(file, iceberg.EntryStatusDELETED, 1)
			}
			deleteFile := partitionStatsFile(t, spec, iceberg.EntryContentPosDeletes, "pos.parquet", map[int]any{1000: int32(5)}, 2).Build()
			tbl := partitionStatisticsTestTable(t, version,
				partitionStatisticsTestManifest{spec: spec, content: iceberg.ManifestContentData, entries: []iceberg.ManifestEntry{
					existing("a.parquet", 1, 11), existing("b.parquet", 1, 12), existing("c.parquet", 1, 13),
					existing("expired-in-mixed.parquet", 1, 99),
					existing("expired.parquet", 2, 99), deleted("deleted-only.parquet", 3),
					existing("live.parquet", 4, 11), deleted("live.parquet", 4),
				}},
				partitionStatisticsTestManifest{spec: spec, content: iceberg.ManifestContentDeletes, entries: []iceberg.ManifestEntry{
					iceberg.NewManifestEntry(iceberg.EntryStatusEXISTING, new(int64(12)), new(int64(1)), new(int64(1)), deleteFile),
				}},
			)
			builder, err := MetadataBuilderFromBase(tbl.Metadata(), tbl.MetadataLocation())
			require.NoError(t, err)
			builder.snapshotList = append(builder.snapshotList,
				Snapshot{SnapshotID: 11, TimestampMs: 600}, Snapshot{SnapshotID: 12, TimestampMs: 900},
				Snapshot{SnapshotID: 13, TimestampMs: 900})
			metadata, err := builder.Build()
			require.NoError(t, err)
			tbl = tbl.withMetadata(metadata, tbl.MetadataLocation())
			rows := partitionStatsCompute(t, tbl)
			require.EqualValues(t, 5, rows.NumRows())
			require.Equal(t, []int64{4, 1, 0, 1, 0}, partitionStatsInt64(t, rows, "data_record_count").Int64Values())
			timestamps := partitionStatsInt64(t, rows, "last_updated_at")
			ids := partitionStatsInt64(t, rows, "last_updated_snapshot_id")
			require.Equal(t, int64(900), timestamps.Value(0))
			require.Equal(t, int64(12), ids.Value(0))
			require.True(t, timestamps.IsNull(1))
			require.True(t, ids.IsNull(1))
			require.Equal(t, int64(1000), timestamps.Value(2))
			require.Equal(t, int64(10), ids.Value(2))
			require.Equal(t, int64(1000), timestamps.Value(3))
			require.Equal(t, int64(10), ids.Value(3))
			require.Equal(t, int64(900), timestamps.Value(4))
			require.Equal(t, int64(12), ids.Value(4))
		})
	}
}

func TestPartitionStatisticsV2V3NoDeletesHaveExactTotals(t *testing.T) {
	spec := partitionedSpec()
	data := partitionStatsFile(t, spec, iceberg.EntryContentData, "a.parquet", map[int]any{1000: nil}, 12).Build()
	for _, version := range []int{2, 3} {
		tbl := partitionStatisticsTestTable(t, version, partitionStatisticsTestManifest{
			spec: spec, content: iceberg.ManifestContentData,
			entries: []iceberg.ManifestEntry{partitionStatsEntry(data, iceberg.EntryStatusADDED, 1)},
		})
		rows := partitionStatsCompute(t, tbl)
		require.Equal(t, []int64{12}, partitionStatsInt64(t, rows, "total_record_count").Int64Values())
		require.Zero(t, partitionStatsInt64(t, rows, "total_record_count").NullN())
		require.Zero(t, partitionStatsInt64(t, rows, "position_delete_record_count").NullN())
		require.Equal(t, []int64{0}, partitionStatsInt64(t, rows, "position_delete_record_count").Int64Values())
		if version == 3 {
			require.Equal(t, []int32{0}, partitionStatsInt32(t, rows, "dv_count").Int32Values())
			require.Zero(t, partitionStatsInt32(t, rows, "dv_count").NullN())
			for _, name := range []string{"position_delete_record_count", "position_delete_file_count", "equality_delete_record_count", "equality_delete_file_count", "dv_count"} {
				field := rows.Schema().Field(rows.Schema().FieldIndices(name)[0])
				require.False(t, field.Nullable, name)
			}
		}
	}
}

func TestPartitionStatisticsV3SharedPuffinAndExactTotals(t *testing.T) {
	spec := partitionedSpec()
	data := func(path string, part int32, count int64) iceberg.DataFile {
		return partitionStatsFile(t, spec, iceberg.EntryContentData, path, map[int]any{1000: part}, count).Build()
	}
	vector := func(target string, part int32, count, offset int64) iceberg.DataFile {
		return partitionStatsFile(t, spec, iceberg.EntryContentPosDeletes, "shared.puffin", map[int]any{1000: part}, count).
			ReferencedDataFile(target).ContentOffset(offset).ContentSizeInBytes(20).Build()
	}
	tbl := partitionStatisticsTestTable(t, 3,
		partitionStatisticsTestManifest{spec: spec, content: iceberg.ManifestContentData, entries: []iceberg.ManifestEntry{
			partitionStatsEntry(data("a.parquet", 2, 10), iceberg.EntryStatusADDED, 1),
			partitionStatsEntry(data("b.parquet", 10, 7), iceberg.EntryStatusEXISTING, 1),
		}},
		partitionStatisticsTestManifest{spec: spec, content: iceberg.ManifestContentDeletes, entries: []iceberg.ManifestEntry{
			partitionStatsEntry(vector("a.parquet", 2, 3, 4), iceberg.EntryStatusADDED, 2),
			partitionStatsEntry(vector("b.parquet", 10, 2, 40), iceberg.EntryStatusADDED, 2),
		}},
	)
	rows := partitionStatsCompute(t, tbl)
	require.Equal(t, []int32{1, 1}, partitionStatsInt32(t, rows, "dv_count").Int32Values())
	require.Equal(t, []int64{3, 2}, partitionStatsInt64(t, rows, "position_delete_record_count").Int64Values())
	require.Equal(t, []int32{0, 0}, partitionStatsInt32(t, rows, "position_delete_file_count").Int32Values())
	require.Equal(t, []int64{7, 5}, partitionStatsInt64(t, rows, "total_record_count").Int64Values())
	require.Zero(t, partitionStatsInt64(t, rows, "total_record_count").NullN())
}

func TestPartitionStatisticsV3IncludesCarriedV2Deletes(t *testing.T) {
	spec := partitionedSpec()
	part := map[int]any{1000: int32(2)}
	data := partitionStatsFile(t, spec, iceberg.EntryContentData, "a.parquet", part, 10).Build()
	legacy := partitionStatsFile(t, spec, iceberg.EntryContentPosDeletes, "legacy.parquet", part, 2).Build()
	vector := partitionStatsFile(t, spec, iceberg.EntryContentPosDeletes, "deletes.puffin", part, 3).
		ReferencedDataFile("a.parquet").ContentOffset(4).ContentSizeInBytes(20).Build()
	tbl := partitionStatisticsTestTable(t, 3,
		partitionStatisticsTestManifest{spec: spec, version: 2, content: iceberg.ManifestContentData, entries: []iceberg.ManifestEntry{
			partitionStatsEntry(data, iceberg.EntryStatusEXISTING, 1),
		}},
		partitionStatisticsTestManifest{spec: spec, version: 2, content: iceberg.ManifestContentDeletes, entries: []iceberg.ManifestEntry{
			partitionStatsEntry(legacy, iceberg.EntryStatusEXISTING, 1),
		}},
		partitionStatisticsTestManifest{spec: spec, version: 3, content: iceberg.ManifestContentDeletes, entries: []iceberg.ManifestEntry{
			partitionStatsEntry(vector, iceberg.EntryStatusADDED, 3),
		}},
	)
	rows := partitionStatsCompute(t, tbl)
	require.Equal(t, []int64{5}, partitionStatsInt64(t, rows, "position_delete_record_count").Int64Values())
	require.Equal(t, []int32{1}, partitionStatsInt32(t, rows, "position_delete_file_count").Int32Values())
	require.Equal(t, []int32{1}, partitionStatsInt32(t, rows, "dv_count").Int32Values())
	require.True(t, partitionStatsInt64(t, rows, "total_record_count").IsNull(0))
}

func TestPartitionStatisticsGlobalEqualityDeletesLeaveAllTotalsUnknown(t *testing.T) {
	spec := partitionedSpec()
	global := iceberg.NewPartitionSpecID(1)
	data := partitionStatsFile(t, spec, iceberg.EntryContentData, "data.parquet", map[int]any{1000: int32(2)}, 10).Build()
	deletes := partitionStatsFile(t, global, iceberg.EntryContentEqDeletes, "global.parquet", nil, 2).Build()
	for _, version := range []int{2, 3} {
		tbl := partitionStatisticsTestTable(t, version,
			partitionStatisticsTestManifest{spec: spec, content: iceberg.ManifestContentData, entries: []iceberg.ManifestEntry{
				partitionStatsEntry(data, iceberg.EntryStatusADDED, 1),
			}},
			partitionStatisticsTestManifest{spec: global, content: iceberg.ManifestContentDeletes, entries: []iceberg.ManifestEntry{
				partitionStatsEntry(deletes, iceberg.EntryStatusADDED, 2),
			}},
		)
		rows := partitionStatsCompute(t, tbl)
		require.Equal(t, []int32{1, 0}, partitionStatsInt32(t, rows, "spec_id").Int32Values(), "NULL partition tuple sorts first")
		require.Equal(t, []int64{2, 0}, partitionStatsInt64(t, rows, "equality_delete_record_count").Int64Values())
		require.Equal(t, 2, partitionStatsInt64(t, rows, "total_record_count").NullN(), "delete metrics in another spec can affect this data")
	}
}

func TestPartitionStatisticsKeepsSpecIDsSeparate(t *testing.T) {
	spec := partitionedSpec()
	other := iceberg.NewPartitionSpecID(1, iceberg.PartitionField{
		SourceIDs: []int{1}, FieldID: 1000, Name: "renamed_id", Transform: iceberg.IdentityTransform{},
	})
	part := map[int]any{1000: int32(2)}
	tbl := partitionStatisticsTestTable(t, 3,
		partitionStatisticsTestManifest{spec: spec, content: iceberg.ManifestContentData, entries: []iceberg.ManifestEntry{
			partitionStatsEntry(partitionStatsFile(t, spec, iceberg.EntryContentData, "old.parquet", part, 4).Build(), iceberg.EntryStatusEXISTING, 1),
		}},
		partitionStatisticsTestManifest{spec: other, content: iceberg.ManifestContentData, entries: []iceberg.ManifestEntry{
			partitionStatsEntry(partitionStatsFile(t, other, iceberg.EntryContentData, "new.parquet", part, 7).Build(), iceberg.EntryStatusADDED, 2),
		}},
	)
	rows := partitionStatsCompute(t, tbl)
	require.Equal(t, []int32{0, 1}, partitionStatsInt32(t, rows, "spec_id").Int32Values())
	require.Equal(t, []int64{4, 7}, partitionStatsInt64(t, rows, "data_record_count").Int64Values())
}

func TestPartitionStatisticsAllMetricOverflows(t *testing.T) {
	spec := partitionedSpec()
	for _, tt := range []struct {
		name    string
		row     inspectPartitionAggregate
		content iceberg.ManifestEntryContent
		path    string
	}{
		{"data records", inspectPartitionAggregate{dataRecordCount: math.MaxInt64}, iceberg.EntryContentData, "data.parquet"},
		{"data files", inspectPartitionAggregate{dataFileCount: math.MaxInt32}, iceberg.EntryContentData, "data.parquet"},
		{"data bytes", inspectPartitionAggregate{dataFileSize: math.MaxInt64}, iceberg.EntryContentData, "data.parquet"},
		{"position records", inspectPartitionAggregate{positionDeleteCount: math.MaxInt64}, iceberg.EntryContentPosDeletes, "delete.parquet"},
		{"position files", inspectPartitionAggregate{positionDeleteFiles: math.MaxInt32}, iceberg.EntryContentPosDeletes, "delete.parquet"},
		{"equality records", inspectPartitionAggregate{equalityDeleteCount: math.MaxInt64}, iceberg.EntryContentEqDeletes, "delete.parquet"},
		{"equality files", inspectPartitionAggregate{equalityDeleteFiles: math.MaxInt32}, iceberg.EntryContentEqDeletes, "delete.parquet"},
		{"vectors", inspectPartitionAggregate{deletionVectorCount: math.MaxInt32}, iceberg.EntryContentPosDeletes, "delete.puffin"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			file := partitionStatsFile(t, spec, tt.content, tt.path, map[int]any{1000: int32(2)}, 1).Build()
			require.ErrorIs(t, accumulatePartitionStatistics(&tt.row, file), iceberg.ErrInvalidArgument)
		})
	}
}

type partitionStatisticsOverrideFile struct {
	iceberg.DataFile
	target         *string
	offset, length *int64
	size           int64
}

func (f partitionStatisticsOverrideFile) ReferencedDataFile() *string { return f.target }
func (f partitionStatisticsOverrideFile) ContentOffset() *int64       { return f.offset }
func (f partitionStatisticsOverrideFile) ContentSizeInBytes() *int64  { return f.length }
func (f partitionStatisticsOverrideFile) FileSizeBytes() int64        { return f.size }

func TestPartitionStatisticsRejectsInvalidVectorMetadata(t *testing.T) {
	base := partitionStatsFile(t, partitionedSpec(), iceberg.EntryContentPosDeletes, "deletes.puffin", map[int]any{1000: int32(2)}, 3).
		ReferencedDataFile("data.parquet").ContentOffset(4).ContentSizeInBytes(20).Build()
	for _, tt := range []struct {
		name   string
		mutate func(*partitionStatisticsOverrideFile)
	}{
		{"missing target", func(f *partitionStatisticsOverrideFile) { f.target = nil }},
		{"empty target", func(f *partitionStatisticsOverrideFile) { f.target = new("") }},
		{"missing offset", func(f *partitionStatisticsOverrideFile) { f.offset = nil }},
		{"negative offset", func(f *partitionStatisticsOverrideFile) { f.offset = new(int64(-1)) }},
		{"missing length", func(f *partitionStatisticsOverrideFile) { f.length = nil }},
		{"zero length", func(f *partitionStatisticsOverrideFile) { f.length = new(int64(0)) }},
		{"negative length", func(f *partitionStatisticsOverrideFile) { f.length = new(int64(-1)) }},
		{"outside container", func(f *partitionStatisticsOverrideFile) { f.offset = new(int64(1020)) }},
		{"length exceeds container", func(f *partitionStatisticsOverrideFile) { f.length = new(int64(2048)) }},
		{"range overflow", func(f *partitionStatisticsOverrideFile) { f.offset = new(int64(math.MaxInt64)) }},
	} {
		t.Run(tt.name, func(t *testing.T) {
			file := partitionStatisticsOverrideFile{DataFile: base, target: base.ReferencedDataFile(), offset: base.ContentOffset(), length: base.ContentSizeInBytes(), size: base.FileSizeBytes()}
			tt.mutate(&file)
			index := partitionStatisticsFileIndex{paths: make(map[string]string)}
			_, err := index.add(file, "manifest.avro", 3)
			require.ErrorIs(t, err, iceberg.ErrInvalidArgument)
		})
	}
	index := partitionStatisticsFileIndex{paths: make(map[string]string)}
	_, err := index.add(base, "manifest.avro", 2)
	require.ErrorIs(t, err, iceberg.ErrInvalidArgument)
}

func TestPartitionStatisticsVectorAndPathIdentities(t *testing.T) {
	spec := partitionedSpec()
	part := map[int]any{1000: int32(2)}
	base := partitionStatsFile(t, spec, iceberg.EntryContentPosDeletes, "shared.puffin", part, 3).
		ReferencedDataFile("a.parquet").ContentOffset(4).ContentSizeInBytes(20).Build()
	regularBuilder, err := iceberg.NewDataFileBuilder(spec, iceberg.EntryContentData, "shared.puffin", iceberg.ParquetFile, part, nil, nil, 5, 1024)
	require.NoError(t, err)
	for _, tt := range []struct {
		name string
		next iceberg.DataFile
		want string
	}{
		{"same blob", base, "duplicate live deletion vector"},
		{"same blob another target", partitionStatsFile(t, spec, iceberg.EntryContentPosDeletes, "shared.puffin", part, 3).
			ReferencedDataFile("b.parquet").ContentOffset(4).ContentSizeInBytes(20).Build(), "duplicate live deletion vector"},
		{"another blob", partitionStatsFile(t, spec, iceberg.EntryContentPosDeletes, "shared.puffin", part, 3).
			ReferencedDataFile("b.parquet").ContentOffset(40).ContentSizeInBytes(20).Build(), ""},
		{"container size mismatch", partitionStatisticsOverrideFile{
			DataFile: base, target: new("b.parquet"), offset: new(int64(40)), length: new(int64(20)), size: 2048,
		}, "inconsistent Puffin container size"},
		{"regular file same path", regularBuilder.Build(), "incompatible live references"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			index := partitionStatisticsFileIndex{paths: make(map[string]string)}
			_, err := index.add(base, "first.avro", 3)
			require.NoError(t, err)
			_, err = index.add(tt.next, "second.avro", 3)
			if tt.want == "" {
				require.NoError(t, err)
			} else {
				require.ErrorIs(t, err, iceberg.ErrInvalidArgument)
				require.ErrorContains(t, err, tt.want)
			}
		})
	}
	index := partitionStatisticsFileIndex{paths: make(map[string]string)}
	regular := regularBuilder.Build()
	_, err = index.add(regular, "first.avro", 3)
	require.NoError(t, err)
	_, err = index.add(regular, "second.avro", 3)
	require.ErrorContains(t, err, "duplicate live data file")
}

func TestPartitionStatisticsVectorTotalsValidateTargets(t *testing.T) {
	for _, tt := range []struct {
		name                         string
		missing, differentPartition  bool
		dataSequence, vectorSequence int64
		vectorCount                  int64
		wantUnknown                  bool
	}{
		{name: "valid", dataSequence: 1, vectorSequence: 2, vectorCount: 3},
		{name: "same sequence", dataSequence: 2, vectorSequence: 2, vectorCount: 3},
		{name: "dangling", missing: true, vectorCount: 3, wantUnknown: true},
		{name: "other spec or partition", differentPartition: true, vectorCount: 3, wantUnknown: true},
		{name: "too many deleted rows", dataSequence: 1, vectorSequence: 2, vectorCount: 11, wantUnknown: true},
		{name: "older vector", dataSequence: 3, vectorSequence: 2, vectorCount: 3, wantUnknown: true},
		{name: "unknown data sequence", dataSequence: -1, vectorSequence: 2, vectorCount: 3, wantUnknown: true},
		{name: "unknown vector sequence", dataSequence: 1, vectorSequence: -1, vectorCount: 3, wantUnknown: true},
	} {
		t.Run(tt.name, func(t *testing.T) {
			row := &inspectPartitionAggregate{dataRecordCount: 10}
			vectorRow := row
			if tt.differentPartition {
				vectorRow = &inspectPartitionAggregate{}
			}
			data := make(map[string]partitionStatisticsDataFile)
			if !tt.missing {
				data["data.parquet"] = partitionStatisticsDataFile{row: row, count: 10, sequence: tt.dataSequence}
			}
			vectors := map[string][]partitionStatisticsVector{"data.parquet": {{row: vectorRow, count: tt.vectorCount, sequence: tt.vectorSequence}}}
			unrelated := &inspectPartitionAggregate{dataRecordCount: 20}
			allRows := []*inspectPartitionAggregate{row, unrelated}
			if vectorRow != row {
				allRows = append(allRows, vectorRow)
			}
			err := finishPartitionStatistics(t.Context(), allRows, data, vectors, true)
			require.NoError(t, err)
			require.Equal(t, int64(20), *unrelated.totalRecordCount)
			if tt.wantUnknown {
				require.Nil(t, row.totalRecordCount)
				require.Nil(t, vectorRow.totalRecordCount)
			} else {
				require.Equal(t, int64(7), *row.totalRecordCount)
			}
		})
	}
}

func TestPartitionStatisticsMultipleVectorsHaveUnknownTotal(t *testing.T) {
	spec := partitionedSpec()
	part := map[int]any{1000: int32(2)}
	data := partitionStatsFile(t, spec, iceberg.EntryContentData, "data.parquet", part, 10).Build()
	vector := func(offset int64) iceberg.ManifestEntry {
		file := partitionStatsFile(t, spec, iceberg.EntryContentPosDeletes, "shared.puffin", part, 3).
			ReferencedDataFile("data.parquet").ContentOffset(offset).ContentSizeInBytes(20).Build()

		return partitionStatsEntry(file, iceberg.EntryStatusADDED, 2)
	}
	tbl := partitionStatisticsTestTable(t, 3,
		partitionStatisticsTestManifest{spec: spec, content: iceberg.ManifestContentData, entries: []iceberg.ManifestEntry{partitionStatsEntry(data, iceberg.EntryStatusADDED, 1)}},
		partitionStatisticsTestManifest{spec: spec, content: iceberg.ManifestContentDeletes, entries: []iceberg.ManifestEntry{vector(4), vector(40)}},
	)
	rows := partitionStatsCompute(t, tbl)
	require.EqualValues(t, 1, rows.NumRows())
	require.Equal(t, 1, partitionStatsInt64(t, rows, "total_record_count").NullN())
	require.Equal(t, []int64{6}, partitionStatsInt64(t, rows, "position_delete_record_count").Int64Values())
	require.Equal(t, []int32{2}, partitionStatsInt32(t, rows, "dv_count").Int32Values())
}

func TestPartitionStatisticsInvalidVectorOnlyAffectsRelatedPartitions(t *testing.T) {
	spec := partitionedSpec()
	data := func(path string, part int32, count int64) iceberg.ManifestEntry {
		file := partitionStatsFile(t, spec, iceberg.EntryContentData, path, map[int]any{1000: part}, count).Build()

		return partitionStatsEntry(file, iceberg.EntryStatusADDED, 1)
	}
	vector := func(path, target string, part int32) iceberg.ManifestEntry {
		file := partitionStatsFile(t, spec, iceberg.EntryContentPosDeletes, path, map[int]any{1000: part}, 3).
			ReferencedDataFile(target).ContentOffset(4).ContentSizeInBytes(20).Build()

		return partitionStatsEntry(file, iceberg.EntryStatusADDED, 2)
	}
	tbl := partitionStatisticsTestTable(t, 3,
		partitionStatisticsTestManifest{spec: spec, content: iceberg.ManifestContentData, entries: []iceberg.ManifestEntry{
			data("data.parquet", 1, 10), data("unrelated.parquet", 3, 20),
		}},
		partitionStatisticsTestManifest{spec: spec, content: iceberg.ManifestContentDeletes, entries: []iceberg.ManifestEntry{
			vector("mismatched.puffin", "data.parquet", 2), vector("dangling.puffin", "missing.parquet", 4),
		}},
	)
	rows := partitionStatsCompute(t, tbl)
	require.EqualValues(t, 4, rows.NumRows())
	totals := partitionStatsInt64(t, rows, "total_record_count")
	require.True(t, totals.IsNull(0))
	require.True(t, totals.IsNull(1))
	require.Equal(t, int64(20), totals.Value(2))
	require.True(t, totals.IsNull(3))
	require.Equal(t, []int64{0, 3, 0, 3}, partitionStatsInt64(t, rows, "position_delete_record_count").Int64Values())
	require.Equal(t, []int32{0, 1, 0, 1}, partitionStatsInt32(t, rows, "dv_count").Int32Values())
}

func partitionStatsAdvanceBranch(t *testing.T, base Metadata, branch string, childID int64) Metadata {
	t.Helper()
	parent := base.SnapshotByName(branch)
	require.NotNil(t, parent)
	builder, err := MetadataBuilderFromBase(base, "")
	require.NoError(t, err)
	require.NoError(t, builder.AddSnapshot(&Snapshot{
		SnapshotID: childID, ParentSnapshotID: new(parent.SnapshotID), SequenceNumber: builder.currentLastSequenceNumber() + 1,
		TimestampMs: base.LastUpdatedMillis() + 1, ManifestList: parent.ManifestList,
		Summary: &Summary{Operation: OpAppend}, FirstRowID: new(int64(0)), AddedRows: new(int64(0)),
	}))
	require.NoError(t, builder.SetSnapshotRef(branch, childID, BranchRef))
	meta, err := builder.Build()
	require.NoError(t, err)

	return meta
}

func TestPartitionStatisticsCommitAndConcurrencyFences(t *testing.T) {
	for _, version := range []int{2, 3} {
		for _, change := range []string{"none", "head", "schema", "default spec", "spec history"} {
			t.Run(strconv.Itoa(version)+"/"+change, func(t *testing.T) {
				spec := partitionedSpec()
				data := partitionStatsFile(t, spec, iceberg.EntryContentData, "data.parquet", map[int]any{1000: int32(2)}, 10).Build()
				tbl := partitionStatisticsTestTable(t, version, partitionStatisticsTestManifest{
					spec: spec, content: iceberg.ManifestContentData,
					entries: []iceberg.ManifestEntry{partitionStatsEntry(data, iceberg.EntryStatusADDED, 1)},
				})
				base := tbl.Metadata()
				current := base
				if change == "head" {
					current = partitionStatsAdvanceBranch(t, base, MainBranch, 11)
				} else if change != "none" {
					builder, err := MetadataBuilderFromBase(base, "")
					require.NoError(t, err)
					switch change {
					case "schema":
						require.NoError(t, builder.AddSchema(iceberg.NewSchema(1,
							iceberg.NestedField{ID: 1, Name: "id", Type: iceberg.PrimitiveTypes.Int32, Required: true},
							iceberg.NestedField{ID: 2, Name: "extra", Type: iceberg.PrimitiveTypes.String})))
						require.NoError(t, builder.SetCurrentSchemaID(-1))
					case "default spec":
						other := iceberg.NewPartitionSpecID(1)
						require.NoError(t, builder.AddPartitionSpec(&other, false))
						require.NoError(t, builder.SetDefaultSpecID(-1))
					case "spec history":
						other := iceberg.NewPartitionSpecID(1, iceberg.PartitionField{
							SourceIDs: []int{1}, FieldID: 1001, Name: "bucket", Transform: iceberg.BucketTransform{NumBuckets: 4},
						})
						require.NoError(t, builder.AddPartitionSpec(&other, false))
					}
					current, err = builder.Build()
					require.NoError(t, err)
				}
				cat := &headTrackingCatalog{metadata: current}
				fs, err := tbl.FS(t.Context())
				require.NoError(t, err)
				tbl = New(tbl.Identifier(), base, tbl.MetadataLocation(), func(context.Context) (iceio.IO, error) { return fs, nil }, cat)
				tx := tbl.NewTransaction()
				stats, err := tx.ComputePartitionStatistics(t.Context())
				require.NoError(t, err)
				_, err = tx.Commit(t.Context())
				if change == "none" {
					require.NoError(t, err)
					require.Equal(t, []PartitionStatisticsFile{*stats}, slices.Collect(cat.metadata.PartitionStatistics()))
					require.Equal(t, base.Snapshots(), cat.metadata.Snapshots(), "statistics registration must not create a snapshot")
				} else {
					require.ErrorIs(t, err, ErrCommitFailed)
					require.Empty(t, slices.Collect(cat.metadata.PartitionStatistics()))
				}
				require.Equal(t, int32(1), cat.attempts.Load())
			})
		}
	}
}

func TestPartitionStatisticsUsesTransactionBranch(t *testing.T) {
	spec := partitionedSpec()
	data := partitionStatsFile(t, spec, iceberg.EntryContentData, "data.parquet", map[int]any{1000: int32(2)}, 10).Build()
	tbl := partitionStatisticsTestTable(t, 3, partitionStatisticsTestManifest{
		spec: spec, content: iceberg.ManifestContentData,
		entries: []iceberg.ManifestEntry{partitionStatsEntry(data, iceberg.EntryStatusADDED, 1)},
	})
	builder, err := MetadataBuilderFromBase(tbl.Metadata(), "")
	require.NoError(t, err)
	require.NoError(t, builder.SetSnapshotRef("feature", 10, BranchRef))
	withBranch, err := builder.Build()
	require.NoError(t, err)
	base := partitionStatsAdvanceBranch(t, withBranch, "feature", 11)
	current := partitionStatsAdvanceBranch(t, base, MainBranch, 12)
	cat := &headTrackingCatalog{metadata: current}
	fs, err := tbl.FS(t.Context())
	require.NoError(t, err)
	tbl = New(tbl.Identifier(), base, tbl.MetadataLocation(), func(context.Context) (iceio.IO, error) { return fs, nil }, cat)
	tx, err := tbl.NewTransactionOnBranch("feature")
	require.NoError(t, err)
	stats, err := tx.ComputePartitionStatistics(t.Context())
	require.NoError(t, err)
	require.EqualValues(t, 11, stats.SnapshotID)
	_, err = tx.Commit(t.Context())
	require.NoError(t, err, "an independent main commit must not invalidate feature's statistics")
	require.EqualValues(t, 12, cat.metadata.SnapshotByName(MainBranch).SnapshotID)
	require.EqualValues(t, 11, cat.metadata.SnapshotByName("feature").SnapshotID)
	require.Equal(t, []PartitionStatisticsFile{*stats}, slices.Collect(cat.metadata.PartitionStatistics()))
}

func TestPartitionStatisticsV2V3FailureAndEmptySnapshots(t *testing.T) {
	for _, version := range []int{2, 3} {
		tbl := partitionStatisticsTestTable(t, version)
		rows := partitionStatsCompute(t, tbl)
		require.Zero(t, rows.NumRows())
		tx := tbl.NewTransaction()
		require.NoError(t, tx.SetProperties(iceberg.Properties{"changed": "true"}))
		_, err := tx.ComputePartitionStatistics(t.Context())
		require.ErrorIs(t, err, iceberg.ErrInvalidArgument)
		ctx, cancel := context.WithCancel(t.Context())
		cancel()
		_, err = tbl.NewTransaction().ComputePartitionStatistics(ctx)
		require.ErrorIs(t, err, context.Canceled)
		fs, err := tbl.FS(t.Context())
		require.NoError(t, err)
		_, err = writePartitionStatisticsParquet(ctx, fs.(iceio.WriteFileIO), "mem://stats/table/metadata/cancelled.parquet", statsTestPartitionType(),
			[]*inspectPartitionAggregate{{partition: map[int]any{1000: int32(2)}}}, version)
		require.ErrorIs(t, err, context.Canceled)
		_, err = fs.Open(t.Context(), "mem://stats/table/metadata/cancelled.parquet")
		require.Error(t, err)
	}
	tbl := partitionStatisticsTestTable(t, 2)
	tx := tbl.NewTransaction()
	tx.meta.formatVersion = 4
	_, err := tx.ComputePartitionStatistics(t.Context())
	require.ErrorIs(t, err, iceberg.ErrNotImplemented)
}

type partitionStatisticsCloseFailureIO struct {
	iceio.WriteFileIO
	failure error
}

func (f partitionStatisticsCloseFailureIO) Create(ctx context.Context, path string) (iceio.FileWriter, error) {
	writer, err := f.WriteFileIO.Create(ctx, path)
	if err != nil {
		return nil, err
	}

	return partitionStatisticsCloseFailureWriter{FileWriter: writer, failure: f.failure}, nil
}

type partitionStatisticsCloseFailureWriter struct {
	iceio.FileWriter
	failure error
}

func (w partitionStatisticsCloseFailureWriter) Close() error {
	if err := w.FileWriter.Close(); err != nil {
		return err
	}

	return w.failure
}

func TestPartitionStatisticsV2V3RemovesFileAfterCloseFailure(t *testing.T) {
	for _, version := range []int{2, 3} {
		fs := iceio.NewMemFS()
		failure := errors.New("injected close failure")
		path := "mem://stats/table/metadata/failed.parquet"
		_, err := writePartitionStatisticsParquet(t.Context(), partitionStatisticsCloseFailureIO{WriteFileIO: fs, failure: failure}, path,
			statsTestPartitionType(), []*inspectPartitionAggregate{{partition: map[int]any{1000: int32(2)}, dataRecordCount: 10}}, version)
		require.ErrorIs(t, err, failure)
		_, err = fs.Open(t.Context(), path)
		require.Error(t, err, "a failed file must not remain available for registration")
	}
}
