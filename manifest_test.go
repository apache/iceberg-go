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
	"bytes"
	"testing"
	"time"

	"github.com/apache/iceberg-go/internal"
	"github.com/hamba/avro/v2/ocf"
	"github.com/stretchr/testify/suite"
)

var (
	falseBool                   = false
	snapshotID            int64 = 9182715666859759686
	fileCount             int32 = 3
	zero                  int32 = 0
	zero64                int64 = 0
	addedRows             int64 = 237993
	manifestFileRecordsV1       = []manifestFileV1{{
		Path:               "/home/iceberg/warehouse/nyc/taxis_partitioned/metadata/0125c686-8aa6-4502-bdcc-b6d17ca41a3b-m0.avro",
		Len:                7989,
		PartitionSpecID:    0,
		AddedSnapshotID:    &snapshotID,
		AddedFilesCount:    &fileCount,
		ExistingFilesCount: &zero,
		DeletedFilesCount:  &zero,
		AddedRowsCount:     &addedRows,
		ExistingRowsCount:  &zero64,
		DeletedRowsCount:   &zero64,
		Partitions: &[]fieldSummary{{
			ContainsNull: true, ContainsNaN: &falseBool,
			LowerBound: &[]byte{0x01, 0x00, 0x00, 0x00},
			UpperBound: &[]byte{0x02, 0x00, 0x00, 0x00},
		}},
	}}

	manifestFileRecordsV2 = []manifestFileV2{{
		Path:               "/home/iceberg/warehouse/nyc/taxis_partitioned/metadata/0125c686-8aa6-4502-bdcc-b6d17ca41a3b-m0.avro",
		Len:                7989,
		PartitionSpecID:    0,
		Content:            ManifestContentDeletes,
		SeqNumber:          3,
		MinSeqNumber:       3,
		AddedSnapshotID:    snapshotID,
		AddedFilesCount:    3,
		ExistingFilesCount: 0,
		DeletedFilesCount:  0,
		AddedRowsCount:     addedRows,
		ExistingRowsCount:  0,
		DeletedRowsCount:   0,
		Partitions: &[]fieldSummary{{
			ContainsNull: true,
			ContainsNaN:  &falseBool,
			LowerBound:   &[]byte{0x01, 0x00, 0x00, 0x00},
			UpperBound:   &[]byte{0x02, 0x00, 0x00, 0x00},
		}},
	}}

	entrySnapshotID        int64 = 8744736658442914487
	intZero                      = 0
	manifestEntryV1Records       = []*manifestEntryV1{
		{
			EntryStatus: EntryStatusADDED,
			Snapshot:    entrySnapshotID,
			Data: dataFile{
				Path:             "/home/iceberg/warehouse/nyc/taxis_partitioned/data/VendorID=null/00000-633-d8a4223e-dc97-45a1-86e1-adaba6e8abd7-00001.parquet",
				Format:           ParquetFile,
				PartitionData:    map[string]any{"VendorID": int(1), "tpep_pickup_datetime": time.Unix(1925, 0)},
				RecordCount:      19513,
				FileSize:         388872,
				BlockSizeInBytes: 67108864,
				ColSizes: &[]colMap[int, int64]{
					{Key: 1, Value: 53},
					{Key: 2, Value: 98153},
					{Key: 3, Value: 98693},
					{Key: 4, Value: 53},
					{Key: 5, Value: 53},
					{Key: 6, Value: 53},
					{Key: 7, Value: 17425},
					{Key: 8, Value: 18528},
					{Key: 9, Value: 53},
					{Key: 10, Value: 44788},
					{Key: 11, Value: 35571},
					{Key: 12, Value: 53},
					{Key: 13, Value: 1243},
					{Key: 14, Value: 2355},
					{Key: 15, Value: 12750},
					{Key: 16, Value: 4029},
					{Key: 17, Value: 110},
					{Key: 18, Value: 47194},
					{Key: 19, Value: 2948},
				},
				ValCounts: &[]colMap[int, int64]{
					{Key: 1, Value: 19513},
					{Key: 2, Value: 19513},
					{Key: 3, Value: 19513},
					{Key: 4, Value: 19513},
					{Key: 5, Value: 19513},
					{Key: 6, Value: 19513},
					{Key: 7, Value: 19513},
					{Key: 8, Value: 19513},
					{Key: 9, Value: 19513},
					{Key: 10, Value: 19513},
					{Key: 11, Value: 19513},
					{Key: 12, Value: 19513},
					{Key: 13, Value: 19513},
					{Key: 14, Value: 19513},
					{Key: 15, Value: 19513},
					{Key: 16, Value: 19513},
					{Key: 17, Value: 19513},
					{Key: 18, Value: 19513},
					{Key: 19, Value: 19513},
				},
				NullCounts: &[]colMap[int, int64]{
					{Key: 1, Value: 19513},
					{Key: 2, Value: 0},
					{Key: 3, Value: 0},
					{Key: 4, Value: 19513},
					{Key: 5, Value: 19513},
					{Key: 6, Value: 19513},
					{Key: 7, Value: 0},
					{Key: 8, Value: 0},
					{Key: 9, Value: 19513},
					{Key: 10, Value: 0},
					{Key: 11, Value: 0},
					{Key: 12, Value: 19513},
					{Key: 13, Value: 0},
					{Key: 14, Value: 0},
					{Key: 15, Value: 0},
					{Key: 16, Value: 0},
					{Key: 17, Value: 0},
					{Key: 18, Value: 0},
					{Key: 19, Value: 0},
				},
				NaNCounts: &[]colMap[int, int64]{
					{Key: 16, Value: 0},
					{Key: 17, Value: 0},
					{Key: 18, Value: 0},
					{Key: 19, Value: 0},
					{Key: 10, Value: 0},
					{Key: 11, Value: 0},
					{Key: 12, Value: 0},
					{Key: 13, Value: 0},
					{Key: 14, Value: 0},
					{Key: 15, Value: 0},
				},
				LowerBounds: &[]colMap[int, []byte]{
					{Key: 2, Value: []byte("2020-04-01 00:00")},
					{Key: 3, Value: []byte("2020-04-01 00:12")},
					{Key: 7, Value: []byte{0x03, 0x00, 0x00, 0x00}},
					{Key: 8, Value: []byte{0x01, 0x00, 0x00, 0x00}},
					{Key: 10, Value: []byte{0xf6, 0x28, 0x5c, 0x8f, 0xc2, 0x05, 'S', 0xc0}},
					{Key: 11, Value: []byte{0, 0, 0, 0, 0, 0, 0, 0}},
					{Key: 13, Value: []byte{0, 0, 0, 0, 0, 0, 0, 0}},
					{Key: 14, Value: []byte{0, 0, 0, 0, 0, 0, 0xe0, 0xbf}},
					{Key: 15, Value: []byte{')', '\\', 0x8f, 0xc2, 0xf5, '(', 0x08, 0xc0}},
					{Key: 16, Value: []byte{0, 0, 0, 0, 0, 0, 0, 0}},
					{Key: 17, Value: []byte{0, 0, 0, 0, 0, 0, 0, 0}},
					{Key: 18, Value: []byte{0xf6, '(', '\\', 0x8f, 0xc2, 0xc5, 'S', 0xc0}},
					{Key: 19, Value: []byte{0, 0, 0, 0, 0, 0, 0x04, 0xc0}},
				},
				UpperBounds: &[]colMap[int, []byte]{
					{Key: 2, Value: []byte("2020-04-30 23:5:")},
					{Key: 3, Value: []byte("2020-05-01 00:41")},
					{Key: 7, Value: []byte{'\t', 0x01, 0x00, 0x00}},
					{Key: 8, Value: []byte{'\t', 0x01, 0x00, 0x00}},
					{Key: 10, Value: []byte{0xcd, 0xcc, 0xcc, 0xcc, 0xcc, ',', '_', '@'}},
					{Key: 11, Value: []byte{0x1f, 0x85, 0xeb, 'Q', '\\', 0xe2, 0xfe, '@'}},
					{Key: 13, Value: []byte{0, 0, 0, 0, 0, 0, 0x12, '@'}},
					{Key: 14, Value: []byte{0, 0, 0, 0, 0, 0, 0xe0, '?'}},
					{Key: 15, Value: []byte{'q', '=', '\n', 0xd7, 0xa3, 0xf0, '1', '@'}},
					{Key: 16, Value: []byte{0, 0, 0, 0, 0, '`', 'B', '@'}},
					{Key: 17, Value: []byte{'3', '3', '3', '3', '3', '3', 0xd3, '?'}},
					{Key: 18, Value: []byte{0, 0, 0, 0, 0, 0x18, 'b', '@'}},
					{Key: 19, Value: []byte{0, 0, 0, 0, 0, 0, 0x04, '@'}},
				},
				SplitOffsets: &[]int64{4},
				SortOrder:    &intZero,
			},
		},
		{
			EntryStatus: EntryStatusADDED,
			Snapshot:    8744736658442914487,
			Data: dataFile{
				Path:             "/home/iceberg/warehouse/nyc/taxis_partitioned/data/VendorID=1/00000-633-d8a4223e-dc97-45a1-86e1-adaba6e8abd7-00002.parquet",
				Format:           ParquetFile,
				PartitionData:    map[string]any{"VendorID": int(1), "tpep_pickup_datetime": time.Unix(1925, 0)},
				RecordCount:      95050,
				FileSize:         1265950,
				BlockSizeInBytes: 67108864,
				ColSizes: &[]colMap[int, int64]{
					{Key: 1, Value: 318},
					{Key: 2, Value: 329806},
					{Key: 3, Value: 331632},
					{Key: 4, Value: 15343},
					{Key: 5, Value: 2351},
					{Key: 6, Value: 3389},
					{Key: 7, Value: 71269},
					{Key: 8, Value: 76429},
					{Key: 9, Value: 16383},
					{Key: 10, Value: 86992},
					{Key: 11, Value: 89608},
					{Key: 12, Value: 265},
					{Key: 13, Value: 19377},
					{Key: 14, Value: 1692},
					{Key: 15, Value: 76162},
					{Key: 16, Value: 4354},
					{Key: 17, Value: 759},
					{Key: 18, Value: 120650},
					{Key: 19, Value: 11804},
				},
				ValCounts: &[]colMap[int, int64]{
					{Key: 1, Value: 95050},
					{Key: 2, Value: 95050},
					{Key: 3, Value: 95050},
					{Key: 4, Value: 95050},
					{Key: 5, Value: 95050},
					{Key: 6, Value: 95050},
					{Key: 7, Value: 95050},
					{Key: 8, Value: 95050},
					{Key: 9, Value: 95050},
					{Key: 10, Value: 95050},
					{Key: 11, Value: 95050},
					{Key: 12, Value: 95050},
					{Key: 13, Value: 95050},
					{Key: 14, Value: 95050},
					{Key: 15, Value: 95050},
					{Key: 16, Value: 95050},
					{Key: 17, Value: 95050},
					{Key: 18, Value: 95050},
					{Key: 19, Value: 95050},
				},
				NullCounts: &[]colMap[int, int64]{
					{Key: 1, Value: 0},
					{Key: 2, Value: 0},
					{Key: 3, Value: 0},
					{Key: 4, Value: 0},
					{Key: 5, Value: 0},
					{Key: 6, Value: 0},
					{Key: 7, Value: 0},
					{Key: 8, Value: 0},
					{Key: 9, Value: 0},
					{Key: 10, Value: 0},
					{Key: 11, Value: 0},
					{Key: 12, Value: 95050},
					{Key: 13, Value: 0},
					{Key: 14, Value: 0},
					{Key: 15, Value: 0},
					{Key: 16, Value: 0},
					{Key: 17, Value: 0},
					{Key: 18, Value: 0},
					{Key: 19, Value: 0},
				},
				NaNCounts: &[]colMap[int, int64]{
					{Key: 16, Value: 0},
					{Key: 17, Value: 0},
					{Key: 18, Value: 0},
					{Key: 19, Value: 0},
					{Key: 10, Value: 0},
					{Key: 11, Value: 0},
					{Key: 12, Value: 0},
					{Key: 13, Value: 0},
					{Key: 14, Value: 0},
					{Key: 15, Value: 0},
				},
				LowerBounds: &[]colMap[int, []byte]{
					{Key: 1, Value: []byte{0x01, 0x00, 0x00, 0x00}},
					{Key: 2, Value: []byte("2020-04-01 00:00")},
					{Key: 3, Value: []byte("2020-04-01 00:13")},
					{Key: 4, Value: []byte{0x00, 0x00, 0x00, 0x00}},
					{Key: 5, Value: []byte{0x01, 0x00, 0x00, 0x00}},
					{Key: 6, Value: []byte("N")},
					{Key: 7, Value: []byte{0x01, 0x00, 0x00, 0x00}},
					{Key: 8, Value: []byte{0x01, 0x00, 0x00, 0x00}},
					{Key: 9, Value: []byte{0x01, 0x00, 0x00, 0x00}},
					{Key: 10, Value: []byte{0, 0, 0, 0, 0, 0, 0, 0}},
					{Key: 11, Value: []byte{0, 0, 0, 0, 0, 0, 0, 0}},
					{Key: 13, Value: []byte{0, 0, 0, 0, 0, 0, 0, 0}},
					{Key: 14, Value: []byte{0, 0, 0, 0, 0, 0, 0, 0}},
					{Key: 15, Value: []byte{0, 0, 0, 0, 0, 0, 0, 0}},
					{Key: 16, Value: []byte{0, 0, 0, 0, 0, 0, 0, 0}},
					{Key: 17, Value: []byte{0, 0, 0, 0, 0, 0, 0, 0}},
					{Key: 18, Value: []byte{0, 0, 0, 0, 0, 0, 0, 0}},
					{Key: 19, Value: []byte{0, 0, 0, 0, 0, 0, 0, 0}},
				},
				UpperBounds: &[]colMap[int, []byte]{
					{Key: 1, Value: []byte{0x01, 0x00, 0x00, 0x00}},
					{Key: 2, Value: []byte("2020-04-30 23:5:")},
					{Key: 3, Value: []byte("2020-05-01 00:1:")},
					{Key: 4, Value: []byte{0x06, 0x00, 0x00, 0x00}},
					{Key: 5, Value: []byte{'c', 0x00, 0x00, 0x00}},
					{Key: 6, Value: []byte("Y")},
					{Key: 7, Value: []byte{'\t', 0x01, 0x00, 0x00}},
					{Key: 8, Value: []byte{'\t', 0x01, 0x00, 0x00}},
					{Key: 9, Value: []byte{0x04, 0x01, 0x00, 0x00}},
					{Key: 10, Value: []byte{'\\', 0x8f, 0xc2, 0xf5, '(', '8', 0x8c, '@'}},
					{Key: 11, Value: []byte{0xcd, 0xcc, 0xcc, 0xcc, 0xcc, ',', 'f', '@'}},
					{Key: 13, Value: []byte{0, 0, 0, 0, 0, 0, 0x1c, '@'}},
					{Key: 14, Value: []byte{0x9a, 0x99, 0x99, 0x99, 0x99, 0x99, 0xf1, '?'}},
					{Key: 15, Value: []byte{0, 0, 0, 0, 0, 0, 'Y', '@'}},
					{Key: 16, Value: []byte{0, 0, 0, 0, 0, 0xb0, 'X', '@'}},
					{Key: 17, Value: []byte{'3', '3', '3', '3', '3', '3', 0xd3, '?'}},
					{Key: 18, Value: []byte{0xc3, 0xf5, '(', '\\', 0x8f, ':', 0x8c, '@'}},
					{Key: 19, Value: []byte{0, 0, 0, 0, 0, 0, 0x04, '@'}},
				},
				SplitOffsets: &[]int64{4},
				SortOrder:    &intZero,
			},
		},
	}

	manifestEntryV2Records = []*manifestEntryV2{
		{
			EntryStatus: EntryStatusADDED,
			Snapshot:    &entrySnapshotID,
			Data: dataFile{
				Path:             manifestEntryV1Records[0].Data.Path,
				Format:           manifestEntryV1Records[0].Data.Format,
				PartitionData:    manifestEntryV1Records[0].Data.PartitionData,
				RecordCount:      manifestEntryV1Records[0].Data.RecordCount,
				FileSize:         manifestEntryV1Records[0].Data.FileSize,
				BlockSizeInBytes: manifestEntryV1Records[0].Data.BlockSizeInBytes,
				ColSizes:         manifestEntryV1Records[0].Data.ColSizes,
				ValCounts:        manifestEntryV1Records[0].Data.ValCounts,
				NullCounts:       manifestEntryV1Records[0].Data.NullCounts,
				NaNCounts:        manifestEntryV1Records[0].Data.NaNCounts,
				LowerBounds:      manifestEntryV1Records[0].Data.LowerBounds,
				UpperBounds:      manifestEntryV1Records[0].Data.UpperBounds,
				SplitOffsets:     manifestEntryV1Records[0].Data.SplitOffsets,
				SortOrder:        manifestEntryV1Records[0].Data.SortOrder,
			},
		},
		{
			EntryStatus: EntryStatusADDED,
			Snapshot:    &entrySnapshotID,
			Data: dataFile{
				Path:             manifestEntryV1Records[1].Data.Path,
				Format:           manifestEntryV1Records[1].Data.Format,
				PartitionData:    manifestEntryV1Records[1].Data.PartitionData,
				RecordCount:      manifestEntryV1Records[1].Data.RecordCount,
				FileSize:         manifestEntryV1Records[1].Data.FileSize,
				BlockSizeInBytes: manifestEntryV1Records[1].Data.BlockSizeInBytes,
				ColSizes:         manifestEntryV1Records[1].Data.ColSizes,
				ValCounts:        manifestEntryV1Records[1].Data.ValCounts,
				NullCounts:       manifestEntryV1Records[1].Data.NullCounts,
				NaNCounts:        manifestEntryV1Records[1].Data.NaNCounts,
				LowerBounds:      manifestEntryV1Records[1].Data.LowerBounds,
				UpperBounds:      manifestEntryV1Records[1].Data.UpperBounds,
				SplitOffsets:     manifestEntryV1Records[1].Data.SplitOffsets,
				SortOrder:        manifestEntryV1Records[1].Data.SortOrder,
			},
		},
	}
)

type ManifestTestSuite struct {
	suite.Suite

	v1ManifestList    bytes.Buffer
	v1ManifestEntries bytes.Buffer

	v2ManifestList    bytes.Buffer
	v2ManifestEntries bytes.Buffer
}

func (m *ManifestTestSuite) writeManifestList() {
	enc, err := ocf.NewEncoder(internal.AvroSchemaCache.Get(internal.ManifestListV1Key).String(),
		&m.v1ManifestList, ocf.WithMetadata(map[string][]byte{
			"avro.codec": []byte("deflate"),
		}),
		ocf.WithCodec(ocf.Deflate))
	m.Require().NoError(err)

	m.Require().NoError(enc.Encode(manifestFileRecordsV1[0]))
	enc.Close()

	enc, err = ocf.NewEncoder(internal.AvroSchemaCache.Get(internal.ManifestListV2Key).String(),
		&m.v2ManifestList, ocf.WithMetadata(map[string][]byte{
			"format-version": []byte("2"),
			"avro.codec":     []byte("deflate"),
		}), ocf.WithCodec(ocf.Deflate))
	m.Require().NoError(err)

	m.Require().NoError(enc.Encode(manifestFileRecordsV2[0]))
	enc.Close()
}

func (m *ManifestTestSuite) writeManifestEntries() {
	enc, err := ocf.NewEncoder(internal.AvroSchemaCache.Get(internal.ManifestEntryV1Key).String(), &m.v1ManifestEntries,
		ocf.WithMetadata(map[string][]byte{
			"format-version": []byte("1"),
		}), ocf.WithCodec(ocf.Deflate))
	m.Require().NoError(err)

	for _, ent := range manifestEntryV1Records {
		m.Require().NoError(enc.Encode(ent))
	}
	m.Require().NoError(enc.Close())

	enc, err = ocf.NewEncoder(internal.AvroSchemaCache.Get(internal.ManifestEntryV2Key).String(),
		&m.v2ManifestEntries, ocf.WithMetadata(map[string][]byte{
			"format-version": []byte("2"),
			"avro.codec":     []byte("deflate"),
		}), ocf.WithCodec(ocf.Deflate))
	m.Require().NoError(err)

	for _, ent := range manifestEntryV2Records {
		m.Require().NoError(enc.Encode(ent))
	}
	m.Require().NoError(enc.Close())
}

func (m *ManifestTestSuite) SetupSuite() {
	m.writeManifestList()
	m.writeManifestEntries()
}

func (m *ManifestTestSuite) TestManifestEntriesV1() {
	var mockfs internal.MockFS
	manifest := manifestFileV1{
		Path: manifestFileRecordsV1[0].Path,
	}

	mockfs.Test(m.T())
	mockfs.On("Open", manifest.FilePath()).Return(&internal.MockFile{
		Contents: bytes.NewReader(m.v1ManifestEntries.Bytes())}, nil)
	defer mockfs.AssertExpectations(m.T())
	entries, err := manifest.FetchEntries(&mockfs, false)
	m.Require().NoError(err)
	m.Len(entries, 2)
	m.Zero(manifest.PartitionID())
	m.Zero(manifest.SnapshotID())
	m.Zero(manifest.AddedDataFiles())
	m.Zero(manifest.ExistingDataFiles())
	m.Zero(manifest.DeletedDataFiles())
	m.Zero(manifest.ExistingRows())
	m.Zero(manifest.DeletedRows())
	m.Zero(manifest.AddedRows())

	entry1 := entries[0]

	m.Equal(EntryStatusADDED, entry1.Status())
	m.EqualValues(8744736658442914487, entry1.SnapshotID())
	m.Zero(entry1.SequenceNum())
	m.Nil(entry1.FileSequenceNum())

	datafile := entry1.DataFile()
	m.Equal(EntryContentData, datafile.ContentType())
	m.Equal("/home/iceberg/warehouse/nyc/taxis_partitioned/data/VendorID=null/00000-633-d8a4223e-dc97-45a1-86e1-adaba6e8abd7-00001.parquet", datafile.FilePath())
	m.Equal(ParquetFile, datafile.FileFormat())
	m.EqualValues(19513, datafile.Count())
	m.EqualValues(388872, datafile.FileSizeBytes())
	m.Equal(map[int]int64{
		1:  53,
		2:  98153,
		3:  98693,
		4:  53,
		5:  53,
		6:  53,
		7:  17425,
		8:  18528,
		9:  53,
		10: 44788,
		11: 35571,
		12: 53,
		13: 1243,
		14: 2355,
		15: 12750,
		16: 4029,
		17: 110,
		18: 47194,
		19: 2948,
	}, datafile.ColumnSizes())
	m.Equal(map[int]int64{
		1:  19513,
		2:  19513,
		3:  19513,
		4:  19513,
		5:  19513,
		6:  19513,
		7:  19513,
		8:  19513,
		9:  19513,
		10: 19513,
		11: 19513,
		12: 19513,
		13: 19513,
		14: 19513,
		15: 19513,
		16: 19513,
		17: 19513,
		18: 19513,
		19: 19513,
	}, datafile.ValueCounts())
	m.Equal(map[int]int64{
		1:  19513,
		2:  0,
		3:  0,
		4:  19513,
		5:  19513,
		6:  19513,
		7:  0,
		8:  0,
		9:  19513,
		10: 0,
		11: 0,
		12: 19513,
		13: 0,
		14: 0,
		15: 0,
		16: 0,
		17: 0,
		18: 0,
		19: 0,
	}, datafile.NullValueCounts())
	m.Equal(map[int]int64{
		16: 0, 17: 0, 18: 0, 19: 0, 10: 0, 11: 0, 12: 0, 13: 0, 14: 0, 15: 0,
	}, datafile.NaNValueCounts())

	m.Equal(map[int][]byte{
		2:  []byte("2020-04-01 00:00"),
		3:  []byte("2020-04-01 00:12"),
		7:  {0x03, 0x00, 0x00, 0x00},
		8:  {0x01, 0x00, 0x00, 0x00},
		10: {0xf6, '(', '\\', 0x8f, 0xc2, 0x05, 'S', 0xc0},
		11: {0, 0, 0, 0, 0, 0, 0, 0},
		13: {0, 0, 0, 0, 0, 0, 0, 0},
		14: {0, 0, 0, 0, 0, 0, 0xe0, 0xbf},
		15: {')', '\\', 0x8f, 0xc2, 0xf5, '(', 0x08, 0xc0},
		16: {0, 0, 0, 0, 0, 0, 0, 0},
		17: {0, 0, 0, 0, 0, 0, 0, 0},
		18: {0xf6, '(', '\\', 0x8f, 0xc2, 0xc5, 'S', 0xc0},
		19: {0, 0, 0, 0, 0, 0, 0x04, 0xc0},
	}, datafile.LowerBoundValues())

	m.Equal(map[int][]byte{
		2:  []byte("2020-04-30 23:5:"),
		3:  []byte("2020-05-01 00:41"),
		7:  {'\t', 0x01, 0, 0},
		8:  {'\t', 0x01, 0, 0},
		10: {0xcd, 0xcc, 0xcc, 0xcc, 0xcc, ',', '_', '@'},
		11: {0x1f, 0x85, 0xeb, 'Q', '\\', 0xe2, 0xfe, '@'},
		13: {0, 0, 0, 0, 0, 0, 0x12, '@'},
		14: {0, 0, 0, 0, 0, 0, 0xe0, '?'},
		15: {'q', '=', '\n', 0xd7, 0xa3, 0xf0, '1', '@'},
		16: {0, 0, 0, 0, 0, '`', 'B', '@'},
		17: {'3', '3', '3', '3', '3', '3', 0xd3, '?'},
		18: {0, 0, 0, 0, 0, 0x18, 'b', '@'},
		19: {0, 0, 0, 0, 0, 0, 0x04, '@'},
	}, datafile.UpperBoundValues())

	m.Nil(datafile.MetadataKey())
	m.Equal([]int64{4}, datafile.Splits())
	m.Nil(datafile.EqualityFieldIDs())
	m.Zero(*datafile.SortOrderID())
}

func (m *ManifestTestSuite) TestReadManifestListV1() {
	list, err := ReadManifestList(&m.v1ManifestList)
	m.Require().NoError(err)

	m.Len(list, 1)
	m.Equal(1, list[0].Version())
	m.EqualValues(7989, list[0].Length())
	m.Equal(ManifestContentData, list[0].ManifestContent())
	m.Zero(list[0].SequenceNum())
	m.Zero(list[0].MinSequenceNum())
	m.EqualValues(9182715666859759686, list[0].SnapshotID())
	m.EqualValues(3, list[0].AddedDataFiles())
	m.True(list[0].HasAddedFiles())
	m.Zero(list[0].ExistingDataFiles())
	m.False(list[0].HasExistingFiles())
	m.Zero(list[0].DeletedDataFiles())
	m.Equal(addedRows, list[0].AddedRows())
	m.Zero(list[0].ExistingRows())
	m.Zero(list[0].DeletedRows())
	m.Nil(list[0].Metadata())
	m.Zero(list[0].PartitionID())
	m.Equal(snapshotID, list[0].SnapshotID())

	part := list[0].PartitionList()[0]
	m.True(part.ContainsNull)
	m.False(*part.ContainsNaN)
	m.Equal([]byte{0x01, 0x00, 0x00, 0x00}, *part.LowerBound)
	m.Equal([]byte{0x02, 0x00, 0x00, 0x00}, *part.UpperBound)
}

func (m *ManifestTestSuite) TestReadManifestListV2() {
	list, err := ReadManifestList(&m.v2ManifestList)
	m.Require().NoError(err)

	m.Equal("/home/iceberg/warehouse/nyc/taxis_partitioned/metadata/0125c686-8aa6-4502-bdcc-b6d17ca41a3b-m0.avro", list[0].FilePath())
	m.Len(list, 1)
	m.Equal(2, list[0].Version())
	m.EqualValues(7989, list[0].Length())
	m.Equal(ManifestContentDeletes, list[0].ManifestContent())
	m.EqualValues(3, list[0].SequenceNum())
	m.EqualValues(3, list[0].MinSequenceNum())
	m.EqualValues(9182715666859759686, list[0].SnapshotID())
	m.EqualValues(3, list[0].AddedDataFiles())
	m.True(list[0].HasAddedFiles())
	m.Zero(list[0].ExistingDataFiles())
	m.False(list[0].HasExistingFiles())
	m.Zero(list[0].DeletedDataFiles())
	m.Equal(addedRows, list[0].AddedRows())
	m.Zero(list[0].ExistingRows())
	m.Zero(list[0].DeletedRows())
	m.Nil(list[0].Metadata())
	m.Zero(list[0].PartitionID())

	part := list[0].PartitionList()[0]
	m.True(part.ContainsNull)
	m.False(*part.ContainsNaN)
	m.Equal([]byte{0x01, 0x00, 0x00, 0x00}, *part.LowerBound)
	m.Equal([]byte{0x02, 0x00, 0x00, 0x00}, *part.UpperBound)
}

func (m *ManifestTestSuite) TestManifestEntriesV2() {
	var mockfs internal.MockFS
	manifest := manifestFileV2{
		Path: manifestFileRecordsV2[0].Path,
	}

	mockfs.Test(m.T())
	mockfs.On("Open", manifest.FilePath()).Return(&internal.MockFile{
		Contents: bytes.NewReader(m.v2ManifestEntries.Bytes())}, nil)
	defer mockfs.AssertExpectations(m.T())
	entries, err := manifest.FetchEntries(&mockfs, false)
	m.Require().NoError(err)
	m.Len(entries, 2)
	m.Zero(manifest.PartitionID())
	m.Zero(manifest.SnapshotID())
	m.Zero(manifest.AddedDataFiles())
	m.Zero(manifest.ExistingDataFiles())
	m.Zero(manifest.DeletedDataFiles())
	m.Zero(manifest.ExistingRows())
	m.Zero(manifest.DeletedRows())
	m.Zero(manifest.AddedRows())

	entry1 := entries[0]

	m.Equal(EntryStatusADDED, entry1.Status())
	m.Equal(entrySnapshotID, entry1.SnapshotID())
	m.Zero(entry1.SequenceNum())
	m.Zero(*entry1.FileSequenceNum())

	datafile := entry1.DataFile()
	m.Equal(EntryContentData, datafile.ContentType())
	m.Equal("/home/iceberg/warehouse/nyc/taxis_partitioned/data/VendorID=null/00000-633-d8a4223e-dc97-45a1-86e1-adaba6e8abd7-00001.parquet", datafile.FilePath())
	m.Equal(ParquetFile, datafile.FileFormat())
	m.EqualValues(19513, datafile.Count())
	m.EqualValues(388872, datafile.FileSizeBytes())
	m.Equal(map[int]int64{
		1:  53,
		2:  98153,
		3:  98693,
		4:  53,
		5:  53,
		6:  53,
		7:  17425,
		8:  18528,
		9:  53,
		10: 44788,
		11: 35571,
		12: 53,
		13: 1243,
		14: 2355,
		15: 12750,
		16: 4029,
		17: 110,
		18: 47194,
		19: 2948,
	}, datafile.ColumnSizes())
	m.Equal(map[int]int64{
		1:  19513,
		2:  19513,
		3:  19513,
		4:  19513,
		5:  19513,
		6:  19513,
		7:  19513,
		8:  19513,
		9:  19513,
		10: 19513,
		11: 19513,
		12: 19513,
		13: 19513,
		14: 19513,
		15: 19513,
		16: 19513,
		17: 19513,
		18: 19513,
		19: 19513,
	}, datafile.ValueCounts())
	m.Equal(map[int]int64{
		1:  19513,
		2:  0,
		3:  0,
		4:  19513,
		5:  19513,
		6:  19513,
		7:  0,
		8:  0,
		9:  19513,
		10: 0,
		11: 0,
		12: 19513,
		13: 0,
		14: 0,
		15: 0,
		16: 0,
		17: 0,
		18: 0,
		19: 0,
	}, datafile.NullValueCounts())
	m.Equal(map[int]int64{
		16: 0, 17: 0, 18: 0, 19: 0, 10: 0, 11: 0, 12: 0, 13: 0, 14: 0, 15: 0,
	}, datafile.NaNValueCounts())

	m.Equal(map[int][]byte{
		2:  []byte("2020-04-01 00:00"),
		3:  []byte("2020-04-01 00:12"),
		7:  {0x03, 0x00, 0x00, 0x00},
		8:  {0x01, 0x00, 0x00, 0x00},
		10: {0xf6, '(', '\\', 0x8f, 0xc2, 0x05, 'S', 0xc0},
		11: {0, 0, 0, 0, 0, 0, 0, 0},
		13: {0, 0, 0, 0, 0, 0, 0, 0},
		14: {0, 0, 0, 0, 0, 0, 0xe0, 0xbf},
		15: {')', '\\', 0x8f, 0xc2, 0xf5, '(', 0x08, 0xc0},
		16: {0, 0, 0, 0, 0, 0, 0, 0},
		17: {0, 0, 0, 0, 0, 0, 0, 0},
		18: {0xf6, '(', '\\', 0x8f, 0xc2, 0xc5, 'S', 0xc0},
		19: {0, 0, 0, 0, 0, 0, 0x04, 0xc0},
	}, datafile.LowerBoundValues())

	m.Equal(map[int][]byte{
		2:  []byte("2020-04-30 23:5:"),
		3:  []byte("2020-05-01 00:41"),
		7:  {'\t', 0x01, 0, 0},
		8:  {'\t', 0x01, 0, 0},
		10: {0xcd, 0xcc, 0xcc, 0xcc, 0xcc, ',', '_', '@'},
		11: {0x1f, 0x85, 0xeb, 'Q', '\\', 0xe2, 0xfe, '@'},
		13: {0, 0, 0, 0, 0, 0, 0x12, '@'},
		14: {0, 0, 0, 0, 0, 0, 0xe0, '?'},
		15: {'q', '=', '\n', 0xd7, 0xa3, 0xf0, '1', '@'},
		16: {0, 0, 0, 0, 0, '`', 'B', '@'},
		17: {'3', '3', '3', '3', '3', '3', 0xd3, '?'},
		18: {0, 0, 0, 0, 0, 0x18, 'b', '@'},
		19: {0, 0, 0, 0, 0, 0, 0x04, '@'},
	}, datafile.UpperBoundValues())

	m.Nil(datafile.MetadataKey())
	m.Equal([]int64{4}, datafile.Splits())
	m.Nil(datafile.EqualityFieldIDs())
	m.Zero(*datafile.SortOrderID())
}

func TestManifests(t *testing.T) {
	suite.Run(t, new(ManifestTestSuite))
}
