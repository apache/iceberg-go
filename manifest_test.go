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
	"io"
	"testing"
	"time"

	"github.com/apache/iceberg-go/internal"
	"github.com/hamba/avro/v2"
	"github.com/hamba/avro/v2/ocf"
	"github.com/stretchr/testify/suite"
)

var (
	falseBool                   = false
	snapshotID            int64 = 9182715666859759686
	addedRows             int64 = 237993
	manifestFileRecordsV1       = []ManifestFile{
		NewManifestFile(1, "/home/iceberg/warehouse/nyc/taxis_partitioned/metadata/0125c686-8aa6-4502-bdcc-b6d17ca41a3b-m0.avro",
			7989, 0, snapshotID).
			AddedFiles(3).
			ExistingFiles(0).
			DeletedFiles(0).
			AddedRows(addedRows).
			ExistingRows(0).
			DeletedRows(0).
			Partitions([]FieldSummary{{
				ContainsNull: true, ContainsNaN: &falseBool,
				LowerBound: &[]byte{0x01, 0x00, 0x00, 0x00},
				UpperBound: &[]byte{0x02, 0x00, 0x00, 0x00},
			}}).Build(),
	}

	manifestFileRecordsV2 = []ManifestFile{
		NewManifestFile(2, "/home/iceberg/warehouse/nyc/taxis_partitioned/metadata/0125c686-8aa6-4502-bdcc-b6d17ca41a3b-m0.avro",
			7989, 0, snapshotID).
			Content(ManifestContentDeletes).
			SequenceNum(3, 3).
			AddedFiles(3).
			ExistingFiles(0).
			DeletedFiles(0).
			AddedRows(addedRows).
			ExistingRows(0).
			DeletedRows(0).
			Partitions([]FieldSummary{{
				ContainsNull: true,
				ContainsNaN:  &falseBool,
				LowerBound:   &[]byte{0x01, 0x00, 0x00, 0x00},
				UpperBound:   &[]byte{0x02, 0x00, 0x00, 0x00},
			}}).Build(),
	}

	entrySnapshotID        int64 = 8744736658442914487
	intZero                      = 0
	manifestEntryV1Records       = []*manifestEntry{
		{
			EntryStatus: EntryStatusADDED,
			Snapshot:    &entrySnapshotID,
			Data: &dataFile{
				// bad value for Content but this field doesn't exist in V1
				// so it shouldn't get written and shouldn't be read back out
				// so the roundtrip test asserts that we get the default value
				// back out.
				Content:          EntryContentEqDeletes,
				Path:             "/home/iceberg/warehouse/nyc/taxis_partitioned/data/VendorID=null/00000-633-d8a4223e-dc97-45a1-86e1-adaba6e8abd7-00001.parquet",
				Format:           ParquetFile,
				PartitionData:    map[string]any{"VendorID": int(1), "tpep_pickup_datetime": time.Unix(1925, 0).UnixMicro()},
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
				Splits:    &[]int64{4},
				SortOrder: &intZero,
			},
		},
		{
			EntryStatus: EntryStatusADDED,
			Snapshot:    &entrySnapshotID,
			Data: &dataFile{
				Path:             "/home/iceberg/warehouse/nyc/taxis_partitioned/data/VendorID=1/00000-633-d8a4223e-dc97-45a1-86e1-adaba6e8abd7-00002.parquet",
				Format:           ParquetFile,
				PartitionData:    map[string]any{"VendorID": int(1), "tpep_pickup_datetime": time.Unix(1925, 0).UnixMicro()},
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
				Splits:    &[]int64{4},
				SortOrder: &intZero,
			},
		},
	}

	dataRecord0 = manifestEntryV1Records[0].Data.(*dataFile)
	dataRecord1 = manifestEntryV1Records[1].Data.(*dataFile)

	manifestEntryV2Records = []*manifestEntry{
		{
			EntryStatus: EntryStatusADDED,
			Snapshot:    &entrySnapshotID,
			Data: &dataFile{
				Path:             dataRecord0.Path,
				Format:           dataRecord0.Format,
				PartitionData:    dataRecord0.PartitionData,
				RecordCount:      dataRecord0.RecordCount,
				FileSize:         dataRecord0.FileSize,
				BlockSizeInBytes: dataRecord0.BlockSizeInBytes,
				ColSizes:         dataRecord0.ColSizes,
				ValCounts:        dataRecord0.ValCounts,
				NullCounts:       dataRecord0.NullCounts,
				NaNCounts:        dataRecord0.NaNCounts,
				LowerBounds:      dataRecord0.LowerBounds,
				UpperBounds:      dataRecord0.UpperBounds,
				Splits:           dataRecord0.Splits,
				SortOrder:        dataRecord0.SortOrder,
			},
		},
		{
			EntryStatus: EntryStatusADDED,
			Snapshot:    &entrySnapshotID,
			Data: &dataFile{
				Path:             dataRecord1.Path,
				Format:           dataRecord1.Format,
				PartitionData:    dataRecord1.PartitionData,
				RecordCount:      dataRecord1.RecordCount,
				FileSize:         dataRecord1.FileSize,
				BlockSizeInBytes: dataRecord1.BlockSizeInBytes,
				ColSizes:         dataRecord1.ColSizes,
				ValCounts:        dataRecord1.ValCounts,
				NullCounts:       dataRecord1.NullCounts,
				NaNCounts:        dataRecord1.NaNCounts,
				LowerBounds:      dataRecord1.LowerBounds,
				UpperBounds:      dataRecord1.UpperBounds,
				Splits:           dataRecord1.Splits,
				SortOrder:        dataRecord1.SortOrder,
			},
		},
	}

	testSchema = NewSchema(0,
		NestedField{ID: 1, Name: "VendorID", Type: PrimitiveTypes.Int32, Required: true},
		NestedField{ID: 2, Name: "tpep_pickup_datetime", Type: PrimitiveTypes.Timestamp, Required: true},
		NestedField{ID: 3, Name: "tpep_dropoff_datetime", Type: PrimitiveTypes.Timestamp, Required: true},
		NestedField{ID: 4, Name: "passenger_count", Type: PrimitiveTypes.Int64, Required: false},
		NestedField{ID: 5, Name: "trip_distance", Type: PrimitiveTypes.Float64, Required: true},
		NestedField{ID: 6, Name: "RatecodeID", Type: PrimitiveTypes.Int64, Required: false},
		NestedField{ID: 7, Name: "store_and_fwd_flag", Type: PrimitiveTypes.String, Required: false},
		NestedField{ID: 8, Name: "PULocationID", Type: PrimitiveTypes.Int32, Required: false},
		NestedField{ID: 9, Name: "DOLocationID", Type: PrimitiveTypes.Int32, Required: false},
		NestedField{ID: 10, Name: "payment_type", Type: PrimitiveTypes.Int64, Required: true},
		NestedField{ID: 11, Name: "fare_amount", Type: PrimitiveTypes.Float64, Required: true},
		NestedField{ID: 12, Name: "extra", Type: PrimitiveTypes.Float64, Required: false},
		NestedField{ID: 13, Name: "mta_tax", Type: PrimitiveTypes.Float64, Required: false},
		NestedField{ID: 14, Name: "tip_amount", Type: PrimitiveTypes.Float64, Required: false},
		NestedField{ID: 15, Name: "tolls_amount", Type: PrimitiveTypes.Float64, Required: false},
		NestedField{ID: 16, Name: "improvement_surcharge", Type: PrimitiveTypes.Float64, Required: false},
		NestedField{ID: 17, Name: "total_amount", Type: PrimitiveTypes.Float64, Required: true},
		NestedField{ID: 18, Name: "congestion_surcharge", Type: PrimitiveTypes.Float64, Required: false},
		NestedField{ID: 19, Name: "VendorID", Type: PrimitiveTypes.Int32, Required: false},
	)
)

type ManifestTestSuite struct {
	suite.Suite

	v1ManifestList    bytes.Buffer
	v1ManifestEntries bytes.Buffer

	v2ManifestList    bytes.Buffer
	v2ManifestEntries bytes.Buffer
}

func (m *ManifestTestSuite) writeManifestList() {
	m.Require().NoError(WriteManifestList(1, &m.v1ManifestList, snapshotID, nil, nil, manifestFileRecordsV1))
	unassignedSequenceNum := int64(-1)
	m.Require().NoError(WriteManifestList(2, &m.v2ManifestList, snapshotID, nil, &unassignedSequenceNum, manifestFileRecordsV2))
}

func (m *ManifestTestSuite) writeManifestEntries() {
	manifestEntryV1Recs := make([]ManifestEntry, len(manifestEntryV1Records))
	for i, rec := range manifestEntryV1Records {
		manifestEntryV1Recs[i] = rec
	}

	manifestEntryV2Recs := make([]ManifestEntry, len(manifestEntryV2Records))
	for i, rec := range manifestEntryV2Records {
		manifestEntryV2Recs[i] = rec
	}

	partitionSpec := NewPartitionSpecID(1,
		PartitionField{FieldID: 1000, SourceID: 1, Name: "VendorID", Transform: IdentityTransform{}},
		PartitionField{FieldID: 1001, SourceID: 2, Name: "tpep_pickup_datetime", Transform: IdentityTransform{}})

	mf, err := WriteManifest("/home/iceberg/warehouse/nyc/taxis_partitioned/metadata/0125c686-8aa6-4502-bdcc-b6d17ca41a3b-m0.avro",
		&m.v1ManifestEntries, 1, partitionSpec, testSchema, entrySnapshotID, manifestEntryV1Recs)
	m.Require().NoError(err)

	m.EqualValues(m.v1ManifestEntries.Len(), mf.Length())

	mf, err = WriteManifest("/home/iceberg/warehouse/nyc/taxis_partitioned/metadata/0125c686-8aa6-4502-bdcc-b6d17ca41a3b-m0.avro",
		&m.v2ManifestEntries, 2, partitionSpec, testSchema, entrySnapshotID, manifestEntryV2Recs)
	m.Require().NoError(err)

	m.EqualValues(m.v2ManifestEntries.Len(), mf.Length())
}

func (m *ManifestTestSuite) SetupSuite() {
	m.writeManifestList()
	m.writeManifestEntries()
}

func (m *ManifestTestSuite) TestManifestEntriesV1() {
	var mockfs internal.MockFS
	manifest := manifestFile{
		version: 1,
		Path:    manifestFileRecordsV1[0].FilePath(),
	}

	mockfs.Test(m.T())
	mockfs.On("Open", manifest.FilePath()).Return(&internal.MockFile{
		Contents: bytes.NewReader(m.v1ManifestEntries.Bytes()),
	}, nil)
	defer mockfs.AssertExpectations(m.T())
	entries, err := manifest.FetchEntries(&mockfs, false)
	m.Require().NoError(err)
	m.Len(entries, 2)
	m.Zero(manifest.PartitionSpecID())
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
	m.NotNil(entry1.FileSequenceNum())
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

	m.Nil(datafile.KeyMetadata())
	m.Equal([]int64{4}, datafile.SplitOffsets())
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
	m.Nil(list[0].KeyMetadata())
	m.Zero(list[0].PartitionSpecID())
	m.Equal(snapshotID, list[0].SnapshotID())

	part := list[0].Partitions()[0]
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
	m.Nil(list[0].KeyMetadata())
	m.Zero(list[0].PartitionSpecID())

	part := list[0].Partitions()[0]
	m.True(part.ContainsNull)
	m.False(*part.ContainsNaN)
	m.Equal([]byte{0x01, 0x00, 0x00, 0x00}, *part.LowerBound)
	m.Equal([]byte{0x02, 0x00, 0x00, 0x00}, *part.UpperBound)
}

func (m *ManifestTestSuite) TestReadManifestListIncompleteSchema() {
	// This prevents a regression that could be caused by using a schema cache
	// across multiple read/write operations of an avro file. While it may sound
	// like a reasonable idea (caches speed things up, right?), it isn't that
	// sort of cache: it's really a resolver to allow files with incomplete
	// schemas, which we don't want.

	// If a schema cache *were* in use, this would populate it with a definition for
	// the missing record type in the incomplete schema. So we'll first "warm up"
	// any cache. (Note: if working correctly, this will have no such side effect.)
	var buf bytes.Buffer
	seqNum := int64(9876)
	err := WriteManifestList(2, &buf, 1234, nil, &seqNum, []ManifestFile{
		NewManifestFile(2, "s3://bucket/namespace/table/metadata/abcd-0123.avro", 99, 0, 1234).Build(),
	})
	m.NoError(err)
	files, err := ReadManifestList(&buf)
	m.NoError(err)
	m.Len(files, 1)

	// This schema is that of a v2 manifest list, except that it refers to
	// a type named "field_summary" for the "partitions" field, instead of
	// actually including the definition of the "field_summary" record type.
	// This omission should result in an error. But if a schema cache were
	// in use, this could get resolved based on a type of the same name read
	// from a file that defined it.
	incompleteSchema := `
	{
		"name": "manifest_file",
		"type": "record",
		"fields": [
			{
				"name": "manifest_path",
				"type": "string",
				"field-id": 500
			},
			{
				"name": "manifest_length",
				"type": "long",
				"field-id": 501
			},
			{
				"name": "partition_spec_id",
				"type": "int",
				"field-id": 502
			},
			{
				"name": "content",
				"type": "int",
				"default": 0,
				"field-id": 517
			},
			{
				"name": "sequence_number",
				"type": "long",
				"default": 0,
				"field-id": 515
			},
			{
				"name": "min_sequence_number",
				"type": "long",
				"default": 0,
				"field-id": 516
			},
			{
				"name": "added_snapshot_id",
				"type": "long",
				"field-id": 503
			},
			{
				"name": "added_files_count",
				"type": "int",
				"field-id": 504
			},
			{
				"name": "existing_files_count",
				"type": "int",
				"field-id": 505
			},
			{
				"name": "deleted_files_count",
				"type": "int",
				"field-id": 506
			},
			{
				"name": "partitions",
				"type": [
					"null",
					{
						"type": "array",
						"items": "field_summary",
						"element-id": 508
					}
				],
				"field-id": 507
			},
			{
				"name": "added_rows_count",
				"type": "long",
				"field-id": 512
			},
			{
				"name": "existing_rows_count",
				"type": "long",
				"field-id": 513
			},
			{
				"name": "deleted_rows_count",
				"type": "long",
				"field-id": 514
			},
			{
				"name": "key_metadata",
				"type": [
					"null",
					"bytes"
				],
				"field-id": 519
			}
		]
	}`

	// We'll generate a file that is missing part of its schema
	cache := &avro.SchemaCache{}
	sch, err := internal.NewManifestFileSchema(2)
	m.NoError(err)
	enc, err := ocf.NewEncoderWithSchema(sch, &buf,
		ocf.WithEncoderSchemaCache(cache),
		ocf.WithSchemaMarshaler(func(schema avro.Schema) ([]byte, error) {
			return []byte(incompleteSchema), nil
		}),
		ocf.WithMetadata(map[string][]byte{
			"format-version":     {'2'},
			"snapshot-id":        []byte("1234"),
			"sequence-number":    []byte("9876"),
			"parent-snapshot-id": []byte("null"),
		}),
	)
	m.NoError(err)
	for _, file := range files {
		m.NoError(enc.Encode(file))
	}

	// This should fail because the file's schema is incomplete.
	_, err = ReadManifestList(&buf)
	m.ErrorContains(err, "unknown type: field_summary")
}

func (m *ManifestTestSuite) TestReadManifestIncompleteSchema() {
	// This prevents a regression that could be caused by using a schema cache
	// across multiple read/write operations of an avro file. While it may sound
	// like a reasonable idea (caches speed things up, right?), it isn't that
	// sort of cache: it's really a resolver to allow files with incomplete
	// schemas, which we don't want.

	// If a schema cache *were* in use, this would populate it with a definition for
	// the missing record type in the incomplete schema. So we'll first "warm up"
	// any cache. (Note: if working correctly, this will have no such side effect.)
	var buf bytes.Buffer
	partitionSpec := NewPartitionSpecID(1)
	snapshotID := int64(12345678)
	seqNum := int64(9876)
	dataFileBuilder, err := NewDataFileBuilder(
		partitionSpec,
		EntryContentData,
		"s3://bucket/namespace/table/data/abcd-0123.parquet",
		ParquetFile,
		map[int]any{},
		100,
		100*1000*1000,
	)
	m.NoError(err)
	file, err := WriteManifest(
		"s3://bucket/namespace/table/metadata/abcd-0123.avro", &buf, 2,
		partitionSpec,
		NewSchema(123,
			NestedField{ID: 1, Name: "id", Type: Int64Type{}},
			NestedField{ID: 2, Name: "name", Type: StringType{}},
		),
		snapshotID,
		[]ManifestEntry{NewManifestEntry(
			EntryStatusADDED,
			&snapshotID,
			&seqNum, &seqNum,
			dataFileBuilder.Build(),
		)},
	)
	m.NoError(err)

	entries, err := ReadManifest(file, &buf, false)
	m.NoError(err)
	m.Len(entries, 1)

	// This schema is that of a v2 manifest file, except that it refers to
	// a type named "r2" for the "data_file" field, instead of actually
	// including the definition of the "data_file" record type.
	// This omission should result in an error. But if a schema cache were
	// in use, this could get resolved based on a type of the same name read
	// from a file that defined it.
	incompleteSchema := `
	{
		"name": "manifest_entry",
		"type": "record",
		"fields": [
			{
				"name": "status",
				"type": "int",
				"field-id": 0
			},
			{
				"name": "snapshot_id",
				"type": [
					"null",
					"long"
				],
				"field-id": 1
			},
			{
				"name": "sequence_number",
				"type": [
					"null",
					"long"
				],
				"field-id": 3
			},
			{
				"name": "file_sequence_number",
				"type": [
					"null",
					"long"
				],
				"field-id": 4
			},
			{
				"name": "data_file",
				"type": "r2",
				"field-id": 2
			}
		]
	}`

	// We'll generate a file that is missing part of its schema
	cache := &avro.SchemaCache{}
	partitionSchema, err := avro.NewRecordSchema("r102", "", nil) // empty struct
	m.NoError(err)
	sch, err := internal.NewManifestEntrySchema(partitionSchema, 2)
	m.NoError(err)
	enc, err := ocf.NewEncoderWithSchema(sch, &buf,
		ocf.WithEncoderSchemaCache(cache),
		ocf.WithSchemaMarshaler(func(schema avro.Schema) ([]byte, error) {
			return []byte(incompleteSchema), nil
		}),
		ocf.WithMetadata(map[string][]byte{
			"format-version": {'2'},
			// TODO: spec says other things are required, like schema and partition-spec info,
			//       but this package currently only looks at this one value when reading...
		}),
	)
	m.NoError(err)
	for _, entry := range entries {
		m.NoError(enc.Encode(entry))
	}

	// This should fail because the file's schema is incomplete.
	_, err = ReadManifest(file, &buf, false)
	m.ErrorContains(err, "unknown type: r2")
}

func (m *ManifestTestSuite) TestManifestEntriesV2() {
	manifest := manifestFile{
		version: 2,
		SpecID:  1,
		Path:    manifestFileRecordsV2[0].FilePath(),
	}

	partitionSpec := NewPartitionSpecID(1,
		PartitionField{FieldID: 1000, SourceID: 1, Name: "VendorID", Transform: IdentityTransform{}},
		PartitionField{FieldID: 1001, SourceID: 2, Name: "tpep_pickup_datetime", Transform: IdentityTransform{}})

	mockedFile := &internal.MockFile{
		Contents: bytes.NewReader(m.v2ManifestEntries.Bytes()),
	}
	manifestReader, err := NewManifestReader(&manifest, mockedFile)
	m.Require().NoError(err)
	m.Equal(2, manifestReader.Version())
	m.Equal(ManifestContentData, manifestReader.ManifestContent())
	loadedSchema, err := manifestReader.Schema()
	m.Require().NoError(err)
	m.True(loadedSchema.Equals(testSchema))
	loadedPartitionSpec, err := manifestReader.PartitionSpec()
	m.Require().NoError(err)
	m.True(loadedPartitionSpec.Equals(partitionSpec))

	entry1, err := manifestReader.ReadEntry()
	m.Require().NoError(err)
	_, err = manifestReader.ReadEntry()
	m.Require().NoError(err)
	_, err = manifestReader.ReadEntry()
	m.Require().ErrorIs(err, io.EOF)

	m.Equal(int32(1), manifest.PartitionSpecID())
	m.Zero(manifest.SnapshotID())
	m.Zero(manifest.AddedDataFiles())
	m.Zero(manifest.ExistingDataFiles())
	m.Zero(manifest.DeletedDataFiles())
	m.Zero(manifest.ExistingRows())
	m.Zero(manifest.DeletedRows())
	m.Zero(manifest.AddedRows())

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

	m.Nil(datafile.KeyMetadata())
	m.Equal([]int64{4}, datafile.SplitOffsets())
	m.Nil(datafile.EqualityFieldIDs())
	m.Zero(*datafile.SortOrderID())
}

func (m *ManifestTestSuite) TestManifestEntryBuilder() {
	dataFileBuilder, err := NewDataFileBuilder(
		NewPartitionSpec(),
		EntryContentData,
		"sample.parquet",
		ParquetFile,
		map[int]any{1001: int(1), 1002: time.Unix(1925, 0).UnixMicro()},
		1,
		2,
	)
	m.Require().NoError(err)

	dataFileBuilder.ColumnSizes(map[int]int64{
		1: 1,
		2: 2,
	}).ValueCounts(map[int]int64{
		1: 1,
		2: 2,
	}).DistinctValueCounts(map[int]int64{
		1: 1,
		2: 2,
	}).NullValueCounts(map[int]int64{
		1: 0,
		2: 0,
	}).NaNValueCounts(map[int]int64{
		1: 0,
		2: 0,
	}).LowerBoundValues(map[int][]byte{
		1: {0x01, 0x00, 0x00, 0x00},
		2: []byte("2020-04-01 00:00"),
	}).UpperBoundValues(map[int][]byte{
		1: {0x01, 0x00, 0x00, 0x00},
		2: []byte("2020-04-30 23:5:"),
	}).SplitOffsets([]int64{4}).EqualityFieldIDs([]int{1, 1}).SortOrderID(0)

	snapshotEntryID := int64(1)
	entry := NewManifestEntryBuilder(
		EntryStatusEXISTING,
		&snapshotEntryID,
		dataFileBuilder.Build()).Build()

	m.Assert().Equal(EntryStatusEXISTING, entry.Status())
	m.Assert().EqualValues(1, entry.SnapshotID())
	// unassigned sequence number
	m.Assert().Equal(int64(-1), entry.SequenceNum())
	m.Assert().Nil(entry.FileSequenceNum())
	data := entry.DataFile()
	m.Assert().Equal(EntryContentData, data.ContentType())
	m.Assert().Equal("sample.parquet", data.FilePath())
	m.Assert().Equal(ParquetFile, data.FileFormat())
	m.Assert().EqualValues(1, data.Count())
	m.Assert().EqualValues(2, data.FileSizeBytes())
	m.Assert().Equal(map[int]int64{
		1: 1,
		2: 2,
	}, data.ColumnSizes())
	m.Assert().Equal(map[int]int64{
		1: 1,
		2: 2,
	}, data.ValueCounts())
	m.Assert().Equal(map[int]int64{
		1: 0,
		2: 0,
	}, data.NullValueCounts())
	m.Assert().Equal(map[int]int64{
		1: 1,
		2: 2,
	}, data.DistinctValueCounts())
	m.Assert().Equal(map[int]int64{
		1: 0,
		2: 0,
	}, data.NaNValueCounts())
	m.Assert().Equal(map[int][]byte{
		1: {0x01, 0x00, 0x00, 0x00},
		2: []byte("2020-04-01 00:00"),
	}, data.LowerBoundValues())
	m.Assert().Equal(map[int][]byte{
		1: {0x01, 0x00, 0x00, 0x00},
		2: []byte("2020-04-30 23:5:"),
	}, data.UpperBoundValues())
	m.Assert().Equal([]int64{4}, data.SplitOffsets())
	m.Assert().Equal([]int{1, 1}, data.EqualityFieldIDs())
	m.Assert().Equal(0, *data.SortOrderID())
}

func (m *ManifestTestSuite) TestManifestWriterMeta() {
	sch := NewSchema(0, NestedField{ID: 0, Name: "test01", Type: StringType{}})
	w, err := NewManifestWriter(2, io.Discard, *UnpartitionedSpec, sch, 1)
	m.Require().NoError(err)
	md, err := w.meta()
	m.Require().NoError(err)
	m.NotEqual("null", string(md["partition-spec"]))
	m.Equal("[]", string(md["partition-spec"]))
}

func TestManifests(t *testing.T) {
	suite.Run(t, new(ManifestTestSuite))
}
