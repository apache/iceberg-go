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
	"compress/flate"
	"encoding/json"
	"errors"
	"io"
	"io/fs"
	"strconv"
	"testing"
	"time"

	"github.com/apache/iceberg-go/internal"
	iceio "github.com/apache/iceberg-go/io"
	"github.com/stretchr/testify/suite"
	"github.com/twmb/avro"
	"github.com/twmb/avro/ocf"
)

var (
	falseBool                   = false
	testEqualityIDs             = []int{1, 2}
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

	manifestFileRecordsV3 = []ManifestFile{
		NewManifestFile(3, "/home/iceberg/warehouse/nyc/taxis_partitioned/metadata/0125c686-8aa6-4502-bdcc-b6d17ca41a3b-m0.avro",
			7989, 0, snapshotID).
			Content(ManifestContentData).
			SequenceNum(5, 5).
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
				EqualityIDs:      &testEqualityIDs,
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

	testFirstRowID         int64 = 1000
	manifestEntryV3Records       = []*manifestEntry{
		{
			EntryStatus: EntryStatusADDED,
			Snapshot:    &entrySnapshotID,
			Data: &dataFile{
				Path:                    dataRecord0.Path,
				Format:                  dataRecord0.Format,
				PartitionData:           dataRecord0.PartitionData,
				RecordCount:             dataRecord0.RecordCount,
				FileSize:                dataRecord0.FileSize,
				BlockSizeInBytes:        dataRecord0.BlockSizeInBytes,
				ColSizes:                dataRecord0.ColSizes,
				ValCounts:               dataRecord0.ValCounts,
				NullCounts:              dataRecord0.NullCounts,
				NaNCounts:               dataRecord0.NaNCounts,
				LowerBounds:             dataRecord0.LowerBounds,
				UpperBounds:             dataRecord0.UpperBounds,
				Splits:                  dataRecord0.Splits,
				SortOrder:               dataRecord0.SortOrder,
				EqualityIDs:             &testEqualityIDs,
				FirstRowIDField:         &testFirstRowID,
				ReferencedDataFileField: nil,
				ContentOffsetField:      nil,
				ContentSizeInBytesField: nil,
			},
		},
		{
			EntryStatus: EntryStatusADDED,
			Snapshot:    &entrySnapshotID,
			Data: &dataFile{
				Path:                    dataRecord1.Path,
				Format:                  dataRecord1.Format,
				PartitionData:           dataRecord1.PartitionData,
				RecordCount:             dataRecord1.RecordCount,
				FileSize:                dataRecord1.FileSize,
				BlockSizeInBytes:        dataRecord1.BlockSizeInBytes,
				ColSizes:                dataRecord1.ColSizes,
				ValCounts:               dataRecord1.ValCounts,
				NullCounts:              dataRecord1.NullCounts,
				NaNCounts:               dataRecord1.NaNCounts,
				LowerBounds:             dataRecord1.LowerBounds,
				UpperBounds:             dataRecord1.UpperBounds,
				Splits:                  dataRecord1.Splits,
				SortOrder:               dataRecord1.SortOrder,
				FirstRowIDField:         &testFirstRowID,
				ReferencedDataFileField: nil,
				ContentOffsetField:      nil,
				ContentSizeInBytesField: nil,
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

	v3ManifestList    bytes.Buffer
	v3ManifestEntries bytes.Buffer
}

func (m *ManifestTestSuite) writeManifestList() {
	err := WriteManifestList(1, &m.v1ManifestList, snapshotID, nil, nil, 0, manifestFileRecordsV1)
	m.Require().NoError(err)
	unassignedSequenceNum := int64(-1)
	err = WriteManifestList(2, &m.v2ManifestList, snapshotID, nil, &unassignedSequenceNum, 0, manifestFileRecordsV2)
	m.Require().NoError(err)
	v3SequenceNum := int64(5)
	firstRowID := int64(1000)
	err = WriteManifestList(3, &m.v3ManifestList, snapshotID, nil, &v3SequenceNum, firstRowID, manifestFileRecordsV3)
	m.Require().NoError(err)
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

	manifestEntryV3Recs := make([]ManifestEntry, len(manifestEntryV3Records))
	for i, rec := range manifestEntryV3Records {
		manifestEntryV3Recs[i] = rec
	}

	partitionSpec := NewPartitionSpecID(1,
		PartitionField{FieldID: 1000, SourceIDs: []int{1}, Name: "VendorID", Transform: IdentityTransform{}},
		PartitionField{FieldID: 1001, SourceIDs: []int{2}, Name: "tpep_pickup_datetime", Transform: IdentityTransform{}})

	mf, err := WriteManifest("/home/iceberg/warehouse/nyc/taxis_partitioned/metadata/0125c686-8aa6-4502-bdcc-b6d17ca41a3b-m0.avro",
		&m.v1ManifestEntries, 1, partitionSpec, testSchema, entrySnapshotID, manifestEntryV1Recs)
	m.Require().NoError(err)

	m.EqualValues(m.v1ManifestEntries.Len(), mf.Length())

	mf, err = WriteManifest("/home/iceberg/warehouse/nyc/taxis_partitioned/metadata/0125c686-8aa6-4502-bdcc-b6d17ca41a3b-m0.avro",
		&m.v2ManifestEntries, 2, partitionSpec, testSchema, entrySnapshotID, manifestEntryV2Recs)
	m.Require().NoError(err)

	m.EqualValues(m.v2ManifestEntries.Len(), mf.Length())

	mf, err = WriteManifest("/home/iceberg/warehouse/nyc/taxis_partitioned/metadata/0125c686-8aa6-4502-bdcc-b6d17ca41a3b-m0.avro",
		&m.v3ManifestEntries, 3, partitionSpec, testSchema, entrySnapshotID, manifestEntryV3Recs)
	m.Require().NoError(err)

	m.EqualValues(m.v3ManifestEntries.Len(), mf.Length())
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
	entries := make([]ManifestEntry, 0, 2)
	for entry, err := range manifest.Entries(&mockfs, false) {
		m.Require().NoError(err)
		entries = append(entries, entry)
	}
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

func (m *ManifestTestSuite) TestReadManifestListV3() {
	list, err := ReadManifestList(&m.v3ManifestList)
	m.Require().NoError(err)

	m.Equal("/home/iceberg/warehouse/nyc/taxis_partitioned/metadata/0125c686-8aa6-4502-bdcc-b6d17ca41a3b-m0.avro", list[0].FilePath())
	m.Len(list, 1)
	m.Equal(3, list[0].Version())
	m.EqualValues(7989, list[0].Length())
	m.Equal(ManifestContentData, list[0].ManifestContent())
	m.EqualValues(5, list[0].SequenceNum())
	m.EqualValues(5, list[0].MinSequenceNum())

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

	// V3 manifest list assigns first_row_id to data manifests
	m.Require().NotNil(list[0].FirstRowID(), "v3 data manifest should have first_row_id")
	m.EqualValues(1000, *list[0].FirstRowID())

	part := list[0].Partitions()[0]
	m.True(part.ContainsNull)
	m.False(*part.ContainsNaN)
	m.Equal([]byte{0x01, 0x00, 0x00, 0x00}, *part.LowerBound)
	m.Equal([]byte{0x02, 0x00, 0x00, 0x00}, *part.UpperBound)
}

// writeLegacyManifestListV1 creates a V1 manifest list OCF using the pre-1.4 Java
// Iceberg field names (added_data_files_count etc.) as writer schema field names.
func writeLegacyManifestListV1(t *testing.T) bytes.Buffer {
	t.Helper()

	const schemaJSON = `{
		"type": "record",
		"name": "manifest_file",
		"fields": [
			{"name": "manifest_path", "type": "string", "field-id": 500},
			{"name": "manifest_length", "type": "long", "field-id": 501},
			{"name": "partition_spec_id", "type": "int", "field-id": 502},
			{"name": "added_snapshot_id", "type": "long", "field-id": 503},
			{"name": "added_data_files_count", "type": ["null", "int"], "default": null, "field-id": 504},
			{"name": "existing_data_files_count", "type": ["null", "int"], "default": null, "field-id": 505},
			{"name": "deleted_data_files_count", "type": ["null", "int"], "default": null, "field-id": 506},
			{"name": "added_rows_count", "type": ["null", "long"], "default": null, "field-id": 512},
			{"name": "existing_rows_count", "type": ["null", "long"], "default": null, "field-id": 513},
			{"name": "deleted_rows_count", "type": ["null", "long"], "default": null, "field-id": 514}
		]
	}`

	type legacyRecord struct {
		Path                   string `avro:"manifest_path"`
		Len                    int64  `avro:"manifest_length"`
		SpecID                 int32  `avro:"partition_spec_id"`
		AddedSnapshotID        int64  `avro:"added_snapshot_id"`
		AddedDataFilesCount    *int32 `avro:"added_data_files_count"`
		ExistingDataFilesCount *int32 `avro:"existing_data_files_count"`
		DeletedDataFilesCount  *int32 `avro:"deleted_data_files_count"`
		AddedRowsCount         *int64 `avro:"added_rows_count"`
		ExistingRowsCount      *int64 `avro:"existing_rows_count"`
		DeletedRowsCount       *int64 `avro:"deleted_rows_count"`
	}

	sc, err := avro.Parse(schemaJSON)
	if err != nil {
		t.Fatal(err)
	}

	var buf bytes.Buffer
	enc, err := ocf.NewWriter(&buf, sc, ocf.WithSchema(schemaJSON))
	if err != nil {
		t.Fatal(err)
	}

	added := int32(3)
	existing := int32(1)
	deleted := int32(2)
	addedR := int64(100)

	if err := enc.Encode(legacyRecord{
		Path:                   "/path/to/manifest.avro",
		Len:                    1234,
		SpecID:                 0,
		AddedSnapshotID:        snapshotID,
		AddedDataFilesCount:    &added,
		ExistingDataFilesCount: &existing,
		DeletedDataFilesCount:  &deleted,
		AddedRowsCount:         &addedR,
	}); err != nil {
		t.Fatal(err)
	}

	if err := enc.Close(); err != nil {
		t.Fatal(err)
	}

	return buf
}

// writeLegacyManifestListV2 creates a V2 manifest list OCF using the pre-1.4 Java
// Iceberg field names (added_data_files_count etc.) as writer schema field names.
func writeLegacyManifestListV2(t *testing.T) bytes.Buffer {
	t.Helper()

	const schemaJSON = `{
		"type": "record",
		"name": "manifest_file",
		"fields": [
			{"name": "manifest_path", "type": "string", "field-id": 500},
			{"name": "manifest_length", "type": "long", "field-id": 501},
			{"name": "partition_spec_id", "type": "int", "field-id": 502},
			{"name": "content", "type": "int", "field-id": 517},
			{"name": "sequence_number", "type": "long", "field-id": 515},
			{"name": "min_sequence_number", "type": "long", "field-id": 516},
			{"name": "added_snapshot_id", "type": "long", "field-id": 503},
			{"name": "added_data_files_count", "type": "int", "field-id": 504},
			{"name": "existing_data_files_count", "type": "int", "field-id": 505},
			{"name": "deleted_data_files_count", "type": "int", "field-id": 506},
			{"name": "added_rows_count", "type": "long", "field-id": 512},
			{"name": "existing_rows_count", "type": "long", "field-id": 513},
			{"name": "deleted_rows_count", "type": "long", "field-id": 514}
		]
	}`

	type legacyRecord struct {
		Path                   string `avro:"manifest_path"`
		Len                    int64  `avro:"manifest_length"`
		SpecID                 int32  `avro:"partition_spec_id"`
		Content                int32  `avro:"content"`
		SeqNumber              int64  `avro:"sequence_number"`
		MinSeqNumber           int64  `avro:"min_sequence_number"`
		AddedSnapshotID        int64  `avro:"added_snapshot_id"`
		AddedDataFilesCount    int32  `avro:"added_data_files_count"`
		ExistingDataFilesCount int32  `avro:"existing_data_files_count"`
		DeletedDataFilesCount  int32  `avro:"deleted_data_files_count"`
		AddedRowsCount         int64  `avro:"added_rows_count"`
		ExistingRowsCount      int64  `avro:"existing_rows_count"`
		DeletedRowsCount       int64  `avro:"deleted_rows_count"`
	}

	sc, err := avro.Parse(schemaJSON)
	if err != nil {
		t.Fatal(err)
	}

	var buf bytes.Buffer
	enc, err := ocf.NewWriter(&buf, sc,
		ocf.WithSchema(schemaJSON),
		ocf.WithMetadata(map[string][]byte{"format-version": []byte("2")}))
	if err != nil {
		t.Fatal(err)
	}

	if err := enc.Encode(legacyRecord{
		Path:                   "/path/to/manifest.avro",
		Len:                    1234,
		SpecID:                 0,
		Content:                0,
		SeqNumber:              3,
		MinSeqNumber:           3,
		AddedSnapshotID:        snapshotID,
		AddedDataFilesCount:    3,
		ExistingDataFilesCount: 1,
		DeletedDataFilesCount:  2,
		AddedRowsCount:         100,
	}); err != nil {
		t.Fatal(err)
	}

	if err := enc.Close(); err != nil {
		t.Fatal(err)
	}

	return buf
}

// TestReadManifestListLegacyFieldNamesV1 verifies that V1 manifest lists written
// by pre-1.4 Java Iceberg (with added_data_files_count etc.) are decoded correctly.
func (m *ManifestTestSuite) TestReadManifestListLegacyFieldNamesV1() {
	buf := writeLegacyManifestListV1(m.T())
	list, err := ReadManifestList(&buf)
	m.Require().NoError(err)
	m.Len(list, 1)
	m.Equal(1, list[0].Version())
	m.EqualValues(3, list[0].AddedDataFiles())
	m.True(list[0].HasAddedFiles())
	m.EqualValues(1, list[0].ExistingDataFiles())
	m.True(list[0].HasExistingFiles())
	m.EqualValues(2, list[0].DeletedDataFiles())
}

// TestReadManifestListLegacyFieldNamesV2 verifies that V2 manifest lists written
// by pre-1.4 Java Iceberg (with added_data_files_count etc.) are decoded correctly.
func (m *ManifestTestSuite) TestReadManifestListLegacyFieldNamesV2() {
	buf := writeLegacyManifestListV2(m.T())
	list, err := ReadManifestList(&buf)
	m.Require().NoError(err)
	m.Len(list, 1)
	m.Equal(2, list[0].Version())
	m.EqualValues(3, list[0].AddedDataFiles())
	m.True(list[0].HasAddedFiles())
	m.EqualValues(1, list[0].ExistingDataFiles())
	m.True(list[0].HasExistingFiles())
	m.EqualValues(2, list[0].DeletedDataFiles())
}

// writeManifestListNoFormatVersion writes a valid v2 manifest list Avro file that
// omits the "format-version" metadata key, simulating a file produced by a
// non-Java Iceberg implementation that strictly follows the spec.
func writeManifestListNoFormatVersion(t *testing.T, version int) bytes.Buffer {
	t.Helper()

	fileSchema, err := internal.NewManifestFileSchema(version)
	if err != nil {
		t.Fatal(err)
	}

	var buf bytes.Buffer
	wr, err := ocf.NewWriter(&buf, fileSchema,
		ocf.WithSchema(fileSchema.String()),
		ocf.WithMetadata(map[string][]byte{
			"snapshot-id":     []byte("1234"),
			"sequence-number": []byte("0"),
		}),
		ocf.WithCodec(ocf.DeflateCodec(-1)))
	if err != nil {
		t.Fatal(err)
	}
	if err := wr.Close(); err != nil {
		t.Fatal(err)
	}

	return buf
}

func (m *ManifestTestSuite) TestReadManifestListMissingFormatVersion() {
	// A manifest list without "format-version" should succeed, defaulting to v1.
	buf := writeManifestListNoFormatVersion(m.T(), 1)
	files, err := ReadManifestList(&buf)
	m.NoError(err)
	m.Empty(files) // the file has no entries, just headers
}

// writeManifestNoFormatVersion writes a valid v1 manifest entry Avro file that
// omits the "format-version" metadata key, simulating files produced by the Java
// Iceberg library (format-version is optional for v1 per the Iceberg spec).
func writeManifestNoFormatVersion(t *testing.T) bytes.Buffer {
	t.Helper()

	partitionSpec := NewPartitionSpec()
	partitionSchema, err := partitionTypeToAvroSchema(partitionSpec.PartitionType(testSchema))
	if err != nil {
		t.Fatal(err)
	}
	entrySchema, err := internal.NewManifestEntrySchema(partitionSchema, 1)
	if err != nil {
		t.Fatal(err)
	}

	schemaJSON, err := json.Marshal(testSchema)
	if err != nil {
		t.Fatal(err)
	}

	var buf bytes.Buffer
	w, err := ocf.NewWriter(&buf, entrySchema,
		ocf.WithMetadata(map[string][]byte{
			// intentionally omit "format-version" to simulate Java Iceberg v1 files
			"schema":            schemaJSON,
			"schema-id":         []byte(strconv.Itoa(testSchema.ID)),
			"partition-spec":    []byte("[]"),
			"partition-spec-id": []byte("0"),
			"content":           []byte("data"),
		}),
		ocf.WithCodec(ocf.DeflateCodec(flate.DefaultCompression)))
	if err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}

	return buf
}

func (m *ManifestTestSuite) TestNewManifestReaderMissingFormatVersion() {
	// A v1 manifest file without "format-version" should succeed, defaulting to the
	// version from the manifest list entry (matching the Java Iceberg behavior).
	buf := writeManifestNoFormatVersion(m.T())
	manifest := manifestFile{version: 1}
	reader, err := NewManifestReader(&manifest, &buf)
	m.Require().NoError(err)
	m.Equal(1, reader.Version())
	m.NoError(reader.Close())
}

func (m *ManifestTestSuite) TestV3DataManifestFirstRowIDInheritance() {
	// Build a v3 data manifest with two entries that have null first_row_id.
	partitionSpec := NewPartitionSpecID(1,
		PartitionField{FieldID: 1000, SourceIDs: []int{1}, Name: "x", Transform: IdentityTransform{}})
	firstCount, secondCount := int64(10), int64(20)
	entriesWithNullFirstRowID := []ManifestEntry{
		&manifestEntry{
			EntryStatus: EntryStatusADDED,
			Snapshot:    &entrySnapshotID,
			Data: &dataFile{
				Content:          EntryContentData,
				Path:             "/data/file1.parquet",
				Format:           ParquetFile,
				PartitionData:    map[string]any{"x": int(1)},
				RecordCount:      firstCount,
				FileSize:         1000,
				BlockSizeInBytes: 64 * 1024,
				FirstRowIDField:  nil, // null so reader will inherit
			},
		},
		&manifestEntry{
			EntryStatus: EntryStatusADDED,
			Snapshot:    &entrySnapshotID,
			Data: &dataFile{
				Content:          EntryContentData,
				Path:             "/data/file2.parquet",
				Format:           ParquetFile,
				PartitionData:    map[string]any{"x": int(2)},
				RecordCount:      secondCount,
				FileSize:         2000,
				BlockSizeInBytes: 64 * 1024,
				FirstRowIDField:  nil,
			},
		},
	}
	var manifestBuf bytes.Buffer
	_, err := WriteManifest("/manifest.avro", &manifestBuf, 3, partitionSpec, testSchema, entrySnapshotID, entriesWithNullFirstRowID)
	m.Require().NoError(err)

	manifestFirstRowID := int64(1000)
	file := &manifestFile{
		version:         3,
		Path:            "/manifest.avro",
		Content:         ManifestContentData,
		FirstRowIDValue: &manifestFirstRowID,
	}
	entries, err := ReadManifest(file, bytes.NewReader(manifestBuf.Bytes()), false)
	m.Require().NoError(err)
	m.Require().Len(entries, 2)

	// First entry gets manifest's first_row_id.
	m.Require().NotNil(entries[0].DataFile().FirstRowID())
	m.EqualValues(1000, *entries[0].DataFile().FirstRowID())
	// Second entry gets previous + previous file's record_count.
	m.Require().NotNil(entries[1].DataFile().FirstRowID())
	m.EqualValues(1000+firstCount, *entries[1].DataFile().FirstRowID())
}

func (m *ManifestTestSuite) TestReadManifestListIncompleteSchema() {
	// Verify that reading a manifest list whose embedded schema references
	// an undefined named type ("field_summary" without its definition)
	// fails. A stray cache or cross-file resolver could mask this by
	// reusing a definition from a previously-read file.
	var buf bytes.Buffer
	seqNum := int64(9876)
	err := WriteManifestList(2, &buf, 1234, nil, &seqNum, 0, []ManifestFile{
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
	sch, err := internal.NewManifestFileSchema(2)
	m.NoError(err)
	wr, err := ocf.NewWriter(&buf, sch,
		ocf.WithSchema(incompleteSchema),
		ocf.WithMetadata(map[string][]byte{
			"format-version":     {'2'},
			"snapshot-id":        []byte("1234"),
			"sequence-number":    []byte("9876"),
			"parent-snapshot-id": []byte("null"),
		}),
	)
	m.NoError(err)
	for _, file := range files {
		m.NoError(wr.Encode(file))
	}

	// This should fail because the file's schema is incomplete.
	_, err = ReadManifestList(&buf)
	m.Error(err)
}

func (m *ManifestTestSuite) TestReadManifestIncompleteSchema() {
	// Verify that reading a manifest entry whose embedded schema references
	// an undefined named type ("r2" without its definition) fails. A stray
	// cache or cross-file resolver could mask this by reusing a definition
	// from a previously-read file.
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
		map[int]string{},
		map[int]int{},
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
	partNode := avro.SchemaNode{
		Type: "record", Name: "r102", Fields: nil,
	}
	partitionSchema, err := partNode.Schema()
	m.NoError(err)
	sch, err := internal.NewManifestEntrySchema(partitionSchema, 2)
	m.NoError(err)
	wr, err := ocf.NewWriter(&buf, sch,
		ocf.WithSchema(incompleteSchema),
		ocf.WithMetadata(map[string][]byte{
			"format-version": {'2'},
			// TODO: spec says other things are required, like schema and partition-spec info,
			//       but this package currently only looks at this one value when reading...
		}),
	)
	m.NoError(err)
	for _, entry := range entries {
		m.NoError(wr.Encode(entry))
	}

	// This should fail because the file's schema is incomplete.
	_, err = ReadManifest(file, &buf, false)
	m.Error(err)
}

func (m *ManifestTestSuite) TestManifestEntriesV2() {
	manifest := manifestFile{
		version: 2,
		SpecID:  1,
		Path:    manifestFileRecordsV2[0].FilePath(),
	}

	partitionSpec := NewPartitionSpecID(1,
		PartitionField{FieldID: 1000, SourceIDs: []int{1}, Name: "VendorID", Transform: IdentityTransform{}},
		PartitionField{FieldID: 1001, SourceIDs: []int{2}, Name: "tpep_pickup_datetime", Transform: IdentityTransform{}})

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
	m.Equal(testEqualityIDs, datafile.EqualityFieldIDs())
	m.Zero(*datafile.SortOrderID())

	m.equalityIDsSchemaIsInt(manifestReader.rd.Schema())
}

func (m *ManifestTestSuite) TestManifestEntriesV3() {
	manifest := manifestFile{
		version: 3,
		SpecID:  1,
		Path:    manifestFileRecordsV3[0].FilePath(),
	}
	partitionSpec := NewPartitionSpecID(1,
		PartitionField{FieldID: 1000, SourceIDs: []int{1}, Name: "VendorID", Transform: IdentityTransform{}},
		PartitionField{FieldID: 1001, SourceIDs: []int{2}, Name: "tpep_pickup_datetime", Transform: IdentityTransform{}})
	mockedFile := &internal.MockFile{
		Contents: bytes.NewReader(m.v3ManifestEntries.Bytes()),
	}
	manifestReader, err := NewManifestReader(&manifest, mockedFile)
	m.Require().NoError(err)
	m.Equal(3, manifestReader.Version())
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
	// Test v3-specific fields
	m.NotNil(datafile.FirstRowID())
	m.EqualValues(testFirstRowID, *datafile.FirstRowID())
	m.Nil(datafile.ReferencedDataFile()) // Only for position delete files
	m.Nil(datafile.ContentOffset())
	m.Nil(datafile.ContentSizeInBytes())
	// Verify existing fields still work
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
	m.Nil(datafile.KeyMetadata())
	m.Equal([]int64{4}, datafile.SplitOffsets())
	m.Equal(testEqualityIDs, datafile.EqualityFieldIDs())
	m.Zero(*datafile.SortOrderID())

	m.equalityIDsSchemaIsInt(manifestReader.rd.Schema())
}

func (m *ManifestTestSuite) TestNewManifestReaderZstdManifestEntriesV2() {
	manifest := manifestFile{
		version: 2,
		SpecID:  1,
		Path:    manifestFileRecordsV2[0].FilePath(),
	}

	partitionSpec := NewPartitionSpecID(1,
		PartitionField{FieldID: 1000, SourceIDs: []int{1}, Name: "VendorID", Transform: IdentityTransform{}},
		PartitionField{FieldID: 1001, SourceIDs: []int{2}, Name: "tpep_pickup_datetime", Transform: IdentityTransform{}})

	partitionSchema, err := partitionTypeToAvroSchema(partitionSpec.PartitionType(testSchema))
	m.Require().NoError(err)

	entrySchema, err := internal.NewManifestEntrySchema(partitionSchema, 2)
	m.Require().NoError(err)

	mw := ManifestWriter{
		version: 2,
		spec:    partitionSpec,
		schema:  testSchema,
		content: ManifestContentData,
	}
	md, err := mw.meta()
	m.Require().NoError(err)

	var buf bytes.Buffer
	wr, err := ocf.NewWriter(&buf, entrySchema,
		ocf.WithSchema(entrySchema.String()),
		ocf.WithMetadata(md),
		ocf.WithCodec(ocf.MustZstdCodec(nil, nil)))
	m.Require().NoError(err)

	m.Require().NoError(wr.Encode(manifestEntryV2Records[0]))
	m.Require().NoError(wr.Encode(manifestEntryV2Records[1]))
	m.Require().NoError(wr.Close())

	manifestReader, err := NewManifestReader(&manifest, bytes.NewReader(buf.Bytes()))
	m.Require().NoError(err)
	defer func() {
		m.Require().NoError(manifestReader.Close())
	}()

	entry1, err := manifestReader.ReadEntry()
	m.Require().NoError(err)
	m.Equal(manifestEntryV2Records[0].DataFile().FilePath(), entry1.DataFile().FilePath())

	entry2, err := manifestReader.ReadEntry()
	m.Require().NoError(err)
	m.Equal(manifestEntryV2Records[1].DataFile().FilePath(), entry2.DataFile().FilePath())

	_, err = manifestReader.ReadEntry()
	m.Require().ErrorIs(err, io.EOF)
}

func (m *ManifestTestSuite) TestManifestEntryBuilder() {
	dataFileBuilder, err := NewDataFileBuilder(
		NewPartitionSpec(),
		EntryContentData,
		"sample.parquet",
		ParquetFile,
		map[int]any{1001: int(1), 1002: time.Unix(1925, 0).UnixMicro()},
		map[int]string{},
		map[int]int{},
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

// equalityIDsSchemaIsInt asserts equality_ids uses Avro "int", not "long".
func (m *ManifestTestSuite) equalityIDsSchemaIsInt(sc *avro.Schema) {
	m.T().Helper()

	root := sc.Root()
	var dataFileField *avro.SchemaField
	for i := range root.Fields {
		if root.Fields[i].Name == "data_file" {
			dataFileField = &root.Fields[i]

			break
		}
	}
	m.Require().NotNil(dataFileField, "data_file field not found in manifest_entry schema")

	dfType := dataFileField.Type
	var eqIDsField *avro.SchemaField
	for i := range dfType.Fields {
		if dfType.Fields[i].Name == "equality_ids" {
			eqIDsField = &dfType.Fields[i]

			break
		}
	}
	m.Require().NotNil(eqIDsField, "equality_ids field not found in data_file schema")

	// equality_ids is ["null", {"type":"array","items":"int"}]
	m.Require().Equal("union", eqIDsField.Type.Type)
	var arrayBranch *avro.SchemaNode
	for i := range eqIDsField.Type.Branches {
		if eqIDsField.Type.Branches[i].Type == "array" {
			arrayBranch = &eqIDsField.Type.Branches[i]

			break
		}
	}
	m.Require().NotNil(arrayBranch, "equality_ids union should contain an array schema")
	m.Equal("int", arrayBranch.Items.Type,
		"equality_ids array elements must be Avro int (not long) per the Iceberg spec")
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

func (m *ManifestTestSuite) TestV3ManifestListWriterRowIDTracking() {
	// Test v3 ManifestListWriter NextRowID functionality
	var buf bytes.Buffer
	firstRowID := int64(5000)
	sequenceNum := int64(10)
	writer, err := NewManifestListWriterV3(&buf, snapshotID, sequenceNum, firstRowID, nil)
	m.Require().NoError(err)
	// Test NextRowID method
	m.NotNil(writer.NextRowID())
	m.EqualValues(firstRowID, *writer.NextRowID())
	// Create test manifests with row counts
	manifests := []ManifestFile{
		NewManifestFile(3, "test1.avro", 100, 1, snapshotID).
			AddedRows(1000).ExistingRows(500).Build(),
		NewManifestFile(3, "test2.avro", 200, 1, snapshotID).
			AddedRows(2000).ExistingRows(300).Build(),
	}
	// Add manifests - should update nextRowID
	err = writer.AddManifests(manifests)
	m.Require().NoError(err)
	// NextRowID should have been incremented by total row counts
	// manifest1: 1000 (added) + 500 (existing) = 1500
	// manifest2: 2000 (added) + 300 (existing) = 2300
	// Expected: 5000 + 1500 + 2300 = 8800
	expectedNextRowID := firstRowID + 1500 + 2300
	m.EqualValues(expectedNextRowID, *writer.NextRowID())
	// Assigned row-id delta (for snapshot.added-rows) = 1500 + 2300 = 3800
	m.EqualValues(int64(3800), *writer.NextRowID()-firstRowID)
	err = writer.Close()
	m.Require().NoError(err)
}

func (m *ManifestTestSuite) TestV3ManifestListWriterAssignedRowIDDelta() {
	// Assigned row-id delta = sum of (existing+added) for all data manifests in list.
	var buf bytes.Buffer
	commitSnapID := int64(100)
	otherSnapID := int64(99)
	firstRowID := int64(0)
	sequenceNum := int64(1)
	writer, err := NewManifestListWriterV3(&buf, commitSnapID, sequenceNum, firstRowID, nil)
	m.Require().NoError(err)
	manifests := []ManifestFile{
		NewManifestFile(3, "current.avro", 100, 1, commitSnapID).AddedRows(10).ExistingRows(5).Build(),
		NewManifestFile(3, "carried.avro", 200, 1, otherSnapID).SequenceNum(0, 0).AddedRows(100).ExistingRows(50).Build(),
		NewManifestFile(3, "current2.avro", 300, 1, commitSnapID).AddedRows(20).Build(),
	}
	err = writer.AddManifests(manifests)
	m.Require().NoError(err)
	// Delta = 15 + 150 + 20 = 185 (all data manifests get row-id range)
	m.EqualValues(185, *writer.NextRowID()-firstRowID)
	m.Require().NoError(writer.Close())
}

func (m *ManifestTestSuite) TestV3ManifestListWriterDeltaIgnoresNonDataManifests() {
	// Only data manifests get row-id assignment; delete manifests must not affect delta.
	var buf bytes.Buffer
	commitSnapID := int64(1)
	firstRowID := int64(100)
	sequenceNum := int64(1)
	writer, err := NewManifestListWriterV3(&buf, commitSnapID, sequenceNum, firstRowID, nil)
	m.Require().NoError(err)
	manifests := []ManifestFile{
		NewManifestFile(3, "data.avro", 100, 1, commitSnapID).AddedRows(10).ExistingRows(5).Build(),
		NewManifestFile(3, "deletes.avro", 200, 1, commitSnapID).Content(ManifestContentDeletes).AddedRows(100).Build(),
		NewManifestFile(3, "data2.avro", 300, 1, commitSnapID).AddedRows(20).Build(),
	}
	err = writer.AddManifests(manifests)
	m.Require().NoError(err)
	// Delta = 15 + 20 = 35 (only data manifests; delete manifest ignored)
	m.EqualValues(35, *writer.NextRowID()-firstRowID)
	m.Require().NoError(writer.Close())
}

func (m *ManifestTestSuite) TestV3ManifestListWriterPersistsPerManifestFirstRowIDStart() {
	// Persisted first_row_id per manifest must be the start of each assigned row-id range.
	var buf bytes.Buffer
	commitSnapID := int64(100)
	firstRowID := int64(5000)
	sequenceNum := int64(1)

	writer, err := NewManifestListWriterV3(&buf, commitSnapID, sequenceNum, firstRowID, nil)
	m.Require().NoError(err)

	manifests := []ManifestFile{
		NewManifestFile(3, "m1.avro", 10, 1, commitSnapID).AddedRows(10).ExistingRows(5).Build(), // delta = 15
		NewManifestFile(3, "m2.avro", 10, 1, commitSnapID).AddedRows(7).Build(),                  // delta = 7
	}
	m.Require().NoError(writer.AddManifests(manifests))
	m.Require().NoError(writer.Close())

	list, err := ReadManifestList(bytes.NewReader(buf.Bytes()))
	m.Require().NoError(err)
	m.Require().Len(list, 2)

	firstManifest, ok := list[0].(*manifestFile)
	m.Require().True(ok, "expected v3 manifest file type")
	secondManifest, ok := list[1].(*manifestFile)
	m.Require().True(ok, "expected v3 manifest file type")
	m.Require().NotNil(firstManifest.FirstRowID(), "first manifest should have first_row_id")
	m.Require().NotNil(secondManifest.FirstRowID(), "second manifest should have first_row_id")

	m.EqualValues(5000, *firstManifest.FirstRowID()) // start of first range
	m.EqualValues(5015, *secondManifest.FirstRowID())
	m.EqualValues(5022, *writer.NextRowID())
}

func (m *ManifestTestSuite) TestV3PrepareEntrySequenceNumberValidation() {
	// Test v3writerImpl.prepareEntry sequence number validation logic
	v3Writer := v3writerImpl{}
	snapshotID := int64(12345)
	// Test case 1: Entry with nil sequence number and matching snapshot - should succeed
	entry1 := &manifestEntry{
		EntryStatus: EntryStatusADDED,
		Snapshot:    &snapshotID,
		SeqNum:      nil, // Will be inherited
	}
	result, err := v3Writer.prepareEntry(entry1, snapshotID)
	m.Require().NoError(err)
	m.Equal(entry1, result)
	// Test case 2: Entry with nil sequence number but mismatched snapshot - should fail
	differentSnapshot := int64(54321)
	entry2 := &manifestEntry{
		EntryStatus: EntryStatusADDED,
		Snapshot:    &differentSnapshot,
		SeqNum:      nil,
	}
	_, err = v3Writer.prepareEntry(entry2, snapshotID)
	m.Require().Error(err)
	m.Contains(err.Error(), "found unassigned sequence number for entry from snapshot")
	// Test case 3: Entry with nil sequence number but not ADDED status - should fail
	entry3 := &manifestEntry{
		EntryStatus: EntryStatusEXISTING,
		Snapshot:    &snapshotID,
		SeqNum:      nil,
	}
	_, err = v3Writer.prepareEntry(entry3, snapshotID)
	m.Require().Error(err)
	m.Contains(err.Error(), "only entries with status ADDED can be missing a sequence number")
	// Test case 4: Entry with assigned sequence number - should succeed
	assignedSeqNum := int64(42)
	entry4 := &manifestEntry{
		EntryStatus: EntryStatusEXISTING,
		Snapshot:    &snapshotID,
		SeqNum:      &assignedSeqNum,
	}
	result, err = v3Writer.prepareEntry(entry4, snapshotID)
	m.Require().NoError(err)
	m.Equal(entry4, result)
}

var errLimitedWrite = errors.New("write limit exceeded")

type limitedWriter struct {
	limit   int
	written int
	err     error
}

func (w *limitedWriter) Write(p []byte) (int, error) {
	if w.written+len(p) > w.limit {
		return 0, w.err
	}
	w.written += len(p)

	return len(p), nil
}

func (m *ManifestTestSuite) TestWriteManifestListClosesWriterOnError() {
	// A v2 manifest list cannot reference v3 manifests because the v2 entry
	// schema has no first_row_id column; this gives us a deterministic
	// AddManifests failure to assert that Close still flushes.
	seqNum := int64(7)
	var header bytes.Buffer
	writer, err := NewManifestListWriterV2(&header, snapshotID, seqNum, nil)
	m.Require().NoError(err)
	m.Require().NoError(writer.Close())

	out := &limitedWriter{limit: header.Len(), err: errLimitedWrite}
	err = WriteManifestList(2, out, snapshotID, nil, &seqNum, 0, []ManifestFile{
		manifestFileRecordsV2[0],
		manifestFileRecordsV3[0],
	})
	m.Require().Error(err)
	m.Require().ErrorContains(err, "manifest list v2 cannot reference v3 manifest files")
	m.Require().ErrorIs(err, errLimitedWrite)
}

// TestV2ManifestListAcceptsV1Manifests verifies the spec-mandated upgrade path:
// a v2 manifest list must be able to reference v1 manifest files written before
// the table was upgraded, with sequence_number and min_sequence_number both
// inherited as 0 and content inherited as data.
func (m *ManifestTestSuite) TestV2ManifestListAcceptsV1Manifests() {
	seqNum := int64(42)
	var buf bytes.Buffer

	err := WriteManifestList(2, &buf, snapshotID, nil, &seqNum, 0, []ManifestFile{
		manifestFileRecordsV1[0],
	})
	m.Require().NoError(err)

	got, err := ReadManifestList(&buf)
	m.Require().NoError(err)
	m.Require().Len(got, 1)

	entry := got[0]
	m.Equal(manifestFileRecordsV1[0].FilePath(), entry.FilePath())
	m.Equal(manifestFileRecordsV1[0].Length(), entry.Length())
	m.Equal(ManifestContentData, entry.ManifestContent(), "v1 inheritance: content must be data")
	m.Equal(int64(0), entry.SequenceNum(), "v1 inheritance: sequence_number must be 0")
	m.Equal(int64(0), entry.MinSequenceNum(), "v1 inheritance: min_sequence_number must be 0")
	m.Equal(manifestFileRecordsV1[0].AddedRows(), entry.AddedRows())
}

// TestV3ManifestListAcceptsV1AndV2Manifests verifies that a v3 manifest list
// can reference both v1 and v2 manifest files (e.g. after a v1->v3 upgrade)
// and that first_row_id is assigned to data manifests during the write.
func (m *ManifestTestSuite) TestV3ManifestListAcceptsV1AndV2Manifests() {
	seqNum := int64(7)
	firstRowID := int64(1000)
	var buf bytes.Buffer

	err := WriteManifestList(3, &buf, snapshotID, nil, &seqNum, firstRowID, []ManifestFile{
		manifestFileRecordsV1[0],
		manifestFileRecordsV2[0],
	})
	m.Require().NoError(err)

	got, err := ReadManifestList(&buf)
	m.Require().NoError(err)
	m.Require().Len(got, 2)

	v1Entry := got[0]
	m.Equal(manifestFileRecordsV1[0].FilePath(), v1Entry.FilePath())
	m.Equal(ManifestContentData, v1Entry.ManifestContent())
	m.Equal(int64(0), v1Entry.SequenceNum())
	m.Equal(int64(0), v1Entry.MinSequenceNum())
	m.Require().NotNil(v1Entry.FirstRowID(), "v3 list must assign first_row_id for data manifests")
	m.Equal(firstRowID, *v1Entry.FirstRowID())

	v2Entry := got[1]
	m.Equal(manifestFileRecordsV2[0].FilePath(), v2Entry.FilePath())
	// manifestFileRecordsV2[0] is a delete manifest, so first_row_id is not
	// assigned (assignment is data-only per the v3 ManifestListWriter rules).
	m.Equal(ManifestContentDeletes, v2Entry.ManifestContent())
	m.Nil(v2Entry.FirstRowID(), "delete manifests must not be assigned first_row_id")
}

// TestV2ManifestListRejectsV3Manifests confirms that a v2 manifest list still
// refuses to reference v3 manifest files, since the v2 entry schema has no
// place to record first_row_id and accepting them would silently drop data.
func (m *ManifestTestSuite) TestV2ManifestListRejectsV3Manifests() {
	seqNum := int64(1)
	var buf bytes.Buffer

	err := WriteManifestList(2, &buf, snapshotID, nil, &seqNum, 0, []ManifestFile{
		manifestFileRecordsV3[0],
	})
	m.Require().Error(err)
	m.Require().ErrorIs(err, ErrInvalidArgument)
	m.Require().ErrorContains(err, "manifest list v2 cannot reference v3 manifest files")
}

// TestManifestRoundTripSortOrderID verifies that a sort_order_id written onto
// a data file survives an avro manifest round-trip (write → read). This
// backs the end-to-end guarantee that callers of WriteTask/WriteFileInfo see
// the value they set on disk.
func (m *ManifestTestSuite) TestManifestRoundTripSortOrderID() {
	var buf bytes.Buffer
	partitionSpec := NewPartitionSpecID(0)
	snapshotID := int64(12345678)
	seqNum := int64(9876)
	const expectedSortOrderID = 3

	dataFileBuilder, err := NewDataFileBuilder(
		partitionSpec,
		EntryContentData,
		"s3://bucket/ns/table/data/round-trip.parquet",
		ParquetFile,
		map[int]any{},
		map[int]string{},
		map[int]int{},
		10,
		10*1000,
	)
	m.Require().NoError(err)
	dataFileBuilder.SortOrderID(expectedSortOrderID)

	file, err := WriteManifest(
		"s3://bucket/ns/table/metadata/round-trip.avro", &buf, 2,
		partitionSpec,
		NewSchema(0,
			NestedField{ID: 1, Name: "id", Type: Int64Type{}},
		),
		snapshotID,
		[]ManifestEntry{NewManifestEntry(
			EntryStatusADDED,
			&snapshotID,
			&seqNum, &seqNum,
			dataFileBuilder.Build(),
		)},
	)
	m.Require().NoError(err)

	entries, err := ReadManifest(file, &buf, false)
	m.Require().NoError(err)
	m.Require().Len(entries, 1)
	got := entries[0].DataFile().SortOrderID()
	m.Require().NotNil(got, "SortOrderID must round-trip to a non-nil value")
	m.Equal(expectedSortOrderID, *got)
}

// TestWriteManifestV3OmitsDistinctCounts verifies the v3 writer clears
// data_file.distinct_counts (deprecated by v3 spec; Java parity:
// apache/iceberg#12182). v2 round-trip will be added with #1038.
func (m *ManifestTestSuite) TestWriteManifestV3OmitsDistinctCounts() {
	partitionSpec := NewPartitionSpecID(0)
	snapshotID := int64(1)
	seqNum := int64(1)

	dataFileBuilder, err := NewDataFileBuilder(
		partitionSpec,
		EntryContentData,
		"s3://bucket/ns/table/data/distinct.parquet",
		ParquetFile,
		map[int]any{},
		map[int]string{},
		map[int]int{},
		1,
		1,
	)
	m.Require().NoError(err)
	dataFileBuilder.DistinctValueCounts(map[int]int64{1: 42})

	entry := NewManifestEntry(
		EntryStatusADDED,
		&snapshotID,
		&seqNum, &seqNum,
		dataFileBuilder.Build(),
	)

	var buf bytes.Buffer
	_, err = WriteManifest(
		"s3://bucket/ns/table/metadata/distinct.avro", &buf, 3,
		partitionSpec,
		NewSchema(
			0,
			NestedField{ID: 1, Name: "id", Type: Int64Type{}, Required: true},
		),
		snapshotID,
		[]ManifestEntry{entry},
	)
	m.Require().NoError(err)

	df, ok := entry.DataFile().(*dataFile)
	m.Require().True(ok)
	m.Nil(df.DistinctCounts, "v3 writer must clear DistinctCounts on the entry's *dataFile")
}

func (m *ManifestTestSuite) TestWriteManifestClosesWriterOnEntryError() {
	partitionSpec := NewPartitionSpecID(1,
		PartitionField{FieldID: 1000, SourceIDs: []int{1}, Name: "VendorID", Transform: IdentityTransform{}},
		PartitionField{FieldID: 1001, SourceIDs: []int{2}, Name: "tpep_pickup_datetime", Transform: IdentityTransform{}})

	var header bytes.Buffer
	writer, err := NewManifestWriter(2, &header, partitionSpec, testSchema, entrySnapshotID)
	m.Require().NoError(err)
	headerLen := header.Len()

	m.Require().NoError(writer.Add(manifestEntryV2Records[0]))
	m.Require().NoError(writer.Close())

	badEntry := *manifestEntryV2Records[1]
	badEntry.EntryStatus = EntryStatusEXISTING
	badEntry.SeqNum = nil

	out := &limitedWriter{limit: headerLen, err: errLimitedWrite}
	_, err = WriteManifest("test.avro", out, 2, partitionSpec, testSchema, entrySnapshotID, []ManifestEntry{
		manifestEntryV2Records[0],
		&badEntry,
	})
	m.Require().Error(err)
	m.Require().ErrorContains(err, "only entries with status ADDED")
	m.Require().ErrorIs(err, errLimitedWrite)
}

type trackCloseFile struct {
	contents   *bytes.Reader
	closeCount int
	closeErr   error
	readLimit  int
	readErr    error
	nRead      int
}

var _ iceio.File = (*trackCloseFile)(nil)

func newTrackCloseFile(data []byte) *trackCloseFile {
	return &trackCloseFile{contents: bytes.NewReader(data)}
}

func (f *trackCloseFile) Stat() (fs.FileInfo, error) { return nil, nil }

func (f *trackCloseFile) Read(p []byte) (int, error) {
	if f.readErr != nil && f.nRead >= f.readLimit {
		return 0, f.readErr
	}
	if f.readErr != nil && f.nRead+len(p) > f.readLimit {
		p = p[:f.readLimit-f.nRead]
	}
	n, err := f.contents.Read(p)
	f.nRead += n

	return n, err
}

func (f *trackCloseFile) Close() error {
	f.closeCount++

	return f.closeErr
}

func (f *trackCloseFile) Seek(offset int64, whence int) (int64, error) {
	return f.contents.Seek(offset, whence)
}

func (f *trackCloseFile) ReadAt(p []byte, off int64) (int, error) {
	return f.contents.ReadAt(p, off)
}

var (
	errMidStreamRead  = errors.New("simulated mid-stream read error")
	errCloseFinalPair = errors.New("simulated close error")
)

func (m *ManifestTestSuite) TestEntriesEarlyBreakClosesFile() {
	var mockfs internal.MockFS
	manifest := manifestFile{
		version: 2,
		SpecID:  1,
		Path:    manifestFileRecordsV2[0].FilePath(),
	}

	file := newTrackCloseFile(m.v2ManifestEntries.Bytes())
	mockfs.Test(m.T())
	mockfs.On("Open", manifest.FilePath()).Return(file, nil)
	defer mockfs.AssertExpectations(m.T())

	yielded := 0
	for entry, err := range manifest.Entries(&mockfs, false) {
		m.Require().NoError(err, "no error expected on a healthy manifest before break")
		m.Require().NotNil(entry)
		yielded++
		if yielded == 1 {
			break
		}
	}
	m.Equal(1, yielded, "iteration must stop after the first entry on early break")
	m.Equal(1, file.closeCount, "file must be closed exactly once on early break")
}

func (m *ManifestTestSuite) TestEntriesMidStreamErrorYieldsAndStops() {
	var mockfs internal.MockFS
	manifest := manifestFile{
		version: 2,
		SpecID:  1,
		Path:    manifestFileRecordsV2[0].FilePath(),
	}

	data := m.v2ManifestEntries.Bytes()
	file := newTrackCloseFile(data)
	file.readLimit = len(data) / 2
	file.readErr = errMidStreamRead
	mockfs.Test(m.T())
	mockfs.On("Open", manifest.FilePath()).Return(file, nil)
	defer mockfs.AssertExpectations(m.T())

	var (
		entries  []ManifestEntry
		gotError error
		yields   int
	)
	for entry, err := range manifest.Entries(&mockfs, false) {
		yields++
		if err != nil {
			gotError = err

			break
		}
		entries = append(entries, entry)
	}
	m.Require().NotNil(gotError, "iterator must yield a non-nil error")
	m.Require().ErrorIs(gotError, errMidStreamRead,
		"yielded error must equal or wrap the simulated mid-stream read error")
	m.Equal(yields, len(entries)+1,
		"the error pair must follow zero or more entry pairs and stop iteration")
	m.Equal(1, file.closeCount, "file must be closed exactly once after a mid-stream error")
}

func (m *ManifestTestSuite) TestEntriesCloseErrorAsFinalPair() {
	var mockfs internal.MockFS
	manifest := manifestFile{
		version: 2,
		SpecID:  1,
		Path:    manifestFileRecordsV2[0].FilePath(),
	}

	file := newTrackCloseFile(m.v2ManifestEntries.Bytes())
	file.closeErr = errCloseFinalPair
	mockfs.Test(m.T())
	mockfs.On("Open", manifest.FilePath()).Return(file, nil)
	defer mockfs.AssertExpectations(m.T())

	var (
		entries []ManifestEntry
		errs    []error
	)
	for entry, err := range manifest.Entries(&mockfs, false) {
		if err != nil {
			errs = append(errs, err)

			continue
		}
		entries = append(entries, entry)
	}
	m.Len(entries, 2, "iteration must consume every entry before the terminal close pair")
	m.Require().Len(errs, 1, "iterator must yield exactly one terminal close error")
	m.ErrorIs(errs[0], errCloseFinalPair,
		"terminal error must equal or wrap the simulated close error")
	m.Equal(1, file.closeCount, "file must be closed exactly once even when Close returns an error")
}

// TestWriteManifestV2KeepsDistinctCounts is a regression guard that v2
// manifest writers preserve data_file.distinct_counts (id 111) per the
// Iceberg v2 spec. Fixes #1038.
func (m *ManifestTestSuite) TestWriteManifestV2KeepsDistinctCounts() {
	m.assertDistinctCountsRoundTrip(2)
}

// TestWriteManifestV1KeepsDistinctCounts is a regression guard that v1
// manifest writers preserve data_file.distinct_counts (id 111) per the
// Iceberg v1 spec. Fixes #1038.
func (m *ManifestTestSuite) TestWriteManifestV1KeepsDistinctCounts() {
	m.assertDistinctCountsRoundTrip(1)
}

// assertDistinctCountsRoundTrip writes a manifest at the given format
// version with distinct_counts populated for one column, round-trips it
// through ReadManifest, and asserts the read side observes the same map.
func (m *ManifestTestSuite) assertDistinctCountsRoundTrip(version int) {
	partitionSpec := NewPartitionSpecID(0)
	snapshotID := int64(1)
	seqNum := int64(1)

	dataFileBuilder, err := NewDataFileBuilder(
		partitionSpec,
		EntryContentData,
		"s3://bucket/ns/table/data/distinct.parquet",
		ParquetFile,
		map[int]any{},
		map[int]string{},
		map[int]int{},
		1,
		1,
	)
	m.Require().NoError(err)
	dataFileBuilder.DistinctValueCounts(map[int]int64{1: 42})

	var buf bytes.Buffer
	file, err := WriteManifest(
		"s3://bucket/ns/table/metadata/distinct.avro", &buf, version,
		partitionSpec,
		NewSchema(0,
			NestedField{ID: 1, Name: "id", Type: Int64Type{}, Required: true},
		),
		snapshotID,
		[]ManifestEntry{NewManifestEntry(
			EntryStatusADDED,
			&snapshotID,
			&seqNum, &seqNum,
			dataFileBuilder.Build(),
		)},
	)
	m.Require().NoError(err)

	entries, err := ReadManifest(file, &buf, false)
	m.Require().NoError(err)
	m.Require().Len(entries, 1)

	m.Equal(map[int]int64{1: 42}, entries[0].DataFile().DistinctValueCounts(),
		"manifest writer must preserve distinct_counts for the requested format version")
}
