// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package internal

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/iceberg-go"
	iceio "github.com/apache/iceberg-go/io"
	"github.com/stretchr/testify/require"
)

func TestORCReadDatesWithoutPresenceStream(t *testing.T) {
	path := writeORCFixture(t, "struct<d:date>", map[int]map[string]string{1: {"iceberg.id": "1"}},
		[]any{time.Date(1969, 12, 31, 0, 0, 0, 0, time.UTC)},
		[]any{time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)})
	rd := openORCFixture(t, path, 2)
	records, err := rd.GetRecords(context.Background(), nil, nil)
	require.NoError(t, err)
	defer records.Release()
	require.True(t, records.Next())
	values := records.RecordBatch().Column(0).(*array.Date32)
	require.EqualValues(t, -1, values.Value(0))
	require.EqualValues(t, 19723, values.Value(1))
	require.False(t, records.Next())
	require.NoError(t, records.Err())
}

func TestORCRejectsDatePresenceStream(t *testing.T) {
	path := writeORCFixture(t, "struct<d:date>", map[int]map[string]string{1: {"iceberg.id": "1"}},
		[]any{time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)}, []any{nil})
	stat, err := os.Stat(path)
	require.NoError(t, err)
	b, err := iceberg.NewDataFileBuilder(*iceberg.UnpartitionedSpec, iceberg.EntryContentData, path,
		iceberg.OrcFile, nil, nil, nil, 2, stat.Size())
	require.NoError(t, err)
	source, err := GetFile(context.Background(), iceio.LocalFS{}, b.Build(), false)
	require.NoError(t, err)
	_, err = source.GetReader(context.Background())
	require.ErrorIs(t, err, iceberg.ErrNotImplemented)
}

func TestORCRejectsHistoricalDateWithoutCalendarConversion(t *testing.T) {
	path := writeORCFixture(t, "struct<d:date>", map[int]map[string]string{1: {"iceberg.id": "1"}},
		[]any{time.Date(1582, 10, 14, 0, 0, 0, 0, time.UTC)})
	rd := openORCFixture(t, path, 1)
	records, err := rd.GetRecords(context.Background(), nil, nil)
	require.NoError(t, err)
	defer records.Release()
	require.False(t, records.Next())
	require.ErrorIs(t, records.Err(), iceberg.ErrNotImplemented)
}

func TestORCReadCompressedDates(t *testing.T) {
	for _, codec := range []string{"zlib", "snappy"} {
		t.Run(codec, func(t *testing.T) {
			rd := openORCFixture(t, "testdata/orc/date_"+codec+".orc", 2)
			records, err := rd.GetRecords(context.Background(), nil, nil)
			require.NoError(t, err)
			defer records.Release()
			require.True(t, records.Next())
			values := records.RecordBatch().Column(0).(*array.Date32)
			require.EqualValues(t, -1, values.Value(0))
			require.EqualValues(t, 19723, values.Value(1))
			require.False(t, records.Next())
			require.NoError(t, records.Err())
		})
	}
}

func TestORCReadNestedDates(t *testing.T) {
	schema := "struct<profile:struct<d:date>>"
	attrs := map[int]map[string]string{1: {"iceberg.id": "1"}, 2: {"iceberg.id": "2"}}
	d := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)
	t.Run("non-null", func(t *testing.T) {
		path := writeORCFixture(t, schema, attrs, []any{[]any{d}})
		rd := openORCFixture(t, path, 1)
		records, err := rd.GetRecords(context.Background(), nil, nil)
		require.NoError(t, err)
		defer records.Release()
		require.True(t, records.Next())
		st := records.RecordBatch().Column(0).(*array.Struct)
		require.EqualValues(t, 19723, st.Field(0).(*array.Date32).Value(0))
		require.False(t, records.Next())
		require.NoError(t, records.Err())
	})
	t.Run("nullable-child", func(t *testing.T) {
		path := writeORCFixture(t, schema, attrs, []any{[]any{d}}, []any{[]any{nil}})
		stat, err := os.Stat(path)
		require.NoError(t, err)
		b, err := iceberg.NewDataFileBuilder(*iceberg.UnpartitionedSpec, iceberg.EntryContentData, path, iceberg.OrcFile, nil, nil, nil, 2, stat.Size())
		require.NoError(t, err)
		src, err := GetFile(context.Background(), iceio.LocalFS{}, b.Build(), false)
		require.NoError(t, err)
		_, err = src.GetReader(context.Background())
		require.ErrorIs(t, err, iceberg.ErrNotImplemented)
	})
}
