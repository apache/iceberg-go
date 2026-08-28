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

package internal

import (
	"context"
	"errors"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/iceberg-go"
	iceio "github.com/apache/iceberg-go/io"
	"github.com/geoarrow/geoarrow-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type failingWriteFileWriter struct {
	writeErr error
	abortErr error

	abortCalled bool
	closeCalled bool
}

func (w *failingWriteFileWriter) Write(arrow.RecordBatch) error {
	return w.writeErr
}

func (*failingWriteFileWriter) BytesWritten() int64 {
	return 0
}

func (w *failingWriteFileWriter) Close() (iceberg.DataFile, error) {
	w.closeCalled = true

	return nil, nil
}

func (w *failingWriteFileWriter) Abort() error {
	w.abortCalled = true

	return w.abortErr
}

func TestWriteDataFileBatchesAbortsOnWriteError(t *testing.T) {
	tests := []struct {
		name     string
		abortErr error
	}{
		{name: "abort succeeds"},
		{name: "abort fails", abortErr: errors.New("abort failed")},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			writeErr := errors.New("write failed")
			writer := &failingWriteFileWriter{
				writeErr: writeErr,
				abortErr: tt.abortErr,
			}

			// The fake writer ignores the batch, so nil keeps this focused on cleanup.
			df, err := writeDataFileBatches(writer, []arrow.RecordBatch{nil})
			require.Nil(t, df)
			require.Error(t, err)

			assert.True(t, errors.Is(err, writeErr))
			if tt.abortErr != nil {
				assert.True(t, errors.Is(err, tt.abortErr))
			} else {
				assert.True(t, err == writeErr, "abort success should return the raw write error")
			}
			assert.True(t, writer.abortCalled)
			assert.False(t, writer.closeCalled)
		})
	}
}

func TestNewFileWriterCachesGeoNormalizationColumns(t *testing.T) {
	typeDef := geoarrow.NewWKBType(geoarrow.WKBWithBinaryStorage())
	arrowSchema := arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int32, Nullable: false},
		{Name: "geom", Type: typeDef, Nullable: true},
	}, nil)
	fileSchema := iceberg.NewSchema(0,
		iceberg.NestedField{ID: 1, Name: "id", Type: iceberg.PrimitiveTypes.Int32, Required: true},
		iceberg.NestedField{ID: 2, Name: "geom", Type: iceberg.GeometryType{}, Required: false},
	)
	format := parquetFormat{}
	writer, err := format.NewFileWriter(context.Background(), iceio.NewMemFS(), nil, WriteFileInfo{
		FileSchema: fileSchema,
		Spec:       *iceberg.UnpartitionedSpec,
		FileName:   "geo-cache.parquet",
		WriteProps: format.GetWriteProperties(iceberg.Properties{}),
	}, arrowSchema)
	require.NoError(t, err)
	defer func() { require.NoError(t, writer.Abort()) }()

	parquetWriter, ok := writer.(*ParquetFileWriter)
	require.True(t, ok)
	require.Equal(t, []int{1}, parquetWriter.geoNormalizeCols)
}

func TestNewFileWriterUsesProvidedSchemaMetadata(t *testing.T) {
	arrowSchema := arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int32, Nullable: false},
	}, nil)
	fileSchema := iceberg.NewSchema(0,
		iceberg.NestedField{ID: 1, Name: "id", Type: iceberg.PrimitiveTypes.Int32, Required: true},
	)
	colMapping := map[string]int{"provided": 99}
	variantFieldIDs := map[int]struct{}{99: {}}
	format := parquetFormat{}
	writer, err := format.NewFileWriter(context.Background(), iceio.NewMemFS(), nil, WriteFileInfo{
		FileSchema:      fileSchema,
		Spec:            *iceberg.UnpartitionedSpec,
		FileName:        "provided-schema-metadata.parquet",
		ColMapping:      colMapping,
		VariantFieldIDs: variantFieldIDs,
		WriteProps:      format.GetWriteProperties(iceberg.Properties{}),
	}, arrowSchema)
	require.NoError(t, err)
	defer func() { require.NoError(t, writer.Abort()) }()

	parquetWriter, ok := writer.(*ParquetFileWriter)
	require.True(t, ok)
	assert.Equal(t, colMapping, parquetWriter.colMapping)
	assert.Equal(t, variantFieldIDs, parquetWriter.variantFieldIDs)
	assert.Equal(t, 99, parquetWriter.colMapping["provided"])
}
