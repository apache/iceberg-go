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
	"os"
	"testing"

	"github.com/apache/iceberg-go"
	iceio "github.com/apache/iceberg-go/io"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
)

func TestTableAvroWritesRejectedBeforeReaderAvailable(t *testing.T) {
	location := t.TempDir()
	schema := iceberg.NewSchema(0,
		iceberg.NestedField{ID: 1, Name: "id", Type: iceberg.Int64Type{}, Required: true},
	)
	spec := iceberg.NewPartitionSpec()
	props := iceberg.Properties{WriteFormatDefaultKey: "avro"}
	meta, err := NewMetadata(schema, &spec, UnsortedSortOrder, location, props)
	require.NoError(t, err)
	builder, err := MetadataBuilderFromBase(meta, "")
	require.NoError(t, err)

	t.Run("batch writer", func(t *testing.T) {
		writer, err := newDataFileWriter(location, iceio.LocalFS{}, builder, props)
		require.Nil(t, writer)
		require.ErrorIs(t, err, iceberg.ErrNotImplemented)
		require.ErrorContains(t, err, "AVRO")
	})

	t.Run("streaming writer", func(t *testing.T) {
		writeID := uuid.New()
		args := recordWritingArgs{
			fs:        iceio.LocalFS{},
			writeUUID: &writeID,
			counter:   func(yield func(int) bool) { yield(0) },
		}
		factory, err := newWriterFactory(location, args, builder, schema, 1024)
		require.Nil(t, factory)
		require.ErrorIs(t, err, iceberg.ErrNotImplemented)
		require.ErrorContains(t, err, "AVRO")
	})

	files, err := os.ReadDir(location)
	require.NoError(t, err)
	require.Empty(t, files)
}
