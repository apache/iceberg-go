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
	"fmt"
	"io"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/compute"
	"github.com/apache/iceberg-go"
	iceio "github.com/apache/iceberg-go/io"
)

func GetFile(ctx context.Context, fs iceio.IO, dataFile iceberg.DataFile, isPosDeletes bool) (FileSource, error) {
	switch dataFile.FileFormat() {
	case iceberg.ParquetFile:
		return &ParquetFileSource{
			mem:          compute.GetAllocator(ctx),
			fs:           fs,
			file:         dataFile,
			isPosDeletes: isPosDeletes,
		}, nil
	default:
		return nil, fmt.Errorf("%w: only parquet format is implemented, got %s",
			iceberg.ErrNotImplemented, dataFile.FileFormat())
	}
}

type FileSource interface {
	GetReader(context.Context) (FileReader, error)
}

type FileReader interface {
	io.Closer

	PrunedSchema(projectedIDs map[int]struct{}) (*arrow.Schema, []int, error)
	GetRecords(ctx context.Context, cols []int, tester any) (array.RecordReader, error)
	ReadTable(context.Context) (arrow.Table, error)
}
