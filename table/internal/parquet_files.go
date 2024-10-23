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
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/arrow-go/v18/parquet"
	"github.com/apache/arrow-go/v18/parquet/file"
	"github.com/apache/arrow-go/v18/parquet/pqarrow"
	"github.com/apache/iceberg-go"
	iceio "github.com/apache/iceberg-go/io"
)

func GetFile(mem memory.Allocator, fs iceio.IO, dataFile iceberg.DataFile, isPosDeletes bool) (FileSource, error) {
	if mem == nil {
		mem = memory.DefaultAllocator
	}

	switch dataFile.FileFormat() {
	case iceberg.ParquetFile:
		return &ParquetFileSource{
			mem:          mem,
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

	ReadTable(context.Context) (arrow.Table, error)
}

type ParquetFileSource struct {
	mem          memory.Allocator
	fs           iceio.IO
	file         iceberg.DataFile
	isPosDeletes bool
}

type wrapPqArrowReader struct {
	*pqarrow.FileReader
}

func (w wrapPqArrowReader) Close() error {
	return w.ParquetReader().Close()
}

func (pfs *ParquetFileSource) GetReader(ctx context.Context) (FileReader, error) {
	pf, err := pfs.fs.Open(pfs.file.FilePath())
	if err != nil {
		return nil, err
	}

	rdr, err := file.NewParquetReader(pf,
		file.WithReadProps(parquet.NewReaderProperties(pfs.mem)))
	if err != nil {
		return nil, err
	}

	// TODO: grab these from the context
	arrProps := pqarrow.ArrowReadProperties{
		Parallel:  true,
		BatchSize: 1 << 17,
	}

	if pfs.isPosDeletes {
		// for dictionary for filepath col
		arrProps.SetReadDict(0, true)
	}

	fr, err := pqarrow.NewFileReader(rdr, arrProps, pfs.mem)
	if err != nil {
		return nil, err
	}

	return wrapPqArrowReader{fr}, nil
}
