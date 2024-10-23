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
	"context"
	"runtime"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/compute"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/iceberg-go"
	iceio "github.com/apache/iceberg-go/io"
	"github.com/apache/iceberg-go/table/internal"
	"golang.org/x/sync/errgroup"
)

func readAllDeleteFiles(ctx context.Context, fs iceio.IO, tasks []FileScanTask) (map[string][]*arrow.Chunked, error) {
	var (
		deletesPerFile = make(map[string][]*arrow.Chunked)
		uniqueDeletes  = make(map[string]iceberg.DataFile)
		err            error
	)

	for _, t := range tasks {
		for _, d := range t.DeleteFiles {
			if _, ok := uniqueDeletes[d.FilePath()]; !ok {
				uniqueDeletes[d.FilePath()] = d
			}
		}
	}

	if len(uniqueDeletes) == 0 {
		return deletesPerFile, nil
	}

	g, ctx := errgroup.WithContext(ctx)
	g.SetLimit(runtime.NumCPU())

	perFileChan := make(chan map[string]*arrow.Chunked, runtime.NumCPU())
	go func() {
		defer close(perFileChan)
		for _, v := range uniqueDeletes {
			g.Go(func() error {
				deletes, err := readDeletes(ctx, fs, v)
				if deletes != nil {
					perFileChan <- deletes
				}
				return err
			})
		}

		err = g.Wait()
	}()

	for deletes := range perFileChan {
		for file, arr := range deletes {
			deletesPerFile[file] = append(deletesPerFile[file], arr)
		}
	}

	return deletesPerFile, err
}

func readDeletes(ctx context.Context, fs iceio.IO, dataFile iceberg.DataFile) (map[string]*arrow.Chunked, error) {
	src, err := internal.GetFile(memory.DefaultAllocator, fs, dataFile, true)
	if err != nil {
		return nil, err
	}

	rdr, err := src.GetReader(ctx)
	if err != nil {
		return nil, err
	}
	defer rdr.Close()

	tbl, err := rdr.ReadTable(ctx)
	if err != nil {
		return nil, err
	}
	defer tbl.Release()

	tbl, err = array.UnifyTableDicts(memory.DefaultAllocator, tbl)
	if err != nil {
		return nil, err
	}
	defer tbl.Release()

	filePathCol := tbl.Column(tbl.Schema().FieldIndices("file_path")[0]).Data()
	posCol := tbl.Column(tbl.Schema().FieldIndices("pos")[0]).Data()
	dict := filePathCol.Chunk(0).(*array.Dictionary).Dictionary().(*array.String)

	results := make(map[string]*arrow.Chunked)
	for i := 0; i < dict.Len(); i++ {
		v := dict.Value(i)

		mask, err := compute.CallFunction(ctx, "equal", nil,
			compute.NewDatumWithoutOwning(filePathCol), compute.NewDatum(v))
		if err != nil {
			return nil, err
		}
		defer mask.Release()

		filtered, err := compute.Filter(ctx, compute.NewDatumWithoutOwning(posCol),
			mask, *compute.DefaultFilterOptions())
		if err != nil {
			return nil, err
		}

		results[v] = filtered.(*compute.ChunkedDatum).Value
	}
	return results, nil
}

type set[T comparable] map[T]struct{}

type arrowScan struct {
	fs              iceio.IO
	metadata        Metadata
	projectedSchema *iceberg.Schema
	boundRowFilter  iceberg.BooleanExpression
	caseSensitive   bool
	rowLimit        int64
}

func (as *arrowScan) projectedFieldIDs() (set[int], error) {
	idset := set[int]{}
	for _, field := range as.projectedSchema.Fields() {
		switch field.Type.(type) {
		case *iceberg.MapType, *iceberg.ListType:
		default:
			idset[field.ID] = struct{}{}
		}
	}

	if as.boundRowFilter != nil {
		extracted, err := iceberg.ExtractFieldIDs(as.boundRowFilter)
		if err != nil {
			return nil, err
		}

		for _, id := range extracted {
			idset[id] = struct{}{}
		}
	}

	return idset, nil
}
