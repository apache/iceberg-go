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
	"fmt"
	"iter"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/iceberg-go"
	"github.com/apache/iceberg-go/io"
	"github.com/apache/iceberg-go/table/internal"
	"github.com/google/uuid"
	"golang.org/x/sync/errgroup"
)

type WriteTask struct {
	Uuid        uuid.UUID
	ID          int
	Schema      *iceberg.Schema
	Batches     []arrow.Record
	SortOrderID int
}

func (w WriteTask) GenerateDataFileName(extension string) string {
	// Mimics the behavior in the Java API:
	// https://github.com/apache/iceberg/blob/a582968975dd30ff4917fbbe999f1be903efac02/core/src/main/java/org/apache/iceberg/io/OutputFileFactory.java#L92-L101
	return fmt.Sprintf("00000-%d-%s.%s", w.ID, w.Uuid, extension)
}

type writer struct {
	loc        LocationProvider
	fs         io.WriteFileIO
	fileSchema *iceberg.Schema
	format     internal.FileFormat
	props      any
	meta       *MetadataBuilder
}

func (w *writer) writeFile(ctx context.Context, task WriteTask, ch chan<- iceberg.DataFile) error {
	defer func() {
		for _, b := range task.Batches {
			b.Release()
		}
	}()

	batches := make([]arrow.Record, len(task.Batches))
	for i, b := range task.Batches {
		rec, err := ToRequestedSchema(ctx, w.fileSchema,
			task.Schema, b, false, true, false)
		if err != nil {
			return err
		}
		batches[i] = rec
	}

	statsCols, err := computeStatsPlan(w.fileSchema, w.meta.props)
	if err != nil {
		return err
	}

	filePath := w.loc.NewDataLocation(
		task.GenerateDataFileName("parquet"))

	df, err := w.format.WriteDataFile(ctx, w.fs, internal.WriteFileInfo{
		FileSchema: w.fileSchema,
		FileName:   filePath,
		StatsCols:  statsCols,
		WriteProps: w.props,
	}, batches)
	if err != nil {
		return err
	}

	ch <- df

	return nil
}

func writeFiles(ctx context.Context, rootLocation string, fs io.WriteFileIO, meta *MetadataBuilder, tasks iter.Seq[WriteTask]) iter.Seq2[iceberg.DataFile, error] {
	locProvider, err := LoadLocationProvider(rootLocation, meta.props)
	if err != nil {
		return func(yield func(iceberg.DataFile, error) bool) {
			yield(nil, err)
		}
	}

	ch := make(chan iceberg.DataFile, 10)

	var g *errgroup.Group
	g, ctx = errgroup.WithContext(ctx)

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	nworkers := 5
	input := make(chan WriteTask, nworkers)
	go func() {
		defer close(input)
		for t := range tasks {
			input <- t
		}
	}()

	format := internal.GetFileFormat(iceberg.ParquetFile)
	fileSchema := meta.CurrentSchema()
	sanitized, err := iceberg.SanitizeColumnNames(fileSchema)
	if err != nil {
		return func(yield func(iceberg.DataFile, error) bool) {
			yield(nil, err)
		}
	}

	// if the schema needs to be transformed, use the transformed schema
	// and adjust the arrow schema appropriately. otherwise we just
	// use the original schema.
	if !sanitized.Equals(fileSchema) {
		fileSchema = sanitized
	}

	w := &writer{
		loc:        locProvider,
		fs:         fs,
		fileSchema: fileSchema,
		format:     format,
		props:      format.GetWriteProperties(meta.props),
		meta:       meta,
	}

	for range nworkers {
		g.Go(func() error {
			for task := range input {
				if err := w.writeFile(ctx, task, ch); err != nil {
					return err
				}
			}

			return nil
		})
	}

	go func() {
		defer close(ch)
		err = g.Wait()
	}()

	return func(yield func(iceberg.DataFile, error) bool) {
		defer cancel()
		for file := range ch {
			if !yield(file, nil) {
				return
			}
		}

		if err != nil {
			yield(nil, err)
		}
	}
}
