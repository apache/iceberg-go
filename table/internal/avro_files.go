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
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package internal

import (
	"context"
	"errors"
	"fmt"
	"io"
	"math"
	"strconv"
	"sync/atomic"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/compute"
	"github.com/apache/iceberg-go"
	iceio "github.com/apache/iceberg-go/io"
	"github.com/twmb/avro/ocf"
)

// AvroFileSource reads Iceberg Avro object container files. The decoder
// streams bounded Arrow batches, preserving field IDs and nested values.
// Projection is applied by the scanner after the full file schema is read.
type AvroFileSource struct {
	fs   iceio.IO
	file iceberg.DataFile
}

func (s *AvroFileSource) GetReader(ctx context.Context) (FileReader, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	f, err := s.fs.Open(ctx, s.file.FilePath())
	if err != nil {
		return nil, err
	}
	reader, err := newAvroFileReader(ctx, f)
	if err != nil {
		return nil, errors.Join(err, f.Close())
	}

	return reader, nil
}

type avroFileReader struct {
	file     iceio.File
	records  *ocf.Reader
	schema   *arrow.Schema
	fileSize int64
	used     bool
	closed   bool
}

func newAvroFileReader(ctx context.Context, f iceio.File) (*avroFileReader, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	stat, err := f.Stat()
	if err != nil {
		return nil, err
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	records, err := ocf.NewReader(f)
	if err != nil {
		return nil, err
	}
	schemaJSON := string(records.Metadata()["avro.schema"])
	if err := validateAvroReadSchema(schemaJSON); err != nil {
		return nil, errors.Join(err, records.Close())
	}
	schema, err := avroArrowSchema(schemaJSON)
	if err != nil {
		return nil, errors.Join(err, records.Close())
	}
	if err := ctx.Err(); err != nil {
		return nil, errors.Join(err, records.Close())
	}

	return &avroFileReader{
		file: f, records: records, schema: schema, fileSize: stat.Size(),
	}, nil
}

func (r *avroFileReader) Metadata() Metadata             { return nil }
func (r *avroFileReader) SourceFileSize() int64          { return r.fileSize }
func (r *avroFileReader) Schema() (*arrow.Schema, error) { return r.schema, nil }

func (r *avroFileReader) PrunedSchema(_ map[int]struct{}, _ iceberg.NameMapping) (*arrow.Schema, []int, error) {
	// Retain all file fields for residual filters and schema evolution.
	return r.schema, nil, nil
}

func (r *avroFileReader) GetRecords(ctx context.Context, cols []int, tester any) (array.RecordReader, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if r.closed {
		return nil, fmt.Errorf("%w: Avro file reader is closed", iceberg.ErrInvalidArgument)
	}
	if r.used {
		return nil, fmt.Errorf("%w: Avro file streams are single-use", iceberg.ErrInvalidArgument)
	}
	if cols != nil {
		return nil, fmt.Errorf("%w: Avro column selection is not supported", iceberg.ErrNotImplemented)
	}
	if tester != nil {
		return nil, fmt.Errorf("%w: Avro files do not support Parquet row-group filters", iceberg.ErrInvalidArgument)
	}
	r.used = true
	wrapped := &avroRecordReader{records: r.records, schema: r.schema, ctx: ctx}
	wrapped.refs.Store(1)

	return wrapped, nil
}

// avroRecordReader owns the current Arrow batch. Its OCF stream belongs to
// avroFileReader and is closed when that FileReader is closed.
type avroRecordReader struct {
	records  *ocf.Reader
	schema   *arrow.Schema
	current  arrow.RecordBatch
	refs     atomic.Int64
	ctx      context.Context
	readErr  error
	finished bool
}

func (r *avroRecordReader) Retain() { r.refs.Add(1) }

func (r *avroRecordReader) Release() {
	if r.refs.Add(-1) == 0 && r.current != nil {
		r.current.Release()
		r.current = nil
	}
}

func (r *avroRecordReader) Schema() *arrow.Schema {
	return r.schema
}

func (r *avroRecordReader) Err() error {
	return r.readErr
}

func (r *avroRecordReader) Record() arrow.RecordBatch {
	return r.current
}

func (r *avroRecordReader) RecordBatch() arrow.RecordBatch {
	return r.current
}

const avroRecordsPerBatch = 2048

func (r *avroRecordReader) Next() bool {
	if r.readErr != nil || r.finished {
		return false
	}
	if r.current != nil {
		r.current.Release()
		r.current = nil
	}
	if err := r.ctx.Err(); err != nil {
		r.readErr = err
		return false
	}

	builder := array.NewRecordBuilder(compute.GetAllocator(r.ctx), r.schema)
	defer builder.Release()
	var count int
	for count < avroRecordsPerBatch {
		if err := r.ctx.Err(); err != nil {
			r.readErr = err
			return false
		}
		var row map[string]any
		if err := r.records.Decode(&row); err != nil {
			if errors.Is(err, io.EOF) {
				r.finished = true

				break
			}
			r.readErr = fmt.Errorf("decode Avro record: %w", err)
			return false
		}
		if err := r.ctx.Err(); err != nil {
			r.readErr = err
			return false
		}
		for i, field := range r.schema.Fields() {
			value, present := row[field.Name]
			if (!present || value == nil) && !field.Nullable {
				r.readErr = fmt.Errorf("%w: Avro field %q is required", iceberg.ErrInvalidSchema, field.Name)
				return false
			}
			if err := appendAvroValue(builder.Field(i), value); err != nil {
				r.readErr = fmt.Errorf("avro field %q: %w", field.Name, err)
				return false
			}
		}
		count++
	}
	if count == 0 {
		return false
	}
	r.current = builder.NewRecordBatch()
	return true
}

func (r *avroFileReader) ReadTable(ctx context.Context) (arrow.Table, error) {
	records, err := r.GetRecords(ctx, nil, nil)
	if err != nil {
		return nil, err
	}
	defer records.Release()
	var batches []arrow.RecordBatch
	defer func() {
		for _, batch := range batches {
			batch.Release()
		}
	}()
	for records.Next() {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		batch := records.RecordBatch()
		batch.Retain()
		batches = append(batches, batch)
	}
	if err := records.Err(); err != nil {
		return nil, err
	}

	return array.NewTableFromRecords(records.Schema(), batches), nil
}

func (r *avroFileReader) Close() error {
	if r.closed {
		return nil
	}
	r.closed = true
	return errors.Join(r.records.Close(), r.file.Close())
}

func addAvroFieldID(field arrow.Field, props map[string]any, property string, used map[int]struct{}) (arrow.Field, error) {
	raw, ok := props[property]
	if !ok {
		return field, nil // Name mapping can supply IDs for legacy files.
	}
	var id int64
	switch value := raw.(type) {
	case int:
		id = int64(value)
	case int64:
		id = value
	case float64:
		if math.Trunc(value) != value || value > math.MaxInt32-200 || value < 1 {
			return field, fmt.Errorf("%w: invalid Avro %s %v", iceberg.ErrInvalidSchema, property, raw)
		}
		id = int64(value)
	default:

		return field, fmt.Errorf("%w: Avro %s must be an integer", iceberg.ErrInvalidSchema, property)
	}
	if id < 1 || id > math.MaxInt32-200 {
		return field, fmt.Errorf("%w: invalid Avro %s %d", iceberg.ErrInvalidSchema, property, id)
	}
	if _, exists := used[int(id)]; exists {
		return field, fmt.Errorf("%w: duplicate Avro field ID %d", iceberg.ErrInvalidSchema, id)
	}
	used[int(id)] = struct{}{}
	meta := field.Metadata.ToMap()
	meta["PARQUET:field_id"] = strconv.FormatInt(id, 10)
	field.Metadata = arrow.MetadataFrom(meta)

	return field, nil
}
