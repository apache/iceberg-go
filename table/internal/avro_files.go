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
	"math"
	"strconv"
	"sync/atomic"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	arrowavro "github.com/apache/arrow-go/v18/arrow/avro"
	"github.com/apache/arrow-go/v18/arrow/compute"
	"github.com/apache/iceberg-go"
	iceio "github.com/apache/iceberg-go/io"
	avro "github.com/twmb/avro"
	"github.com/twmb/avro/ocf"
)

// AvroFileSource reads Iceberg data files stored as Avro object container files.
// The decoder streams Arrow record batches. Column pruning is performed by the
// scanner after decoding, rather than by the OCF decoder. Nullable records and
// containers with nested complex values are rejected because the underlying
// decoder does not safely handle them.
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
	records  *arrowavro.OCFReader
	schema   *arrow.Schema
	fileSize int64
	used     bool
	closed   bool
}

// newAvroFileReader reads the original OCF header separately because Arrow's
// Avro-to-Arrow conversion drops Iceberg's custom field-id properties.
func newAvroFileReader(ctx context.Context, f iceio.File) (*avroFileReader, error) {
	stat, err := f.Stat()
	if err != nil {
		return nil, err
	}
	header, err := ocf.NewReader(f)
	if err != nil {
		return nil, err
	}
	schemaJSON := string(header.Metadata()["avro.schema"])
	if err := header.Close(); err != nil {
		return nil, err
	}
	if err := validateAvroReadSchema(schemaJSON); err != nil {
		return nil, err
	}
	if _, err := f.Seek(0, 0); err != nil {
		return nil, err
	}
	records, err := arrowavro.NewOCFReader(f,
		arrowavro.WithAllocator(compute.GetAllocator(ctx)),
		arrowavro.WithChunk(2048),
		arrowavro.WithReadCacheSize(16),
		arrowavro.WithRecordCacheSize(1))
	if err != nil {
		return nil, err
	}
	schema, err := schemaWithAvroFieldIDs(records.Schema(), schemaJSON)
	if err != nil {
		records.Release()

		return nil, err
	}

	return &avroFileReader{file: f, records: records, schema: schema, fileSize: stat.Size()}, nil
}

func (r *avroFileReader) Metadata() Metadata             { return nil }
func (r *avroFileReader) SourceFileSize() int64          { return r.fileSize }
func (r *avroFileReader) Schema() (*arrow.Schema, error) { return r.schema, nil }

func (r *avroFileReader) PrunedSchema(_ map[int]struct{}, _ iceberg.NameMapping) (*arrow.Schema, []int, error) {
	// Keep the full schema to preserve the IDs needed by residual filters and
	// schema evolution. GetRecords similarly reads every OCF column.

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
	// The FileReader retains its reference until Close. The scanner releases
	// only the additional reference returned here.
	r.records.Retain()
	wrapped := &avroRecordReader{records: r.records, schema: r.schema, ctx: ctx}
	wrapped.refs.Store(1)

	return wrapped, nil
}

// avroRecordReader exposes the Iceberg-aware schema on both the reader and
// its batches. Timestamp timezone annotations only change interpretation;
// the underlying timestamp values and nested buffers remain unchanged.
type avroRecordReader struct {
	records array.RecordReader
	schema  *arrow.Schema
	current arrow.RecordBatch
	refs    atomic.Int64
	ctx     context.Context
	readErr error
}

func (r *avroRecordReader) Retain() { r.refs.Add(1) }
func (r *avroRecordReader) Release() {
	if r.refs.Add(-1) == 0 {
		if r.current != nil {
			r.current.Release()
		}
		r.records.Release()
	}
}
func (r *avroRecordReader) Schema() *arrow.Schema { return r.schema }
func (r *avroRecordReader) Err() error {
	if r.readErr != nil {
		return r.readErr
	}

	return r.records.Err()
}
func (r *avroRecordReader) Record() arrow.RecordBatch      { return r.current }
func (r *avroRecordReader) RecordBatch() arrow.RecordBatch { return r.current }
func (r *avroRecordReader) Next() bool {
	if r.readErr != nil {
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
	if !r.records.Next() {
		// Cancellation can happen while waiting for the decoder.
		if err := r.ctx.Err(); err != nil {
			r.readErr = err
		}

		return false
	}
	if err := r.ctx.Err(); err != nil {
		r.readErr = err

		return false
	}
	batch := r.records.RecordBatch()
	if batch == nil {
		r.readErr = fmt.Errorf("%w: Avro decoder returned a nil record batch", iceberg.ErrInvalidSchema)

		return false
	}
	if batch.NumCols() != int64(r.schema.NumFields()) {
		r.readErr = fmt.Errorf("%w: Avro decoder returned %d columns, expected %d",
			iceberg.ErrInvalidSchema, batch.NumCols(), r.schema.NumFields())

		return false
	}
	columns := make([]arrow.Array, batch.NumCols())
	for i, col := range batch.Columns() {
		data, err := avroRetagData(col.Data(), r.schema.Field(i).Type)
		if err != nil {
			for _, column := range columns[:i] {
				column.Release()
			}
			r.readErr = fmt.Errorf("avro field %q: %w", r.schema.Field(i).Name, err)

			return false
		}
		columns[i] = array.MakeFromData(data)
		data.Release()
	}
	r.current = array.NewRecordBatch(r.schema, columns, batch.NumRows())
	for _, col := range columns {
		col.Release()
	}

	return true
}

func avroRetagData(source arrow.ArrayData, target arrow.DataType) (*array.Data, error) {
	// Retagging shares the original buffers: only physically compatible types
	// can be used. Avro timestamp-nanos may be decoded as an int64 by Arrow.
	sourceType := source.DataType()
	if sourceType.ID() != target.ID() &&
		(sourceType.ID() != arrow.INT64 || target.ID() != arrow.TIMESTAMP) {
		return nil, fmt.Errorf("%w: cannot retag Avro %s as %s", iceberg.ErrInvalidSchema, sourceType, target)
	}
	if from, ok := sourceType.(*arrow.FixedSizeBinaryType); ok {
		if to, ok := target.(*arrow.FixedSizeBinaryType); !ok || from.ByteWidth != to.ByteWidth {
			return nil, fmt.Errorf("%w: Avro fixed-size binary width changed from %s to %s", iceberg.ErrInvalidSchema, sourceType, target)
		}
	}
	if from, ok := sourceType.(*arrow.Decimal128Type); ok {
		if to, ok := target.(*arrow.Decimal128Type); !ok || from.Precision != to.Precision || from.Scale != to.Scale {
			return nil, fmt.Errorf("%w: Avro decimal type changed from %s to %s", iceberg.ErrInvalidSchema, sourceType, target)
		}
	}
	if from, ok := sourceType.(*arrow.TimestampType); ok {
		if to, ok := target.(*arrow.TimestampType); ok && from.Unit != to.Unit {
			return nil, fmt.Errorf("%w: Avro timestamp unit changed from %s to %s", iceberg.ErrInvalidSchema, from.Unit, to.Unit)
		}
	}
	var children []arrow.ArrayData
	if nested, ok := target.(arrow.NestedType); ok {
		fields := nested.Fields()
		sourceChildren := source.Children()
		if len(sourceChildren) != len(fields) {
			return nil, fmt.Errorf("%w: Avro nested %s has %d children, expected %d",
				iceberg.ErrInvalidSchema, target, len(sourceChildren), len(fields))
		}
		children = make([]arrow.ArrayData, 0, len(fields))
		for i, field := range fields {
			child, err := avroRetagData(sourceChildren[i], field.Type)
			if err != nil {
				for _, created := range children {
					created.Release()
				}

				return nil, err
			}
			children = append(children, child)
		}
	}
	data := array.NewData(target, source.Len(), source.Buffers(), children, source.NullN(), source.Offset())
	for _, child := range children {
		child.Release()
	}

	return data, nil
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
	r.records.Release()

	return r.file.Close()
}

// schemaWithAvroFieldIDs restores Iceberg IDs stored in the Avro OCF writer
// schema. The scanner's ArrowSchemaToIceberg uses these IDs, not field names,
// to implement renamed-column reads and projections.
func schemaWithAvroFieldIDs(sc *arrow.Schema, schemaJSON string) (*arrow.Schema, error) {
	parsed, err := avro.Parse(schemaJSON)
	if err != nil {
		return nil, fmt.Errorf("%w: parse Avro writer schema: %v", iceberg.ErrInvalidSchema, err)
	}
	root := parsed.Root().ExpandReferences()
	if root.Type != "record" || len(root.Fields) != sc.NumFields() {
		return nil, fmt.Errorf("%w: Avro root record does not match Arrow schema", iceberg.ErrInvalidSchema)
	}
	fields := make([]arrow.Field, sc.NumFields())
	used := make(map[int]struct{})
	for i, af := range root.Fields {
		source := sc.Field(i)
		if source.Name != af.Name {
			return nil, fmt.Errorf("%w: Avro field %q differs from Arrow field %q", iceberg.ErrInvalidSchema, af.Name, source.Name)
		}
		fields[i], err = decorateAvroField(source, af.Type, af.Props, "field-id", used)
		if err != nil {
			return nil, fmt.Errorf("avro field %q: %w", source.Name, err)
		}
	}
	metadata := sc.Metadata()

	return arrow.NewSchema(fields, &metadata), nil
}

func decorateAvroField(field arrow.Field, node avro.SchemaNode, props map[string]any, idProperty string, used map[int]struct{}) (arrow.Field, error) {
	var err error
	field, err = addAvroFieldID(field, props, idProperty, used)
	if err != nil {
		return field, err
	}
	if node.Type == "union" {
		// Only a nullable ["null", T] union is legal in Iceberg data files.
		var nonNull *avro.SchemaNode
		for i := range node.Branches {
			if node.Branches[i].Type == "null" {
				continue
			}
			if nonNull != nil {
				return field, fmt.Errorf("%w: Avro union has multiple non-null alternatives", iceberg.ErrNotImplemented)
			}
			nonNull = &node.Branches[i]
		}
		if nonNull == nil || len(node.Branches) != 2 {
			return field, fmt.Errorf("%w: Avro union is not nullable with one value type", iceberg.ErrNotImplemented)
		}
		node = *nonNull
	}
	switch node.Type {
	case "record":
		st, ok := field.Type.(*arrow.StructType)
		if !ok || st.NumFields() != len(node.Fields) {
			return field, fmt.Errorf("%w: Avro record and Arrow struct differ", iceberg.ErrInvalidSchema)
		}
		children := make([]arrow.Field, st.NumFields())
		for i, child := range node.Fields {
			if st.Field(i).Name != child.Name {
				return field, fmt.Errorf("%w: Avro struct field %q differs from Arrow field %q", iceberg.ErrInvalidSchema, child.Name, st.Field(i).Name)
			}
			children[i], err = decorateAvroField(st.Field(i), child.Type, child.Props, "field-id", used)
			if err != nil {
				return field, fmt.Errorf("record field %q: %w", child.Name, err)
			}
		}
		field.Type = arrow.StructOf(children...)
	case "array":
		if node.LogicalType == "map" {
			// Arrow's OCF reader returns this Iceberg map encoding as a list,
			// not a map. Silently treating it as a list would lose map values.

			return field, fmt.Errorf("%w: Avro array-backed Iceberg maps", iceberg.ErrNotImplemented)
		}
		lt, ok := field.Type.(*arrow.ListType)
		if !ok || node.Items == nil {
			return field, fmt.Errorf("%w: Avro array and Arrow list differ", iceberg.ErrInvalidSchema)
		}
		item, err := decorateAvroField(lt.ElemField(), *node.Items, node.Props, "element-id", used)
		if err != nil {
			return field, err
		}
		field.Type = arrow.ListOfField(item)
	case "map":
		mt, ok := field.Type.(*arrow.MapType)
		if !ok || node.Values == nil {
			return field, fmt.Errorf("%w: Avro map and Arrow map differ", iceberg.ErrInvalidSchema)
		}
		key, err := addAvroFieldID(mt.KeyField(), node.Props, "key-id", used)
		if err != nil {
			return field, err
		}
		value, err := decorateAvroField(mt.ItemField(), *node.Values, node.Props, "value-id", used)
		if err != nil {
			return field, err
		}
		field.Type = arrow.MapOfFields(key, value)
	case "boolean", "int", "long", "float", "double", "bytes", "string", "fixed", "null":
		if node.LogicalType == "timestamp-micros" || node.LogicalType == "timestamp-nanos" {
			// Iceberg timestamps are Avro long values. Reinterpreting another
			// physical type as a timestamp would corrupt the decoded buffers.
			if node.Type != "long" {
				return field, fmt.Errorf("%w: Avro %s requires long, got %s", iceberg.ErrInvalidSchema, node.LogicalType, node.Type)
			}
			unit := arrow.Microsecond
			if node.LogicalType == "timestamp-nanos" {
				unit = arrow.Nanosecond
			}
			switch original := field.Type.(type) {
			case *arrow.Int64Type:
				// The Arrow decoder can leave Iceberg's timestamp-nanos as int64.
			case *arrow.TimestampType:
				if original.Unit != unit {
					return field, fmt.Errorf("%w: Avro timestamp source has unit %s, expected %s", iceberg.ErrInvalidSchema, original.Unit, unit)
				}
			default:
				return field, fmt.Errorf("%w: Avro timestamp source has type %s", iceberg.ErrInvalidSchema, field.Type)
			}
			tz := ""
			if value, ok := node.Props["adjust-to-utc"]; ok {
				utc, ok := value.(bool)
				if !ok {
					return field, fmt.Errorf("%w: Avro adjust-to-utc must be a boolean", iceberg.ErrInvalidSchema)
				}
				if utc {
					tz = "UTC"
				}
			}
			field.Type = &arrow.TimestampType{Unit: unit, TimeZone: tz}
		}
	default:

		return field, fmt.Errorf("%w: unsupported Avro type %q", iceberg.ErrNotImplemented, node.Type)
	}

	return field, nil
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
