// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package internal

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"strconv"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/iceberg-go"
	iceio "github.com/apache/iceberg-go/io"
	"github.com/twmb/avro"
	"github.com/twmb/avro/ocf"
)

// avroFormat writes Avro Object Container Files using stable Iceberg field IDs.
// Reading stays in the independent Avro scanner change.
type avroFormat struct{}

type (
	avroWriteProps   struct{ codec string }
	avroWriteSummary struct {
		rows   int64
		nulls  map[int]int64
		values map[int]int64
	}
)

func (avroFormat) Open(context.Context, iceio.IO, string) (FileReader, error) {
	return nil, fmt.Errorf("%w: Avro reading requires the Avro scanner", iceberg.ErrNotImplemented)
}

func (avroFormat) PathToIDMapping(sc *iceberg.Schema) (map[string]int, error) {
	if sc == nil {
		return nil, fmt.Errorf("%w: missing Avro schema", iceberg.ErrInvalidArgument)
	}
	mapping := make(map[string]int, sc.NumFields())
	for _, f := range sc.Fields() {
		mapping[f.Name] = f.ID
	}

	return mapping, nil
}

func (avroFormat) DataFileStatsFromMeta(meta Metadata, _ map[int]StatisticsCollector, _ map[string]int, _ map[int]struct{}, _ *arrow.Schema) *DataFileStatistics {
	s := meta.(*avroWriteSummary)

	return &DataFileStatistics{RecordCount: s.rows, NullValueCounts: s.nulls, ValueCounts: s.values}
}

func (avroFormat) GetWriteProperties(props iceberg.Properties) any {
	return avroWriteProps{codec: props.Get("write.avro.compression-codec", "snappy")}
}

func (f avroFormat) WriteDataFile(ctx context.Context, fs iceio.WriteFileIO, partitions map[int]any, info WriteFileInfo, batches []arrow.RecordBatch) (iceberg.DataFile, error) {
	if len(batches) == 0 {
		return nil, fmt.Errorf("%w: no Avro input batches", iceberg.ErrInvalidArgument)
	}
	w, err := f.NewFileWriter(ctx, fs, partitions, info, batches[0].Schema())
	if err != nil {
		return nil, err
	}

	return writeDataFileBatches(w, batches)
}

func (avroFormat) NewFileWriter(ctx context.Context, fs iceio.WriteFileIO, partitions map[int]any, info WriteFileInfo, sc *arrow.Schema) (FileWriter, error) {
	if info.FileSchema == nil || sc == nil {
		return nil, fmt.Errorf("%w: missing Avro schema", iceberg.ErrInvalidArgument)
	}
	if info.Content != iceberg.EntryContentData {
		return nil, fmt.Errorf("%w: Avro delete-file writing is not implemented", iceberg.ErrNotImplemented)
	}
	fields := info.FileSchema.Fields()
	if len(fields) != len(sc.Fields()) {
		return nil, fmt.Errorf("%w: Avro file field count differs from Arrow", iceberg.ErrInvalidArgument)
	}
	for i, f := range fields {
		if sc.Field(i).Name != f.Name {
			return nil, fmt.Errorf("%w: Avro field %d name mismatch: %q != %q", iceberg.ErrInvalidArgument, i, sc.Field(i).Name, f.Name)
		}
	}
	root, err := avroRecordSchema(fields, "iceberg_record")
	if err != nil {
		return nil, err
	}
	raw, err := json.Marshal(root)
	if err != nil {
		return nil, err
	}
	parsed, err := avro.Parse(string(raw))
	if err != nil {
		return nil, fmt.Errorf("avro schema: %w", err)
	}
	var props avroWriteProps
	if info.WriteProps != nil {
		var ok bool
		props, ok = info.WriteProps.(avroWriteProps)
		if !ok {
			return nil, fmt.Errorf("%w: wrong Avro write properties %T", iceberg.ErrInvalidArgument, info.WriteProps)
		}
	}
	var opts []ocf.WriterOpt
	switch props.codec {
	case "", "null":
	case "snappy":
		opts = append(opts, ocf.WithCodec(ocf.SnappyCodec()))
	case "deflate":
		opts = append(opts, ocf.WithCodec(ocf.DeflateCodec(-1)))
	case "zstd":
		opts = append(opts, ocf.WithCodec(ocf.MustZstdCodec(nil, nil)))
	default:
		return nil, fmt.Errorf("%w: unsupported Avro codec %q", iceberg.ErrInvalidArgument, props.codec)
	}
	file, err := fs.Create(ctx, info.FileName)
	if err != nil {
		return nil, err
	}
	counter := &avroCountingWriter{w: file}
	ocfWriter, err := ocf.NewWriter(counter, parsed, opts...)
	if err != nil {
		_ = file.Close()

		return nil, fmt.Errorf("create Avro OCF: %w", err)
	}

	return &avroFileWriter{writer: ocfWriter, file: file, counter: counter, fs: fs, info: info, partitions: partitions, fields: fields, nulls: make(map[int]int64), values: make(map[int]int64)}, nil
}

type avroCountingWriter struct {
	w     io.Writer
	count int64
}

func (w *avroCountingWriter) Write(p []byte) (int, error) {
	n, e := w.w.Write(p)
	w.count += int64(n)

	return n, e
}

type avroFileWriter struct {
	writer     *ocf.Writer
	file       io.Closer
	counter    *avroCountingWriter
	fs         iceio.WriteFileIO
	info       WriteFileInfo
	partitions map[int]any
	fields     []iceberg.NestedField
	nulls      map[int]int64
	values     map[int]int64
	rows       int64
	closed     bool
}

func (w *avroFileWriter) BytesWritten() int64 { return w.counter.count }
func (w *avroFileWriter) Write(batch arrow.RecordBatch) error {
	if w.closed {
		return errors.New("avro writer is closed")
	}
	if int(batch.NumCols()) != len(w.fields) {
		return fmt.Errorf("%w: Avro batch has wrong number of columns", iceberg.ErrInvalidArgument)
	}
	for i, f := range w.fields {
		if batch.Schema().Field(i).Name != f.Name {
			return fmt.Errorf("%w: Avro batch schema changed at %s", iceberg.ErrInvalidArgument, f.Name)
		}
	}
	for row := range batch.NumRows() {
		record := make(map[string]any, len(w.fields))
		for i, f := range w.fields {
			value, err := avroValue(batch.Column(i), int(row), f.Type)
			if err != nil {
				return fmt.Errorf("avro field %q, row %d: %w", f.Name, row, err)
			}
			if value == nil && f.Required {
				return fmt.Errorf("%w: null required Avro field %q", iceberg.ErrInvalidArgument, f.Name)
			}
			record[f.Name] = value
		}
		if err := w.writer.Encode(record); err != nil {
			return fmt.Errorf("encode Avro row %d: %w", row, err)
		}
	}
	for i, f := range w.fields {
		w.values[f.ID] += batch.NumRows()
		w.nulls[f.ID] += int64(batch.Column(i).NullN())
	}
	w.rows += batch.NumRows()

	return nil
}

func (w *avroFileWriter) Close() (_ iceberg.DataFile, err error) {
	if w.closed {
		return nil, errors.New("avro writer already closed")
	}
	w.closed = true
	if err = w.writer.Close(); err != nil {
		_ = w.file.Close()

		return nil, err
	}
	if err = w.file.Close(); err != nil {
		return nil, err
	}
	stats := &DataFileStatistics{RecordCount: w.rows, ValueCounts: w.values, NullValueCounts: w.nulls}

	return stats.ToDataFile(DataFileOpts{
		Schema: w.info.FileSchema, Spec: w.info.Spec, Path: w.info.FileName,
		Format: iceberg.AvroFile, Content: w.info.Content, FileSize: w.counter.count,
		PartitionValues: w.partitions, SortOrderID: w.info.SortOrderID,
	}), nil
}

func (w *avroFileWriter) Abort() error {
	if w.closed {
		return nil
	}
	w.closed = true
	closeErr := w.file.Close()
	removeErr := w.fs.Remove(context.Background(), w.info.FileName)
	if errors.Is(removeErr, fs.ErrNotExist) || os.IsNotExist(removeErr) {
		removeErr = nil
	}

	return errors.Join(closeErr, removeErr)
}

// avroRecordSchema retains field IDs even when fields are renamed later.
func avroRecordSchema(fields []iceberg.NestedField, name string) (any, error) {
	out := make([]any, 0, len(fields))
	for _, f := range fields {
		typ, err := icebergAvroType(f.Type, f.ID)
		if err != nil {
			return nil, fmt.Errorf("%s: %w", f.Name, err)
		}
		field := map[string]any{"name": f.Name, "type": typ, "field-id": f.ID}
		if !f.Required {
			field["type"] = []any{"null", typ}
			field["default"] = nil
		}
		out = append(out, field)
	}

	return map[string]any{"type": "record", "name": name, "fields": out}, nil
}

func icebergAvroType(t iceberg.Type, id int) (any, error) {
	switch typ := t.(type) {
	case iceberg.BooleanType:
		return "boolean", nil
	case iceberg.Int32Type:
		return "int", nil
	case iceberg.Int64Type:
		return "long", nil
	case iceberg.Float32Type:
		return "float", nil
	case iceberg.Float64Type:
		return "double", nil
	case iceberg.StringType:
		return "string", nil
	case iceberg.BinaryType:
		return "bytes", nil
	case iceberg.DateType:
		return map[string]any{"type": "int", "logicalType": "date"}, nil
	case iceberg.TimeType:
		return map[string]any{"type": "long", "logicalType": "time-micros"}, nil
	case iceberg.TimestampType:
		return map[string]any{"type": "long", "logicalType": "timestamp-micros", "adjust-to-utc": false}, nil
	case iceberg.TimestampTzType:
		return map[string]any{"type": "long", "logicalType": "timestamp-micros", "adjust-to-utc": true}, nil
	case iceberg.FixedType:
		return map[string]any{"type": "fixed", "name": "fixed_" + strconv.Itoa(id), "size": typ.Len()}, nil
	case iceberg.UUIDType:
		return nil, fmt.Errorf("%w: Avro UUID writing", iceberg.ErrNotImplemented)
	case iceberg.DecimalType:
		return nil, fmt.Errorf("%w: Avro decimal writing", iceberg.ErrNotImplemented)
	case *iceberg.StructType:
		return avroRecordSchema(typ.Fields(), "r_"+strconv.Itoa(id))
	case *iceberg.ListType:
		elem, err := icebergAvroType(typ.Element, typ.ElementID)
		if err != nil {
			return nil, err
		}
		if !typ.ElementRequired {
			elem = []any{"null", elem}
		}

		return map[string]any{"type": "array", "items": elem, "element-id": typ.ElementID}, nil
	case *iceberg.MapType:
		value, err := icebergAvroType(typ.ValueType, typ.ValueID)
		if err != nil {
			return nil, err
		}
		if !typ.ValueRequired {
			value = []any{"null", value}
		}
		if _, ok := typ.KeyType.(iceberg.StringType); ok {
			return map[string]any{"type": "map", "values": value, "key-id": typ.KeyID, "value-id": typ.ValueID}, nil
		}
		key, err := icebergAvroType(typ.KeyType, typ.KeyID)
		if err != nil {
			return nil, err
		}

		return map[string]any{"type": "array", "logicalType": "map", "items": map[string]any{
			"type": "record", "name": "k" + strconv.Itoa(typ.KeyID) + "_v" + strconv.Itoa(typ.ValueID),
			"fields": []any{
				map[string]any{"name": "key", "type": key, "field-id": typ.KeyID},
				map[string]any{"name": "value", "type": value, "field-id": typ.ValueID},
			},
		}}, nil
	default:
		return nil, fmt.Errorf("%w: Avro encoding for Iceberg type %s", iceberg.ErrNotImplemented, t)
	}
}

// avroValue follows the Iceberg field layout, rather than Arrow JSON
// representations (which convert binary values to base64 and dates to text).
func avroValue(col arrow.Array, row int, typ iceberg.Type) (any, error) {
	if col.IsNull(row) {
		return nil, nil
	}
	switch t := typ.(type) {
	case *iceberg.StructType:
		a, ok := col.(*array.Struct)
		if !ok {
			return nil, fmt.Errorf("expected struct, got %T", col)
		}
		fields := t.Fields()
		if len(fields) != a.NumField() {
			return nil, errors.New("struct field count mismatch")
		}
		out := make(map[string]any, len(fields))
		for i, f := range fields {
			v, err := avroValue(a.Field(i), row, f.Type)
			if err != nil {
				return nil, err
			}
			if v == nil && f.Required {
				return nil, fmt.Errorf("null required struct field %q", f.Name)
			}
			out[f.Name] = v
		}

		return out, nil
	case *iceberg.ListType:
		a, ok := col.(array.ListLike)
		if !ok {
			return nil, fmt.Errorf("expected list, got %T", col)
		}
		start, end := a.ValueOffsets(row)
		vals := a.ListValues()
		out := make([]any, 0, int(end-start))
		for i := start; i < end; i++ {
			v, err := avroValue(vals, int(i), t.Element)
			if err != nil {
				return nil, err
			}
			if v == nil && t.ElementRequired {
				return nil, errors.New("null required list element")
			}
			out = append(out, v)
		}

		return out, nil
	case *iceberg.MapType:
		a, ok := col.(*array.Map)
		if !ok {
			return nil, fmt.Errorf("expected map, got %T", col)
		}
		start, end := a.ValueOffsets(row)
		if _, ok := t.KeyType.(iceberg.StringType); ok {
			out := make(map[string]any, int(end-start))
			for i := start; i < end; i++ {
				key, err := avroValue(a.Keys(), int(i), t.KeyType)
				if err != nil {
					return nil, err
				}
				value, err := avroValue(a.Items(), int(i), t.ValueType)
				if err != nil {
					return nil, err
				}
				if value == nil && t.ValueRequired {
					return nil, errors.New("null required map value")
				}
				out[key.(string)] = value
			}

			return out, nil
		}
		out := make([]any, 0, int(end-start))
		for i := start; i < end; i++ {
			key, err := avroValue(a.Keys(), int(i), t.KeyType)
			if err != nil {
				return nil, err
			}
			value, err := avroValue(a.Items(), int(i), t.ValueType)
			if err != nil {
				return nil, err
			}
			if value == nil && t.ValueRequired {
				return nil, errors.New("null required map value")
			}
			out = append(out, map[string]any{"key": key, "value": value})
		}

		return out, nil
	}
	switch a := col.(type) {
	case *array.Boolean:
		return a.Value(row), nil
	case *array.Int32:
		return a.Value(row), nil
	case *array.Int64:
		return a.Value(row), nil
	case *array.Float32:
		return a.Value(row), nil
	case *array.Float64:
		return a.Value(row), nil
	case *array.String:
		return a.Value(row), nil
	case *array.LargeString:
		return a.Value(row), nil
	case *array.Binary:
		return a.Value(row), nil
	case *array.LargeBinary:
		return a.Value(row), nil
	case *array.FixedSizeBinary:
		return a.Value(row), nil
	case *array.Date32:
		return int32(a.Value(row)), nil
	case *array.Time64:
		return int64(a.Value(row)), nil
	case *array.Timestamp:
		return int64(a.Value(row)), nil
	default:
		return nil, fmt.Errorf("%w: Avro Arrow array %T", iceberg.ErrNotImplemented, col)
	}
}
