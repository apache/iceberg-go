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
	"math/big"
	"os"
	"strconv"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/extensions"
	"github.com/apache/iceberg-go"
	iceberginternal "github.com/apache/iceberg-go/internal"
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
	if len(batches) == 0 || batches[0] == nil {
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
	if fs == nil {
		return nil, fmt.Errorf("%w: missing Avro file IO", iceberg.ErrInvalidArgument)
	}
	fields := info.FileSchema.Fields()
	if len(fields) != len(sc.Fields()) {
		return nil, fmt.Errorf("%w: Avro file field count differs from Arrow", iceberg.ErrInvalidArgument)
	}
	for i, f := range fields {
		if sc.Field(i).Name != f.Name {
			return nil, fmt.Errorf("%w: Avro field %d name mismatch: %q != %q", iceberg.ErrInvalidArgument, i, sc.Field(i).Name, f.Name)
		}
		if err := validateAvroArrowType(f.Type, sc.Field(i).Type); err != nil {
			return nil, fmt.Errorf("avro field %q: %w", f.Name, err)
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
		closeErr := file.Close()
		removeErr := fs.Remove(context.Background(), info.FileName)

		return nil, errors.Join(fmt.Errorf("create Avro OCF: %w", err), closeErr, removeErr)
	}

	return &avroFileWriter{writer: ocfWriter, file: file, counter: counter, fs: fs, info: info, partitions: partitions, fields: fields, schema: sc, nulls: make(map[int]int64), values: make(map[int]int64)}, nil
}

// validateAvroArrowType rejects incompatible Arrow physical types before
// writing an OCF header. Without this check, timestamp units and nested field
// order can silently change Iceberg values.
func validateAvroArrowType(t iceberg.Type, dt arrow.DataType) error {
	var valid bool
	switch typ := t.(type) {
	case iceberg.BooleanType:
		valid = dt.ID() == arrow.BOOL
	case iceberg.Int32Type:
		valid = dt.ID() == arrow.INT32
	case iceberg.Int64Type:
		valid = dt.ID() == arrow.INT64
	case iceberg.Float32Type:
		valid = dt.ID() == arrow.FLOAT32
	case iceberg.Float64Type:
		valid = dt.ID() == arrow.FLOAT64
	case iceberg.StringType:
		valid = dt.ID() == arrow.STRING || dt.ID() == arrow.LARGE_STRING
	case iceberg.BinaryType:
		valid = dt.ID() == arrow.BINARY || dt.ID() == arrow.LARGE_BINARY
	case iceberg.FixedType:
		fixed, ok := dt.(*arrow.FixedSizeBinaryType)
		valid = ok && fixed.ByteWidth == typ.Len()
	case iceberg.UUIDType:
		valid = arrow.TypeEqual(dt, extensions.NewUUIDType())
	case iceberg.DecimalType:
		decimal, ok := dt.(*arrow.Decimal128Type)
		valid = ok && int(decimal.Precision) == typ.Precision() && int(decimal.Scale) == typ.Scale()
	case iceberg.DateType:
		valid = dt.ID() == arrow.DATE32
	case iceberg.TimeType:
		valid = arrow.TypeEqual(dt, arrow.FixedWidthTypes.Time64us)
	case iceberg.TimestampType:
		ts, ok := dt.(*arrow.TimestampType)
		valid = ok && ts.Unit == arrow.Microsecond && ts.TimeZone == ""
	case iceberg.TimestampNsType:
		ts, ok := dt.(*arrow.TimestampType)
		valid = ok && ts.Unit == arrow.Nanosecond && ts.TimeZone == ""
	case iceberg.TimestampTzNsType:
		ts, ok := dt.(*arrow.TimestampType)
		if ok && ts.Unit == arrow.Nanosecond {
			switch ts.TimeZone {
			case "UTC", "Etc/UTC", "+00:00", "Z":
				valid = true
			}
		}
	case iceberg.TimestampTzType:
		ts, ok := dt.(*arrow.TimestampType)
		if ok && ts.Unit == arrow.Microsecond {
			switch ts.TimeZone {
			case "UTC", "Etc/UTC", "+00:00", "Z":
				valid = true
			}
		}
	case *iceberg.StructType:
		st, ok := dt.(*arrow.StructType)
		if !ok || st.NumFields() != len(typ.Fields()) {
			break
		}
		for i, field := range typ.Fields() {
			if st.Field(i).Name != field.Name {
				return fmt.Errorf("%w: Avro struct field %q was reordered", iceberg.ErrInvalidArgument, field.Name)
			}
			if err := validateAvroArrowType(field.Type, st.Field(i).Type); err != nil {
				return fmt.Errorf("struct field %q: %w", field.Name, err)
			}
		}
		valid = true
	case *iceberg.ListType:
		switch dt.(type) {
		case *arrow.ListType, *arrow.LargeListType, *arrow.FixedSizeListType:
			if err := validateAvroArrowType(typ.Element, dt.(arrow.ListLikeType).Elem()); err != nil {
				return fmt.Errorf("list element: %w", err)
			}
			valid = true
		}
	case *iceberg.MapType:
		mt, ok := dt.(*arrow.MapType)
		if !ok {
			break
		}
		if err := validateAvroArrowType(typ.KeyType, mt.KeyType()); err != nil {
			return fmt.Errorf("map key: %w", err)
		}
		if err := validateAvroArrowType(typ.ValueType, mt.ItemType()); err != nil {
			return fmt.Errorf("map value: %w", err)
		}
		valid = true
	default:
		return fmt.Errorf("%w: Avro writing for %s", iceberg.ErrNotImplemented, t)
	}
	if !valid {
		return fmt.Errorf("%w: Iceberg %s cannot be written from Arrow %s", iceberg.ErrInvalidArgument, t, dt)
	}

	return nil
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
	schema     *arrow.Schema
	nulls      map[int]int64
	values     map[int]int64
	rows       int64
	failed     error
	closed     bool
}

func (w *avroFileWriter) BytesWritten() int64 { return w.counter.count }
func (w *avroFileWriter) Write(batch arrow.RecordBatch) (err error) {
	if w.closed {
		return errors.New("avro writer is closed")
	}
	if w.failed != nil {
		return w.failed
	}
	// An OCF stream cannot roll back records written before a batch error.
	// Prevent Close from publishing mismatched record-count metadata.
	defer func() {
		if err != nil {
			w.failed = err
		}
	}()
	if batch == nil {
		return fmt.Errorf("%w: nil Avro record batch", iceberg.ErrInvalidArgument)
	}
	if int(batch.NumCols()) != len(w.fields) {
		return fmt.Errorf("%w: Avro batch has wrong number of columns", iceberg.ErrInvalidArgument)
	}
	if !w.schema.Equal(batch.Schema()) {
		return fmt.Errorf("%w: Avro record batch schema changed", iceberg.ErrInvalidArgument)
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
	if w.failed != nil {
		return nil, errors.Join(w.failed, w.Abort())
	}
	w.closed = true
	if err = w.writer.Close(); err != nil {
		closeErr := w.file.Close()
		removeErr := w.fs.Remove(context.Background(), w.info.FileName)

		return nil, errors.Join(err, closeErr, removeErr)
	}
	if err = w.file.Close(); err != nil {
		removeErr := w.fs.Remove(context.Background(), w.info.FileName)

		return nil, errors.Join(err, removeErr)
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
	case iceberg.TimestampNsType:
		return map[string]any{"type": "long", "logicalType": "timestamp-nanos", "adjust-to-utc": false}, nil
	case iceberg.TimestampTzNsType:
		return map[string]any{"type": "long", "logicalType": "timestamp-nanos", "adjust-to-utc": true}, nil
	case iceberg.FixedType:
		return map[string]any{"type": "fixed", "name": "fixed_" + strconv.Itoa(id), "size": typ.Len()}, nil
	case iceberg.UUIDType:
		return map[string]any{"type": "fixed", "name": "uuid_" + strconv.Itoa(id), "size": 16, "logicalType": "uuid"}, nil
	case iceberg.DecimalType:
		return map[string]any{
			"type": "fixed", "name": "decimal_" + strconv.Itoa(id),
			"size":        iceberginternal.DecimalRequiredBytes(typ.Precision()),
			"logicalType": "decimal", "precision": typ.Precision(), "scale": typ.Scale(),
		}, nil
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
	case iceberg.UUIDType:
		ext, ok := col.(array.ExtensionArray)
		if !ok {
			return nil, fmt.Errorf("%w: Avro UUID requires an Arrow extension array, got %T", iceberg.ErrInvalidArgument, col)
		}
		storage, ok := ext.Storage().(*array.FixedSizeBinary)
		if !ok || len(storage.Value(row)) != 16 {
			return nil, fmt.Errorf("%w: Avro UUID requires 16-byte storage", iceberg.ErrInvalidArgument)
		}

		return append([]byte(nil), storage.Value(row)...), nil
	case iceberg.DecimalType:
		dec, ok := col.(*array.Decimal128)
		if !ok {
			return nil, fmt.Errorf("%w: Avro decimal requires Arrow decimal128, got %T", iceberg.ErrInvalidArgument, col)
		}
		value := dec.Value(row)
		if !value.FitsInPrecision(int32(t.Precision())) {
			return nil, fmt.Errorf("%w: Avro decimal exceeds precision %d", iceberg.ErrInvalidArgument, t.Precision())
		}

		return avroSignedFixed(value.BigInt(), iceberginternal.DecimalRequiredBytes(t.Precision()))
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
				keyString, ok := key.(string)
				if !ok {
					return nil, fmt.Errorf("%w: Avro map key must be a non-null string", iceberg.ErrInvalidArgument)
				}
				if _, exists := out[keyString]; exists {
					return nil, fmt.Errorf("%w: duplicate Avro map key %q", iceberg.ErrInvalidArgument, keyString)
				}
				out[keyString] = value
			}

			return out, nil
		}
		out := make([]any, 0, int(end-start))
		for i := start; i < end; i++ {
			key, err := avroValue(a.Keys(), int(i), t.KeyType)
			if err != nil {
				return nil, err
			}
			if key == nil {
				return nil, fmt.Errorf("%w: null Avro map key", iceberg.ErrInvalidArgument)
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
		value := int64(a.Value(row))
		if value < 0 || value >= 86400000000 {
			return nil, fmt.Errorf("%w: Avro time is outside a day", iceberg.ErrInvalidArgument)
		}

		return value, nil
	case *array.Timestamp:
		return int64(a.Value(row)), nil
	default:
		return nil, fmt.Errorf("%w: Avro Arrow array %T", iceberg.ErrNotImplemented, col)
	}
}

// avroSignedFixed encodes the unscaled decimal as a fixed-width, big-endian
// two's-complement integer. Iceberg requires the minimum width for precision.
func avroSignedFixed(value *big.Int, size int) ([]byte, error) {
	if value == nil || size <= 0 {
		return nil, fmt.Errorf("%w: invalid Avro decimal value or width", iceberg.ErrInvalidArgument)
	}
	limit := new(big.Int).Lsh(big.NewInt(1), uint(size*8-1))
	minValue := new(big.Int).Neg(new(big.Int).Set(limit))
	if value.Cmp(minValue) < 0 || value.Cmp(limit) >= 0 {
		return nil, fmt.Errorf("%w: Avro decimal value does not fit %d bytes", iceberg.ErrInvalidArgument, size)
	}
	unscaled := new(big.Int).Set(value)
	if unscaled.Sign() < 0 {
		unscaled.Add(unscaled, new(big.Int).Lsh(big.NewInt(1), uint(size*8)))
	}

	return unscaled.FillBytes(make([]byte, size)), nil
}
