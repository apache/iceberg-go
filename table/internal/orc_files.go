// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied. See the License for the
// specific language governing permissions and limitations
// under the License.

package internal

import (
	"context"
	"errors"
	"fmt"
	"io"
	"math"
	"math/big"
	"strings"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/compute"
	"github.com/apache/arrow-go/v18/arrow/decimal128"
	"github.com/apache/arrow-go/v18/arrow/extensions"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/iceberg-go"
	iceio "github.com/apache/iceberg-go/io"
	"github.com/scritchley/orc"
)

// ORCFileSource opens an Iceberg ORC data file and yields Arrow record batches.
// ORC stripes are decoded lazily with a bounded record batch size.
type ORCFileSource struct {
	fs   iceio.IO
	file iceberg.DataFile
}

func (s *ORCFileSource) GetReader(ctx context.Context) (_ FileReader, err error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	f, err := s.fs.Open(ctx, s.file.FilePath())
	if err != nil {
		return nil, err
	}
	defer func() {
		if err != nil {
			err = errors.Join(err, f.Close())
		}
	}()
	if err = orcValidFooterSignature(f); err != nil {
		return nil, err
	}
	schema, size, err := readORCFileSchema(f)
	if err != nil {
		return nil, err
	}
	for _, field := range schema.Fields() {
		if field.Name == "" || field.Name == "*" || strings.Contains(field.Name, ".") {
			return nil, fmt.Errorf("%w: ORC top-level field name %q", iceberg.ErrNotImplemented, field.Name)
		}
	}
	decoder, err := orc.NewReader(sizedORCFile{ReaderAt: f, size: size})
	if err != nil {
		return nil, fmt.Errorf("open ORC reader: %w", err)
	}

	return &orcFileReader{file: f, decoder: decoder, schema: schema, size: size, allocator: compute.GetAllocator(ctx)}, nil
}

type orcFileReader struct {
	file      iceio.File
	decoder   *orc.Reader
	schema    *arrow.Schema
	size      int64
	allocator memory.Allocator
	used      bool
	closed    bool
}

func (r *orcFileReader) Metadata() Metadata             { return nil }
func (r *orcFileReader) SourceFileSize() int64          { return r.size }
func (r *orcFileReader) Schema() (*arrow.Schema, error) { return r.schema, nil }

// The whole logical schema is returned because current ORC file readers don't
// support Iceberg field-ID-aware physical leaf projection. The scanner projects
// the decoded batches by field ID and applies residual predicates afterward.
func (r *orcFileReader) PrunedSchema(_ map[int]struct{}, _ iceberg.NameMapping) (*arrow.Schema, []int, error) {
	return r.schema, nil, nil
}

func (r *orcFileReader) GetRecords(ctx context.Context, cols []int, tester any) (array.RecordReader, error) {
	if r.closed || r.used {
		return nil, fmt.Errorf("%w: ORC file streams are single-use", iceberg.ErrInvalidArgument)
	}
	if cols != nil {
		return nil, fmt.Errorf("%w: ORC column selection is not supported", iceberg.ErrNotImplemented)
	}
	if tester != nil {
		return nil, fmt.Errorf("%w: ORC does not support Parquet row-group testers", iceberg.ErrInvalidArgument)
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	r.used = true
	names := make([]string, r.schema.NumFields())
	for i := range names {
		names[i] = r.schema.Field(i).Name
	}
	cursor := r.decoder.Select(names...)

	return array.ReaderFromIter(r.schema, func(yield func(arrow.RecordBatch, error) bool) {
		builder := array.NewRecordBuilder(r.allocator, r.schema)
		defer builder.Release()
		n := 0
		for cursor.Stripes() {
			if err := ctx.Err(); err != nil {
				yield(nil, err)

				return
			}
			for cursor.Next() {
				if err := ctx.Err(); err != nil {
					yield(nil, err)

					return
				}
				values := cursor.Row()
				if len(values) != len(names) {
					yield(nil, fmt.Errorf("%w: ORC row contains %d columns, expected %d", iceberg.ErrInvalidSchema, len(values), len(names)))

					return
				}
				for i, value := range values {
					if err := appendORCField(builder.Field(i), r.schema.Field(i), value); err != nil {
						yield(nil, fmt.Errorf("ORC column %q: %w", names[i], err))

						return
					}
				}
				n++
				if n == 2048 {
					if !yield(builder.NewRecordBatch(), nil) {
						return
					}
					n = 0
				}
			}
			if err := cursor.Err(); err != nil {
				yield(nil, err)

				return
			}
		}
		if err := cursor.Err(); err != nil {
			yield(nil, err)
		} else if n != 0 {
			yield(builder.NewRecordBatch(), nil)
		}
	}), nil
}

func (r *orcFileReader) ReadTable(ctx context.Context) (arrow.Table, error) {
	reader, err := r.GetRecords(ctx, nil, nil)
	if err != nil {
		return nil, err
	}
	defer reader.Release()
	var batches []arrow.RecordBatch
	defer func() {
		for _, batch := range batches {
			batch.Release()
		}
	}()
	for reader.Next() {
		batch := reader.RecordBatch()
		batch.Retain()
		batches = append(batches, batch)
	}
	if err := reader.Err(); err != nil {
		return nil, err
	}

	return array.NewTableFromRecords(r.schema, batches), nil
}

func (r *orcFileReader) Close() error {
	if r.closed {
		return nil
	}
	r.closed = true

	return errors.Join(r.decoder.Close(), r.file.Close())
}

func appendORCField(builder array.Builder, field arrow.Field, value any) error {
	if value == nil && !field.Nullable {
		return fmt.Errorf("%w: required ORC field %q is null", iceberg.ErrInvalidSchema, field.Name)
	}

	return appendORCValue(builder, field.Type, value)
}

func appendORCValue(builder array.Builder, dataType arrow.DataType, value any) error {
	if value == nil {
		builder.AppendNull()

		return nil
	}
	badType := func() error {
		return fmt.Errorf("%w: invalid ORC value %T for Arrow %s", iceberg.ErrInvalidSchema, value, dataType)
	}
	switch b := builder.(type) {
	case *array.BooleanBuilder:
		v, ok := value.(bool)
		if !ok {
			return badType()
		}
		b.Append(v)
	case *array.Int32Builder:
		v, ok := orcInt64(value)
		if !ok || v < math.MinInt32 || v > math.MaxInt32 {
			return badType()
		}
		b.Append(int32(v))
	case *array.Int64Builder:
		v, ok := orcInt64(value)
		if !ok {
			return badType()
		}
		b.Append(v)
	case *array.Time64Builder:
		v, ok := orcInt64(value)
		if !ok || v < 0 || v >= 86_400_000_000 {
			return badType()
		}
		b.Append(arrow.Time64(v))
	case *array.Float32Builder:
		switch v := value.(type) {
		case float32:
			b.Append(v)
		case orc.Float:
			b.Append(float32(v))
		default:

			return badType()
		}
	case *array.Float64Builder:
		switch v := value.(type) {
		case float64:
			b.Append(v)
		case orc.Double:
			b.Append(float64(v))
		default:

			return badType()
		}
	case *array.StringBuilder:
		v, ok := value.(string)
		if !ok {
			return badType()
		}
		b.Append(v)
	case *array.BinaryBuilder:
		v, ok := value.([]byte)
		if !ok {
			return badType()
		}
		b.Append(v)
	case *extensions.UUIDBuilder:
		v, ok := value.([]byte)
		if !ok || len(v) != 16 {
			return badType()
		}
		var id [16]byte
		copy(id[:], v)
		b.AppendBytes(id)
	case *array.FixedSizeBinaryBuilder:
		v, ok := value.([]byte)
		if !ok || len(v) != dataType.(*arrow.FixedSizeBinaryType).ByteWidth {
			return badType()
		}
		b.Append(v)
	case *array.Date32Builder:
		var v time.Time
		switch x := value.(type) {
		case orc.Date:
			v = x.Time
		case time.Time:
			v = x
		default:

			return badType()
		}
		seconds := v.UTC().Unix()
		days := seconds / 86400
		if seconds < 0 && seconds%86400 != 0 {
			days--
		}
		if days < math.MinInt32 || days > math.MaxInt32 {
			return badType()
		}
		// Older ORC writers may use a hybrid Julian/Gregorian calendar.
		// The decoder does not expose that calendar or convert its dates.
		if days < -141427 { // 1582-10-15, the Gregorian cutover.
			return fmt.Errorf("%w: historical ORC dates before 1582-10-15", iceberg.ErrNotImplemented)
		}
		b.Append(arrow.Date32(days))
	case *array.TimestampBuilder:
		v, ok := value.(time.Time)
		if !ok {
			return badType()
		}
		ts, ok := dataType.(*arrow.TimestampType)
		if !ok {
			return badType()
		}
		switch ts.Unit {
		case arrow.Microsecond:
			micros := v.UnixMicro()
			// UnixMicro can wrap outside int64. Reconstruct the whole seconds
			// (using floor division) to check the conversion before appending.
			seconds := micros / 1_000_000
			if micros%1_000_000 < 0 {
				seconds--
			}
			if seconds != v.Unix() {
				return badType()
			}
			b.Append(arrow.Timestamp(micros))
		case arrow.Nanosecond:
			// Do not use UnixNano without a range check: it overflows for
			// dates outside the range supported by int64 nanoseconds.
			nanos := new(big.Int).Mul(big.NewInt(v.Unix()), big.NewInt(1_000_000_000))
			nanos.Add(nanos, big.NewInt(int64(v.Nanosecond())))
			if !nanos.IsInt64() {
				return badType()
			}
			b.Append(arrow.Timestamp(nanos.Int64()))
		default:
			return fmt.Errorf("%w: unsupported ORC timestamp unit %v", iceberg.ErrNotImplemented, ts.Unit)
		}
	case *array.Decimal128Builder:
		v, ok := value.(orc.Decimal)
		if !ok || v.Int == nil {
			return badType()
		}
		dec, ok := dataType.(*arrow.Decimal128Type)
		if !ok {
			return badType()
		}
		i := new(big.Int).Set(v.Int)
		diff := int64(dec.Scale) - v.Scale
		if diff > 38 || diff < -38 {
			return badType()
		}
		if diff > 0 {
			i.Mul(i, new(big.Int).Exp(big.NewInt(10), big.NewInt(diff), nil))
		} else if diff < 0 {
			divisor := new(big.Int).Exp(big.NewInt(10), big.NewInt(-diff), nil)
			q, rem := new(big.Int), new(big.Int)
			q.QuoRem(i, divisor, rem)
			if rem.Sign() != 0 {
				return badType()
			}
			i = q
		}
		if i.BitLen() > 127 {
			return badType()
		}
		num := decimal128.FromBigInt(i)
		if !num.FitsInPrecision(dec.Precision) {
			return badType()
		}
		b.Append(num)
	case *array.StructBuilder:
		data, ok := value.(orc.Struct)
		if !ok {
			return badType()
		}
		st, ok := dataType.(*arrow.StructType)
		if !ok {
			return badType()
		}
		b.Append(true)
		for i := range st.NumFields() {
			field := st.Field(i)
			member, exists := data[field.Name]
			if !exists {
				return fmt.Errorf("%w: ORC struct member %q is missing", iceberg.ErrInvalidSchema, field.Name)
			}
			if err := appendORCField(b.FieldBuilder(i), field, member); err != nil {
				return fmt.Errorf("struct member %q: %w", field.Name, err)
			}
		}
	case *array.ListBuilder:
		items, ok := value.([]any)
		if !ok {
			return badType()
		}
		lt, ok := dataType.(*arrow.ListType)
		if !ok {
			return badType()
		}
		b.Append(true)
		for _, item := range items {
			if err := appendORCField(b.ValueBuilder(), lt.ElemField(), item); err != nil {
				return err
			}
		}
	case *array.MapBuilder:
		entries, ok := value.([]orc.MapEntry)
		if !ok {
			return badType()
		}
		mt, ok := dataType.(*arrow.MapType)
		if !ok {
			return badType()
		}
		b.Append(true)
		for _, entry := range entries {
			if entry.Key == nil {
				return fmt.Errorf("%w: ORC map contains a null key", iceberg.ErrInvalidSchema)
			}
			if err := appendORCField(b.KeyBuilder(), mt.KeyField(), entry.Key); err != nil {
				return err
			}
			if err := appendORCField(b.ItemBuilder(), mt.ItemField(), entry.Value); err != nil {
				return err
			}
		}
	default:

		return fmt.Errorf("%w: unsupported ORC Arrow builder %T", iceberg.ErrNotImplemented, builder)
	}

	return nil
}

func orcInt64(v any) (int64, bool) {
	switch n := v.(type) {
	case int:

		return int64(n), true
	case int8:

		return int64(n), true
	case int16:

		return int64(n), true
	case int32:

		return int64(n), true
	case int64:

		return n, true
	case uint8:

		return int64(n), true
	case uint16:

		return int64(n), true
	case uint32:

		return int64(n), true
	case uint64:
		if n <= math.MaxInt64 {
			return int64(n), true
		}
	}

	return 0, false
}

var (
	_ FileSource = (*ORCFileSource)(nil)
	_ FileReader = (*orcFileReader)(nil)
	_ io.Closer  = (*orcFileReader)(nil)
)
