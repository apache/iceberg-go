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
	"bytes"
	"context"
	"fmt"
	"math"
	"slices"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/iceberg-go"
)

// Values have a common schema, so null tags and length prefixes make the row
// encoding unambiguous. No JSON/string fallback or unchecked hash is used.
// NaN payloads compare equal, while signed zeros remain distinct, as in Java.
func encodeChangelogValue(ctx context.Context, key *bytes.Buffer, column arrow.Array, row int, limit int64) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	check := func(n int64) error {
		if n > limit-int64(key.Len()) {
			return fmt.Errorf("%w: encoded row exceeds MaxBufferedBytes", ErrChangelogBufferLimit)
		}

		return nil
	}
	if err := check(1); err != nil {
		return err
	}
	if extension, ok := column.(array.ExtensionArray); ok {
		return encodeChangelogValue(ctx, key, extension.Storage(), row, limit)
	}
	if column.IsNull(row) {
		key.WriteByte(0)

		return nil
	}
	switch col := column.(type) {
	case *array.Dictionary:
		return encodeChangelogValue(ctx, key, col.Dictionary(), col.GetValueIndex(row), limit)
	case *array.Struct:
		key.WriteByte(1)
		for i := range col.NumField() {
			if err := encodeChangelogValue(ctx, key, col.Field(i), row, limit); err != nil {
				return err
			}
		}
	case *array.Map:
		start, end := col.ValueOffsets(row)
		// Reserve entry length prefixes before allocating temporary map keys.
		if err := check(9 + 8*(end-start)); err != nil {
			return err
		}
		key.WriteByte(1)
		bufPutUint64(key, uint64(end-start))
		remaining := limit - int64(key.Len()) - 8*(end-start)
		entries := make([]string, 0, end-start)
		for i := start; i < end; i++ {
			var entry bytes.Buffer
			if err := encodeChangelogValue(ctx, &entry, col.Keys(), int(i), remaining); err != nil {
				return err
			}
			if err := encodeChangelogValue(ctx, &entry, col.Items(), int(i), remaining); err != nil {
				return err
			}
			remaining -= int64(entry.Len())
			entries = append(entries, entry.String())
		}
		// Map entry order does not change the logical row.
		slices.Sort(entries)
		if err := ctx.Err(); err != nil {
			return err
		}
		for _, entry := range entries {
			bufPutUint64(key, uint64(len(entry)))
			key.WriteString(entry)
		}
	case array.ListLike:
		if err := check(9); err != nil {
			return err
		}
		key.WriteByte(1)
		start, end := col.ValueOffsets(row)
		bufPutUint64(key, uint64(end-start))
		for i := start; i < end; i++ {
			if err := encodeChangelogValue(ctx, key, col.ListValues(), int(i), limit); err != nil {
				return err
			}
		}
	case *array.Float32:
		if err := check(5); err != nil {
			return err
		}
		key.WriteByte(1)
		bits := math.Float32bits(col.Value(row))
		if math.IsNaN(float64(col.Value(row))) {
			bits = 0x7fc00000
		}
		bufPutUint32(key, bits)
	case *array.Float64:
		if err := check(9); err != nil {
			return err
		}
		key.WriteByte(1)
		bits := math.Float64bits(col.Value(row))
		if math.IsNaN(col.Value(row)) {
			bits = 0x7ff8000000000000
		}
		bufPutUint64(key, bits)
	case *array.Decimal128:
		if err := check(17); err != nil {
			return err
		}
		key.WriteByte(1)
		value := col.Value(row)
		bufPutUint64(key, uint64(value.HighBits()))
		bufPutUint64(key, value.LowBits())
	case *array.Decimal256:
		if err := check(33); err != nil {
			return err
		}
		key.WriteByte(1)
		for _, part := range col.Value(row).Array() {
			bufPutUint64(key, part)
		}
	case interface{ Value(int) string }:
		value := col.Value(row)
		if err := check(9 + int64(len(value))); err != nil {
			return err
		}
		key.WriteByte(1)
		bufPutUint64(key, uint64(len(value)))
		key.WriteString(value)
	case interface{ Value(int) []byte }:
		value := col.Value(row)
		if err := check(9 + int64(len(value))); err != nil {
			return err
		}
		key.WriteByte(1)
		bufPutUint64(key, uint64(len(value)))
		key.Write(value)
	case *array.Boolean:
		if err := check(2); err != nil {
			return err
		}
		encodeArrowValue(key, column, row)
	case *array.Int8, *array.Int16, *array.Int32, *array.Int64,
		*array.Date32, *array.Date64, *array.Time32, *array.Time64, *array.Timestamp:
		if err := check(1 + int64(column.DataType().(arrow.FixedWidthDataType).BitWidth()/8)); err != nil {
			return err
		}
		encodeArrowValue(key, column, row)
	default:
		return fmt.Errorf("%w: changelog row comparison for Arrow type %s", iceberg.ErrNotImplemented, column.DataType())
	}

	return nil
}
