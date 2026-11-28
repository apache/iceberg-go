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
	"fmt"
	"math"
	"sync/atomic"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/bitutil"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/iceberg-go"
)

// normalizeTimestampMillisType replaces any Arrow timestamp[ms] type (including
// nested struct, list, map, and dictionary children) with timestamp[us] while
// preserving time zones, nullability, and field metadata. It returns (dt, false)
// without allocating when no millisecond timestamp is present.
func normalizeTimestampMillisType(dt arrow.DataType) (arrow.DataType, bool) {
	switch t := dt.(type) {
	case *arrow.TimestampType:
		if t.Unit != arrow.Millisecond {
			return dt, false
		}

		return &arrow.TimestampType{Unit: arrow.Microsecond, TimeZone: t.TimeZone}, true

	case *arrow.StructType:
		var fields []arrow.Field
		for i := range t.NumFields() {
			f := t.Field(i)
			newT, changed := normalizeTimestampMillisType(f.Type)
			if !changed {
				continue
			}
			if fields == nil {
				fields = make([]arrow.Field, t.NumFields())
				copy(fields, t.Fields())
			}
			fields[i].Type = newT
		}
		if fields == nil {
			return dt, false
		}

		return arrow.StructOf(fields...), true

	case *arrow.ListType:
		elem := t.ElemField()
		newT, changed := normalizeTimestampMillisType(elem.Type)
		if !changed {
			return dt, false
		}
		elem.Type = newT

		return arrow.ListOfField(elem), true

	case *arrow.LargeListType:
		elem := t.ElemField()
		newT, changed := normalizeTimestampMillisType(elem.Type)
		if !changed {
			return dt, false
		}
		elem.Type = newT

		return arrow.LargeListOfField(elem), true

	case *arrow.FixedSizeListType:
		elem := t.ElemField()
		newT, changed := normalizeTimestampMillisType(elem.Type)
		if !changed {
			return dt, false
		}
		elem.Type = newT

		return arrow.FixedSizeListOfField(t.Len(), elem), true

	case *arrow.ListViewType:
		elem := t.ElemField()
		newT, changed := normalizeTimestampMillisType(elem.Type)
		if !changed {
			return dt, false
		}
		elem.Type = newT

		return arrow.ListViewOfField(elem), true

	case *arrow.LargeListViewType:
		elem := t.ElemField()
		newT, changed := normalizeTimestampMillisType(elem.Type)
		if !changed {
			return dt, false
		}
		elem.Type = newT

		return arrow.LargeListViewOfField(elem), true

	case *arrow.MapType:
		keyField := t.KeyField()
		itemField := t.ItemField()
		newKey, keyChanged := normalizeTimestampMillisType(keyField.Type)
		newItem, itemChanged := normalizeTimestampMillisType(itemField.Type)
		if !keyChanged && !itemChanged {
			return dt, false
		}
		keyField.Type = newKey
		itemField.Type = newItem
		out := arrow.MapOfFields(keyField, itemField)
		out.KeysSorted = t.KeysSorted

		return out, true

	case *arrow.DictionaryType:
		newVal, changed := normalizeTimestampMillisType(t.ValueType)
		if !changed {
			return dt, false
		}

		return &arrow.DictionaryType{
			IndexType: t.IndexType,
			ValueType: newVal,
			Ordered:   t.Ordered,
		}, true

	default:
		// Parquet-to-Arrow and Iceberg schema mapping do not produce union,
		// run-end-encoded, or extension types wrapping timestamp[ms].
		return dt, false
	}
}

func normalizeTimestampMillisSchema(sc *arrow.Schema) (*arrow.Schema, bool) {
	if sc == nil {
		return nil, false
	}

	var fields []arrow.Field
	for i := range sc.NumFields() {
		f := sc.Field(i)
		newT, changed := normalizeTimestampMillisType(f.Type)
		if !changed {
			continue
		}
		if fields == nil {
			fields = make([]arrow.Field, sc.NumFields())
			copy(fields, sc.Fields())
		}
		fields[i].Type = newT
	}
	if fields == nil {
		return sc, false
	}

	meta := sc.Metadata()

	return arrow.NewSchema(fields, &meta), true
}

func normalizeTimestampMillisLeaf(
	alloc memory.Allocator, a *array.Timestamp, targetType *arrow.TimestampType,
) (arrow.Array, error) {
	const (
		unitsPerMilli = 1000
		maxMillis     = math.MaxInt64 / unitsPerMilli
		minMillis     = math.MinInt64 / unitsPerMilli
	)

	n := a.Len()
	if n == 0 {
		newData := array.NewData(targetType, 0, []*memory.Buffer{nil, nil}, nil, 0, 0)
		defer newData.Release()

		return array.NewTimestampData(newData), nil
	}

	outBuf := memory.NewResizableBuffer(alloc)
	defer outBuf.Release()
	outBuf.Resize(arrow.TimestampTraits.BytesRequired(n))
	dst := arrow.TimestampTraits.CastFromBytes(outBuf.Bytes())

	var nulls int
	if a.Data().Buffers()[0] != nil {
		nulls = a.NullN()
	}

	if nulls < n {
		src := a.TimestampValues()
		_ = dst[n-1]
		for i, v := range src {
			if v > maxMillis || v < minMillis {
				if nulls > 0 && a.IsNull(i) {
					dst[i] = 0

					continue
				}

				return nil, fmt.Errorf("%w: Parquet TIMESTAMP(MILLIS) value %d overflows int64 microseconds",
					iceberg.ErrInvalidArgument, v)
			}
			dst[i] = v * unitsPerMilli
		}
	}

	var nullBuf *memory.Buffer
	if nulls > 0 {
		srcNullBuf := a.Data().Buffers()[0]
		offset := a.Data().Offset()
		switch {
		case offset == 0:
			nullBuf = srcNullBuf
		case offset%8 == 0:
			sliced := memory.SliceBuffer(srcNullBuf, offset/8, int(bitutil.BytesForBits(int64(n))))
			defer sliced.Release()
			nullBuf = sliced
		default:
			copied := memory.NewResizableBuffer(alloc)
			defer copied.Release()
			copied.Resize(int(bitutil.BytesForBits(int64(n))))
			bitutil.CopyBitmap(srcNullBuf.Bytes(), offset, n, copied.Bytes(), 0)
			nullBuf = copied
		}
	}

	newData := array.NewData(targetType, n, []*memory.Buffer{nullBuf, outBuf}, nil, nulls, 0)
	defer newData.Release()

	return array.NewTimestampData(newData), nil
}

func normalizeTimestampMillisArray(alloc memory.Allocator, arr arrow.Array) (arrow.Array, error) {
	newType, changed := normalizeTimestampMillisType(arr.DataType())
	if !changed {
		arr.Retain()

		return arr, nil
	}

	switch a := arr.(type) {
	case *array.Timestamp:
		return normalizeTimestampMillisLeaf(alloc, a, newType.(*arrow.TimestampType))

	case *array.Dictionary:
		newDict, err := normalizeTimestampMillisArray(alloc, a.Dictionary())
		if err != nil {
			return nil, err
		}
		defer newDict.Release()

		data := a.Data()
		newData := array.NewDataWithDictionary(
			newType.(*arrow.DictionaryType),
			data.Len(),
			data.Buffers(),
			data.NullN(),
			data.Offset(),
			newDict.Data().(*array.Data),
		)
		defer newData.Release()

		return array.MakeFromData(newData), nil

	default:
		data := a.Data()
		srcChildren := data.Children()
		newChildren := make([]arrow.Array, len(srcChildren))
		childData := make([]arrow.ArrayData, len(srcChildren))
		releaseChildren := func(n int) {
			for i := range n {
				if newChildren[i] != nil {
					newChildren[i].Release()
				}
			}
		}

		for i, cd := range srcChildren {
			srcChild := array.MakeFromData(cd)
			normChild, err := normalizeTimestampMillisArray(alloc, srcChild)
			srcChild.Release()
			if err != nil {
				releaseChildren(i)

				return nil, err
			}
			newChildren[i] = normChild
			childData[i] = normChild.Data()
		}
		defer releaseChildren(len(newChildren))

		newData := array.NewData(newType, data.Len(), data.Buffers(), childData, data.NullN(), data.Offset())
		defer newData.Release()

		return array.MakeFromData(newData), nil
	}
}

func normalizeTimestampMillisRecord(alloc memory.Allocator, schema *arrow.Schema, rec arrow.RecordBatch) (arrow.RecordBatch, error) {
	cols := make([]arrow.Array, rec.NumCols())
	releaseCols := func(n int) {
		for i := range n {
			if cols[i] != nil {
				cols[i].Release()
			}
		}
	}

	for i := range int(rec.NumCols()) {
		col, err := normalizeTimestampMillisArray(alloc, rec.Column(i))
		if err != nil {
			releaseCols(i)

			return nil, err
		}
		cols[i] = col
	}
	defer releaseCols(len(cols))

	return array.NewRecordBatch(schema, cols, rec.NumRows()), nil
}

type timestampMillisRecordReader struct {
	inner   array.RecordReader
	schema  *arrow.Schema
	alloc   memory.Allocator
	current arrow.RecordBatch
	err     error
	refs    atomic.Int64
}

func newTimestampMillisRecordReader(
	inner array.RecordReader,
	schema *arrow.Schema,
	alloc memory.Allocator,
) *timestampMillisRecordReader {
	r := &timestampMillisRecordReader{
		inner:  inner,
		schema: schema,
		alloc:  alloc,
	}
	r.refs.Store(1)

	return r
}

func (r *timestampMillisRecordReader) Retain() {
	r.refs.Add(1)
}

func (r *timestampMillisRecordReader) Release() {
	if r.refs.Add(-1) == 0 {
		if r.current != nil {
			r.current.Release()
			r.current = nil
		}
		if r.inner != nil {
			r.inner.Release()
			r.inner = nil
		}
	}
}

func (r *timestampMillisRecordReader) Schema() *arrow.Schema {
	return r.schema
}

func (r *timestampMillisRecordReader) Record() arrow.RecordBatch {
	return r.current
}

func (r *timestampMillisRecordReader) RecordBatch() arrow.RecordBatch {
	return r.current
}

func (r *timestampMillisRecordReader) Err() error {
	if r.err != nil {
		return r.err
	}
	if r.inner != nil {
		return r.inner.Err()
	}

	return nil
}

func (r *timestampMillisRecordReader) Next() bool {
	if r.err != nil {
		return false
	}
	if r.current != nil {
		r.current.Release()
		r.current = nil
	}
	if !r.inner.Next() {
		return false
	}

	converted, err := normalizeTimestampMillisRecord(r.alloc, r.schema, r.inner.RecordBatch())
	if err != nil {
		r.err = err

		return false
	}
	r.current = converted

	return true
}
