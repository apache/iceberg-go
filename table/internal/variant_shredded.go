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
	"sync/atomic"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/extensions"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/arrow-go/v18/parquet/variant"
)

// ReassembleShreddedVariant returns the variant.Value reconstructed
// from one row of a shredded Parquet variant column, following the
// Parquet Variant Shredding spec:
//
//   - typed_value wins when present at any level,
//   - the residual value supplies fields that typed_value omits,
//   - object fields are merged element-wise.
//
// metadata is the variant metadata bytes for the row. value is the
// residual variant value bytes (may be empty/nil if the whole value
// is shredded). typedValue is the typed_value column of the
// shredded variant — its data type must already satisfy the
// shredding-spec invariants that arrow-go's
// extensions.NewVariantType enforces. row is the index into
// typedValue to reconstruct.
//
// The actual walk is delegated to arrow-go's *extensions.VariantArray
// reader, which implements the spec. This wrapper exists so the
// shredded-vs-non-shredded distinction has a single named entry
// point.
func ReassembleShreddedVariant(metadata, value []byte, typedValue arrow.Array, row int) (variant.Value, error) {
	if typedValue == nil {
		// Pure unshredded path — no typed_value, just (metadata, value).
		return variant.New(metadata, value)
	}
	if row < 0 || row >= typedValue.Len() {
		return variant.NullValue, fmt.Errorf("row %d out of range for typedValue of length %d", row, typedValue.Len())
	}

	mem := memory.DefaultAllocator

	metaArr := singleRowBinary(mem, metadata, false)
	defer metaArr.Release()

	valueArr := singleRowBinary(mem, value, true)
	defer valueArr.Release()

	typedSlice := array.NewSlice(typedValue, int64(row), int64(row+1))
	defer typedSlice.Release()

	structFields := []arrow.Field{
		{Name: "metadata", Type: arrow.BinaryTypes.Binary, Nullable: false},
		{Name: "value", Type: arrow.BinaryTypes.Binary, Nullable: true},
		{Name: "typed_value", Type: typedValue.DataType(), Nullable: true},
	}
	structArr, err := array.NewStructArrayWithFields(
		[]arrow.Array{metaArr, valueArr, typedSlice},
		structFields,
	)
	if err != nil {
		return variant.NullValue, fmt.Errorf("building shredded variant struct: %w", err)
	}
	defer structArr.Release()

	vt, err := extensions.NewVariantType(arrow.StructOf(structFields...))
	if err != nil {
		return variant.NullValue, fmt.Errorf("constructing shredded VariantType: %w", err)
	}

	variantArr := array.NewExtensionArrayWithStorage(vt, structArr).(*extensions.VariantArray)
	defer variantArr.Release()

	return variantArr.Value(0)
}

// ReassembleShreddedVariantColumn rewrites a *extensions.VariantArray
// that uses the 3-field shredded storage into a non-shredded
// *extensions.VariantArray whose per-row .Value(i) is byte-equivalent
// to the input's reassembled variant.
//
// Used from the Parquet read path so a shredded column is invisible
// to the scanner: downstream callers see the same
// struct<metadata, value> layout regardless of how the Parquet file
// was written. If arr is already non-shredded the function returns
// arr unchanged (with a Retain) so the caller can keep using its
// Release pattern uniformly.
func ReassembleShreddedVariantColumn(arr *extensions.VariantArray, mem memory.Allocator) (*extensions.VariantArray, error) {
	if !arr.IsShredded() {
		arr.Retain()
		return arr, nil
	}
	if mem == nil {
		mem = memory.DefaultAllocator
	}

	bldr := extensions.NewVariantBuilder(mem, extensions.NewDefaultVariantType())
	defer bldr.Release()
	bldr.Reserve(arr.Len())

	for i := 0; i < arr.Len(); i++ {
		if arr.IsNull(i) {
			bldr.AppendNull()

			continue
		}
		v, err := arr.Value(i)
		if err != nil {
			return nil, fmt.Errorf("reassembling shredded variant row %d: %w", i, err)
		}
		bldr.Append(v)
	}

	out := bldr.NewArray().(*extensions.VariantArray)

	return out, nil
}

// singleRowBinary returns a length-1 binary array carrying b. When
// nullable is true and b is empty, the row is null instead of an
// empty byte slice; this matches the Parquet shredded layout where
// the value column is nullable and "no residual" is encoded as null,
// not as a zero-length payload.
func singleRowBinary(mem memory.Allocator, b []byte, nullable bool) arrow.Array {
	bldr := array.NewBinaryBuilder(mem, arrow.BinaryTypes.Binary)
	defer bldr.Release()
	if nullable && len(b) == 0 {
		bldr.AppendNull()
	} else {
		bldr.Append(b)
	}

	return bldr.NewArray()
}

// reassemblingRecordReader wraps an array.RecordReader so that any
// shredded variant column in each record is replaced with its
// reassembled non-shredded equivalent before being handed to the
// caller. Downstream code sees a uniform struct<metadata, value>
// layout regardless of how the underlying Parquet file encoded the
// column.
//
// If the inner reader's schema has no shredded variant columns the
// wrapper passes records through unchanged.
type reassemblingRecordReader struct {
	inner       array.RecordReader
	schema      *arrow.Schema   // schema with shredded variant types rewritten to non-shredded
	variantCols []int           // top-level column indices that need post-processing
	cur         arrow.RecordBatch
	refCount    atomic.Int64
}

// WrapShreddedVariantReader returns a RecordReader that transparently
// reassembles shredded variant columns produced by the inner reader.
// The returned reader's Schema reports the rewritten (non-shredded)
// variant types so the rest of the pipeline sees the same shape
// regardless of physical encoding. When the inner schema has no
// shredded variant columns, the returned reader is a thin pass-
// through that shares the inner schema and never copies records.
func WrapShreddedVariantReader(inner array.RecordReader, mem memory.Allocator) array.RecordReader {
	if mem == nil {
		mem = memory.DefaultAllocator
	}

	srcFields := inner.Schema().Fields()
	dstFields := make([]arrow.Field, len(srcFields))
	var variantCols []int
	for i, f := range srcFields {
		ext, ok := f.Type.(arrow.ExtensionType)
		if !ok || ext.ExtensionName() != "parquet.variant" {
			dstFields[i] = f

			continue
		}
		vt, isVT := ext.(*extensions.VariantType)
		if !isVT || vt.TypedValue().Type == nil {
			// Non-shredded variant; nothing to rewrite.
			dstFields[i] = f

			continue
		}

		// Replace the shredded VariantType with the default 2-field one.
		rewritten := f
		rewritten.Type = extensions.NewDefaultVariantType()
		dstFields[i] = rewritten
		variantCols = append(variantCols, i)
	}

	if len(variantCols) == 0 {
		return inner
	}

	srcMeta := inner.Schema().Metadata()
	rdr := &reassemblingRecordReader{
		inner:       inner,
		schema:      arrow.NewSchema(dstFields, &srcMeta),
		variantCols: variantCols,
	}
	rdr.refCount.Store(1)
	inner.Retain()

	return rdr
}

func (r *reassemblingRecordReader) Retain() {
	r.refCount.Add(1)
}

func (r *reassemblingRecordReader) Release() {
	if r.refCount.Add(-1) == 0 {
		if r.cur != nil {
			r.cur.Release()
			r.cur = nil
		}
		r.inner.Release()
	}
}

func (r *reassemblingRecordReader) Schema() *arrow.Schema { return r.schema }

func (r *reassemblingRecordReader) Err() error { return r.inner.Err() }

func (r *reassemblingRecordReader) Next() bool {
	if r.cur != nil {
		r.cur.Release()
		r.cur = nil
	}
	if !r.inner.Next() {
		return false
	}
	r.cur = r.materialize(r.inner.RecordBatch())

	return true
}

func (r *reassemblingRecordReader) RecordBatch() arrow.RecordBatch { return r.cur }
func (r *reassemblingRecordReader) Record() arrow.RecordBatch      { return r.cur }

// materialize rebuilds rec with shredded variant columns reassembled
// in place. The new RecordBatch carries the wrapper's rewritten
// schema. On reassembly failure the row is left untouched and the
// error surfaces via Err() on the next iteration boundary.
func (r *reassemblingRecordReader) materialize(rec arrow.RecordBatch) arrow.RecordBatch {
	rec.Retain()
	numCols := int(rec.NumCols())
	cols := make([]arrow.Array, numCols)
	for i := 0; i < numCols; i++ {
		cols[i] = rec.Column(i)
		cols[i].Retain()
	}
	rec.Release()

	for _, idx := range r.variantCols {
		vArr, ok := cols[idx].(*extensions.VariantArray)
		if !ok || !vArr.IsShredded() {
			continue
		}
		reassembled, err := ReassembleShreddedVariantColumn(vArr, memory.DefaultAllocator)
		if err != nil {
			// Best-effort: keep the original column and let downstream
			// .Value(i) calls surface the underlying error per row.
			continue
		}
		cols[idx].Release()
		cols[idx] = reassembled
	}

	out := array.NewRecord(r.schema, cols, rec.NumRows())
	for _, c := range cols {
		c.Release()
	}

	return out
}
