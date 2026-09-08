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
	"log/slog"
	"strconv"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/compute"
	"github.com/apache/arrow-go/v18/arrow/extensions"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/iceberg-go"
	"github.com/google/uuid"
)

// augmentSchemaWithExtracts returns fileSchema plus one primitive column per variant extract term.
func augmentSchemaWithExtracts(fileSchema *iceberg.Schema, cols []iceberg.VariantExtractColumn) *iceberg.Schema {
	fields := fileSchema.Fields()
	for _, c := range cols {
		fields = append(fields, iceberg.NestedField{
			ID:   c.FieldID,
			Name: c.Name,
			Type: c.Term.Type().(iceberg.PrimitiveType),
		})
	}

	return iceberg.NewSchemaWithIdentifiers(fileSchema.ID, fileSchema.IdentifierFieldIDs, fields...)
}

// buildExtractColumn materializes one variant extract term into a typed Arrow array over rec.
func buildExtractColumn(col iceberg.VariantExtractColumn, rec arrow.RecordBatch, mem memory.Allocator) (arrow.Array, arrow.Field, error) {
	typ := col.Term.Type().(iceberg.PrimitiveType)
	dt, err := TypeToArrowType(typ, false, false)
	if err != nil {
		return nil, arrow.Field{}, err
	}

	bldr := array.NewBuilder(mem, dt)
	defer bldr.Release()

	n := int(rec.NumRows())
	varName := col.Term.Ref().Field().Name
	arr := resolveVariantSource(rec, col.Term.Ref().Field().ID, col.SourcePath)
	if arr == nil {
		return nil, arrow.Field{}, fmt.Errorf("%w: variant extract column %q not found in file", iceberg.ErrInvalidArgument, varName)
	}
	varr, ok := arr.(*extensions.VariantArray)
	if !ok {
		return nil, arrow.Field{}, fmt.Errorf("%w: variant extract column %q is not a VariantArray (got %T)", iceberg.ErrInvalidArgument, varName, arr)
	}

	for i := range n {
		if varr.IsNull(i) {
			bldr.AppendNull()

			continue
		}

		v, verr := varr.Value(i)
		if verr != nil {
			slog.Warn("variant extract: skipping undecodable variant value", "column", varName, "row", i, "err", verr)
			bldr.AppendNull()

			continue
		}

		lit, ok := col.Term.ExtractValue(v)
		if !ok {
			bldr.AppendNull()

			continue
		}

		if aerr := appendExtractLiteral(bldr, lit); aerr != nil {
			return nil, arrow.Field{}, aerr
		}
	}

	field := arrow.Field{
		Name:     col.Name,
		Type:     dt,
		Nullable: true,
		Metadata: arrow.NewMetadata([]string{ArrowParquetFieldIDKey}, []string{strconv.Itoa(col.FieldID)}),
	}

	return bldr.NewArray(), field, nil
}

// resolveVariantSource locates the extract's source array by top-level field id
// (rename-proof), falling back to the file-schema path segments when ids are absent.
func resolveVariantSource(rec arrow.RecordBatch, fieldID int, sourcePath []string) arrow.Array {
	for i, f := range rec.Schema().Fields() {
		if v, ok := f.Metadata.GetValue(ArrowParquetFieldIDKey); ok {
			if id, err := strconv.Atoi(v); err == nil && id == fieldID {
				return rec.Column(i)
			}
		}
	}
	if len(sourcePath) == 0 {
		return nil
	}

	return descendByPath(rec.Schema(), rec.Columns(), sourcePath)
}

func descendByPath(schema *arrow.Schema, cols []arrow.Array, path []string) arrow.Array {
	var (
		col   arrow.Array
		ftype arrow.DataType
	)
	found := false
	for i, f := range schema.Fields() {
		if f.Name == path[0] {
			col, ftype, found = cols[i], f.Type, true

			break
		}
	}
	if !found {
		return nil
	}
	for _, seg := range path[1:] {
		st, ok := col.(*array.Struct)
		if !ok {
			return nil
		}
		stype, ok := ftype.(*arrow.StructType)
		if !ok {
			return nil
		}
		idx, ok := stype.FieldIdx(seg)
		if !ok {
			return nil
		}
		col, ftype = st.Field(idx), stype.Field(idx).Type
	}

	return col
}

// appendExtractLiteral appends a decoded extract literal to its typed builder, erroring rather than panicking on a type mismatch.
func appendExtractLiteral(bldr array.Builder, lit iceberg.Literal) error {
	v := lit.Any()
	wrongType := func() error {
		return fmt.Errorf("%w: variant extract value %T does not match builder %T", iceberg.ErrNotImplemented, v, bldr)
	}
	switch b := bldr.(type) {
	case *array.BooleanBuilder:
		x, ok := v.(bool)
		if !ok {
			return wrongType()
		}
		b.Append(x)
	case *array.Int32Builder:
		x, ok := v.(int32)
		if !ok {
			return wrongType()
		}
		b.Append(x)
	case *array.Int64Builder:
		x, ok := v.(int64)
		if !ok {
			return wrongType()
		}
		b.Append(x)
	case *array.Float32Builder:
		x, ok := v.(float32)
		if !ok {
			return wrongType()
		}
		b.Append(x)
	case *array.Float64Builder:
		x, ok := v.(float64)
		if !ok {
			return wrongType()
		}
		b.Append(x)
	case *array.StringBuilder:
		x, ok := v.(string)
		if !ok {
			return wrongType()
		}
		b.Append(x)
	case *array.BinaryBuilder:
		x, ok := v.([]byte)
		if !ok {
			return wrongType()
		}
		b.Append(x)
	case *array.FixedSizeBinaryBuilder:
		x, ok := v.([]byte)
		if !ok {
			return wrongType()
		}
		b.Append(x)
	case *extensions.UUIDBuilder:
		x, ok := v.(uuid.UUID)
		if !ok {
			return wrongType()
		}
		b.Append(x)
	case *array.Date32Builder:
		x, ok := v.(iceberg.Date)
		if !ok {
			return wrongType()
		}
		b.Append(arrow.Date32(x))
	case *array.Time64Builder:
		x, ok := v.(iceberg.Time)
		if !ok {
			return wrongType()
		}
		b.Append(arrow.Time64(x))
	case *array.TimestampBuilder:
		switch x := v.(type) {
		case iceberg.Timestamp:
			b.Append(arrow.Timestamp(x))
		case iceberg.TimestampNano:
			b.Append(arrow.Timestamp(x))
		default:
			return wrongType()
		}
	case *array.Decimal128Builder:
		x, ok := v.(iceberg.Decimal)
		if !ok {
			return wrongType()
		}
		b.Append(x.Val)
	default:
		return fmt.Errorf("%w: variant extract target builder %T", iceberg.ErrNotImplemented, bldr)
	}

	return nil
}

// extractResidualFilter appends derived extract columns to each batch, runs base, then strips them.
func (as *arrowScan) extractResidualFilter(ctx context.Context, cols []iceberg.VariantExtractColumn, base recProcessFn) recProcessFn {
	mem := compute.GetAllocator(ctx)

	return func(rec arrow.RecordBatch) (arrow.RecordBatch, error) {
		origSchema := rec.Schema()
		origN := int(rec.NumCols())

		derived := make([]arrow.Array, 0, len(cols))
		fields := make([]arrow.Field, 0, len(cols))
		for _, c := range cols {
			arr, field, err := buildExtractColumn(c, rec, mem)
			if err != nil {
				for _, a := range derived {
					a.Release()
				}
				rec.Release()

				return nil, err
			}
			derived = append(derived, arr)
			fields = append(fields, field)
		}

		augFields := append(append([]arrow.Field{}, origSchema.Fields()...), fields...)
		md := origSchema.Metadata()
		augSchema := arrow.NewSchema(augFields, &md)
		augRec := array.NewRecordBatch(augSchema, append(rec.Columns(), derived...), rec.NumRows())
		rec.Release()
		for _, a := range derived {
			a.Release()
		}

		filtered, err := base(augRec)
		if err != nil {
			return nil, err
		}

		out := array.NewRecordBatch(origSchema, filtered.Columns()[:origN], filtered.NumRows())
		filtered.Release()

		return out, nil
	}
}
