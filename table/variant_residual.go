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
	varIdx := fieldIndexByID(rec.Schema(), col.Term.Ref().Field().ID)
	if varIdx < 0 {
		// Arrow field may lack PARQUET:field_id metadata on name-mapping reads; fall back to the column name.
		if idxs := rec.Schema().FieldIndices(varName); len(idxs) == 1 {
			varIdx = idxs[0]
		}
	}
	if varIdx < 0 {
		return nil, arrow.Field{}, fmt.Errorf("%w: variant extract column %q not found in file", iceberg.ErrInvalidArgument, varName)
	}
	varr, ok := rec.Column(varIdx).(*extensions.VariantArray)
	if !ok {
		return nil, arrow.Field{}, fmt.Errorf("%w: variant extract column %q is not a VariantArray (got %T)", iceberg.ErrInvalidArgument, varName, rec.Column(varIdx))
	}

	for i := 0; i < n; i++ {
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

// appendExtractLiteral appends a decoded extract literal to its typed builder.
func appendExtractLiteral(bldr array.Builder, lit iceberg.Literal) error {
	switch b := bldr.(type) {
	case *array.BooleanBuilder:
		b.Append(lit.Any().(bool))
	case *array.Int32Builder:
		b.Append(lit.Any().(int32))
	case *array.Int64Builder:
		b.Append(lit.Any().(int64))
	case *array.Float32Builder:
		b.Append(lit.Any().(float32))
	case *array.Float64Builder:
		b.Append(lit.Any().(float64))
	case *array.StringBuilder:
		b.Append(lit.Any().(string))
	case *array.BinaryBuilder:
		b.Append(lit.Any().([]byte))
	case *array.FixedSizeBinaryBuilder:
		b.Append(lit.Any().([]byte))
	case *extensions.UUIDBuilder:
		b.Append(lit.Any().(uuid.UUID))
	case *array.Date32Builder:
		b.Append(arrow.Date32(lit.Any().(iceberg.Date)))
	case *array.Time64Builder:
		b.Append(arrow.Time64(lit.Any().(iceberg.Time)))
	case *array.TimestampBuilder:
		switch v := lit.Any().(type) {
		case iceberg.Timestamp:
			b.Append(arrow.Timestamp(v))
		case iceberg.TimestampNano:
			b.Append(arrow.Timestamp(v))
		default:
			return fmt.Errorf("%w: variant extract timestamp value %T", iceberg.ErrNotImplemented, v)
		}
	case *array.Decimal128Builder:
		b.Append(lit.Any().(iceberg.Decimal).Val)
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
