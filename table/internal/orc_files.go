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
package internal

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"math"
	"math/bits"
	"strconv"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/iceberg-go"
	iceio "github.com/apache/iceberg-go/io"
)

// orcFormat implements an initial, uncompressed single-stripe ORC writer.
// Uncompressed ORC output supports primitive and nested columns, timestamps,
// and Iceberg data, positional delete, and equality delete file content.
type orcFormat struct{}

type orcWriteSummary struct {
	rows   int64
	nulls  map[int]int64
	values map[int]int64
}

func (orcFormat) Open(context.Context, iceio.IO, string) (FileReader, error) {
	return nil, fmt.Errorf("%w: ORC reading requires an ORC reader", iceberg.ErrNotImplemented)
}

func (orcFormat) PathToIDMapping(schema *iceberg.Schema) (map[string]int, error) {
	if schema == nil {
		return nil, fmt.Errorf("%w: missing ORC schema", iceberg.ErrInvalidArgument)
	}
	result := make(map[string]int, schema.NumFields())
	var addField func(string, iceberg.NestedField)
	addField = func(prefix string, field iceberg.NestedField) {
		path := prefix + field.Name
		result[path] = field.ID
		if nested, ok := field.Type.(iceberg.NestedType); ok {
			for _, child := range nested.Fields() {
				addField(path+".", child)
			}
		}
	}
	for _, field := range schema.Fields() {
		addField("", field)
	}

	return result, nil
}

func (orcFormat) DataFileStatsFromMeta(meta Metadata, _ map[int]StatisticsCollector, _ map[string]int, _ map[int]struct{}, _ *arrow.Schema) *DataFileStatistics {
	s := meta.(*orcWriteSummary)

	return &DataFileStatistics{RecordCount: s.rows, ValueCounts: s.values, NullValueCounts: s.nulls}
}

func (orcFormat) GetWriteProperties(iceberg.Properties) any { return struct{}{} }

func (f orcFormat) WriteDataFile(ctx context.Context, fs iceio.WriteFileIO, partitionValues map[int]any, info WriteFileInfo, batches []arrow.RecordBatch) (iceberg.DataFile, error) {
	if len(batches) == 0 {
		return nil, fmt.Errorf("%w: no ORC batches", iceberg.ErrInvalidArgument)
	}
	w, err := f.NewFileWriter(ctx, fs, partitionValues, info, batches[0].Schema())
	if err != nil {
		return nil, err
	}

	return writeDataFileBatches(w, batches)
}

type orcColumn struct {
	field          iceberg.NestedField
	kind           uint64
	children       []int
	childNames     []string
	present        []bool
	integers       []uint64
	integerBytes   int64
	bits           []bool
	data           bytes.Buffer
	lengths        []uint64
	lengthBytes    int64
	secondary      []uint64
	secondaryBytes int64
	nulls          int64
}

func (c *orcColumn) addInteger(value uint64) {
	c.integers = append(c.integers, value)
	c.integerBytes += int64((bits.Len64(value|1) + 6) / 7)
}

func (c *orcColumn) addLength(length uint64) {
	c.lengths = append(c.lengths, length)
	c.lengthBytes += int64((bits.Len64(length|1) + 6) / 7)
}

func (c *orcColumn) addSecondary(value uint64) {
	c.secondary = append(c.secondary, value)
	c.secondaryBytes += int64((bits.Len64(value|1) + 6) / 7)
}

func newORCColumn(field iceberg.NestedField, arrowField arrow.Field) (orcColumn, error) {
	c := orcColumn{field: field}
	switch field.Type.(type) {
	case *iceberg.StructType:
		c.kind = 12
		if _, ok := arrowField.Type.(*arrow.StructType); !ok {
			return c, fmt.Errorf("%w: ORC %s expects an Arrow struct", iceberg.ErrInvalidArgument, field.Name)
		}
	case *iceberg.ListType:
		c.kind = 10
		switch arrowField.Type.(type) {
		case *arrow.ListType, *arrow.LargeListType, *arrow.FixedSizeListType:
		default:
			return c, fmt.Errorf("%w: ORC %s expects an Arrow list", iceberg.ErrInvalidArgument, field.Name)
		}
	case *iceberg.MapType:
		c.kind = 11
		if _, ok := arrowField.Type.(*arrow.MapType); !ok {
			return c, fmt.Errorf("%w: ORC %s expects an Arrow map", iceberg.ErrInvalidArgument, field.Name)
		}
	case iceberg.BooleanType:
		c.kind = 0
		if arrowField.Type.ID() != arrow.BOOL {
			return c, fmt.Errorf("ORC %s: expected Arrow bool", field.Name)
		}
	case iceberg.Int32Type:
		c.kind = 3
		if arrowField.Type.ID() != arrow.INT32 {
			return c, fmt.Errorf("ORC %s: expected Arrow int32", field.Name)
		}
	case iceberg.Int64Type:
		c.kind = 4
		if arrowField.Type.ID() != arrow.INT64 {
			return c, fmt.Errorf("ORC %s: expected Arrow int64", field.Name)
		}
	case iceberg.TimeType:
		c.kind = 4
		if !arrow.TypeEqual(arrowField.Type, arrow.FixedWidthTypes.Time64us) {
			return c, fmt.Errorf("ORC %s: expected Arrow time64 in microseconds", field.Name)
		}
	case iceberg.TimestampType, iceberg.TimestampTzType, iceberg.TimestampNsType, iceberg.TimestampTzNsType:
		ts, ok := arrowField.Type.(*arrow.TimestampType)
		if !ok {
			return c, fmt.Errorf("%w: ORC %s expects an Arrow timestamp", iceberg.ErrInvalidArgument, field.Name)
		}
		expectedUnit := arrow.Microsecond
		switch field.Type.(type) {
		case iceberg.TimestampNsType, iceberg.TimestampTzNsType:
			expectedUnit = arrow.Nanosecond
		}
		if ts.Unit != expectedUnit {
			return c, fmt.Errorf("%w: ORC %s timestamp precision differs from Iceberg schema", iceberg.ErrInvalidArgument, field.Name)
		}
		switch field.Type.(type) {
		case iceberg.TimestampTzType, iceberg.TimestampTzNsType:
			if ts.TimeZone != "UTC" {
				return c, fmt.Errorf("%w: ORC %s requires UTC timezone", iceberg.ErrInvalidArgument, field.Name)
			}
			c.kind = 18 // TIMESTAMP_INSTANT
		default:
			if ts.TimeZone != "" {
				return c, fmt.Errorf("%w: ORC %s timestamp must not have a timezone", iceberg.ErrInvalidArgument, field.Name)
			}
			c.kind = 9 // TIMESTAMP
		}
	case iceberg.Float32Type:
		c.kind = 5
		if arrowField.Type.ID() != arrow.FLOAT32 {
			return c, fmt.Errorf("ORC %s: expected Arrow float32", field.Name)
		}
	case iceberg.Float64Type:
		c.kind = 6
		if arrowField.Type.ID() != arrow.FLOAT64 {
			return c, fmt.Errorf("ORC %s: expected Arrow float64", field.Name)
		}
	case iceberg.StringType:
		c.kind = 7
		if arrowField.Type.ID() != arrow.STRING && arrowField.Type.ID() != arrow.LARGE_STRING {
			return c, fmt.Errorf("ORC %s: expected Arrow string", field.Name)
		}
	case iceberg.BinaryType:
		c.kind = 8
		if arrowField.Type.ID() != arrow.BINARY && arrowField.Type.ID() != arrow.LARGE_BINARY {
			return c, fmt.Errorf("ORC %s: expected Arrow binary", field.Name)
		}
	case iceberg.DateType:
		c.kind = 15
		if arrowField.Type.ID() != arrow.DATE32 {
			return c, fmt.Errorf("ORC %s: expected Arrow date32", field.Name)
		}
	default:
		return c, fmt.Errorf("%w: ORC writer cannot encode %s (%s)", iceberg.ErrNotImplemented, field.Name, field.Type)
	}
	if arrowField.Name != field.Name {
		return c, fmt.Errorf("%w: field %d name differs from schema", iceberg.ErrInvalidArgument, field.ID)
	}

	return c, nil
}

// appendORCColumn assigns ORC type IDs in the format's depth-first order.
func appendORCColumn(columns *[]orcColumn, field iceberg.NestedField, af arrow.Field) (int, error) {
	c, err := newORCColumn(field, af)
	if err != nil {
		return 0, err
	}
	index := len(*columns)
	*columns = append(*columns, c)

	appendChild := func(child iceberg.NestedField, childArrow arrow.Field) error {
		childArrow.Name = child.Name
		id, err := appendORCColumn(columns, child, childArrow)
		if err != nil {
			return err
		}
		(*columns)[index].children = append((*columns)[index].children, id)

		return nil
	}

	switch typ := field.Type.(type) {
	case *iceberg.StructType:
		arrowType := af.Type.(*arrow.StructType)
		fields := typ.Fields()
		if len(fields) != arrowType.NumFields() {
			return 0, fmt.Errorf("%w: ORC %s struct field counts differ", iceberg.ErrInvalidArgument, field.Name)
		}
		for i, child := range fields {
			if arrowType.Field(i).Name != child.Name {
				return 0, fmt.Errorf("%w: ORC %s struct field %d differs", iceberg.ErrInvalidArgument, field.Name, i)
			}
			if err := appendChild(child, arrowType.Field(i)); err != nil {
				return 0, err
			}
			(*columns)[index].childNames = append((*columns)[index].childNames, child.Name)
		}
	case *iceberg.ListType:
		arrowType := af.Type.(arrow.ListLikeType)
		if err := appendChild(typ.ElementField(), arrowType.ElemField()); err != nil {
			return 0, err
		}
	case *iceberg.MapType:
		arrowType := af.Type.(*arrow.MapType)
		if err := appendChild(typ.KeyField(), arrowType.KeyField()); err != nil {
			return 0, err
		}
		if err := appendChild(typ.ValueField(), arrowType.ItemField()); err != nil {
			return 0, err
		}
	}

	return index, nil
}

type orcFileWriter struct {
	ctx             context.Context
	fs              iceio.WriteFileIO
	info            WriteFileInfo
	partitionValues map[int]any
	columns         []orcColumn
	rootIDs         []int
	schema          *arrow.Schema
	lastPath        string
	lastPosition    int64
	hasLastPosition bool
	firstPath       string
	rows            int64
	size            int64
	failed          error
	closed          bool
}

func (orcFormat) NewFileWriter(ctx context.Context, fs iceio.WriteFileIO, partitionValues map[int]any, info WriteFileInfo, schema *arrow.Schema) (FileWriter, error) {
	if info.FileSchema == nil || schema == nil {
		return nil, fmt.Errorf("%w: missing ORC schema", iceberg.ErrInvalidArgument)
	}
	switch info.Content {
	case iceberg.EntryContentData:
		if len(info.EqualityFieldIDs) != 0 {
			return nil, fmt.Errorf("%w: data files cannot have equality field IDs", iceberg.ErrInvalidArgument)
		}
	case iceberg.EntryContentEqDeletes:
		if len(info.EqualityFieldIDs) == 0 {
			return nil, fmt.Errorf("%w: ORC equality deletes require equality field IDs", iceberg.ErrInvalidArgument)
		}
		seen := make(map[int]struct{}, len(info.EqualityFieldIDs))
		for _, id := range info.EqualityFieldIDs {
			field, ok := info.FileSchema.FindFieldByID(id)
			if !ok {
				return nil, fmt.Errorf("%w: ORC equality field %d missing from schema", iceberg.ErrInvalidArgument, id)
			}
			if _, ok := field.Type.(iceberg.PrimitiveType); !ok {
				return nil, fmt.Errorf("%w: ORC equality field %d must be primitive", iceberg.ErrInvalidArgument, id)
			}
			if _, ok := seen[id]; ok {
				return nil, fmt.Errorf("%w: duplicate ORC equality field %d", iceberg.ErrInvalidArgument, id)
			}
			seen[id] = struct{}{}
		}
	case iceberg.EntryContentPosDeletes:
		if len(info.EqualityFieldIDs) != 0 || info.SortOrderID != 0 {
			return nil, fmt.Errorf("%w: ORC position deletes cannot set equality IDs or sort order", iceberg.ErrInvalidArgument)
		}
		want := iceberg.PositionalDeleteSchema.Fields()
		got := info.FileSchema.Fields()
		if len(got) != len(want) {
			return nil, fmt.Errorf("%w: ORC position delete schema is invalid", iceberg.ErrInvalidArgument)
		}
		for i := range want {
			if got[i].ID != want[i].ID || got[i].Name != want[i].Name || got[i].Required != want[i].Required || !got[i].Type.Equals(want[i].Type) {
				return nil, fmt.Errorf("%w: ORC position delete field %d differs from schema", iceberg.ErrInvalidArgument, i)
			}
		}
	default:
		return nil, fmt.Errorf("%w: unsupported ORC file content %d", iceberg.ErrInvalidArgument, info.Content)
	}
	fields := info.FileSchema.Fields()
	if len(fields) != len(schema.Fields()) {
		return nil, fmt.Errorf("%w: ORC schema field counts differ", iceberg.ErrInvalidArgument)
	}
	columns := make([]orcColumn, 0, len(fields))
	rootIDs := make([]int, 0, len(fields))
	for i, field := range fields {
		id, err := appendORCColumn(&columns, field, schema.Field(i))
		if err != nil {
			return nil, err
		}
		rootIDs = append(rootIDs, id)
	}
	if fs == nil {
		return nil, fmt.Errorf("%w: missing ORC file IO", iceberg.ErrInvalidArgument)
	}

	return &orcFileWriter{
		ctx: ctx, fs: fs, info: info, columns: columns, rootIDs: rootIDs,
		schema: schema, partitionValues: partitionValues,
	}, nil
}

func (w *orcFileWriter) BytesWritten() int64 {
	if w.closed {
		return w.size
	}
	var size int64
	for i := range w.columns {
		c := &w.columns[i]
		// Use actual varint widths for accurate rolling-file estimates.
		size += int64(c.data.Len()) + c.integerBytes + c.lengthBytes + c.secondaryBytes
		size += int64((len(c.integers)+127)/128 + (len(c.lengths)+127)/128 + (len(c.secondary)+127)/128)
		if c.nulls > 0 {
			size += orcByteRLESize((len(c.present) + 7) / 8)
		}
		if c.kind == 0 {
			size += orcByteRLESize((len(c.bits) + 7) / 8)
		}
	}

	return size
}

func orcByteRLESize(n int) int64 {
	return int64(n + (n+127)/128)
}

// validatePositionBatch enforces the Iceberg ordering and nonnegative offsets
// before mutating any column buffers.
func (w *orcFileWriter) validatePositionBatch(batch arrow.RecordBatch) error {
	var pathAt func(int) string
	switch col := batch.Column(0).(type) {
	case *array.String:
		pathAt = col.Value
	case *array.LargeString:
		pathAt = col.Value
	default:
		return fmt.Errorf("%w: ORC position deletes require string file_path", iceberg.ErrInvalidArgument)
	}
	positionCol, ok := batch.Column(1).(*array.Int64)
	if !ok {
		return fmt.Errorf("%w: ORC position deletes require int64 pos", iceberg.ErrInvalidArgument)
	}
	for row := range int(batch.NumRows()) {
		if batch.Column(0).IsNull(row) || positionCol.IsNull(row) {
			return fmt.Errorf("%w: ORC position deletes cannot contain null paths or offsets", iceberg.ErrInvalidArgument)
		}
		path, pos := pathAt(row), positionCol.Value(row)
		if path == "" || pos < 0 {
			return fmt.Errorf("%w: ORC position delete requires a nonempty path and nonnegative offset", iceberg.ErrInvalidArgument)
		}
		if w.hasLastPosition && (path < w.lastPath || (path == w.lastPath && pos < w.lastPosition)) {
			return fmt.Errorf("%w: ORC position deletes must be sorted by file_path and pos", iceberg.ErrInvalidArgument)
		}
		if !w.hasLastPosition {
			w.firstPath = path
		}
		w.lastPath, w.lastPosition, w.hasLastPosition = path, pos, true
	}

	return nil
}

func (w *orcFileWriter) Write(batch arrow.RecordBatch) (err error) {
	if w.closed {
		return errors.New("ORC writer is closed")
	}
	if w.failed != nil {
		return w.failed
	}
	// A failed batch may have appended partial child streams. Never finalize it.
	defer func() {
		if err != nil {
			w.failed = err
		}
	}()
	if batch == nil || !w.schema.Equal(batch.Schema()) {
		return fmt.Errorf("%w: ORC record batch schema changed", iceberg.ErrInvalidArgument)
	}
	if int(batch.NumCols()) != len(w.rootIDs) {
		return fmt.Errorf("%w: ORC record batch column count changed", iceberg.ErrInvalidArgument)
	}
	if w.info.Content == iceberg.EntryContentPosDeletes {
		if err := w.validatePositionBatch(batch); err != nil {
			return err
		}
	}
	for col, id := range w.rootIDs {
		for row := range int(batch.NumRows()) {
			if err := w.appendValue(id, batch.Column(col), row); err != nil {
				return err
			}
		}
	}
	w.rows += batch.NumRows()

	return nil
}

func (w *orcFileWriter) appendValue(id int, values arrow.Array, row int) error {
	c := &w.columns[id]
	present := !values.IsNull(row)
	if !present && c.field.Required {
		return fmt.Errorf("%w: null required ORC column %q", iceberg.ErrInvalidArgument, c.field.Name)
	}
	c.present = append(c.present, present)
	if !present {
		c.nulls++

		return nil
	}

	switch c.kind {
	case 12: // STRUCT
		a := values.(*array.Struct)
		for i, childID := range c.children {
			if err := w.appendValue(childID, a.Field(i), row); err != nil {
				return err
			}
		}

		return nil
	case 10: // LIST
		a := values.(interface {
			ValueOffsets(int) (int64, int64)
			ListValues() arrow.Array
		})
		start, end := a.ValueOffsets(row)
		c.addLength(uint64(end - start))
		child := a.ListValues()
		for i := int(start); i < int(end); i++ {
			if err := w.appendValue(c.children[0], child, i); err != nil {
				return err
			}
		}

		return nil
	case 11: // MAP
		a := values.(*array.Map)
		start, end := a.ValueOffsets(row)
		c.addLength(uint64(end - start))
		for i := int(start); i < int(end); i++ {
			if err := w.appendValue(c.children[0], a.Keys(), i); err != nil {
				return err
			}
			if err := w.appendValue(c.children[1], a.Items(), i); err != nil {
				return err
			}
		}

		return nil
	}

	switch a := values.(type) {
	case *array.Boolean:
		c.bits = append(c.bits, a.Value(row))
	case *array.Int32:
		c.addInteger(orcZigZag(int64(a.Value(row))))
	case *array.Int64:
		v := a.Value(row)
		c.addInteger(orcZigZag(v))
	case *array.Date32:
		c.addInteger(orcZigZag(int64(a.Value(row))))
	case *array.Time64:
		v := int64(a.Value(row))
		if v < 0 || v >= 86400000000 {
			return fmt.Errorf("%w: ORC time is outside a day", iceberg.ErrInvalidArgument)
		}
		c.addInteger(orcZigZag(v))
	case *array.Timestamp:
		v := int64(a.Value(row))
		unit := int64(1_000_000)
		switch c.field.Type.(type) {
		case iceberg.TimestampNsType, iceberg.TimestampTzNsType:
			unit = 1_000_000_000
		}
		seconds, fraction := v/unit, v%unit
		if fraction < 0 {
			seconds--
			fraction += unit
		}
		c.addInteger(orcZigZag(seconds - 1_420_070_400))
		if unit == 1_000_000 {
			fraction *= 1_000
		}
		c.addSecondary(orcTimestampNanos(uint64(fraction)))
	case *array.Float32:
		var b [4]byte
		binary.LittleEndian.PutUint32(b[:], math.Float32bits(a.Value(row)))
		c.data.Write(b[:])
	case *array.Float64:
		var b [8]byte
		binary.LittleEndian.PutUint64(b[:], math.Float64bits(a.Value(row)))
		c.data.Write(b[:])
	case *array.String:
		v := a.Value(row)
		c.data.WriteString(v)
		c.addLength(uint64(len(v)))
	case *array.LargeString:
		v := a.Value(row)
		c.data.WriteString(v)
		c.addLength(uint64(len(v)))
	case *array.Binary:
		v := a.Value(row)
		c.data.Write(v)
		c.addLength(uint64(len(v)))
	case *array.LargeBinary:
		v := a.Value(row)
		c.data.Write(v)
		c.addLength(uint64(len(v)))
	default:
		return fmt.Errorf("%w: unsupported Arrow ORC input %T", iceberg.ErrNotImplemented, values)
	}

	return nil
}

func (w *orcFileWriter) Abort() error {
	// No output is created until Close.
	w.closed = true
	w.columns = nil

	return nil
}

func (w *orcFileWriter) Close() (iceberg.DataFile, error) {
	if w.closed {
		return nil, errors.New("ORC writer already closed")
	}
	if w.failed != nil {
		w.closed = true
		w.columns = nil

		return nil, fmt.Errorf("ORC writer cannot close after a failed write: %w", w.failed)
	}
	w.closed = true
	// Serialize before creating the destination so failures cannot leave an
	// empty or incomplete object behind.
	data, err := w.encode()
	if err != nil {
		return nil, err
	}
	if err := w.ctx.Err(); err != nil {
		return nil, err
	}
	file, err := w.fs.Create(w.ctx, w.info.FileName)
	if err != nil {
		return nil, err
	}
	n, writeErr := file.Write(data)
	if writeErr == nil && n != len(data) {
		writeErr = io.ErrShortWrite
	}
	closeErr := file.Close()
	if writeErr != nil || closeErr != nil {
		_ = w.fs.Remove(context.Background(), w.info.FileName)

		return nil, errors.Join(writeErr, closeErr)
	}
	w.size = int64(len(data))
	nulls := make(map[int]int64, len(w.columns))
	values := make(map[int]int64, len(w.columns))
	for _, c := range w.columns {
		nulls[c.field.ID] = c.nulls
		values[c.field.ID] = int64(len(c.present))
	}
	stats := &DataFileStatistics{
		RecordCount: w.rows, NullValueCounts: nulls, ValueCounts: values,
		EqualityFieldIDs: append([]int(nil), w.info.EqualityFieldIDs...),
	}

	opts := DataFileOpts{
		Schema: w.info.FileSchema, Spec: w.info.Spec, Path: w.info.FileName,
		Format: iceberg.OrcFile, Content: w.info.Content, FileSize: w.size,
		PartitionValues: w.partitionValues, SortOrderID: w.info.SortOrderID,
	}
	if w.info.Content == iceberg.EntryContentPosDeletes && w.hasLastPosition && w.firstPath == w.lastPath {
		path := w.firstPath
		opts.ReferencedDataFile = &path
	}

	return stats.ToDataFile(opts), nil
}

func (w *orcFileWriter) encode() ([]byte, error) {
	// The stripe contains no indexes and uses ORC's v1 DIRECT encoding.
	// The only compression kind emitted here is NONE.
	out := []byte("ORC")
	stripeFooter := []byte{}
	for idx := range w.columns {
		c := &w.columns[idx]
		columnID := uint64(idx + 1)
		if c.nulls > 0 {
			present := orcEncodeByteRLE(orcPackBits(c.present))
			out = append(out, present...)
			stripeFooter = orcPBBytes(stripeFooter, 1, orcStream(0, columnID, len(present)))
		}

		switch c.kind {
		case 12: // STRUCT uses no data stream.
		case 10, 11: // LIST and MAP store lengths, but no data stream.
			lengths := orcEncodeIntegers(c.lengths)
			out = append(out, lengths...)
			stripeFooter = orcPBBytes(stripeFooter, 1, orcStream(2, columnID, len(lengths)))
		default:
			var data []byte
			switch c.kind {
			case 0:
				data = orcEncodeByteRLE(orcPackBits(c.bits))
			case 3, 4, 9, 15, 18:
				data = orcEncodeIntegers(c.integers)
			default:
				data = c.data.Bytes()
			}
			out = append(out, data...)
			stripeFooter = orcPBBytes(stripeFooter, 1, orcStream(1, columnID, len(data)))
			if c.kind == 7 || c.kind == 8 {
				lengths := orcEncodeIntegers(c.lengths)
				out = append(out, lengths...)
				stripeFooter = orcPBBytes(stripeFooter, 1, orcStream(2, columnID, len(lengths)))
			}
			if c.kind == 9 || c.kind == 18 {
				secondary := orcEncodeIntegers(c.secondary)
				out = append(out, secondary...)
				stripeFooter = orcPBBytes(stripeFooter, 1, orcStream(5, columnID, len(secondary)))
			}
		}
	}
	if orcHasTimestamps(w.columns) {
		stripeFooter = orcPBBytes(stripeFooter, 3, []byte("UTC"))
	}
	dataLength := len(out) - 3
	// Column encodings follow depth-first ORC type IDs, including nested types.
	for i := 0; i <= len(w.columns); i++ {
		stripeFooter = orcPBBytes(stripeFooter, 2, orcPBVarint(nil, 1, 0))
	}
	out = append(out, stripeFooter...)
	stripeInfo := orcPBVarint(nil, 1, 3)
	stripeInfo = orcPBVarint(stripeInfo, 2, 0)
	stripeInfo = orcPBVarint(stripeInfo, 3, uint64(dataLength))
	stripeInfo = orcPBVarint(stripeInfo, 4, uint64(len(stripeFooter)))
	stripeInfo = orcPBVarint(stripeInfo, 5, uint64(w.rows))
	root := orcPBVarint(nil, 1, 12) // STRUCT
	var childIDs []byte
	for _, id := range w.rootIDs {
		childIDs = binary.AppendUvarint(childIDs, uint64(id+1))
	}
	root = orcPBBytes(root, 2, childIDs)
	for _, field := range w.info.FileSchema.Fields() {
		root = orcPBBytes(root, 3, []byte(field.Name))
	}
	footer := orcPBVarint(nil, 1, 3)
	footer = orcPBVarint(footer, 2, uint64(dataLength+len(stripeFooter)))
	footer = orcPBBytes(footer, 3, stripeInfo)
	footer = orcPBBytes(footer, 4, root)
	for _, c := range w.columns {
		t := orcPBVarint(nil, 1, c.kind)
		if len(c.children) > 0 {
			var childIDs []byte
			for _, id := range c.children {
				childIDs = binary.AppendUvarint(childIDs, uint64(id+1))
			}
			t = orcPBBytes(t, 2, childIDs)
		}
		for _, name := range c.childNames {
			t = orcPBBytes(t, 3, []byte(name))
		}
		t = orcPBBytes(t, 7, orcPair("iceberg.id", strconv.Itoa(c.field.ID)))
		t = orcPBBytes(t, 7, orcPair("iceberg.required", strconv.FormatBool(c.field.Required)))
		if _, ok := c.field.Type.(iceberg.TimeType); ok {
			t = orcPBBytes(t, 7, orcPair("iceberg.long-type", "TIME"))
		}
		if c.kind == 9 || c.kind == 18 {
			unit := "MICROS"
			switch c.field.Type.(type) {
			case iceberg.TimestampNsType, iceberg.TimestampTzNsType:
				unit = "NANOS"
			}
			t = orcPBBytes(t, 7, orcPair("iceberg.timestamp-unit", unit))
		}
		footer = orcPBBytes(footer, 4, t)
	}
	footer = orcPBVarint(footer, 6, uint64(w.rows))
	// A zero row index stride means no row indexes were written.
	footer = orcPBVarint(footer, 8, 0)
	out = append(out, footer...)
	ps := orcPBVarint(nil, 1, uint64(len(footer)))
	ps = orcPBVarint(ps, 2, 0)
	ps = orcPBVarint(ps, 3, 256<<10)
	ps = orcPBBytes(ps, 4, []byte{0, 12})
	ps = orcPBVarint(ps, 5, 0)
	ps = orcPBVarint(ps, 6, 6)
	ps = orcPBBytes(ps, 8000, []byte("ORC"))
	if len(ps) > 255 {
		return nil, fmt.Errorf("ORC postscript too large: %d", len(ps))
	}
	out = append(out, ps...)
	out = append(out, byte(len(ps)))

	return out, nil
}

// orcTimestampNanos encodes the nanosecond fraction in ORC's compact format.
func orcTimestampNanos(nanos uint64) uint64 {
	if nanos == 0 {
		return 0
	}
	if nanos%100 != 0 {
		return nanos << 3
	}
	zeros := uint64(1)
	nanos /= 100
	for nanos%10 == 0 && zeros < 7 {
		nanos /= 10
		zeros++
	}

	return nanos<<3 | zeros
}

func orcHasTimestamps(columns []orcColumn) bool {
	for _, c := range columns {
		if c.kind == 9 || c.kind == 18 {
			return true
		}
	}

	return false
}

func orcPBVarint(out []byte, field int, n uint64) []byte {
	out = binary.AppendUvarint(out, uint64(field<<3))

	return binary.AppendUvarint(out, n)
}

func orcPBBytes(out []byte, field int, v []byte) []byte {
	out = binary.AppendUvarint(out, uint64(field<<3|2))
	out = binary.AppendUvarint(out, uint64(len(v)))

	return append(out, v...)
}

func orcPair(key, value string) []byte {
	p := orcPBBytes(nil, 1, []byte(key))

	return orcPBBytes(p, 2, []byte(value))
}

func orcStream(kind, column uint64, length int) []byte {
	b := orcPBVarint(nil, 1, kind)
	b = orcPBVarint(b, 2, column)

	return orcPBVarint(b, 3, uint64(length))
}

func orcZigZag(value int64) uint64 {
	return uint64(value<<1) ^ uint64(value>>63)
}

func orcEncodeIntegers(values []uint64) []byte {
	var out []byte
	for len(values) > 0 {
		n := min(128, len(values))
		// ORC RLEv1 negative control byte means n literal values.
		out = append(out, byte(256-n))
		for _, v := range values[:n] {
			out = binary.AppendUvarint(out, v)
		}
		values = values[n:]
	}

	return out
}

func orcPackBits(bits []bool) []byte {
	out := make([]byte, (len(bits)+7)/8)
	for i, v := range bits {
		if v {
			out[i/8] |= 1 << uint(7-i%8)
		}
	}

	return out
}

func orcEncodeByteRLE(values []byte) []byte {
	var out []byte
	for len(values) > 0 {
		n := min(128, len(values))
		out = append(out, byte(256-n))
		out = append(out, values[:n]...)
		values = values[n:]
	}

	return out
}
