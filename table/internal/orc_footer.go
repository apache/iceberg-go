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
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"math"
	"strconv"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/extensions"
	"github.com/apache/iceberg-go"
	iceio "github.com/apache/iceberg-go/io"
	"github.com/scritchley/orc"
	orcpb "github.com/scritchley/orc/proto"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/protoadapt"
)

const maxORCFooterBytes int64 = 64 << 20

// sizedORCFile adapts Iceberg FileIO to the ORC ReaderAt interface.
type sizedORCFile struct {
	io.ReaderAt
	size int64
}

func (s sizedORCFile) Size() int64 { return s.size }

// readORCFileSchema reads the original ORC footer, including Iceberg type
// attributes. The older ORC reader library discards type attributes when
// exposing its TypeDescription, so those attributes must be read separately.
func readORCFileSchema(file iceio.File) (*arrow.Schema, int64, error) {
	stat, err := file.Stat()
	if err != nil {
		return nil, 0, err
	}
	size := stat.Size()
	if size < 4 {
		return nil, 0, fmt.Errorf("%w: ORC file too short", iceberg.ErrInvalidSchema)
	}

	n := min(size, 256)
	tail := make([]byte, int(n))
	if _, err = file.ReadAt(tail, size-n); err != nil {
		return nil, 0, fmt.Errorf("read ORC postscript: %w", err)
	}
	psLen := int(tail[len(tail)-1])
	if psLen <= 0 || psLen >= len(tail) {
		return nil, 0, fmt.Errorf("%w: invalid ORC postscript length", iceberg.ErrInvalidSchema)
	}
	var ps orcpb.PostScript
	if err = proto.Unmarshal(tail[len(tail)-psLen-1:len(tail)-1], protoadapt.MessageV2Of(&ps)); err != nil {
		return nil, 0, fmt.Errorf("decode ORC postscript: %w", err)
	}
	footerLen := int64(ps.GetFooterLength())
	footerEnd := size - int64(psLen) - 1
	if footerLen <= 0 || footerLen > footerEnd || footerLen > maxORCFooterBytes {
		return nil, 0, fmt.Errorf("%w: invalid ORC footer length", iceberg.ErrInvalidSchema)
	}

	encoded := io.NewSectionReader(file, footerEnd-footerLen, footerLen)
	var decoder io.Reader
	switch ps.GetCompression() {
	case orcpb.CompressionKind_NONE:
		decoder = encoded
	case orcpb.CompressionKind_ZLIB:
		decoder = orc.CompressionZlib{}.Decoder(encoded)
	case orcpb.CompressionKind_SNAPPY:
		decoder = orc.CompressionSnappy{}.Decoder(encoded)
	default:
		return nil, 0, fmt.Errorf("%w: ORC compression %s", iceberg.ErrNotImplemented, ps.GetCompression())
	}
	payload, err := io.ReadAll(io.LimitReader(decoder, maxORCFooterBytes+1))
	if err != nil {
		return nil, 0, fmt.Errorf("decode ORC footer: %w", err)
	}
	if int64(len(payload)) > maxORCFooterBytes {
		return nil, 0, fmt.Errorf("%w: ORC footer exceeds size limit", iceberg.ErrInvalidSchema)
	}
	var footer orcpb.Footer
	if err = proto.Unmarshal(payload, protoadapt.MessageV2Of(&footer)); err != nil {
		return nil, 0, fmt.Errorf("parse ORC footer: %w", err)
	}
	if err := validateORCDateStreams(file, size, ps.GetCompression(), &footer); err != nil {
		return nil, 0, err
	}
	types := footer.GetTypes()
	if len(types) == 0 {
		return nil, 0, fmt.Errorf("%w: ORC footer contains no types", iceberg.ErrInvalidSchema)
	}
	if err := validateORCTypeTree(types); err != nil {
		return nil, 0, err
	}
	if types[0].GetKind() != orcpb.Type_STRUCT {
		return nil, 0, fmt.Errorf("%w: ORC root must be a struct", iceberg.ErrInvalidSchema)
	}

	seen := make(map[int]struct{})
	fields, err := orcStructFields(types, types[0], seen)
	if err != nil {
		return nil, 0, err
	}

	return arrow.NewSchema(fields, nil), size, nil
}

// ORC subtype indexes form a tree, not a graph. Reused or cyclic indexes
// would otherwise recurse indefinitely or interpret one column twice.
// Limit the depth of untrusted footer types before recursively converting
// them to Arrow schemas or handing them to the ORC decoder.
const maxORCTypeNesting = 256

func validateORCTypeTree(types []*orcpb.Type) error {
	type pendingType struct {
		index uint32
		depth int
	}
	pending := []pendingType{{index: 0, depth: 1}}
	seen := make(map[uint32]struct{}, len(types))
	for len(pending) != 0 {
		item := pending[len(pending)-1]
		pending = pending[:len(pending)-1]
		if item.depth > maxORCTypeNesting {
			return fmt.Errorf("%w: ORC type tree exceeds maximum nesting depth %d", iceberg.ErrInvalidSchema, maxORCTypeNesting)
		}
		if uint64(item.index) >= uint64(len(types)) || types[item.index] == nil {
			return fmt.Errorf("%w: invalid ORC type index %d", iceberg.ErrInvalidSchema, item.index)
		}
		if _, exists := seen[item.index]; exists {
			return fmt.Errorf("%w: repeated ORC type index %d", iceberg.ErrInvalidSchema, item.index)
		}
		seen[item.index] = struct{}{}
		for _, child := range types[item.index].GetSubtypes() {
			pending = append(pending, pendingType{index: child, depth: item.depth + 1})
		}
	}
	// Every footer type must be reachable from the root. Otherwise a
	// malformed footer could contain columns that the reader silently drops.
	if len(seen) != len(types) {
		return fmt.Errorf("%w: ORC type tree contains %d unreferenced types", iceberg.ErrInvalidSchema, len(types)-len(seen))
	}

	return nil
}

func orcStructFields(types []*orcpb.Type, root *orcpb.Type, seen map[int]struct{}) ([]arrow.Field, error) {
	names, ids := root.GetFieldNames(), root.GetSubtypes()
	if root.GetKind() != orcpb.Type_STRUCT || len(names) != len(ids) {
		return nil, fmt.Errorf("%w: invalid ORC struct fields", iceberg.ErrInvalidSchema)
	}
	fields := make([]arrow.Field, len(names))
	usedNames := make(map[string]struct{}, len(names))
	for i, name := range names {
		if name == "" {
			return nil, fmt.Errorf("%w: ORC struct has an empty field name", iceberg.ErrInvalidSchema)
		}
		if _, exists := usedNames[name]; exists {
			return nil, fmt.Errorf("%w: duplicate ORC struct field %q", iceberg.ErrInvalidSchema, name)
		}
		usedNames[name] = struct{}{}
		f, err := orcSchemaField(types, ids[i], name, seen)
		if err != nil {
			return nil, fmt.Errorf("ORC field %q: %w", name, err)
		}
		fields[i] = f
	}

	return fields, nil
}

func orcSchemaField(types []*orcpb.Type, column uint32, name string, seen map[int]struct{}) (arrow.Field, error) {
	if uint64(column) >= uint64(len(types)) {
		return arrow.Field{}, fmt.Errorf("%w: invalid ORC column index %d", iceberg.ErrInvalidSchema, column)
	}
	t := types[column]
	attrs, err := orcTypeAttributes(t)
	if err != nil {
		return arrow.Field{}, err
	}
	var field arrow.Field
	field.Name = name
	if idText, ok := attrs["iceberg.id"]; ok {
		id, err := strconv.Atoi(idText)
		if err != nil || id <= 0 || int64(id) > math.MaxInt32-200 {
			return field, fmt.Errorf("%w: invalid iceberg.id %q", iceberg.ErrInvalidSchema, idText)
		}
		if _, exists := seen[id]; exists {
			return field, fmt.Errorf("%w: duplicate iceberg.id %d", iceberg.ErrInvalidSchema, id)
		}
		seen[id] = struct{}{}
		field.Metadata = arrow.MetadataFrom(map[string]string{"PARQUET:field_id": idText})
	}
	if required, ok := attrs["iceberg.required"]; ok {
		val, err := strconv.ParseBool(required)
		if err != nil {
			return field, fmt.Errorf("%w: invalid iceberg.required %q", iceberg.ErrInvalidSchema, required)
		}
		field.Nullable = !val
	} else {
		field.Nullable = true
	}

	switch t.GetKind() {
	case orcpb.Type_BOOLEAN:
		field.Type = arrow.FixedWidthTypes.Boolean
	case orcpb.Type_BYTE, orcpb.Type_SHORT, orcpb.Type_INT:
		field.Type = arrow.PrimitiveTypes.Int32
	case orcpb.Type_LONG:
		switch attrs["iceberg.long-type"] {
		case "":
			field.Type = arrow.PrimitiveTypes.Int64
		case "TIME":
			field.Type = arrow.FixedWidthTypes.Time64us
		default:
			return field, fmt.Errorf("%w: unknown ORC iceberg.long-type", iceberg.ErrNotImplemented)
		}
	case orcpb.Type_FLOAT:
		field.Type = arrow.PrimitiveTypes.Float32
	case orcpb.Type_DOUBLE:
		field.Type = arrow.PrimitiveTypes.Float64
	case orcpb.Type_STRING, orcpb.Type_CHAR, orcpb.Type_VARCHAR:
		field.Type = arrow.BinaryTypes.String
	case orcpb.Type_BINARY:
		switch attrs["iceberg.binary-type"] {
		case "", "BINARY":
			field.Type = arrow.BinaryTypes.Binary
		case "UUID":
			field.Type = extensions.NewUUIDType()
		case "FIXED":
			n, err := strconv.Atoi(attrs["iceberg.length"])
			if err != nil || n < 1 || n > math.MaxInt32 {
				return field, fmt.Errorf("%w: invalid ORC fixed length", iceberg.ErrInvalidSchema)
			}
			field.Type = &arrow.FixedSizeBinaryType{ByteWidth: n}
		default:
			// Geometry and geography require a separate value adapter.
			return field, fmt.Errorf("%w: ORC iceberg.binary-type %q", iceberg.ErrNotImplemented, attrs["iceberg.binary-type"])
		}
	case orcpb.Type_DATE:
		field.Type = arrow.FixedWidthTypes.Date32
	case orcpb.Type_TIMESTAMP:
		return field, fmt.Errorf("%w: ORC timestamps", iceberg.ErrNotImplemented)
	case orcpb.Type_DECIMAL:
		precision, scale := t.GetPrecision(), t.GetScale()
		if precision == 0 {
			precision = 38
		}
		if precision > 18 || scale > precision {
			return field, fmt.Errorf("%w: ORC decimal(%d,%d)", iceberg.ErrNotImplemented, precision, scale)
		}
		field.Type = &arrow.Decimal128Type{Precision: int32(precision), Scale: int32(scale)}
	case orcpb.Type_STRUCT:
		if attrs["iceberg.struct-type"] != "" {
			return field, fmt.Errorf("%w: ORC struct type %q", iceberg.ErrNotImplemented, attrs["iceberg.struct-type"])
		}
		children, err := orcStructFields(types, t, seen)
		if err != nil {
			return field, err
		}
		field.Type = arrow.StructOf(children...)
	case orcpb.Type_LIST:
		children := t.GetSubtypes()
		if len(children) != 1 {
			return field, fmt.Errorf("%w: ORC list must have one element", iceberg.ErrInvalidSchema)
		}
		item, err := orcSchemaField(types, children[0], "element", seen)
		if err != nil {
			return field, err
		}
		field.Type = arrow.ListOfField(item)
	case orcpb.Type_MAP:
		return field, fmt.Errorf("%w: ORC maps", iceberg.ErrNotImplemented)
	default:
		return field, fmt.Errorf("%w: ORC type %s", iceberg.ErrNotImplemented, t.GetKind())
	}

	return field, nil
}

// The ORC dependency's older generated proto.Type does not know the repeated
// StringPair attributes field (field number 7). Protobuf preserves these in
// XXX_unrecognized. Parse that field without changing the dependency.
func orcTypeAttributes(typ *orcpb.Type) (map[string]string, error) {
	attrs := make(map[string]string)
	data := typ.XXX_unrecognized
	for len(data) > 0 {
		number, wire, value, rest, err := consumeORCProtoField(data)
		if err != nil {
			return nil, err
		}
		if number == 7 {
			if wire != 2 {
				return nil, fmt.Errorf("%w: invalid ORC attribute wire type", iceberg.ErrInvalidSchema)
			}
			var key, val string
			var hasKey, hasValue bool
			for len(value) > 0 {
				n, w, part, remaining, err := consumeORCProtoField(value)
				if err != nil {
					return nil, err
				}
				switch n {
				case 1:
					if w != 2 || hasKey {
						return nil, fmt.Errorf("%w: repeated or invalid ORC attribute key", iceberg.ErrInvalidSchema)
					}
					key, hasKey = string(part), true
				case 2:
					if w != 2 || hasValue {
						return nil, fmt.Errorf("%w: repeated or invalid ORC attribute value", iceberg.ErrInvalidSchema)
					}
					val, hasValue = string(part), true
				}
				value = remaining
			}
			if !hasKey || !hasValue || key == "" {
				return nil, fmt.Errorf("%w: incomplete ORC type attribute", iceberg.ErrInvalidSchema)
			}
			if _, exists := attrs[key]; exists {
				return nil, fmt.Errorf("%w: duplicate ORC attribute %q", iceberg.ErrInvalidSchema, key)
			}
			attrs[key] = val
		}
		data = rest
	}

	return attrs, nil
}

func consumeORCProtoField(data []byte) (number int, wire int, value, rest []byte, err error) {
	tag, n := binary.Uvarint(data)
	fieldNumber := tag >> 3
	if n <= 0 || fieldNumber == 0 || fieldNumber > (1<<29)-1 {
		return 0, 0, nil, nil, fmt.Errorf("%w: malformed ORC protobuf tag", iceberg.ErrInvalidSchema)
	}
	number, wire = int(tag>>3), int(tag&7)
	data = data[n:]
	var length uint64
	switch wire {
	case 0:
		_, n = binary.Uvarint(data)
		if n <= 0 {
			return 0, 0, nil, nil, fmt.Errorf("%w: malformed protobuf varint", iceberg.ErrInvalidSchema)
		}

		return number, wire, data[:n], data[n:], nil
	case 1:
		length = 8
	case 2:
		length, n = binary.Uvarint(data)
		if n <= 0 {
			return 0, 0, nil, nil, fmt.Errorf("%w: malformed protobuf length", iceberg.ErrInvalidSchema)
		}
		data = data[n:]
	case 5:
		length = 4
	default:
		return 0, 0, nil, nil, fmt.Errorf("%w: unsupported protobuf wire type %d", iceberg.ErrInvalidSchema, wire)
	}
	if length > uint64(len(data)) {
		return 0, 0, nil, nil, fmt.Errorf("%w: truncated ORC protobuf field", iceberg.ErrInvalidSchema)
	}

	return number, wire, data[:int(length)], data[int(length):], nil
}

// Used by tests to verify ORC StringPair parsing without depending on the
// outdated proto.Type exposing its custom attributes.
func encodeORCAttribute(name, value string) []byte {
	b := make([]byte, 0, 8+len(name)+len(value))
	b = binary.AppendUvarint(b, 1<<3|2)
	b = binary.AppendUvarint(b, uint64(len(name)))
	b = append(b, name...)
	b = binary.AppendUvarint(b, 2<<3|2)
	b = binary.AppendUvarint(b, uint64(len(value)))
	b = append(b, value...)
	out := binary.AppendUvarint(nil, 7<<3|2)
	out = binary.AppendUvarint(out, uint64(len(b)))

	return append(out, b...)
}

// Keeping a small footer sanity test in the reader prevents accidental use of
// legacy ORC format versions with fields it cannot decode.
func orcValidFooterSignature(file iceio.File) error {
	prefix := []byte{0, 0, 0}
	if _, err := file.ReadAt(prefix, 0); err != nil {
		return err
	}
	if !bytes.Equal(prefix, []byte("ORC")) {
		return fmt.Errorf("%w: missing ORC file magic", iceberg.ErrInvalidSchema)
	}

	return nil
}
