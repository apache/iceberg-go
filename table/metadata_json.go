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
	"encoding/json"
	"fmt"
	"sort"
	"strconv"

	"github.com/apache/iceberg-go"
)

const maxJSONDepth = 10_000

type metadataJSONFields struct {
	formatVersion  metadataJSONSpan
	lastUpdatedMS  metadataJSONSpan
	lastPartition  metadataJSONSpan
	partitionSpec  metadataJSONSpan
	partitionSpecs metadataJSONSpan
}

type jsonReplacement struct {
	span metadataJSONSpan
	raw  []byte
}

func prepareMetadataJSON(data []byte) ([]byte, int, error) {
	// encoding/json accepts a top-level null for a struct and leaves the format
	// version at its zero value. Preserve the resulting format-version error.
	if bytes.Equal(bytes.TrimSpace(data), []byte("null")) {
		return nil, 0, ErrInvalidMetadataFormatVersion
	}

	fields, err := scanMetadataJSON(data)
	if err != nil {
		return nil, 0, fmt.Errorf("%w: %w", ErrInvalidMetadata, err)
	}

	var formatVersion int
	if fields.formatVersion.empty() {
		return nil, 0, ErrInvalidMetadataFormatVersion
	}
	if err := json.Unmarshal(fields.formatVersion.slice(data), &formatVersion); err != nil {
		return nil, 0, fmt.Errorf("%w: %w", ErrInvalidMetadata, err)
	}
	if formatVersion < 1 || formatVersion > supportedTableFormatVersion {
		return nil, 0, ErrInvalidMetadataFormatVersion
	}
	if fields.lastUpdatedMS.empty() || bytes.Equal(bytes.TrimSpace(fields.lastUpdatedMS.slice(data)), []byte("null")) {
		return nil, 0, fmt.Errorf("%w: last-updated-ms is absent or null", ErrInvalidMetadata)
	}

	replacements, err := partitionSpecReplacements(data, fields)
	if err != nil {
		return nil, 0, err
	}

	return applyJSONReplacements(data, replacements), formatVersion, nil
}

func partitionSpecReplacements(data []byte, fields metadataJSONFields) ([]jsonReplacement, error) {
	type rawPartitionSpec struct {
		ID     json.RawMessage              `json:"spec-id"`
		Fields []map[string]json.RawMessage `json:"fields"`
	}

	var specs []rawPartitionSpec
	usesSpecList := !fields.partitionSpecs.empty()
	if usesSpecList {
		if err := json.Unmarshal(fields.partitionSpecs.slice(data), &specs); err != nil {
			return nil, err
		}
		for i, spec := range specs {
			if len(spec.ID) == 0 || bytes.Equal(bytes.TrimSpace(spec.ID), []byte("null")) {
				return nil, fmt.Errorf("%w: partition spec at index %d is missing required spec-id", ErrInvalidMetadata, i)
			}
		}
	} else if !fields.partitionSpec.empty() {
		var partitionFields []map[string]json.RawMessage
		if err := json.Unmarshal(fields.partitionSpec.slice(data), &partitionFields); err != nil {
			return nil, err
		}
		specs = []rawPartitionSpec{{Fields: partitionFields}}
	} else {
		return nil, nil
	}

	lastAssignedID := iceberg.PartitionDataIDStart - 1
	lastPartitionIDSet := false
	if !fields.lastPartition.empty() {
		var lastPartitionID *int
		if err := json.Unmarshal(fields.lastPartition.slice(data), &lastPartitionID); err == nil && lastPartitionID != nil {
			lastAssignedID = max(lastAssignedID, *lastPartitionID)
			lastPartitionIDSet = true
		}
	}

	missingFields := make([]map[string]json.RawMessage, 0)
	for _, spec := range specs {
		for _, field := range spec.Fields {
			rawFieldID, ok := field["field-id"]
			if !ok {
				missingFields = append(missingFields, field)

				continue
			}

			var fieldID *int
			if err := json.Unmarshal(rawFieldID, &fieldID); err == nil && fieldID != nil {
				lastAssignedID = max(lastAssignedID, *fieldID)
			}
		}
	}
	if len(missingFields) == 0 {
		return nil, nil
	}

	for _, field := range missingFields {
		lastAssignedID++
		field["field-id"] = json.RawMessage(strconv.AppendInt(nil, int64(lastAssignedID), 10))
	}

	replacements := make([]jsonReplacement, 0, 2)
	if usesSpecList {
		raw, err := json.Marshal(specs)
		if err != nil {
			return nil, err
		}
		replacements = append(replacements, jsonReplacement{span: fields.partitionSpecs, raw: raw})
	} else {
		raw, err := json.Marshal(specs[0].Fields)
		if err != nil {
			return nil, err
		}
		replacements = append(replacements, jsonReplacement{span: fields.partitionSpec, raw: raw})
	}
	if lastPartitionIDSet {
		replacements = append(replacements, jsonReplacement{
			span: fields.lastPartition,
			raw:  strconv.AppendInt(nil, int64(lastAssignedID), 10),
		})
	}

	return replacements, nil
}

func applyJSONReplacements(data []byte, replacements []jsonReplacement) []byte {
	if len(replacements) == 0 {
		return data
	}
	sort.Slice(replacements, func(i, j int) bool { return replacements[i].span.start < replacements[j].span.start })

	size := len(data)
	for _, replacement := range replacements {
		size += len(replacement.raw) - (replacement.span.end - replacement.span.start)
	}
	out := make([]byte, 0, size)
	last := 0
	for _, replacement := range replacements {
		out = append(out, data[last:replacement.span.start]...)
		out = append(out, replacement.raw...)
		last = replacement.span.end
	}

	return append(out, data[last:]...)
}

func scanMetadataJSON(data []byte) (metadataJSONFields, error) {
	var fields metadataJSONFields
	scanner := jsonStructuralScanner{data: data}
	scanner.skipSpace()
	if !scanner.consume('{') {
		return fields, scanner.syntaxError("expected metadata object")
	}
	scanner.skipSpace()
	if scanner.consume('}') {
		scanner.skipSpace()
		if scanner.pos != len(data) {
			return fields, scanner.syntaxError("unexpected data after metadata object")
		}

		return fields, nil
	}

	for {
		scanner.skipSpace()
		keyStart := scanner.pos
		if err := scanner.skipString(); err != nil {
			return fields, err
		}
		var key string
		if err := json.Unmarshal(data[keyStart:scanner.pos], &key); err != nil {
			return fields, err
		}
		scanner.skipSpace()
		if !scanner.consume(':') {
			return fields, scanner.syntaxError("expected colon after object key")
		}
		scanner.skipSpace()
		valueStart := scanner.pos
		if err := scanner.skipValue(1); err != nil {
			return fields, err
		}
		span := metadataJSONSpan{start: valueStart, end: scanner.pos}
		switch key {
		case "format-version":
			fields.formatVersion = span
		case "last-updated-ms":
			fields.lastUpdatedMS = span
		case "last-partition-id":
			fields.lastPartition = span
		case "partition-spec":
			fields.partitionSpec = span
		case "partition-specs":
			fields.partitionSpecs = span
		}

		scanner.skipSpace()
		if scanner.consume('}') {
			break
		}
		if !scanner.consume(',') {
			return fields, scanner.syntaxError("expected comma or closing brace")
		}
	}
	scanner.skipSpace()
	if scanner.pos != len(data) {
		return fields, scanner.syntaxError("unexpected data after metadata object")
	}

	return fields, nil
}

type jsonStructuralScanner struct {
	data []byte
	pos  int
}

func (s *jsonStructuralScanner) skipValue(depth int) error {
	if depth > maxJSONDepth {
		return s.syntaxError("maximum JSON depth exceeded")
	}
	if s.pos >= len(s.data) {
		return s.syntaxError("unexpected end of JSON input")
	}
	switch s.data[s.pos] {
	case '{':
		return s.skipObject(depth)
	case '[':
		return s.skipArray(depth)
	case '"':
		return s.skipString()
	case 't':
		return s.skipLiteral("true")
	case 'f':
		return s.skipLiteral("false")
	case 'n':
		return s.skipLiteral("null")
	default:
		return s.skipNumber()
	}
}

func (s *jsonStructuralScanner) skipObject(depth int) error {
	s.pos++
	s.skipSpace()
	if s.consume('}') {
		return nil
	}
	for {
		s.skipSpace()
		if err := s.skipString(); err != nil {
			return err
		}
		s.skipSpace()
		if !s.consume(':') {
			return s.syntaxError("expected colon after object key")
		}
		s.skipSpace()
		if err := s.skipValue(depth + 1); err != nil {
			return err
		}
		s.skipSpace()
		if s.consume('}') {
			return nil
		}
		if !s.consume(',') {
			return s.syntaxError("expected comma or closing brace")
		}
	}
}

func (s *jsonStructuralScanner) skipArray(depth int) error {
	s.pos++
	s.skipSpace()
	if s.consume(']') {
		return nil
	}
	for {
		if err := s.skipValue(depth + 1); err != nil {
			return err
		}
		s.skipSpace()
		if s.consume(']') {
			return nil
		}
		if !s.consume(',') {
			return s.syntaxError("expected comma or closing bracket")
		}
		s.skipSpace()
	}
}

func (s *jsonStructuralScanner) skipString() error {
	if !s.consume('"') {
		return s.syntaxError("expected string")
	}
	for s.pos < len(s.data) {
		char := s.data[s.pos]
		s.pos++
		switch char {
		case '"':
			return nil
		case '\\':
			if s.pos >= len(s.data) {
				return s.syntaxError("unexpected end of JSON string")
			}
			escaped := s.data[s.pos]
			s.pos++
			if bytes.IndexByte([]byte(`"\\/bfnrt`), escaped) >= 0 {
				continue
			}
			if escaped != 'u' || s.pos+4 > len(s.data) {
				return s.syntaxError("invalid character in string escape")
			}
			for range 4 {
				if !isHex(s.data[s.pos]) {
					return s.syntaxError("invalid unicode escape")
				}
				s.pos++
			}
		default:
			if char < 0x20 {
				return s.syntaxError("invalid control character in string")
			}
		}
	}

	return s.syntaxError("unexpected end of JSON string")
}

func (s *jsonStructuralScanner) skipLiteral(literal string) error {
	if len(s.data)-s.pos < len(literal) || string(s.data[s.pos:s.pos+len(literal)]) != literal {
		return s.syntaxError("invalid JSON literal")
	}
	s.pos += len(literal)

	return nil
}

func (s *jsonStructuralScanner) skipNumber() error {
	start := s.pos
	if s.consume('-') && s.pos == len(s.data) {
		return s.syntaxError("invalid JSON number")
	}
	if s.consume('0') {
		if s.pos < len(s.data) && s.data[s.pos] >= '0' && s.data[s.pos] <= '9' {
			return s.syntaxError("invalid leading zero in JSON number")
		}
	} else if !s.consumeDigits() {
		return s.syntaxError("invalid JSON value")
	}
	if s.consume('.') && !s.consumeDigits() {
		return s.syntaxError("invalid JSON number fraction")
	}
	if s.pos < len(s.data) && (s.data[s.pos] == 'e' || s.data[s.pos] == 'E') {
		s.pos++
		if s.pos < len(s.data) && (s.data[s.pos] == '+' || s.data[s.pos] == '-') {
			s.pos++
		}
		if !s.consumeDigits() {
			return s.syntaxError("invalid JSON number exponent")
		}
	}
	if s.pos == start {
		return s.syntaxError("invalid JSON value")
	}

	return nil
}

func (s *jsonStructuralScanner) consumeDigits() bool {
	start := s.pos
	for s.pos < len(s.data) && s.data[s.pos] >= '0' && s.data[s.pos] <= '9' {
		s.pos++
	}

	return s.pos > start
}

func (s *jsonStructuralScanner) skipSpace() {
	for s.pos < len(s.data) {
		switch s.data[s.pos] {
		case ' ', '\t', '\n', '\r':
			s.pos++
		default:
			return
		}
	}
}

func (s *jsonStructuralScanner) consume(char byte) bool {
	if s.pos < len(s.data) && s.data[s.pos] == char {
		s.pos++

		return true
	}

	return false
}

func (s *jsonStructuralScanner) syntaxError(message string) error {
	return fmt.Errorf("%s at byte %d", message, s.pos+1)
}

func isHex(char byte) bool {
	return char >= '0' && char <= '9' || char >= 'a' && char <= 'f' || char >= 'A' && char <= 'F'
}

type metadataJSONSpan struct {
	start int
	end   int
}

func (s metadataJSONSpan) empty() bool {
	return s.start == 0 && s.end == 0
}

func (s metadataJSONSpan) slice(data []byte) []byte {
	return data[s.start:s.end]
}
