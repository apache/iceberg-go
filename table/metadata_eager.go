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
	"encoding/binary"
	"encoding/json"
	"math"
	"strings"
	"unicode/utf8"
)

// parseEagerMetadataV2 recognizes the ordinary v2 snapshot and log layouts.
// Individual snapshots and logs can fall back to the general decoder.
func parseEagerMetadataV2(data []byte) (*metadataV2, bool) {
	if len(data) < 4096 {
		return nil, false
	}
	versionKey := []byte(`"format-version"`)
	versionAt := bytes.Index(data, versionKey)
	if versionAt < 0 {
		return nil, false
	}
	version := metadataJSONCursor{data: data, pos: versionAt + len(versionKey)}
	if !version.take(':') || !version.take('2') || !utf8.Valid(data) {
		return nil, false
	}

	cursor := metadataJSONCursor{data: data}
	if !cursor.take('{') {
		return nil, false
	}

	skeleton := make([]byte, 0, 1024)
	var snapshots []Snapshot
	var snapshotLog []SnapshotLogEntry
	var metadataLog []MetadataLogEntry
	var sawSnapshotLog, sawMetadataLog bool
	previousEnd := 0
	for {
		key, ok := cursor.objectField()
		if !ok {
			return nil, false
		}
		valueAt := cursor.pos
		switch string(key) {
		case "snapshots":
			if snapshots != nil {
				return nil, false
			}
			snapshots, ok = readMetadataArray(&cursor, readEagerSnapshot)
		case "snapshot-log":
			if sawSnapshotLog {
				return nil, false
			}
			sawSnapshotLog = true
			snapshotLog, ok = readEagerLogArray(&cursor, readEagerSnapshotLog)
			if snapshotLog == nil {
				valueAt = -1
			}
		case "metadata-log":
			if sawMetadataLog {
				return nil, false
			}
			sawMetadataLog = true
			metadataLog, ok = readEagerLogArray(&cursor, readEagerMetadataLog)
			if metadataLog == nil {
				valueAt = -1
			}
		default:
			if bytes.EqualFold(key, []byte("snapshots")) || bytes.EqualFold(key, []byte("snapshot-log")) ||
				bytes.EqualFold(key, []byte("metadata-log")) || !cursor.skipValue(1) {
				return nil, false
			}
			valueAt = -1
		}
		if !ok {
			return nil, false
		}
		if valueAt >= 0 {
			skeleton = append(skeleton, data[previousEnd:valueAt]...)
			skeleton = append(skeleton, '[', ']')
			previousEnd = cursor.pos
		}
		if cursor.take('}') {
			break
		}
		if !cursor.take(',') {
			return nil, false
		}
	}
	if snapshots == nil {
		return nil, false
	}

	skeleton = append(skeleton, data[previousEnd:]...)
	header, formatVersion, err := preflightMetadataBytes(skeleton)
	if err != nil || formatVersion != 2 {
		return nil, false
	}
	normalized, err := assignMissingPartitionFieldIDsFromMetadata(skeleton, header)
	if err != nil {
		return nil, false
	}
	next, err := decodeMetadataV2(normalized)
	if err != nil || next.FormatVersion() != 2 {
		return nil, false
	}
	next.SnapshotList = snapshots
	if snapshotLog != nil {
		next.SnapshotLog = snapshotLog
	}
	if metadataLog != nil {
		next.MetadataLog = metadataLog
	}
	if err := next.finishUnmarshal(); err != nil {
		return nil, false
	}

	return next, true
}

type metadataJSONCursor struct {
	data []byte
	pos  int
}

func (c *metadataJSONCursor) take(value byte) bool {
	c.pos = skipJSONSpace(c.data, c.pos)
	if c.pos >= len(c.data) || c.data[c.pos] != value {
		return false
	}
	c.pos++

	return true
}

func (c *metadataJSONCursor) objectField() ([]byte, bool) {
	key, ok := c.stringBytes()

	return key, ok && c.take(':')
}

func (c *metadataJSONCursor) stringBytes() ([]byte, bool) {
	if !c.take('"') {
		return nil, false
	}
	length := bytes.IndexByte(c.data[c.pos:], '"')
	if length < 0 {
		return nil, false
	}
	end := c.pos + length
	if !plainMetadataString(c.data[c.pos:end]) {
		return nil, false
	}
	value := c.data[c.pos:end]
	c.pos = end + 1

	return value, true
}

// Check eight bytes at a time for JSON escapes or unescaped control bytes.
// Subtraction sets a high bit in a byte lane that contains a zero or a value
// below 0x20, respectively.
func plainMetadataString(value []byte) bool {
	const highBits = 0x8080808080808080
	for len(value) >= 8 {
		word := binary.LittleEndian.Uint64(value)
		slashes := word ^ 0x5c5c5c5c5c5c5c5c
		if (slashes-0x0101010101010101)&^slashes&highBits != 0 ||
			(word-0x2020202020202020)&^word&highBits != 0 {
			return false
		}
		value = value[8:]
	}
	for _, ch := range value {
		if ch == 0x5c || ch < 0x20 {
			return false
		}
	}

	return true
}

func (c *metadataJSONCursor) integer() (int64, bool) {
	c.pos = skipJSONSpace(c.data, c.pos)
	negative := c.pos < len(c.data) && c.data[c.pos] == '-'
	if negative {
		c.pos++
	}
	digits := c.pos
	var value uint64
	for c.pos < len(c.data) {
		digit := c.data[c.pos] - '0'
		if digit > 9 {
			break
		}
		if c.pos == digits+19 {
			return 0, false
		}
		value = value*10 + uint64(digit)
		c.pos++
	}
	if c.pos == digits || (c.pos > digits+1 && c.data[digits] == '0') {
		return 0, false
	}
	if negative {
		return -int64(value), value <= uint64(math.MaxInt64)+1
	}

	return int64(value), value <= math.MaxInt64
}

// Match encoding/json's nesting limit even when a history entry is later
// decoded out of its enclosing metadata object and history array.
const (
	maxMetadataJSONDepth      = 10_000
	metadataHistoryEntryDepth = 2
)

// The retained bytes for this value are validated by encoding/json.
func (c *metadataJSONCursor) skipValue(enclosingDepth int) bool {
	c.pos = skipJSONSpace(c.data, c.pos)
	depth := enclosingDepth
	for c.pos < len(c.data) {
		switch c.data[c.pos] {
		case '"':
			c.pos = scanJSONString(c.data, c.pos)

			continue
		case '[', '{':
			depth++
			if depth > maxMetadataJSONDepth {
				return false
			}
		case ']', '}':
			if depth == enclosingDepth {
				return true
			}
			depth--
		case ',':
			if depth == enclosingDepth {
				return true
			}
		}
		c.pos++
	}

	return false
}

func readMetadataArray[T any](c *metadataJSONCursor, entry func(*metadataJSONCursor) (T, bool)) ([]T, bool) {
	if !c.take('[') {
		return nil, false
	}
	if c.take(']') {
		return []T{}, true
	}
	var values []T
	for {
		start := c.pos
		value, ok := entry(c)
		if !ok {
			// A single entry can use the general decoder without slowing down
			// the rest of the array.
			c.pos = start
			var decoded T
			if !c.skipValue(metadataHistoryEntryDepth) || json.Unmarshal(c.data[start:c.pos], &decoded) != nil {
				return nil, false
			}
			value = decoded
		}
		done := c.take(']')
		// Reserve capacity only once we know the history has multiple entries.
		if values == nil && !done {
			values = make([]T, 0, 64)
		}
		values = append(values, value)
		if done {
			return values, true
		}
		if !c.take(',') {
			return nil, false
		}
	}
}

func readEagerSnapshot(c *metadataJSONCursor) (Snapshot, bool) {
	var snapshot Snapshot
	if !c.take('{') {
		return snapshot, false
	}

	const required = 1 | 2
	seen := 0
	for {
		key, ok := c.objectField()
		if !ok {
			return snapshot, false
		}
		var field int
		switch string(key) {
		case "snapshot-id":
			field = 1
			snapshot.SnapshotID, ok = c.integer()
		case "timestamp-ms":
			field = 2
			snapshot.TimestampMs, ok = c.integer()
		case "sequence-number":
			field = 4
			snapshot.SequenceNumber, ok = c.integer()
		case "parent-snapshot-id":
			field = 8
			var parent int64
			parent, ok = c.integer()
			snapshot.ParentSnapshotID = &parent
		case "manifest-list":
			field = 16
			var value []byte
			value, ok = c.stringBytes()
			snapshot.ManifestList = string(value)
		case "schema-id":
			field = 32
			var schemaID int64
			schemaID, ok = c.integer()
			id := int(schemaID)
			ok = ok && int64(id) == schemaID
			snapshot.SchemaID = &id
		case "summary":
			field = 64
			snapshot.Summary, ok = readEagerSummary(c)
		default:
			return snapshot, false
		}
		if !ok || seen&field != 0 {
			return snapshot, false
		}
		seen |= field
		if c.take('}') {
			return snapshot, seen&required == required
		}
		if !c.take(',') {
			return snapshot, false
		}
	}
}

func readEagerSummary(c *metadataJSONCursor) (*Summary, bool) {
	if !c.take('{') {
		return nil, false
	}
	// Keep only decoded property bytes. The buffer belongs to this summary.
	var text strings.Builder
	var props map[string]string
	var operation string
	var operationSeen bool
	if !c.take('}') {
		for {
			key, ok := c.objectField()
			if !ok {
				return nil, false
			}
			value, ok := c.stringBytes()
			if !ok {
				return nil, false
			}
			if string(key) == operationKey {
				operation, operationSeen = string(value), true
			} else {
				if props == nil {
					props = make(map[string]string, 14)
					text.Grow(384)
				}
				start := text.Len()
				text.Write(key)
				middle := text.Len()
				text.Write(value)
				owned := text.String()
				props[owned[start:middle]] = owned[middle:]
			}
			if c.take('}') {
				break
			}
			if !c.take(',') {
				return nil, false
			}
		}
	}
	if props == nil {
		props = map[string]string{}
	}
	var summary Summary
	if err := summary.setJSONProperties(props, operation, operationSeen); err != nil {
		return nil, false
	}

	return &summary, true
}

func readEagerLogArray[T any](c *metadataJSONCursor, read func(*metadataJSONCursor) (T, bool)) ([]T, bool) {
	c.pos = skipJSONSpace(c.data, c.pos)
	if bytes.HasPrefix(c.data[c.pos:], []byte("null")) {
		c.pos += len("null")

		return nil, true
	}

	return readMetadataArray(c, read)
}

func readEagerSnapshotLog(c *metadataJSONCursor) (SnapshotLogEntry, bool) {
	var entry SnapshotLogEntry
	ok := readEagerLogEntry(c, "snapshot-id", func(c *metadataJSONCursor) bool {
		var ok bool
		entry.SnapshotID, ok = c.integer()

		return ok
	}, &entry.TimestampMs)

	return entry, ok
}

func readEagerMetadataLog(c *metadataJSONCursor) (MetadataLogEntry, bool) {
	var entry MetadataLogEntry
	ok := readEagerLogEntry(c, "metadata-file", func(c *metadataJSONCursor) bool {
		var ok bool
		var value []byte
		value, ok = c.stringBytes()
		entry.MetadataFile = string(value)

		return ok
	}, &entry.TimestampMs)

	return entry, ok
}

func readEagerLogEntry(c *metadataJSONCursor, field string, read func(*metadataJSONCursor) bool, timestamp *int64) bool {
	if !c.take('{') {
		return false
	}
	seen := 0
	for range 2 {
		key, ok := c.objectField()
		if !ok {
			return false
		}
		switch string(key) {
		case field:
			if seen&1 != 0 || !read(c) {
				return false
			}
			seen |= 1
		case "timestamp-ms":
			if seen&2 != 0 {
				return false
			}
			if *timestamp, ok = c.integer(); !ok {
				return false
			}
			seen |= 2
		default:
			return false
		}
		if seen == 3 {
			return c.take('}')
		}
		if !c.take(',') {
			return false
		}
	}

	return false
}
