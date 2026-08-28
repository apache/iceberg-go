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
	"errors"
	"fmt"
	"strconv"
	"sync"
)

// deferredSnapshotState owns the raw snapshot array and the materialized
// collection. It is pointer-owned because commonMetadata is copied by format
// upgrades and builders; copying a used sync.Once would be unsafe.
type deferredSnapshotState struct {
	raw     json.RawMessage
	entries []deferredSnapshotEntry
	byID    map[int64]int

	allOnce   sync.Once
	mu        sync.RWMutex
	snapshots []Snapshot
	index     *snapshotIndexData
	err       error
}

type deferredSnapshotEntry struct {
	start int
	end   int

	once     sync.Once
	snapshot Snapshot
	err      error
}

func (s *deferredSnapshotState) load() ([]Snapshot, *snapshotIndexData, error) {
	s.allOnce.Do(func() {
		s.mu.RLock()
		raw := s.raw
		s.mu.RUnlock()

		var snapshots []Snapshot
		if err := json.Unmarshal(raw, &snapshots); err != nil {
			s.mu.Lock()
			s.err = fmt.Errorf("%w: deferred snapshots: %w", ErrInvalidMetadata, err)
			s.mu.Unlock()

			return
		}

		s.mu.Lock()
		s.snapshots = snapshots
		s.index = buildSnapshotIndex(snapshots)
		s.raw = nil
		s.entries = nil
		s.byID = nil
		s.mu.Unlock()
	})

	s.mu.RLock()
	defer s.mu.RUnlock()

	return s.snapshots, s.index, s.err
}

func (s *deferredSnapshotState) snapshotByID(id int64) (*Snapshot, error) {
	s.mu.RLock()
	if s.err != nil {
		err := s.err
		s.mu.RUnlock()

		return nil, err
	}
	if s.snapshots != nil {
		i, ok := snapshotIndexPosition(s.index, s.snapshots, id)
		if !ok {
			s.mu.RUnlock()

			return nil, nil
		}
		snapshot := cloneSnapshotPtr(&s.snapshots[i])
		s.mu.RUnlock()

		return snapshot, nil
	}
	entryIndex, ok := s.byID[id]
	s.mu.RUnlock()
	if !ok {
		return nil, nil
	}
	entry := &s.entries[entryIndex]

	entry.once.Do(func() {
		s.mu.RLock()
		if s.snapshots != nil {
			i, found := snapshotIndexPosition(s.index, s.snapshots, id)
			if found {
				entry.snapshot = s.snapshots[i]
			}
			s.mu.RUnlock()

			return
		}
		raw := s.raw[entry.start:entry.end]
		s.mu.RUnlock()

		if err := json.Unmarshal(raw, &entry.snapshot); err != nil {
			entry.err = fmt.Errorf("%w: deferred snapshot %d: %w", ErrInvalidMetadata, id, err)
		}
	})
	if entry.err != nil {
		return nil, entry.err
	}

	return cloneSnapshotPtr(&entry.snapshot), nil
}

type deferredSnapshotFields struct {
	SnapshotID        *int64          `json:"snapshot-id"`
	ParentSnapshotID  *int64          `json:"parent-snapshot-id,omitempty"`
	SequenceNumber    int64           `json:"sequence-number"`
	TimestampMs       *int64          `json:"timestamp-ms"`
	ManifestList      string          `json:"manifest-list,omitempty"`
	ManifestLocations json.RawMessage `json:"manifests,omitempty"`
	Summary           json.RawMessage `json:"summary,omitempty"`
	SchemaID          *int            `json:"schema-id,omitempty"`
	FirstRowID        *int64          `json:"first-row-id,omitempty"`
	AddedRows         *int64          `json:"added-rows,omitempty"`
}

func (s deferredSnapshotFields) validateHeavyFields() error {
	if err := validateStringArray(s.ManifestLocations); err != nil {
		return err
	}

	return validateSummaryObject(s.Summary)
}

func validateStringArray(raw json.RawMessage) error {
	raw = bytes.TrimSpace(raw)
	if len(raw) == 0 || bytes.Equal(raw, []byte("null")) {
		return nil
	}
	if raw[0] != '[' || raw[len(raw)-1] != ']' {
		return errors.New("cannot unmarshal manifests into []string")
	}
	for i := 1; i < len(raw)-1; {
		i = skipJSONSpace(raw, i)
		if i == len(raw)-1 {
			return nil
		}
		if raw[i] != '"' {
			return errors.New("cannot unmarshal manifest location into string")
		}
		i = scanJSONString(raw, i)
		i = skipJSONSpace(raw, i)
		if i < len(raw)-1 && raw[i] == ',' {
			i++

			continue
		}
		if i != len(raw)-1 {
			return errors.New("invalid manifests array")
		}
	}

	return nil
}

func validateSummaryObject(raw json.RawMessage) error {
	raw = bytes.TrimSpace(raw)
	if len(raw) == 0 || bytes.Equal(raw, []byte("null")) {
		return nil
	}
	if raw[0] != '{' || raw[len(raw)-1] != '}' {
		return errors.New("cannot unmarshal summary into map[string]string")
	}
	for i := 1; i < len(raw)-1; {
		i = skipJSONSpace(raw, i)
		if i == len(raw)-1 {
			return nil
		}
		if raw[i] != '"' {
			return errors.New("invalid summary key")
		}
		keyStart := i
		i = scanJSONString(raw, i)
		key := raw[keyStart:i]
		i = skipJSONSpace(raw, i)
		if i >= len(raw)-1 || raw[i] != ':' {
			return errors.New("invalid summary object")
		}
		i = skipJSONSpace(raw, i+1)
		if i >= len(raw)-1 || raw[i] != '"' {
			return errors.New("cannot unmarshal summary value into string")
		}
		valueStart := i
		i = scanJSONString(raw, i)
		if summaryKeyIsOperation(key) && i == valueStart+2 {
			return fmt.Errorf("%w: found empty operation", ErrInvalidOperation)
		}
		i = skipJSONSpace(raw, i)
		if i < len(raw)-1 && raw[i] == ',' {
			i++

			continue
		}
		if i != len(raw)-1 {
			return errors.New("invalid summary object")
		}
	}

	return nil
}

func summaryKeyIsOperation(raw []byte) bool {
	if bytes.Equal(raw, []byte(`"operation"`)) {
		return true
	}
	if !bytes.Contains(raw, []byte{'\\'}) {
		return false
	}
	decoded, err := strconv.Unquote(string(raw))

	return err == nil && decoded == operationKey
}

func skipJSONSpace(raw []byte, i int) int {
	for i < len(raw) {
		switch raw[i] {
		case ' ', '\t', '\n', '\r':
			i++
		default:
			return i
		}
	}

	return i
}

func scanJSONString(raw []byte, i int) int {
	for i++; i < len(raw); i++ {
		switch raw[i] {
		case '\\':
			i++
		case '"':
			return i + 1
		}
	}

	return len(raw)
}

type rawJSONSpan struct {
	start int
	end   int
}

func splitJSONArray(raw []byte) ([]rawJSONSpan, error) {
	raw = bytes.TrimSpace(raw)
	if len(raw) < 2 || raw[0] != '[' || raw[len(raw)-1] != ']' {
		return nil, errors.New("expected JSON array")
	}

	elements := make([]rawJSONSpan, 0)
	start, depth := 1, 0
	for i := 1; i < len(raw)-1; i++ {
		switch raw[i] {
		case '"':
			i = scanJSONString(raw, i) - 1
		case '{', '[':
			depth++
		case '}', ']':
			depth--
		case ',':
			if depth == 0 {
				elementStart, elementEnd := trimJSONSpan(raw, start, i)
				elements = append(elements, rawJSONSpan{start: elementStart, end: elementEnd})
				start = i + 1
			}
		}
	}
	elementStart, elementEnd := trimJSONSpan(raw, start, len(raw)-1)
	if elementStart < elementEnd {
		elements = append(elements, rawJSONSpan{start: elementStart, end: elementEnd})
	}

	return elements, nil
}

func trimJSONSpan(raw []byte, start, end int) (int, int) {
	start = skipJSONSpace(raw, start)
	for end > start {
		switch raw[end-1] {
		case ' ', '\t', '\n', '\r':
			end--
		default:
			return start, end
		}
	}

	return start, end
}

func parseNormalizedMetadataBytesDeferredSnapshots(normalized []byte, formatVersion int) (Metadata, error) {
	metadata, rawSnapshots, err := decodeMetadataWithRawSnapshots(normalized, formatVersion)
	if err != nil {
		if errors.Is(err, ErrInvalidMetadata) {
			return nil, err
		}

		return nil, fmt.Errorf("%w: %w", ErrInvalidMetadata, err)
	}

	common := commonMetadataOf(metadata)
	deferred, eager, err := prepareDeferredSnapshots(rawSnapshots, common, metadata)
	if err != nil {
		return nil, err
	}
	common.SnapshotList = eager

	if err := finishDeferredMetadataUnmarshal(metadata); err != nil {
		return nil, err
	}
	if deferred == nil {
		return metadata, nil
	}
	common.deferredSnapshots = deferred

	return metadata, nil
}

func decodeMetadataWithRawSnapshots(normalized []byte, formatVersion int) (Metadata, json.RawMessage, error) {
	var rawSnapshots json.RawMessage
	switch formatVersion {
	case 1:
		next := initMetadataV1Deser()
		type alias metadataV1
		aux := struct {
			*alias
			SnapshotList json.RawMessage `json:"snapshots"`
		}{alias: (*alias)(next)}
		if err := json.Unmarshal(normalized, &aux); err != nil {
			return nil, nil, err
		}
		rawSnapshots = aux.SnapshotList

		return next, rawSnapshots, nil
	case 2:
		next := initMetadataV2Deser()
		type alias metadataV2
		aux := struct {
			*alias
			SnapshotList json.RawMessage `json:"snapshots"`
		}{alias: (*alias)(next)}
		if err := json.Unmarshal(normalized, &aux); err != nil {
			return nil, nil, err
		}
		rawSnapshots = aux.SnapshotList

		return next, rawSnapshots, nil
	case 3:
		next := initMetadataV3Deser()
		type alias metadataV3
		aux := struct {
			*alias
			SnapshotList json.RawMessage `json:"snapshots"`
		}{alias: (*alias)(next)}
		if err := json.Unmarshal(normalized, &aux); err != nil {
			return nil, nil, err
		}
		rawSnapshots = aux.SnapshotList

		return next, rawSnapshots, nil
	default:
		return nil, nil, ErrInvalidMetadataFormatVersion
	}
}

func finishDeferredMetadataUnmarshal(metadata Metadata) error {
	switch typed := metadata.(type) {
	case *metadataV1:
		return typed.finishUnmarshal()
	case *metadataV2:
		return typed.finishUnmarshal()
	case *metadataV3:
		return typed.finishUnmarshal()
	default:
		return fmt.Errorf("%w: unsupported metadata implementation %T", ErrInvalidMetadata, metadata)
	}
}

func prepareDeferredSnapshots(rawSnapshots json.RawMessage, common *commonMetadata, metadata Metadata) (*deferredSnapshotState, []Snapshot, error) {
	rawSnapshots = bytes.TrimSpace(rawSnapshots)
	if len(rawSnapshots) == 0 || bytes.Equal(rawSnapshots, []byte("null")) {
		return nil, nil, nil
	}

	rawEntries, err := splitJSONArray(rawSnapshots)
	if err != nil {
		return nil, nil, fmt.Errorf("%w: snapshots: %w", ErrInvalidMetadata, err)
	}
	if len(rawEntries) == 0 {
		return nil, []Snapshot{}, nil
	}

	refIDs := make(map[int64]struct{})
	for _, ref := range common.SnapshotRefs {
		refIDs[ref.SnapshotID] = struct{}{}
	}
	if common.CurrentSnapshotID != nil && *common.CurrentSnapshotID != -1 {
		refIDs[*common.CurrentSnapshotID] = struct{}{}
	}

	eager := make([]Snapshot, 0, len(refIDs))
	seen := make(map[int64]struct{}, len(rawEntries))
	state := &deferredSnapshotState{
		raw:     rawSnapshots,
		entries: make([]deferredSnapshotEntry, len(rawEntries)),
		byID:    make(map[int64]int, len(rawEntries)),
	}
	for i, span := range rawEntries {
		raw := rawSnapshots[span.start:span.end]
		var fields deferredSnapshotFields
		if err := json.Unmarshal(raw, &fields); err != nil {
			return nil, nil, fmt.Errorf("%w: snapshot at index %d: %w", ErrInvalidMetadata, i, err)
		}
		if fields.SnapshotID == nil {
			return nil, nil, fmt.Errorf("%w: snapshot-id is absent or null", ErrInvalidMetadata)
		}
		if fields.TimestampMs == nil {
			return nil, nil, fmt.Errorf("%w: timestamp-ms is absent or null", ErrInvalidMetadata)
		}
		if _, ok := seen[*fields.SnapshotID]; ok {
			return nil, nil, fmt.Errorf("%w: duplicate snapshot ID %d", ErrInvalidMetadata, *fields.SnapshotID)
		}
		seen[*fields.SnapshotID] = struct{}{}
		state.entries[i] = deferredSnapshotEntry{start: span.start, end: span.end}
		state.byID[*fields.SnapshotID] = i

		if err := fields.validateHeavyFields(); err != nil {
			return nil, nil, fmt.Errorf("%w: snapshot %d: %w", ErrInvalidMetadata, *fields.SnapshotID, err)
		}
		if metadata.Version() > 1 && fields.SequenceNumber > metadata.LastSequenceNumber() {
			return nil, nil, fmt.Errorf("%w: snapshot %d has sequence number %d which is greater than last-sequence-number %d",
				ErrInvalidMetadata, *fields.SnapshotID, fields.SequenceNumber, metadata.LastSequenceNumber())
		}
		if metadata.Version() >= 3 && fields.FirstRowID != nil && *fields.FirstRowID < 0 {
			return nil, nil, fmt.Errorf("%w: snapshot %d has invalid first-row-id %d",
				ErrInvalidMetadata, *fields.SnapshotID, *fields.FirstRowID)
		}

		if _, ok := refIDs[*fields.SnapshotID]; ok {
			var snapshot Snapshot
			if err := json.Unmarshal(raw, &snapshot); err != nil {
				return nil, nil, fmt.Errorf("%w: snapshot %d: %w", ErrInvalidMetadata, *fields.SnapshotID, err)
			}
			eager = append(eager, snapshot)
		}
	}

	return state, eager, nil
}

func commonMetadataOf(metadata Metadata) *commonMetadata {
	switch typed := metadata.(type) {
	case *metadataV1:
		return &typed.commonMetadata
	case *metadataV2:
		return &typed.commonMetadata
	case *metadataV3:
		return &typed.commonMetadata
	default:
		return nil
	}
}
