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
	"encoding/json"
	"errors"
	"fmt"
	"strconv"

	"github.com/apache/iceberg-go"
	"github.com/apache/iceberg-go/io"
	"golang.org/x/exp/maps"
)

type Operation string

const (
	OpAppend    Operation = "append"
	OpReplace   Operation = "replace"
	OpOverwrite Operation = "overwrite"
	OpDelete    Operation = "delete"
)

var (
	ErrInvalidOperation = errors.New("invalid operation value")
	ErrMissingOperation = errors.New("missing operation key")
)

func ValidOperation(s string) (Operation, error) {
	switch s {
	case "append", "replace", "overwrite", "delete":
		return Operation(s), nil
	}
	return "", fmt.Errorf("%w: found '%s'", ErrInvalidOperation, s)
}

const operationKey = "operation"

type Summary struct {
	Operation  Operation
	Properties map[string]string
}

func (s *Summary) Equals(other *Summary) bool {
	if s == other {
		return true
	}

	if s != nil && other == nil {
		return false
	}

	if s.Operation != other.Operation {
		return false
	}

	if len(s.Properties) == 0 && len(other.Properties) == 0 {
		return true
	}

	return maps.Equal(s.Properties, other.Properties)
}

func (s *Summary) UnmarshalJSON(b []byte) (err error) {
	alias := map[string]string{}
	if err = json.Unmarshal(b, &alias); err != nil {
		return
	}

	op, ok := alias[operationKey]
	if !ok {
		return ErrMissingOperation
	}

	if s.Operation, err = ValidOperation(op); err != nil {
		return
	}

	delete(alias, operationKey)
	s.Properties = alias
	return nil
}

func (s *Summary) MarshalJSON() ([]byte, error) {
	props := maps.Clone(s.Properties)
	if s.Operation != "" {
		if props == nil {
			props = make(map[string]string)
		}
		props[operationKey] = string(s.Operation)
	}

	return json.Marshal(props)
}

type Snapshot struct {
	SnapshotID       int64    `json:"snapshot-id"`
	ParentSnapshotID *int64   `json:"parent-snapshot-id,omitempty"`
	SequenceNumber   int      `json:"sequence-number"`
	TimestampMs      int      `json:"timestamp-ms"`
	ManifestList     string   `json:"manifest-list,omitempty"`
	Summary          *Summary `json:"summary,omitempty"`
	SchemaID         *int     `json:"schema-id,omitempty"`
}

func (s Snapshot) String() string {
	var (
		op, parent, schema string
	)

	if s.Summary != nil {
		op = string(s.Summary.Operation) + ": "
	}
	if s.ParentSnapshotID != nil {
		parent = ", parent_id=" + strconv.FormatInt(*s.ParentSnapshotID, 10)
	}
	if s.SchemaID != nil {
		schema = ", schema_id=" + strconv.Itoa(*s.SchemaID)
	}
	return fmt.Sprintf("%sid=%d%s%s", op, s.SnapshotID, parent, schema)
}

func (s Snapshot) Equals(other Snapshot) bool {
	switch {
	case s.ParentSnapshotID == nil && other.ParentSnapshotID != nil:
		fallthrough
	case s.ParentSnapshotID != nil && other.ParentSnapshotID == nil:
		fallthrough
	case s.SchemaID == nil && other.SchemaID != nil:
		fallthrough
	case s.SchemaID != nil && other.SchemaID == nil:
		return false
	}

	return s.SnapshotID == other.SnapshotID &&
		((s.ParentSnapshotID == other.ParentSnapshotID) || (*s.ParentSnapshotID == *other.ParentSnapshotID)) &&
		((s.SchemaID == other.SchemaID) || (*s.SchemaID == *other.SchemaID)) &&
		s.SequenceNumber == other.SequenceNumber &&
		s.TimestampMs == other.TimestampMs &&
		s.ManifestList == other.ManifestList &&
		s.Summary.Equals(other.Summary)
}

func (s Snapshot) Manifests(fio io.IO) ([]iceberg.ManifestFile, error) {
	if s.ManifestList != "" {
		f, err := fio.Open(s.ManifestList)
		if err != nil {
			return nil, fmt.Errorf("could not open manifest file: %w", err)
		}
		defer f.Close()
		return iceberg.ReadManifestList(f)
	}

	return nil, nil
}

type MetadataLogEntry struct {
	MetadataFile string `json:"metadata-file"`
	TimestampMs  int    `json:"timestamp-ms"`
}

type SnapshotLogEntry struct {
	SnapshotID  int64 `json:"snapshot-id"`
	TimestampMs int   `json:"timestamp-ms"`
}
