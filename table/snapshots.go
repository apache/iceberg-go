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
	"encoding/json"
	"errors"
	"fmt"
	"strconv"

	"github.com/apache/iceberg-go"
	"github.com/thanos-io/objstore"
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

// ValidOperation ensures that a given string is one of the valid operation
// types: append,replace,overwrite,delete
func ValidOperation(s string) (Operation, error) {
	switch s {
	case "append", "replace", "overwrite", "delete":
		return Operation(s), nil
	}
	return "", fmt.Errorf("%w: found '%s'", ErrInvalidOperation, s)
}

const operationKey = "operation"

// Summary stores the summary information for a snapshot indicating
// the operation that created the snapshot, and various properties
// which might exist in the summary.
type Summary struct {
	Operation  Operation
	Properties map[string]string
}

func (s *Summary) String() string {
	out := string(s.Operation)
	if s.Properties != nil {
		data, _ := json.Marshal(s.Properties)
		out += ", " + string(data)
	}
	return out
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
	SequenceNumber   int64    `json:"sequence-number"`
	TimestampMs      int64    `json:"timestamp-ms"`
	ManifestList     string   `json:"manifest-list,omitempty"`
	Summary          *Summary `json:"summary,omitempty"`
	SchemaID         *int     `json:"schema-id,omitempty"`
}

func (s Snapshot) String() string {
	var (
		op, parent, schema string
	)

	if s.Summary != nil {
		op = s.Summary.String() + ": "
	}
	if s.ParentSnapshotID != nil {
		parent = ", parent_id=" + strconv.FormatInt(*s.ParentSnapshotID, 10)
	}
	if s.SchemaID != nil {
		schema = ", schema_id=" + strconv.Itoa(*s.SchemaID)
	}
	return fmt.Sprintf("%sid=%d%s%s, sequence_number=%d, timestamp_ms=%d, manifest_list=%s",
		op, s.SnapshotID, parent, schema, s.SequenceNumber, s.TimestampMs, s.ManifestList)
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

func (s Snapshot) Manifests(bucket objstore.Bucket) ([]iceberg.ManifestFile, error) {
	if s.ManifestList != "" {
		f, err := bucket.Get(context.TODO(), s.ManifestList)
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
	TimestampMs  int64  `json:"timestamp-ms"`
}

type SnapshotLogEntry struct {
	SnapshotID  int64 `json:"snapshot-id"`
	TimestampMs int64 `json:"timestamp-ms"`
}
