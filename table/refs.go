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
	"reflect"
)

const MainBranch = "main"

// RefType will be either a BranchRef or a TagRef
type RefType string

const (
	BranchRef RefType = "branch"
	TagRef    RefType = "tag"
)

var ErrInvalidRefType = errors.New("invalid snapshot ref type, should be 'branch' or 'tag'")

// SnapshotRef represents the reference information for a specific snapshot
type SnapshotRef struct {
	SnapshotID         int64   `json:"snapshot-id"`
	SnapshotRefType    RefType `json:"type"`
	MinSnapshotsToKeep *int    `json:"min-snapshots-to-keep,omitempty"`
	MaxSnapshotAgeMs   *int64  `json:"max-snapshot-age-ms,omitempty"`
	MaxRefAgeMs        *int64  `json:"max-ref-age-ms,omitempty"`
}

func (s *SnapshotRef) Equals(rhs SnapshotRef) bool {
	return reflect.DeepEqual(s, &rhs)
}

func (s *SnapshotRef) UnmarshalJSON(b []byte) error {
	type Alias SnapshotRef
	aux := (*Alias)(s)

	if err := json.Unmarshal(b, aux); err != nil {
		return err
	}

	switch s.SnapshotRefType {
	case BranchRef, TagRef:
	default:
		return ErrInvalidRefType
	}

	return nil
}
