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
	"io"

	"github.com/google/uuid"
)

const (
	reqAssertCreate                  = "assert-create"
	reqAssertTableUUID               = "assert-table-uuid"
	reqAssertRefSnapshotID           = "assert-ref-snapshot-id"
	reqAssertDefaultSpecID           = "assert-default-spec-id"
	reqAssertCurrentSchemaID         = "assert-current-schema-id"
	reqAssertDefaultSortOrderID      = "assert-default-sort-order-id"
	reqAssertLastAssignedFieldID     = "assert-last-assigned-field-id"
	reqAssertLastAssignedPartitionID = "assert-last-assigned-partition-id"
)

var ErrInvalidRequirement = errors.New("invalid requirement")

// A Requirement is a validation rule that must be satisfied before attempting to
// make and commit changes to a table. Requirements are used to ensure that the
// table is in a valid state before making changes.
type Requirement interface {
	// Validate checks that the current table metadata satisfies the requirement.
	Validate(Metadata) error
	GetType() string
}

// baseRequirement is a common struct that all requirements embed. It is used to
// identify the type of the requirement.
type baseRequirement struct {
	Type string `json:"type"`
}

func (b baseRequirement) GetType() string {
	return b.Type
}

type assertCreate struct {
	baseRequirement
}

// AssertCreate creates a requirement that the table does not already exist.
func AssertCreate() Requirement {
	return &assertCreate{
		baseRequirement: baseRequirement{Type: reqAssertCreate},
	}
}

func (a *assertCreate) Validate(meta Metadata) error {
	if meta != nil {
		return errors.New("Table already exists")
	}

	return nil
}

type assertTableUuid struct {
	baseRequirement
	UUID uuid.UUID `json:"uuid"`
}

// AssertTableUUID creates a requirement that the table UUID matches the given UUID.
func AssertTableUUID(uuid uuid.UUID) Requirement {
	return &assertTableUuid{
		baseRequirement: baseRequirement{Type: reqAssertTableUUID},
		UUID:            uuid,
	}
}

func (a *assertTableUuid) Validate(meta Metadata) error {
	if meta == nil {
		return errors.New("requirement failed: current table metadata does not exist")
	}

	if meta.TableUUID() != a.UUID {
		return fmt.Errorf("UUID mismatch: %s != %s", meta.TableUUID(), a.UUID)
	}

	return nil
}

type assertRefSnapshotID struct {
	baseRequirement
	Ref        string `json:"ref"`
	SnapshotID *int64 `json:"snapshot-id"`
}

// AssertRefSnapshotID creates a requirement which ensures that the table branch
// or tag identified by the given ref must reference the given snapshot id.
// If the id is nil, the ref must not already exist.
func AssertRefSnapshotID(ref string, id *int64) Requirement {
	return &assertRefSnapshotID{
		baseRequirement: baseRequirement{Type: reqAssertRefSnapshotID},
		Ref:             ref,
		SnapshotID:      id,
	}
}

func (a *assertRefSnapshotID) Validate(meta Metadata) error {
	if meta == nil {
		return errors.New("requirement failed: current table metadata does not exist")
	}

	var r *SnapshotRef
	for name, ref := range meta.Refs() {
		if name == a.Ref {
			r = &ref

			break
		}
	}

	if r != nil {
		if a.SnapshotID == nil {
			return fmt.Errorf("requirement failed: %s %s was created concurrently", r.SnapshotRefType, a.Ref)
		}

		if r.SnapshotID != *a.SnapshotID {
			return fmt.Errorf("requirement failed: %s %s has changed: expected id %d, found %d", r.SnapshotRefType, a.Ref, a.SnapshotID, r.SnapshotID)
		}
	} else if a.SnapshotID != nil {
		return fmt.Errorf("requirement failed: branch or tag %s is missing, expected %d", a.Ref, a.SnapshotID)
	}

	return nil
}

type assertLastAssignedFieldId struct {
	baseRequirement
	LastAssignedFieldID int `json:"last-assigned-field-id"`
}

// AssertLastAssignedFieldID validates that the table's last assigned column ID
// matches the given id.
func AssertLastAssignedFieldID(id int) Requirement {
	return &assertLastAssignedFieldId{
		baseRequirement:     baseRequirement{Type: reqAssertLastAssignedFieldID},
		LastAssignedFieldID: id,
	}
}

func (a *assertLastAssignedFieldId) Validate(meta Metadata) error {
	if meta == nil {
		return errors.New("requirement failed: current table metadata does not exist")
	}

	if meta.LastColumnID() != a.LastAssignedFieldID {
		return fmt.Errorf("requirement failed: last assigned field id has changed: expected %d, found %d", a.LastAssignedFieldID, meta.LastColumnID())
	}

	return nil
}

type assertCurrentSchemaId struct {
	baseRequirement
	CurrentSchemaID int `json:"current-schema-id"`
}

// AssertCurrentSchemaId creates a requirement that the table's current schema ID
// matches the given id.
func AssertCurrentSchemaID(id int) Requirement {
	return &assertCurrentSchemaId{
		baseRequirement: baseRequirement{Type: reqAssertCurrentSchemaID},
		CurrentSchemaID: id,
	}
}

func (a *assertCurrentSchemaId) Validate(meta Metadata) error {
	if meta == nil {
		return errors.New("requirement failed: current table metadata does not exist")
	}

	if meta.CurrentSchema().ID != a.CurrentSchemaID {
		return fmt.Errorf("requirement failed: current schema id has changed: expected %d, found %d", a.CurrentSchemaID, meta.CurrentSchema().ID)
	}

	return nil
}

type assertLastAssignedPartitionId struct {
	baseRequirement
	LastAssignedPartitionID int `json:"last-assigned-partition-id"`
}

// AssertLastAssignedPartitionID creates a requriement that the table's last assigned partition ID
// matches the given id.
func AssertLastAssignedPartitionID(id int) Requirement {
	return &assertLastAssignedPartitionId{
		baseRequirement:         baseRequirement{Type: reqAssertLastAssignedPartitionID},
		LastAssignedPartitionID: id,
	}
}

func (a *assertLastAssignedPartitionId) Validate(meta Metadata) error {
	if meta == nil {
		return errors.New("requirement failed: current table metadata does not exist")
	}

	if *meta.LastPartitionSpecID() != a.LastAssignedPartitionID {
		return fmt.Errorf("requirement failed: last assigned partition id has changed: expected %d, found %d", a.LastAssignedPartitionID, *meta.LastPartitionSpecID())
	}

	return nil
}

type assertDefaultSpecId struct {
	baseRequirement
	DefaultSpecID int `json:"default-spec-id"`
}

// AssertDefaultSpecID creates a requirement that the table's default partition spec ID
// matches the given id.
func AssertDefaultSpecID(id int) Requirement {
	return &assertDefaultSpecId{
		baseRequirement: baseRequirement{Type: reqAssertDefaultSpecID},
		DefaultSpecID:   id,
	}
}

func (a *assertDefaultSpecId) Validate(meta Metadata) error {
	if meta == nil {
		return errors.New("requirement failed: current table metadata does not exist")
	}

	if meta.DefaultPartitionSpec() != a.DefaultSpecID {
		return fmt.Errorf("requirement failed: default spec id has changed: expected %d, found %d", a.DefaultSpecID, meta.DefaultPartitionSpec())
	}

	return nil
}

type assertDefaultSortOrderId struct {
	baseRequirement
	DefaultSortOrderID int `json:"default-sort-order-id"`
}

// AssertDefaultSortOrderID creates a requirement that the table's default sort order ID
// matches the given id.
func AssertDefaultSortOrderID(id int) Requirement {
	return &assertDefaultSortOrderId{
		baseRequirement:    baseRequirement{Type: reqAssertDefaultSortOrderID},
		DefaultSortOrderID: id,
	}
}

func (a *assertDefaultSortOrderId) Validate(meta Metadata) error {
	if meta == nil {
		return errors.New("requirement failed: current table metadata does not exist")
	}

	if meta.DefaultSortOrder() != a.DefaultSortOrderID {
		return fmt.Errorf("requirement failed: default sort order id has changed: expected %d, found %d", a.DefaultSortOrderID, meta.DefaultSortOrder())
	}

	return nil
}

// ParseRequirement parses json data provided by the reader into a Requirement
func ParseRequirement(r io.Reader) (Requirement, error) {
	data, err := io.ReadAll(r)
	if err != nil {
		return nil, err
	}

	return ParseRequirementBytes(data)
}

// ParseRequirementString parses json string into a Requirement
func ParseRequirementString(s string) (Requirement, error) {
	return ParseRequirementBytes([]byte(s))
}

// ParseRequirementBytes parses json bytes into a Requirement
func ParseRequirementBytes(b []byte) (Requirement, error) {
	var base baseRequirement
	if err := json.Unmarshal(b, &base); err != nil {
		return nil, err
	}

	switch base.Type {
	case reqAssertCreate:
		return AssertCreate(), nil

	case reqAssertTableUUID:
		var req assertTableUuid
		if err := json.Unmarshal(b, &req); err != nil {
			return nil, err
		}

		return AssertTableUUID(req.UUID), nil

	case reqAssertRefSnapshotID:
		var req assertRefSnapshotID
		if err := json.Unmarshal(b, &req); err != nil {
			return nil, err
		}

		return AssertRefSnapshotID(req.Ref, req.SnapshotID), nil

	case reqAssertDefaultSpecID:
		var req assertDefaultSpecId
		if err := json.Unmarshal(b, &req); err != nil {
			return nil, err
		}

		return AssertDefaultSpecID(req.DefaultSpecID), nil

	case reqAssertCurrentSchemaID:
		var req assertCurrentSchemaId
		if err := json.Unmarshal(b, &req); err != nil {
			return nil, err
		}

		return AssertCurrentSchemaID(req.CurrentSchemaID), nil

	case reqAssertDefaultSortOrderID:
		var req assertDefaultSortOrderId
		if err := json.Unmarshal(b, &req); err != nil {
			return nil, err
		}

		return AssertDefaultSortOrderID(req.DefaultSortOrderID), nil

	case reqAssertLastAssignedFieldID:
		var req assertLastAssignedFieldId
		if err := json.Unmarshal(b, &req); err != nil {
			return nil, err
		}

		return AssertLastAssignedFieldID(req.LastAssignedFieldID), nil

	case reqAssertLastAssignedPartitionID:
		var req assertLastAssignedPartitionId
		if err := json.Unmarshal(b, &req); err != nil {
			return nil, err
		}

		return AssertLastAssignedPartitionID(req.LastAssignedPartitionID), nil
	}

	return nil, ErrInvalidRequirement
}
