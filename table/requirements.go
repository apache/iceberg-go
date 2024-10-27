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
	"fmt"

	"github.com/google/uuid"
)

// A Requirement is a validation rule that must be satisfied before attempting to
// make and commit changes to a table. Requirements are used to ensure that the
// table is in a valid state before making changes.
type Requirement interface {
	// Validate checks that the current table metadata satisfies the requirement.
	Validate(Metadata) error
}

// baseRequirement is a common struct that all requirements embed. It is used to
// identify the type of the requirement.
type baseRequirement struct {
	Type string `json:"type"`
}

type assertCreate struct {
	baseRequirement
}

// AssertCreate creates a requirement that the table does not already exist.
func AssertCreate() Requirement {
	return &assertCreate{
		baseRequirement: baseRequirement{Type: "assert-create"},
	}
}

func (a *assertCreate) Validate(meta Metadata) error {
	if meta != nil {
		return fmt.Errorf("Table already exists")
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
		baseRequirement: baseRequirement{Type: "assert-table-uuid"},
		UUID:            uuid,
	}
}

func (a *assertTableUuid) Validate(meta Metadata) error {
	if meta == nil {
		return fmt.Errorf("requirement failed: current table metadata does not exist")
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
		baseRequirement: baseRequirement{Type: "assert-ref-snapshot-id"},
		Ref:             ref,
		SnapshotID:      id,
	}
}

func (a *assertRefSnapshotID) Validate(meta Metadata) error {
	if meta == nil {
		return fmt.Errorf("requirement failed: current table metadata does not exist")
	}

	var r *SnapshotRef
	for name, ref := range meta.Refs() {
		if name == a.Ref {
			r = &ref
			break
		}
	}
	if r == nil {
		return fmt.Errorf("requirement failed: branch or tag %s is missing, expected %d", a.Ref, a.SnapshotID)
	}

	if a.SnapshotID == nil {
		return fmt.Errorf("requirement failed: %s %s was created concurrently", r.SnapshotRefType, a.Ref)
	}

	if r.SnapshotID != *a.SnapshotID {
		return fmt.Errorf("requirement failed: %s %s has changed: expected id %d, found %d", r.SnapshotRefType, a.Ref, a.SnapshotID, r.SnapshotID)
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
		baseRequirement:     baseRequirement{Type: "assert-last-assigned-field-id"},
		LastAssignedFieldID: id,
	}
}

func (a *assertLastAssignedFieldId) Validate(meta Metadata) error {
	if meta == nil {
		return fmt.Errorf("requirement failed: current table metadata does not exist")
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
		baseRequirement: baseRequirement{Type: "assert-current-schema-id"},
		CurrentSchemaID: id,
	}
}

func (a *assertCurrentSchemaId) Validate(meta Metadata) error {
	if meta == nil {
		return fmt.Errorf("requirement failed: current table metadata does not exist")
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
		baseRequirement:         baseRequirement{Type: "assert-last-assigned-partition-id"},
		LastAssignedPartitionID: id,
	}
}

func (a *assertLastAssignedPartitionId) Validate(meta Metadata) error {
	if meta == nil {
		return fmt.Errorf("requirement failed: current table metadata does not exist")
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
		baseRequirement: baseRequirement{Type: "assert-default-spec-id"},
		DefaultSpecID:   id,
	}
}

func (a *assertDefaultSpecId) Validate(meta Metadata) error {
	if meta == nil {
		return fmt.Errorf("requirement failed: current table metadata does not exist")
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
		baseRequirement:    baseRequirement{Type: "assert-default-sort-order-id"},
		DefaultSortOrderID: id,
	}
}

func (a *assertDefaultSortOrderId) Validate(meta Metadata) error {
	if meta == nil {
		return fmt.Errorf("requirement failed: current table metadata does not exist")
	}

	if meta.DefaultSortOrder() != a.DefaultSortOrderID {
		return fmt.Errorf("requirement failed: default sort order id has changed: expected %d, found %d", a.DefaultSortOrderID, meta.DefaultSortOrder())
	}

	return nil
}
