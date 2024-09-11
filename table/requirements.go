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

type Requirement interface {
	Validate(Metadata) error
}

type baseRequirement struct {
	Type string `json:"type"`
}

type AssertCreate struct {
	baseRequirement
}

func NewAssertCreate() *AssertCreate {
	return &AssertCreate{
		baseRequirement: baseRequirement{Type: "assert-create"},
	}
}

func (a *AssertCreate) Validate(meta Metadata) error {
	if meta != nil {
		return fmt.Errorf("Table already exists")
	}

	return nil
}

type AssertTableUuid struct {
	baseRequirement
	UUID uuid.UUID `json:"uuid"`
}

func NewAssertTableUUID(uuid uuid.UUID) *AssertTableUuid {
	return &AssertTableUuid{
		baseRequirement: baseRequirement{Type: "assert-table-uuid"},
		UUID:            uuid,
	}
}

func (a *AssertTableUuid) Validate(meta Metadata) error {
	if meta == nil {
		return fmt.Errorf("requirement failed: current table metadata does not exist")
	}

	if meta.TableUUID() != a.UUID {
		return fmt.Errorf("UUID mismatch: %s != %s", meta.TableUUID(), a.UUID)
	}

	return nil
}

type AssertRefSnapshotID struct {
	baseRequirement
	Ref        string `json:"ref"`
	SnapshotID *int64 `json:"snapshot-id"`
}

func NewAssertRefSnapshotID(ref string, id *int64) *AssertRefSnapshotID {
	return &AssertRefSnapshotID{
		baseRequirement: baseRequirement{Type: "assert-ref-snapshot-id"},
		Ref:             ref,
		SnapshotID:      id,
	}
}

func (a *AssertRefSnapshotID) Validate(meta Metadata) error {
	if meta == nil {
		return fmt.Errorf("requirement failed: current table metadata does not exist")
	}

	ref, ok := meta.Refs()[a.Ref]
	if !ok {
		return fmt.Errorf("requirement failed: branch or tag %s is missing, expected %d", a.Ref, a.SnapshotID)
	}

	if a.SnapshotID == nil {
		return fmt.Errorf("requirement failed: %s %s was created concurrently", ref.SnapshotRefType, a.Ref)
	}

	if ref.SnapshotID != *a.SnapshotID {
		return fmt.Errorf("requirement failed: %s %s has changed: expected id %d, found %d", ref.SnapshotRefType, a.Ref, a.SnapshotID, ref.SnapshotID)
	}

	return nil
}

type AssertLastAssignedFieldId struct {
	baseRequirement
	LastAssignedFieldID int `json:"last-assigned-field-id"`
}

func NewAssertLastAssignedFieldID(id int) *AssertLastAssignedFieldId {
	return &AssertLastAssignedFieldId{
		baseRequirement:     baseRequirement{Type: "assert-last-assigned-field-id"},
		LastAssignedFieldID: id,
	}
}

func (a *AssertLastAssignedFieldId) Validate(meta Metadata) error {
	if meta == nil {
		return fmt.Errorf("requirement failed: current table metadata does not exist")
	}

	if meta.LastColumnID() != a.LastAssignedFieldID {
		return fmt.Errorf("requirement failed: last assigned field id has changed: expected %d, found %d", a.LastAssignedFieldID, meta.LastColumnID())
	}

	return nil
}

type AssertCurrentSchemaId struct {
	baseRequirement
	CurrentSchemaID int `json:"current-schema-id"`
}

func NewAssertCurrentSchemaID(id int) *AssertCurrentSchemaId {
	return &AssertCurrentSchemaId{
		baseRequirement: baseRequirement{Type: "assert-current-schema-id"},
		CurrentSchemaID: id,
	}
}

func (a *AssertCurrentSchemaId) Validate(meta Metadata) error {
	if meta == nil {
		return fmt.Errorf("requirement failed: current table metadata does not exist")
	}

	if meta.CurrentSchema().ID != a.CurrentSchemaID {
		return fmt.Errorf("requirement failed: current schema id has changed: expected %d, found %d", a.CurrentSchemaID, meta.CurrentSchema().ID)
	}

	return nil
}

type AssertLastAssignedPartitionId struct {
	baseRequirement
	LastAssignedPartitionID int `json:"last-assigned-partition-id"`
}

func NewAssertLastAssignedPartitionID(id int) *AssertLastAssignedPartitionId {
	return &AssertLastAssignedPartitionId{
		baseRequirement:         baseRequirement{Type: "assert-last-assigned-partition-id"},
		LastAssignedPartitionID: id,
	}
}

func (a *AssertLastAssignedPartitionId) Validate(meta Metadata) error {
	if meta == nil {
		return fmt.Errorf("requirement failed: current table metadata does not exist")
	}

	if *meta.LastPartitionSpecID() != a.LastAssignedPartitionID {
		return fmt.Errorf("requirement failed: last assigned partition id has changed: expected %d, found %d", a.LastAssignedPartitionID, *meta.LastPartitionSpecID())
	}

	return nil
}

type AssertDefaultSpecId struct {
	baseRequirement
	DefaultSpecID int `json:"default-spec-id"`
}

func NewAssertDefaultSpecID(id int) *AssertDefaultSpecId {
	return &AssertDefaultSpecId{
		baseRequirement: baseRequirement{Type: "assert-default-spec-id"},
		DefaultSpecID:   id,
	}
}

func (a *AssertDefaultSpecId) Validate(meta Metadata) error {
	if meta == nil {
		return fmt.Errorf("requirement failed: current table metadata does not exist")
	}

	if meta.DefaultPartitionSpec() != a.DefaultSpecID {
		return fmt.Errorf("requirement failed: default spec id has changed: expected %d, found %d", a.DefaultSpecID, meta.DefaultPartitionSpec())
	}

	return nil
}

type AssertDefaultSortOrderId struct {
	baseRequirement
	DefaultSortOrderID int `json:"default-sort-order-id"`
}

func NewAssertDefaultSortOrderID(id int) *AssertDefaultSortOrderId {
	return &AssertDefaultSortOrderId{
		baseRequirement:    baseRequirement{Type: "assert-default-sort-order-id"},
		DefaultSortOrderID: id,
	}
}

func (a *AssertDefaultSortOrderId) Validate(meta Metadata) error {
	if meta == nil {
		return fmt.Errorf("requirement failed: current table metadata does not exist")
	}

	if meta.DefaultSortOrder() != a.DefaultSortOrderID {
		return fmt.Errorf("requirement failed: default sort order id has changed: expected %d, found %d", a.DefaultSortOrderID, meta.DefaultSortOrder())
	}

	return nil
}
