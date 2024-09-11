// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
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

	"github.com/apache/iceberg-go"
	"github.com/google/uuid"
)

// Update represents a change to a table's metadata.
type Update interface {
	// Action returns the name of the action that the update represents.
	Action() string
	// Apply applies the update to the given metadata builder.
	Apply(*MetadataBuilder) error
}

// baseUpdate contains the common fields for all updates. It is used to identify the type
// of the update.
type baseUpdate struct {
	ActionName string `json:"Action"`
}

func (u *baseUpdate) Action() string {
	return u.ActionName
}

// AssignUUIDUpdate assigns a UUID to the table metadata.
type AssignUUIDUpdate struct {
	baseUpdate
	UUID uuid.UUID `json:"uuid"`
}

// NewAssignUUIDUpdate creates a new AssignUUIDUpdate with the given UUID.
func NewAssignUUIDUpdate(uuid uuid.UUID) *AssignUUIDUpdate {
	return &AssignUUIDUpdate{
		baseUpdate: baseUpdate{ActionName: "assign-uuid"},
		UUID:       uuid,
	}
}

// Apply updates the UUID on the given metadata builder.
func (u *AssignUUIDUpdate) Apply(builder *MetadataBuilder) error {
	_, err := builder.SetUUID(u.UUID)
	return err
}

// UpgradeFormatVersionUpdate upgrades the format version of the table metadata.
type UpgradeFormatVersionUpdate struct {
	baseUpdate
	FormatVersion int `json:"format-version"`
}

// NewUpgradeFormatVersionUpdate creates a new UpgradeFormatVersionUpdate with the given format version.
func NewUpgradeFormatVersionUpdate(formatVersion int) *UpgradeFormatVersionUpdate {
	return &UpgradeFormatVersionUpdate{
		baseUpdate:    baseUpdate{ActionName: "upgrade-format-version"},
		FormatVersion: formatVersion,
	}
}

// Apply upgrades the format version on the given metadata builder.
func (u *UpgradeFormatVersionUpdate) Apply(builder *MetadataBuilder) error {
	_, err := builder.SetFormatVersion(u.FormatVersion)
	return err
}

// AddSchemaUpdate adds a schema to the table metadata.
type AddSchemaUpdate struct {
	baseUpdate
	Schema       *iceberg.Schema `json:"schema"`
	LastColumnID int             `json:"last-column-id"`
	initial      bool
}

// NewAddSchemaUpdate creates a new AddSchemaUpdate with the given schema and last column ID.
// If the initial flag is set to true, the schema is considered the initial schema of the table,
// and all previously added schemas in the metadata builder are removed.
func NewAddSchemaUpdate(schema *iceberg.Schema, lastColumnID int, initial bool) *AddSchemaUpdate {
	return &AddSchemaUpdate{
		baseUpdate:   baseUpdate{ActionName: "add-schema"},
		Schema:       schema,
		LastColumnID: lastColumnID,
		initial:      initial,
	}
}

func (u *AddSchemaUpdate) Apply(builder *MetadataBuilder) error {
	_, err := builder.AddSchema(u.Schema, u.LastColumnID, u.initial)
	return err
}

// SetCurrentSchemaUpdate sets the current schema of the table metadata.
type SetCurrentSchemaUpdate struct {
	baseUpdate
	SchemaID int `json:"schema-id"`
}

// NewSetCurrentSchemaUpdate creates a new SetCurrentSchemaUpdate with the given schema ID.
func NewSetCurrentSchemaUpdate(id int) *SetCurrentSchemaUpdate {
	return &SetCurrentSchemaUpdate{
		baseUpdate: baseUpdate{ActionName: "set-current-schema"},
		SchemaID:   id,
	}
}

func (u *SetCurrentSchemaUpdate) Apply(builder *MetadataBuilder) error {
	_, err := builder.SetCurrentSchemaID(u.SchemaID)
	return err
}

// AddPartitionSpecUpdate adds a partition spec to the table metadata.
type AddPartitionSpecUpdate struct {
	baseUpdate
	Spec    *iceberg.PartitionSpec `json:"spec"`
	initial bool
}

// NewAddPartitionSpecUpdate creates a new AddPartitionSpecUpdate with the given partition spec.
// If the initial flag is set to true, the spec is considered the initial spec of the table,
// and all other previously added specs in the metadata builder are removed.
func NewAddPartitionSpecUpdate(spec *iceberg.PartitionSpec, initial bool) *AddPartitionSpecUpdate {
	return &AddPartitionSpecUpdate{
		baseUpdate: baseUpdate{ActionName: "add-spec"},
		Spec:       spec,
		initial:    initial,
	}
}

func (u *AddPartitionSpecUpdate) Apply(builder *MetadataBuilder) error {
	_, err := builder.AddPartitionSpec(u.Spec, u.initial)
	return err
}

// SetDefaultSpecUpdate sets the default partition spec of the table metadata.
type SetDefaultSpecUpdate struct {
	baseUpdate
	SpecID int `json:"spec-id"`
}

// NewSetDefaultSpecUpdate creates a new SetDefaultSpecUpdate with the given spec ID.
func NewSetDefaultSpecUpdate(id int) *SetDefaultSpecUpdate {
	return &SetDefaultSpecUpdate{
		baseUpdate: baseUpdate{ActionName: "set-default-spec"},
		SpecID:     id,
	}
}

func (u *SetDefaultSpecUpdate) Apply(builder *MetadataBuilder) error {
	_, err := builder.SetDefaultSpecID(u.SpecID)
	return err
}

// AddSortOrderUpdate adds a sort order to the table metadata.
type AddSortOrderUpdate struct {
	baseUpdate
	SortOrder *SortOrder `json:"sort-order"`
	initial   bool
}

// NewAddSortOrderUpdate creates a new AddSortOrderUpdate with the given sort order.
// If the initial flag is set to true, the sort order is considered the initial sort order of the table,
// and all previously added sort orders in the metadata builder are removed.
func NewAddSortOrderUpdate(sortOrder *SortOrder, initial bool) *AddSortOrderUpdate {
	return &AddSortOrderUpdate{
		baseUpdate: baseUpdate{ActionName: "add-sort-order"},
		SortOrder:  sortOrder,
		initial:    initial,
	}
}

func (u *AddSortOrderUpdate) Apply(builder *MetadataBuilder) error {
	_, err := builder.AddSortOrder(u.SortOrder, u.initial)
	return err
}

// SetDefaultSortOrderUpdate sets the default sort order of the table metadata.
type SetDefaultSortOrderUpdate struct {
	baseUpdate
	SortOrderID int `json:"sort-order-id"`
}

// NewSetDefaultSortOrderUpdate creates a new SetDefaultSortOrderUpdate with the given sort order ID.
func NewSetDefaultSortOrderUpdate(id int) *SetDefaultSortOrderUpdate {
	return &SetDefaultSortOrderUpdate{
		baseUpdate:  baseUpdate{ActionName: "set-default-sort-order"},
		SortOrderID: id,
	}
}

func (u *SetDefaultSortOrderUpdate) Apply(builder *MetadataBuilder) error {
	_, err := builder.SetDefaultSortOrderID(u.SortOrderID)
	return err
}

// AddSnapshotUpdate adds a snapshot to the table metadata.
type AddSnapshotUpdate struct {
	baseUpdate
	Snapshot *Snapshot `json:"snapshot"`
}

// NewAddSnapshotUpdate creates a new AddSnapshotUpdate with the given snapshot.
func NewAddSnapshotUpdate(snapshot *Snapshot) *AddSnapshotUpdate {
	return &AddSnapshotUpdate{
		baseUpdate: baseUpdate{ActionName: "add-snapshot"},
		Snapshot:   snapshot,
	}
}

func (u *AddSnapshotUpdate) Apply(builder *MetadataBuilder) error {
	_, err := builder.AddSnapshot(u.Snapshot)
	return err
}

// SetCurrentSnapshotUpdate sets the current snapshot of the table metadata.
type SetSnapshotRefUpdate struct {
	baseUpdate
	RefName            string  `json:"ref-name"`
	RefType            RefType `json:"type"`
	SnapshotID         int64   `json:"snapshot-id"`
	MaxRefAgeMs        int64   `json:"max-ref-age-ms,omitempty"`
	MaxSnapshotAgeMs   int64   `json:"max-snapshot-age-ms,omitempty"`
	MinSnapshotsToKeep int     `json:"min-snapshots-to-keep,omitempty"`
}

// NewSetSnapshotRefUpdate creates a new SetSnapshotRefUpdate with the given snapshot reference information.
// MaxRefAgeMs, MaxSnapshotAgeMs, and MinSnapshotsToKeep are optional, and any non-positive values are ignored.
func NewSetSnapshotRefUpdate(
	name string,
	snapshotID int64,
	refType RefType,
	maxRefAgeMs, maxSnapshotAgeMs int64,
	minSnapshotsToKeep int,
) *SetSnapshotRefUpdate {
	return &SetSnapshotRefUpdate{
		baseUpdate:         baseUpdate{ActionName: "set-snapshot-ref"},
		RefName:            name,
		RefType:            refType,
		SnapshotID:         snapshotID,
		MaxRefAgeMs:        maxRefAgeMs,
		MaxSnapshotAgeMs:   maxSnapshotAgeMs,
		MinSnapshotsToKeep: minSnapshotsToKeep,
	}
}

func (u *SetSnapshotRefUpdate) Apply(builder *MetadataBuilder) error {
	opts := []setSnapshotRefOption{}
	if u.MaxRefAgeMs >= 0 {
		opts = append(opts, WithMaxRefAgeMs(u.MaxRefAgeMs))
	}
	if u.MaxSnapshotAgeMs >= 0 {
		opts = append(opts, WithMaxSnapshotAgeMs(u.MaxSnapshotAgeMs))
	}
	if u.MinSnapshotsToKeep >= 0 {
		opts = append(opts, WithMinSnapshotsToKeep(u.MinSnapshotsToKeep))
	}

	_, err := builder.SetSnapshotRef(
		u.RefName,
		u.SnapshotID,
		u.RefType,
		opts...,
	)
	return err
}

// SetLocationUpdate sets the location of the table metadata.
type SetLocationUpdate struct {
	baseUpdate
	Location string `json:"location"`
}

// NewSetLocationUpdate creates a new SetLocationUpdate with the given location.
func NewSetLocationUpdate(loc string) *SetLocationUpdate {
	return &SetLocationUpdate{
		baseUpdate: baseUpdate{ActionName: "set-location"},
		Location:   loc,
	}
}

func (u *SetLocationUpdate) Apply(builder *MetadataBuilder) error {
	_, err := builder.SetLoc(u.Location)
	return err
}

// SetPropertyUpdate sets a number of properties in the table metadata.
type SetPropertiesUpdate struct {
	baseUpdate
	Updates iceberg.Properties `json:"updates"`
}

// NewSetPropertiesUpdate creates a new SetPropertiesUpdate with the given properties.
func NewSetPropertiesUpdate(updates iceberg.Properties) *SetPropertiesUpdate {
	return &SetPropertiesUpdate{
		baseUpdate: baseUpdate{ActionName: "set-properties"},
		Updates:    updates,
	}
}

func (u *SetPropertiesUpdate) Apply(builder *MetadataBuilder) error {
	_, err := builder.SetProperties(u.Updates)
	return err
}

// RemovePropertiesUpdate removes a number of properties from the table metadata.
// The properties are identified by their names, and if a property with the given name does not exist,
// it is ignored.
type RemovePropertiesUpdate struct {
	baseUpdate
	Removals []string `json:"removals"`
}

// NewRemovePropertiesUpdate creates a new RemovePropertiesUpdate with the given property names.
func NewRemovePropertiesUpdate(removals []string) *RemovePropertiesUpdate {
	return &RemovePropertiesUpdate{
		baseUpdate: baseUpdate{ActionName: "remove-properties"},
		Removals:   removals,
	}
}

func (u *RemovePropertiesUpdate) Apply(builder *MetadataBuilder) error {
	_, err := builder.RemoveProperties(u.Removals)
	return err
}

// RemoveSnapshotsUpdate removes a number of snapshots from the table metadata.
type RemoveSnapshotsUpdate struct {
	baseUpdate
	SnapshotIDs []int64 `json:"snapshot-ids"`
}

// NewRemoveSnapshotsUpdate creates a new RemoveSnapshotsUpdate with the given snapshot IDs.
func NewRemoveSnapshotsUpdate(ids []int64) *RemoveSnapshotsUpdate {
	return &RemoveSnapshotsUpdate{
		baseUpdate:  baseUpdate{ActionName: "remove-snapshots"},
		SnapshotIDs: ids,
	}
}

func (u *RemoveSnapshotsUpdate) Apply(builder *MetadataBuilder) error {
	return fmt.Errorf("%w: remove-snapshots", iceberg.ErrNotImplemented)
}

// RemoveSnapshotRefUpdate removes a snapshot reference from the table metadata.
type RemoveSnapshotRefUpdate struct {
	baseUpdate
	RefName string `json:"ref-name"`
}

// NewRemoveSnapshotRefUpdate creates a new RemoveSnapshotRefUpdate with the given reference name.
func NewRemoveSnapshotRefUpdate(ref string) *RemoveSnapshotRefUpdate {
	return &RemoveSnapshotRefUpdate{
		baseUpdate: baseUpdate{ActionName: "remove-snapshot-ref"},
		RefName:    ref,
	}
}

func (u *RemoveSnapshotRefUpdate) Apply(builder *MetadataBuilder) error {
	return fmt.Errorf("%w: remove-snapshot-ref", iceberg.ErrNotImplemented)
}
