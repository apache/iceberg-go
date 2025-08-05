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
	"context"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/apache/iceberg-go"
	"github.com/google/uuid"
)

// These are the various update actions defined in the iceberg spec
const (
	UpdateAddSpec      = "add-spec"
	UpdateAddSchema    = "add-schema"
	UpdateAddSnapshot  = "add-snapshot"
	UpdateAddSortOrder = "add-sort-order"

	UpdateAssignUUID = "assign-uuid"

	UpdateRemoveProperties  = "remove-properties"
	UpdateRemoveSchemas     = "remove-schemas"
	UpdateRemoveSnapshots   = "remove-snapshots"
	UpdateRemoveSnapshotRef = "remove-snapshot-ref"
	UpdateRemoveSpec        = "remove-partition-specs"

	UpdateSetCurrentSchema    = "set-current-schema"
	UpdateSetDefaultSortOrder = "set-default-sort-order"
	UpdateSetDefaultSpec      = "set-default-spec"
	UpdateSetLocation         = "set-location"
	UpdateSetProperties       = "set-properties"
	UpdateSetSnapshotRef      = "set-snapshot-ref"

	UpdateUpgradeFormatVersion = "upgrade-format-version"
)

// Update represents a change to a table's metadata.
type Update interface {
	// Action returns the name of the action that the update represents.
	Action() string
	// Apply applies the update to the given metadata builder.
	Apply(*MetadataBuilder) error
	// PostCommit is called after successful commit of the update
	PostCommit(context.Context, *Table, *Table) error
}

type Updates []Update

func (u *Updates) UnmarshalJSON(data []byte) error {
	var rawUpdates []json.RawMessage
	if err := json.Unmarshal(data, &rawUpdates); err != nil {
		return err
	}

	for _, raw := range rawUpdates {
		var base baseUpdate
		if err := json.Unmarshal(raw, &base); err != nil {
			return err
		}

		var upd Update
		switch base.ActionName {
		case UpdateAssignUUID:
			upd = &assignUUIDUpdate{}
		case UpdateUpgradeFormatVersion:
			upd = &upgradeFormatVersionUpdate{}
		case UpdateAddSchema:
			upd = &addSchemaUpdate{}
		case UpdateSetCurrentSchema:
			upd = &setCurrentSchemaUpdate{}
		case UpdateAddSpec:
			upd = &addPartitionSpecUpdate{}
		case UpdateSetDefaultSpec:
			upd = &setDefaultSpecUpdate{}
		case UpdateAddSortOrder:
			upd = &addSortOrderUpdate{}
		case UpdateSetDefaultSortOrder:
			upd = &setDefaultSortOrderUpdate{}
		case UpdateAddSnapshot:
			upd = &addSnapshotUpdate{}
		case UpdateSetSnapshotRef:
			upd = &setSnapshotRefUpdate{}
		case UpdateRemoveSnapshots:
			upd = &removeSnapshotsUpdate{}
		case UpdateRemoveSnapshotRef:
			upd = &removeSnapshotRefUpdate{}
		case UpdateSetLocation:
			upd = &setLocationUpdate{}
		case UpdateSetProperties:
			upd = &setPropertiesUpdate{}
		case UpdateRemoveProperties:
			upd = &removePropertiesUpdate{}
		case UpdateRemoveSpec:
			upd = &removeSpecUpdate{}
		case UpdateRemoveSchemas:
			upd = &removeSchemasUpdate{}
		default:
			return fmt.Errorf("unknown update action: %s", base.ActionName)
		}

		if err := json.Unmarshal(raw, upd); err != nil {
			return err
		}
		*u = append(*u, upd)
	}

	return nil
}

// baseUpdate contains the common fields for all updates. It is used to identify the type
// of the update.
type baseUpdate struct {
	ActionName string `json:"action"`
}

func (u *baseUpdate) Action() string {
	return u.ActionName
}

func (u *baseUpdate) PostCommit(_ context.Context, _ *Table, _ *Table) error {
	return nil
}

type assignUUIDUpdate struct {
	baseUpdate
	UUID uuid.UUID `json:"uuid"`
}

// NewAssignUUIDUpdate creates a new update to assign a UUID to the table metadata.
func NewAssignUUIDUpdate(uuid uuid.UUID) *assignUUIDUpdate {
	return &assignUUIDUpdate{
		baseUpdate: baseUpdate{ActionName: UpdateAssignUUID},
		UUID:       uuid,
	}
}

func (u *assignUUIDUpdate) Apply(builder *MetadataBuilder) error {
	_, err := builder.SetUUID(u.UUID)

	return err
}

type upgradeFormatVersionUpdate struct {
	baseUpdate
	FormatVersion int `json:"format-version"`
}

// NewUpgradeFormatVersionUpdate creates a new update that upgrades the format version
// of the table metadata to the given formatVersion.
func NewUpgradeFormatVersionUpdate(formatVersion int) *upgradeFormatVersionUpdate {
	return &upgradeFormatVersionUpdate{
		baseUpdate:    baseUpdate{ActionName: UpdateUpgradeFormatVersion},
		FormatVersion: formatVersion,
	}
}

func (u *upgradeFormatVersionUpdate) Apply(builder *MetadataBuilder) error {
	_, err := builder.SetFormatVersion(u.FormatVersion)

	return err
}

type addSchemaUpdate struct {
	baseUpdate
	Schema  *iceberg.Schema `json:"schema"`
	initial bool
}

// NewAddSchemaUpdate creates a new update that adds the given schema and updates the lastColumnID based on the schema.
func NewAddSchemaUpdate(schema *iceberg.Schema) *addSchemaUpdate {
	return &addSchemaUpdate{
		baseUpdate: baseUpdate{ActionName: UpdateAddSchema},
		Schema:     schema,
	}
}

func (u *addSchemaUpdate) Apply(builder *MetadataBuilder) error {
	_, err := builder.AddSchema(u.Schema)

	return err
}

type setCurrentSchemaUpdate struct {
	baseUpdate
	SchemaID int `json:"schema-id"`
}

// NewSetCurrentSchemaUpdate creates a new update that sets the current schema of the table
// metadata to the given schema ID.
func NewSetCurrentSchemaUpdate(id int) *setCurrentSchemaUpdate {
	return &setCurrentSchemaUpdate{
		baseUpdate: baseUpdate{ActionName: UpdateSetCurrentSchema},
		SchemaID:   id,
	}
}

func (u *setCurrentSchemaUpdate) Apply(builder *MetadataBuilder) error {
	_, err := builder.SetCurrentSchemaID(u.SchemaID)

	return err
}

type addPartitionSpecUpdate struct {
	baseUpdate
	Spec    *iceberg.PartitionSpec `json:"spec"`
	initial bool
}

// NewAddPartitionSpecUpdate creates a new update that adds the given partition spec to the table
// metadata. If the initial flag is set to true, the spec is considered the initial spec of the table,
// and all other previously added specs in the metadata builder are removed.
func NewAddPartitionSpecUpdate(spec *iceberg.PartitionSpec, initial bool) *addPartitionSpecUpdate {
	return &addPartitionSpecUpdate{
		baseUpdate: baseUpdate{ActionName: UpdateAddSpec},
		Spec:       spec,
		initial:    initial,
	}
}

func (u *addPartitionSpecUpdate) Apply(builder *MetadataBuilder) error {
	_, err := builder.AddPartitionSpec(u.Spec, u.initial)

	return err
}

type setDefaultSpecUpdate struct {
	baseUpdate
	SpecID int `json:"spec-id"`
}

// NewSetDefaultSpecUpdate creates a new update that sets the default partition spec of the
// table metadata to the given spec ID.
func NewSetDefaultSpecUpdate(id int) *setDefaultSpecUpdate {
	return &setDefaultSpecUpdate{
		baseUpdate: baseUpdate{ActionName: UpdateSetDefaultSpec},
		SpecID:     id,
	}
}

func (u *setDefaultSpecUpdate) Apply(builder *MetadataBuilder) error {
	_, err := builder.SetDefaultSpecID(u.SpecID)

	return err
}

type addSortOrderUpdate struct {
	baseUpdate
	SortOrder *SortOrder `json:"sort-order"`
	initial   bool
}

// NewAddSortOrderUpdate creates a new update that adds the given sort order to the table metadata.
// If the initial flag is set to true, the sort order is considered the initial sort order of the table,
// and all previously added sort orders in the metadata builder are removed.
func NewAddSortOrderUpdate(sortOrder *SortOrder, initial bool) *addSortOrderUpdate {
	return &addSortOrderUpdate{
		baseUpdate: baseUpdate{ActionName: UpdateAddSortOrder},
		SortOrder:  sortOrder,
		initial:    initial,
	}
}

func (u *addSortOrderUpdate) Apply(builder *MetadataBuilder) error {
	_, err := builder.AddSortOrder(u.SortOrder, u.initial)

	return err
}

type setDefaultSortOrderUpdate struct {
	baseUpdate
	SortOrderID int `json:"sort-order-id"`
}

// NewSetDefaultSortOrderUpdate creates a new update that sets the default sort order of the table metadata
// to the given sort order ID.
func NewSetDefaultSortOrderUpdate(id int) *setDefaultSortOrderUpdate {
	return &setDefaultSortOrderUpdate{
		baseUpdate:  baseUpdate{ActionName: UpdateSetDefaultSortOrder},
		SortOrderID: id,
	}
}

func (u *setDefaultSortOrderUpdate) Apply(builder *MetadataBuilder) error {
	_, err := builder.SetDefaultSortOrderID(u.SortOrderID)

	return err
}

type addSnapshotUpdate struct {
	baseUpdate
	Snapshot *Snapshot `json:"snapshot"`
}

// NewAddSnapshotUpdate creates a new update that adds the given snapshot to the table metadata.
func NewAddSnapshotUpdate(snapshot *Snapshot) *addSnapshotUpdate {
	return &addSnapshotUpdate{
		baseUpdate: baseUpdate{ActionName: UpdateAddSnapshot},
		Snapshot:   snapshot,
	}
}

func (u *addSnapshotUpdate) Apply(builder *MetadataBuilder) error {
	_, err := builder.AddSnapshot(u.Snapshot)

	return err
}

type setSnapshotRefUpdate struct {
	baseUpdate
	RefName            string  `json:"ref-name"`
	RefType            RefType `json:"type"`
	SnapshotID         int64   `json:"snapshot-id"`
	MaxRefAgeMs        int64   `json:"max-ref-age-ms,omitempty"`
	MaxSnapshotAgeMs   int64   `json:"max-snapshot-age-ms,omitempty"`
	MinSnapshotsToKeep int     `json:"min-snapshots-to-keep,omitempty"`
}

// NewSetSnapshotRefUpdate creates a new update that sets the given snapshot reference
// as the current snapshot of the table metadata. MaxRefAgeMs, MaxSnapshotAgeMs,
// and MinSnapshotsToKeep are optional, and any non-positive values are ignored.
func NewSetSnapshotRefUpdate(
	name string,
	snapshotID int64,
	refType RefType,
	maxRefAgeMs, maxSnapshotAgeMs int64,
	minSnapshotsToKeep int,
) *setSnapshotRefUpdate {
	return &setSnapshotRefUpdate{
		baseUpdate:         baseUpdate{ActionName: UpdateSetSnapshotRef},
		RefName:            name,
		RefType:            refType,
		SnapshotID:         snapshotID,
		MaxRefAgeMs:        maxRefAgeMs,
		MaxSnapshotAgeMs:   maxSnapshotAgeMs,
		MinSnapshotsToKeep: minSnapshotsToKeep,
	}
}

func (u *setSnapshotRefUpdate) Apply(builder *MetadataBuilder) error {
	opts := []setSnapshotRefOption{}
	if u.MaxRefAgeMs > 0 {
		opts = append(opts, WithMaxRefAgeMs(u.MaxRefAgeMs))
	}
	if u.MaxSnapshotAgeMs > 0 {
		opts = append(opts, WithMaxSnapshotAgeMs(u.MaxSnapshotAgeMs))
	}
	if u.MinSnapshotsToKeep > 0 {
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

type setLocationUpdate struct {
	baseUpdate
	Location string `json:"location"`
}

// NewSetLocationUpdate creates a new update that sets the location of the table metadata.
func NewSetLocationUpdate(loc string) Update {
	return &setLocationUpdate{
		baseUpdate: baseUpdate{ActionName: UpdateSetLocation},
		Location:   loc,
	}
}

func (u *setLocationUpdate) Apply(builder *MetadataBuilder) error {
	_, err := builder.SetLoc(u.Location)

	return err
}

type setPropertiesUpdate struct {
	baseUpdate
	Updates iceberg.Properties `json:"updates"`
}

// NewSetPropertiesUpdate creates a new update that sets the given properties in the
// table metadata.
func NewSetPropertiesUpdate(updates iceberg.Properties) *setPropertiesUpdate {
	return &setPropertiesUpdate{
		baseUpdate: baseUpdate{ActionName: UpdateSetProperties},
		Updates:    updates,
	}
}

func (u *setPropertiesUpdate) Apply(builder *MetadataBuilder) error {
	_, err := builder.SetProperties(u.Updates)

	return err
}

type removePropertiesUpdate struct {
	baseUpdate
	Removals []string `json:"removals"`
}

// NewRemovePropertiesUpdate creates a new update that removes properties from the table metadata.
// The properties are identified by their names, and if a property with the given name does not exist,
// it is ignored.
func NewRemovePropertiesUpdate(removals []string) *removePropertiesUpdate {
	return &removePropertiesUpdate{
		baseUpdate: baseUpdate{ActionName: UpdateRemoveProperties},
		Removals:   removals,
	}
}

func (u *removePropertiesUpdate) Apply(builder *MetadataBuilder) error {
	_, err := builder.RemoveProperties(u.Removals)

	return err
}

type removeSnapshotsUpdate struct {
	baseUpdate
	SnapshotIDs []int64 `json:"snapshot-ids"`
}

// NewRemoveSnapshotsUpdate creates a new update that removes all snapshots from
// the table metadata with the given snapshot IDs.
func NewRemoveSnapshotsUpdate(ids []int64) *removeSnapshotsUpdate {
	return &removeSnapshotsUpdate{
		baseUpdate:  baseUpdate{ActionName: UpdateRemoveSnapshots},
		SnapshotIDs: ids,
	}
}

func (u *removeSnapshotsUpdate) Apply(builder *MetadataBuilder) error {
	_, err := builder.RemoveSnapshots(u.SnapshotIDs)

	return err
}

func (u *removeSnapshotsUpdate) PostCommit(ctx context.Context, preTable *Table, postTable *Table) error {
	prefs, err := preTable.FS(ctx)
	if err != nil {
		return err
	}

	filesToDelete := make(map[string]struct{})

	for _, snapId := range u.SnapshotIDs {
		snap := preTable.Metadata().SnapshotByID(snapId)
		if snap == nil {
			return errors.New("snapshot should never be nil")
		}

		filesToDelete[snap.ManifestList] = struct{}{}
	}

	for _, snapId := range u.SnapshotIDs {
		snap := preTable.SnapshotByID(snapId)
		if snap == nil {
			return errors.New("missing snapshot")
		}

		mans, err := snap.Manifests(prefs)
		if err != nil {
			return err
		}

		for _, man := range mans {
			filesToDelete[man.FilePath()] = struct{}{}

			entries, err := man.FetchEntries(prefs, false)
			if err != nil {
				return err
			}

			for _, entry := range entries {
				filesToDelete[entry.DataFile().FilePath()] = struct{}{}
			}
		}
	}

	for _, snap := range postTable.Metadata().Snapshots() {
		mans, err := snap.Manifests(prefs)
		if err != nil {
			return err
		}

		for _, man := range mans {
			delete(filesToDelete, man.FilePath())

			entries, err := man.FetchEntries(prefs, false)
			if err != nil {
				return err
			}

			for _, entry := range entries {
				if entry.Status() != iceberg.EntryStatusDELETED {
					delete(filesToDelete, entry.DataFile().FilePath())
				}
			}
		}
	}

	var res error

	for f := range filesToDelete {
		if err := prefs.Remove(f); err != nil {
			res = errors.Join(res, err)
		}
	}

	return res
}

type removeSnapshotRefUpdate struct {
	baseUpdate
	RefName string `json:"ref-name"`
}

// NewRemoveSnapshotRefUpdate creates a new update that removes a snapshot reference
// from the table metadata.
func NewRemoveSnapshotRefUpdate(ref string) *removeSnapshotRefUpdate {
	return &removeSnapshotRefUpdate{
		baseUpdate: baseUpdate{ActionName: UpdateRemoveSnapshotRef},
		RefName:    ref,
	}
}

func (u *removeSnapshotRefUpdate) Apply(builder *MetadataBuilder) error {
	_, err := builder.RemoveSnapshotRef(u.RefName)

	return err
}

type removeSpecUpdate struct {
	baseUpdate
	SpecIds []int64 `json:"spec-ids"`
}

// NewRemoveSpecUpdate creates a new Update that removes a list of partition specs
// from the table metadata.
func NewRemoveSpecUpdate(specIds []int64) *removeSpecUpdate {
	return &removeSpecUpdate{
		baseUpdate: baseUpdate{ActionName: UpdateRemoveSpec},
		SpecIds:    specIds,
	}
}

func (u *removeSpecUpdate) Apply(builder *MetadataBuilder) error {
	return fmt.Errorf("%w: %s", iceberg.ErrNotImplemented, UpdateRemoveSpec)
}

type removeSchemasUpdate struct {
	baseUpdate
	SchemaIds []int64 `json:"schema-ids"`
}

// NewRemoveSchemasUpdate creates a new Update that removes a list of schemas from
// the table metadata.
func NewRemoveSchemasUpdate(schemaIds []int64) *removeSchemasUpdate {
	return &removeSchemasUpdate{
		baseUpdate: baseUpdate{ActionName: UpdateRemoveSchemas},
		SchemaIds:  schemaIds,
	}
}

func (u *removeSchemasUpdate) Apply(builder *MetadataBuilder) error {
	return fmt.Errorf("%w: %s", iceberg.ErrNotImplemented, UpdateRemoveSchemas)
}
