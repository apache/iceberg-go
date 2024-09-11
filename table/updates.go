package table

import (
	"fmt"

	"github.com/apache/iceberg-go"
	"github.com/google/uuid"
)

type Update interface {
	Action() string
	Apply(*MetadataBuilder) error
}

type baseUpdate struct {
	ActionName string `json:"action"`
}

func (u *baseUpdate) Action() string {
	return u.ActionName
}

type AssignUUIDUpdate struct {
	baseUpdate
	UUID uuid.UUID `json:"uuid"`
}

func NewAssignUUIDUpdate(uuid uuid.UUID) *AssignUUIDUpdate {
	return &AssignUUIDUpdate{
		baseUpdate: baseUpdate{ActionName: "assign-uuid"},
		UUID:       uuid,
	}
}

func (u *AssignUUIDUpdate) Apply(builder *MetadataBuilder) error {
	_, err := builder.SetUUID(u.UUID)
	return err
}

type UpgradeFormatVersionUpdate struct {
	baseUpdate
	FormatVersion int `json:"format-version"`
}

func NewUpgradeFormatVersionUpdate(formatVersion int) *UpgradeFormatVersionUpdate {
	return &UpgradeFormatVersionUpdate{
		baseUpdate:    baseUpdate{ActionName: "upgrade-format-version"},
		FormatVersion: formatVersion,
	}
}

func (u *UpgradeFormatVersionUpdate) Apply(builder *MetadataBuilder) error {
	_, err := builder.SetFormatVersion(u.FormatVersion)
	return err
}

type AddSchemaUpdate struct {
	baseUpdate
	Schema       *iceberg.Schema `json:"schema"`
	LastColumnID int             `json:"last-column-id"`
	initial      bool
}

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

type SetCurrentSchemaUpdate struct {
	baseUpdate
	SchemaID int `json:"schema-id"`
}

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

type AddPartitionSpecUpdate struct {
	baseUpdate
	Spec    *iceberg.PartitionSpec `json:"spec"`
	initial bool
}

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

type SetDefaultSpecUpdate struct {
	baseUpdate
	SpecID int `json:"spec-id"`
}

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

type AddSortOrderUpdate struct {
	baseUpdate
	SortOrder *SortOrder `json:"sort-order"`
	initial   bool
}

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

type SetDefaultSortOrderUpdate struct {
	baseUpdate
	SortOrderID int `json:"sort-order-id"`
}

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

type AddSnapshotUpdate struct {
	baseUpdate
	Snapshot *Snapshot `json:"snapshot"`
}

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

type SetSnapshotRefUpdate struct {
	baseUpdate
	RefName            string  `json:"ref-name"`
	RefType            RefType `json:"type"`
	SnapshotID         int64   `json:"snapshot-id"`
	MaxRefAgeMs        *int64  `json:"max-ref-age-ms,omitempty"`
	MaxSnapshotAgeMs   *int64  `json:"max-snapshot-age-ms,omitempty"`
	MinSnapshotsToKeep *int    `json:"min-snapshots-to-keep,omitempty"`
}

func NewSetSnapshotRefUpdate(
	name string,
	snapshotID int64,
	refType RefType,
	maxRefAgeMs, maxSnapshotAgeMs *int64,
	minSnapshotsToKeep *int,
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
	_, err := builder.SetSnapshotRef(
		u.RefName,
		u.SnapshotID,
		u.RefType,
		u.MaxRefAgeMs,
		u.MaxSnapshotAgeMs,
		u.MinSnapshotsToKeep,
	)
	return err
}

type SetLocationUpdate struct {
	baseUpdate
	Location string `json:"location"`
}

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

type SetPropertiesUpdate struct {
	baseUpdate
	Updates iceberg.Properties `json:"updates"`
}

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

type RemovePropertiesUpdate struct {
	baseUpdate
	Removals []string `json:"removals"`
}

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

type RemoveSnapshotsUpdate struct {
	baseUpdate
	SnapshotIDs []int64 `json:"snapshot-ids"`
}

func NewRemoveSnapshotsUpdate(ids []int64) *RemoveSnapshotsUpdate {
	return &RemoveSnapshotsUpdate{
		baseUpdate:  baseUpdate{ActionName: "remove-snapshots"},
		SnapshotIDs: ids,
	}
}

func (u *RemoveSnapshotsUpdate) Apply(builder *MetadataBuilder) error {
	return fmt.Errorf("%w: remove-snapshots", iceberg.ErrNotImplemented)
}

type RemoveSnapshotRefUpdate struct {
	baseUpdate
	RefName string `json:"ref-name"`
}

func NewRemoveSnapshotRefUpdate(ref string) *RemoveSnapshotRefUpdate {
	return &RemoveSnapshotRefUpdate{
		baseUpdate: baseUpdate{ActionName: "remove-snapshot-ref"},
		RefName:    ref,
	}
}

func (u *RemoveSnapshotRefUpdate) Apply(builder *MetadataBuilder) error {
	return fmt.Errorf("%w: remove-snapshot-ref", iceberg.ErrNotImplemented)
}
