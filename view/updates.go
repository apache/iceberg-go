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

package view

import (
	"encoding/json"
	"fmt"

	"github.com/apache/iceberg-go"
)

const (
	UpdateActionAssignUUID            = "assign-uuid"
	UpdateActionUpgradeFormatVersion  = "upgrade-format-version"
	UpdateActionAddSchema             = "add-schema"
	UpdateActionSetLocation           = "set-location"
	UpdateActionSetProperties         = "set-properties"
	UpdateActionRemoveProperties      = "remove-properties"
	UpdateActionAddViewVersion        = "add-view-version"
	UpdateActionSetCurrentViewVersion = "set-current-view-version"
)

// ViewUpdate represents a change to view metadata.
type ViewUpdate interface {
	// Action returns the name of the action that the update represents.
	Action() string
	// Apply applies the update to the given metadata builder.
	Apply(*MetadataBuilder) error
}

type baseViewUpdate struct {
	ActionName string `json:"action"`
}

func (u *baseViewUpdate) Action() string {
	return u.ActionName
}

// ViewUpdates represents a list of view updates.
type ViewUpdates []ViewUpdate

func (u *ViewUpdates) UnmarshalJSON(data []byte) error {
	var rawUpdates []json.RawMessage
	if err := json.Unmarshal(data, &rawUpdates); err != nil {
		return err
	}

	for _, raw := range rawUpdates {
		var base baseViewUpdate
		if err := json.Unmarshal(raw, &base); err != nil {
			return err
		}

		var upd ViewUpdate
		switch base.ActionName {
		case UpdateActionAssignUUID:
			upd = &AssignUUIDUpdate{}
		case UpdateActionUpgradeFormatVersion:
			upd = &UpgradeFormatVersionUpdate{}
		case UpdateActionAddSchema:
			upd = &AddSchemaUpdate{}
		case UpdateActionSetLocation:
			upd = &SetLocationUpdate{}
		case UpdateActionSetProperties:
			upd = &SetPropertiesUpdate{}
		case UpdateActionRemoveProperties:
			upd = &RemovePropertiesUpdate{}
		case UpdateActionAddViewVersion:
			upd = &AddViewVersionUpdate{}
		case UpdateActionSetCurrentViewVersion:
			upd = &SetCurrentViewVersionUpdate{}
		default:
			return fmt.Errorf("%w: unknown update action: %s", iceberg.ErrInvalidArgument, base.ActionName)
		}

		if err := json.Unmarshal(raw, upd); err != nil {
			return err
		}
		*u = append(*u, upd)
	}

	return nil
}

// AssignUUIDUpdate assigns a UUID to the view MetadataBuilder.
type AssignUUIDUpdate struct {
	baseViewUpdate
	UUID string `json:"uuid"`
}

// Apply assigns the UUID to the view MetadataBuilder.
func (u *AssignUUIDUpdate) Apply(b *MetadataBuilder) error {
	return b.AssignUUID(u.UUID)
}

// NewAssignUUIDUpdate creates a new update that assigns a UUID to the view MetadataBuilder.
func NewAssignUUIDUpdate(uuid string) *AssignUUIDUpdate {
	return &AssignUUIDUpdate{
		baseViewUpdate: baseViewUpdate{ActionName: UpdateActionAssignUUID},
		UUID:           uuid,
	}
}

// UpgradeFormatVersionUpdate upgrades the format version of the view MetadataBuilder to the given version.
type UpgradeFormatVersionUpdate struct {
	baseViewUpdate
	FormatVersion int `json:"format-version"`
}

// Apply upgrades the format version of the view MetadataBuilder to the given version.
func (u *UpgradeFormatVersionUpdate) Apply(b *MetadataBuilder) error {
	return b.UpgradeFormatVersion(u.FormatVersion)
}

// NewUpgradeFormatVersionUpdate creates a new update that upgrades the format version.
func NewUpgradeFormatVersionUpdate(version int) *UpgradeFormatVersionUpdate {
	return &UpgradeFormatVersionUpdate{
		baseViewUpdate: baseViewUpdate{ActionName: UpdateActionUpgradeFormatVersion},
		FormatVersion:  version,
	}
}

// AddSchemaUpdate adds a new schema to the view MetadataBuilder.
type AddSchemaUpdate struct {
	baseViewUpdate
	Schema       *iceberg.Schema `json:"schema"`
	LastColumnID *int            `json:"last-column-id,omitempty"`
}

func (u *AddSchemaUpdate) Apply(b *MetadataBuilder) error {
	return b.AddSchema(u.Schema)
}

// NewAddSchemaUpdate creates a new update that adds a new schema.
func NewAddSchemaUpdate(schema *iceberg.Schema) *AddSchemaUpdate {
	return &AddSchemaUpdate{
		baseViewUpdate: baseViewUpdate{ActionName: UpdateActionAddSchema},
		Schema:         schema,
	}
}

// SetLocationUpdate updates the view location in the MetadataBuilder.
type SetLocationUpdate struct {
	baseViewUpdate
	Location string `json:"location"`
}

// Apply updates the view location of the MetadataBuilder.
func (u *SetLocationUpdate) Apply(b *MetadataBuilder) error {
	return b.SetLocation(u.Location)
}

// NewSetLocationUpdate creates a new update that updates the view location in the MetadataBuilder.
func NewSetLocationUpdate(location string) *SetLocationUpdate {
	return &SetLocationUpdate{
		baseViewUpdate: baseViewUpdate{ActionName: UpdateActionSetLocation},
		Location:       location,
	}
}

// SetPropertiesUpdate sets view properties.
type SetPropertiesUpdate struct {
	baseViewUpdate
	Updates map[string]string `json:"updates"`
}

// Apply sets view properties in the MetadataBuilder.
func (u *SetPropertiesUpdate) Apply(b *MetadataBuilder) error {
	return b.SetProperties(u.Updates)
}

// NewSetPropertiesUpdate creates a new update that sets view properties in the view MetadataBuilder.
func NewSetPropertiesUpdate(updates map[string]string) *SetPropertiesUpdate {
	return &SetPropertiesUpdate{
		baseViewUpdate: baseViewUpdate{ActionName: UpdateActionSetProperties},
		Updates:        updates,
	}
}

// RemovePropertiesUpdate removes view properties in the view MetadataBuilder.
type RemovePropertiesUpdate struct {
	baseViewUpdate
	Removals []string `json:"removals"`
}

// Apply removes view properties from the view MetadataBuilder.
func (u *RemovePropertiesUpdate) Apply(b *MetadataBuilder) error {
	return b.RemoveProperties(u.Removals)
}

// NewRemovePropertiesUpdate creates a new update that removes view properties from the view MetadataBuilder.
func NewRemovePropertiesUpdate(removals []string) *RemovePropertiesUpdate {
	return &RemovePropertiesUpdate{
		baseViewUpdate: baseViewUpdate{ActionName: UpdateActionRemoveProperties},
		Removals:       removals,
	}
}

// AddViewVersionUpdate adds a new view version to the view MetadataBuilder.
type AddViewVersionUpdate struct {
	baseViewUpdate
	ViewVersion *Version `json:"view-version"`
}

// Apply adds a new view version to the view MetadataBuilder.
func (u *AddViewVersionUpdate) Apply(b *MetadataBuilder) error {
	return b.AddVersion(u.ViewVersion)
}

// NewAddViewVersionUpdate creates a new update that adds a new view version to the view MetadataBuilder.
func NewAddViewVersionUpdate(version *Version) *AddViewVersionUpdate {
	return &AddViewVersionUpdate{
		baseViewUpdate: baseViewUpdate{ActionName: UpdateActionAddViewVersion},
		ViewVersion:    version,
	}
}

// SetCurrentViewVersionUpdate sets the current view version of the view MetadataBuilder.
// VersionID can be -1 to reference the last added version.
type SetCurrentViewVersionUpdate struct {
	baseViewUpdate
	VersionID int64 `json:"version-id"`
}

// Apply sets the current view version of the view MetadataBuilder.
func (u *SetCurrentViewVersionUpdate) Apply(b *MetadataBuilder) error {
	return b.SetCurrentVersionID(u.VersionID)
}

// NewSetCurrentViewVersionUpdate creates a new update that sets the current view version of the view MetadataBuilder.
func NewSetCurrentViewVersionUpdate(versionID int64) *SetCurrentViewVersionUpdate {
	return &SetCurrentViewVersionUpdate{
		baseViewUpdate: baseViewUpdate{ActionName: UpdateActionSetCurrentViewVersion},
		VersionID:      versionID,
	}
}
