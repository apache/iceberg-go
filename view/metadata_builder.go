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
	"cmp"
	"fmt"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/apache/iceberg-go"
	"github.com/google/uuid"
)

const (
	LastAdded                  = -1
	InitialSchemaID            = 0
	InitialVersionID           = 1
	DefaultViewFormatVersion   = 1
	SupportedViewFormatVersion = 1
)

// MetadataBuilder is a struct used for building and updating Iceberg view metadata.
type MetadataBuilder struct {
	base Metadata

	formatVersion    int
	uuid             string
	location         string
	currentVersionID int64
	versions         []*Version
	schemas          []*iceberg.Schema
	history          []VersionLogEntry
	properties       map[string]string

	versionsById map[int64]*Version
	schemasById  map[int]*iceberg.Schema

	changes []ViewUpdate

	lastAddedVersionID *int64
	lastAddedSchemaID  *int
	historyEntry       *VersionLogEntry
	previousVersion    *Version
}

// ViewMetadataBuildResult contains the built metadata and the list of changes applied.
type ViewMetadataBuildResult struct {
	Metadata *metadata
	Changes  []ViewUpdate
}

// NewMetadataBuilder creates an empty builder with the specified format version.
func NewMetadataBuilder(formatVersion int) (*MetadataBuilder, error) {
	if formatVersion < 1 || formatVersion > SupportedViewFormatVersion {
		return nil, fmt.Errorf("%w: %d (supported: 1-%d)",
			iceberg.ErrInvalidFormatVersion, formatVersion, SupportedViewFormatVersion)
	}

	return &MetadataBuilder{
		formatVersion:    formatVersion,
		versions:         make([]*Version, 0),
		schemas:          make([]*iceberg.Schema, 0),
		history:          make([]VersionLogEntry, 0),
		properties:       make(map[string]string),
		versionsById:     make(map[int64]*Version),
		schemasById:      make(map[int]*iceberg.Schema),
		changes:          make([]ViewUpdate, 0),
		currentVersionID: -1,
	}, nil
}

// MetadataBuilderFrom creates a builder from existing metadata.
func MetadataBuilderFrom(base Metadata) *MetadataBuilder {
	schemas := make([]*iceberg.Schema, 0)
	schemasById := make(map[int]*iceberg.Schema)
	for schema := range base.Schemas() {
		schemas = append(schemas, schema)
		schemasById[schema.ID] = schema
	}

	versions := make([]*Version, 0)
	versionsById := make(map[int64]*Version)
	for version := range base.Versions() {
		v := version
		versions = append(versions, &v)
		versionsById[v.VersionID] = &v
	}

	history := make([]VersionLogEntry, 0)
	for entry := range base.VersionLog() {
		history = append(history, entry)
	}

	properties := make(map[string]string)
	for k, v := range base.Properties() {
		properties[k] = v
	}

	currentVersionID := int64(-1)
	var previousVersion *Version
	if base.CurrentVersion() != nil {
		currentVersionID = base.CurrentVersion().VersionID
		previousVersion = base.CurrentVersion()
	}

	return &MetadataBuilder{
		base:             base,
		formatVersion:    base.FormatVersion(),
		uuid:             base.ViewUUID(),
		location:         base.Location(),
		currentVersionID: currentVersionID,
		versions:         versions,
		schemas:          schemas,
		history:          history,
		properties:       properties,
		versionsById:     versionsById,
		schemasById:      schemasById,
		changes:          make([]ViewUpdate, 0),
		previousVersion:  previousVersion,
	}
}

// AssignUUID assigns a UUID to the view.
func (b *MetadataBuilder) AssignUUID(uuidStr string) error {
	if uuidStr == "" {
		return fmt.Errorf("%w: cannot set uuid to empty string", iceberg.ErrInvalidArgument)
	}

	if err := uuid.Validate(uuidStr); err != nil {
		return fmt.Errorf("%w: invalid uuid format: %s", iceberg.ErrInvalidArgument, err)
	}

	if b.uuid != "" && b.uuid != uuidStr {
		return fmt.Errorf("%w: cannot reassign uuid from %s to %s", iceberg.ErrInvalidArgument, b.uuid, uuidStr)
	}

	if b.uuid == uuidStr {
		return nil
	}

	b.changes = append(b.changes, NewAssignUUIDUpdate(uuidStr))
	b.uuid = uuidStr

	return nil
}

// UpgradeFormatVersion upgrades the format version.
func (b *MetadataBuilder) UpgradeFormatVersion(version int) error {
	if version < b.formatVersion {
		return fmt.Errorf("%w: cannot downgrade from %d to %d",
			iceberg.ErrInvalidFormatVersion, b.formatVersion, version)
	}

	if version > SupportedViewFormatVersion {
		return fmt.Errorf("%w: %d (supported: 1-%d)",
			iceberg.ErrInvalidFormatVersion, version, SupportedViewFormatVersion)
	}

	if version == b.formatVersion {
		return nil
	}

	b.changes = append(b.changes, NewUpgradeFormatVersionUpdate(version))
	b.formatVersion = version

	return nil
}

// SetLocation sets the base location for the view.
func (b *MetadataBuilder) SetLocation(location string) error {
	if location == "" {
		return fmt.Errorf("%w: location cannot be empty", iceberg.ErrInvalidArgument)
	}

	if b.location == location {
		return nil
	}

	b.changes = append(b.changes, NewSetLocationUpdate(location))
	b.location = location

	return nil
}

// SetProperties sets view properties.
func (b *MetadataBuilder) SetProperties(updates map[string]string) error {
	if len(updates) == 0 {
		return nil
	}

	b.changes = append(b.changes, NewSetPropertiesUpdate(updates))
	for k, v := range updates {
		b.properties[k] = v
	}

	return nil
}

// RemoveProperties removes view properties.
func (b *MetadataBuilder) RemoveProperties(removals []string) error {
	if len(removals) == 0 {
		return nil
	}

	b.changes = append(b.changes, NewRemovePropertiesUpdate(removals))
	for _, key := range removals {
		delete(b.properties, key)
	}

	return nil
}

// AddSchema adds a schema to the view.
func (b *MetadataBuilder) AddSchema(schema *iceberg.Schema) error {
	if schema == nil {
		return fmt.Errorf("%w: cannot add nil schema", iceberg.ErrInvalidArgument)
	}

	newSchemaID := b.reuseOrCreateNewSchemaID(schema)
	schema.ID = newSchemaID

	if _, err := b.GetSchemaByID(newSchemaID); err == nil {
		if b.lastAddedSchemaID == nil || *b.lastAddedSchemaID != newSchemaID {
			b.changes = append(b.changes, NewAddSchemaUpdate(schema))
			b.lastAddedSchemaID = &newSchemaID
		}

		return nil
	}

	b.schemas = append(b.schemas, schema)
	b.schemasById[newSchemaID] = schema
	b.changes = append(b.changes, NewAddSchemaUpdate(schema))
	b.lastAddedSchemaID = &newSchemaID

	return nil
}

// GetSchemaByID retrieves a schema by its ID.
func (b *MetadataBuilder) GetSchemaByID(id int) (*iceberg.Schema, error) {
	if schema, ok := b.schemasById[id]; ok {
		return schema, nil
	}

	return nil, fmt.Errorf("%w: schema with id %d not found", iceberg.ErrInvalidArgument, id)
}

func (b *MetadataBuilder) reuseOrCreateNewSchemaID(schema *iceberg.Schema) int {
	for _, existing := range b.schemas {
		if schema.Equals(existing) {
			return existing.ID
		}
	}

	return b.nextSchemaID()
}

func (b *MetadataBuilder) nextSchemaID() int {
	maxID := InitialSchemaID - 1
	for _, schema := range b.schemas {
		if schema.ID > maxID {
			maxID = schema.ID
		}
	}

	return maxID + 1
}

func validateUniqueDialects(version *Version) error {
	seenDialects := make(map[string]bool)

	for _, repr := range version.Representations {
		dialect := strings.ToLower(repr.Dialect)
		if seenDialects[dialect] {
			return fmt.Errorf("%w: cannot add multiple queries for dialect %s",
				ErrInvalidViewMetadata, repr.Dialect)
		}
		seenDialects[dialect] = true
	}

	return nil
}

// AddVersion adds a view version.
func (b *MetadataBuilder) AddVersion(version *Version) error {
	if version == nil {
		return fmt.Errorf("%w: cannot add nil version", iceberg.ErrInvalidArgument)
	}

	if err := validateUniqueDialects(version); err != nil {
		return err
	}

	if _, err := b.GetSchemaByID(version.SchemaID); err != nil {
		return fmt.Errorf("%w: cannot add version with unknown schema: %w", iceberg.ErrInvalidArgument, err)
	}

	newVersionID := b.reuseOrCreateNewVersionID(version)

	version.VersionID = newVersionID

	if _, err := b.GetVersionByID(newVersionID); err == nil {
		if b.lastAddedVersionID == nil || *b.lastAddedVersionID != newVersionID {
			b.changes = append(b.changes, NewAddViewVersionUpdate(version))
			b.lastAddedVersionID = &newVersionID
		}

		return nil
	}

	b.versions = append(b.versions, version)
	b.versionsById[newVersionID] = version
	b.changes = append(b.changes, NewAddViewVersionUpdate(version))
	b.lastAddedVersionID = &newVersionID

	return nil
}

// GetVersionByID retrieves a version by its ID.
func (b *MetadataBuilder) GetVersionByID(id int64) (*Version, error) {
	if version, ok := b.versionsById[id]; ok {
		return version, nil
	}

	return nil, fmt.Errorf("%w: version with id %d not found", iceberg.ErrInvalidArgument, id)
}

func (b *MetadataBuilder) reuseOrCreateNewVersionID(version *Version) int64 {
	for _, existing := range b.versions {
		if version.Equals(existing) {
			return existing.VersionID
		}
	}

	return b.nextVersionID()
}

func (b *MetadataBuilder) nextVersionID() int64 {
	maxID := int64(InitialVersionID - 1)
	for _, version := range b.versions {
		if version.VersionID > maxID {
			maxID = version.VersionID
		}
	}

	return maxID + 1
}

// SetCurrentVersionID sets the current version by ID.
// versionID can be LastAdded (-1) to reference the last added version.
func (b *MetadataBuilder) SetCurrentVersionID(versionID int64) error {
	if versionID == LastAdded {
		if b.lastAddedVersionID == nil {
			return fmt.Errorf("%w: cannot set last added version: no version has been added", iceberg.ErrInvalidArgument)
		}
		versionID = *b.lastAddedVersionID
	}

	version, err := b.GetVersionByID(versionID)
	if err != nil {
		return fmt.Errorf("%w: cannot set current version to %d: %w", iceberg.ErrInvalidArgument, versionID, err)
	}

	if b.currentVersionID == versionID {
		return nil
	}

	isAddedVersion := false
	for _, update := range b.changes {
		if addUpdate, ok := update.(*AddViewVersionUpdate); ok {
			if addUpdate.ViewVersion.VersionID == versionID {
				isAddedVersion = true

				break
			}
		}
	}

	timestamp := time.Now().UnixMilli()
	if isAddedVersion {
		timestamp = version.TimestampMs
	}

	b.historyEntry = &VersionLogEntry{
		VersionID:   versionID,
		TimestampMs: timestamp,
	}

	b.currentVersionID = versionID
	b.changes = append(b.changes, NewSetCurrentViewVersionUpdate(versionID))

	return nil
}

// SetCurrentVersion adds a version and its schema, then sets it as current.
func (b *MetadataBuilder) SetCurrentVersion(version *Version, schema *iceberg.Schema) error {
	if version == nil {
		return fmt.Errorf("%w: cannot set current version to nil", iceberg.ErrInvalidArgument)
	}

	if err := b.AddSchema(schema); err != nil {
		return err
	}

	newVersion := version.Copy()
	if b.lastAddedSchemaID != nil {
		newVersion.SchemaID = *b.lastAddedSchemaID
	}

	if err := b.AddVersion(newVersion); err != nil {
		return err
	}

	return b.SetCurrentVersionID(LastAdded)
}

func (b *MetadataBuilder) expireVersions() []*Version {
	historySize := PropertyVersionHistorySizeDefault
	if val, ok := b.properties[PropertyVersionHistorySize]; ok {
		if size, err := strconv.Atoi(val); err == nil && size > 0 {
			historySize = size
		}
	}

	protected := make(map[int64]bool)

	protected[b.currentVersionID] = true

	for _, update := range b.changes {
		if addUpdate, ok := update.(*AddViewVersionUpdate); ok {
			protected[addUpdate.ViewVersion.VersionID] = true
		}
	}

	sorted := make([]*Version, len(b.versions))
	copy(sorted, b.versions)
	slices.SortFunc(sorted, func(a, b *Version) int {
		return cmp.Compare(b.TimestampMs, a.TimestampMs)
	})

	kept := make([]*Version, 0, historySize)
	for _, v := range sorted {
		if protected[v.VersionID] || len(kept) < historySize {
			kept = append(kept, v)
		}
	}

	return kept
}

func (b *MetadataBuilder) updateHistory(history []VersionLogEntry, validIDs map[int64]bool) []VersionLogEntry {
	retained := make([]VersionLogEntry, 0, len(history))

	for _, entry := range history {
		if validIDs[entry.VersionID] {
			retained = append(retained, entry)
		} else {
			retained = retained[:0]
		}
	}

	return retained
}

func (b *MetadataBuilder) checkDialectDropped(prev, curr *Version) error {
	dropAllowed := b.properties[PropertyReplaceDropDialectAllowed] == "true"
	if dropAllowed {
		return nil
	}

	prevDialects := make(map[string]bool)
	for _, repr := range prev.Representations {
		prevDialects[strings.ToLower(repr.Dialect)] = true
	}

	for _, repr := range curr.Representations {
		delete(prevDialects, strings.ToLower(repr.Dialect))
	}

	if len(prevDialects) > 0 {
		missing := make([]string, 0, len(prevDialects))
		for dialect := range prevDialects {
			missing = append(missing, dialect)
		}
		slices.Sort(missing)

		return fmt.Errorf("%w: dropping dialects %v not allowed (set %s=true to allow)",
			ErrInvalidViewMetadata, missing, PropertyReplaceDropDialectAllowed)
	}

	return nil
}

// Build constructs the final view metadata.
func (b *MetadataBuilder) Build() (*ViewMetadataBuildResult, error) {
	if val, ok := b.properties[PropertyVersionHistorySize]; ok {
		if historySize, err := strconv.Atoi(val); err == nil && historySize <= 0 {
			return nil, fmt.Errorf("%w: %s must be positive but was %d",
				ErrInvalidViewMetadata, PropertyVersionHistorySize, historySize)
		}
	}

	if b.previousVersion != nil {
		currentVersion, _ := b.GetVersionByID(b.currentVersionID)
		if err := b.checkDialectDropped(b.previousVersion, currentVersion); err != nil {
			return nil, err
		}
	}

	keptVersions := b.expireVersions()

	validIDs := make(map[int64]bool)
	for _, v := range keptVersions {
		validIDs[v.VersionID] = true
	}

	history := b.history
	if b.historyEntry != nil {
		history = append(history, *b.historyEntry)
	}

	history = b.updateHistory(history, validIDs)

	versions := make([]Version, len(keptVersions))
	for i, v := range keptVersions {
		versions[i] = *v
	}

	schemas := make([]*iceberg.Schema, len(b.schemas))
	copy(schemas, b.schemas)

	properties := make(iceberg.Properties)
	for k, v := range b.properties {
		properties[k] = v
	}

	metadata := &metadata{
		UUID:             b.uuid,
		FmtVersion:       b.formatVersion,
		Loc:              b.location,
		SchemaList:       schemas,
		CurrentVersionId: b.currentVersionID,
		VersionList:      versions,
		VersionLogList:   history,
		Props:            properties,
	}

	if err := metadata.validate(); err != nil {
		return nil, err
	}

	return &ViewMetadataBuildResult{
		Metadata: metadata,
		Changes:  b.changes,
	}, nil
}
