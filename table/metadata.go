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
	"maps"
	"slices"
	"time"

	"github.com/apache/iceberg-go"

	"github.com/google/uuid"
)

const (
	PARTITION_FIELD_ID_START       = 1000
	SUPPORTED_TABLE_FORMAT_VERSION = 2
)

// Metadata for an iceberg table as specified in the Iceberg spec
//
// https://iceberg.apache.org/spec/#iceberg-table-spec
type Metadata interface {
	// Version indicates the version of this metadata, 1 for V1, 2 for V2, etc.
	Version() int
	// TableUUID returns a UUID that identifies the table, generated when the
	// table is created. Implementations must throw an exception if a table's
	// UUID does not match the expected UUID after refreshing metadata.
	TableUUID() uuid.UUID
	// Location is the table's base location. This is used by writers to determine
	// where to store data files, manifest files, and table metadata files.
	Location() string
	// LastUpdatedMillis is the timestamp in milliseconds from the unix epoch when
	// the table was last updated. Each table metadata file should update this
	// field just before writing.
	LastUpdatedMillis() int64
	// LastColumnID returns the highest assigned column ID for the table.
	// This is used to ensure fields are always assigned an unused ID when
	// evolving schemas.
	LastColumnID() int
	// Schemas returns the list of schemas, stored as objects with their
	// schema-id.
	Schemas() []*iceberg.Schema
	// CurrentSchema returns the table's current schema.
	CurrentSchema() *iceberg.Schema
	// PartitionSpecs returns the list of all partition specs in the table.
	PartitionSpecs() []iceberg.PartitionSpec
	// PartitionSpec returns the current partition spec that the table is using.
	PartitionSpec() iceberg.PartitionSpec
	// DefaultPartitionSpec is the ID of the current spec that writers should
	// use by default.
	DefaultPartitionSpec() int
	// LastPartitionSpecID is the highest assigned partition field ID across
	// all partition specs for the table. This is used to ensure partition
	// fields are always assigned an unused ID when evolving specs.
	LastPartitionSpecID() *int
	// Snapshots returns the list of valid snapshots. Valid snapshots are
	// snapshots for which all data files exist in the file system. A data
	// file must not be deleted from the file system until the last snapshot
	// in which it was listed is garbage collected.
	Snapshots() []Snapshot
	// SnapshotByID find and return a specific snapshot by its ID. Returns
	// nil if the ID is not found in the list of snapshots.
	SnapshotByID(int64) *Snapshot
	// SnapshotByName searches the list of snapshots for a snapshot with a given
	// ref name. Returns nil if there's no ref with this name for a snapshot.
	SnapshotByName(name string) *Snapshot
	// CurrentSnapshot returns the table's current snapshot.
	CurrentSnapshot() *Snapshot
	// Ref returns the snapshot ref for the main branch.
	Ref() SnapshotRef
	// Refs returns a map of snapshot refs by name.
	Refs() map[string]SnapshotRef
	// SnapshotLogs returns the list of snapshot logs for the table.
	SnapshotLogs() []SnapshotLogEntry
	// SortOrder returns the table's current sort order, ie: the one with the
	// ID that matches the default-sort-order-id.
	SortOrder() SortOrder
	// SortOrders returns the list of sort orders in the table.
	SortOrders() []SortOrder
	// DefaultSortOrder returns the ID of the current sort order that writers
	// should use by default.
	DefaultSortOrder() int
	// Properties is a string to string map of table properties. This is used
	// to control settings that affect reading and writing and is not intended
	// to be used for arbitrary metadata. For example, commit.retry.num-retries
	// is used to control the number of commit retries.
	Properties() iceberg.Properties
	// PreviousFiles returns the list of metadata log entries for the table.
	PreviousFiles() []MetadataLogEntry

	Equals(Metadata) bool
}

type MetadataBuilder struct {
	base    Metadata
	updates []Update

	// common fields
	formatVersion      int
	uuid               uuid.UUID
	loc                string
	lastUpdatedMS      int64
	lastColumnId       int
	schemaList         []*iceberg.Schema
	currentSchemaID    int
	specs              []iceberg.PartitionSpec
	defaultSpecID      int
	lastPartitionID    *int
	props              iceberg.Properties
	snapshotList       []Snapshot
	currentSnapshotID  *int64
	snapshotLog        []SnapshotLogEntry
	metadataLog        []MetadataLogEntry
	sortOrderList      []SortOrder
	defaultSortOrderID int
	refs               map[string]SnapshotRef

	// V2 specific
	lastSequenceNumber *int64
}

func NewMetadataBuilder() (*MetadataBuilder, error) {
	return &MetadataBuilder{
		updates:       make([]Update, 0),
		schemaList:    make([]*iceberg.Schema, 0),
		specs:         make([]iceberg.PartitionSpec, 0),
		props:         make(iceberg.Properties),
		snapshotList:  make([]Snapshot, 0),
		snapshotLog:   make([]SnapshotLogEntry, 0),
		metadataLog:   make([]MetadataLogEntry, 0),
		sortOrderList: make([]SortOrder, 0),
		refs:          make(map[string]SnapshotRef),
	}, nil
}

func MetadataBuilderFromBase(metadata Metadata) (*MetadataBuilder, error) {
	b := &MetadataBuilder{}
	b.base = metadata

	b.formatVersion = metadata.Version()
	b.uuid = metadata.TableUUID()
	b.loc = metadata.Location()
	b.lastUpdatedMS = metadata.LastUpdatedMillis()
	b.lastColumnId = metadata.LastColumnID()
	b.schemaList = metadata.Schemas()
	b.currentSchemaID = metadata.CurrentSchema().ID
	b.specs = metadata.PartitionSpecs()
	b.defaultSpecID = metadata.DefaultPartitionSpec()
	b.lastPartitionID = metadata.LastPartitionSpecID()
	b.props = metadata.Properties()
	b.snapshotList = metadata.Snapshots()
	b.currentSnapshotID = &metadata.CurrentSnapshot().SnapshotID
	b.sortOrderList = metadata.SortOrders()
	b.defaultSortOrderID = metadata.DefaultSortOrder()
	b.refs = metadata.Refs()
	b.snapshotLog = metadata.SnapshotLogs()
	b.metadataLog = metadata.PreviousFiles()

	return b, nil
}

func (b *MetadataBuilder) AddSchema(schema *iceberg.Schema, newLastColumnID int, initial bool) (*MetadataBuilder, error) {
	if newLastColumnID < b.lastColumnId {
		return nil, fmt.Errorf("%w: newLastColumnID %d, must be >= %d", iceberg.ErrInvalidArgument, newLastColumnID, b.lastColumnId)
	}

	var schemas []*iceberg.Schema
	if initial {
		schemas = []*iceberg.Schema{schema}
	} else {
		schemas = append(b.schemaList, schema)
	}

	b.lastColumnId = newLastColumnID
	b.schemaList = schemas
	b.updates = append(b.updates, NewAddSchemaUpdate(schema, newLastColumnID, initial))

	return b, nil
}

func (b *MetadataBuilder) AddPartitionSpec(spec *iceberg.PartitionSpec, initial bool) (*MetadataBuilder, error) {
	for _, s := range b.specs {
		if s.ID() == spec.ID() && !initial {
			return nil, fmt.Errorf("partition spec with id %d already exists", spec.ID())
		}
	}

	var maxFieldID int
	if len(spec.Fields()) > 0 {
		maxField := slices.MaxFunc(spec.Fields(), func(a, b iceberg.PartitionField) int {
			return a.FieldID - b.FieldID
		})
		maxFieldID = maxField.FieldID
	}

	prev := PARTITION_FIELD_ID_START - 1
	if b.lastPartitionID != nil {
		prev = *b.lastPartitionID
	}
	lastPartitionID := max(maxFieldID, prev)

	var specs []iceberg.PartitionSpec
	if initial {
		specs = []iceberg.PartitionSpec{*spec}
	} else {
		specs = append(b.specs, *spec)
	}

	b.specs = specs
	b.lastPartitionID = &lastPartitionID
	b.updates = append(b.updates, NewAddPartitionSpecUpdate(spec, initial))

	return b, nil
}

func (b *MetadataBuilder) AddSnapshot(snapshot *Snapshot) (*MetadataBuilder, error) {
	if snapshot == nil {
		return nil, nil
	}

	if len(b.schemaList) == 0 {
		return nil, errors.New("can't add snapshot with no added schemas")
	} else if len(b.specs) == 0 {
		return nil, errors.New("can't add snapshot with no added partition specs")
	} else if len(b.sortOrderList) == 0 {
		return nil, errors.New("can't add snapshot with no added sort orders")
	} else if s, _ := b.SnapshotByID(snapshot.SnapshotID); s != nil {
		return nil, fmt.Errorf("can't add snapshot with id %d, already exists", snapshot.SnapshotID)
	} else if b.formatVersion == 2 &&
		snapshot.SequenceNumber > 0 &&
		snapshot.SequenceNumber <= *b.lastSequenceNumber &&
		snapshot.ParentSnapshotID != nil {
		return nil, fmt.Errorf("can't add snapshot with sequence number %d, must be > than last sequence number %d",
			snapshot.SequenceNumber, b.lastSequenceNumber)
	}

	b.updates = append(b.updates, NewAddSnapshotUpdate(snapshot))
	b.lastUpdatedMS = snapshot.TimestampMs
	b.lastSequenceNumber = &snapshot.SequenceNumber
	b.snapshotList = append(b.snapshotList, *snapshot)
	return b, nil
}

func (b *MetadataBuilder) AddSortOrder(sortOrder *SortOrder, initial bool) (*MetadataBuilder, error) {
	var sortOrders []SortOrder
	if initial {
		sortOrders = []SortOrder{*sortOrder}
	} else {
		sortOrders = append(b.sortOrderList, *sortOrder)
	}

	b.sortOrderList = sortOrders
	b.updates = append(b.updates, NewAddSortOrderUpdate(sortOrder, initial))

	return b, nil
}

func (b *MetadataBuilder) RemoveProperties(keys []string) (*MetadataBuilder, error) {
	if len(keys) == 0 {
		return b, nil
	}

	b.updates = append(b.updates, NewRemovePropertiesUpdate(keys))
	for _, key := range keys {
		delete(b.props, key)
	}

	return b, nil
}

func (b *MetadataBuilder) SetCurrentSchemaID(currentSchemaID int) (*MetadataBuilder, error) {
	if currentSchemaID == -1 {
		currentSchemaID = b.MaxSchemaID()
		if !b.isAddedSchemaID(currentSchemaID) {
			return nil, errors.New("can't set current schema to last added schema, no schema has been added")
		}
	}

	if currentSchemaID == b.currentSchemaID {
		return b, nil
	}

	_, err := b.GetSchemaByID(currentSchemaID)
	if err != nil {
		return nil, fmt.Errorf("can't set current schema to schema with id %d: %w", currentSchemaID, err)
	}

	b.updates = append(b.updates, NewSetCurrentSchemaUpdate(currentSchemaID))
	b.currentSchemaID = currentSchemaID
	return b, nil
}

func (b *MetadataBuilder) SetDefaultSortOrderID(defaultSortOrderID int) (*MetadataBuilder, error) {
	if defaultSortOrderID == -1 {
		defaultSortOrderID = b.MaxSortOrderID()
		if !b.isAddedSortOrder(defaultSortOrderID) {
			return nil, fmt.Errorf("can't set default sort order to last added with no added sort orders")
		}
	}

	if defaultSortOrderID == b.defaultSortOrderID {
		return b, nil
	}

	if _, err := b.GetSortOrderByID(defaultSortOrderID); err != nil {
		return nil, fmt.Errorf("can't set default sort order to sort order with id %d: %w", defaultSortOrderID, err)
	}

	b.updates = append(b.updates, NewSetDefaultSortOrderUpdate(defaultSortOrderID))
	b.defaultSortOrderID = defaultSortOrderID
	return b, nil
}

func (b *MetadataBuilder) SetDefaultSpecID(defaultSpecID int) (*MetadataBuilder, error) {
	if defaultSpecID == -1 {
		defaultSpecID = b.MaxSpecID()
		if !b.isAddedSpecID(defaultSpecID) {
			return nil, fmt.Errorf("can't set default spec to last added with no added partition specs")
		}
	}

	if defaultSpecID == b.defaultSpecID {
		return b, nil
	}

	if _, err := b.GetSpecByID(defaultSpecID); err != nil {
		return nil, fmt.Errorf("can't set default spec to spec with id %d: %w", defaultSpecID, err)
	}

	b.updates = append(b.updates, NewSetDefaultSpecUpdate(defaultSpecID))
	b.defaultSpecID = defaultSpecID
	return b, nil
}

func (b *MetadataBuilder) SetFormatVersion(formatVersion int) (*MetadataBuilder, error) {
	if formatVersion < b.formatVersion {
		return nil, fmt.Errorf("downgrading format version from %d to %d is not allowed",
			b.formatVersion, formatVersion)
	}

	if formatVersion > SUPPORTED_TABLE_FORMAT_VERSION {
		return nil, fmt.Errorf("unsupported format version %d", formatVersion)
	}

	if formatVersion == b.formatVersion {
		return b, nil
	}

	b.updates = append(b.updates, NewUpgradeFormatVersionUpdate(formatVersion))
	b.formatVersion = formatVersion
	return b, nil
}

func (b *MetadataBuilder) SetLoc(loc string) (*MetadataBuilder, error) {
	if b.loc == loc {
		return b, nil
	}

	b.updates = append(b.updates, NewSetLocationUpdate(loc))
	b.loc = loc
	return b, nil
}

func (b *MetadataBuilder) SetProperties(props iceberg.Properties) (*MetadataBuilder, error) {
	if len(props) == 0 {
		return b, nil
	}

	b.updates = append(b.updates, NewSetPropertiesUpdate(props))
	maps.Copy(b.props, props)
	return b, nil
}

func (b *MetadataBuilder) SetSnapshotRef(
	name string,
	snapshotID int64,
	refType RefType,
	maxRefAgeMs, maxSnapshotAgeMs *int64,
	minSnapshotsToKeep *int,
) (*MetadataBuilder, error) {
	ref := SnapshotRef{
		SnapshotID:         snapshotID,
		SnapshotRefType:    refType,
		MinSnapshotsToKeep: minSnapshotsToKeep,
		MaxRefAgeMs:        maxRefAgeMs,
		MaxSnapshotAgeMs:   maxSnapshotAgeMs,
	}

	if existingRef, ok := b.refs[name]; ok && existingRef.Equals(ref) {
		return b, nil
	}

	snapshot, err := b.SnapshotByID(snapshotID)
	if err != nil {
		return nil, fmt.Errorf("can't set snapshot ref %s to unknown snapshot %d: %w", name, snapshotID, err)
	}

	b.updates = append(b.updates, NewSetSnapshotRefUpdate(name, snapshotID, refType, maxRefAgeMs, maxSnapshotAgeMs, minSnapshotsToKeep))
	if refType == MainBranch {
		b.currentSnapshotID = &snapshotID
		b.snapshotLog = append(b.snapshotLog, SnapshotLogEntry{
			SnapshotID:  snapshotID,
			TimestampMs: snapshot.TimestampMs,
		})
		b.lastUpdatedMS = time.Now().Local().UnixMilli()
	}

	if b.isAddedSnapshot(snapshotID) {
		b.lastUpdatedMS = snapshot.TimestampMs
	}

	b.refs[name] = SnapshotRef{
		SnapshotID:         snapshotID,
		SnapshotRefType:    refType,
		MinSnapshotsToKeep: minSnapshotsToKeep,
		MaxRefAgeMs:        maxRefAgeMs,
		MaxSnapshotAgeMs:   maxSnapshotAgeMs,
	}
	return b, nil
}

func (b *MetadataBuilder) SetUUID(uuid uuid.UUID) (*MetadataBuilder, error) {
	if b.uuid == uuid {
		return b, nil
	}

	b.updates = append(b.updates, NewAssignUUIDUpdate(uuid))
	b.uuid = uuid
	return b, nil
}

func (b *MetadataBuilder) buildCommonMetadata() *commonMetadata {
	return &commonMetadata{
		FormatVersion:      b.formatVersion,
		UUID:               b.uuid,
		Loc:                b.loc,
		LastUpdatedMS:      b.lastUpdatedMS,
		LastColumnId:       b.lastColumnId,
		SchemaList:         b.schemaList,
		CurrentSchemaID:    b.currentSchemaID,
		Specs:              b.specs,
		DefaultSpecID:      b.defaultSpecID,
		LastPartitionID:    b.lastPartitionID,
		Props:              b.props,
		SnapshotList:       b.snapshotList,
		CurrentSnapshotID:  b.currentSnapshotID,
		SnapshotLog:        b.snapshotLog,
		MetadataLog:        b.metadataLog,
		SortOrderList:      b.sortOrderList,
		DefaultSortOrderID: b.defaultSortOrderID,
		SnapshotRefs:       b.refs,
	}
}

func (b *MetadataBuilder) GetSchemaByID(id int) (*iceberg.Schema, error) {
	for _, s := range b.schemaList {
		if s.ID == id {
			return s, nil
		}
	}

	return nil, fmt.Errorf("schema with id %d not found", id)
}

func (b *MetadataBuilder) GetSpecByID(id int) (*iceberg.PartitionSpec, error) {
	for _, s := range b.specs {
		if s.ID() == id {
			return &s, nil
		}
	}

	return nil, fmt.Errorf("partition spec with id %d not found", id)
}

func (b *MetadataBuilder) GetSortOrderByID(id int) (*SortOrder, error) {
	for _, s := range b.sortOrderList {
		if s.OrderID == id {
			return &s, nil
		}
	}

	return nil, fmt.Errorf("sort order with id %d not found", id)
}

func (b *MetadataBuilder) MaxSchemaID() int {
	max := 0
	for _, s := range b.schemaList {
		if s.ID > max {
			max = s.ID
		}
	}

	return max
}

func (b *MetadataBuilder) MaxSpecID() int {
	max := 0
	for _, s := range b.specs {
		if s.ID() > max {
			max = s.ID()
		}
	}

	return max
}

func (b *MetadataBuilder) MaxSortOrderID() int {
	max := 0
	for _, s := range b.sortOrderList {
		if s.OrderID > max {
			max = s.OrderID
		}
	}

	return max
}

func (b *MetadataBuilder) SnapshotByID(id int64) (*Snapshot, error) {
	for _, s := range b.snapshotList {
		if s.SnapshotID == id {
			return &s, nil
		}
	}

	return nil, fmt.Errorf("snapshot with id %d not found", id)
}

func (b *MetadataBuilder) isAddedSchemaID(id int) bool {
	for _, u := range b.updates {
		if u.Action() == "add-schema" &&
			u.(*AddSchemaUpdate).Schema.ID == id {
			return true
		}
	}

	return false
}

func (b *MetadataBuilder) isAddedSnapshot(id int64) bool {
	for _, u := range b.updates {
		if u.Action() == "add-snapshot" &&
			u.(*AddSnapshotUpdate).Snapshot.SnapshotID == id {
			return true
		}
	}

	return false
}

func (b *MetadataBuilder) isAddedSpecID(id int) bool {
	for _, u := range b.updates {
		if u.Action() == "add-partition-spec" &&
			u.(*AddPartitionSpecUpdate).Spec.ID() == id {
			return true
		}

	}

	return false
}

func (b *MetadataBuilder) isAddedSortOrder(id int) bool {
	for _, u := range b.updates {
		if u.Action() == "add-sort-order" &&
			u.(*AddSortOrderUpdate).SortOrder.OrderID == id {
			return true
		}
	}

	return false
}

func (b *MetadataBuilder) Build() (Metadata, error) {
	common := b.buildCommonMetadata()
	switch b.formatVersion {
	case 1:
		schema, err := b.GetSchemaByID(b.currentSchemaID)
		if err != nil {
			return nil, fmt.Errorf("can't build metadata, missing schema for schema ID %d: %w", b.currentSchemaID, err)
		}
		partition, err := b.GetSpecByID(b.defaultSpecID)
		if err != nil {
			return nil, fmt.Errorf("can't build metadata, missing partition spec for spec ID %d: %w", b.defaultSpecID, err)
		}
		return &metadataV1{
			Schema:         schema,
			Partition:      partition.Fields(),
			commonMetadata: *common,
		}, nil
	case 2:
		return &metadataV2{
			LastSequenceNumber: *b.lastSequenceNumber,
			commonMetadata:     *common,
		}, nil
	default:
		panic("unreachable: invalid format version")
	}
}

var (
	ErrInvalidMetadataFormatVersion = errors.New("invalid or missing format-version in table metadata")
	ErrInvalidMetadata              = errors.New("invalid metadata")
)

// ParseMetadata parses json metadata provided by the passed in reader,
// returning an error if one is encountered.
func ParseMetadata(r io.Reader) (Metadata, error) {
	data, err := io.ReadAll(r)
	if err != nil {
		return nil, err
	}

	return ParseMetadataBytes(data)
}

// ParseMetadataString is like [ParseMetadata], but for a string rather than
// an io.Reader.
func ParseMetadataString(s string) (Metadata, error) {
	return ParseMetadataBytes([]byte(s))
}

// ParseMetadataBytes is like [ParseMetadataString] but for a byte slice.
func ParseMetadataBytes(b []byte) (Metadata, error) {
	ver := struct {
		FormatVersion int `json:"format-version"`
	}{}
	if err := json.Unmarshal(b, &ver); err != nil {
		return nil, err
	}

	var ret Metadata
	switch ver.FormatVersion {
	case 1:
		ret = &metadataV1{}
	case 2:
		ret = &metadataV2{}
	default:
		return nil, ErrInvalidMetadataFormatVersion
	}

	return ret, json.Unmarshal(b, ret)
}

func sliceEqualHelper[T interface{ Equals(T) bool }](s1, s2 []T) bool {
	return slices.EqualFunc(s1, s2, func(t1, t2 T) bool {
		return t1.Equals(t2)
	})
}

// https://iceberg.apache.org/spec/#iceberg-table-spec
type commonMetadata struct {
	FormatVersion      int                     `json:"format-version"`
	UUID               uuid.UUID               `json:"table-uuid"`
	Loc                string                  `json:"location"`
	LastUpdatedMS      int64                   `json:"last-updated-ms"`
	LastColumnId       int                     `json:"last-column-id"`
	SchemaList         []*iceberg.Schema       `json:"schemas"`
	CurrentSchemaID    int                     `json:"current-schema-id"`
	Specs              []iceberg.PartitionSpec `json:"partition-specs"`
	DefaultSpecID      int                     `json:"default-spec-id"`
	LastPartitionID    *int                    `json:"last-partition-id,omitempty"`
	Props              iceberg.Properties      `json:"properties"`
	SnapshotList       []Snapshot              `json:"snapshots,omitempty"`
	CurrentSnapshotID  *int64                  `json:"current-snapshot-id,omitempty"`
	SnapshotLog        []SnapshotLogEntry      `json:"snapshot-log"`
	MetadataLog        []MetadataLogEntry      `json:"metadata-log"`
	SortOrderList      []SortOrder             `json:"sort-orders"`
	DefaultSortOrderID int                     `json:"default-sort-order-id"`
	SnapshotRefs       map[string]SnapshotRef  `json:"refs"`
}

func (c *commonMetadata) Ref() SnapshotRef                  { return c.SnapshotRefs[MainBranch] }
func (c *commonMetadata) Refs() map[string]SnapshotRef      { return c.SnapshotRefs }
func (c *commonMetadata) SnapshotLogs() []SnapshotLogEntry  { return c.SnapshotLog }
func (c *commonMetadata) PreviousFiles() []MetadataLogEntry { return c.MetadataLog }
func (c *commonMetadata) Equals(other *commonMetadata) bool {
	switch {
	case c.LastPartitionID == nil && other.LastPartitionID != nil:
		fallthrough
	case c.LastPartitionID != nil && other.LastPartitionID == nil:
		fallthrough
	case c.CurrentSnapshotID == nil && other.CurrentSnapshotID != nil:
		fallthrough
	case c.CurrentSnapshotID != nil && other.CurrentSnapshotID == nil:
		return false
	}

	switch {
	case !sliceEqualHelper(c.SchemaList, other.SchemaList):
		fallthrough
	case !sliceEqualHelper(c.SnapshotList, other.SnapshotList):
		fallthrough
	case !sliceEqualHelper(c.Specs, other.Specs):
		fallthrough
	case !maps.Equal(c.Props, other.Props):
		fallthrough
	case !maps.EqualFunc(c.SnapshotRefs, other.SnapshotRefs, func(sr1, sr2 SnapshotRef) bool { return sr1.Equals(sr2) }):
		return false
	}

	return c.FormatVersion == other.FormatVersion && c.UUID == other.UUID &&
		((c.LastPartitionID == other.LastPartitionID) || (*c.LastPartitionID == *other.LastPartitionID)) &&
		((c.CurrentSnapshotID == other.CurrentSnapshotID) || (*c.CurrentSnapshotID == *other.CurrentSnapshotID)) &&
		c.Loc == other.Loc && c.LastUpdatedMS == other.LastUpdatedMS &&
		c.LastColumnId == other.LastColumnId && c.CurrentSchemaID == other.CurrentSchemaID &&
		c.DefaultSpecID == other.DefaultSpecID && c.DefaultSortOrderID == other.DefaultSortOrderID &&
		slices.Equal(c.SnapshotLog, other.SnapshotLog) && slices.Equal(c.MetadataLog, other.MetadataLog) &&
		sliceEqualHelper(c.SortOrderList, other.SortOrderList)

}

func (c *commonMetadata) TableUUID() uuid.UUID       { return c.UUID }
func (c *commonMetadata) Location() string           { return c.Loc }
func (c *commonMetadata) LastUpdatedMillis() int64   { return c.LastUpdatedMS }
func (c *commonMetadata) LastColumnID() int          { return c.LastColumnId }
func (c *commonMetadata) Schemas() []*iceberg.Schema { return c.SchemaList }
func (c *commonMetadata) CurrentSchema() *iceberg.Schema {
	for _, s := range c.SchemaList {
		if s.ID == c.CurrentSchemaID {
			return s
		}
	}
	panic("should never get here")
}

func (c *commonMetadata) PartitionSpecs() []iceberg.PartitionSpec {
	return c.Specs
}

func (c *commonMetadata) DefaultPartitionSpec() int {
	return c.DefaultSpecID
}

func (c *commonMetadata) PartitionSpec() iceberg.PartitionSpec {
	for _, s := range c.Specs {
		if s.ID() == c.DefaultSpecID {
			return s
		}
	}
	return *iceberg.UnpartitionedSpec
}

func (c *commonMetadata) LastPartitionSpecID() *int { return c.LastPartitionID }
func (c *commonMetadata) Snapshots() []Snapshot     { return c.SnapshotList }
func (c *commonMetadata) SnapshotByID(id int64) *Snapshot {
	for i := range c.SnapshotList {
		if c.SnapshotList[i].SnapshotID == id {
			return &c.SnapshotList[i]
		}
	}
	return nil
}

func (c *commonMetadata) SnapshotByName(name string) *Snapshot {
	if ref, ok := c.SnapshotRefs[name]; ok {
		return c.SnapshotByID(ref.SnapshotID)
	}
	return nil
}

func (c *commonMetadata) CurrentSnapshot() *Snapshot {
	if c.CurrentSnapshotID == nil {
		return nil
	}
	return c.SnapshotByID(*c.CurrentSnapshotID)
}

func (c *commonMetadata) SortOrders() []SortOrder { return c.SortOrderList }
func (c *commonMetadata) SortOrder() SortOrder {
	for _, s := range c.SortOrderList {
		if s.OrderID == c.DefaultSortOrderID {
			return s
		}
	}
	return UnsortedSortOrder
}

func (c *commonMetadata) DefaultSortOrder() int {
	return c.DefaultSortOrderID
}

func (c *commonMetadata) Properties() iceberg.Properties {
	return c.Props
}

// preValidate updates values in the metadata struct with defaults based on
// combinations of struct members. Such as initializing slices as empty slices
// if they were null in the metadata, or normalizing inconsistencies between
// metadata versions.
func (c *commonMetadata) preValidate() {
	if c.CurrentSnapshotID != nil && *c.CurrentSnapshotID == -1 {
		// treat -1 as the same as nil, clean this up in pre-validation
		// to make the validation logic simplified later
		c.CurrentSnapshotID = nil
	}

	if c.CurrentSnapshotID != nil {
		if _, ok := c.SnapshotRefs[MainBranch]; !ok {
			c.SnapshotRefs[MainBranch] = SnapshotRef{
				SnapshotID:      *c.CurrentSnapshotID,
				SnapshotRefType: BranchRef,
			}
		}
	}

	if c.MetadataLog == nil {
		c.MetadataLog = []MetadataLogEntry{}
	}

	if c.SnapshotRefs == nil {
		c.SnapshotRefs = make(map[string]SnapshotRef)
	}

	if c.SnapshotLog == nil {
		c.SnapshotLog = []SnapshotLogEntry{}
	}
}

func (c *commonMetadata) checkSchemas() error {
	// check that current-schema-id is present in schemas
	for _, s := range c.SchemaList {
		if s.ID == c.CurrentSchemaID {
			return nil
		}
	}

	return fmt.Errorf("%w: current-schema-id %d can't be found in any schema",
		ErrInvalidMetadata, c.CurrentSchemaID)
}

func (c *commonMetadata) checkPartitionSpecs() error {
	for _, spec := range c.Specs {
		if spec.ID() == c.DefaultSpecID {
			return nil
		}
	}

	return fmt.Errorf("%w: default-spec-id %d can't be found",
		ErrInvalidMetadata, c.DefaultSpecID)
}

func (c *commonMetadata) checkSortOrders() error {
	if c.DefaultSortOrderID == UnsortedSortOrderID {
		return nil
	}

	for _, o := range c.SortOrderList {
		if o.OrderID == c.DefaultSortOrderID {
			return nil
		}
	}

	return fmt.Errorf("%w: default-sort-order-id %d can't be found in %+v",
		ErrInvalidMetadata, c.DefaultSortOrderID, c.SortOrderList)
}

func (c *commonMetadata) validate() error {
	if err := c.checkSchemas(); err != nil {
		return err
	}

	if err := c.checkPartitionSpecs(); err != nil {
		return err
	}

	if err := c.checkSortOrders(); err != nil {
		return err
	}

	switch {
	case c.LastUpdatedMS == 0:
		// last-updated-ms is required
		return fmt.Errorf("%w: missing last-updated-ms", ErrInvalidMetadata)
	case c.LastColumnId == 0:
		// last-column-id is required
		return fmt.Errorf("%w: missing last-column-id", ErrInvalidMetadata)
	}

	return nil
}

func (c *commonMetadata) Version() int { return c.FormatVersion }

type metadataV1 struct {
	Schema    *iceberg.Schema          `json:"schema"`
	Partition []iceberg.PartitionField `json:"partition-spec"`

	commonMetadata
}

func (m *metadataV1) Equals(other Metadata) bool {
	rhs, ok := other.(*metadataV1)
	if !ok {
		return false
	}

	return m.Schema.Equals(rhs.Schema) && slices.Equal(m.Partition, rhs.Partition) &&
		m.commonMetadata.Equals(&rhs.commonMetadata)
}

func (m *metadataV1) preValidate() {
	if len(m.SchemaList) == 0 {
		m.SchemaList = []*iceberg.Schema{m.Schema}
	}

	if len(m.Specs) == 0 {
		m.Specs = []iceberg.PartitionSpec{
			iceberg.NewPartitionSpec(m.Partition...)}
		m.DefaultSpecID = m.Specs[0].ID()
	}

	if m.LastPartitionID == nil {
		id := m.Specs[0].LastAssignedFieldID()
		for _, spec := range m.Specs[1:] {
			last := spec.LastAssignedFieldID()
			if last > id {
				id = last
			}
		}
		m.LastPartitionID = &id
	}

	if len(m.SortOrderList) == 0 {
		m.SortOrderList = []SortOrder{UnsortedSortOrder}
	}

	m.commonMetadata.preValidate()
}

func (m *metadataV1) UnmarshalJSON(b []byte) error {
	type Alias metadataV1
	aux := (*Alias)(m)

	if err := json.Unmarshal(b, aux); err != nil {
		return err
	}

	m.preValidate()
	return m.validate()
}

func (m *metadataV1) ToV2() metadataV2 {
	commonOut := m.commonMetadata
	commonOut.FormatVersion = 2
	if commonOut.UUID.String() == "" {
		commonOut.UUID = uuid.New()
	}

	return metadataV2{commonMetadata: commonOut}
}

type metadataV2 struct {
	LastSequenceNumber int64 `json:"last-sequence-number"`

	commonMetadata
}

func (m *metadataV2) Equals(other Metadata) bool {
	rhs, ok := other.(*metadataV2)
	if !ok {
		return false
	}

	return m.LastSequenceNumber == rhs.LastSequenceNumber &&
		m.commonMetadata.Equals(&rhs.commonMetadata)
}

func (m *metadataV2) UnmarshalJSON(b []byte) error {
	type Alias metadataV2
	aux := (*Alias)(m)

	if err := json.Unmarshal(b, aux); err != nil {
		return err
	}

	m.preValidate()
	return m.validate()
}
