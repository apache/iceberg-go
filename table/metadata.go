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
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"iter"
	"maps"
	"slices"
	"strconv"
	"time"

	"github.com/apache/iceberg-go"

	"github.com/google/uuid"
)

const (
	partitionFieldStartID       = 1000
	supportedTableFormatVersion = 2
)

func generateSnapshotID() int64 {
	var (
		rndUUID = uuid.New()
		out     [8]byte
	)

	for i := range 8 {
		lhs, rhs := rndUUID[i], rndUUID[i+8]
		out[i] = lhs ^ rhs
	}

	snapshotID := int64(binary.LittleEndian.Uint64(out[:]))
	if snapshotID < 0 {
		snapshotID = -snapshotID
	}

	return snapshotID
}

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
	// Refs returns a list of snapshot name/reference pairs.
	Refs() iter.Seq2[string, SnapshotRef]
	// SnapshotLogs returns the list of snapshot logs for the table.
	SnapshotLogs() iter.Seq[SnapshotLogEntry]
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
	PreviousFiles() iter.Seq[MetadataLogEntry]
	Equals(Metadata) bool

	NameMapping() iceberg.NameMapping

	LastSequenceNumber() int64
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

	// >v1 specific
	lastSequenceNumber *int64
	// update tracking
	lastAddedSchemaID *int
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
	b.schemaList = slices.Clone(metadata.Schemas())
	b.currentSchemaID = metadata.CurrentSchema().ID
	b.specs = slices.Clone(metadata.PartitionSpecs())
	defaultSpecID := metadata.DefaultPartitionSpec()
	b.defaultSpecID = defaultSpecID
	b.lastPartitionID = metadata.LastPartitionSpecID()
	b.props = maps.Clone(metadata.Properties())
	b.snapshotList = slices.Clone(metadata.Snapshots())
	b.sortOrderList = slices.Clone(metadata.SortOrders())
	b.defaultSortOrderID = metadata.DefaultSortOrder()
	if metadata.Version() > 1 {
		seq := metadata.LastSequenceNumber()
		b.lastSequenceNumber = &seq
	}

	if metadata.CurrentSnapshot() != nil {
		b.currentSnapshotID = &metadata.CurrentSnapshot().SnapshotID
	}

	b.refs = maps.Collect(metadata.Refs())
	b.snapshotLog = slices.Collect(metadata.SnapshotLogs())
	b.metadataLog = slices.Collect(metadata.PreviousFiles())

	return b, nil
}

func (b *MetadataBuilder) HasChanges() bool { return len(b.updates) > 0 }

func (b *MetadataBuilder) CurrentSpec() (*iceberg.PartitionSpec, error) {
	return b.GetSpecByID(b.defaultSpecID)
}

func (b *MetadataBuilder) CurrentSchema() *iceberg.Schema {
	s, _ := b.GetSchemaByID(b.currentSchemaID)

	return s
}

func (b *MetadataBuilder) LastUpdatedMS() int64 { return b.lastUpdatedMS }

func (b *MetadataBuilder) nextSequenceNumber() int64 {
	if b.formatVersion > 1 {
		if b.lastSequenceNumber == nil {
			return 0
		}

		return *b.lastSequenceNumber + 1
	}

	return 0
}

func (b *MetadataBuilder) newSnapshotID() int64 {
	snapshotID := generateSnapshotID()
	for slices.ContainsFunc(b.snapshotList, func(s Snapshot) bool { return s.SnapshotID == snapshotID }) {
		snapshotID = generateSnapshotID()
	}

	return snapshotID
}

func (b *MetadataBuilder) currentSnapshot() *Snapshot {
	if b.currentSnapshotID == nil {
		return nil
	}

	s, _ := b.SnapshotByID(*b.currentSnapshotID)

	return s
}

func (b *MetadataBuilder) AddSchema(schema *iceberg.Schema) (*MetadataBuilder, error) {
	newSchemaID := b.reuseOrCreateNewSchemaID(schema)

	if _, err := b.GetSchemaByID(newSchemaID); err == nil {
		if b.lastAddedSchemaID == nil || *b.lastAddedSchemaID != newSchemaID {
			b.updates = append(b.updates, NewAddSchemaUpdate(schema))
			b.lastAddedSchemaID = &newSchemaID
		}

		return b, nil
	}

	b.lastColumnId = max(b.lastColumnId, schema.HighestFieldID())

	schema.ID = newSchemaID

	b.schemaList = append(b.schemaList, schema)
	b.updates = append(b.updates, NewAddSchemaUpdate(schema))
	b.lastAddedSchemaID = &newSchemaID

	return b, nil
}

func (b *MetadataBuilder) AddPartitionSpec(spec *iceberg.PartitionSpec, initial bool) (*MetadataBuilder, error) {
	for _, s := range b.specs {
		if s.ID() == spec.ID() && !initial {
			return nil, fmt.Errorf("partition spec with id %d already exists", spec.ID())
		}
	}

	maxFieldID := 0
	for f := range spec.Fields() {
		maxFieldID = max(maxFieldID, f.FieldID)
	}

	prev := partitionFieldStartID - 1
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
	} else if s, _ := b.SnapshotByID(snapshot.SnapshotID); s != nil {
		return nil, fmt.Errorf("can't add snapshot with id %d, already exists", snapshot.SnapshotID)
	} else if b.formatVersion == 2 &&
		snapshot.SequenceNumber > 0 &&
		snapshot.ParentSnapshotID != nil &&
		snapshot.SequenceNumber <= *b.lastSequenceNumber {
		return nil, fmt.Errorf("can't add snapshot with sequence number %d, must be > than last sequence number %d",
			snapshot.SequenceNumber, b.lastSequenceNumber)
	}

	b.updates = append(b.updates, NewAddSnapshotUpdate(snapshot))
	b.lastUpdatedMS = snapshot.TimestampMs
	b.lastSequenceNumber = &snapshot.SequenceNumber
	b.snapshotList = append(b.snapshotList, *snapshot)

	return b, nil
}

func (b *MetadataBuilder) RemoveSnapshots(snapshotIds []int64) (*MetadataBuilder, error) {
	if slices.Contains(snapshotIds, *b.currentSnapshotID) {
		return nil, errors.New("current snapshot cannot be removed")
	}

	b.snapshotList = slices.DeleteFunc(b.snapshotList, func(e Snapshot) bool {
		return slices.Contains(snapshotIds, e.SnapshotID)
	})
	b.snapshotLog = slices.DeleteFunc(b.snapshotLog, func(e SnapshotLogEntry) bool {
		return slices.Contains(snapshotIds, e.SnapshotID)
	})
	b.updates = append(b.updates, NewRemoveSnapshotsUpdate(snapshotIds))

	return b, nil
}

func (b *MetadataBuilder) AddSortOrder(sortOrder *SortOrder, initial bool) (*MetadataBuilder, error) {
	var sortOrders []SortOrder
	if !initial {
		sortOrders = append(sortOrders, b.sortOrderList...)
	}

	for _, s := range sortOrders {
		if s.OrderID == sortOrder.OrderID {
			return nil, fmt.Errorf("sort order with id %d already exists", sortOrder.OrderID)
		}
	}

	b.sortOrderList = append(sortOrders, *sortOrder)
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
		if b.lastAddedSchemaID == nil {
			return nil, errors.New("can't set current schema to last added schema, no schema has been added")
		}
		currentSchemaID = *b.lastAddedSchemaID
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
		defaultSortOrderID = maxBy(b.sortOrderList, func(s SortOrder) int {
			return s.OrderID
		})
		if !slices.ContainsFunc(b.updates, func(u Update) bool {
			return u.Action() == UpdateAddSortOrder && u.(*addSortOrderUpdate).SortOrder.OrderID == defaultSortOrderID
		}) {
			return nil, errors.New("can't set default sort order to last added with no added sort orders")
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
		defaultSpecID = maxBy(b.specs, func(s iceberg.PartitionSpec) int {
			return s.ID()
		})
		if !slices.ContainsFunc(b.updates, func(u Update) bool {
			return u.Action() == UpdateAddSpec && u.(*addPartitionSpecUpdate).Spec.ID() == defaultSpecID
		}) {
			return nil, errors.New("can't set default spec to last added with no added partition specs")
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

	if formatVersion > supportedTableFormatVersion {
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
	if b.props == nil {
		b.props = props
	} else {
		maps.Copy(b.props, props)
	}

	return b, nil
}

type setSnapshotRefOption func(*SnapshotRef) error

func WithMaxRefAgeMs(maxRefAgeMs int64) setSnapshotRefOption {
	return func(ref *SnapshotRef) error {
		if maxRefAgeMs <= 0 {
			return fmt.Errorf("%w: maxRefAgeMs %d, must be > 0", iceberg.ErrInvalidArgument, maxRefAgeMs)
		}
		ref.MaxRefAgeMs = &maxRefAgeMs

		return nil
	}
}

func WithMaxSnapshotAgeMs(maxSnapshotAgeMs int64) setSnapshotRefOption {
	return func(ref *SnapshotRef) error {
		if maxSnapshotAgeMs <= 0 {
			return fmt.Errorf("%w: maxSnapshotAgeMs %d, must be > 0", iceberg.ErrInvalidArgument, maxSnapshotAgeMs)
		}
		ref.MaxSnapshotAgeMs = &maxSnapshotAgeMs

		return nil
	}
}

func WithMinSnapshotsToKeep(minSnapshotsToKeep int) setSnapshotRefOption {
	return func(ref *SnapshotRef) error {
		if minSnapshotsToKeep <= 0 {
			return fmt.Errorf("%w: minSnapshotsToKeep %d, must be > 0", iceberg.ErrInvalidArgument, minSnapshotsToKeep)
		}
		ref.MinSnapshotsToKeep = &minSnapshotsToKeep

		return nil
	}
}

func (b *MetadataBuilder) SetSnapshotRef(
	name string,
	snapshotID int64,
	refType RefType,
	options ...setSnapshotRefOption,
) (*MetadataBuilder, error) {
	ref := SnapshotRef{
		SnapshotID:      snapshotID,
		SnapshotRefType: refType,
	}
	for _, opt := range options {
		if err := opt(&ref); err != nil {
			return nil, fmt.Errorf("invalid snapshot ref option: %w", err)
		}
	}

	var maxRefAgeMs, maxSnapshotAgeMs int64
	var minSnapshotsToKeep int
	if ref.MaxRefAgeMs != nil {
		maxRefAgeMs = *ref.MaxRefAgeMs
	}
	if ref.MaxSnapshotAgeMs != nil {
		maxSnapshotAgeMs = *ref.MaxSnapshotAgeMs
	}
	if ref.MinSnapshotsToKeep != nil {
		minSnapshotsToKeep = *ref.MinSnapshotsToKeep
	}

	if existingRef, ok := b.refs[name]; ok && existingRef.Equals(ref) {
		return b, nil
	}

	snapshot, err := b.SnapshotByID(snapshotID)
	if err != nil {
		return nil, fmt.Errorf("can't set snapshot ref %s to unknown snapshot %d: %w", name, snapshotID, err)
	}

	isAddedSnapshot := slices.ContainsFunc(b.updates, func(u Update) bool {
		return u.Action() == UpdateAddSnapshot && u.(*addSnapshotUpdate).Snapshot.SnapshotID == snapshotID
	})
	if isAddedSnapshot {
		b.lastUpdatedMS = snapshot.TimestampMs
	}

	b.updates = append(b.updates, NewSetSnapshotRefUpdate(name, snapshotID, refType, maxRefAgeMs, maxSnapshotAgeMs, minSnapshotsToKeep))
	if name == MainBranch {
		b.currentSnapshotID = &snapshotID
		if !isAddedSnapshot {
			b.lastUpdatedMS = time.Now().Local().UnixMilli()
		}
		b.snapshotLog = append(b.snapshotLog, SnapshotLogEntry{
			SnapshotID:  snapshotID,
			TimestampMs: b.lastUpdatedMS,
		})
	}

	if slices.ContainsFunc(b.updates, func(u Update) bool {
		return u.Action() == UpdateAddSnapshot && u.(*addSnapshotUpdate).Snapshot.SnapshotID == snapshotID
	}) {
		b.lastUpdatedMS = snapshot.TimestampMs
	}

	b.refs[name] = ref

	return b, nil
}

func (b *MetadataBuilder) RemoveSnapshotRef(name string) (*MetadataBuilder, error) {
	if _, found := b.refs[name]; !found {
		return nil, fmt.Errorf("snapshot ref not found: %s", name)
	}

	if name == MainBranch {
		b.currentSnapshotID = nil
		b.snapshotLog = b.snapshotLog[:0]
	}

	delete(b.refs, name)
	b.updates = append(b.updates, NewRemoveSnapshotRefUpdate(name))

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

func (b *MetadataBuilder) SetLastUpdatedMS() *MetadataBuilder {
	b.lastUpdatedMS = time.Now().UnixMilli()

	return b
}

func (b *MetadataBuilder) buildCommonMetadata() (*commonMetadata, error) {
	if _, err := b.GetSpecByID(b.defaultSpecID); err != nil {
		return nil, fmt.Errorf("defaultSpecID is invalid: %w", err)
	}
	defaultSpecID := b.defaultSpecID

	if b.lastUpdatedMS == 0 {
		b.lastUpdatedMS = time.Now().UnixMilli()
	}

	return &commonMetadata{
		FormatVersion:      b.formatVersion,
		UUID:               b.uuid,
		Loc:                b.loc,
		LastUpdatedMS:      b.lastUpdatedMS,
		LastColumnId:       b.lastColumnId,
		SchemaList:         b.schemaList,
		CurrentSchemaID:    b.currentSchemaID,
		Specs:              b.specs,
		DefaultSpecID:      defaultSpecID,
		LastPartitionID:    b.lastPartitionID,
		Props:              b.props,
		SnapshotList:       b.snapshotList,
		CurrentSnapshotID:  b.currentSnapshotID,
		SnapshotLog:        b.snapshotLog,
		MetadataLog:        b.metadataLog,
		SortOrderList:      b.sortOrderList,
		DefaultSortOrderID: b.defaultSortOrderID,
		SnapshotRefs:       b.refs,
	}, nil
}

func (b *MetadataBuilder) GetSchemaByID(id int) (*iceberg.Schema, error) {
	for _, s := range b.schemaList {
		if s.ID == id {
			return s, nil
		}
	}

	return nil, fmt.Errorf("%w: schema with id %d not found", iceberg.ErrInvalidArgument, id)
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

func (b *MetadataBuilder) SnapshotByID(id int64) (*Snapshot, error) {
	for _, s := range b.snapshotList {
		if s.SnapshotID == id {
			return &s, nil
		}
	}

	return nil, fmt.Errorf("snapshot with id %d not found", id)
}

func (b *MetadataBuilder) NameMapping() iceberg.NameMapping {
	if nameMappingJson, ok := b.props[DefaultNameMappingKey]; ok {
		nm := iceberg.NameMapping{}
		if err := json.Unmarshal([]byte(nameMappingJson), &nm); err == nil {
			return nm
		}
	}

	return nil
}

func (b *MetadataBuilder) TrimMetadataLogs(maxEntries int) *MetadataBuilder {
	if len(b.metadataLog) <= maxEntries {
		return b
	}

	b.metadataLog = b.metadataLog[len(b.metadataLog)-maxEntries:]

	return b
}

func (b *MetadataBuilder) AppendMetadataLog(entry MetadataLogEntry) *MetadataBuilder {
	b.metadataLog = append(b.metadataLog, entry)

	return b
}

func (b *MetadataBuilder) Build() (Metadata, error) {
	common, err := b.buildCommonMetadata()
	if err != nil {
		return nil, err
	}
	if err := common.validate(); err != nil {
		return nil, err
	}

	switch b.formatVersion {
	case 1:
		schema, err := b.GetSchemaByID(b.currentSchemaID)
		if err != nil {
			return nil, fmt.Errorf("can't build metadata, missing schema for schema ID %d: %w", b.currentSchemaID, err)
		}

		partition, err := b.GetSpecByID(common.DefaultSpecID)
		if err != nil {
			return nil, fmt.Errorf("can't build metadata, missing partition spec for spec ID %d: %w", b.defaultSpecID, err)
		}

		partitionFields := make([]iceberg.PartitionField, 0)
		for f := range partition.Fields() {
			partitionFields = append(partitionFields, f)
		}

		return &metadataV1{
			Schema:         schema,
			Partition:      partitionFields,
			commonMetadata: *common,
		}, nil

	case 2:
		var lastSequenceNumber int64

		if b.lastSequenceNumber != nil {
			lastSequenceNumber = *b.lastSequenceNumber
		}

		return &metadataV2{
			LastSeqNum:     lastSequenceNumber,
			commonMetadata: *common,
		}, nil

	default:
		return nil, fmt.Errorf("unknown format version %d", b.formatVersion)
	}
}

func (b *MetadataBuilder) reuseOrCreateNewSchemaID(newSchema *iceberg.Schema) int {
	newSchemaID := newSchema.ID
	for _, schema := range b.schemaList {
		if newSchema.Equals(schema) {
			return schema.ID
		}
		if schema.ID >= newSchemaID {
			newSchemaID = schema.ID + 1
		}
	}

	return newSchemaID
}

// maxBy returns the maximum value of extract(e) for all e in elems.
// If elems is empty, returns 0.
func maxBy[S ~[]E, E any](elems S, extract func(e E) int) int {
	m := 0
	for _, e := range elems {
		m = max(m, extract(e))
	}

	return m
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
	Props              iceberg.Properties      `json:"properties,omitempty"`
	SnapshotList       []Snapshot              `json:"snapshots,omitempty"`
	CurrentSnapshotID  *int64                  `json:"current-snapshot-id,omitempty"`
	SnapshotLog        []SnapshotLogEntry      `json:"snapshot-log,omitempty"`
	MetadataLog        []MetadataLogEntry      `json:"metadata-log,omitempty"`
	SortOrderList      []SortOrder             `json:"sort-orders"`
	DefaultSortOrderID int                     `json:"default-sort-order-id"`
	SnapshotRefs       map[string]SnapshotRef  `json:"refs,omitempty"`
}

func (c *commonMetadata) Ref() SnapshotRef                     { return c.SnapshotRefs[MainBranch] }
func (c *commonMetadata) Refs() iter.Seq2[string, SnapshotRef] { return maps.All(c.SnapshotRefs) }
func (c *commonMetadata) SnapshotLogs() iter.Seq[SnapshotLogEntry] {
	return slices.Values(c.SnapshotLog)
}

func (c *commonMetadata) PreviousFiles() iter.Seq[MetadataLogEntry] {
	return slices.Values(c.MetadataLog)
}

func (c *commonMetadata) Equals(other *commonMetadata) bool {
	if other == nil {
		return false
	}

	if c == other {
		return true
	}

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

	if c.SnapshotRefs == nil {
		c.SnapshotRefs = map[string]SnapshotRef{}
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

func (c *commonMetadata) constructRefs() {
	if c.CurrentSnapshotID != nil {
		_, ok := c.SnapshotRefs[MainBranch]
		if !ok {
			c.SnapshotRefs[MainBranch] = SnapshotRef{
				SnapshotID:      *c.CurrentSnapshotID,
				SnapshotRefType: BranchRef,
			}
		}
	}
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

	c.constructRefs()

	switch {
	case c.LastUpdatedMS == 0:
		// last-updated-ms is required
		return fmt.Errorf("%w: missing last-updated-ms", ErrInvalidMetadata)
	case c.LastColumnId < 0:
		// last-column-id is required
		return fmt.Errorf("%w: missing last-column-id", ErrInvalidMetadata)
	}

	return nil
}

func (c *commonMetadata) NameMapping() iceberg.NameMapping {
	if nameMappingJson, ok := c.Props[DefaultNameMappingKey]; ok {
		nm := iceberg.NameMapping{}
		if err := json.Unmarshal([]byte(nameMappingJson), &nm); err == nil {
			return nm
		}
	}

	return nil
}

func (c *commonMetadata) Version() int { return c.FormatVersion }

type metadataV1 struct {
	Schema    *iceberg.Schema          `json:"schema,omitempty"`
	Partition []iceberg.PartitionField `json:"partition-spec,omitempty"`

	commonMetadata
}

func (m *metadataV1) LastSequenceNumber() int64 { return 0 }

func (m *metadataV1) Equals(other Metadata) bool {
	rhs, ok := other.(*metadataV1)
	if !ok {
		return false
	}

	if m == rhs {
		return true
	}

	return m.Schema.Equals(rhs.Schema) && slices.Equal(m.Partition, rhs.Partition) &&
		m.commonMetadata.Equals(&rhs.commonMetadata)
}

func (m *metadataV1) preValidate() {
	if len(m.SchemaList) == 0 && m.Schema != nil {
		m.SchemaList = []*iceberg.Schema{m.Schema}
	}

	if len(m.Specs) == 0 {
		m.Specs = []iceberg.PartitionSpec{
			iceberg.NewPartitionSpec(m.Partition...),
		}
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

	// Set LastColumnId to -1 to indicate that it is not set as LastColumnId = 0 is a valid value for when no schema is present
	aux.LastColumnId = -1

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
	LastSeqNum int64 `json:"last-sequence-number"`

	commonMetadata
}

func (m *metadataV2) LastSequenceNumber() int64 { return m.LastSeqNum }

func (m *metadataV2) Equals(other Metadata) bool {
	rhs, ok := other.(*metadataV2)
	if !ok {
		return false
	}

	if m == rhs {
		return true
	}

	return m.LastSeqNum == rhs.LastSeqNum &&
		m.commonMetadata.Equals(&rhs.commonMetadata)
}

func (m *metadataV2) UnmarshalJSON(b []byte) error {
	type Alias metadataV2
	aux := (*Alias)(m)

	// Set LastColumnId to -1 to indicate that it is not set as LastColumnId = 0 is a valid value for when no schema is present
	aux.LastColumnId = -1

	if err := json.Unmarshal(b, aux); err != nil {
		return err
	}

	m.preValidate()

	return m.validate()
}

const DefaultFormatVersion = 2

// NewMetadata creates a new table metadata object using the provided schema, information, generating a fresh UUID for
// the new table metadata. By default, this will generate a V2 table metadata, but this can be modified
// by adding a "format-version" property to the props map. An error will be returned if the "format-version"
// property exists and is not a valid version number.
func NewMetadata(sc *iceberg.Schema, partitions *iceberg.PartitionSpec, sortOrder SortOrder, location string, props iceberg.Properties) (Metadata, error) {
	return NewMetadataWithUUID(sc, partitions, sortOrder, location, props, uuid.Nil)
}

// NewMetadataWithUUID is like NewMetadata, but allows the caller to specify the UUID of the table rather than creating a new one.
func NewMetadataWithUUID(sc *iceberg.Schema, partitions *iceberg.PartitionSpec, sortOrder SortOrder, location string, props iceberg.Properties, tableUuid uuid.UUID) (Metadata, error) {
	freshSchema, err := iceberg.AssignFreshSchemaIDs(sc, nil)
	if err != nil {
		return nil, err
	}

	freshPartitions, err := iceberg.AssignFreshPartitionSpecIDs(partitions, sc, freshSchema)
	if err != nil {
		return nil, err
	}

	freshSortOrder, err := AssignFreshSortOrderIDs(sortOrder, sc, freshSchema)
	if err != nil {
		return nil, err
	}

	if tableUuid == uuid.Nil {
		tableUuid = uuid.New()
	}

	formatVersion := DefaultFormatVersion
	if props != nil {
		verStr, ok := props["format-version"]
		if ok {
			if formatVersion, err = strconv.Atoi(verStr); err != nil {
				formatVersion = DefaultFormatVersion
			}
			delete(props, "format-version")
		}
	}

	lastPartitionID := freshPartitions.LastAssignedFieldID()
	common := commonMetadata{
		LastUpdatedMS:      time.Now().UnixMilli(),
		LastColumnId:       freshSchema.HighestFieldID(),
		FormatVersion:      formatVersion,
		UUID:               tableUuid,
		Loc:                location,
		SchemaList:         []*iceberg.Schema{freshSchema},
		CurrentSchemaID:    freshSchema.ID,
		Specs:              []iceberg.PartitionSpec{freshPartitions},
		DefaultSpecID:      freshPartitions.ID(),
		LastPartitionID:    &lastPartitionID,
		Props:              props,
		SortOrderList:      []SortOrder{freshSortOrder},
		DefaultSortOrderID: freshSortOrder.OrderID,
	}

	switch formatVersion {
	case 1:
		return &metadataV1{
			commonMetadata: common,
			Schema:         freshSchema,
			Partition:      slices.Collect(freshPartitions.Fields()),
		}, nil
	case 2:
		return &metadataV2{commonMetadata: common}, nil
	default:
		return nil, fmt.Errorf("invalid format version: %d", formatVersion)
	}
}
