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

	"github.com/apache/iceberg-go"

	"github.com/google/uuid"
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
	// SortOrder returns the table's current sort order, ie: the one with the
	// ID that matches the default-sort-order-id.
	SortOrder() SortOrder
	// SortOrders returns the list of sort orders in the table.
	SortOrders() []SortOrder
	// Properties is a string to string map of table properties. This is used
	// to control settings that affect reading and writing and is not intended
	// to be used for arbitrary metadata. For example, commit.retry.num-retries
	// is used to control the number of commit retries.
	Properties() iceberg.Properties
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
		ret = &MetadataV1{}
	case 2:
		ret = &MetadataV2{}
	default:
		return nil, ErrInvalidMetadataFormatVersion
	}

	return ret, json.Unmarshal(b, ret)
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
	Refs               map[string]SnapshotRef  `json:"refs"`
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
	if ref, ok := c.Refs[name]; ok {
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
		if _, ok := c.Refs[MainBranch]; !ok {
			c.Refs[MainBranch] = SnapshotRef{
				SnapshotID:      *c.CurrentSnapshotID,
				SnapshotRefType: BranchRef,
			}
		}
	}

	if c.MetadataLog == nil {
		c.MetadataLog = []MetadataLogEntry{}
	}

	if c.Refs == nil {
		c.Refs = make(map[string]SnapshotRef)
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

type MetadataV1 struct {
	Schema    iceberg.Schema           `json:"schema"`
	Partition []iceberg.PartitionField `json:"partition-spec"`

	commonMetadata
}

func (m *MetadataV1) preValidate() {
	if len(m.SchemaList) == 0 {
		m.SchemaList = []*iceberg.Schema{&m.Schema}
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

func (m *MetadataV1) UnmarshalJSON(b []byte) error {
	type Alias MetadataV1
	aux := (*Alias)(m)

	if err := json.Unmarshal(b, aux); err != nil {
		return err
	}

	m.preValidate()
	return m.validate()
}

func (m *MetadataV1) ToV2() MetadataV2 {
	commonOut := m.commonMetadata
	commonOut.FormatVersion = 2
	if commonOut.UUID.String() == "" {
		commonOut.UUID = uuid.New()
	}

	return MetadataV2{commonMetadata: commonOut}
}

type MetadataV2 struct {
	LastSequenceNumber int `json:"last-sequence-number"`

	commonMetadata
}

func (m *MetadataV2) UnmarshalJSON(b []byte) error {
	type Alias MetadataV2
	aux := (*Alias)(m)

	if err := json.Unmarshal(b, aux); err != nil {
		return err
	}

	m.preValidate()
	return m.validate()
}
