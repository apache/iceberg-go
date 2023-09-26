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

type Metadata interface {
	Version() int
	TableUUID() uuid.UUID
	Loc() string
	LastUpdated() int
	LastColumn() int
	Schemas() []*iceberg.Schema
	CurrentSchema() *iceberg.Schema
	PartitionSpecs() []iceberg.PartitionSpec
	PartitionSpec() iceberg.PartitionSpec
	DefaultPartitionSpec() int
	LastPartitionSpecID() *int
	Snapshots() []Snapshot
	SnapshotByID(int64) *Snapshot
	SnapshotByName(name string) *Snapshot
	CurrentSnapshot() *Snapshot
	SortOrder() SortOrder
	SortOrders() []SortOrder
	Properties() iceberg.Properties
}

var (
	ErrInvalidMetadataFormatVersion = errors.New("invalid or missing format-version in table metadata")
	ErrInvalidMetadata              = errors.New("invalid metadata")
)

func ParseMetadata(r io.Reader) (Metadata, error) {
	data, err := io.ReadAll(r)
	if err != nil {
		return nil, err
	}

	return ParseMetadataBytes(data)
}

func ParseMetadataString(s string) (Metadata, error) {
	return ParseMetadataBytes([]byte(s))
}

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
	Location           string                  `json:"location"`
	LastUpdatedMS      int                     `json:"last-updated-ms"`
	LastColumnID       int                     `json:"last-column-id"`
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
func (c *commonMetadata) Loc() string                { return c.Location }
func (c *commonMetadata) LastUpdated() int           { return c.LastUpdatedMS }
func (c *commonMetadata) LastColumn() int            { return c.LastColumnID }
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
	case c.LastColumnID == 0:
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
