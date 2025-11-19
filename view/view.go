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
	"errors"
	"fmt"
	"iter"
	"slices"
	"strings"

	"github.com/apache/iceberg-go"
)

var (
	ErrInvalidViewMetadata              = errors.New("invalid view metadata")
	ErrInvalidViewMetadataFormatVersion = errors.New("invalid or missing format-version in view metadata")
)

// Metadata defines the format for view metadata,
// similar to how Iceberg supports a common table format for tables
type Metadata interface {
	// ViewUUID identifies the view, generated when the view is created
	ViewUUID() string
	// FormatVersion is the version number for the view format; must be 1
	FormatVersion() int
	// Location is the view's base location; used to create metadata file locations
	Location() string
	// Schemas is a list of known schemas
	Schemas() iter.Seq[*iceberg.Schema]
	// CurrentVersion is the current version of the view
	CurrentVersion() *Version
	// Versions is a list of known versions of the view
	Versions() iter.Seq[Version]
	// VersionLog is a list of version log entries with the timestamp and version-id for every change to current-version-id
	VersionLog() iter.Seq[VersionLogEntry]
	// Properties is a string to string map of view properties
	Properties() iceberg.Properties
}

type metadata struct {
	UUID             string             `json:"view-uuid"`
	FmtVersion       int                `json:"format-version"`
	Loc              string             `json:"location"`
	SchemaList       []*iceberg.Schema  `json:"schemas"`
	CurrentVersionId int64              `json:"current-version-id"`
	VersionList      []Version          `json:"versions"`
	VersionLogList   []VersionLogEntry  `json:"version-log"`
	Props            iceberg.Properties `json:"properties"`
}

func (m *metadata) ViewUUID() string {
	return m.UUID
}

func (m *metadata) FormatVersion() int {
	return m.FmtVersion
}

func (m *metadata) Location() string {
	return m.Loc
}

func (m *metadata) Schemas() iter.Seq[*iceberg.Schema] {
	return slices.Values(m.SchemaList)
}

func (m *metadata) CurrentVersion() *Version {
	for i := range m.VersionList {
		if m.VersionList[i].VersionID == m.CurrentVersionId {
			return &m.VersionList[i]
		}
	}

	return nil
}

func (m *metadata) Versions() iter.Seq[Version] {
	return slices.Values(m.VersionList)
}

func (m *metadata) VersionLog() iter.Seq[VersionLogEntry] {
	return slices.Values(m.VersionLogList)
}

func (m *metadata) Properties() iceberg.Properties {
	return m.Props
}

// Version represents the view definition at a point in time
type Version struct {
	VersionID        int64               `json:"version-id"`
	SchemaID         int                 `json:"schema-id"`
	TimestampMs      int64               `json:"timestamp-ms"`
	Summary          map[string]string   `json:"summary"`
	Representations  []SQLRepresentation `json:"representations"`
	DefaultCatalog   *string             `json:"default-catalog"`
	DefaultNamespace []string            `json:"default-namespace"`
}

// Copy creates a deep copy of the Version.
func (v *Version) Copy() *Version {
	if v == nil {
		return nil
	}

	copied := &Version{
		VersionID:   v.VersionID,
		SchemaID:    v.SchemaID,
		TimestampMs: v.TimestampMs,
	}

	if v.Summary != nil {
		copied.Summary = make(map[string]string, len(v.Summary))
		for k, val := range v.Summary {
			copied.Summary[k] = val
		}
	}

	if v.Representations != nil {
		copied.Representations = make([]SQLRepresentation, len(v.Representations))
		copy(copied.Representations, v.Representations)
	}

	if v.DefaultCatalog != nil {
		catalog := *v.DefaultCatalog
		copied.DefaultCatalog = &catalog
	}

	if v.DefaultNamespace != nil {
		copied.DefaultNamespace = make([]string, len(v.DefaultNamespace))
		copy(copied.DefaultNamespace, v.DefaultNamespace)
	}

	return copied
}

// Equals compares two versions for semantic equivalence.
// It compares all fields except VersionID and TimestampMs.
// Returns true if the versions are functionally identical.
func (v *Version) Equals(other *Version) bool {
	if v == nil && other == nil {
		return true
	}
	if v == nil || other == nil {
		return false
	}

	if v.SchemaID != other.SchemaID {
		return false
	}

	if len(v.Summary) != len(other.Summary) {
		return false
	}
	for k, vVal := range v.Summary {
		if otherVal, ok := other.Summary[k]; !ok || vVal != otherVal {
			return false
		}
	}

	if len(v.Representations) != len(other.Representations) {
		return false
	}
	for i, r := range v.Representations {
		otherR := other.Representations[i]
		if r.Type != otherR.Type || r.SQL != otherR.SQL || r.Dialect != otherR.Dialect {
			return false
		}
	}

	if (v.DefaultCatalog == nil) != (other.DefaultCatalog == nil) {
		return false
	}
	if v.DefaultCatalog != nil && *v.DefaultCatalog != *other.DefaultCatalog {
		return false
	}

	if len(v.DefaultNamespace) != len(other.DefaultNamespace) {
		return false
	}
	for i, ns := range v.DefaultNamespace {
		if other.DefaultNamespace[i] != ns {
			return false
		}
	}

	return true
}

// SQLRepresentation is a view in SQL with a given dialect
type SQLRepresentation struct {
	Type    string `json:"type"`
	SQL     string `json:"sql"`
	Dialect string `json:"dialect"`
}

// VersionLogEntry contains a change to the view state.
// At the given timestamp, the current version was set to the given version ID.
type VersionLogEntry struct {
	TimestampMs int64 `json:"timestamp-ms"`
	VersionID   int64 `json:"version-id"`
}

func (m *metadata) preValidate() {
	if m.SchemaList == nil {
		m.SchemaList = []*iceberg.Schema{}
	}

	if m.VersionList == nil {
		m.VersionList = []Version{}
	}

	if m.VersionLogList == nil {
		m.VersionLogList = []VersionLogEntry{}
	}

	if m.Props == nil {
		m.Props = iceberg.Properties{}
	}
}

func (m *metadata) validate() error {
	if m.UUID == "" {
		return fmt.Errorf("%w: view-uuid is required", ErrInvalidViewMetadata)
	}

	if m.FmtVersion == -1 {
		return fmt.Errorf("%w: format-version is required", ErrInvalidViewMetadataFormatVersion)
	}

	if m.FmtVersion < 1 || m.FmtVersion > 1 {
		return fmt.Errorf("%w: format-version %d (only version 1 is supported)",
			ErrInvalidViewMetadataFormatVersion, m.FmtVersion)
	}

	if m.Loc == "" {
		return fmt.Errorf("%w: location is required", ErrInvalidViewMetadata)
	}

	if len(m.VersionList) == 0 {
		return fmt.Errorf("%w: at least one version is required", ErrInvalidViewMetadata)
	}

	if m.CurrentVersionId == -1 {
		return fmt.Errorf("%w: current-version-id is required", ErrInvalidViewMetadata)
	}

	if len(m.SchemaList) == 0 {
		return fmt.Errorf("%w: at least one schema is required", ErrInvalidViewMetadata)
	}

	if err := m.checkCurrentVersionExists(); err != nil {
		return err
	}

	if err := m.checkVersionSchemasExist(); err != nil {
		return err
	}

	if err := m.checkDialectsUnique(); err != nil {
		return err
	}

	return nil
}

func (m *metadata) checkCurrentVersionExists() error {
	for _, v := range m.VersionList {
		if v.VersionID == m.CurrentVersionId {
			return nil
		}
	}

	return fmt.Errorf("%w: current-version-id %d not found in versions",
		ErrInvalidViewMetadata, m.CurrentVersionId)
}

func (m *metadata) checkVersionSchemasExist() error {
	schemaIDs := make(map[int]bool)
	for _, schema := range m.SchemaList {
		schemaIDs[schema.ID] = true
	}

	for _, version := range m.VersionList {
		if !schemaIDs[version.SchemaID] {
			return fmt.Errorf("%w: version %d references unknown schema-id %d",
				ErrInvalidViewMetadata, version.VersionID, version.SchemaID)
		}
	}

	return nil
}

func (m *metadata) checkDialectsUnique() error {
	for _, version := range m.VersionList {
		seenDialects := make(map[string]bool)
		for _, repr := range version.Representations {
			dialect := strings.ToLower(repr.Dialect)
			if seenDialects[dialect] {
				return fmt.Errorf("%w: version %d has duplicate dialect %s",
					ErrInvalidViewMetadata, version.VersionID, repr.Dialect)
			}
			seenDialects[dialect] = true
		}
	}

	return nil
}

func (m *metadata) UnmarshalJSON(b []byte) error {
	type Alias metadata
	aux := (*Alias)(m)

	aux.FmtVersion = -1
	aux.CurrentVersionId = -1

	if err := json.Unmarshal(b, aux); err != nil {
		return err
	}

	m.preValidate()

	return m.validate()
}
