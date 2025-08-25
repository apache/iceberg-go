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

// Package catalog provides an interface for Catalog implementations along with
// a registry for registering catalog implementations.
//
// Subpackages of this package provide some default implementations for select
// catalog types which will register themselves if imported. For instance,
// adding the following import:
//
//	import _ "github.com/apache/iceberg-go/catalog/rest"
//
// Will register the REST catalog implementation.
package catalog

import (
	"context"
	"errors"
	"fmt"
	"iter"
	"maps"
	"strings"

	"github.com/apache/iceberg-go"
	iceinternal "github.com/apache/iceberg-go/internal"
	"github.com/apache/iceberg-go/table"
)

type Type string

const (
	REST     Type = "rest"
	Hive     Type = "hive"
	Glue     Type = "glue"
	DynamoDB Type = "dynamodb"
	SQL      Type = "sql"
)

var (
	// ErrNoSuchTable is returned when a table does not exist in the catalog.
	ErrNoSuchTable            = errors.New("table does not exist")
	ErrNoSuchNamespace        = errors.New("namespace does not exist")
	ErrNamespaceAlreadyExists = errors.New("namespace already exists")
	ErrTableAlreadyExists     = errors.New("table already exists")
	ErrCatalogNotFound        = errors.New("catalog type not registered")
	ErrNamespaceNotEmpty      = errors.New("namespace is not empty")
	ErrNoSuchView             = errors.New("view does not exist")
	ErrViewAlreadyExists      = errors.New("view already exists")
)

type PropertiesUpdateSummary struct {
	Removed []string `json:"removed"`
	Updated []string `json:"updated"`
	Missing []string `json:"missing"`
}

// CreateTableCfg represents the configuration used for CreateTable operations
type CreateTableCfg struct {
	Location      string
	PartitionSpec *iceberg.PartitionSpec
	SortOrder     table.SortOrder
	Properties    iceberg.Properties
}

// Catalog for iceberg table operations like create, drop, load, list and others.
type Catalog interface {
	// CatalogType returns the type of the catalog.
	CatalogType() Type

	// CreateTable creates a new iceberg table in the catalog using the provided identifier
	// and schema. Options can be used to optionally provide location, partition spec, sort order,
	// and custom properties.
	CreateTable(ctx context.Context, identifier table.Identifier, schema *iceberg.Schema, opts ...CreateTableOpt) (*table.Table, error)
	// CommitTable commits the table metadata and updates to the catalog, returning the new metadata
	CommitTable(ctx context.Context, identifier table.Identifier, requirements []table.Requirement, updates []table.Update) (table.Metadata, string, error)
	// ListTables returns a list of table identifiers in the catalog, with the returned
	// identifiers containing the information required to load the table via that catalog.
	ListTables(ctx context.Context, namespace table.Identifier) iter.Seq2[table.Identifier, error]
	// LoadTable loads a table from the catalog and returns a Table with the metadata.
	LoadTable(ctx context.Context, identifier table.Identifier) (*table.Table, error)
	// DropTable tells the catalog to drop the table entirely.
	DropTable(ctx context.Context, identifier table.Identifier) error
	// RenameTable tells the catalog to rename a given table by the identifiers
	// provided, and then loads and returns the destination table
	RenameTable(ctx context.Context, from, to table.Identifier) (*table.Table, error)
	// CheckTableExists returns if the table exists
	CheckTableExists(ctx context.Context, identifier table.Identifier) (bool, error)
	// ListNamespaces returns the list of available namespaces, optionally filtering by a
	// parent namespace
	ListNamespaces(ctx context.Context, parent table.Identifier) ([]table.Identifier, error)
	// CreateNamespace tells the catalog to create a new namespace with the given properties
	CreateNamespace(ctx context.Context, namespace table.Identifier, props iceberg.Properties) error
	// DropNamespace tells the catalog to drop the namespace and all tables in that namespace
	DropNamespace(ctx context.Context, namespace table.Identifier) error
	// CheckNamespaceExists returns if the namespace exists
	CheckNamespaceExists(ctx context.Context, namespace table.Identifier) (bool, error)
	// LoadNamespaceProperties returns the current properties in the catalog for
	// a given namespace
	LoadNamespaceProperties(ctx context.Context, namespace table.Identifier) (iceberg.Properties, error)
	// UpdateNamespaceProperties allows removing, adding, and/or updating properties of a namespace
	UpdateNamespaceProperties(ctx context.Context, namespace table.Identifier,
		removals []string, updates iceberg.Properties) (PropertiesUpdateSummary, error)
}

func ToIdentifier(ident ...string) table.Identifier {
	if len(ident) == 1 {
		if ident[0] == "" {
			return nil
		}

		return table.Identifier(strings.Split(ident[0], "."))
	}

	return table.Identifier(ident)
}

func TableNameFromIdent(ident table.Identifier) string {
	if len(ident) == 0 {
		return ""
	}

	return ident[len(ident)-1]
}

func NamespaceFromIdent(ident table.Identifier) table.Identifier {
	return ident[:len(ident)-1]
}

type CreateTableOpt func(*CreateTableCfg)

func WithLocation(location string) CreateTableOpt {
	return func(cfg *CreateTableCfg) {
		cfg.Location = strings.TrimRight(location, "/")
	}
}

func WithPartitionSpec(spec *iceberg.PartitionSpec) CreateTableOpt {
	return func(cfg *CreateTableCfg) {
		cfg.PartitionSpec = spec
	}
}

func WithSortOrder(order table.SortOrder) CreateTableOpt {
	return func(cfg *CreateTableCfg) {
		cfg.SortOrder = order
	}
}

func WithProperties(props iceberg.Properties) CreateTableOpt {
	return func(cfg *CreateTableCfg) {
		cfg.Properties = props
	}
}

//lint:ignore U1000 this is linked to by catalogs via go:linkname but we don't want to export it
func checkForOverlap(removals []string, updates iceberg.Properties) error {
	overlap := []string{}
	for _, key := range removals {
		if _, ok := updates[key]; ok {
			overlap = append(overlap, key)
		}
	}
	if len(overlap) > 0 {
		return fmt.Errorf("conflict between removals and updates for keys: %v", overlap)
	}

	return nil
}

//lint:ignore U1000 this is linked to by catalogs via go:linkname but we don't want to export it
func getUpdatedPropsAndUpdateSummary(currentProps iceberg.Properties, removals []string, updates iceberg.Properties) (iceberg.Properties, PropertiesUpdateSummary, error) {
	if err := checkForOverlap(removals, updates); err != nil {
		return nil, PropertiesUpdateSummary{}, err
	}
	var (
		updatedProps = maps.Clone(currentProps)
		removed      = make([]string, 0, len(removals))
		updated      = make([]string, 0, len(updates))
	)

	for _, key := range removals {
		if _, exists := updatedProps[key]; exists {
			delete(updatedProps, key)
			removed = append(removed, key)
		}
	}

	for key, value := range updates {
		if updatedProps[key] != value {
			updated = append(updated, key)
			updatedProps[key] = value
		}
	}

	summary := PropertiesUpdateSummary{
		Removed: removed,
		Updated: updated,
		Missing: iceinternal.Difference(removals, removed),
	}

	return updatedProps, summary, nil
}
