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

package hive

import (
	"context"
	"errors"
	"fmt"
	"iter"
	"maps"
	"strings"
	_ "unsafe"

	"github.com/apache/iceberg-go"
	"github.com/apache/iceberg-go/catalog"
	"github.com/apache/iceberg-go/catalog/internal"
	"github.com/apache/iceberg-go/io"
	"github.com/apache/iceberg-go/table"
	"github.com/beltran/gohive/hive_metastore"
)

const (
	// Property keys for namespace properties
	locationKey    = "location"
	commentKey     = "comment"
	descriptionKey = "description"
)

var _ catalog.Catalog = (*Catalog)(nil)

func init() {
	catalog.Register("hive", catalog.RegistrarFunc(func(ctx context.Context, _ string, props iceberg.Properties) (catalog.Catalog, error) {
		return NewCatalog(props)
	}))
}

// Catalog implements the catalog.Catalog interface for Hive Metastore.
type Catalog struct {
	client HiveClient
	opts   *HiveOptions
}

// NewCatalog creates a new Hive Metastore catalog.
func NewCatalog(props iceberg.Properties, opts ...Option) (*Catalog, error) {
	o := NewHiveOptions()
	o.ApplyProperties(props)

	for _, opt := range opts {
		opt(o)
	}

	if o.URI == "" {
		return nil, errors.New("hive.uri is required")
	}

	client, err := NewHiveClient(o.URI, o)
	if err != nil {
		return nil, fmt.Errorf("failed to create Hive client: %w", err)
	}

	return &Catalog{
		client: client,
		opts:   o,
	}, nil
}

// NewCatalogWithClient creates a new Hive Metastore catalog with a custom client.
// This is useful for testing with mock clients.
func NewCatalogWithClient(client HiveClient, props iceberg.Properties) *Catalog {
	o := NewHiveOptions()
	o.ApplyProperties(props)

	return &Catalog{
		client: client,
		opts:   o,
	}
}

// CatalogType returns the type of the catalog.
func (c *Catalog) CatalogType() catalog.Type {
	return catalog.Hive
}

// Close closes the connection to the Hive Metastore.
func (c *Catalog) Close() error {
	c.client.Close()

	return nil
}

// ListTables returns a list of table identifiers in the given namespace.
func (c *Catalog) ListTables(ctx context.Context, namespace table.Identifier) iter.Seq2[table.Identifier, error] {
	return func(yield func(table.Identifier, error) bool) {
		database, err := identifierToDatabase(namespace)
		if err != nil {
			yield(nil, err)

			return
		}

		// Get all table names in the database
		tableNames, err := c.client.GetTables(ctx, database, "*")
		if err != nil {
			yield(nil, fmt.Errorf("failed to list tables in %s: %w", database, err))

			return
		}

		if len(tableNames) == 0 {
			return
		}

		// Check each table to see if it's an Iceberg table
		for _, tableName := range tableNames {
			tbl, err := c.client.GetTable(ctx, database, tableName)
			if err != nil {
				// Skip tables we can't read
				continue
			}
			if isIcebergTable(tbl) {
				if !yield(TableIdentifier(database, tableName), nil) {
					return
				}
			}
		}
	}
}

// LoadTable loads a table from the catalog.
func (c *Catalog) LoadTable(ctx context.Context, identifier table.Identifier) (*table.Table, error) {
	database, tableName, err := identifierToTableName(identifier)
	if err != nil {
		return nil, err
	}

	hiveTbl, err := c.getIcebergTable(ctx, database, tableName)
	if err != nil {
		return nil, err
	}

	metadataLocation, err := getMetadataLocation(hiveTbl)
	if err != nil {
		return nil, fmt.Errorf("failed to get metadata location: %w", err)
	}

	return table.NewFromLocation(
		ctx,
		identifier,
		metadataLocation,
		io.LoadFSFunc(c.opts.props, metadataLocation),
		c,
	)
}

// CreateTable creates a new table in the catalog.
func (c *Catalog) CreateTable(ctx context.Context, identifier table.Identifier, schema *iceberg.Schema, opts ...catalog.CreateTableOpt) (*table.Table, error) {
	staged, err := internal.CreateStagedTable(ctx, c.opts.props, c.LoadNamespaceProperties, identifier, schema, opts...)
	if err != nil {
		return nil, err
	}

	database, tableName, err := identifierToTableName(identifier)
	if err != nil {
		return nil, err
	}

	// Get the filesystem for writing metadata
	afs, err := staged.FS(ctx)
	if err != nil {
		return nil, err
	}
	wfs, ok := afs.(io.WriteFileIO)
	if !ok {
		return nil, errors.New("loaded filesystem IO does not support writing")
	}

	// Write the metadata file
	compression := staged.Table.Properties().Get(table.MetadataCompressionKey, table.MetadataCompressionDefault)
	if err := internal.WriteTableMetadata(staged.Metadata(), wfs, staged.MetadataLocation(), compression); err != nil {
		return nil, err
	}

	// Create the Hive table
	hiveTbl := constructHiveTable(database, tableName, staged.Table.Location(), staged.MetadataLocation(), schema, staged.Table.Properties())

	if err := c.client.CreateTable(ctx, hiveTbl); err != nil {
		if isAlreadyExistsError(err) {
			return nil, fmt.Errorf("%w: %s.%s", catalog.ErrTableAlreadyExists, database, tableName)
		}

		return nil, fmt.Errorf("failed to create table %s.%s: %w", database, tableName, err)
	}

	return c.LoadTable(ctx, identifier)
}

// DropTable drops a table from the catalog.
func (c *Catalog) DropTable(ctx context.Context, identifier table.Identifier) error {
	database, tableName, err := identifierToTableName(identifier)
	if err != nil {
		return err
	}

	// Verify it's an Iceberg table
	if _, err := c.getIcebergTable(ctx, database, tableName); err != nil {
		return err
	}

	// Drop the table (deleteData=false for external tables)
	if err := c.client.DropTable(ctx, database, tableName, false); err != nil {
		return fmt.Errorf("failed to drop table %s.%s: %w", database, tableName, err)
	}

	return nil
}

// RenameTable renames a table in the catalog.
func (c *Catalog) RenameTable(ctx context.Context, from, to table.Identifier) (*table.Table, error) {
	fromDB, fromTable, err := identifierToTableName(from)
	if err != nil {
		return nil, err
	}

	toDB, toTable, err := identifierToTableName(to)
	if err != nil {
		return nil, err
	}

	// Check that target namespace exists
	exists, err := c.CheckNamespaceExists(ctx, DatabaseIdentifier(toDB))
	if err != nil {
		return nil, err
	}
	if !exists {
		return nil, fmt.Errorf("%w: %s", catalog.ErrNoSuchNamespace, toDB)
	}

	// Get the existing table
	hiveTbl, err := c.getIcebergTable(ctx, fromDB, fromTable)
	if err != nil {
		return nil, err
	}

	// Update table name and database
	hiveTbl.TableName = toTable
	hiveTbl.DbName = toDB

	// Alter the table to rename it
	if err := c.client.AlterTable(ctx, fromDB, fromTable, hiveTbl); err != nil {
		return nil, fmt.Errorf("failed to rename table %s.%s to %s.%s: %w", fromDB, fromTable, toDB, toTable, err)
	}

	return c.LoadTable(ctx, to)
}

// CommitTable commits updates to a table.
func (c *Catalog) CommitTable(ctx context.Context, identifier table.Identifier, requirements []table.Requirement, updates []table.Update) (table.Metadata, string, error) {
	database, tableName, err := identifierToTableName(identifier)
	if err != nil {
		return nil, "", err
	}

	// Load current table state
	currentHiveTbl, err := c.client.GetTable(ctx, database, tableName)
	if err != nil && !isNoSuchObjectError(err) {
		return nil, "", err
	}

	var current *table.Table
	if currentHiveTbl != nil && isIcebergTable(currentHiveTbl) {
		metadataLoc, err := getMetadataLocation(currentHiveTbl)
		if err != nil {
			return nil, "", err
		}
		current, err = table.NewFromLocation(ctx, identifier, metadataLoc, io.LoadFSFunc(c.opts.props, metadataLoc), c)
		if err != nil {
			return nil, "", err
		}
	}

	// Create staged table with updates
	staged, err := internal.UpdateAndStageTable(ctx, current, identifier, requirements, updates, c)
	if err != nil {
		return nil, "", err
	}

	// Check if there are actual changes
	if current != nil && staged.Metadata().Equals(current.Metadata()) {
		return current.Metadata(), current.MetadataLocation(), nil
	}

	// Write new metadata
	if err := internal.WriteMetadata(ctx, staged.Metadata(), staged.MetadataLocation(), staged.Properties()); err != nil {
		return nil, "", err
	}

	// Update Hive table
	if current != nil {
		updatedHiveTbl := updateHiveTableForCommit(currentHiveTbl, staged.MetadataLocation())

		if err := c.client.AlterTable(ctx, database, tableName, updatedHiveTbl); err != nil {
			return nil, "", fmt.Errorf("failed to commit table %s.%s: %w", database, tableName, err)
		}
	} else {
		// Create new table
		hiveTbl := constructHiveTable(database, tableName, staged.Table.Location(), staged.MetadataLocation(), staged.Metadata().CurrentSchema(), staged.Properties())
		if err := c.client.CreateTable(ctx, hiveTbl); err != nil {
			return nil, "", fmt.Errorf("failed to create table %s.%s: %w", database, tableName, err)
		}
	}

	return staged.Metadata(), staged.MetadataLocation(), nil
}

// CheckTableExists checks if a table exists in the catalog.
func (c *Catalog) CheckTableExists(ctx context.Context, identifier table.Identifier) (bool, error) {
	database, tableName, err := identifierToTableName(identifier)
	if err != nil {
		return false, err
	}

	hiveTbl, err := c.client.GetTable(ctx, database, tableName)
	if err != nil {
		if isNoSuchObjectError(err) {
			return false, nil
		}

		return false, err
	}

	return isIcebergTable(hiveTbl), nil
}

// Namespace operations

// ListNamespaces returns a list of namespaces in the catalog.
func (c *Catalog) ListNamespaces(ctx context.Context, parent table.Identifier) ([]table.Identifier, error) {
	// Hive doesn't support hierarchical namespaces
	if len(parent) > 0 {
		return nil, errors.New("hierarchical namespace is not supported")
	}

	databases, err := c.client.GetAllDatabases(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to list namespaces: %w", err)
	}

	namespaces := make([]table.Identifier, len(databases))
	for i, db := range databases {
		namespaces[i] = DatabaseIdentifier(db)
	}

	return namespaces, nil
}

// CreateNamespace creates a new namespace in the catalog.
func (c *Catalog) CreateNamespace(ctx context.Context, namespace table.Identifier, props iceberg.Properties) error {
	database, err := identifierToDatabase(namespace)
	if err != nil {
		return err
	}

	db := &hive_metastore.Database{
		Name:       database,
		Parameters: make(map[string]string),
	}

	for k, v := range props {
		switch k {
		case locationKey, "Location":
			db.LocationUri = v
		case commentKey, descriptionKey, "Description":
			db.Description = v
		default:
			db.Parameters[k] = v
		}
	}

	if err := c.client.CreateDatabase(ctx, db); err != nil {
		if isAlreadyExistsError(err) {
			return fmt.Errorf("%w: %s", catalog.ErrNamespaceAlreadyExists, database)
		}

		return fmt.Errorf("failed to create namespace %s: %w", database, err)
	}

	return nil
}

// DropNamespace drops a namespace from the catalog.
func (c *Catalog) DropNamespace(ctx context.Context, namespace table.Identifier) error {
	database, err := identifierToDatabase(namespace)
	if err != nil {
		return err
	}

	// Check if namespace exists
	_, err = c.client.GetDatabase(ctx, database)
	if err != nil {
		if isNoSuchObjectError(err) {
			return fmt.Errorf("%w: %s", catalog.ErrNoSuchNamespace, database)
		}

		return err
	}

	// Drop database (cascade=false to fail if not empty)
	if err := c.client.DropDatabase(ctx, database, false, false); err != nil {
		if isInvalidOperationError(err) {
			return fmt.Errorf("%w: %s", catalog.ErrNamespaceNotEmpty, database)
		}

		return fmt.Errorf("failed to drop namespace %s: %w", database, err)
	}

	return nil
}

// CheckNamespaceExists checks if a namespace exists in the catalog.
func (c *Catalog) CheckNamespaceExists(ctx context.Context, namespace table.Identifier) (bool, error) {
	database, err := identifierToDatabase(namespace)
	if err != nil {
		return false, err
	}

	_, err = c.client.GetDatabase(ctx, database)
	if err != nil {
		if isNoSuchObjectError(err) {
			return false, nil
		}

		return false, err
	}

	return true, nil
}

// LoadNamespaceProperties loads the properties for a namespace.
func (c *Catalog) LoadNamespaceProperties(ctx context.Context, namespace table.Identifier) (iceberg.Properties, error) {
	database, err := identifierToDatabase(namespace)
	if err != nil {
		return nil, err
	}

	db, err := c.client.GetDatabase(ctx, database)
	if err != nil {
		if isNoSuchObjectError(err) {
			return nil, fmt.Errorf("%w: %s", catalog.ErrNoSuchNamespace, database)
		}

		return nil, fmt.Errorf("failed to get namespace %s: %w", database, err)
	}

	props := make(iceberg.Properties)
	if db.Parameters != nil {
		maps.Copy(props, db.Parameters)
	}
	if db.LocationUri != "" {
		props[locationKey] = db.LocationUri
	}
	if db.Description != "" {
		props[commentKey] = db.Description
	}

	return props, nil
}

// avoid circular dependency
//
//go:linkname getUpdatedPropsAndUpdateSummary github.com/apache/iceberg-go/catalog.getUpdatedPropsAndUpdateSummary
func getUpdatedPropsAndUpdateSummary(currentProps iceberg.Properties, removals []string, updates iceberg.Properties) (iceberg.Properties, catalog.PropertiesUpdateSummary, error)

// UpdateNamespaceProperties updates the properties for a namespace.
func (c *Catalog) UpdateNamespaceProperties(ctx context.Context, namespace table.Identifier,
	removals []string, updates iceberg.Properties,
) (catalog.PropertiesUpdateSummary, error) {
	currentProps, err := c.LoadNamespaceProperties(ctx, namespace)
	if err != nil {
		return catalog.PropertiesUpdateSummary{}, err
	}

	updatedProperties, propertiesUpdateSummary, err := getUpdatedPropsAndUpdateSummary(currentProps, removals, updates)
	if err != nil {
		return catalog.PropertiesUpdateSummary{}, err
	}

	database, _ := identifierToDatabase(namespace)

	db := &hive_metastore.Database{
		Name:       database,
		Parameters: make(map[string]string),
	}

	for k, v := range updatedProperties {
		switch k {
		case locationKey, "Location":
			db.LocationUri = v
		case commentKey, descriptionKey, "Description":
			db.Description = v
		default:
			db.Parameters[k] = v
		}
	}

	if err := c.client.AlterDatabase(ctx, database, db); err != nil {
		return catalog.PropertiesUpdateSummary{}, fmt.Errorf("failed to update namespace properties %s: %w", database, err)
	}

	return propertiesUpdateSummary, nil
}

// getIcebergTable retrieves a table and validates it's an Iceberg table.
func (c *Catalog) getIcebergTable(ctx context.Context, database, tableName string) (*hive_metastore.Table, error) {
	hiveTbl, err := c.client.GetTable(ctx, database, tableName)
	if err != nil {
		if isNoSuchObjectError(err) {
			return nil, fmt.Errorf("%w: %s.%s", catalog.ErrNoSuchTable, database, tableName)
		}

		return nil, fmt.Errorf("failed to get table %s.%s: %w", database, tableName, err)
	}

	if !isIcebergTable(hiveTbl) {
		return nil, fmt.Errorf("table %s.%s is not an Iceberg table", database, tableName)
	}

	return hiveTbl, nil
}

func identifierToTableName(identifier table.Identifier) (string, string, error) {
	if len(identifier) != 2 {
		return "", "", fmt.Errorf("invalid identifier, expected [database, table]: %v", identifier)
	}

	return identifier[0], identifier[1], nil
}

func identifierToDatabase(identifier table.Identifier) (string, error) {
	if len(identifier) != 1 {
		return "", fmt.Errorf("invalid identifier, expected [database]: %v", identifier)
	}

	return identifier[0], nil
}

// TableIdentifier returns a table identifier for a Hive table.
func TableIdentifier(database, tableName string) table.Identifier {
	return []string{database, tableName}
}

// DatabaseIdentifier returns a database identifier for a Hive database.
func DatabaseIdentifier(database string) table.Identifier {
	return []string{database}
}

// Error checking helpers for Hive Metastore exceptions

func isNoSuchObjectError(err error) bool {
	if err == nil {
		return false
	}

	errStr := err.Error()

	return strings.Contains(errStr, "NoSuchObjectException") ||
		strings.Contains(errStr, "not found") ||
		strings.Contains(errStr, "does not exist")
}

func isAlreadyExistsError(err error) bool {
	if err == nil {
		return false
	}

	errStr := err.Error()

	return strings.Contains(errStr, "AlreadyExistsException") ||
		strings.Contains(errStr, "already exists")
}

func isInvalidOperationError(err error) bool {
	if err == nil {
		return false
	}

	errStr := err.Error()

	return strings.Contains(errStr, "InvalidOperationException") ||
		strings.Contains(errStr, "is not empty")
}
