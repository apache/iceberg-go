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

package catalog

import (
	"context"
	"errors"
	"fmt"

	"github.com/apache/iceberg-go"
	"github.com/apache/iceberg-go/io"
	"github.com/apache/iceberg-go/table"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/glue"
	"github.com/aws/aws-sdk-go-v2/service/glue/types"
)

const (
	glueTypeIceberg      = "ICEBERG"
	databaseTypePropsKey = "database_type"
	tableTypePropsKey    = "table_type"
	descriptionPropsKey  = "Description"

	// Database location.
	locationPropsKey = "Location"

	// Table metadata location pointer.
	metadataLocationPropsKey = "metadata_location"
)

var (
	_ Catalog = (*GlueCatalog)(nil)
)

type glueAPI interface {
	CreateTable(ctx context.Context, params *glue.CreateTableInput, optFns ...func(*glue.Options)) (*glue.CreateTableOutput, error)
	GetTable(ctx context.Context, params *glue.GetTableInput, optFns ...func(*glue.Options)) (*glue.GetTableOutput, error)
	GetTables(ctx context.Context, params *glue.GetTablesInput, optFns ...func(*glue.Options)) (*glue.GetTablesOutput, error)
	DeleteTable(ctx context.Context, params *glue.DeleteTableInput, optFns ...func(*glue.Options)) (*glue.DeleteTableOutput, error)
	GetDatabase(ctx context.Context, params *glue.GetDatabaseInput, optFns ...func(*glue.Options)) (*glue.GetDatabaseOutput, error)
	GetDatabases(ctx context.Context, params *glue.GetDatabasesInput, optFns ...func(*glue.Options)) (*glue.GetDatabasesOutput, error)
	CreateDatabase(ctx context.Context, params *glue.CreateDatabaseInput, optFns ...func(*glue.Options)) (*glue.CreateDatabaseOutput, error)
	DeleteDatabase(ctx context.Context, params *glue.DeleteDatabaseInput, optFns ...func(*glue.Options)) (*glue.DeleteDatabaseOutput, error)
	UpdateDatabase(ctx context.Context, params *glue.UpdateDatabaseInput, optFns ...func(*glue.Options)) (*glue.UpdateDatabaseOutput, error)
}

type GlueCatalog struct {
	glueSvc glueAPI
}

// NewGlueCatalog creates a new instance of GlueCatalog with the given options.
func NewGlueCatalog(opts ...Option[GlueCatalog]) *GlueCatalog {
	glueOps := &options{}

	for _, o := range opts {
		o(glueOps)
	}

	return &GlueCatalog{
		glueSvc: glue.NewFromConfig(glueOps.awsConfig),
	}
}

func (c *GlueCatalog) CatalogType() CatalogType {
	return Glue
}

// ListTables returns a list of Iceberg tables in the given Glue database.
//
// The namespace should just contain the Glue database name.
func (c *GlueCatalog) ListTables(ctx context.Context, namespace table.Identifier) ([]table.Identifier, error) {
	database, err := identifierToGlueDatabase(namespace)
	if err != nil {
		return nil, err
	}

	params := &glue.GetTablesInput{DatabaseName: aws.String(database)}

	var icebergTables []table.Identifier

	for {
		tblsRes, err := c.glueSvc.GetTables(ctx, params)
		if err != nil {
			return nil, fmt.Errorf("failed to list tables in namespace %s: %w", database, err)
		}

		icebergTables = append(icebergTables,
			filterTableListByType(database, tblsRes.TableList, glueTypeIceberg)...)

		if tblsRes.NextToken == nil {
			break
		}

		params.NextToken = tblsRes.NextToken
	}

	return icebergTables, nil
}

// LoadTable loads a table from the catalog table details.
//
// The identifier should contain the Glue database name, then Glue table name.
func (c *GlueCatalog) LoadTable(ctx context.Context, identifier table.Identifier, props iceberg.Properties) (*table.Table, error) {
	database, tableName, err := identifierToGlueTable(identifier)
	if err != nil {
		return nil, err
	}

	if props == nil {
		props = map[string]string{}
	}

	glueTable, err := c.getTable(ctx, database, tableName)
	if err != nil {
		return nil, err
	}

	location, ok := glueTable.Parameters[metadataLocationPropsKey]
	if !ok {
		return nil, fmt.Errorf("missing metadata location for table %s.%s", database, tableName)
	}

	// TODO: consider providing a way to directly access the S3 iofs to enable testing of the catalog.
	iofs, err := io.LoadFS(props, location)
	if err != nil {
		return nil, fmt.Errorf("failed to load table %s.%s: %w", database, tableName, err)
	}

	icebergTable, err := table.NewFromLocation([]string{tableName}, location, iofs)
	if err != nil {
		return nil, fmt.Errorf("failed to create table from location %s.%s: %w", database, tableName, err)
	}

	return icebergTable, nil
}

// DropTable deletes an Iceberg table from the Glue catalog.
func (c *GlueCatalog) DropTable(ctx context.Context, identifier table.Identifier) error {
	database, tableName, err := identifierToGlueTable(identifier)
	if err != nil {
		return err
	}

	// Check if the table exists and is an Iceberg table.
	_, err = c.getTable(ctx, database, tableName)
	if err != nil {
		return err
	}

	params := &glue.DeleteTableInput{
		DatabaseName: aws.String(database),
		Name:         aws.String(tableName),
	}
	_, err = c.glueSvc.DeleteTable(ctx, params)
	if err != nil {
		return fmt.Errorf("failed to drop table %s.%s: %w", database, tableName, err)
	}

	return nil
}

// RenameTable renames an Iceberg table in the Glue catalog.
func (c *GlueCatalog) RenameTable(ctx context.Context, from, to table.Identifier) (*table.Table, error) {
	fromDatabase, fromTable, err := identifierToGlueTable(from)
	if err != nil {
		return nil, err
	}

	toDatabase, toTable, err := identifierToGlueTable(to)
	if err != nil {
		return nil, err
	}

	if fromDatabase != toDatabase {
		return nil, fmt.Errorf("cannot rename table across namespaces: %s -> %s", fromDatabase, toDatabase)
	}

	// Fetch the existing Glue table to copy the metadata into the new table.
	fromGlueTable, err := c.getTable(ctx, fromDatabase, fromTable)
	if err != nil {
		return nil, fmt.Errorf("failed to fetch the table %s.%s: %w", fromDatabase, fromTable, err)
	}

	// Create the new table.
	_, err = c.glueSvc.CreateTable(ctx, &glue.CreateTableInput{
		DatabaseName: aws.String(toDatabase),
		TableInput: &types.TableInput{
			Name:              aws.String(toTable),
			Owner:             fromGlueTable.Owner,
			Description:       fromGlueTable.Description,
			Parameters:        fromGlueTable.Parameters,
			StorageDescriptor: fromGlueTable.StorageDescriptor,
		},
	})
	if err != nil {
		return nil, fmt.Errorf("failed to create the table %s.%s: %w", fromDatabase, fromTable, err)
	}

	// Drop the old table.
	_, err = c.glueSvc.DeleteTable(ctx, &glue.DeleteTableInput{
		DatabaseName: aws.String(fromDatabase),
		Name:         aws.String(fromTable),
	})
	if err != nil {
		// Best-effort rollback the table creation.
		_, rollbackErr := c.glueSvc.DeleteTable(ctx, &glue.DeleteTableInput{
			DatabaseName: aws.String(toDatabase),
			Name:         aws.String(toTable),
		})
		if rollbackErr != nil {
			fmt.Printf("failed to rollback the new table %s.%s: %v", toDatabase, toTable, rollbackErr)
		}

		return nil, fmt.Errorf("failed to rename the table %s.%s: %w", fromDatabase, fromTable, err)
	}

	// Load the new table to return.
	renamedTable, err := c.LoadTable(ctx, GlueTableIdentifier(toDatabase, toTable), nil)
	if err != nil {
		return nil, fmt.Errorf("failed to load renamed table %s.%s: %w", toDatabase, toTable, err)
	}

	return renamedTable, nil
}

// ListNamespaces returns a list of Iceberg namespaces from the given Glue catalog.
func (c *GlueCatalog) ListNamespaces(ctx context.Context, parent table.Identifier) ([]table.Identifier, error) {
	params := &glue.GetDatabasesInput{}

	if parent != nil {
		return nil, fmt.Errorf("hierarchical namespace is not supported")
	}

	var icebergNamespaces []table.Identifier

	for {
		databasesResp, err := c.glueSvc.GetDatabases(ctx, params)
		if err != nil {
			return nil, fmt.Errorf("failed to list databases: %w", err)
		}

		icebergNamespaces = append(icebergNamespaces,
			filterDatabaseListByType(databasesResp.DatabaseList, glueTypeIceberg)...)

		if databasesResp.NextToken == nil {
			break
		}

		params.NextToken = databasesResp.NextToken
	}

	return icebergNamespaces, nil
}

// CreateNamespace creates a new Iceberg namespace in the Glue catalog.
func (c *GlueCatalog) CreateNamespace(ctx context.Context, namespace table.Identifier, props iceberg.Properties) error {
	database, err := identifierToGlueDatabase(namespace)
	if err != nil {
		return err
	}

	databaseParameters := map[string]string{
		databaseTypePropsKey: glueTypeIceberg,
	}

	description := props[descriptionPropsKey]
	locationURI := props[locationPropsKey]

	if description != "" {
		databaseParameters[descriptionPropsKey] = description
	}
	if locationURI != "" {
		databaseParameters[locationPropsKey] = locationURI
	}

	databaseInput := &types.DatabaseInput{
		Name:       aws.String(database),
		Parameters: databaseParameters,
	}

	params := &glue.CreateDatabaseInput{DatabaseInput: databaseInput}
	_, err = c.glueSvc.CreateDatabase(ctx, params)

	if err != nil {
		return fmt.Errorf("failed to create database %s: %w", database, err)
	}

	return nil
}

// DropNamespace deletes an Iceberg namespace from the Glue catalog.
func (c *GlueCatalog) DropNamespace(ctx context.Context, namespace table.Identifier) error {
	databaseName, err := identifierToGlueDatabase(namespace)
	if err != nil {
		return err
	}

	// Check if the database exists and is an iceberg database.
	_, err = c.getDatabase(ctx, databaseName)
	if err != nil {
		return err
	}

	params := &glue.DeleteDatabaseInput{Name: aws.String(databaseName)}
	_, err = c.glueSvc.DeleteDatabase(ctx, params)
	if err != nil {
		return fmt.Errorf("failed to drop namespace %s: %w", databaseName, err)
	}

	return nil
}

// LoadNamespaceProperties loads the properties of an Iceberg namespace from the Glue catalog.
func (c *GlueCatalog) LoadNamespaceProperties(ctx context.Context, namespace table.Identifier) (iceberg.Properties, error) {
	databaseName, err := identifierToGlueDatabase(namespace)
	if err != nil {
		return nil, err
	}

	database, err := c.getDatabase(ctx, databaseName)
	if err != nil {
		return nil, err
	}

	props := make(map[string]string)
	if database.Parameters != nil {
		for k, v := range database.Parameters {
			props[k] = v
		}
	}

	return props, nil
}

// UpdateNamespaceProperties updates the properties of an Iceberg namespace in the Glue catalog.
// The removals list contains the keys to remove, and the updates map contains the keys and values to update.
func (c *GlueCatalog) UpdateNamespaceProperties(ctx context.Context, namespace table.Identifier,
	removals []string, updates iceberg.Properties) (PropertiesUpdateSummary, error) {

	databaseName, err := identifierToGlueDatabase(namespace)
	if err != nil {
		return PropertiesUpdateSummary{}, err
	}

	database, err := c.getDatabase(ctx, databaseName)
	if err != nil {
		return PropertiesUpdateSummary{}, err
	}

	overlap := []string{}
	for _, key := range removals {
		if _, exists := updates[key]; exists {
			overlap = append(overlap, key)
		}
	}
	if len(overlap) > 0 {
		return PropertiesUpdateSummary{}, fmt.Errorf("conflict between removals and updates for keys: %v", overlap)
	}

	updatedProperties := make(map[string]string)
	if database.Parameters != nil {
		for k, v := range database.Parameters {
			updatedProperties[k] = v
		}
	}

	// Removals.
	removed := []string{}
	for _, key := range removals {
		if _, exists := updatedProperties[key]; exists {
			delete(updatedProperties, key)
			removed = append(removed, key)
		}
	}

	// Updates.
	updated := []string{}
	for key, value := range updates {
		if updatedProperties[key] != value {
			updatedProperties[key] = value
			updated = append(updated, key)
		}
	}

	_, err = c.glueSvc.UpdateDatabase(ctx, &glue.UpdateDatabaseInput{Name: aws.String(databaseName), DatabaseInput: &types.DatabaseInput{
		Name:       aws.String(databaseName),
		Parameters: updatedProperties,
	}})
	if err != nil {
		return PropertiesUpdateSummary{}, fmt.Errorf("failed to update namespace properties %s: %w", databaseName, err)
	}

	propertiesUpdateSummary := PropertiesUpdateSummary{
		Removed: removed,
		Updated: updated,
		Missing: iceberg.Difference(removals, removed),
	}

	return propertiesUpdateSummary, nil
}

// GetTable loads a table from the Glue Catalog using the given database and table name.
func (c *GlueCatalog) getTable(ctx context.Context, database, tableName string) (*types.Table, error) {
	tblRes, err := c.glueSvc.GetTable(ctx,
		&glue.GetTableInput{
			DatabaseName: aws.String(database),
			Name:         aws.String(tableName),
		},
	)
	if err != nil {
		if errors.Is(err, &types.EntityNotFoundException{}) {
			return nil, fmt.Errorf("failed to get table %s.%s: %w", database, tableName, ErrNoSuchTable)
		}
		return nil, fmt.Errorf("failed to get table %s.%s: %w", database, tableName, err)
	}

	if tblRes.Table.Parameters[tableTypePropsKey] != glueTypeIceberg {
		return nil, fmt.Errorf("table %s.%s is not an iceberg table", database, tableName)
	}

	return tblRes.Table, nil
}

// GetDatabase loads a database from the Glue Catalog using the given database name.
func (c *GlueCatalog) getDatabase(ctx context.Context, databaseName string) (*types.Database, error) {
	database, err := c.glueSvc.GetDatabase(ctx, &glue.GetDatabaseInput{Name: aws.String(databaseName)})
	if err != nil {
		if errors.Is(err, &types.EntityNotFoundException{}) {
			return nil, fmt.Errorf("failed to get namespace %s: %w", databaseName, ErrNoSuchNamespace)
		}
		return nil, fmt.Errorf("failed to get namespace %s: %w", databaseName, err)
	}

	if database.Database.Parameters[databaseTypePropsKey] != glueTypeIceberg {
		return nil, fmt.Errorf("namespace %s is not an iceberg namespace", databaseName)
	}

	return database.Database, nil
}

func identifierToGlueTable(identifier table.Identifier) (string, string, error) {
	if len(identifier) != 2 {
		return "", "", fmt.Errorf("invalid identifier, missing database name: %v", identifier)
	}

	return identifier[0], identifier[1], nil
}

func identifierToGlueDatabase(identifier table.Identifier) (string, error) {
	if len(identifier) != 1 {
		return "", fmt.Errorf("invalid identifier, missing database name: %v", identifier)
	}

	return identifier[0], nil
}

// GlueTableIdentifier returns a glue table identifier for an Iceberg table in the format [database, table].
func GlueTableIdentifier(database string, tableName string) table.Identifier {
	return []string{database, tableName}
}

// GlueDatabaseIdentifier returns a database identifier for a Glue database in the format [database].
func GlueDatabaseIdentifier(database string) table.Identifier {
	return []string{database}
}

func filterTableListByType(database string, tableList []types.Table, tableType string) []table.Identifier {
	var filtered []table.Identifier

	for _, tbl := range tableList {
		if tbl.Parameters[tableTypePropsKey] != tableType {
			continue
		}
		filtered = append(filtered, GlueTableIdentifier(database, aws.ToString(tbl.Name)))
	}

	return filtered
}

func filterDatabaseListByType(databases []types.Database, databaseType string) []table.Identifier {
	var filtered []table.Identifier

	for _, database := range databases {
		if database.Parameters[databaseTypePropsKey] != databaseType {
			continue
		}
		filtered = append(filtered, GlueDatabaseIdentifier(aws.ToString(database.Name)))
	}

	return filtered
}
