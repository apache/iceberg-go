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

package glue

import (
	"context"
	"errors"
	"fmt"
	"iter"
	"maps"
	"strconv"
	"strings"
	_ "unsafe"

	"github.com/apache/iceberg-go"
	"github.com/apache/iceberg-go/catalog"
	"github.com/apache/iceberg-go/catalog/internal"
	"github.com/apache/iceberg-go/io"
	"github.com/apache/iceberg-go/table"
	"github.com/apache/iceberg-go/utils"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/glue"
	"github.com/aws/aws-sdk-go-v2/service/glue/types"
)

const (
	// Use the same conventions as in the pyiceberg project.
	// See: https://github.com/apache/iceberg-python/blob/main/pyiceberg/catalog/__init__.py#L82-L96
	glueTypeIceberg     = "ICEBERG"
	tableTypePropsKey   = "table_type"
	descriptionPropsKey = "Description"

	// Database location.
	locationPropsKey = "Location"

	// Table metadata location pointer.
	metadataLocationPropsKey = "metadata_location"

	// The ID of the Glue Data Catalog where the tables reside. If none is provided, Glue
	// automatically uses the caller's AWS account ID by default.
	// See: https://docs.aws.amazon.com/glue/latest/dg/aws-glue-api-catalog-databases.html
	CatalogIdKey = "glue.id"

	AccessKeyID     = "glue.access-key-id"
	SecretAccessKey = "glue.secret-access-key"
	SessionToken    = "glue.session-token"
	Region          = "glue.region"
	Endpoint        = "glue.endpoint"
	MaxRetries      = "glue.max-retries"
	RetryMode       = "glue.retry-mode"

	icebergFieldIDKey       = "iceberg.field.id"
	icebergFieldOptionalKey = "iceberg.field.optional"
	icebergFieldCurrentKey  = "iceberg.field.current"
)

var _ catalog.Catalog = (*Catalog)(nil)

func init() {
	catalog.Register("glue", catalog.RegistrarFunc(func(ctx context.Context, _ string, props iceberg.Properties) (catalog.Catalog, error) {
		awsConfig, err := toAwsConfig(ctx, props)
		if err != nil {
			return nil, err
		}

		return NewCatalog(WithAwsConfig(awsConfig), WithAwsProperties(AwsProperties(props))), nil
	}))
}

func toAwsConfig(ctx context.Context, p iceberg.Properties) (aws.Config, error) {
	opts := make([]func(*config.LoadOptions) error, 0)

	for k, v := range p {
		switch k {
		case Region:
			opts = append(opts, config.WithRegion(v))
		case Endpoint:
			opts = append(opts, config.WithBaseEndpoint(v))
		case MaxRetries:
			maxRetry, err := strconv.Atoi(v)
			if err != nil {
				return aws.Config{}, err
			}
			opts = append(opts, config.WithRetryMaxAttempts(maxRetry))
		case RetryMode:
			m, err := aws.ParseRetryMode(v)
			if err != nil {
				return aws.Config{}, err
			}
			opts = append(opts, config.WithRetryMode(m))
		}
	}

	key, secret, token := p[AccessKeyID], p[SecretAccessKey], p[SessionToken]
	if key != "" || secret != "" || token != "" {
		opts = append(opts, config.WithCredentialsProvider(
			credentials.NewStaticCredentialsProvider(key, secret, token)))
	}

	return config.LoadDefaultConfig(ctx, opts...)
}

type glueAPI interface {
	CreateTable(ctx context.Context, params *glue.CreateTableInput, optFns ...func(*glue.Options)) (*glue.CreateTableOutput, error)
	GetTable(ctx context.Context, params *glue.GetTableInput, optFns ...func(*glue.Options)) (*glue.GetTableOutput, error)
	GetTables(ctx context.Context, params *glue.GetTablesInput, optFns ...func(*glue.Options)) (*glue.GetTablesOutput, error)
	DeleteTable(ctx context.Context, params *glue.DeleteTableInput, optFns ...func(*glue.Options)) (*glue.DeleteTableOutput, error)
	UpdateTable(ctx context.Context, params *glue.UpdateTableInput, optFns ...func(*glue.Options)) (*glue.UpdateTableOutput, error)
	GetDatabase(ctx context.Context, params *glue.GetDatabaseInput, optFns ...func(*glue.Options)) (*glue.GetDatabaseOutput, error)
	GetDatabases(ctx context.Context, params *glue.GetDatabasesInput, optFns ...func(*glue.Options)) (*glue.GetDatabasesOutput, error)
	CreateDatabase(ctx context.Context, params *glue.CreateDatabaseInput, optFns ...func(*glue.Options)) (*glue.CreateDatabaseOutput, error)
	DeleteDatabase(ctx context.Context, params *glue.DeleteDatabaseInput, optFns ...func(*glue.Options)) (*glue.DeleteDatabaseOutput, error)
	UpdateDatabase(ctx context.Context, params *glue.UpdateDatabaseInput, optFns ...func(*glue.Options)) (*glue.UpdateDatabaseOutput, error)
}

type Catalog struct {
	glueSvc   glueAPI
	catalogId *string
	awsCfg    *aws.Config
	props     iceberg.Properties
}

// NewCatalog creates a new instance of glue.Catalog with the given options.
func NewCatalog(opts ...Option) *Catalog {
	glueOps := &options{}

	for _, o := range opts {
		o(glueOps)
	}

	if glueOps.awsProperties == nil {
		glueOps.awsProperties = AwsProperties{}
	}

	var catalogId *string
	if val, ok := glueOps.awsProperties[CatalogIdKey]; ok {
		catalogId = &val
	} else {
		catalogId = nil
	}

	return &Catalog{
		glueSvc:   glue.NewFromConfig(glueOps.awsConfig),
		catalogId: catalogId,
		awsCfg:    &glueOps.awsConfig,
		props:     iceberg.Properties(glueOps.awsProperties),
	}
}

// ListTables returns a list of Iceberg tables in the given Glue database.
//
// The namespace should just contain the Glue database name.
func (c *Catalog) ListTables(ctx context.Context, namespace table.Identifier) iter.Seq2[table.Identifier, error] {
	return func(yield func(table.Identifier, error) bool) {
		database, err := identifierToGlueDatabase(namespace)
		if err != nil {
			yield(table.Identifier{}, err)

			return
		}

		paginator := glue.NewGetTablesPaginator(c.glueSvc, &glue.GetTablesInput{
			CatalogId:    c.catalogId,
			DatabaseName: aws.String(database),
		})

		for paginator.HasMorePages() {
			page, err := paginator.NextPage(ctx)
			if err != nil {
				yield(table.Identifier{}, fmt.Errorf("failed to list tables in namespace %s: %w", database, err))

				return
			}

			icebergTables := filterTableListByType(database, page.TableList, glueTypeIceberg)
			for _, tbl := range icebergTables {
				if !yield(tbl, nil) {
					return
				}
			}
		}
	}
}

// LoadTable loads a table from the catalog table details.
//
// The identifier should contain the Glue database name, then Glue table name.
func (c *Catalog) LoadTable(ctx context.Context, identifier table.Identifier) (*table.Table, error) {
	database, tableName, err := identifierToGlueTable(identifier)
	if err != nil {
		return nil, err
	}

	glueTable, err := c.getTable(ctx, database, tableName)
	if err != nil {
		return nil, err
	}

	location, ok := glueTable.Parameters[metadataLocationPropsKey]
	if !ok {
		return nil, fmt.Errorf("missing metadata location for table %s.%s", database, tableName)
	}

	ctx = utils.WithAwsConfig(ctx, c.awsCfg)

	icebergTable, err := table.NewFromLocation(
		ctx,
		identifier,
		location,
		io.LoadFSFunc(nil, location),
		c,
	)
	if err != nil {
		return nil, fmt.Errorf("failed to create table from location %s.%s: %w", database, tableName, err)
	}

	return icebergTable, nil
}

func (c *Catalog) CatalogType() catalog.Type {
	return catalog.Glue
}

// CreateTable creates a new Iceberg table in the Glue catalog.
// This function will create the metadata file in S3 using the catalog and table properties,
// to determine the bucket and key for the metadata location.
func (c *Catalog) CreateTable(ctx context.Context, identifier table.Identifier, schema *iceberg.Schema, opts ...catalog.CreateTableOpt) (*table.Table, error) {
	staged, err := internal.CreateStagedTable(ctx, c.props, c.LoadNamespaceProperties, identifier, schema, opts...)
	if err != nil {
		return nil, err
	}

	database, tableName, err := identifierToGlueTable(identifier)
	if err != nil {
		return nil, err
	}

	afs, err := staged.FS(ctx)
	if err != nil {
		return nil, err
	}
	wfs, ok := afs.(io.WriteFileIO)
	if !ok {
		return nil, errors.New("loaded filesystem IO does not support writing")
	}

	if err := internal.WriteTableMetadata(staged.Metadata(), wfs, staged.MetadataLocation()); err != nil {
		return nil, err
	}

	var tableDescription *string
	if desc := staged.Properties().Get("Description", ""); desc != "" {
		tableDescription = aws.String(desc)
	}

	tableInput := &types.TableInput{
		Name: aws.String(tableName),
		Parameters: map[string]string{
			tableTypePropsKey:        glueTypeIceberg,
			metadataLocationPropsKey: staged.MetadataLocation(),
		},
		TableType: aws.String("EXTERNAL_TABLE"),
		StorageDescriptor: &types.StorageDescriptor{
			Location: aws.String(staged.Metadata().Location()),
			Columns:  schemaToGlueColumns(schema, true),
		},
		Description: tableDescription,
	}
	_, err = c.glueSvc.CreateTable(ctx, &glue.CreateTableInput{
		CatalogId:    c.catalogId,
		DatabaseName: aws.String(database),
		TableInput:   tableInput,
	})
	if err != nil {
		return nil, fmt.Errorf("failed to create table %s.%s: %w", database, tableName, err)
	}
	createdTable, err := c.LoadTable(ctx, identifier)
	if err != nil {
		// Attempt to clean up the table if loading fails
		_, cleanupErr := c.glueSvc.DeleteTable(ctx, &glue.DeleteTableInput{
			CatalogId:    c.catalogId,
			DatabaseName: aws.String(database),
			Name:         aws.String(tableName),
		})
		if cleanupErr != nil {
			return nil, fmt.Errorf("failed to create table %s.%s and cleanup failed: %v (original error: %w)",
				database, tableName, cleanupErr, err)
		}

		return nil, fmt.Errorf("failed to create table %s.%s: %w", database, tableName, err)
	}

	return createdTable, nil
}

// RegisterTable registers a new table using existing metadata.
func (c *Catalog) RegisterTable(ctx context.Context, identifier table.Identifier, metadataLocation string) (*table.Table, error) {
	database, tableName, err := identifierToGlueTable(identifier)
	if err != nil {
		return nil, err
	}
	// Load the metadata file to get table properties
	ctx = utils.WithAwsConfig(ctx, c.awsCfg)
	// Read the metadata file
	metadata, err := table.NewFromLocation(
		ctx,
		[]string{tableName},
		metadataLocation,
		io.LoadFSFunc(nil, metadataLocation),
		c,
	)
	if err != nil {
		return nil, fmt.Errorf("failed to read table metadata from %s: %w", metadataLocation, err)
	}
	tableInput := &types.TableInput{
		Name:       aws.String(tableName),
		Parameters: map[string]string{},
		TableType:  aws.String("EXTERNAL_TABLE"),
		StorageDescriptor: &types.StorageDescriptor{
			Location: aws.String(metadataLocation),
			Columns:  schemaToGlueColumns(metadata.Schema(), true),
		},
	}
	_, err = c.glueSvc.CreateTable(ctx, &glue.CreateTableInput{
		CatalogId:    c.catalogId,
		DatabaseName: aws.String(database),
		TableInput:   tableInput,
		OpenTableFormatInput: &types.OpenTableFormatInput{
			IcebergInput: &types.IcebergInput{
				MetadataOperation: types.MetadataOperationCreate,
			},
		},
	})
	if err != nil {
		return nil, fmt.Errorf("failed to register table %s.%s: %w", database, tableName, err)
	}

	return c.LoadTable(ctx, identifier)
}

func (c *Catalog) CommitTable(ctx context.Context, identifier table.Identifier, requirements []table.Requirement, updates []table.Update) (table.Metadata, string, error) {
	// Load current table
	database, tableName, err := identifierToGlueTable(identifier)
	if err != nil {
		return nil, "", err
	}
	current, err := c.LoadTable(ctx, identifier)
	if err != nil && !errors.Is(err, catalog.ErrNoSuchTable) {
		return nil, "", err
	}

	// Create a staging table with the updates applied
	staged, err := internal.UpdateAndStageTable(ctx, current, identifier, requirements, updates, c)
	if err != nil {
		return nil, "", err
	}
	if current != nil && staged.Metadata().Equals(current.Metadata()) {
		return current.Metadata(), current.MetadataLocation(), nil
	}
	if err := internal.WriteMetadata(ctx, staged.Metadata(), staged.MetadataLocation(), staged.Properties()); err != nil {
		return nil, "", err
	}

	// Build and call Glue update request
	tableInput, err := buildGlueTableInput(ctx, database, tableName, staged, c)
	if err != nil {
		return nil, "", err
	}
	_, err = c.glueSvc.UpdateTable(ctx, &glue.UpdateTableInput{
		CatalogId:    c.catalogId,
		DatabaseName: aws.String(database),
		TableInput:   tableInput,
	})
	if err != nil {
		return nil, "", err
	}

	return staged.Metadata(), staged.MetadataLocation(), err
}

// DropTable deletes an Iceberg table from the Glue catalog.
func (c *Catalog) DropTable(ctx context.Context, identifier table.Identifier) error {
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
		CatalogId:    c.catalogId,
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
func (c *Catalog) RenameTable(ctx context.Context, from, to table.Identifier) (*table.Table, error) {
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
		CatalogId:    c.catalogId,
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
		CatalogId:    c.catalogId,
		DatabaseName: aws.String(fromDatabase),
		Name:         aws.String(fromTable),
	})
	if err != nil {
		// Best-effort rollback the table creation.
		_, rollbackErr := c.glueSvc.DeleteTable(ctx, &glue.DeleteTableInput{
			CatalogId:    c.catalogId,
			DatabaseName: aws.String(toDatabase),
			Name:         aws.String(toTable),
		})
		if rollbackErr != nil {
			fmt.Printf("failed to rollback the new table %s.%s: %v", toDatabase, toTable, rollbackErr)
		}

		return nil, fmt.Errorf("failed to rename the table %s.%s: %w", fromDatabase, fromTable, err)
	}

	// Load the new table to return.
	renamedTable, err := c.LoadTable(ctx, TableIdentifier(toDatabase, toTable))
	if err != nil {
		return nil, fmt.Errorf("failed to load renamed table %s.%s: %w", toDatabase, toTable, err)
	}

	return renamedTable, nil
}

// CheckTableExists returns if an Iceberg table exists in the Glue catalog.
func (c *Catalog) CheckTableExists(ctx context.Context, identifier table.Identifier) (bool, error) {
	database, tableName, err := identifierToGlueTable(identifier)
	if err != nil {
		return false, err
	}

	_, err = c.getTable(ctx, database, tableName)
	if err != nil {
		if errors.Is(err, catalog.ErrNoSuchTable) {
			return false, nil
		}

		return false, err
	}

	return true, nil
}

// CreateNamespace creates a new Iceberg namespace in the Glue catalog.
func (c *Catalog) CreateNamespace(ctx context.Context, namespace table.Identifier, props iceberg.Properties) error {
	database, err := identifierToGlueDatabase(namespace)
	if err != nil {
		return err
	}

	databaseParameters := map[string]string{}
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

	params := &glue.CreateDatabaseInput{CatalogId: c.catalogId, DatabaseInput: databaseInput}
	_, err = c.glueSvc.CreateDatabase(ctx, params)
	if err != nil {
		return fmt.Errorf("failed to create database %s: %w", database, err)
	}

	return nil
}

func (c *Catalog) CheckNamespaceExists(ctx context.Context, namespace table.Identifier) (bool, error) {
	databaseName, err := identifierToGlueDatabase(namespace)
	if err != nil {
		return false, err
	}

	_, err = c.getDatabase(ctx, databaseName)
	if err != nil {
		if errors.Is(err, catalog.ErrNoSuchNamespace) {
			return false, nil
		}

		return false, err
	}

	return true, nil
}

// DropNamespace deletes an Iceberg namespace from the Glue catalog.
func (c *Catalog) DropNamespace(ctx context.Context, namespace table.Identifier) error {
	databaseName, err := identifierToGlueDatabase(namespace)
	if err != nil {
		return err
	}

	// Check if the database exists and is an iceberg database.
	_, err = c.getDatabase(ctx, databaseName)
	if err != nil {
		return err
	}

	params := &glue.DeleteDatabaseInput{CatalogId: c.catalogId, Name: aws.String(databaseName)}
	_, err = c.glueSvc.DeleteDatabase(ctx, params)
	if err != nil {
		return fmt.Errorf("failed to drop namespace %s: %w", databaseName, err)
	}

	return nil
}

// LoadNamespaceProperties loads the properties of an Iceberg namespace from the Glue catalog.
func (c *Catalog) LoadNamespaceProperties(ctx context.Context, namespace table.Identifier) (iceberg.Properties, error) {
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

// avoid circular dependency while still avoiding having to export the getUpdatedPropsAndUpdateSummary function
// so that we can re-use it in the catalog implementations without duplicating the code.

//go:linkname getUpdatedPropsAndUpdateSummary github.com/apache/iceberg-go/catalog.getUpdatedPropsAndUpdateSummary
func getUpdatedPropsAndUpdateSummary(currentProps iceberg.Properties, removals []string, updates iceberg.Properties) (iceberg.Properties, catalog.PropertiesUpdateSummary, error)

// UpdateNamespaceProperties updates the properties of an Iceberg namespace in the Glue catalog.
// The removals list contains the keys to remove, and the updates map contains the keys and values to update.
func (c *Catalog) UpdateNamespaceProperties(ctx context.Context, namespace table.Identifier,
	removals []string, updates iceberg.Properties,
) (catalog.PropertiesUpdateSummary, error) {
	databaseName, err := identifierToGlueDatabase(namespace)
	if err != nil {
		return catalog.PropertiesUpdateSummary{}, err
	}

	database, err := c.getDatabase(ctx, databaseName)
	if err != nil {
		return catalog.PropertiesUpdateSummary{}, err
	}

	updatedProperties, propertiesUpdateSummary, err := getUpdatedPropsAndUpdateSummary(database.Parameters, removals, updates)
	if err != nil {
		return catalog.PropertiesUpdateSummary{}, err
	}

	_, err = c.glueSvc.UpdateDatabase(ctx, &glue.UpdateDatabaseInput{CatalogId: c.catalogId, Name: aws.String(databaseName), DatabaseInput: &types.DatabaseInput{
		Name:       aws.String(databaseName),
		Parameters: updatedProperties,
	}})
	if err != nil {
		return catalog.PropertiesUpdateSummary{}, fmt.Errorf("failed to update namespace properties %s: %w", databaseName, err)
	}

	return propertiesUpdateSummary, nil
}

// ListNamespaces returns a list of Iceberg namespaces from the given Glue catalog.
func (c *Catalog) ListNamespaces(ctx context.Context, parent table.Identifier) ([]table.Identifier, error) {
	params := &glue.GetDatabasesInput{
		CatalogId: c.catalogId,
	}

	if parent != nil {
		return nil, errors.New("hierarchical namespace is not supported")
	}

	var icebergNamespaces []table.Identifier

	paginator := glue.NewGetDatabasesPaginator(c.glueSvc, params)
	for paginator.HasMorePages() {
		rsp, err := paginator.NextPage(ctx)
		if err != nil {
			return nil, fmt.Errorf("failed to list databases: %w", err)
		}

		for _, database := range rsp.DatabaseList {
			icebergNamespaces = append(icebergNamespaces, DatabaseIdentifier(aws.ToString(database.Name)))
		}
	}

	return icebergNamespaces, nil
}

// GetTable loads a table from the Glue Catalog using the given database and table name.
func (c *Catalog) getTable(ctx context.Context, database, tableName string) (*types.Table, error) {
	tblRes, err := c.glueSvc.GetTable(ctx,
		&glue.GetTableInput{
			CatalogId:    c.catalogId,
			DatabaseName: aws.String(database),
			Name:         aws.String(tableName),
		},
	)
	if err != nil {
		var notFoundErr *types.EntityNotFoundException
		if errors.As(err, &notFoundErr) {
			return nil, fmt.Errorf("failed to get table %s.%s: %w", database, tableName, catalog.ErrNoSuchTable)
		}

		return nil, fmt.Errorf("failed to get table %s.%s: %w", database, tableName, err)
	}

	if !strings.EqualFold(tblRes.Table.Parameters[tableTypePropsKey], glueTypeIceberg) {
		return nil, fmt.Errorf("table %s.%s is not an iceberg table", database, tableName)
	}

	return tblRes.Table, nil
}

// GetDatabase loads a database from the Glue Catalog using the given database name.
func (c *Catalog) getDatabase(ctx context.Context, databaseName string) (*types.Database, error) {
	database, err := c.glueSvc.GetDatabase(ctx, &glue.GetDatabaseInput{CatalogId: c.catalogId, Name: aws.String(databaseName)})
	if err != nil {
		var notFoundErr *types.EntityNotFoundException
		if errors.As(err, &notFoundErr) {
			return nil, fmt.Errorf("failed to get namespace %s: %w", databaseName, catalog.ErrNoSuchNamespace)
		}

		return nil, fmt.Errorf("failed to get namespace %s: %w", databaseName, err)
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

// TableIdentifier returns a glue table identifier for an Iceberg table in the format [database, table].
func TableIdentifier(database string, tableName string) table.Identifier {
	return []string{database, tableName}
}

// DatabaseIdentifier returns a database identifier for a Glue database in the format [database].
func DatabaseIdentifier(database string) table.Identifier {
	return []string{database}
}

func filterTableListByType(database string, tableList []types.Table, tableType string) []table.Identifier {
	var filtered []table.Identifier
	for _, tbl := range tableList {
		if !strings.EqualFold(tbl.Parameters[tableTypePropsKey], tableType) {
			continue
		}
		filtered = append(filtered, TableIdentifier(database, aws.ToString(tbl.Name)))
	}

	return filtered
}

func buildGlueTableInput(ctx context.Context, database string, tableName string, staged *table.StagedTable, cat *Catalog) (*types.TableInput, error) {
	glueTable, err := cat.getTable(ctx, database, tableName)
	if err != nil {
		return nil, err
	}

	glueProperties := prepareProperties(staged.Properties(), staged.MetadataLocation())
	description := staged.Properties()["comment"]
	if description == "" {
		description = aws.ToString(glueTable.Description)
	}
	existingColumnMap := map[string]string{}
	for _, column := range glueTable.StorageDescriptor.Columns {
		existingColumnMap[*column.Name] = *column.Comment
	}
	var glueColumns []types.Column
	for _, column := range schemaToGlueColumns(staged.Metadata().CurrentSchema(), true) {
		col := types.Column{
			Name:    column.Name,
			Comment: column.Comment,
			Type:    column.Type,
		}
		if column.Comment == nil || *column.Comment == "" {
			col.Comment = aws.String(existingColumnMap[*column.Name])
		}
		glueColumns = append(glueColumns, col)
	}

	return &types.TableInput{
		Name:        aws.String(tableName),
		Description: aws.String(description),
		Parameters:  glueProperties,
		StorageDescriptor: &types.StorageDescriptor{
			Location: aws.String(staged.Location()),
			Columns:  glueColumns,
		},
	}, nil
}

func prepareProperties(icebergProperties iceberg.Properties, newMetadataLocation string) iceberg.Properties {
	glueProperties := make(iceberg.Properties)
	maps.Copy(glueProperties, icebergProperties)
	glueProperties[tableTypePropsKey] = glueTypeIceberg
	glueProperties[metadataLocationPropsKey] = newMetadataLocation

	return glueProperties
}
