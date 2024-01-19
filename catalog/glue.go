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

	"github.com/apache/iceberg-go/io"
	"github.com/apache/iceberg-go/table"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/glue"
	"github.com/aws/aws-sdk-go-v2/service/glue/types"
)

var (
	_ Catalog = (*GlueCatalog)(nil)
)

type GlueAPI interface {
	GetTable(ctx context.Context, params *glue.GetTableInput, optFns ...func(*glue.Options)) (*glue.GetTableOutput, error)
	GetTables(ctx context.Context, params *glue.GetTablesInput, optFns ...func(*glue.Options)) (*glue.GetTablesOutput, error)
}

type GlueCatalog struct {
	glueSvc GlueAPI
}

func NewGlueCatalog(awscfg aws.Config) *GlueCatalog {
	return &GlueCatalog{
		glueSvc: glue.NewFromConfig(awscfg),
	}
}

// ListTables returns a list of iceberg tables in the given Glue database.
//
// The namespace should just contain the Glue database name.
func (c *GlueCatalog) ListTables(ctx context.Context, namespace table.Identifier) ([]table.Identifier, error) {
	database, err := identifierToGlueDatabase(namespace)
	if err != nil {
		return nil, err
	}

	params := &glue.GetTablesInput{DatabaseName: aws.String(database)}

	tblsRes, err := c.glueSvc.GetTables(ctx, params)
	if err != nil {
		return nil, fmt.Errorf("failed to list tables in namespace %s: %w", database, err)
	}

	var icebergTables []table.Identifier

	for _, tbl := range tblsRes.TableList {
		// skip non iceberg tables
		// TODO: consider what this would look like for non ICEBERG tables as you can convert them to ICEBERG tables via the Glue catalog API.
		if tbl.Parameters["table_type"] != "ICEBERG" {
			continue
		}

		icebergTables = append(icebergTables,
			GlueTableIdentifier(database, aws.ToString(tbl.Name)),
		)
	}

	return icebergTables, nil
}

// LoadTable loads a table from the catalog table details.
//
// The identifier should contain the Glue database name, then glue table name.
func (c *GlueCatalog) LoadTable(ctx context.Context, identifier table.Identifier) (*table.Table, error) {
	database, tableName, err := identifierToGlueTable(identifier)
	if err != nil {
		return nil, err
	}

	location, err := c.getTable(ctx, database, tableName)
	if err != nil {
		return nil, err
	}

	// TODO: consider providing a way to directly access the S3 iofs to enable testing of the catalog.
	iofs, err := io.LoadFS(map[string]string{}, location)
	if err != nil {
		return nil, fmt.Errorf("failed to load table %s.%s: %w", database, tableName, err)
	}

	icebergTable, err := table.NewFromLocation([]string{tableName}, location, iofs)
	if err != nil {
		return nil, fmt.Errorf("failed to create table from location %s.%s: %w", database, tableName, err)
	}

	return icebergTable, nil
}

func (c *GlueCatalog) CatalogType() CatalogType {
	return Glue
}

// GetTable loads a table from the Glue Catalog using the given database and table name.
func (c *GlueCatalog) getTable(ctx context.Context, database, tableName string) (string, error) {
	tblRes, err := c.glueSvc.GetTable(ctx,
		&glue.GetTableInput{
			DatabaseName: aws.String(database),
			Name:         aws.String(tableName),
		},
	)
	if err != nil {
		if errors.Is(err, &types.EntityNotFoundException{}) {
			return "", ErrNoSuchTable
		}
		return "", fmt.Errorf("failed to get table %s.%s: %w", database, tableName, err)
	}

	if tblRes.Table.Parameters["table_type"] != "ICEBERG" {
		return "", errors.New("table is not an iceberg table")
	}

	return tblRes.Table.Parameters["metadata_location"], nil
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

// GlueTableIdentifier returns a glue table identifier for an iceberg table in the format [database, table].
func GlueTableIdentifier(database string, table string) table.Identifier {
	return []string{database, table}
}

// GlueDatabaseIdentifier returns a database identifier for a Glue database in the format [database].
func GlueDatabaseIdentifier(database string) table.Identifier {
	return []string{database}
}
