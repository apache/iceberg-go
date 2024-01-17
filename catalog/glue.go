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

// GetTable loads a table from the Glue Catalog using the given database and table name.
func (c *GlueCatalog) GetTable(ctx context.Context, identifier table.Identifier) (*table.Table, error) {
	database, tableName, err := identifierToGlueTable(identifier)
	if err != nil {
		return nil, err
	}

	params := &glue.GetTableInput{DatabaseName: aws.String(database), Name: aws.String(tableName)}

	tblRes, err := c.glueSvc.GetTable(ctx, params)
	if err != nil {
		if errors.Is(err, &types.EntityNotFoundException{}) {
			return nil, ErrNoSuchTable
		}
		return nil, fmt.Errorf("failed to get table %s.%s: %w", database, tableName, err)
	}

	iofs, err := io.LoadFS(map[string]string{}, tblRes.Table.Parameters["metadata_location"])
	if err != nil {
		return nil, fmt.Errorf("failed to load table %s.%s: %w", database, tableName, err)
	}

	icebergTable, err := table.NewFromLocation([]string{tableName}, tblRes.Table.Parameters["metadata_location"], iofs)
	if err != nil {
		return nil, fmt.Errorf("failed to create table from location %s.%s: %w", database, tableName, err)
	}

	return icebergTable, nil
}

// ListTables returns a list of iceberg tables in the given Glue database.
func (c *GlueCatalog) ListTables(ctx context.Context, identifier table.Identifier) ([]*table.Table, error) {
	database, err := identifierToGlueDatabase(identifier)
	if err != nil {
		return nil, err
	}

	params := &glue.GetTablesInput{DatabaseName: aws.String(database)}

	tblsRes, err := c.glueSvc.GetTables(ctx, params)
	if err != nil {
		return nil, fmt.Errorf("failed to list tables in namespace %s: %w", database, err)
	}

	var icebergTables []*table.Table

	for _, tbl := range tblsRes.TableList {
		// skip non iceberg tables
		// TODO: consider what this would look like for non ICEBERG tables as you can convert them to ICEBERG tables via the Glue catalog API.
		if tbl.Parameters["table_type"] != "ICEBERG" {
			continue
		}

		iofs, err := io.LoadFS(map[string]string{}, tbl.Parameters["metadata_location"])
		if err != nil {
			return nil, fmt.Errorf("failed to load table %s.%s: %w", database, aws.ToString(tbl.Name), err)
		}

		icebergTable, err := table.NewFromLocation([]string{*tbl.Name}, tbl.Parameters["metadata_location"], iofs)
		if err != nil {
			return nil, fmt.Errorf("failed to create table from location %s.%s: %w", database, aws.ToString(tbl.Name), err)
		}

		icebergTables = append(icebergTables, icebergTable)
	}

	return icebergTables, nil
}

func (c *GlueCatalog) CatalogType() CatalogType {
	return Glue
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
