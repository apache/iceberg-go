package catalog

import (
	"context"
	"errors"

	"github.com/apache/iceberg-go/table"
)

type CatalogType string

const (
	REST     CatalogType = "rest"
	Hive     CatalogType = "hive"
	Glue     CatalogType = "glue"
	DynamoDB CatalogType = "dynamodb"
	SQL      CatalogType = "sql"
)

var (
	// ErrNoSuchTable is returned when a table does not exist in the catalog.
	ErrNoSuchTable = errors.New("table does not exist")
)

// Catalog for iceberg table operations like create, drop, load, list and others.
type Catalog interface {
	GetTable(ctx context.Context, identifier table.Identifier) (CatalogTable, error)
	ListTables(ctx context.Context, identifier table.Identifier) ([]CatalogTable, error)
	CatalogType() CatalogType
}

// CatalogTable is the details of a table in a catalog.
type CatalogTable struct {
	Identifier  table.Identifier // this identifier may vary depending on the catalog implementation
	Location    string           // URL to the table location
	CatalogType CatalogType
}
