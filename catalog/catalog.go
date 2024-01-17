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
	GetTable(ctx context.Context, identifier table.Identifier) (*table.Table, error)
	ListTables(ctx context.Context, identifier table.Identifier) ([]*table.Table, error)
	CatalogType() CatalogType
}
