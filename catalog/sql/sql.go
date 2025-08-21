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

package sql

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"iter"
	"maps"
	"slices"
	"strings"
	"sync"
	_ "unsafe"

	"github.com/apache/iceberg-go"
	"github.com/apache/iceberg-go/catalog"
	"github.com/apache/iceberg-go/catalog/internal"
	"github.com/apache/iceberg-go/io"
	"github.com/apache/iceberg-go/table"
	"github.com/uptrace/bun"
	"github.com/uptrace/bun/dialect/feature"
	"github.com/uptrace/bun/dialect/mssqldialect"
	"github.com/uptrace/bun/dialect/mysqldialect"
	"github.com/uptrace/bun/dialect/oracledialect"
	"github.com/uptrace/bun/dialect/pgdialect"
	"github.com/uptrace/bun/dialect/sqlitedialect"
	"github.com/uptrace/bun/extra/bundebug"
	"github.com/uptrace/bun/schema"
)

type SupportedDialect string

const (
	Postgres SupportedDialect = "postgres"
	MySQL    SupportedDialect = "mysql"
	SQLite   SupportedDialect = "sqlite"
	MSSQL    SupportedDialect = "mssql"
	Oracle   SupportedDialect = "oracle"
)

const (
	DialectKey           = "sql.dialect"
	DriverKey            = "sql.driver"
	initCatalogTablesKey = "init_catalog_tables"
)

const (
	TableType = "TABLE"
	ViewType  = "VIEW"
)

func init() {
	catalog.Register("sql", catalog.RegistrarFunc(func(ctx context.Context, name string, p iceberg.Properties) (c catalog.Catalog, err error) {
		driver, ok := p[DriverKey]
		if !ok {
			return nil, errors.New("must provide driver to pass to sql.Open")
		}

		dialect := strings.ToLower(p[DialectKey])
		if dialect == "" {
			return nil, errors.New("must provide sql dialect to use")
		}

		uri := strings.TrimPrefix(p.Get("uri", ""), "sql://")
		sqldb, err := sql.Open(driver, uri)
		if err != nil {
			return nil, err
		}

		defer func() {
			if r := recover(); r != nil {
				err = fmt.Errorf("failed to create SQL catalog: %v", r)
			}
		}()

		return NewCatalog(p.Get(name, "sql"), sqldb, SupportedDialect(dialect), p)
	}))
}

var _ catalog.Catalog = (*Catalog)(nil)

var (
	minimalNamespaceProps = iceberg.Properties{"exists": "true"}

	dialects  = map[SupportedDialect]schema.Dialect{}
	dialectMx sync.Mutex
)

func createDialect(d SupportedDialect) schema.Dialect {
	switch d {
	case Postgres:
		return pgdialect.New()
	case MySQL:
		return mysqldialect.New()
	case SQLite:
		return sqlitedialect.New()
	case MSSQL:
		return mssqldialect.New()
	case Oracle:
		return oracledialect.New()
	default:
		panic("unsupported sql dialect")
	}
}

func getDialect(d SupportedDialect) schema.Dialect {
	dialectMx.Lock()
	defer dialectMx.Unlock()
	ret, ok := dialects[d]
	if !ok {
		ret = createDialect(d)
		dialects[d] = ret
	}

	return ret
}

type sqlIcebergTable struct {
	bun.BaseModel `bun:"table:iceberg_tables"`

	CatalogName              string `bun:",pk"`
	TableNamespace           string `bun:",pk"`
	TableName                string `bun:",pk"`
	IcebergType              string // TableType or ViewType
	MetadataLocation         sql.NullString
	PreviousMetadataLocation sql.NullString
}

type sqlIcebergNamespaceProps struct {
	bun.BaseModel `bun:"table:iceberg_namespace_properties"`

	CatalogName   string `bun:",pk"`
	Namespace     string `bun:",pk"`
	PropertyKey   string `bun:",pk"`
	PropertyValue sql.NullString
}

func withReadTx[R any](ctx context.Context, db *bun.DB, fn func(context.Context, bun.Tx) (R, error)) (result R, err error) {
	db.RunInTx(ctx, &sql.TxOptions{ReadOnly: true}, func(ctx context.Context, tx bun.Tx) error {
		result, err = fn(ctx, tx)

		return err
	})

	return
}

func withWriteTx(ctx context.Context, db *bun.DB, fn func(context.Context, bun.Tx) error) error {
	return db.RunInTx(ctx, &sql.TxOptions{Isolation: sql.LevelDefault}, func(ctx context.Context, tx bun.Tx) error {
		return fn(ctx, tx)
	})
}

type Catalog struct {
	db    *bun.DB
	name  string
	props iceberg.Properties
}

// NewCatalog creates a new sql-based catalog using the provided sql.DB handle to perform any queries.
//
// The dialect parameter determines the SQL dialect to use for query generation and must be one of the
// supported dialects, i.e. one of the exported SupportedDialect values. The separation here allows for
// the use of different drivers/databases provided they support the chosen sql dialect (e.g. if a particular
// database supports the MySQL dialect, then the database can still be used with this catalog even though
// it's not explicitly implemented).
//
// If the "init_catalog_tables" property is set to "true", then creating the catalog will also attempt to
// to verify whether the necessary tables (iceberg_tables and iceberg_namespace_properties) exist, creating
// them if they do not already exist.
//
// The environment variable ICEBERG_SQL_DEBUG can be set to automatically log the sql queries to the terminal:
// - ICEBERG_SQL_DEBUG=1 logs only failed queries
// - ICEBERG_SQL_DEBUG=2 logs all queries
//
// All interactions with the db are performed within transactions to ensure atomicity and transactional isolation
// of catalog changes.
func NewCatalog(name string, db *sql.DB, dialect SupportedDialect, props iceberg.Properties) (*Catalog, error) {
	cat := &Catalog{db: bun.NewDB(db, getDialect(dialect)), name: name, props: props}

	cat.db.AddQueryHook(bundebug.NewQueryHook(
		bundebug.WithEnabled(false),
		// ICEBERG_SQL_DEBUG=1 logs only failed queries
		// ICEBERG_SQL_DEBUG=2 log all queries
		bundebug.FromEnv("ICEBERG_SQL_DEBUG")))

	if cat.props.GetBool(initCatalogTablesKey, true) {
		return cat, cat.ensureTablesExist()
	}

	return cat, nil
}

func (c *Catalog) Name() string { return c.name }

func (c *Catalog) CatalogType() catalog.Type {
	return catalog.SQL
}

func (c *Catalog) CreateSQLTables(ctx context.Context) error {
	_, err := c.db.NewCreateTable().Model((*sqlIcebergTable)(nil)).
		IfNotExists().Exec(ctx)
	if err != nil {
		return err
	}

	_, err = c.db.NewCreateTable().Model((*sqlIcebergNamespaceProps)(nil)).
		IfNotExists().Exec(ctx)

	return err
}

func (c *Catalog) DropSQLTables(ctx context.Context) error {
	_, err := c.db.NewDropTable().Model((*sqlIcebergTable)(nil)).
		IfExists().Exec(ctx)
	if err != nil {
		return err
	}

	_, err = c.db.NewDropTable().Model((*sqlIcebergNamespaceProps)(nil)).
		IfExists().Exec(ctx)

	return err
}

func (c *Catalog) ensureTablesExist() error {
	return c.CreateSQLTables(context.Background())
}

func (c *Catalog) namespaceExists(ctx context.Context, ns string) (bool, error) {
	return withReadTx(ctx, c.db, func(ctx context.Context, tx bun.Tx) (bool, error) {
		exists, err := tx.NewSelect().Model((*sqlIcebergTable)(nil)).
			Where("catalog_name = ?", c.name).
			Where("table_namespace = ?", ns).
			Limit(1).Exists(ctx)
		if err != nil {
			return false, err
		}
		if exists {
			return true, nil
		}

		return tx.NewSelect().Model((*sqlIcebergNamespaceProps)(nil)).
			Where("catalog_name = ?", c.name).Where("namespace = ?", ns).
			Limit(1).Exists(ctx)
	})
}

func checkValidNamespace(ident table.Identifier) error {
	if len(ident) < 1 {
		return fmt.Errorf("%w: empty namespace identifier", catalog.ErrNoSuchNamespace)
	}

	return nil
}

func (c *Catalog) CreateTable(ctx context.Context, ident table.Identifier, sc *iceberg.Schema, opts ...catalog.CreateTableOpt) (*table.Table, error) {
	staged, err := internal.CreateStagedTable(ctx, c.props, c.LoadNamespaceProperties, ident, sc, opts...)
	if err != nil {
		return nil, err
	}

	nsIdent := catalog.NamespaceFromIdent(ident)
	tblIdent := catalog.TableNameFromIdent(ident)
	ns := strings.Join(nsIdent, ".")
	exists, err := c.namespaceExists(ctx, ns)
	if err != nil {
		return nil, err
	}

	if !exists {
		return nil, fmt.Errorf("%w: %s", catalog.ErrNoSuchNamespace, ns)
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

	err = withWriteTx(ctx, c.db, func(ctx context.Context, tx bun.Tx) error {
		_, err := tx.NewInsert().Model(&sqlIcebergTable{
			CatalogName:      c.name,
			TableNamespace:   ns,
			TableName:        tblIdent,
			MetadataLocation: sql.NullString{String: staged.MetadataLocation(), Valid: true},
			IcebergType:      TableType,
		}).Exec(ctx)
		if err != nil {
			return fmt.Errorf("failed to create table: %w", err)
		}

		return nil
	})
	if err != nil {
		return nil, err
	}

	return c.LoadTable(ctx, ident)
}

func (c *Catalog) CommitTable(ctx context.Context, ident table.Identifier, reqs []table.Requirement, updates []table.Update) (table.Metadata, string, error) {
	ns := catalog.NamespaceFromIdent(ident)
	tblName := catalog.TableNameFromIdent(ident)

	current, err := c.LoadTable(ctx, ident)
	if err != nil && !errors.Is(err, catalog.ErrNoSuchTable) {
		return nil, "", err
	}

	staged, err := internal.UpdateAndStageTable(ctx, current, ident, reqs, updates, c)
	if err != nil {
		return nil, "", err
	}

	if current != nil && staged.Metadata().Equals(current.Metadata()) {
		// no changes, do nothing
		return current.Metadata(), current.MetadataLocation(), nil
	}

	if err := internal.WriteMetadata(ctx, staged.Metadata(), staged.MetadataLocation(), staged.Properties()); err != nil {
		return nil, "", err
	}

	err = withWriteTx(ctx, c.db, func(ctx context.Context, tx bun.Tx) error {
		if current != nil {
			res, err := tx.NewUpdate().Model(&sqlIcebergTable{
				CatalogName:              c.name,
				TableNamespace:           strings.Join(ns, "."),
				TableName:                tblName,
				IcebergType:              TableType,
				MetadataLocation:         sql.NullString{Valid: true, String: staged.MetadataLocation()},
				PreviousMetadataLocation: sql.NullString{Valid: true, String: current.MetadataLocation()},
			}).WherePK().Where("metadata_location = ?", current.MetadataLocation()).
				Where("iceberg_type = ?", TableType).
				Exec(ctx)
			if err != nil {
				return fmt.Errorf("error updating table information: %w", err)
			}

			n, err := res.RowsAffected()
			if err != nil {
				return fmt.Errorf("error updating table information: %w", err)
			}

			if n == 0 {
				return fmt.Errorf("table has been updated by another process: %s.%s", strings.Join(ns, "."), tblName)
			}

			return nil
		}

		_, err := tx.NewInsert().Model(&sqlIcebergTable{
			CatalogName:      c.name,
			TableNamespace:   strings.Join(ns, "."),
			TableName:        tblName,
			IcebergType:      TableType,
			MetadataLocation: sql.NullString{Valid: true, String: staged.MetadataLocation()},
		}).Exec(ctx)
		if err != nil {
			return fmt.Errorf("failed to create table: %w", err)
		}

		return nil
	})
	if err != nil {
		return nil, "", err
	}

	return staged.Metadata(), staged.MetadataLocation(), nil
}

func (c *Catalog) LoadTable(ctx context.Context, identifier table.Identifier) (*table.Table, error) {
	ns := catalog.NamespaceFromIdent(identifier)
	tbl := catalog.TableNameFromIdent(identifier)

	result, err := withReadTx(ctx, c.db, func(ctx context.Context, tx bun.Tx) (*sqlIcebergTable, error) {
		t := new(sqlIcebergTable)
		err := tx.NewSelect().Model(t).
			Where("catalog_name = ?", c.name).
			Where("table_namespace = ?", strings.Join(ns, ".")).
			Where("table_name = ?", tbl).
			Scan(ctx)
		if errors.Is(err, sql.ErrNoRows) {
			return nil, fmt.Errorf("%w: %s", catalog.ErrNoSuchTable, identifier)
		}

		if err != nil {
			return nil, fmt.Errorf("error encountered loading table %s: %w", identifier, err)
		}

		return t, nil
	})
	if err != nil {
		return nil, err
	}

	if !result.MetadataLocation.Valid {
		return nil, fmt.Errorf("%w: %s, metadata location is missing", catalog.ErrNoSuchTable, identifier)
	}

	return table.NewFromLocation(
		ctx,
		identifier,
		result.MetadataLocation.String,
		io.LoadFSFunc(c.props, result.MetadataLocation.String),
		c,
	)
}

func (c *Catalog) DropTable(ctx context.Context, identifier table.Identifier) error {
	ns := strings.Join(catalog.NamespaceFromIdent(identifier), ".")
	tbl := catalog.TableNameFromIdent(identifier)

	return withWriteTx(ctx, c.db, func(ctx context.Context, tx bun.Tx) error {
		res, err := tx.NewDelete().Model(&sqlIcebergTable{
			CatalogName:    c.name,
			TableNamespace: ns,
			TableName:      tbl,
		}).WherePK().Where("iceberg_type = ?", TableType).Exec(ctx)
		if err != nil {
			return fmt.Errorf("failed to delete table entry: %w", err)
		}

		n, err := res.RowsAffected()
		if err != nil {
			return fmt.Errorf("error encountered when deleting table entry: %w", err)
		}

		if n == 0 {
			return fmt.Errorf("%w: %s", catalog.ErrNoSuchTable, identifier)
		}

		return nil
	})
}

func (c *Catalog) RenameTable(ctx context.Context, from, to table.Identifier) (*table.Table, error) {
	fromNs := strings.Join(catalog.NamespaceFromIdent(from), ".")
	fromTbl := catalog.TableNameFromIdent(from)

	toNs := strings.Join(catalog.NamespaceFromIdent(to), ".")
	toTbl := catalog.TableNameFromIdent(to)

	exists, err := c.namespaceExists(ctx, toNs)
	if err != nil {
		return nil, err
	}
	if !exists {
		return nil, fmt.Errorf("%w: %s", catalog.ErrNoSuchNamespace, toNs)
	}

	err = withWriteTx(ctx, c.db, func(ctx context.Context, tx bun.Tx) error {
		exists, err := tx.NewSelect().Model(&sqlIcebergTable{
			CatalogName:    c.name,
			TableNamespace: toNs,
			TableName:      toTbl,
		}).WherePK().Exists(ctx)
		if err != nil {
			return fmt.Errorf("error encountered checking existence of table '%s': %w", to, err)
		}

		if exists {
			return catalog.ErrTableAlreadyExists
		}

		res, err := tx.NewUpdate().Model(&sqlIcebergTable{
			CatalogName:    c.name,
			TableNamespace: fromNs,
			TableName:      fromTbl,
		}).WherePK().Where("iceberg_type = ?", TableType).
			Set("table_namespace = ?", toNs).
			Set("table_name = ?", toTbl).
			Exec(ctx)
		if err != nil {
			return fmt.Errorf("error renaming table from '%s' to %s': %w", from, to, err)
		}

		n, err := res.RowsAffected()
		if err != nil {
			return fmt.Errorf("error renaming table from '%s' to %s': %w", from, to, err)
		}

		if n == 0 {
			return fmt.Errorf("%w: %s", catalog.ErrNoSuchTable, from)
		}

		return nil
	})
	if err != nil {
		return nil, err
	}

	return c.LoadTable(ctx, to)
}

func (c *Catalog) CheckTableExists(ctx context.Context, identifier table.Identifier) (bool, error) {
	_, err := c.LoadTable(ctx, identifier)
	if err != nil {
		if errors.Is(err, catalog.ErrNoSuchTable) {
			return false, nil
		}

		return false, err
	}

	return true, nil
}

func (c *Catalog) CreateNamespace(ctx context.Context, namespace table.Identifier, props iceberg.Properties) error {
	if err := checkValidNamespace(namespace); err != nil {
		return err
	}

	exists, err := c.namespaceExists(ctx, strings.Join(namespace, "."))
	if err != nil {
		return err
	}

	if exists {
		return fmt.Errorf("%w: %s", catalog.ErrNamespaceAlreadyExists, strings.Join(namespace, "."))
	}

	if len(props) == 0 {
		props = minimalNamespaceProps
	}

	nsToCreate := strings.Join(namespace, ".")

	return withWriteTx(ctx, c.db, func(ctx context.Context, tx bun.Tx) error {
		toInsert := make([]sqlIcebergNamespaceProps, 0, len(props))
		for k, v := range props {
			toInsert = append(toInsert, sqlIcebergNamespaceProps{
				CatalogName:   c.name,
				Namespace:     nsToCreate,
				PropertyKey:   k,
				PropertyValue: sql.NullString{String: v, Valid: true},
			})
		}

		_, err := tx.NewInsert().Model(&toInsert).Exec(ctx)
		if err != nil {
			return fmt.Errorf("error inserting namespace properties for namespace '%s': %w", namespace, err)
		}

		return nil
	})
}

func (c *Catalog) DropNamespace(ctx context.Context, namespace table.Identifier) error {
	if err := checkValidNamespace(namespace); err != nil {
		return err
	}

	nsToDelete := strings.Join(namespace, ".")

	exists, err := c.namespaceExists(ctx, nsToDelete)
	if err != nil {
		return err
	}

	if !exists {
		return fmt.Errorf("%w: %s", catalog.ErrNoSuchNamespace, nsToDelete)
	}

	tbls := make([]table.Identifier, 0)
	iter := c.ListTables(ctx, namespace)

	for tbl, err := range iter {
		tbls = append(tbls, tbl)
		if err != nil {
			return err
		}

		break // there is already at least a table
	}

	if len(tbls) > 0 {
		return fmt.Errorf("%w: %d tables exist in namespace %s", catalog.ErrNamespaceNotEmpty, len(tbls), nsToDelete)
	}

	return withWriteTx(ctx, c.db, func(ctx context.Context, tx bun.Tx) error {
		_, err := tx.NewDelete().Model((*sqlIcebergNamespaceProps)(nil)).
			Where("catalog_name = ?", c.name).
			Where("namespace = ?", nsToDelete).Exec(ctx)
		if err != nil {
			return fmt.Errorf("error deleting namespace '%s': %w", namespace, err)
		}

		return nil
	})
}

func (c *Catalog) LoadNamespaceProperties(ctx context.Context, namespace table.Identifier) (iceberg.Properties, error) {
	if err := checkValidNamespace(namespace); err != nil {
		return nil, err
	}

	nsToLoad := strings.Join(namespace, ".")
	exists, err := c.namespaceExists(ctx, nsToLoad)
	if err != nil {
		return nil, err
	}

	if !exists {
		return nil, fmt.Errorf("%w: %s", catalog.ErrNoSuchNamespace, nsToLoad)
	}

	return withReadTx(ctx, c.db, func(ctx context.Context, tx bun.Tx) (iceberg.Properties, error) {
		var props []sqlIcebergNamespaceProps
		err := tx.NewSelect().Model(&props).
			Where("catalog_name = ?", c.name).
			Where("namespace = ?", nsToLoad).Scan(ctx)
		if err != nil {
			return nil, fmt.Errorf("error loading namespace properties for '%s': %w", namespace, err)
		}

		result := make(iceberg.Properties)
		for _, p := range props {
			result[p.PropertyKey] = p.PropertyValue.String
		}

		return result, nil
	})
}

func (c *Catalog) ListTables(ctx context.Context, namespace table.Identifier) iter.Seq2[table.Identifier, error] {
	tables, err := c.listTablesAll(ctx, namespace)
	if err != nil {
		return func(yield func(table.Identifier, error) bool) {
			yield(table.Identifier{}, err)
		}
	}

	return func(yield func(table.Identifier, error) bool) {
		for _, t := range tables {
			if !yield(t, nil) {
				return
			}
		}
	}
}

func (c *Catalog) listTablesAll(ctx context.Context, namespace table.Identifier) ([]table.Identifier, error) {
	if len(namespace) > 0 {
		exists, err := c.namespaceExists(ctx, strings.Join(namespace, "."))
		if err != nil {
			return nil, err
		}
		if !exists {
			return nil, fmt.Errorf("%w: %s", catalog.ErrNoSuchNamespace, strings.Join(namespace, "."))
		}
	}

	ns := strings.Join(namespace, ".")
	tables, err := withReadTx(ctx, c.db, func(ctx context.Context, tx bun.Tx) ([]sqlIcebergTable, error) {
		var tables []sqlIcebergTable
		err := tx.NewSelect().Model(&tables).
			Where("catalog_name = ?", c.name).
			Where("table_namespace = ?", ns).
			Where("iceberg_type = ?", TableType).
			Scan(ctx)

		return tables, err
	})
	if err != nil {
		return nil, fmt.Errorf("error listing tables for namespace '%s': %w", namespace, err)
	}

	ret := make([]table.Identifier, len(tables))
	for i, t := range tables {
		ret[i] = append(strings.Split(t.TableNamespace, "."), t.TableName)
	}

	return ret, nil
}

func (c *Catalog) ListNamespaces(ctx context.Context, parent table.Identifier) ([]table.Identifier, error) {
	tableQuery := c.db.NewSelect().Model((*sqlIcebergTable)(nil)).
		Column("table_namespace").Where("catalog_name = ?", c.name)
	nsQuery := c.db.NewSelect().Model((*sqlIcebergNamespaceProps)(nil)).
		Column("namespace").Where("catalog_name = ?", c.name)

	if len(parent) > 0 {
		ns := strings.Join(parent, ".")
		exists, err := c.namespaceExists(ctx, ns)
		if err != nil {
			return nil, err
		}
		if !exists {
			return nil, fmt.Errorf("%w: %s", catalog.ErrNoSuchNamespace, strings.Join(parent, "."))
		}

		ns += "%"
		tableQuery = tableQuery.Where("table_namespace like ?", ns)
		nsQuery = nsQuery.Where("namespace like ?", ns)
	}

	namespaces, err := withReadTx(ctx, c.db, func(ctx context.Context, tx bun.Tx) ([]string, error) {
		var namespaces []string

		rows, err := tx.QueryContext(ctx, tableQuery.String()+" UNION "+nsQuery.String())
		if err != nil {
			return nil, fmt.Errorf("error listing namespaces for '%s': %w", parent, err)
		}

		err = c.db.ScanRows(ctx, rows, &namespaces)

		return namespaces, err
	})
	if err != nil {
		return nil, err
	}

	ret := make([]table.Identifier, len(namespaces))
	for i, n := range namespaces {
		ret[i] = strings.Split(n, ".")
	}

	return ret, nil
}

// avoid circular dependency while still avoiding having to export the getUpdatedPropsAndUpdateSummary function
// so that we can re-use it in the catalog implementations without duplicating the code.

//go:linkname getUpdatedPropsAndUpdateSummary github.com/apache/iceberg-go/catalog.getUpdatedPropsAndUpdateSummary
func getUpdatedPropsAndUpdateSummary(currentProps iceberg.Properties, removals []string, updates iceberg.Properties) (iceberg.Properties, catalog.PropertiesUpdateSummary, error)

func (c *Catalog) UpdateNamespaceProperties(ctx context.Context, namespace table.Identifier, removals []string, updates iceberg.Properties) (catalog.PropertiesUpdateSummary, error) {
	var summary catalog.PropertiesUpdateSummary
	currentProps, err := c.LoadNamespaceProperties(ctx, namespace)
	if err != nil {
		return summary, err
	}

	_, summary, err = getUpdatedPropsAndUpdateSummary(currentProps, removals, updates)
	if err != nil {
		return summary, err
	}

	nsToUpdate := strings.Join(namespace, ".")

	return summary, withWriteTx(ctx, c.db, func(ctx context.Context, tx bun.Tx) error {
		var m *sqlIcebergNamespaceProps
		if len(removals) > 0 {
			_, err := tx.NewDelete().Model(m).
				Where("catalog_name = ?", c.name).
				Where("namespace = ?", nsToUpdate).
				Where("property_key in (?)", bun.In(removals)).Exec(ctx)
			if err != nil {
				return fmt.Errorf("error deleting properties for '%s': %w", namespace, err)
			}
		}

		if len(updates) > 0 {
			props := make([]sqlIcebergNamespaceProps, 0, len(updates))
			for k, v := range updates {
				props = append(props, sqlIcebergNamespaceProps{
					CatalogName:   c.name,
					Namespace:     nsToUpdate,
					PropertyKey:   k,
					PropertyValue: sql.NullString{String: v, Valid: true},
				})
			}

			q := tx.NewInsert().Model(&props)
			switch {
			case c.db.HasFeature(feature.InsertOnConflict):
				q = q.On("CONFLICT (catalog_name, namespace, property_key) DO UPDATE").
					Set("property_value = EXCLUDED.property_value")
			case c.db.HasFeature(feature.InsertOnDuplicateKey):
				q = q.On("DUPLICATE KEY UPDATE")
			default:
				_, err := tx.NewDelete().Model(m).
					Where("catalog_name = ?", c.name).
					Where("namespace = ?", nsToUpdate).
					Where("property_key in (?)", bun.In(slices.Collect(maps.Keys(updates)))).
					Exec(ctx)
				if err != nil {
					return fmt.Errorf("error deleting properties for '%s': %w", namespace, err)
				}
			}

			_, err := q.Exec(ctx)
			if err != nil {
				return fmt.Errorf("error updating namespace properties for '%s': %w", namespace, err)
			}
		}

		return nil
	})
}

func (c *Catalog) CheckNamespaceExists(ctx context.Context, namespace table.Identifier) (bool, error) {
	return c.namespaceExists(ctx, strings.Join(namespace, "."))
}

// CreateView creates a new view in the catalog.
func (c *Catalog) CreateView(ctx context.Context, identifier table.Identifier, schema *iceberg.Schema, viewSQL string, props iceberg.Properties) error {
	nsIdent := catalog.NamespaceFromIdent(identifier)
	viewIdent := catalog.TableNameFromIdent(identifier)
	ns := strings.Join(nsIdent, ".")

	exists, err := c.namespaceExists(ctx, ns)
	if err != nil {
		return err
	}
	if !exists {
		return fmt.Errorf("%w: %s", catalog.ErrNoSuchNamespace, ns)
	}

	exists, err = c.CheckViewExists(ctx, identifier)
	if err != nil {
		return err
	}
	if exists {
		return fmt.Errorf("%w: %s", catalog.ErrViewAlreadyExists, identifier)
	}

	loc, err := internal.ResolveTableLocation(ctx, "", ns, viewIdent, c.props, c.LoadNamespaceProperties)
	if err != nil {
		return err
	}

	metadataLocation, err := internal.CreateViewMetadata(ctx, c.name, nsIdent, schema, viewSQL, loc, props)
	if err != nil {
		return err
	}
	err = withWriteTx(ctx, c.db, func(ctx context.Context, tx bun.Tx) error {
		_, err := tx.NewInsert().Model(&sqlIcebergTable{
			CatalogName:      c.name,
			TableNamespace:   ns,
			TableName:        viewIdent,
			IcebergType:      ViewType,
			MetadataLocation: sql.NullString{String: metadataLocation, Valid: true},
		}).Exec(ctx)
		if err != nil {
			return fmt.Errorf("failed to create view: %w", err)
		}

		return nil
	})

	return err
}

// ListViews returns a list of view identifiers in the catalog.
func (c *Catalog) ListViews(ctx context.Context, namespace table.Identifier) iter.Seq2[table.Identifier, error] {
	views, err := c.listViewsAll(ctx, namespace)
	if err != nil {
		return func(yield func(table.Identifier, error) bool) {
			yield(table.Identifier{}, err)
		}
	}

	return func(yield func(table.Identifier, error) bool) {
		for _, v := range views {
			if !yield(v, nil) {
				return
			}
		}
	}
}

func (c *Catalog) listViewsAll(ctx context.Context, namespace table.Identifier) ([]table.Identifier, error) {
	if len(namespace) > 0 {
		exists, err := c.namespaceExists(ctx, strings.Join(namespace, "."))
		if err != nil {
			return nil, err
		}
		if !exists {
			return nil, fmt.Errorf("%w: %s", catalog.ErrNoSuchNamespace, strings.Join(namespace, "."))
		}
	}

	ns := strings.Join(namespace, ".")
	views, err := withReadTx(ctx, c.db, func(ctx context.Context, tx bun.Tx) ([]sqlIcebergTable, error) {
		var views []sqlIcebergTable
		err := tx.NewSelect().Model(&views).
			Where("catalog_name = ?", c.name).
			Where("table_namespace = ?", ns).
			Where("iceberg_type = ?", ViewType).
			Scan(ctx)

		return views, err
	})
	if err != nil {
		return nil, fmt.Errorf("error listing views for namespace '%s': %w", namespace, err)
	}

	ret := make([]table.Identifier, len(views))
	for i, v := range views {
		ret[i] = append(strings.Split(v.TableNamespace, "."), v.TableName)
	}

	return ret, nil
}

// DropView deletes a view from the catalog.
func (c *Catalog) DropView(ctx context.Context, identifier table.Identifier) error {
	ns := strings.Join(catalog.NamespaceFromIdent(identifier), ".")
	viewName := catalog.TableNameFromIdent(identifier)

	metadataLocation := ""
	view, err := withReadTx(ctx, c.db, func(ctx context.Context, tx bun.Tx) (*sqlIcebergTable, error) {
		v := new(sqlIcebergTable)
		err := tx.NewSelect().Model(v).
			Where("catalog_name = ?", c.name).
			Where("table_namespace = ?", ns).
			Where("table_name = ?", viewName).
			Where("iceberg_type = ?", ViewType).
			Scan(ctx)
		if errors.Is(err, sql.ErrNoRows) {
			return nil, fmt.Errorf("%w: %s", catalog.ErrNoSuchView, identifier)
		}
		if err != nil {
			return nil, fmt.Errorf("error encountered loading view %s: %w", identifier, err)
		}

		return v, nil
	})
	if err != nil {
		return err
	}

	if view.MetadataLocation.Valid {
		metadataLocation = view.MetadataLocation.String
	}

	err = withWriteTx(ctx, c.db, func(ctx context.Context, tx bun.Tx) error {
		res, err := tx.NewDelete().Model(&sqlIcebergTable{
			CatalogName:    c.name,
			TableNamespace: ns,
			TableName:      viewName,
		}).WherePK().Where("iceberg_type = ?", ViewType).Exec(ctx)
		if err != nil {
			return fmt.Errorf("failed to delete view entry: %w", err)
		}

		n, err := res.RowsAffected()
		if err != nil {
			return fmt.Errorf("error encountered when deleting view entry: %w", err)
		}

		if n == 0 {
			return fmt.Errorf("%w: %s", catalog.ErrNoSuchView, identifier)
		}

		return nil
	})
	if err != nil {
		return err
	}

	if metadataLocation != "" {
		fs, err := io.LoadFS(ctx, c.props, metadataLocation)
		if err != nil {
			return nil
		}

		_ = fs.Remove(metadataLocation)
	}

	return nil
}

// CheckViewExists returns true if a view exists in the catalog.
func (c *Catalog) CheckViewExists(ctx context.Context, identifier table.Identifier) (bool, error) {
	ns := strings.Join(catalog.NamespaceFromIdent(identifier), ".")
	viewName := catalog.TableNameFromIdent(identifier)

	return withReadTx(ctx, c.db, func(ctx context.Context, tx bun.Tx) (bool, error) {
		exists, err := tx.NewSelect().Model(&sqlIcebergTable{
			CatalogName:    c.name,
			TableNamespace: ns,
			TableName:      viewName,
		}).WherePK().Where("iceberg_type = ?", ViewType).Exists(ctx)
		if err != nil {
			return false, fmt.Errorf("error checking view existence: %w", err)
		}

		return exists, nil
	})
}

// LoadView loads a view from the catalog.
func (c *Catalog) LoadView(ctx context.Context, identifier table.Identifier) (map[string]interface{}, error) {
	ns := strings.Join(catalog.NamespaceFromIdent(identifier), ".")
	viewName := catalog.TableNameFromIdent(identifier)

	view, err := withReadTx(ctx, c.db, func(ctx context.Context, tx bun.Tx) (*sqlIcebergTable, error) {
		v := new(sqlIcebergTable)
		err := tx.NewSelect().Model(v).
			Where("catalog_name = ?", c.name).
			Where("table_namespace = ?", ns).
			Where("table_name = ?", viewName).
			Where("iceberg_type = ?", ViewType).
			Scan(ctx)
		if errors.Is(err, sql.ErrNoRows) {
			return nil, fmt.Errorf("%w: %s", catalog.ErrNoSuchView, identifier)
		}
		if err != nil {
			return nil, fmt.Errorf("error encountered loading view %s: %w", identifier, err)
		}

		return v, nil
	})
	if err != nil {
		return nil, err
	}

	if !view.MetadataLocation.Valid {
		return nil, fmt.Errorf("%w: %s, metadata location is missing", catalog.ErrNoSuchView, identifier)
	}

	viewMetadata, err := internal.LoadViewMetadata(ctx, c.props, view.MetadataLocation.String, viewName, ns)
	if err != nil {
		return nil, err
	}

	return viewMetadata, nil
}
