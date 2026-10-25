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
	"fmt"
	"iter"
	"strings"

	"github.com/apache/iceberg-go/catalog"
	"github.com/apache/iceberg-go/table"
	"github.com/uptrace/bun"
)

// ListTables returns the table identifiers in a namespace.
func (c *Catalog) ListTables(ctx context.Context, namespace table.Identifier) iter.Seq2[table.Identifier, error] {
	tables, err := c.listTablesAll(ctx, namespace)

	return listedIdentifiers(tables, err)
}

func (c *Catalog) listTablesAll(ctx context.Context, namespace table.Identifier) ([]table.Identifier, error) {
	return c.listObjectsAll(ctx, namespace, TableType)
}

// ListViews returns a list of view identifiers in the catalog.
func (c *Catalog) ListViews(ctx context.Context, namespace table.Identifier) iter.Seq2[table.Identifier, error] {
	views, err := c.listViewsAll(ctx, namespace)

	return listedIdentifiers(views, err)
}

func (c *Catalog) listViewsAll(ctx context.Context, namespace table.Identifier) ([]table.Identifier, error) {
	return c.listObjectsAll(ctx, namespace, ViewType)
}

// listedIdentifiers converts an eager listing into the catalog's iterator API.
// Errors are yielded once; callers can also stop iteration early.
func listedIdentifiers(identifiers []table.Identifier, err error) iter.Seq2[table.Identifier, error] {
	return func(yield func(table.Identifier, error) bool) {
		if err != nil {
			yield(table.Identifier{}, err)

			return
		}

		for _, ident := range identifiers {
			if !yield(ident, nil) {
				return
			}
		}
	}
}

// listObjectsAll handles the shared namespace resolution and identifier decoding
// for tables and views while keeping their different schema-version filters.
func (c *Catalog) listObjectsAll(ctx context.Context, namespace table.Identifier, objectType string) ([]table.Identifier, error) {
	// Legacy V0 catalogs have no iceberg_type column and cannot contain views.
	// Keep their ListViews behavior independent of namespace existence.
	if c.isV0() && objectType == ViewType {
		return nil, nil
	}

	ns := namespaceToString(namespace)
	if len(namespace) > 0 {
		var exists bool
		var err error
		ns, exists, err = c.resolveNamespaceKey(ctx, namespace)
		if err != nil {
			return nil, err
		}
		if !exists {
			return nil, fmt.Errorf("%w: %s", catalog.ErrNoSuchNamespace, strings.Join(namespace, "."))
		}
	}

	objects, err := withReadTx(ctx, c.db, func(ctx context.Context, tx bun.Tx) ([]sqlIcebergTable, error) {
		var objects []sqlIcebergTable
		sel := tx.NewSelect().Model(&objects).
			Where("catalog_name = ?", c.name).
			Where("table_namespace = ?", ns)

		switch {
		case c.isV0():
			sel = sel.ExcludeColumn("iceberg_type")
		case objectType == ViewType:
			sel = sel.Where("iceberg_type = ?", ViewType)
		default:
			// V1 must still list tables from before the iceberg_type migration.
			sel = sel.Where("(iceberg_type = ? OR iceberg_type IS NULL)", TableType)
		}

		err := sel.Scan(ctx)

		return objects, err
	})
	if err != nil {
		kind := "tables"
		if objectType == ViewType {
			kind = "views"
		}

		return nil, fmt.Errorf("error listing %s for namespace '%s': %w", kind, namespace, err)
	}

	ret := make([]table.Identifier, len(objects))
	for i, obj := range objects {
		namespace, err := namespaceFromString(obj.TableNamespace)
		if err != nil {
			return nil, err
		}
		ret[i] = append(namespace, obj.TableName)
	}

	return ret, nil
}
