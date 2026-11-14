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
	"slices"

	"github.com/apache/iceberg-go/catalog"
	"github.com/apache/iceberg-go/table"
	"github.com/apache/iceberg-go/view"
	"github.com/uptrace/bun"
)

// RenameView atomically changes a view's identifier without moving its metadata.
func (c *Catalog) RenameView(ctx context.Context, from, to table.Identifier) (*view.View, error) {
	if c.isV0() {
		return nil, errViewsUnsupportedOnV0
	}
	if err := catalog.ValidateViewIdentifier(from); err != nil {
		return nil, err
	}
	if err := catalog.ValidateViewIdentifier(to); err != nil {
		return nil, err
	}
	if slices.Equal(from, to) {
		return c.LoadView(ctx, from)
	}
	fromNamespace, err := c.namespaceKey(ctx, catalog.NamespaceFromIdent(from))
	if err != nil {
		return nil, err
	}
	toNamespace, exists, err := c.resolveNamespaceKey(ctx, catalog.NamespaceFromIdent(to))
	if err != nil {
		return nil, err
	}
	if !exists {
		return nil, fmt.Errorf("%w: %s", catalog.ErrNoSuchNamespace, toNamespace)
	}
	fromName, toName := catalog.ObjectNameFromIdent(from), catalog.ObjectNameFromIdent(to)
	err = withSerializableWriteTx(ctx, c.db, func(ctx context.Context, tx bun.Tx) error {
		sourceExists, err := tx.NewSelect().Model((*sqlIcebergTable)(nil)).ColumnExpr("1").
			Where("catalog_name = ?", c.name).
			Where("table_namespace = ?", fromNamespace).
			Where("table_name = ?", fromName).
			Where("iceberg_type = ?", ViewType).Exists(ctx)
		if err != nil {
			return err
		}
		if !sourceExists {
			return fmt.Errorf("%w: %s", catalog.ErrNoSuchView, from)
		}
		exists, err := c.namespaceExistsInTx(ctx, tx, toNamespace)
		if err != nil {
			return err
		}
		if !exists {
			return fmt.Errorf("%w: %s", catalog.ErrNoSuchNamespace, toNamespace)
		}
		if err := c.checkIdentifierAvailable(ctx, tx, toNamespace, toName, to); err != nil {
			return err
		}
		result, err := tx.NewUpdate().Model((*sqlIcebergTable)(nil)).
			Set("table_namespace = ?", toNamespace).
			Set("table_name = ?", toName).
			Where("catalog_name = ?", c.name).
			Where("table_namespace = ?", fromNamespace).
			Where("table_name = ?", fromName).
			Where("iceberg_type = ?", ViewType).Exec(ctx)
		if err != nil {
			return fmt.Errorf("failed to rename view: %w", err)
		}
		affected, err := result.RowsAffected()
		if err != nil {
			return err
		}
		if affected == 0 {
			return fmt.Errorf("%w: %s", catalog.ErrNoSuchView, from)
		}

		return nil
	})
	if err != nil {
		return nil, err
	}

	return c.LoadView(ctx, to)
}
