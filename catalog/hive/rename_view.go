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
	"log"

	"github.com/apache/iceberg-go/catalog"
	"github.com/apache/iceberg-go/table"
	"github.com/apache/iceberg-go/view"
	"github.com/beltran/gohive/hive_metastore"
)

var _ catalog.RenamableViewCatalog = (*Catalog)(nil)

// RenameView changes a view's identifier, moving it to another database if
// needed, without moving its metadata. As in Java's HiveCatalog, renaming onto
// any existing object is rejected, including renaming a view to itself.
func (c *Catalog) RenameView(ctx context.Context, from, to table.Identifier) (_ *view.View, err error) {
	fromDB, fromView, err := identifierToViewName(from)
	if err != nil {
		return nil, err
	}

	toDB, toView, err := identifierToViewName(to)
	if err != nil {
		return nil, err
	}

	exists, err := c.CheckNamespaceExists(ctx, DatabaseIdentifier(toDB))
	if err != nil {
		return nil, err
	}
	if !exists {
		return nil, fmt.Errorf("%w: %s", catalog.ErrNoSuchNamespace, toDB)
	}

	source, err := c.getIcebergView(ctx, fromDB, fromView)
	if err != nil {
		return nil, err
	}
	sourceMetadataLocation, err := getViewMetadataLocation(source)
	if err != nil {
		return nil, err
	}

	lockIdentifiers := []tableLockIdentifier{
		{database: fromDB, table: fromView},
		{database: toDB, table: toView},
	}
	lock, err := acquireLocks(ctx, c.client, lockIdentifiers, c.opts)
	if err != nil {
		return nil, fmt.Errorf("%w: failed to acquire locks for rename %s.%s to %s.%s: %w",
			table.ErrCommitFailed, fromDB, fromView, toDB, toView, err)
	}
	defer func() {
		if releaseErr := lock.releaseForCleanup(ctx); releaseErr != nil {
			if err != nil {
				err = errors.Join(err, releaseErr)
			} else {
				log.Printf("WARNING: failed to release Hive lock after renaming view %s.%s to %s.%s: %v", fromDB, fromView, toDB, toView, releaseErr)
			}
		}
	}()

	hiveTbl, err := c.getIcebergView(ctx, fromDB, fromView)
	if err != nil {
		if errors.Is(err, catalog.ErrNoSuchView) {
			return nil, fmt.Errorf("%w: source view %s.%s changed during rename: %w",
				table.ErrCommitFailed, fromDB, fromView, err)
		}

		return nil, err
	}
	lockedMetadataLocation, err := getViewMetadataLocation(hiveTbl)
	if err != nil {
		return nil, err
	}
	if lockedMetadataLocation != sourceMetadataLocation {
		return nil, fmt.Errorf("%w: source view %s.%s metadata location changed from %s to %s",
			table.ErrCommitFailed, fromDB, fromView, sourceMetadataLocation, lockedMetadataLocation)
	}

	// Use a raw GetTable so that any object at the destination, Iceberg or
	// not, is reported as a conflict.
	destination, err := c.client.GetTable(ctx, toDB, toView)
	if err == nil {
		if isIcebergView(destination) {
			return nil, fmt.Errorf("%w: %s.%s", catalog.ErrViewAlreadyExists, toDB, toView)
		}

		return nil, fmt.Errorf("%w: %s.%s", catalog.ErrTableAlreadyExists, toDB, toView)
	}
	if !isNoSuchObjectError(err) {
		return nil, fmt.Errorf("failed to check destination %s.%s: %w", toDB, toView, err)
	}

	hiveTbl.TableName = toView
	hiveTbl.DbName = toDB

	// The pre-check above is best-effort against writers that skip these HMS
	// locks; the metastore then rejects the rename, as in RenameTable.
	if err := c.client.AlterTable(ctx, fromDB, fromView, hiveTbl); err != nil {
		if isAlreadyExistsError(err) {
			return nil, fmt.Errorf("%w: %s.%s: %w", catalog.ErrViewAlreadyExists, toDB, toView, err)
		}

		return nil, fmt.Errorf("failed to rename view %s.%s to %s.%s: %w", fromDB, fromView, toDB, toView, err)
	}

	return c.LoadView(ctx, to)
}

// getIcebergView retrieves a metastore object and checks it's an Iceberg view.
func (c *Catalog) getIcebergView(ctx context.Context, database, viewName string) (*hive_metastore.Table, error) {
	hiveTbl, err := c.client.GetTable(ctx, database, viewName)
	if err != nil {
		if isNoSuchObjectError(err) {
			return nil, fmt.Errorf("%w: %s.%s", catalog.ErrNoSuchView, database, viewName)
		}

		return nil, fmt.Errorf("failed to get view %s.%s: %w", database, viewName, err)
	}

	if !isIcebergView(hiveTbl) {
		return nil, fmt.Errorf("%w: %s.%s", catalog.ErrNoSuchView, database, viewName)
	}

	return hiveTbl, nil
}
