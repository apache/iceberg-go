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
	"maps"

	"github.com/apache/iceberg-go/catalog"
	icebergio "github.com/apache/iceberg-go/io"
	"github.com/apache/iceberg-go/table"
	"github.com/apache/iceberg-go/view"
)

var _ catalog.UpdatableViewCatalog = (*Catalog)(nil)

// UpdateView commits view metadata while holding the metastore's exclusive lock.
func (c *Catalog) UpdateView(ctx context.Context, ident table.Identifier, requirements []view.Requirement, updates []view.Update) (_ *view.View, err error) {
	database, name, err := identifierToViewName(ident)
	if err != nil {
		return nil, err
	}
	lock, err := acquireLock(ctx, c.client, database, name, c.opts)
	if err != nil {
		return nil, fmt.Errorf("%w: %w", table.ErrCommitFailed, err)
	}
	defer func() {
		if releaseErr := lock.releaseForCleanup(ctx); releaseErr != nil {
			if err != nil {
				err = errors.Join(err, releaseErr)
			} else {
				log.Printf("WARNING: failed to release Hive lock after updating view %s.%s: %v", database, name, releaseErr)
			}
		}
	}()

	provider, ok := c.client.(LockClientProvider)
	if !ok {
		return nil, errors.New("view updates require a separate Hive lock client")
	}
	lockClient, err := provider.NewLockClient(ctx)
	if err != nil {
		return nil, err
	}
	lease := newViewLockLease(ctx, lockClient, lock.lockId, c.opts.viewLockHeartbeatInterval())
	defer func() {
		lease.stop()
		if closeErr := lockClient.Close(); closeErr != nil {
			if err != nil {
				err = errors.Join(err, closeErr)
			} else {
				log.Printf("WARNING: failed to close Hive lock client: %v", closeErr)
			}
		}
	}()
	if err := lease.check(ctx); err != nil {
		return nil, fmt.Errorf("%w: %w", table.ErrCommitFailed, err)
	}

	hiveView, err := c.client.GetTable(ctx, database, name)
	if err != nil {
		if isNoSuchObjectError(err) {
			return nil, fmt.Errorf("%w: %s", catalog.ErrNoSuchView, ident)
		}

		return nil, err
	}
	if !isIcebergView(hiveView) {
		return nil, fmt.Errorf("%w: %s", catalog.ErrNoSuchView, ident)
	}
	oldLocation, err := getViewMetadataLocation(hiveView)
	if err != nil {
		return nil, err
	}
	current, err := view.NewFromLocation(ctx, ident, oldLocation, icebergio.LoadFSFunc(c.opts.props, oldLocation))
	if err != nil {
		return nil, err
	}
	for _, requirement := range requirements {
		if err := requirement.Validate(current.Metadata()); err != nil {
			return nil, fmt.Errorf("%w: %w", table.ErrCommitFailed, err)
		}
	}
	builder, err := view.MetadataBuilderFromBase(current.Metadata())
	if err != nil {
		return nil, err
	}
	for _, update := range updates {
		if err := update.Apply(builder); err != nil {
			return nil, err
		}
	}
	metadata, err := builder.Build()
	if err != nil {
		return nil, err
	}
	if metadata.Equals(current.Metadata()) {
		return current, nil
	}
	representation, err := sqlFromVersion(metadata.CurrentVersion())
	if err != nil {
		return nil, err
	}
	ioProps := maps.Clone(c.opts.props)
	if ioProps == nil {
		ioProps = make(map[string]string)
	}
	maps.Copy(ioProps, metadata.Properties())
	metadataLocation, fs, err := view.WriteMetadata(ctx, metadata.Metadata, ioProps)
	if err != nil {
		return nil, err
	}

	updated := *hiveView
	updated.Parameters = maps.Clone(hiveView.Parameters)
	for key := range current.Properties() {
		delete(updated.Parameters, key)
	}
	maps.Copy(updated.Parameters, metadata.Properties())
	updated.Parameters[TableTypeKey] = TableTypeIcebergView
	updated.Parameters[MetadataLocationKey] = metadataLocation
	updated.Parameters[PreviousMetadataLocationKey] = oldLocation
	updated.Parameters[ExternalKey] = "TRUE"
	updated.Sd = constructHiveViewTable(database, name, metadata.Location(), metadataLocation,
		metadata.CurrentSchema(), representation.SQL, nil).Sd
	updated.ViewOriginalText = representation.SQL
	updated.ViewExpandedText = representation.SQL
	if err := lease.check(ctx); err != nil {
		cleanupErr := fs.Remove(context.WithoutCancel(ctx), metadataLocation)

		return nil, errors.Join(fmt.Errorf("%w: %w", table.ErrCommitFailed, err), cleanupErr)
	}
	if err := c.client.AlterTable(ctx, database, name, &updated); err != nil {
		// The metastore may have applied the change before the response failed.
		// Keep the metadata file so a successful commit remains readable.
		return nil, fmt.Errorf("failed to update view %s.%s: %w", database, name, err)
	}

	if err := lease.check(ctx); err != nil {
		return nil, fmt.Errorf("view update may have committed after Hive lock loss: %w", err)
	}

	return view.New(ident, metadata.Metadata, metadataLocation), nil
}
