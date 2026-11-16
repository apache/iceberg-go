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
	"errors"
	"fmt"
	"maps"

	"github.com/apache/iceberg-go/catalog"
	icebergio "github.com/apache/iceberg-go/io"
	"github.com/apache/iceberg-go/table"
	"github.com/apache/iceberg-go/view"
	"github.com/uptrace/bun"
)

var _ catalog.UpdatableViewCatalog = (*Catalog)(nil)

// UpdateView applies view metadata updates using an atomic metadata-location swap.
func (c *Catalog) UpdateView(ctx context.Context, ident table.Identifier, requirements []view.Requirement, updates []view.Update) (*view.View, error) {
	current, err := c.LoadView(ctx, ident)
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

	namespace, err := c.namespaceKey(ctx, catalog.NamespaceFromIdent(ident))
	if err != nil {
		return nil, err
	}
	ioProps := maps.Clone(c.props)
	if ioProps == nil {
		ioProps = make(map[string]string)
	}
	maps.Copy(ioProps, metadata.Properties())
	metadataLocation, fs, err := view.WriteMetadata(ctx, metadata.Metadata, ioProps)
	if err != nil {
		return nil, err
	}

	err = withWriteTx(ctx, c.db, func(ctx context.Context, tx bun.Tx) error {
		result, err := tx.NewUpdate().Model((*sqlIcebergTable)(nil)).
			Set("metadata_location = ?", metadataLocation).
			Set("previous_metadata_location = ?", current.MetadataLocation()).
			Where("catalog_name = ?", c.name).
			Where("table_namespace = ?", namespace).
			Where("table_name = ?", catalog.ObjectNameFromIdent(ident)).
			Where("iceberg_type = ?", ViewType).
			Where("metadata_location = ?", current.MetadataLocation()).Exec(ctx)
		if err != nil {
			return fmt.Errorf("failed to update view: %w", err)
		}
		affected, err := result.RowsAffected()
		if err != nil {
			return err
		}
		if affected == 0 {
			return fmt.Errorf("%w: view %s metadata location changed", table.ErrCommitFailed, ident)
		}

		return nil
	})
	if err != nil {
		// A failed comparison proves that this file was not committed. Other
		// SQL failures may have an unknown outcome, so retain the metadata.
		if errors.Is(err, table.ErrCommitFailed) {
			err = errors.Join(err, removeUncommittedMetadata(ctx, metadataLocation, func(context.Context) (icebergio.IO, error) { return fs, nil }))
		}

		return nil, err
	}

	return view.New(ident, metadata.Metadata, metadataLocation), nil
}
