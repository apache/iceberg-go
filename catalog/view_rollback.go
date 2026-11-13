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
	"fmt"

	"github.com/apache/iceberg-go"
	"github.com/apache/iceberg-go/table"
	"github.com/apache/iceberg-go/view"
)

// RollbackView makes a retained version current through an updatable catalog.
// It preserves version IDs and schemas and records the rollback in version
// history. The view UUID requirement prevents changing a dropped/recreated view.
// Missing or expired versions are rejected. Rolling back can also fail if the
// target removes a SQL dialect and replace.drop-dialect.allowed is false.
func RollbackView(ctx context.Context, cat Catalog, ident table.Identifier, versionID int64) (*view.View, error) {
	views, canLoad := As[ViewCatalog](cat)
	updater, canUpdate := As[UpdatableViewCatalog](cat)
	if !canLoad || !canUpdate {
		return nil, fmt.Errorf("%w: catalog does not support view rollback", iceberg.ErrNotImplemented)
	}
	current, err := views.LoadView(ctx, ident)
	if err != nil {
		return nil, err
	}
	if current.VersionByID(versionID) == nil {
		return nil, fmt.Errorf("%w: view version %d is not retained", iceberg.ErrInvalidArgument, versionID)
	}
	if current.CurrentVersion().VersionID == versionID {
		return current, nil
	}

	return updater.UpdateView(ctx, ident,
		[]view.Requirement{view.AssertViewUUID(current.Metadata().ViewUUID())},
		[]view.Update{view.NewSetCurrentVersionUpdate(versionID)})
}
