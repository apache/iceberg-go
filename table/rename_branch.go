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

package table

import (
	"context"
	"fmt"

	"github.com/apache/iceberg-go"
)

// RenameBranch atomically renames an existing branch without creating a snapshot.
// The renamed branch retains its snapshot ID and retention settings. The main
// branch cannot be renamed or replaced, and an existing ref cannot be overwritten.
// Concurrent changes to the source or creation of the destination fail the commit.
func (t Table) RenameBranch(ctx context.Context, from, to string) (*Table, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if from == "" || to == "" {
		return nil, fmt.Errorf("%w: branch names must not be empty", iceberg.ErrInvalidArgument)
	}
	if from == MainBranch || to == MainBranch {
		return nil, fmt.Errorf("%w: cannot rename or replace the main branch", iceberg.ErrInvalidArgument)
	}
	if t.metadata == nil || t.cat == nil {
		return nil, fmt.Errorf("%w: rename branch requires a loaded table and catalog", iceberg.ErrInvalidArgument)
	}

	var source SnapshotRef
	var sourceFound, destinationFound bool
	for name, ref := range t.metadata.Refs() {
		switch name {
		case from:
			source = ref
			sourceFound = true
		case to:
			destinationFound = true
		}
	}
	if !sourceFound {
		return nil, fmt.Errorf("%w: branch %q does not exist", iceberg.ErrInvalidArgument, from)
	}
	if source.SnapshotRefType != BranchRef {
		return nil, fmt.Errorf("%w: ref %q is a %s, not a branch", iceberg.ErrInvalidArgument, from, source.SnapshotRefType)
	}
	if destinationFound {
		return nil, fmt.Errorf("%w: ref %q already exists", iceberg.ErrInvalidArgument, to)
	}

	var maxRefAgeMs, maxSnapshotAgeMs int64
	var minSnapshotsToKeep int
	if source.MaxRefAgeMs != nil {
		maxRefAgeMs = *source.MaxRefAgeMs
	}
	if source.MaxSnapshotAgeMs != nil {
		maxSnapshotAgeMs = *source.MaxSnapshotAgeMs
	}
	if source.MinSnapshotsToKeep != nil {
		minSnapshotsToKeep = *source.MinSnapshotsToKeep
	}

	// Send both ref assertions directly to the catalog in one atomic commit.
	// Transaction.apply in older versions deduplicates requirements by type,
	// which would otherwise drop the destination-absence assertion.
	requirements := []Requirement{
		AssertTableUUID(t.metadata.TableUUID()),
		AssertRefSnapshotID(from, &source.SnapshotID),
		AssertRefSnapshotID(to, nil),
	}
	updates := []Update{
		NewSetSnapshotRefUpdate(to, source.SnapshotID, BranchRef,
			maxRefAgeMs, maxSnapshotAgeMs, minSnapshotsToKeep),
		NewRemoveSnapshotRefUpdate(from),
	}

	metadata, metadataLocation, err := t.cat.CommitTable(ctx, t.identifier, requirements, updates)
	if err != nil {
		return nil, fmt.Errorf("rename branch %q to %q: %w", from, to, err)
	}

	updated := t
	updated.metadata = metadata
	updated.metadataLocation = metadataLocation

	return &updated, nil
}
