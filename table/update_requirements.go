// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package table

import (
	"fmt"
	"slices"

	"github.com/apache/iceberg-go"
)

// RequirementsForCreateTable returns the requirements for committing updates
// that create a new table: the table must not already exist. It mirrors Java's
// UpdateRequirements.forCreateTable.
func RequirementsForCreateTable(updates []Update) []Requirement {
	b := newRequirementsBuilder(nil, false)
	b.require(AssertCreate())
	for _, u := range updates {
		b.update(u)
	}

	return b.reqs
}

// RequirementsForReplaceTable returns the requirements for committing updates
// that replace the table described by base. Only the table UUID and the
// last-assigned field and partition IDs are asserted, since a replace
// overwrites the current schema, spec, sort order and refs. It mirrors Java's
// UpdateRequirements.forReplaceTable.
func RequirementsForReplaceTable(base Metadata, updates []Update) ([]Requirement, error) {
	return requirementsForExisting(base, updates, true)
}

// RequirementsForUpdateTable returns the optimistic-concurrency requirements
// for committing updates against the table described by base, so that a
// commit fails rather than silently overwriting a concurrent change. It
// mirrors Java's UpdateRequirements.forUpdateTable.
//
// Callers that build their own []Update instead of going through Transaction
// should use this to derive the requirements passed to Catalog.CommitTable.
func RequirementsForUpdateTable(base Metadata, updates []Update) ([]Requirement, error) {
	return requirementsForExisting(base, updates, false)
}

func requirementsForExisting(base Metadata, updates []Update, isReplace bool) ([]Requirement, error) {
	if base == nil {
		return nil, fmt.Errorf("%w: table metadata is nil", iceberg.ErrInvalidArgument)
	}

	b := newRequirementsBuilder(base, isReplace)
	b.require(AssertTableUUID(base.TableUUID()))
	for _, u := range updates {
		b.update(u)
	}

	return b.reqs, nil
}

type requirementsBuilder struct {
	base      Metadata
	isReplace bool
	reqs      []Requirement

	changedRefs map[string]struct{}
	addedSchema bool
	setSchemaID bool
	addedSpec   bool
	setSpecID   bool
	setOrderID  bool
}

func newRequirementsBuilder(base Metadata, isReplace bool) *requirementsBuilder {
	return &requirementsBuilder{
		base:        base,
		isReplace:   isReplace,
		changedRefs: make(map[string]struct{}),
	}
}

func (b *requirementsBuilder) require(r Requirement) {
	b.reqs = append(b.reqs, r)
}

func (b *requirementsBuilder) update(u Update) {
	switch u.Action() {
	case UpdateSetSnapshotRef:
		if ref, ok := u.(*setSnapshotRefUpdate); ok {
			b.setSnapshotRef(ref.RefName)
		}
	case UpdateAddSchema:
		if !b.addedSchema {
			if b.base != nil {
				b.require(AssertLastAssignedFieldID(b.base.LastColumnID()))
			}
			b.addedSchema = true
		}
	case UpdateSetCurrentSchema:
		b.requireCurrentSchemaNotChanged()
	case UpdateAddSpec:
		if !b.addedSpec {
			if b.base != nil {
				b.require(AssertLastAssignedPartitionID(lastAssignedPartitionID(b.base)))
			}
			b.addedSpec = true
		}
	case UpdateSetDefaultSpec:
		b.requireDefaultSpecNotChanged()
	case UpdateSetDefaultSortOrder:
		if !b.setOrderID {
			if b.base != nil && !b.isReplace {
				b.require(AssertDefaultSortOrderID(b.base.DefaultSortOrder()))
			}
			b.setOrderID = true
		}
	case UpdateRemoveSpec:
		b.requireDefaultSpecNotChanged()
		// Old specs must not be written by a branch that changed concurrently.
		b.requireNoBranchesChanged()
	case UpdateRemoveSchemas:
		b.requireCurrentSchemaNotChanged()
		// Old schemas must not be written by a branch that changed concurrently.
		b.requireNoBranchesChanged()
	}
}

func (b *requirementsBuilder) setSnapshotRef(name string) {
	if _, seen := b.changedRefs[name]; seen {
		return
	}
	b.changedRefs[name] = struct{}{}

	if b.base == nil || b.isReplace {
		return
	}

	// The ref must still point at the base snapshot, or still not exist.
	var id *int64
	if ref, ok := findRef(b.base, name); ok {
		id = &ref.SnapshotID
	}
	b.require(AssertRefSnapshotID(name, id))
}

func (b *requirementsBuilder) requireCurrentSchemaNotChanged() {
	if b.setSchemaID {
		return
	}
	if b.base != nil && !b.isReplace {
		b.require(AssertCurrentSchemaID(b.base.CurrentSchema().ID))
	}
	b.setSchemaID = true
}

func (b *requirementsBuilder) requireDefaultSpecNotChanged() {
	if b.setSpecID {
		return
	}
	if b.base != nil && !b.isReplace {
		b.require(AssertDefaultSpecID(b.base.DefaultPartitionSpec()))
	}
	b.setSpecID = true
}

// requireNoBranchesChanged asserts every non-main branch is unchanged. Unlike
// Java, branches that are already asserted (by an earlier call or a
// set-snapshot-ref update) are skipped so the same assertion is not repeated.
func (b *requirementsBuilder) requireNoBranchesChanged() {
	if b.base == nil || b.isReplace {
		return
	}

	var names []string
	for name, ref := range b.base.Refs() {
		if ref.SnapshotRefType == BranchRef && name != MainBranch {
			names = append(names, name)
		}
	}
	// Refs iterates a map; sort so the requirement order is deterministic.
	slices.Sort(names)

	for _, name := range names {
		if _, seen := b.changedRefs[name]; seen {
			continue
		}
		b.changedRefs[name] = struct{}{}
		ref, _ := findRef(b.base, name)
		b.require(AssertRefSnapshotID(name, &ref.SnapshotID))
	}
}

func findRef(meta Metadata, name string) (SnapshotRef, bool) {
	for n, ref := range meta.Refs() {
		if n == name {
			return ref, true
		}
	}

	return SnapshotRef{}, false
}

// lastAssignedPartitionID returns base's last-assigned partition field ID,
// falling back to the value before the first partition field ID when the
// metadata has none, as UpdateSpec does.
func lastAssignedPartitionID(base Metadata) int {
	if id := base.LastPartitionSpecID(); id != nil {
		return *id
	}

	return iceberg.PartitionDataIDStart - 1
}
