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
// that create a new table, mirroring Java's UpdateRequirements.forCreateTable.
func RequirementsForCreateTable(updates []Update) []Requirement {
	return deriveRequirements(nil, false, AssertCreate(), updates)
}

// RequirementsForReplaceTable returns the requirements for committing updates
// that replace the table described by base. A replace overwrites the current
// schema, spec, sort order and refs, so only ID assignment is guarded.
func RequirementsForReplaceTable(base Metadata, updates []Update) ([]Requirement, error) {
	if base == nil {
		return nil, fmt.Errorf("%w: table metadata is nil", iceberg.ErrInvalidArgument)
	}

	return deriveRequirements(base, true, AssertTableUUID(base.TableUUID()), updates), nil
}

// RequirementsForUpdateTable returns the requirements for committing updates
// against the table described by base, so a commit fails instead of
// overwriting a concurrent change. Use it to build the requirements for
// Catalog.CommitTable when not going through Transaction.
func RequirementsForUpdateTable(base Metadata, updates []Update) ([]Requirement, error) {
	if base == nil {
		return nil, fmt.Errorf("%w: table metadata is nil", iceberg.ErrInvalidArgument)
	}

	return deriveRequirements(base, false, AssertTableUUID(base.TableUUID()), updates), nil
}

// deriveRequirements asserts the base state each update depends on. Every
// requirement is computed from base, so duplicates are dropped by type (and
// ref name for ref assertions), like requirementSemanticKey.
func deriveRequirements(base Metadata, isReplace bool, first Requirement, updates []Update) []Requirement {
	reqs := []Requirement{first}
	if base == nil {
		return reqs
	}

	seen := make(map[string]bool)
	add := func(key string, r Requirement) {
		if !seen[key] {
			seen[key] = true
			reqs = append(reqs, r)
		}
	}
	addRef := func(name string) {
		add(reqAssertRefSnapshotID+"\x00"+name, AssertRefSnapshotID(name, refSnapshotID(base, name)))
	}
	// Old schemas and specs must not be written by a branch that changed concurrently.
	addBranches := func() {
		for _, name := range nonMainBranches(base) {
			addRef(name)
		}
	}
	addCurrentSchema := func() {
		add(reqAssertCurrentSchemaID, AssertCurrentSchemaID(base.CurrentSchema().ID))
	}
	addDefaultSpec := func() {
		add(reqAssertDefaultSpecID, AssertDefaultSpecID(base.DefaultPartitionSpec()))
	}

	for _, u := range updates {
		// A replace overwrites the current state, so only ID assignment is guarded.
		switch u := u.(type) {
		case *addSchemaUpdate:
			add(reqAssertLastAssignedFieldID, AssertLastAssignedFieldID(base.LastColumnID()))
		case *addPartitionSpecUpdate:
			add(reqAssertLastAssignedPartitionID, AssertLastAssignedPartitionID(lastAssignedPartitionID(base)))
		case *setSnapshotRefUpdate:
			if !isReplace {
				addRef(u.RefName)
			}
		case *setCurrentSchemaUpdate:
			if !isReplace {
				addCurrentSchema()
			}
		case *setDefaultSpecUpdate:
			if !isReplace {
				addDefaultSpec()
			}
		case *setDefaultSortOrderUpdate:
			if !isReplace {
				add(reqAssertDefaultSortOrderID, AssertDefaultSortOrderID(base.DefaultSortOrder()))
			}
		case *removeSpecUpdate:
			if !isReplace {
				addDefaultSpec()
				addBranches()
			}
		case *removeSchemasUpdate:
			if !isReplace {
				addCurrentSchema()
				addBranches()
			}
		}
	}

	return reqs
}

// refSnapshotID returns the snapshot the named ref points at, or nil if the
// ref does not exist.
func refSnapshotID(meta Metadata, name string) *int64 {
	for n, ref := range meta.Refs() {
		if n == name {
			return &ref.SnapshotID
		}
	}

	return nil
}

// nonMainBranches returns the names of base's branches other than main, sorted
// so the requirement order is deterministic.
func nonMainBranches(base Metadata) []string {
	var names []string
	for name, ref := range base.Refs() {
		if ref.Type == BranchRef && name != MainBranch {
			names = append(names, name)
		}
	}
	slices.Sort(names)

	return names
}

// lastAssignedPartitionID falls back to the ID before the first partition
// field when base has none, as UpdateSpec does.
func lastAssignedPartitionID(base Metadata) int {
	if id := base.LastPartitionSpecID(); id != nil {
		return *id
	}

	return iceberg.PartitionDataIDStart - 1
}
