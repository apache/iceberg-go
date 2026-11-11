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
	"fmt"
	"slices"
)

func (r *stagedReplay) replan(b *MetadataBuilder, rec expirationRecord) ([]Update, []string, error) {
	updates, walked, err := planExpiration(b, rec.cfg, rec.nowMs)
	if err != nil {
		return nil, nil, err
	}
	// A no-op expiry must not block other updates solely because GC is disabled.
	if len(updates) > 0 {
		if err := validateGCEnabledForSnapshotExpiration(b.props); err != nil {
			return nil, nil, fmt.Errorf("%w: %w", ErrTransactionUnusable, err)
		}
	}

	// Later updates depend on whether the staged plan removed the branch.
	if removes := removesRef(updates, r.branch); removes != rec.removedBranch {
		replanned, staged := "removes", "kept"
		if !removes {
			replanned, staged = "keeps", "removed"
		}

		return nil, nil, fmt.Errorf("%w: %w: replanned snapshot expiration %s commit branch %q, which the staged plan %s",
			ErrTransactionUnusable, ErrCommitFailed, replanned, r.branch, staged)
	}

	return updates, walked, nil
}

func (r *stagedReplay) requirements(base Metadata, reqs []Requirement) []Requirement {
	return expirationAssertions(base, r.branch, r.walkedRefs, reqs)
}

func stagedWalkedRefs(records []expirationRecord) []string {
	var refs []string
	for _, rec := range records {
		if rec.count > 0 {
			refs = append(refs, rec.walked...)
		}
	}

	return refs
}

// Use base ref IDs, not staged IDs. The commit branch already has an assertion
// that the transaction rebases.
func expirationAssertions(base Metadata, branch string, walkedRefs []string, reqs []Requirement) []Requirement {
	asserted := map[string]bool{branch: true}
	for _, r := range reqs {
		if a, ok := r.(*assertRefSnapshotID); ok {
			asserted[a.Ref] = true
		}
	}

	var names []string
	for _, name := range walkedRefs {
		if !asserted[name] {
			asserted[name] = true
			names = append(names, name)
		}
	}
	slices.Sort(names)

	out := make([]Requirement, len(names))
	for i, name := range names {
		out[i] = AssertRefSnapshotID(name, refSnapshotID(base, name))
	}

	return out
}
