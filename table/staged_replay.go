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
	"slices"
	"time"

	"github.com/apache/iceberg-go/io"
)

type stagedReplay struct {
	branch  string
	records []expirationRecord

	// Record positions stay fixed even when replanning changes the update count.
	// Snapshots use their most recent rebuild.
	updates []Update

	// Refs walked by expiries that produced updates.
	walkedRefs []string

	// Updates from the last successful replay.
	// ran distinguishes an empty replay from no replay.
	landed []Update
	ran    bool
}

func newStagedReplay(branch string, updates []Update, records []expirationRecord) *stagedReplay {
	return &stagedReplay{
		branch:     branch,
		records:    records,
		updates:    slices.Clone(updates),
		walkedRefs: stagedWalkedRefs(records),
	}
}

// On error, replay returns partial updates and replaced lists for cleanup.
// The partial updates include a rebuilt snapshot that failed to apply.
func (r *stagedReplay) replay(ctx context.Context, fresh Metadata, fs io.WriteFileIO, attempt int, force bool) ([]Update, []string, error) {
	b, err := MetadataBuilderFromBase(fresh, "")
	if err != nil {
		return nil, nil, err
	}

	var (
		out          []Update
		orphaned     []string
		walkedRefs   []string
		records      = r.records
		chainRebuilt = force
	)
	apply := func(updates ...Update) error {
		for _, u := range updates {
			if err := u.Apply(b); err != nil {
				return fmt.Errorf("apply %s: %w", u.Action(), err)
			}
			out = append(out, u)
		}

		return nil
	}

	for i := 0; i <= len(r.updates); {
		if len(records) > 0 && records[0].pos == i {
			rec := records[0]
			records = records[1:]
			updates, walked, err := r.replan(b, rec)
			if err != nil {
				return out, orphaned, err
			}
			if err := apply(updates...); err != nil {
				return out, orphaned, err
			}
			if len(updates) > 0 {
				walkedRefs = append(walkedRefs, walked...)
			}
			i += rec.count

			continue
		}
		if i == len(r.updates) {
			break
		}

		if su, ok := r.updates[i].(*addSnapshotUpdate); ok && su.rebuildManifestList != nil {
			// Sequence numbers and row IDs must include earlier replayed updates.
			state, err := b.clone().Build()
			if err != nil {
				return out, orphaned, err
			}
			head := b.currentSnapshotForRef(r.branch)
			next, err := rebuildStagedSnapshot(ctx, su, state, head, fs, attempt, chainRebuilt)
			if err != nil {
				return out, orphaned, err
			}
			if next != su {
				orphaned = append(orphaned, su.Snapshot.ManifestList)
				chainRebuilt = true
			}
			r.updates[i] = renewSnapshotTimestamp(next, head)
		}
		if err := apply(r.updates[i]); err != nil {
			return append(out, r.updates[i]), orphaned, fmt.Errorf("%w: staged update does not apply after refresh: %w", ErrTransactionUnusable, err)
		}
		i++
	}
	r.walkedRefs = walkedRefs
	r.landed, r.ran = out, true

	return out, orphaned, nil
}

// renewSnapshotTimestamp stamps a replayed snapshot with the current time, at
// least one millisecond after its parent. AddSnapshot rejects a timestamp more
// than a minute behind the refreshed table.
func renewSnapshotTimestamp(su *addSnapshotUpdate, parent *Snapshot) *addSnapshotUpdate {
	ts := time.Now().UnixMilli()
	if parent != nil {
		ts = max(ts, parent.TimestampMs+1)
	}
	snap := *su.Snapshot
	snap.TimestampMs = ts
	renewed := *su
	renewed.Snapshot = &snap

	return &renewed
}
