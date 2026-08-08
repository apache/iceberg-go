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
	"math/rand/v2"
	"slices"
	"strings"
	"time"

	"github.com/beltran/gohive/hive_metastore"
)

// ErrLockAcquisitionFailed is returned when a lock cannot be acquired after all retries.
var ErrLockAcquisitionFailed = errors.New("failed to acquire lock")

const pendingLockCleanupTimeout = 5 * time.Second

type HiveLock struct {
	client HiveClient
	lockId int64
}

func acquireLock(ctx context.Context, client HiveClient, database, tableName string, opts *HiveOptions) (*HiveLock, error) {
	return acquireLocks(ctx, client, []tableLockIdentifier{{database: database, table: tableName}}, opts)
}

type tableLockIdentifier struct {
	database string
	table    string
}

func acquireLocks(ctx context.Context, client HiveClient, identifiers []tableLockIdentifier, opts *HiveOptions) (_ *HiveLock, err error) {
	identifiers = slices.Clone(identifiers)
	slices.SortFunc(identifiers, func(a, b tableLockIdentifier) int {
		if cmp := strings.Compare(a.database, b.database); cmp != 0 {
			return cmp
		}

		return strings.Compare(a.table, b.table)
	})
	identifiers = slices.Compact(identifiers)

	components := make([]*hive_metastore.LockComponent, len(identifiers))
	for i, ident := range identifiers {
		tableName := ident.table
		components[i] = &hive_metastore.LockComponent{
			Type:      hive_metastore.LockType_EXCLUSIVE,
			Level:     hive_metastore.LockLevel_TABLE,
			Dbname:    ident.database,
			Tablename: &tableName,
		}
	}
	lockReq := &hive_metastore.LockRequest{
		Component: components,
	}

	lockResp, err := client.Lock(ctx, lockReq)
	if err != nil {
		return nil, fmt.Errorf("failed to request lock: %w", err)
	}

	if lockResp.State == hive_metastore.LockState_ACQUIRED {
		return &HiveLock{
			client: client,
			lockId: lockResp.Lockid,
		}, nil
	}

	cleanupPending := true
	defer func() {
		if !cleanupPending {
			return
		}

		cleanupCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), pendingLockCleanupTimeout)
		defer cancel()
		if cleanupErr := client.Unlock(cleanupCtx, lockResp.Lockid); cleanupErr != nil {
			err = errors.Join(err, fmt.Errorf("failed to release pending lock %d: %w", lockResp.Lockid, cleanupErr))
		}
	}()

	// If not acquired immediately, wait and retry
	for attempt := 0; attempt < opts.LockRetries; attempt++ {
		// Wait before checking again
		waitTime := applyJitter(
			calculateBackoff(attempt, opts.LockMinWaitTime, opts.LockMaxWaitTime),
			opts.LockMinWaitTime,
			opts.LockMaxWaitTime,
		)

		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(waitTime):
		}

		// Check lock state
		checkResp, err := client.CheckLock(ctx, lockResp.Lockid)
		if err != nil {
			return nil, fmt.Errorf("failed to check lock status: %w", err)
		}

		switch checkResp.State {
		case hive_metastore.LockState_ACQUIRED:
			cleanupPending = false

			return &HiveLock{
				client: client,
				lockId: lockResp.Lockid,
			}, nil
		case hive_metastore.LockState_WAITING:
			// Continue waiting
			continue
		case hive_metastore.LockState_ABORT:
			return nil, fmt.Errorf("%w: lock was aborted", ErrLockAcquisitionFailed)
		case hive_metastore.LockState_NOT_ACQUIRED:
			return nil, fmt.Errorf("%w: lock not acquired", ErrLockAcquisitionFailed)
		default:
			return nil, fmt.Errorf("%w: unexpected lock state: %v", ErrLockAcquisitionFailed, checkResp.State)
		}
	}

	return nil, fmt.Errorf("%w: exhausted %d retries for tables %s", ErrLockAcquisitionFailed, opts.LockRetries, formatLockIdentifiers(identifiers))
}

func formatLockIdentifiers(identifiers []tableLockIdentifier) string {
	names := make([]string, len(identifiers))
	for i, ident := range identifiers {
		names[i] = ident.database + "." + ident.table
	}

	return strings.Join(names, ", ")
}

func calculateBackoff(attempt int, minWait, maxWait time.Duration) time.Duration {
	if minWait >= maxWait {
		return maxWait
	}
	if attempt <= 0 || minWait <= 0 {
		return minWait
	}
	if attempt > 62 || minWait > maxWait>>attempt {
		return maxWait
	}

	return minWait << attempt
}

// applyJitter spreads a backoff interval so clients contending for the same lock
// stop re-polling in lockstep. calculateBackoff is a pure function of the attempt
// and the configured bounds, and contention is the precondition for retrying at
// all, so without this every waiter issues its CheckLock calls at the same instants
// and each round reaches the metastore as a burst.
//
// The invariants, in the order the code establishes them:
//   - below the cap the jitter is added rather than centred, so the wait is never
//     shorter than the interval calculateBackoff produced;
//   - at the cap there is no headroom left to add into, so the wait is spread
//     downward instead, floored at the last interval the sequence produced before
//     it saturated — a bound the schedule has already cleared, which stops a later
//     attempt from being allowed to wait less than an earlier one;
//   - the result never exceeds maxWait and never falls below minWait.
func applyJitter(d, minWait, maxWait time.Duration) time.Duration {
	if d <= 0 {
		return d
	}

	// A caller that hands in an interval already past the cap is outside the
	// contract; leave it exactly as given rather than silently reshaping it.
	headroom := maxWait - d
	if headroom < 0 {
		return d
	}

	// Add up to another full interval, without exceeding the configured maximum.
	extra := d
	if headroom < extra {
		extra = headroom
	}
	if extra > 0 {
		return d + time.Duration(rand.Int64N(int64(extra)+1))
	}

	// Replay the doubling sequence and keep the largest interval that still fitted
	// under the cap. The guard on scheduled keeps a non-positive or overflowing
	// minWait from spinning here.
	//
	// minWait is applied before the replay rather than relying on it. When minWait
	// is itself >= maxWait the loop cannot run at all, and options.go accepts that
	// configuration, so leaving the floor at d/2 there would allow a wait of a third
	// of the configured minimum.
	floor := max(minWait, d/2)
	for scheduled := minWait; scheduled > 0 && scheduled < maxWait; scheduled <<= 1 {
		if scheduled > floor {
			floor = scheduled
		}
	}
	if floor >= d {
		return d
	}

	return d - time.Duration(rand.Int64N(int64(d-floor)+1))
}

func (l *HiveLock) Release(ctx context.Context) error {
	return l.client.Unlock(ctx, l.lockId)
}

func (l *HiveLock) releaseForCleanup(ctx context.Context) error {
	cleanupCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), pendingLockCleanupTimeout)
	defer cancel()

	if err := l.Release(cleanupCtx); err != nil {
		return fmt.Errorf("failed to release acquired lock %d: %w", l.lockId, err)
	}

	return nil
}

func (l *HiveLock) LockID() int64 {
	if l == nil {
		return 0
	}

	return l.lockId
}
