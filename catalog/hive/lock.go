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
	"math"
	"time"

	"github.com/beltran/gohive/hive_metastore"
)

// ErrLockAcquisitionFailed is returned when a lock cannot be acquired after all retries.
var ErrLockAcquisitionFailed = errors.New("failed to acquire lock")

type HiveLock struct {
	client HiveClient
	lockId int64
}

func acquireLock(ctx context.Context, client HiveClient, database, tableName string, opts *HiveOptions) (*HiveLock, error) {
	lockReq := &hive_metastore.LockRequest{
		Component: []*hive_metastore.LockComponent{{
			Type:      hive_metastore.LockType_EXCLUSIVE,
			Level:     hive_metastore.LockLevel_TABLE,
			Dbname:    database,
			Tablename: &tableName,
		}},
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

	// If not acquired immediately, wait and retry
	for attempt := 0; attempt < opts.LockRetries; attempt++ {
		// Wait before checking again
		waitTime := calculateBackoff(attempt, opts.LockMinWaitTime, opts.LockMaxWaitTime)

		select {
		case <-ctx.Done():
			_ = client.Unlock(ctx, lockResp.Lockid)

			return nil, ctx.Err()
		case <-time.After(waitTime):
		}

		// Check lock state
		checkResp, err := client.CheckLock(ctx, lockResp.Lockid)
		if err != nil {
			_ = client.Unlock(ctx, lockResp.Lockid)

			return nil, fmt.Errorf("failed to check lock status: %w", err)
		}

		switch checkResp.State {
		case hive_metastore.LockState_ACQUIRED:
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

	_ = client.Unlock(ctx, lockResp.Lockid)

	return nil, fmt.Errorf("%w: exhausted %d retries for table %s.%s", ErrLockAcquisitionFailed, opts.LockRetries, database, tableName)
}

func calculateBackoff(attempt int, minWait, maxWait time.Duration) time.Duration {
	wait := time.Duration(float64(minWait) * math.Pow(2, float64(attempt)))
	if wait > maxWait {
		wait = maxWait
	}

	return wait
}

func (l *HiveLock) Release(ctx context.Context) error {
	return l.client.Unlock(ctx, l.lockId)
}

func (l *HiveLock) LockID() int64 {
	if l == nil {
		return 0
	}

	return l.lockId
}
