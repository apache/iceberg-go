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
	"fmt"
	"sync"
	"time"
)

// Heartbeat renews the HMS lock lease. Use a dedicated connection because
// Thrift clients cannot safely send overlapping RPCs on one connection.
type viewLockLease struct {
	client  LockClient
	id      int64
	mu      sync.Mutex
	failure error
	cancel  context.CancelFunc
	done    chan struct{}
}

func newViewLockLease(ctx context.Context, client LockClient, id int64, interval time.Duration) *viewLockLease {
	if interval <= 0 {
		interval = DefaultLockHeartbeatInterval
	}
	ctx, cancel := context.WithCancel(ctx)
	lease := &viewLockLease{client: client, id: id, cancel: cancel, done: make(chan struct{})}
	go func() {
		defer close(lease.done)
		ticker := time.NewTicker(interval)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				if err := lease.check(ctx); err != nil {
					return
				}
			}
		}
	}()

	return lease
}

func (l *viewLockLease) check(ctx context.Context) error {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.failure != nil {
		return l.failure
	}
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	if err := l.client.Heartbeat(ctx, l.id); err != nil {
		l.failure = fmt.Errorf("failed to renew Hive lock: %w", err)
	}

	return l.failure
}

func (l *viewLockLease) stop() {
	l.cancel()
	<-l.done
}
