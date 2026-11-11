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
	"io"
	"net"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	"github.com/beltran/gohive/hive_metastore"
	"github.com/stretchr/testify/require"
)

func TestViewLockLeaseRenewsAndStops(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		var checks atomic.Int32
		client := &viewRenewalTestClient{check: func(ctx context.Context, id int64) (*hive_metastore.LockResponse, error) {
			checks.Add(1)

			return &hive_metastore.LockResponse{Lockid: id, State: hive_metastore.LockState_ACQUIRED}, nil
		}}
		lease := newViewLockLease(t.Context(), client, 7)
		synctest.Wait()
		time.Sleep(3500 * time.Millisecond)
		synctest.Wait()
		require.EqualValues(t, 3, checks.Load())
		require.NoError(t, lease.check(t.Context()))
		lease.stop()
		count := checks.Load()
		time.Sleep(3 * time.Second)
		require.Equal(t, count, checks.Load())
	})
}

func TestViewLockLeaseRemembersRenewalFailure(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		var checks atomic.Int32
		cause := errors.New("lock disappeared")
		client := &viewRenewalTestClient{check: func(context.Context, int64) (*hive_metastore.LockResponse, error) {
			checks.Add(1)

			return nil, cause
		}}
		lease := newViewLockLease(t.Context(), client, 7)
		synctest.Wait()
		time.Sleep(3500 * time.Millisecond)
		synctest.Wait()
		require.EqualValues(t, 1, checks.Load())
		require.ErrorIs(t, lease.check(t.Context()), cause)
		lease.stop()
	})
}

func TestViewLockLeaseRejectsNonAcquiredStates(t *testing.T) {
	for _, state := range []hive_metastore.LockState{hive_metastore.LockState_WAITING, hive_metastore.LockState_NOT_ACQUIRED, hive_metastore.LockState_ABORT} {
		client := &viewRenewalTestClient{check: func(context.Context, int64) (*hive_metastore.LockResponse, error) {
			return &hive_metastore.LockResponse{State: state}, nil
		}}
		lease := newViewLockLease(t.Context(), client, 7)
		require.ErrorIs(t, lease.check(t.Context()), ErrLockAcquisitionFailed)
		lease.stop()
	}
}

func TestLockClientCancellationInterruptsStalledMetastore(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	defer listener.Close()
	received := make(chan struct{})
	done := make(chan struct{})
	go func() {
		defer close(done)
		connection, err := listener.Accept()
		if err != nil {
			return
		}
		defer connection.Close()
		buffer := make([]byte, 4096)
		if _, err := connection.Read(buffer); err != nil {
			return
		}
		close(received)
		io.Copy(io.Discard, connection)
	}()
	original := &thriftClient{uri: "thrift://" + listener.Addr().String(), opts: &HiveOptions{}}
	ctx, cancel := context.WithCancel(t.Context())
	client, err := original.NewLockClient(ctx)
	require.NoError(t, err)
	defer client.Close()
	result := make(chan error, 1)
	go func() { _, err := client.CheckLock(ctx, 1); result <- err }()
	select {
	case <-received:
	case <-time.After(5 * time.Second):
		t.Fatal("metastore did not receive lock request")
	}
	cancel()
	select {
	case err := <-result:
		require.ErrorIs(t, err, context.Canceled)
	case <-time.After(5 * time.Second):
		t.Fatal("cancellation did not interrupt lock RPC")
	}
	<-done
}
