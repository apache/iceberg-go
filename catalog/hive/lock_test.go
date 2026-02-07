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
	"testing"
	"time"

	"github.com/beltran/gohive/hive_metastore"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

func TestAcquireLockImmediateSuccess(t *testing.T) {
	mockClient := new(mockHiveClient)
	ctx := context.Background()
	opts := NewHiveOptions()

	mockClient.On("Lock", ctx, mock.AnythingOfType("*hive_metastore.LockRequest")).
		Return(&hive_metastore.LockResponse{
			Lockid: 123,
			State:  hive_metastore.LockState_ACQUIRED,
		}, nil)

	lock, err := acquireLock(ctx, mockClient, "testdb", "testtable", opts)

	require.NoError(t, err)
	require.NotNil(t, lock)
	assert.Equal(t, int64(123), lock.LockID())

	mockClient.AssertExpectations(t)
}

func TestAcquireLockWithRetry(t *testing.T) {
	mockClient := new(mockHiveClient)
	ctx := context.Background()
	opts := NewHiveOptions()
	opts.LockMinWaitTime = 1 * time.Millisecond // Fast retries for testing
	opts.LockMaxWaitTime = 10 * time.Millisecond
	opts.LockRetries = 3

	// Lock request returns WAITING initially
	mockClient.On("Lock", ctx, mock.AnythingOfType("*hive_metastore.LockRequest")).
		Return(&hive_metastore.LockResponse{
			Lockid: 456,
			State:  hive_metastore.LockState_WAITING,
		}, nil)

	// First CheckLock returns WAITING
	mockClient.On("CheckLock", ctx, int64(456)).
		Return(&hive_metastore.LockResponse{
			Lockid: 456,
			State:  hive_metastore.LockState_WAITING,
		}, nil).Once()

	// Second CheckLock returns ACQUIRED
	mockClient.On("CheckLock", ctx, int64(456)).
		Return(&hive_metastore.LockResponse{
			Lockid: 456,
			State:  hive_metastore.LockState_ACQUIRED,
		}, nil).Once()

	lock, err := acquireLock(ctx, mockClient, "testdb", "testtable", opts)

	require.NoError(t, err)
	require.NotNil(t, lock)
	assert.Equal(t, int64(456), lock.LockID())

	mockClient.AssertExpectations(t)
}

func TestAcquireLockExhaustsRetries(t *testing.T) {
	mockClient := new(mockHiveClient)
	ctx := context.Background()
	opts := NewHiveOptions()
	opts.LockMinWaitTime = 1 * time.Millisecond
	opts.LockMaxWaitTime = 10 * time.Millisecond
	opts.LockRetries = 2

	// Lock request returns WAITING
	mockClient.On("Lock", ctx, mock.AnythingOfType("*hive_metastore.LockRequest")).
		Return(&hive_metastore.LockResponse{
			Lockid: 789,
			State:  hive_metastore.LockState_WAITING,
		}, nil)

	// All CheckLock calls return WAITING
	mockClient.On("CheckLock", ctx, int64(789)).
		Return(&hive_metastore.LockResponse{
			Lockid: 789,
			State:  hive_metastore.LockState_WAITING,
		}, nil)

	mockClient.On("Unlock", ctx, int64(789)).Return(nil)

	lock, err := acquireLock(ctx, mockClient, "testdb", "testtable", opts)

	require.Error(t, err)
	require.Nil(t, lock)
	assert.ErrorIs(t, err, ErrLockAcquisitionFailed)
	assert.Contains(t, err.Error(), "exhausted 2 retries")

	mockClient.AssertExpectations(t)
}

func TestAcquireLockAborted(t *testing.T) {
	mockClient := new(mockHiveClient)
	ctx := context.Background()
	opts := NewHiveOptions()
	opts.LockMinWaitTime = 1 * time.Millisecond

	// Lock request returns WAITING
	mockClient.On("Lock", ctx, mock.AnythingOfType("*hive_metastore.LockRequest")).
		Return(&hive_metastore.LockResponse{
			Lockid: 111,
			State:  hive_metastore.LockState_WAITING,
		}, nil)

	// CheckLock returns ABORT
	mockClient.On("CheckLock", ctx, int64(111)).
		Return(&hive_metastore.LockResponse{
			Lockid: 111,
			State:  hive_metastore.LockState_ABORT,
		}, nil)

	lock, err := acquireLock(ctx, mockClient, "testdb", "testtable", opts)

	require.Error(t, err)
	require.Nil(t, lock)
	assert.ErrorIs(t, err, ErrLockAcquisitionFailed)
	assert.Contains(t, err.Error(), "aborted")

	mockClient.AssertExpectations(t)
}

func TestAcquireLockRequestFails(t *testing.T) {
	mockClient := new(mockHiveClient)
	ctx := context.Background()
	opts := NewHiveOptions()

	// Lock request fails
	mockClient.On("Lock", ctx, mock.AnythingOfType("*hive_metastore.LockRequest")).
		Return(nil, errors.New("connection failed"))

	lock, err := acquireLock(ctx, mockClient, "testdb", "testtable", opts)

	require.Error(t, err)
	require.Nil(t, lock)
	assert.Contains(t, err.Error(), "failed to request lock")

	mockClient.AssertExpectations(t)
}

func TestAcquireLockCheckFails(t *testing.T) {
	mockClient := new(mockHiveClient)
	ctx := context.Background()
	opts := NewHiveOptions()
	opts.LockMinWaitTime = 1 * time.Millisecond

	// Lock request returns WAITING
	mockClient.On("Lock", ctx, mock.AnythingOfType("*hive_metastore.LockRequest")).
		Return(&hive_metastore.LockResponse{
			Lockid: 222,
			State:  hive_metastore.LockState_WAITING,
		}, nil)

	// CheckLock fails
	mockClient.On("CheckLock", ctx, int64(222)).
		Return(nil, errors.New("check failed"))

	// Lock should be released on error
	mockClient.On("Unlock", ctx, int64(222)).Return(nil)

	lock, err := acquireLock(ctx, mockClient, "testdb", "testtable", opts)

	require.Error(t, err)
	require.Nil(t, lock)
	assert.Contains(t, err.Error(), "failed to check lock status")

	mockClient.AssertExpectations(t)
}

func TestAcquireLockContextCancelled(t *testing.T) {
	mockClient := new(mockHiveClient)
	ctx, cancel := context.WithCancel(context.Background())
	opts := NewHiveOptions()
	opts.LockMinWaitTime = 100 * time.Millisecond // Longer wait so we can cancel

	// Lock request returns WAITING
	mockClient.On("Lock", ctx, mock.AnythingOfType("*hive_metastore.LockRequest")).
		Return(&hive_metastore.LockResponse{
			Lockid: 333,
			State:  hive_metastore.LockState_WAITING,
		}, nil)

	// Lock should be released when context is cancelled
	mockClient.On("Unlock", ctx, int64(333)).Return(nil)

	// Cancel context before the wait completes
	go func() {
		time.Sleep(10 * time.Millisecond)
		cancel()
	}()

	lock, err := acquireLock(ctx, mockClient, "testdb", "testtable", opts)

	require.Error(t, err)
	require.Nil(t, lock)
	assert.ErrorIs(t, err, context.Canceled)

	mockClient.AssertExpectations(t)
}

func TestReleaseLock(t *testing.T) {
	mockClient := new(mockHiveClient)
	ctx := context.Background()

	lock := &HiveLock{
		client: mockClient,
		lockId: 999,
	}

	mockClient.On("Unlock", ctx, int64(999)).Return(nil)

	err := lock.Release(ctx)

	require.NoError(t, err)
	mockClient.AssertExpectations(t)
}

func TestCalculateBackoff(t *testing.T) {
	minWait := 100 * time.Millisecond
	maxWait := 1 * time.Second

	tests := []struct {
		attempt  int
		expected time.Duration
	}{
		{0, 100 * time.Millisecond}, // 100ms * 2^0 = 100ms
		{1, 200 * time.Millisecond}, // 100ms * 2^1 = 200ms
		{2, 400 * time.Millisecond}, // 100ms * 2^2 = 400ms
		{3, 800 * time.Millisecond}, // 100ms * 2^3 = 800ms
		{4, 1 * time.Second},        // 100ms * 2^4 = 1.6s, capped at 1s
		{10, 1 * time.Second},       // Capped at maxWait
	}

	for _, tt := range tests {
		t.Run("", func(t *testing.T) {
			result := calculateBackoff(tt.attempt, minWait, maxWait)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestLockConfigurationParsing(t *testing.T) {
	props := map[string]string{
		LockCheckMinWaitTime: "200ms",
		LockCheckMaxWaitTime: "30s",
		LockCheckRetries:     "5",
	}

	opts := NewHiveOptions()
	opts.ApplyProperties(props)

	assert.Equal(t, 200*time.Millisecond, opts.LockMinWaitTime)
	assert.Equal(t, 30*time.Second, opts.LockMaxWaitTime)
	assert.Equal(t, 5, opts.LockRetries)
}

func TestLockConfigurationDefaults(t *testing.T) {
	opts := NewHiveOptions()

	assert.Equal(t, DefaultLockCheckMinWaitTime, opts.LockMinWaitTime)
	assert.Equal(t, DefaultLockCheckMaxWaitTime, opts.LockMaxWaitTime)
	assert.Equal(t, DefaultLockCheckRetries, opts.LockRetries)
}
