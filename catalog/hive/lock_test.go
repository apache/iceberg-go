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

func TestAcquireLocksSortsComponents(t *testing.T) {
	mockClient := new(mockHiveClient)
	ctx := context.Background()
	var request *hive_metastore.LockRequest
	mockClient.On("Lock", ctx, mock.AnythingOfType("*hive_metastore.LockRequest")).
		Run(func(args mock.Arguments) {
			request = args.Get(1).(*hive_metastore.LockRequest)
		}).
		Return(&hive_metastore.LockResponse{Lockid: 124, State: hive_metastore.LockState_ACQUIRED}, nil).Once()
	mockClient.On("Unlock", ctx, int64(124)).Return(nil).Once()

	lock, err := acquireLocks(ctx, mockClient, []tableLockIdentifier{
		{database: "z_db", table: "source"},
		{database: "a_db", table: "destination"},
	}, NewHiveOptions())
	require.NoError(t, err)
	require.Len(t, request.Component, 2)
	assert.Equal(t, "a_db", request.Component[0].Dbname)
	assert.Equal(t, "destination", *request.Component[0].Tablename)
	assert.Equal(t, "z_db", request.Component[1].Dbname)
	assert.Equal(t, "source", *request.Component[1].Tablename)
	require.NoError(t, lock.Release(ctx))
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

	mockClient.On("Unlock", mock.MatchedBy(func(cleanupCtx context.Context) bool {
		_, hasDeadline := cleanupCtx.Deadline()

		return cleanupCtx.Err() == nil && hasDeadline
	}), int64(789)).Return(nil)

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
	mockClient.On("Unlock", mock.Anything, int64(111)).Return(nil)

	lock, err := acquireLock(ctx, mockClient, "testdb", "testtable", opts)

	require.Error(t, err)
	require.Nil(t, lock)
	assert.ErrorIs(t, err, ErrLockAcquisitionFailed)
	assert.Contains(t, err.Error(), "aborted")

	mockClient.AssertExpectations(t)
}

func TestAcquireLockUnexpectedStateCleansUp(t *testing.T) {
	mockClient := new(mockHiveClient)
	ctx := context.Background()
	opts := NewHiveOptions()
	opts.LockMinWaitTime = time.Millisecond

	mockClient.On("Lock", ctx, mock.AnythingOfType("*hive_metastore.LockRequest")).
		Return(&hive_metastore.LockResponse{Lockid: 112, State: hive_metastore.LockState_WAITING}, nil)
	mockClient.On("CheckLock", ctx, int64(112)).
		Return(&hive_metastore.LockResponse{Lockid: 112, State: hive_metastore.LockState(99)}, nil)
	mockClient.On("Unlock", mock.Anything, int64(112)).Return(nil)

	lock, err := acquireLock(ctx, mockClient, "testdb", "testtable", opts)

	require.Nil(t, lock)
	require.ErrorIs(t, err, ErrLockAcquisitionFailed)
	require.ErrorContains(t, err, "unexpected lock state")
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
	mockClient.On("Unlock", mock.Anything, int64(222)).Return(nil)

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

	// Lock cleanup must use a live, bounded context after the caller is cancelled.
	mockClient.On("Unlock", mock.MatchedBy(func(cleanupCtx context.Context) bool {
		_, hasDeadline := cleanupCtx.Deadline()

		return cleanupCtx.Err() == nil && hasDeadline
	}), int64(333)).Return(nil)

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

func TestAcquireLockJoinsCleanupFailure(t *testing.T) {
	mockClient := new(mockHiveClient)
	ctx := context.Background()
	opts := NewHiveOptions()
	opts.LockMinWaitTime = time.Millisecond
	opts.LockRetries = 1
	checkErr := errors.New("check failed")
	cleanupErr := errors.New("unlock failed")

	mockClient.On("Lock", ctx, mock.AnythingOfType("*hive_metastore.LockRequest")).
		Return(&hive_metastore.LockResponse{Lockid: 334, State: hive_metastore.LockState_WAITING}, nil)
	mockClient.On("CheckLock", ctx, int64(334)).Return(nil, checkErr)
	mockClient.On("Unlock", mock.Anything, int64(334)).Return(cleanupErr)

	lock, err := acquireLock(ctx, mockClient, "testdb", "testtable", opts)

	require.Nil(t, lock)
	require.ErrorIs(t, err, checkErr)
	require.ErrorIs(t, err, cleanupErr)
	require.ErrorContains(t, err, "failed to release pending lock 334")
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

func TestReleaseLockForCleanupUsesLiveBoundedContext(t *testing.T) {
	mockClient := new(mockHiveClient)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	lock := &HiveLock{client: mockClient, lockId: 1000}

	mockClient.On("Unlock", mock.MatchedBy(func(cleanupCtx context.Context) bool {
		_, hasDeadline := cleanupCtx.Deadline()

		return cleanupCtx.Err() == nil && hasDeadline
	}), int64(1000)).Return(nil).Once()

	require.NoError(t, lock.releaseForCleanup(ctx))
	mockClient.AssertExpectations(t)
}

func TestReleaseLockForCleanupWrapsFailure(t *testing.T) {
	mockClient := new(mockHiveClient)
	unlockErr := errors.New("unlock failed")
	lock := &HiveLock{client: mockClient, lockId: 1001}
	mockClient.On("Unlock", mock.Anything, int64(1001)).Return(unlockErr).Once()

	err := lock.releaseForCleanup(context.Background())

	require.ErrorIs(t, err, unlockErr)
	require.ErrorContains(t, err, "failed to release acquired lock 1001")
	mockClient.AssertExpectations(t)
}

func TestCalculateBackoff(t *testing.T) {
	minWait := 100 * time.Millisecond
	maxWait := 1 * time.Second

	tests := []struct {
		attempt  int
		expected time.Duration
	}{
		{0, 100 * time.Millisecond},    // 100ms * 1.5^0
		{1, 150 * time.Millisecond},    // 100ms * 1.5^1
		{2, 225 * time.Millisecond},    // 100ms * 1.5^2
		{3, 337500 * time.Microsecond}, // 100ms * 1.5^3
		{4, 506250 * time.Microsecond}, // 100ms * 1.5^4
		{5, 759375 * time.Microsecond}, // 100ms * 1.5^5
		{6, 1 * time.Second},           // 100ms * 1.5^6 ≈ 1.14s, capped at 1s
		{10, 1 * time.Second},          // Capped at maxWait
		{1000, 1 * time.Second},        // Large retry counts remain capped
	}

	for _, tt := range tests {
		t.Run("", func(t *testing.T) {
			result := calculateBackoff(tt.attempt, minWait, maxWait)
			assert.Equal(t, tt.expected, result)
		})
	}

	maxDuration := time.Duration(1<<63 - 1)
	assert.Equal(t, maxDuration, calculateBackoff(10000, time.Nanosecond, maxDuration),
		"extreme attempt counts must cap without overflowing")
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

func TestApplyJitterBelowCapNeverShorterThanInput(t *testing.T) {
	minWait := time.Millisecond
	maxWait := time.Minute

	for _, d := range []time.Duration{
		time.Millisecond,
		100 * time.Millisecond,
		time.Second,
	} {
		for i := 0; i < 500; i++ {
			got := applyJitter(d, minWait, maxWait)
			assert.GreaterOrEqual(t, got, d,
				"jitter must never wait less than the configured interval")
			assert.LessOrEqual(t, got, min(2*d, maxWait),
				"jitter must not exceed one extra interval or the configured maximum")
		}
	}
}

// Once calculateBackoff tops out there is no headroom to add into, so the wait
// is spread downwards. It must still vary, or every retry after the sequence
// saturates puts contending clients back in lockstep.
func TestApplyJitterAtCapStaysWithinBoundsAndVaries(t *testing.T) {
	minWait := 100 * time.Millisecond
	maxWait := time.Second

	// 759.375ms is the last interval the 1.5× sequence produced before saturating,
	// which is the floor the code actually guarantees. Asserting maxWait/1.5 here
	// would pass a regression that let the spread dip below that interval.
	lastUncapped := 759375 * time.Microsecond
	seen := make(map[time.Duration]struct{})
	for i := 0; i < 500; i++ {
		got := applyJitter(maxWait, minWait, maxWait)
		assert.GreaterOrEqual(t, got, lastUncapped,
			"the spread at the cap must be at least the last uncapped interval")
		assert.LessOrEqual(t, got, maxWait,
			"the wait must never exceed the configured maximum")
		seen[got] = struct{}{}
	}

	assert.Greater(t, len(seen), 1,
		"waits at the cap must vary so contending clients do not poll in lockstep")
}

// The spread at the cap must never poll sooner than a configured minimum wait,
// which is reachable whenever minWait is more than half of maxWait.
func TestApplyJitterAtCapNeverPollsSoonerThanMinWait(t *testing.T) {
	minWait := 40 * time.Second
	maxWait := time.Minute

	for i := 0; i < 500; i++ {
		assert.GreaterOrEqual(t, applyJitter(maxWait, minWait, maxWait), minWait,
			"a caller who configures a minimum wait must never poll sooner than it")
	}
}

// The guaranteed minimum must not fall as the sequence saturates. The attempt
// before the cap waits for its own full interval, so flooring the spread at
// maxWait/1.5 would let the first saturated attempt poll sooner than the one
// before it, which inverts the property that the wait between checks only ever
// grows.
//
// Reaching the cap takes a lowered lock-check-max-wait-time or a raised retry
// count; the defaults (100ms, 1 minute, 4 retries) top out at 337.5ms and never
// saturate. The values below are the smallest that put the boundary in range.
func TestApplyJitterMinimumIsMonotonicAcrossTheCap(t *testing.T) {
	minWait := 100 * time.Millisecond
	maxWait := time.Second

	// The sequence runs 100ms, 150ms, 225ms, 337.5ms, 506.25ms, 759.375ms, then
	// saturates at 1s. Attempt 5 draws from [759.375ms, 1s]; attempt 6 is the
	// first capped one.
	//
	// The bound compared against is exact, not sampled. Below the cap the jitter is
	// added on top, so the interval itself is the floor and no estimate is needed.
	// Sampling both sides instead would compare two noisy minima that sit a hair
	// above the same true floor, and which of them lands lower is chance.
	var floor time.Duration
	for attempt := 0; attempt < 10; attempt++ {
		d := calculateBackoff(attempt, minWait, maxWait)

		for i := 0; i < 2000; i++ {
			assert.GreaterOrEqual(t, applyJitter(d, minWait, maxWait), floor,
				"attempt %d may not be allowed to wait less than an earlier attempt", attempt)
		}

		if d < maxWait {
			floor = d
		}
	}
}

// The specific boundary above, pinned so a future change to the floor cannot
// quietly reintroduce the dip without failing on the exact numbers.
func TestApplyJitterAtCapFloorsAtTheLastUncappedInterval(t *testing.T) {
	minWait := 100 * time.Millisecond
	maxWait := time.Second
	lastUncapped := 759375 * time.Microsecond

	assert.Equal(t, lastUncapped, calculateBackoff(5, minWait, maxWait),
		"the last interval before the cap")
	assert.Equal(t, maxWait, calculateBackoff(6, minWait, maxWait),
		"the first interval at the cap")

	for i := 0; i < 2000; i++ {
		assert.GreaterOrEqual(t, applyJitter(maxWait, minWait, maxWait), lastUncapped,
			"the spread at the cap must not reach below the last uncapped interval")
	}
}

// A flat max(minWait, d/1.5) floor is close but not exact. At 300ms/1s the
// sequence runs 300ms, 450ms, 675ms, then saturates, so the floor must be 675ms
// while d/1.5 is only ~666ms. It is the case that distinguishes the replay from a
// flat scale-factor floor, and the place the arithmetic could quietly go wrong.
func TestApplyJitterAtCapFloorsCorrectlyVersusFlatScaleFloor(t *testing.T) {
	minWait := 300 * time.Millisecond
	maxWait := time.Second
	lastUncapped := 675 * time.Millisecond

	require.Equal(t, lastUncapped, calculateBackoff(2, minWait, maxWait),
		"the last interval before the cap")
	require.Equal(t, maxWait, calculateBackoff(3, minWait, maxWait),
		"the first interval at the cap")

	flatScaleFloor := time.Duration(float64(maxWait) / lockCheckBackoffScale)
	require.Less(t, flatScaleFloor, lastUncapped,
		"precondition: the flat floor must sit below the last uncapped interval")

	for i := 0; i < 2000; i++ {
		got := applyJitter(maxWait, minWait, maxWait)
		assert.GreaterOrEqual(t, got, lastUncapped,
			"a flat d/1.5 floor would allow ~666ms here, below what the previous attempt guaranteed")
		assert.LessOrEqual(t, got, maxWait,
			"the wait must never exceed the configured maximum")
	}
}

// A caller may configure minWait above maxWait; options.go accepts it, and
// calculateBackoff resolves it by returning maxWait. The downward spread must not
// then draw below the configured minimum. Flooring at half the interval would
// permit a third of it, because the replay loop cannot run when minWait is already
// past the cap.
func TestApplyJitterHonoursMinWaitAboveMaxWait(t *testing.T) {
	minWait := 90 * time.Second
	maxWait := 60 * time.Second

	d := calculateBackoff(0, minWait, maxWait)
	require.Equal(t, maxWait, d, "calculateBackoff resolves this configuration to the cap")

	for i := 0; i < 2000; i++ {
		got := applyJitter(d, minWait, maxWait)
		assert.Equal(t, maxWait, got,
			"with minWait past the cap the wait cannot be spread at all without breaking it")
	}
}

// Exercises the wiring at the acquireLocks call site rather than the helper alone.
// With minWait=10ms and 4 retries the 1.5× schedule is 10ms, 15ms, 22.5ms, 33.75ms
// before jitter. Call counts are asserted rather than wall-clock delay, because a
// busy CI box can add arbitrary scheduling latency on top.
func TestAcquireLocksRunsTheFullRetrySchedule(t *testing.T) {
	mockClient := new(mockHiveClient)
	ctx := context.Background()
	opts := NewHiveOptions()
	opts.LockMinWaitTime = 10 * time.Millisecond
	opts.LockMaxWaitTime = time.Second
	opts.LockRetries = 4

	mockClient.On("Lock", ctx, mock.AnythingOfType("*hive_metastore.LockRequest")).
		Return(&hive_metastore.LockResponse{
			Lockid: 901,
			State:  hive_metastore.LockState_WAITING,
		}, nil)
	mockClient.On("CheckLock", ctx, int64(901)).
		Return(&hive_metastore.LockResponse{
			Lockid: 901,
			State:  hive_metastore.LockState_WAITING,
		}, nil)
	mockClient.On("Unlock", mock.Anything, int64(901)).Return(nil)

	lock, err := acquireLocks(ctx, mockClient,
		[]tableLockIdentifier{{database: "testdb", table: "testtable"}}, opts)

	require.Error(t, err)
	require.Nil(t, lock)
	assert.ErrorIs(t, err, ErrLockAcquisitionFailed)

	// Counting calls rather than measuring elapsed time. The additive
	// never-shorter property is already covered by
	// TestApplyJitterBelowCapNeverShorterThanInput without touching the clock, and a
	// wall-clock bound is the kind of assertion that flakes on a loaded CI runner.
	mockClient.AssertNumberOfCalls(t, "CheckLock", opts.LockRetries)
	mockClient.AssertExpectations(t)
}

func TestApplyJitterRespectsMaxWait(t *testing.T) {
	minWait := 100 * time.Millisecond
	maxWait := 1 * time.Second

	for i := 0; i < 500; i++ {
		// d above the cap should not be inflated further.
		assert.Equal(t, 2*time.Second, applyJitter(2*time.Second, minWait, maxWait))
	}
}

func TestApplyJitterVaries(t *testing.T) {
	seen := make(map[time.Duration]struct{})
	for i := 0; i < 500; i++ {
		seen[applyJitter(time.Second, time.Millisecond, time.Minute)] = struct{}{}
	}

	// A deterministic implementation returns one value. The range here is a
	// second wide, so observing a single value across 500 draws is not chance.
	assert.Greater(t, len(seen), 1,
		"waits must vary so that clients contending for the same lock do not poll in lockstep")
}

func TestApplyJitterNonPositive(t *testing.T) {
	assert.Equal(t, time.Duration(0), applyJitter(0, time.Millisecond, time.Minute))
	assert.Equal(t, -time.Second, applyJitter(-time.Second, time.Millisecond, time.Minute))
}
