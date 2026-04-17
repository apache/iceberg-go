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
	"errors"
	"fmt"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	"github.com/apache/iceberg-go"
	iceio "github.com/apache/iceberg-go/io"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// flakyCatalog commits successfully only on a specified attempt number.
// Earlier attempts return the given error.
type flakyCatalog struct {
	metadata         Metadata
	failUntilAttempt int
	failWith         error
	attempts         atomic.Int32
}

func (c *flakyCatalog) LoadTable(ctx context.Context, ident Identifier) (*Table, error) {
	return nil, nil
}

func (c *flakyCatalog) CommitTable(ctx context.Context, ident Identifier, reqs []Requirement, updates []Update) (Metadata, string, error) {
	n := c.attempts.Add(1)
	if int(n) <= c.failUntilAttempt {
		return nil, "", c.failWith
	}

	meta, err := UpdateTableMetadata(c.metadata, updates, "")
	if err != nil {
		return nil, "", err
	}
	c.metadata = meta

	return meta, "", nil
}

func newRetryTestTable(t *testing.T, cat CatalogIO, props iceberg.Properties) *Table {
	t.Helper()

	location := filepath.ToSlash(t.TempDir())

	schema := iceberg.NewSchema(0,
		iceberg.NestedField{ID: 1, Name: "id", Type: iceberg.PrimitiveTypes.Int64, Required: true},
	)

	if props == nil {
		props = iceberg.Properties{}
	}
	props[PropertyFormatVersion] = "2"

	meta, err := NewMetadata(schema, iceberg.UnpartitionedSpec,
		UnsortedSortOrder, location, props)
	require.NoError(t, err)

	return New(
		Identifier{"db", "retry_test"},
		meta, location+"/metadata/v1.metadata.json",
		func(ctx context.Context) (iceio.IO, error) {
			return iceio.LocalFS{}, nil
		},
		cat,
	)
}

func TestDoCommit_SucceedsFirstTry(t *testing.T) {
	cat := &flakyCatalog{}
	tbl := newRetryTestTable(t, cat, nil)
	cat.metadata = tbl.Metadata()

	_, err := tbl.doCommit(t.Context(), nil, nil)
	require.NoError(t, err)
	assert.Equal(t, int32(1), cat.attempts.Load())
}

func TestDoCommit_RetriesOnCommitFailed(t *testing.T) {
	cat := &flakyCatalog{
		failUntilAttempt: 2,
		failWith:         fmt.Errorf("REST: %w", ErrCommitFailed),
	}
	tbl := newRetryTestTable(t, cat, iceberg.Properties{
		CommitNumRetriesKey:     "4",
		CommitMinRetryWaitMsKey: "1",
		CommitMaxRetryWaitMsKey: "2",
	})
	cat.metadata = tbl.Metadata()

	_, err := tbl.doCommit(t.Context(), nil, nil)
	require.NoError(t, err)
	assert.Equal(t, int32(3), cat.attempts.Load(), "should retry 2x then succeed on 3rd")
}

func TestDoCommit_GivesUpAfterMaxRetries(t *testing.T) {
	cat := &flakyCatalog{
		failUntilAttempt: 100,
		failWith:         fmt.Errorf("REST: %w", ErrCommitFailed),
	}
	tbl := newRetryTestTable(t, cat, iceberg.Properties{
		CommitNumRetriesKey:     "2",
		CommitMinRetryWaitMsKey: "1",
		CommitMaxRetryWaitMsKey: "2",
	})
	cat.metadata = tbl.Metadata()

	_, err := tbl.doCommit(t.Context(), nil, nil)
	require.Error(t, err)
	assert.ErrorIs(t, err, ErrCommitFailed)
	// 1 initial + 2 retries = 3 attempts
	assert.Equal(t, int32(3), cat.attempts.Load())
}

func TestDoCommit_DoesNotRetryUnknownStateError(t *testing.T) {
	unknownErr := errors.New("500 internal server error")
	cat := &flakyCatalog{
		failUntilAttempt: 5,
		failWith:         unknownErr,
	}
	tbl := newRetryTestTable(t, cat, iceberg.Properties{
		CommitNumRetriesKey:     "10",
		CommitMinRetryWaitMsKey: "1",
		CommitMaxRetryWaitMsKey: "2",
	})
	cat.metadata = tbl.Metadata()

	_, err := tbl.doCommit(t.Context(), nil, nil)
	require.Error(t, err)
	assert.ErrorIs(t, err, unknownErr)
	// Must not retry — unknown state could mean the commit actually succeeded.
	assert.Equal(t, int32(1), cat.attempts.Load())
}

func TestDoCommit_DoesNotRetryUnrelatedError(t *testing.T) {
	otherErr := errors.New("network unreachable")
	cat := &flakyCatalog{
		failUntilAttempt: 5,
		failWith:         otherErr,
	}
	tbl := newRetryTestTable(t, cat, iceberg.Properties{
		CommitNumRetriesKey:     "10",
		CommitMinRetryWaitMsKey: "1",
		CommitMaxRetryWaitMsKey: "2",
	})
	cat.metadata = tbl.Metadata()

	_, err := tbl.doCommit(t.Context(), nil, nil)
	require.Error(t, err)
	assert.ErrorIs(t, err, otherErr)
	assert.Equal(t, int32(1), cat.attempts.Load())
}

func TestDoCommit_RespectsContextCancellation(t *testing.T) {
	cat := &flakyCatalog{
		failUntilAttempt: 100,
		failWith:         fmt.Errorf("REST: %w", ErrCommitFailed),
	}
	tbl := newRetryTestTable(t, cat, iceberg.Properties{
		CommitNumRetriesKey:     "10",
		CommitMinRetryWaitMsKey: "50",
		CommitMaxRetryWaitMsKey: "200",
	})
	cat.metadata = tbl.Metadata()

	ctx, cancel := context.WithTimeout(t.Context(), 30*time.Millisecond)
	defer cancel()

	_, err := tbl.doCommit(ctx, nil, nil)
	require.Error(t, err)
	// Either the commit error bubbles up or context cancellation does.
	// Both are acceptable outcomes — the test just verifies we don't hang.
	assert.True(t, errors.Is(err, context.DeadlineExceeded) || errors.Is(err, ErrCommitFailed))
	assert.Less(t, cat.attempts.Load(), int32(10), "should stop retrying after context cancels")
}

func TestDoCommit_ZeroRetriesOnlyOneAttempt(t *testing.T) {
	cat := &flakyCatalog{
		failUntilAttempt: 5,
		failWith:         fmt.Errorf("REST: %w", ErrCommitFailed),
	}
	tbl := newRetryTestTable(t, cat, iceberg.Properties{
		CommitNumRetriesKey:     "0",
		CommitMinRetryWaitMsKey: "1",
		CommitMaxRetryWaitMsKey: "2",
	})
	cat.metadata = tbl.Metadata()

	_, err := tbl.doCommit(t.Context(), nil, nil)
	require.Error(t, err)
	assert.Equal(t, int32(1), cat.attempts.Load())
}

func TestBackoffDuration_ExponentialWithJitter(t *testing.T) {
	const minMs, maxMs = 100, 60000
	minWait := time.Duration(minMs) * time.Millisecond

	// Attempt 0: cap == minMs, wait is exactly minMs.
	for range 20 {
		d := backoffDuration(0, minMs, maxMs)
		assert.Equal(t, minWait, d)
	}

	// Attempt 3: cap = 800ms (100 << 3), wait in [minMs, 800ms].
	for range 20 {
		d := backoffDuration(3, minMs, maxMs)
		assert.GreaterOrEqual(t, d, minWait)
		assert.LessOrEqual(t, d, 800*time.Millisecond)
	}

	// Attempt 20: overflow protection, wait in [minMs, maxMs].
	for range 20 {
		d := backoffDuration(20, minMs, maxMs)
		assert.GreaterOrEqual(t, d, minWait)
		assert.LessOrEqual(t, d, time.Duration(maxMs)*time.Millisecond)
	}
}

func TestBackoffDuration_HandlesZeroOrNegativeInputs(t *testing.T) {
	// Should fall back to defaults rather than panic or return garbage.
	d := backoffDuration(0, 0, 0)
	assert.Equal(t, time.Duration(CommitMinRetryWaitMsDefault)*time.Millisecond, d)

	d = backoffDuration(0, -1, -1)
	assert.Equal(t, time.Duration(CommitMinRetryWaitMsDefault)*time.Millisecond, d)

	// Very large attempt counts must not panic on shift; clamps to maxMs.
	d = backoffDuration(100, 100, 60000)
	assert.GreaterOrEqual(t, d, 100*time.Millisecond)
	assert.LessOrEqual(t, d, 60000*time.Millisecond)
}
