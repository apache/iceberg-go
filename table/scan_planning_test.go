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
	"testing"

	"github.com/apache/iceberg-go"
	icebergio "github.com/apache/iceberg-go/io"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestScanPlanningRemoteRequiresPlanner(t *testing.T) {
	t.Parallel()

	scan := &Scan{planningMode: ScanPlanningRemote}

	_, err := scan.PlanFiles(context.Background())
	require.ErrorIs(t, err, ErrInvalidOperation)
}

func TestScanPlanningRemoteStoresPlanIO(t *testing.T) {
	t.Parallel()

	pio := fakePlanIO{}
	planner := &fakeScanPlanner{
		result:   ScanPlanningResult{IO: pio},
		supports: true,
	}
	scan := &Scan{
		planner:      planner,
		planningMode: ScanPlanningRemote,
	}

	tasks, err := scan.PlanFiles(context.Background())
	require.NoError(t, err)
	assert.Empty(t, tasks)
	assert.Equal(t, pio, scan.planIO)
}

func TestScanPlanningRemoteClosesPreviousPlanIO(t *testing.T) {
	t.Parallel()

	first := &countingPlanIO{}
	second := &countingPlanIO{}
	planner := &sequenceScanPlanner{
		results: []ScanPlanningResult{{IO: first}, {IO: second}},
	}
	scan := &Scan{
		planner:      planner,
		planningMode: ScanPlanningRemote,
	}

	_, err := scan.PlanFiles(context.Background())
	require.NoError(t, err)
	_, err = scan.PlanFiles(context.Background())
	require.NoError(t, err)

	assert.Equal(t, 1, first.closeCalls)
	assert.Equal(t, 0, second.closeCalls)
	assert.Same(t, second, scan.planIO)
}

func TestReadTasksClosesPlanIOWhenLoadFails(t *testing.T) {
	want := errors.New("load plan io")
	pio := &countingPlanIO{loadErr: want}
	txn, _ := createTestTransactionWithMemIO(t, *iceberg.UnpartitionedSpec)
	scan, err := txn.Scan()
	require.NoError(t, err)
	scan.planIO = pio

	_, _, err = scan.ReadTasks(context.Background(), nil)
	require.ErrorIs(t, err, want)
	assert.Equal(t, 1, pio.closeCalls)
	assert.Nil(t, scan.planIO)
}

func TestScanPlanningRemoteRejectsIncapablePlanner(t *testing.T) {
	t.Parallel()

	scan := &Scan{
		planner:      &fakeScanPlanner{supports: false},
		planningMode: ScanPlanningRemote,
	}

	_, err := scan.PlanFiles(context.Background())
	require.ErrorIs(t, err, ErrInvalidOperation)
}

func TestScanPlanningRemotePropagatesPlannerError(t *testing.T) {
	t.Parallel()

	want := errors.New("planner boom")
	scan := &Scan{
		planner:      &fakeScanPlanner{supports: true, err: want},
		planningMode: ScanPlanningRemote,
	}

	_, err := scan.PlanFiles(context.Background())
	require.ErrorIs(t, err, want)
}

func TestScanPlanningRemoteRejectsConflictingSnapshotSelectors(t *testing.T) {
	t.Parallel()

	planner := &fakeScanPlanner{supports: true}
	scan := &Scan{
		planner:      planner,
		planningMode: ScanPlanningRemote,
	}
	WithSnapshotID(1000)(scan)
	WithSnapshotAsOf(2000)(scan)

	_, err := scan.PlanFiles(context.Background())
	require.ErrorIs(t, err, iceberg.ErrInvalidArgument)
	assert.False(t, planner.called)
}

func TestScanPlanningAutoUsesCapablePlanner(t *testing.T) {
	t.Parallel()

	scan := &Scan{
		planner:      &fakeScanPlanner{result: ScanPlanningResult{Tasks: []FileScanTask{{}}}, supports: true},
		planningMode: ScanPlanningAuto,
	}

	tasks, err := scan.PlanFiles(context.Background())
	require.NoError(t, err)
	assert.Len(t, tasks, 1)
}

func TestScanPlanningPassesIdentifierCopy(t *testing.T) {
	t.Parallel()

	scan := &Scan{
		planner: &fakeScanPlanner{
			result:   ScanPlanningResult{Tasks: []FileScanTask{{}}},
			supports: true,
		},
		planningMode:   ScanPlanningAuto,
		identifier:     Identifier{"db", "scan-copy-test"},
		selectedFields: []string{"*"},
	}

	tasks, err := scan.PlanFiles(context.Background())
	require.NoError(t, err)
	assert.Len(t, tasks, 1)

	planReq := scan.planner.(*fakeScanPlanner)
	planReq.receivedIdentifier[0] = "corrupt"
	assert.Equal(t, Identifier{"db", "scan-copy-test"}, scan.identifier)
}

func TestTransactionScanCopiesIdentifier(t *testing.T) {
	txn, _ := createTestTransactionWithMemIO(t, *iceberg.UnpartitionedSpec)
	txn.tbl.planner = &fakeScanPlanner{
		result:   ScanPlanningResult{Tasks: []FileScanTask{{}}},
		supports: true,
	}

	scan, err := txn.Scan(WithScanPlanningMode(ScanPlanningAuto))
	require.NoError(t, err)
	_, err = scan.PlanFiles(context.Background())
	require.NoError(t, err)

	scan.identifier[0] = "corrupt"
	assert.Equal(t, Identifier{"db", "tbl"}, txn.tbl.identifier)
}

func TestTransactionScanRejectsConflictingSnapshotSelectors(t *testing.T) {
	txn, _ := createTestTransactionWithMemIO(t, *iceberg.UnpartitionedSpec)

	scan, err := txn.Scan(WithSnapshotID(1000), WithSnapshotAsOf(2000))
	require.ErrorIs(t, err, iceberg.ErrInvalidArgument)
	assert.Nil(t, scan)
}

func TestScanPlanningUnknownModeErrors(t *testing.T) {
	t.Parallel()

	scan := &Scan{planningMode: ScanPlanningMode("bogus")}

	_, err := scan.PlanFiles(context.Background())
	require.ErrorIs(t, err, iceberg.ErrInvalidArgument)
}

type fakeScanPlanner struct {
	result   ScanPlanningResult
	supports bool
	err      error
	called   bool
	// captured after PlanFiles receives it
	receivedIdentifier Identifier
}

func (f *fakeScanPlanner) SupportsRemoteScanPlanning() bool { return f.supports }

func (f *fakeScanPlanner) PlanFiles(_ context.Context, req ScanPlanningRequest) (ScanPlanningResult, error) {
	f.called = true
	f.receivedIdentifier = req.Identifier

	return f.result, f.err
}

type fakePlanIO struct{}

func (fakePlanIO) Load(context.Context) (icebergio.IO, error) { return nil, nil }
func (fakePlanIO) Close() error                               { return nil }

type countingPlanIO struct {
	loadErr    error
	closeCalls int
}

func (p *countingPlanIO) Load(context.Context) (icebergio.IO, error) {
	return nil, p.loadErr
}

func (p *countingPlanIO) Close() error {
	p.closeCalls++
	return nil
}

type sequenceScanPlanner struct {
	results []ScanPlanningResult
	index   int
}

func (p *sequenceScanPlanner) SupportsRemoteScanPlanning() bool { return true }

func (p *sequenceScanPlanner) PlanFiles(context.Context, ScanPlanningRequest) (ScanPlanningResult, error) {
	result := p.results[p.index]
	p.index++
	return result, nil
}
