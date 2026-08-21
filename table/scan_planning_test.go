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

	scan := &Scan{metadata: scanTestMetadata(t), planningMode: ScanPlanningRemote}

	_, err := scan.PlanFiles(context.Background())
	require.ErrorIs(t, err, ErrInvalidOperation)
}

func TestScanPlanningRemoteStoresPlanIO(t *testing.T) {
	t.Parallel()

	pio := &fakePlanIO{}
	planner := &fakeScanPlanner{
		result:   ScanPlanningResult{IO: pio},
		supports: true,
	}
	scan := &Scan{
		metadata:     scanTestMetadata(t),
		planner:      planner,
		planningMode: ScanPlanningRemote,
	}

	tasks, err := scan.PlanFiles(context.Background())
	require.NoError(t, err)
	assert.Empty(t, tasks)
	assert.Same(t, pio, scan.planIO.io)
}

func TestScanPlanningRemoteClosesPreviousPlanIO(t *testing.T) {
	t.Parallel()

	first := &countingPlanIO{}
	second := &countingPlanIO{}
	planner := &sequenceScanPlanner{
		results: []ScanPlanningResult{{IO: first}, {IO: second}},
	}
	scan := &Scan{
		metadata:     scanTestMetadata(t),
		planner:      planner,
		planningMode: ScanPlanningRemote,
	}

	_, err := scan.PlanFiles(context.Background())
	require.NoError(t, err)
	_, err = scan.PlanFiles(context.Background())
	require.NoError(t, err)

	assert.Equal(t, 1, first.closeCalls)
	assert.Equal(t, 0, second.closeCalls)
	assert.Same(t, second, scan.planIO.io)
}

func TestRefinedScanRetainsPlanIOOwnership(t *testing.T) {
	t.Parallel()

	tests := map[string]func(*Scan) (*Scan, error){
		"row limit": func(scan *Scan) (*Scan, error) {
			return scan.UseRowLimit(10), nil
		},
		"main ref": func(scan *Scan) (*Scan, error) {
			return scan.UseRef(MainBranch)
		},
	}

	for name, refine := range tests {
		t.Run(name, func(t *testing.T) {
			first := &countingPlanIO{}
			second := &countingPlanIO{}
			planner := &sequenceScanPlanner{
				results: []ScanPlanningResult{{IO: first}, {IO: second}},
			}
			scan := &Scan{
				metadata:     scanTestMetadata(t),
				planner:      planner,
				planningMode: ScanPlanningRemote,
			}

			_, err := scan.PlanFiles(context.Background())
			require.NoError(t, err)

			refined, err := refine(scan)
			require.NoError(t, err)
			assert.Same(t, scan.planIO, refined.planIO)

			_, err = refined.PlanFiles(context.Background())
			require.NoError(t, err)
			assert.Equal(t, 0, first.closeCalls)
			assert.Same(t, first, scan.planIO.io)
			assert.Same(t, second, refined.planIO.io)

			scan.closePlanIO()
			refined.closePlanIO()
			assert.Equal(t, 1, first.closeCalls)
			assert.Equal(t, 1, second.closeCalls)
		})
	}
}

func TestScanPlanningRemoteFailurePreservesPreviousPlanIO(t *testing.T) {
	t.Parallel()

	want := errors.New("replacement plan")
	first := &countingPlanIO{}
	planner := &sequenceScanPlanner{
		results: []ScanPlanningResult{{IO: first}, {}},
		errors:  []error{nil, want},
	}
	scan := &Scan{
		metadata:     scanTestMetadata(t),
		planner:      planner,
		planningMode: ScanPlanningRemote,
	}

	_, err := scan.PlanFiles(context.Background())
	require.NoError(t, err)
	_, err = scan.PlanFiles(context.Background())
	require.ErrorIs(t, err, want)

	assert.Equal(t, 0, first.closeCalls)
	assert.Same(t, first, scan.planIO.io)
}

func TestScanPlanningRemoteKeepsSamePlanIO(t *testing.T) {
	t.Parallel()

	pio := &countingPlanIO{}
	planner := &sequenceScanPlanner{
		results: []ScanPlanningResult{{IO: pio}, {IO: pio}},
	}
	scan := &Scan{metadata: scanTestMetadata(t), planner: planner, planningMode: ScanPlanningRemote}

	_, err := scan.PlanFiles(context.Background())
	require.NoError(t, err)
	state := scan.planIO
	_, err = scan.PlanFiles(context.Background())
	require.NoError(t, err)

	assert.Same(t, state, scan.planIO)
	assert.Equal(t, 0, pio.closeCalls)
}

func TestScanPlanningRemoteRejectsNonComparablePlanIO(t *testing.T) {
	t.Parallel()

	pio := &countingPlanIO{}
	planner := &sequenceScanPlanner{
		results: []ScanPlanningResult{{IO: pio}, {IO: slicePlanIO{1, 2, 3}}},
	}
	scan := &Scan{metadata: scanTestMetadata(t), planner: planner, planningMode: ScanPlanningRemote}

	_, err := scan.PlanFiles(context.Background())
	require.NoError(t, err)
	state := scan.planIO
	_, err = scan.PlanFiles(context.Background())
	require.ErrorIs(t, err, iceberg.ErrInvalidArgument)

	assert.Same(t, state, scan.planIO)
	assert.Equal(t, 0, pio.closeCalls)
}

func TestReadTasksRetainsPlanIOWhenLoadFails(t *testing.T) {
	want := errors.New("load plan io")
	pio := &countingPlanIO{
		loadErrs: []error{want, nil},
		fs:       icebergio.LocalFS{},
	}
	txn, _ := createTestTransactionWithMemIO(t, *iceberg.UnpartitionedSpec)
	scan, err := txn.Scan()
	require.NoError(t, err)
	scan.planIO = mustPlanIOState(t, pio)
	scan.ioF = func(context.Context) (icebergio.IO, error) {
		return nil, errors.New("table IO must not be used")
	}

	_, _, err = scan.ReadTasks(context.Background(), nil)
	require.ErrorIs(t, err, want)
	assert.Equal(t, 0, pio.closeCalls)
	assert.NotNil(t, scan.planIO)

	_, records, err := scan.ReadTasks(context.Background(), nil)
	require.NoError(t, err)
	for _, err := range records {
		require.NoError(t, err)
	}
	assert.Equal(t, 2, pio.loadCalls)
	assert.Equal(t, 0, pio.closeCalls)
}

func TestReadTasksReusesPlanIO(t *testing.T) {
	pio := &countingPlanIO{fs: icebergio.LocalFS{}}
	txn, _ := createTestTransactionWithMemIO(t, *iceberg.UnpartitionedSpec)
	scan, err := txn.Scan()
	require.NoError(t, err)
	scan.planIO = mustPlanIOState(t, pio)
	scan.ioF = func(context.Context) (icebergio.IO, error) {
		return nil, errors.New("table IO must not be used")
	}

	for range 2 {
		_, records, err := scan.ReadTasks(context.Background(), nil)
		require.NoError(t, err)
		for _, err := range records {
			require.NoError(t, err)
		}
	}

	assert.Equal(t, 2, pio.loadCalls)
	assert.Equal(t, 0, pio.closeCalls)
	assert.Same(t, pio, scan.planIO.io)
}

func TestReadTasksKeepsRetiredPlanIOAliveForIterator(t *testing.T) {
	first := &countingPlanIO{fs: icebergio.LocalFS{}}
	second := &countingPlanIO{}
	planner := &sequenceScanPlanner{
		results: []ScanPlanningResult{{IO: first}, {IO: second}},
	}
	txn, _ := createTestTransactionWithMemIO(t, *iceberg.UnpartitionedSpec)
	scan, err := txn.Scan(WithScanPlanningMode(ScanPlanningRemote))
	require.NoError(t, err)
	scan.planner = planner

	tasks, err := scan.PlanFiles(context.Background())
	require.NoError(t, err)
	_, records, err := scan.ReadTasks(context.Background(), tasks)
	require.NoError(t, err)
	assert.Equal(t, 0, first.closeCalls)

	_, err = scan.PlanFiles(context.Background())
	require.NoError(t, err)
	assert.Equal(t, 0, first.closeCalls)
	assert.Same(t, second, scan.planIO.io)

	for _, err := range records {
		require.NoError(t, err)
	}
	assert.Equal(t, 1, first.closeCalls)
	assert.Equal(t, 0, second.closeCalls)
}

func TestScanPlanningLocalClosesPreviousPlanIOAfterSuccess(t *testing.T) {
	pio := &countingPlanIO{}
	txn, _ := createTestTransactionWithMemIO(t, *iceberg.UnpartitionedSpec)
	scan, err := txn.Scan()
	require.NoError(t, err)
	scan.planIO = mustPlanIOState(t, pio)

	_, err = scan.PlanFiles(context.Background())
	require.NoError(t, err)

	assert.Nil(t, scan.planIO)
	assert.Equal(t, 1, pio.closeCalls)
}

func TestScanPlanningRemoteRejectsIncapablePlanner(t *testing.T) {
	t.Parallel()

	scan := &Scan{
		metadata:     scanTestMetadata(t),
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
		metadata:     scanTestMetadata(t),
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
		metadata:     scanTestMetadata(t),
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
		metadata:     scanTestMetadata(t),
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
		metadata: scanTestMetadata(t),
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
	planReq.receivedRequest.Identifier[0] = "corrupt"
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

func TestScanPlanningRemoteSendsCurrentSchema(t *testing.T) {
	t.Parallel()

	meta := scanTestMetadata(t)
	planner := &fakeScanPlanner{supports: true}
	scan := &Scan{
		metadata:     meta,
		planner:      planner,
		planningMode: ScanPlanningRemote,
	}

	_, err := scan.PlanFiles(context.Background())
	require.NoError(t, err)

	got := planner.receivedRequest
	require.NotNil(t, got.Schema)
	assert.Equal(t, meta.CurrentSchema().ID, got.Schema.ID)
	require.NotNil(t, got.UseSnapshotSchema)
	assert.False(t, *got.UseSnapshotSchema)
}

func TestScanPlanningRemoteSendsSnapshotSchema(t *testing.T) {
	t.Parallel()

	meta := scanTestMetadata(t)
	old := iceberg.NewSchema(9,
		iceberg.NestedField{ID: 1, Name: "id", Type: iceberg.PrimitiveTypes.Int64, Required: true},
	)
	snapshotID := int64(42)
	schemaID := 9
	pinned := &planningSnapshotMetadata{
		Metadata: meta,
		extra:    old,
		snapshot: &Snapshot{SnapshotID: snapshotID, SchemaID: &schemaID},
	}

	planner := &fakeScanPlanner{supports: true}
	scan := &Scan{
		metadata:     pinned,
		planner:      planner,
		planningMode: ScanPlanningRemote,
		snapshotID:   &snapshotID,
	}

	_, err := scan.PlanFiles(context.Background())
	require.NoError(t, err)

	got := planner.receivedRequest
	require.NotNil(t, got.Schema)
	assert.Equal(t, schemaID, got.Schema.ID)
	require.NotNil(t, got.UseSnapshotSchema)
	assert.True(t, *got.UseSnapshotSchema)
}

// planningSnapshotMetadata pins one snapshot to an older schema, so a scan can
// resolve a schema other than the current one.
type planningSnapshotMetadata struct {
	Metadata
	extra    *iceberg.Schema
	snapshot *Snapshot
}

func (m *planningSnapshotMetadata) Schemas() []*iceberg.Schema {
	return append(m.Metadata.Schemas(), m.extra)
}

func (m *planningSnapshotMetadata) SnapshotByID(id int64) *Snapshot {
	if m.snapshot != nil && m.snapshot.SnapshotID == id {
		return m.snapshot
	}

	return m.Metadata.SnapshotByID(id)
}

func TestScanPlanningRemoteRejectsLineageSequenceNumber(t *testing.T) {
	t.Parallel()

	// The REST FileScanTask schema carries no data sequence number, so a remote
	// plan cannot supply _last_updated_sequence_number. Rejecting beats handing
	// back nulls where a local scan returns values.

	for name, opt := range map[string]ScanOption{
		"explicit column": func(scan *Scan) {
			scan.selectedFields = []string{"id", iceberg.LastUpdatedSequenceNumberColumnName}
		},
		"row lineage option": WithRowLineage(),
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			planner := &fakeScanPlanner{supports: true}
			scan := &Scan{
				metadata:      scanTestMetadata(t),
				planner:       planner,
				planningMode:  ScanPlanningRemote,
				caseSensitive: true,
			}
			opt(scan)

			_, err := scan.PlanFiles(context.Background())
			require.ErrorIs(t, err, ErrInvalidOperation)
			assert.Contains(t, err.Error(), iceberg.LastUpdatedSequenceNumberColumnName)
			assert.False(t, planner.called, "must fail before reaching the planner")
		})
	}
}

func TestScanPlanningRemoteAllowsRowID(t *testing.T) {
	t.Parallel()

	// _row_id is unaffected: first_row_id rides on the data file, so it survives
	// the wire.

	planner := &fakeScanPlanner{supports: true}
	scan := &Scan{
		metadata:       scanTestMetadata(t),
		planner:        planner,
		planningMode:   ScanPlanningRemote,
		caseSensitive:  true,
		selectedFields: []string{"id", iceberg.RowIDColumnName},
	}

	_, err := scan.PlanFiles(context.Background())
	require.NoError(t, err)
	assert.True(t, planner.called)
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
	receivedRequest ScanPlanningRequest
}

func (f *fakeScanPlanner) SupportsRemoteScanPlanning() bool { return f.supports }

func (f *fakeScanPlanner) PlanFiles(_ context.Context, req ScanPlanningRequest) (ScanPlanningResult, error) {
	f.called = true
	f.receivedRequest = req

	return f.result, f.err
}

type fakePlanIO struct{}

func (fakePlanIO) Load(context.Context) (icebergio.IO, error) { return nil, nil }
func (fakePlanIO) Close() error                               { return nil }

type countingPlanIO struct {
	loadErrs   []error
	fs         icebergio.IO
	loadCalls  int
	closeCalls int
}

func (p *countingPlanIO) Load(context.Context) (icebergio.IO, error) {
	p.loadCalls++
	if p.loadCalls <= len(p.loadErrs) {
		return p.fs, p.loadErrs[p.loadCalls-1]
	}

	return p.fs, nil
}

func (p *countingPlanIO) Close() error {
	p.closeCalls++

	return nil
}

type sequenceScanPlanner struct {
	results []ScanPlanningResult
	errors  []error
	index   int
}

func (p *sequenceScanPlanner) SupportsRemoteScanPlanning() bool { return true }

func (p *sequenceScanPlanner) PlanFiles(context.Context, ScanPlanningRequest) (ScanPlanningResult, error) {
	result := p.results[p.index]
	var err error
	if p.index < len(p.errors) {
		err = p.errors[p.index]
	}
	p.index++

	return result, err
}

type slicePlanIO []int

func (slicePlanIO) Load(context.Context) (icebergio.IO, error) { return nil, nil }
func (slicePlanIO) Close() error                               { return nil }

func mustPlanIOState(t *testing.T, planIO PlanIO) *planIOState {
	t.Helper()

	state, err := newPlanIOState(planIO)
	require.NoError(t, err)

	return state
}
