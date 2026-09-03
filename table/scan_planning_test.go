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
	"time"

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

	pio := &fakePlanIO{}
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

			require.NoError(t, scan.Close())
			require.NoError(t, refined.Close())
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
	scan := &Scan{planner: planner, planningMode: ScanPlanningRemote}

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
	scan := &Scan{planner: planner, planningMode: ScanPlanningRemote}

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

func TestScanPlanningLocalPropagatesPlanIOCloseError(t *testing.T) {
	closeErr := errors.New("close plan io")
	pio := &countingPlanIO{closeErr: closeErr}
	txn, _ := createTestTransactionWithMemIO(t, *iceberg.UnpartitionedSpec)
	scan, err := txn.Scan()
	require.NoError(t, err)
	scan.planIO = mustPlanIOState(t, pio)

	_, err = scan.PlanFiles(context.Background())
	require.ErrorIs(t, err, closeErr)
	assert.Nil(t, scan.planIO)
	assert.Equal(t, 1, pio.closeCalls)
}

func TestScanCloseReleasesPlanIO(t *testing.T) {
	t.Parallel()

	pio := &countingPlanIO{}
	scan := &Scan{
		planner:      &fakeScanPlanner{result: ScanPlanningResult{IO: pio}, supports: true},
		planningMode: ScanPlanningRemote,
	}

	_, err := scan.PlanFiles(context.Background())
	require.NoError(t, err)
	require.NoError(t, scan.Close())
	require.NoError(t, scan.Close())

	assert.Equal(t, 1, pio.closeCalls)
	assert.Nil(t, scan.planIO)

	_, err = scan.PlanFiles(context.Background())
	require.ErrorIs(t, err, ErrInvalidOperation)
}

func TestScanCloseWaitsForActiveReadTasks(t *testing.T) {
	t.Parallel()

	pio := &countingPlanIO{fs: icebergio.LocalFS{}}
	txn, _ := createTestTransactionWithMemIO(t, *iceberg.UnpartitionedSpec)
	scan, err := txn.Scan()
	require.NoError(t, err)
	scan.planIO = mustPlanIOState(t, pio)

	_, records, err := scan.ReadTasks(context.Background(), nil)
	require.NoError(t, err)
	closeDone := make(chan error, 1)
	go func() { closeDone <- scan.Close() }()
	assert.Equal(t, 0, pio.closeCalls)

	for _, err := range records {
		require.NoError(t, err)
	}
	require.NoError(t, <-closeDone)
	assert.Equal(t, 1, pio.closeCalls)
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

func TestScanPlanningRemoteResolvesDefaultProjectionAndSchema(t *testing.T) {
	t.Parallel()

	metadata, err := createTestMetadata(nil, nil)
	require.NoError(t, err)
	planner := &fakeScanPlanner{supports: true}
	scan := (&Table{metadata: metadata}).Scan(
		WithScanPlanningMode(ScanPlanningRemote),
		WithMaxConcurrency(3),
	)
	scan.planner = planner

	_, err = scan.PlanFiles(context.Background())
	require.NoError(t, err)
	assert.Equal(t, []string{"id"}, planner.receivedRequest.SelectedFields)
	assert.Equal(t, 3, planner.receivedRequest.MaxConcurrency)
	assert.True(t, planner.receivedRequest.Schema.Equals(metadata.CurrentSchema()))
	require.NotNil(t, planner.receivedRequest.UseSnapshotSchema)
	assert.False(t, *planner.receivedRequest.UseSnapshotSchema)
	assert.Nil(t, planner.receivedRequest.MinRowsRequested)
}

func TestScanPlanningRemoteOmitsNegativeRowLimit(t *testing.T) {
	t.Parallel()

	planner := &fakeScanPlanner{supports: true}
	scan := &Scan{
		planner:        planner,
		planningMode:   ScanPlanningRemote,
		selectedFields: []string{"*"},
		limit:          -2,
	}

	_, err := scan.PlanFiles(context.Background())
	require.NoError(t, err)
	assert.Nil(t, planner.receivedRequest.MinRowsRequested)
}

func TestScanPlanningRemotePropagatesSnapshotSchemaSemantics(t *testing.T) {
	t.Parallel()

	snapshotTime := time.Now().UnixMilli()
	schemaID := 0
	metadata, err := createTestMetadata([]Snapshot{{
		SnapshotID:  10,
		TimestampMs: snapshotTime,
		SchemaID:    &schemaID,
	}}, []SnapshotLogEntry{{SnapshotID: 10, TimestampMs: snapshotTime}})
	require.NoError(t, err)
	builder, err := MetadataBuilderFromBase(metadata, "")
	require.NoError(t, err)
	currentSchema := iceberg.NewSchema(1,
		iceberg.NestedField{ID: 1, Name: "id", Type: iceberg.PrimitiveTypes.Int64, Required: true},
		iceberg.NestedField{ID: 2, Name: "category", Type: iceberg.PrimitiveTypes.String},
	)
	require.NoError(t, builder.AddSchema(currentSchema))
	require.NoError(t, builder.SetCurrentSchemaID(-1))
	require.NoError(t, builder.SetSnapshotRef("branch", 10, BranchRef))
	require.NoError(t, builder.SetSnapshotRef("tag", 10, TagRef))
	metadata, err = builder.Build()
	require.NoError(t, err)

	var snapshotSchema *iceberg.Schema
	for _, schema := range metadata.Schemas() {
		if schema.ID == schemaID {
			snapshotSchema = schema

			break
		}
	}
	require.NotNil(t, snapshotSchema)
	require.False(t, snapshotSchema.Equals(metadata.CurrentSchema()))

	base := (&Table{metadata: metadata}).Scan(WithScanPlanningMode(ScanPlanningRemote))
	livePlanner := &fakeScanPlanner{supports: true}
	base.planner = livePlanner
	_, err = base.PlanFiles(context.Background())
	require.NoError(t, err)
	assert.True(t, livePlanner.receivedRequest.Schema.Equals(metadata.CurrentSchema()))
	assert.Equal(t, []string{"id", "category"}, livePlanner.receivedRequest.SelectedFields)

	branch, err := base.UseRef("branch")
	require.NoError(t, err)
	branchPlanner := &fakeScanPlanner{supports: true}
	branch.planner = branchPlanner
	_, err = branch.PlanFiles(context.Background())
	require.NoError(t, err)
	require.NotNil(t, branchPlanner.receivedRequest.UseSnapshotSchema)
	assert.False(t, *branchPlanner.receivedRequest.UseSnapshotSchema)
	assert.True(t, branchPlanner.receivedRequest.Schema.Equals(metadata.CurrentSchema()))
	assert.Equal(t, []string{"id", "category"}, branchPlanner.receivedRequest.SelectedFields)

	tag, err := base.UseRef("tag")
	require.NoError(t, err)
	tagPlanner := &fakeScanPlanner{supports: true}
	tag.planner = tagPlanner
	_, err = tag.PlanFiles(context.Background())
	require.NoError(t, err)
	require.NotNil(t, tagPlanner.receivedRequest.UseSnapshotSchema)
	assert.True(t, *tagPlanner.receivedRequest.UseSnapshotSchema)
	assert.True(t, tagPlanner.receivedRequest.Schema.Equals(snapshotSchema))
	assert.Equal(t, []string{"id"}, tagPlanner.receivedRequest.SelectedFields)

	historical := (&Table{metadata: metadata}).Scan(
		WithScanPlanningMode(ScanPlanningRemote),
		WithSnapshotID(10),
		WithLimit(25),
	)
	historicalPlanner := &fakeScanPlanner{supports: true}
	historical.planner = historicalPlanner
	_, err = historical.PlanFiles(context.Background())
	require.NoError(t, err)
	require.NotNil(t, historicalPlanner.receivedRequest.UseSnapshotSchema)
	assert.True(t, *historicalPlanner.receivedRequest.UseSnapshotSchema)
	assert.True(t, historicalPlanner.receivedRequest.Schema.Equals(snapshotSchema))
	assert.Equal(t, []string{"id"}, historicalPlanner.receivedRequest.SelectedFields)
	require.NotNil(t, historicalPlanner.receivedRequest.MinRowsRequested)
	assert.Equal(t, int64(25), *historicalPlanner.receivedRequest.MinRowsRequested)

	asOf := (&Table{metadata: metadata}).Scan(
		WithScanPlanningMode(ScanPlanningRemote),
		WithSnapshotAsOf(snapshotTime),
	)
	asOfPlanner := &fakeScanPlanner{supports: true}
	asOf.planner = asOfPlanner
	_, err = asOf.PlanFiles(context.Background())
	require.NoError(t, err)
	require.NotNil(t, asOfPlanner.receivedRequest.UseSnapshotSchema)
	assert.True(t, *asOfPlanner.receivedRequest.UseSnapshotSchema)
	assert.True(t, asOfPlanner.receivedRequest.Schema.Equals(snapshotSchema))
	assert.Equal(t, []string{"id"}, asOfPlanner.receivedRequest.SelectedFields)
}

func TestScanPlanningRemoteRejectsLastUpdatedSequenceNumber(t *testing.T) {
	t.Parallel()

	planner := &fakeScanPlanner{supports: true}
	scan := &Scan{
		planner:        planner,
		planningMode:   ScanPlanningRemote,
		selectedFields: []string{iceberg.LastUpdatedSequenceNumberColumnName},
		caseSensitive:  true,
	}

	_, err := scan.PlanFiles(context.Background())
	require.ErrorIs(t, err, ErrInvalidOperation)
	assert.Empty(t, planner.receivedIdentifier)
}

func TestScanPlanningAutoFallsBackForLastUpdatedSequenceNumber(t *testing.T) {
	t.Parallel()

	metadata, err := createTestMetadata(nil, nil)
	require.NoError(t, err)
	planner := &fakeScanPlanner{supports: true}
	scan := (&Table{metadata: metadata}).Scan(
		WithScanPlanningMode(ScanPlanningAuto),
		WithRowLineage(),
	)
	scan.planner = planner

	tasks, err := scan.PlanFiles(context.Background())
	require.NoError(t, err)
	assert.Empty(t, tasks)
	assert.Empty(t, planner.receivedIdentifier)
}

func TestScanPlanningRemotePassesFileIOProperties(t *testing.T) {
	t.Parallel()

	metadata, err := createTestMetadata(nil, nil)
	require.NoError(t, err)
	props := iceberg.Properties{"s3.endpoint": "https://table.local"}
	planner := &fakeScanPlanner{supports: true}
	tbl := New(
		Identifier{"db", "tbl"},
		metadata,
		"s3://bucket/db/tbl/metadata/v1.json",
		nil,
		nil,
		WithScanPlanningIOProperties(props),
	)
	tbl.planner = planner

	_, err = tbl.Scan(WithScanPlanningMode(ScanPlanningRemote)).PlanFiles(context.Background())
	require.NoError(t, err)
	assert.Equal(t, props, planner.receivedRequest.FileIOProperties)

	planner.receivedRequest.FileIOProperties["s3.endpoint"] = "https://mutated.local"
	assert.Equal(t, "https://table.local", props["s3.endpoint"])
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

	scan, err := txn.Scan()
	require.NoError(t, err)
	scan.identifier[0] = "corrupt"
	assert.Equal(t, Identifier{"db", "tbl"}, txn.tbl.identifier)
}

func TestTransactionScanKeepsStagedMetadataLocal(t *testing.T) {
	txn, _ := createTestTransactionWithMemIO(t, *iceberg.UnpartitionedSpec)
	planner := &fakeScanPlanner{
		result:   ScanPlanningResult{Tasks: []FileScanTask{{}}},
		supports: true,
	}
	txn.tbl.planner = planner
	dataFile := newTestDataFile(
		t,
		*iceberg.UnpartitionedSpec,
		"mem://default/table-location/data.parquet",
		nil,
	)
	require.NoError(t, txn.AddDataFiles(context.Background(), []iceberg.DataFile{dataFile}, nil))

	scan, err := txn.Scan(WithScanPlanningMode(ScanPlanningAuto))
	require.NoError(t, err)
	tasks, err := scan.PlanFiles(context.Background())
	require.NoError(t, err)
	require.Len(t, tasks, 1)
	assert.Equal(t, dataFile.FilePath(), tasks[0].File.FilePath())
	assert.Empty(t, planner.receivedIdentifier)

	remote, err := txn.Scan(WithScanPlanningMode(ScanPlanningRemote))
	require.NoError(t, err)
	_, err = remote.PlanFiles(context.Background())
	require.ErrorIs(t, err, ErrInvalidOperation)
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
	receivedRequest    ScanPlanningRequest
}

func (f *fakeScanPlanner) SupportsRemoteScanPlanning() bool { return f.supports }

func (f *fakeScanPlanner) PlanFiles(_ context.Context, req ScanPlanningRequest) (ScanPlanningResult, error) {
	f.called = true
	f.receivedIdentifier = req.Identifier
	f.receivedRequest = req

	return f.result, f.err
}

type fakePlanIO struct{}

func (fakePlanIO) Load(context.Context) (icebergio.IO, error) { return nil, nil }
func (fakePlanIO) Close() error                               { return nil }

type countingPlanIO struct {
	loadErrs   []error
	fs         icebergio.IO
	closeErr   error
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

	return p.closeErr
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
