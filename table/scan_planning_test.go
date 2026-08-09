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
	)
	scan.planner = planner

	_, err = scan.PlanFiles(context.Background())
	require.NoError(t, err)
	assert.Equal(t, []string{"id"}, planner.receivedRequest.SelectedFields)
	assert.True(t, planner.receivedRequest.Schema.Equals(metadata.CurrentSchema()))
	require.NotNil(t, planner.receivedRequest.UseSnapshotSchema)
	assert.False(t, *planner.receivedRequest.UseSnapshotSchema)
	assert.Nil(t, planner.receivedRequest.MinRowsRequested)
}

func TestScanPlanningRemotePropagatesSnapshotSchemaSemantics(t *testing.T) {
	t.Parallel()

	schemaID := 0
	metadata, err := createTestMetadata([]Snapshot{{
		SnapshotID:  10,
		TimestampMs: time.Now().Add(time.Hour).UnixMilli(),
		SchemaID:    &schemaID,
	}}, nil)
	require.NoError(t, err)
	builder, err := MetadataBuilderFromBase(metadata, "")
	require.NoError(t, err)
	require.NoError(t, builder.SetSnapshotRef("branch", 10, BranchRef))
	require.NoError(t, builder.SetSnapshotRef("tag", 10, TagRef))
	metadata, err = builder.Build()
	require.NoError(t, err)

	base := (&Table{metadata: metadata}).Scan(WithScanPlanningMode(ScanPlanningRemote))

	branch, err := base.UseRef("branch")
	require.NoError(t, err)
	branchPlanner := &fakeScanPlanner{supports: true}
	branch.planner = branchPlanner
	_, err = branch.PlanFiles(context.Background())
	require.NoError(t, err)
	require.NotNil(t, branchPlanner.receivedRequest.UseSnapshotSchema)
	assert.False(t, *branchPlanner.receivedRequest.UseSnapshotSchema)
	assert.True(t, branchPlanner.receivedRequest.Schema.Equals(metadata.CurrentSchema()))

	tag, err := base.UseRef("tag")
	require.NoError(t, err)
	tagPlanner := &fakeScanPlanner{supports: true}
	tag.planner = tagPlanner
	_, err = tag.PlanFiles(context.Background())
	require.NoError(t, err)
	require.NotNil(t, tagPlanner.receivedRequest.UseSnapshotSchema)
	assert.True(t, *tagPlanner.receivedRequest.UseSnapshotSchema)
	assert.True(t, tagPlanner.receivedRequest.Schema.Equals(metadata.CurrentSchema()))

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
	require.NotNil(t, historicalPlanner.receivedRequest.MinRowsRequested)
	assert.Equal(t, int64(25), *historicalPlanner.receivedRequest.MinRowsRequested)
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

func TestReadTasksClosesPlanIOIfLoadFails(t *testing.T) {
	metadata, err := createTestMetadata(nil, nil)
	require.NoError(t, err)
	loadErr := errors.New("load failed")
	planIO := &failingPlanIO{loadErr: loadErr}
	scan := (&Table{metadata: metadata}).Scan()
	scan.planIO = planIO

	_, _, err = scan.ReadTasks(context.Background(), nil)
	require.ErrorIs(t, err, loadErr)
	assert.True(t, planIO.closed)
	assert.Nil(t, scan.planIO)
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
	// captured after PlanFiles receives it
	receivedIdentifier Identifier
	receivedRequest    ScanPlanningRequest
}

func (f *fakeScanPlanner) SupportsRemoteScanPlanning() bool { return f.supports }

func (f *fakeScanPlanner) PlanFiles(_ context.Context, req ScanPlanningRequest) (ScanPlanningResult, error) {
	f.receivedIdentifier = req.Identifier
	f.receivedRequest = req

	return f.result, f.err
}

type fakePlanIO struct{}

func (fakePlanIO) Load(context.Context) (icebergio.IO, error) { return nil, nil }
func (fakePlanIO) Close() error                               { return nil }

type failingPlanIO struct {
	loadErr error
	closed  bool
}

func (f *failingPlanIO) Load(context.Context) (icebergio.IO, error) { return nil, f.loadErr }
func (f *failingPlanIO) Close() error {
	f.closed = true

	return nil
}
