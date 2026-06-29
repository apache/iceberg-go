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
}

func (f *fakeScanPlanner) SupportsRemoteScanPlanning() bool { return f.supports }

func (f *fakeScanPlanner) PlanFiles(context.Context, ScanPlanningRequest) (ScanPlanningResult, error) {
	return f.result, f.err
}

type fakePlanIO struct{}

func (fakePlanIO) Load(context.Context) (icebergio.IO, error) { return nil, nil }
func (fakePlanIO) Close() error                               { return nil }
