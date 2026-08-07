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
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package table

import (
	"context"
	"sync/atomic"
	"testing"

	"github.com/DataDog/iceberg-go"
	iceio "github.com/DataDog/iceberg-go/io"
	"github.com/DataDog/iceberg-go/metrics"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
)

type refreshUUIDCatalog struct {
	fresh *Table
}

type refreshUUIDMetadataOverride struct {
	Metadata
	id uuid.UUID
}

func (m *refreshUUIDMetadataOverride) TableUUID() uuid.UUID { return m.id }

type changingUUIDRetryCatalog struct {
	initial  Metadata
	fresh    Metadata
	attempts atomic.Int32
}

func (c *changingUUIDRetryCatalog) LoadTable(_ context.Context, ident Identifier) (*Table, error) {
	meta := c.initial
	if c.attempts.Load() > 0 {
		meta = c.fresh
	}

	return New(ident, meta, "metadata.json",
		func(context.Context) (iceio.IO, error) { return iceio.LocalFS{}, nil }, c), nil
}

func (c *changingUUIDRetryCatalog) CommitTable(context.Context, Identifier, []Requirement, []Update) (Metadata, string, error) {
	c.attempts.Add(1)

	return nil, "", ErrCommitFailed
}

func (c *refreshUUIDCatalog) LoadTable(context.Context, Identifier) (*Table, error) {
	return c.fresh, nil
}

func (c *refreshUUIDCatalog) CommitTable(context.Context, Identifier, []Requirement, []Update) (Metadata, string, error) {
	return nil, "", nil
}

type refreshUUIDPlanner struct{}

func (refreshUUIDPlanner) SupportsRemoteScanPlanning() bool { return true }

func (refreshUUIDPlanner) PlanFiles(context.Context, ScanPlanningRequest) (ScanPlanningResult, error) {
	return ScanPlanningResult{}, nil
}

func refreshUUIDMetadata(t *testing.T, id uuid.UUID, location string) Metadata {
	t.Helper()

	meta, err := NewMetadataWithUUID(
		iceberg.NewSchema(0, iceberg.NestedField{ID: 1, Name: "id", Type: iceberg.PrimitiveTypes.Int64}),
		iceberg.UnpartitionedSpec,
		UnsortedSortOrder,
		location,
		iceberg.Properties{
			CommitNumRetriesKey:     "1",
			CommitMinRetryWaitMsKey: "1",
			CommitMaxRetryWaitMsKey: "1",
		},
		id,
	)
	require.NoError(t, err)

	return meta
}

func refreshUUIDMetadataWithOverride(t *testing.T, id uuid.UUID, location string) Metadata {
	return &refreshUUIDMetadataOverride{
		Metadata: refreshUUIDMetadata(t, uuid.New(), location),
		id:       id,
	}
}

func TestRefreshAcceptsMatchingTableUUID(t *testing.T) {
	tableUUID := uuid.New()
	fresh := New(
		Identifier{"db", "table"},
		refreshUUIDMetadata(t, tableUUID, "fresh-location"),
		"fresh-metadata.json",
		func(context.Context) (iceio.IO, error) { return iceio.NewMemFS(), nil },
		nil,
	)
	fresh.planner = refreshUUIDPlanner{}

	tbl := New(
		Identifier{"db", "table"},
		refreshUUIDMetadata(t, tableUUID, "original-location"),
		"original-metadata.json",
		func(context.Context) (iceio.IO, error) { return iceio.LocalFS{}, nil },
		&refreshUUIDCatalog{fresh: fresh},
	)

	require.NoError(t, tbl.Refresh(context.Background()))
	require.Same(t, fresh.metadata, tbl.metadata)
	require.Equal(t, "fresh-metadata.json", tbl.MetadataLocation())
	require.IsType(t, iceio.NewMemFS(), func() iceio.IO {
		fs, err := tbl.FS(context.Background())
		require.NoError(t, err)

		return fs
	}())
	require.Equal(t, refreshUUIDPlanner{}, tbl.planner)
}

func TestRefreshRejectsChangedTableUUIDWithoutMutation(t *testing.T) {
	originalMetadata := refreshUUIDMetadata(t, uuid.New(), "original-location")
	originalReporter := &metrics.InMemoryReporter{}
	originalPlanner := refreshUUIDPlanner{}
	fresh := New(
		Identifier{"db", "table"},
		refreshUUIDMetadata(t, uuid.New(), "fresh-location"),
		"fresh-metadata.json",
		func(context.Context) (iceio.IO, error) { return iceio.NewMemFS(), nil },
		nil,
	)
	fresh.planner = refreshUUIDPlanner{}

	tbl := New(
		Identifier{"db", "table"},
		originalMetadata,
		"original-metadata.json",
		func(context.Context) (iceio.IO, error) { return iceio.LocalFS{}, nil },
		&refreshUUIDCatalog{fresh: fresh},
		WithMetricsReporter(originalReporter),
	)
	tbl.planner = originalPlanner

	err := tbl.Refresh(context.Background())
	require.ErrorIs(t, err, ErrInvalidMetadata)
	require.Same(t, originalMetadata, tbl.metadata)
	require.Equal(t, "original-metadata.json", tbl.MetadataLocation())
	require.Same(t, originalReporter, tbl.MetricsReporter())
	require.Equal(t, originalPlanner, tbl.planner)
	fs, err := tbl.FS(context.Background())
	require.NoError(t, err)
	require.IsType(t, iceio.LocalFS{}, fs)
}

func TestRefreshAllowsMissingTableUUIDOnEitherSide(t *testing.T) {
	for _, tt := range []struct {
		name     string
		original uuid.UUID
		fresh    uuid.UUID
	}{
		{name: "original missing", original: uuid.Nil, fresh: uuid.New()},
		{name: "fresh missing", original: uuid.New(), fresh: uuid.Nil},
		{name: "both missing", original: uuid.Nil, fresh: uuid.Nil},
	} {
		t.Run(tt.name, func(t *testing.T) {
			fresh := New(
				Identifier{"db", "table"},
				refreshUUIDMetadataWithOverride(t, tt.fresh, "fresh-location"),
				"fresh-metadata.json",
				func(context.Context) (iceio.IO, error) { return iceio.NewMemFS(), nil },
				nil,
			)
			tbl := New(
				Identifier{"db", "table"},
				refreshUUIDMetadataWithOverride(t, tt.original, "original-location"),
				"original-metadata.json",
				func(context.Context) (iceio.IO, error) { return iceio.LocalFS{}, nil },
				&refreshUUIDCatalog{fresh: fresh},
			)

			require.NoError(t, tbl.Refresh(context.Background()))
			require.Same(t, fresh.metadata, tbl.metadata)
		})
	}
}

func TestCommitRetryRejectsChangedTableUUIDBeforeRetryCommit(t *testing.T) {
	original := refreshUUIDMetadataWithOverride(t, uuid.New(), "original-location")
	fresh := refreshUUIDMetadataWithOverride(t, uuid.New(), "fresh-location")
	cat := &changingUUIDRetryCatalog{initial: original, fresh: fresh}
	tbl := New(
		Identifier{"db", "table"}, original, "metadata.json",
		func(context.Context) (iceio.IO, error) { return iceio.LocalFS{}, nil }, cat,
	)

	_, err := tbl.doCommit(context.Background(), nil, nil, withCommitBranch(MainBranch))
	require.ErrorIs(t, err, ErrInvalidMetadata)
	require.ErrorContains(t, err, "load a new table handle")
	require.Equal(t, int32(1), cat.attempts.Load(), "changed UUID must stop the retry before a second catalog commit")
}
