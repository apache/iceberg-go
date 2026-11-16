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

package catalog

import (
	"bytes"
	"context"
	"errors"
	"os"
	"testing"

	"github.com/apache/iceberg-go"
	"github.com/apache/iceberg-go/table"
	"github.com/apache/iceberg-go/view"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
)

type rollbackTestCatalog struct {
	Catalog
	ViewCatalog
	current        *view.View
	commitMetadata view.Metadata
	loadError      error
	updateError    error
	called         bool
}

func (c *rollbackTestCatalog) LoadView(ctx context.Context, ident table.Identifier) (*view.View, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}

	return c.current, c.loadError
}

func (c *rollbackTestCatalog) UpdateView(ctx context.Context, ident table.Identifier, requirements []view.Requirement, updates []view.Update) (*view.View, error) {
	c.called = true
	if c.updateError != nil {
		return nil, c.updateError
	}
	metadata := c.current.Metadata()
	if c.commitMetadata != nil {
		metadata = c.commitMetadata
	}
	for _, requirement := range requirements {
		if err := requirement.Validate(metadata); err != nil {
			return nil, err
		}
	}
	builder, err := view.MetadataBuilderFromBase(metadata)
	if err != nil {
		return nil, err
	}
	for _, update := range updates {
		if err := update.Apply(builder); err != nil {
			return nil, err
		}
	}
	updated, err := builder.Build()
	if err != nil {
		return nil, err
	}

	return view.New(ident, updated.Metadata, "new.metadata.json"), nil
}

func rollbackFixture(t *testing.T) *view.View {
	t.Helper()
	data, err := os.ReadFile("../view/testdata/view-metadata.json")
	require.NoError(t, err)
	metadata, err := view.ParseMetadataBytes(data)
	require.NoError(t, err)

	return view.New(table.Identifier{"db", "view"}, metadata, "old.metadata.json")
}

func TestRollbackView(t *testing.T) {
	current := rollbackFixture(t)
	c := &rollbackTestCatalog{current: current}
	wrapped := &capabilityTestDecorator{Catalog: c}
	updated, err := RollbackView(t.Context(), wrapped, current.Identifier(), 1)
	require.NoError(t, err)
	require.True(t, c.called)
	require.EqualValues(t, 1, updated.CurrentVersion().VersionID)
	require.Equal(t, current.VersionByID(1), updated.CurrentVersion())
	require.Equal(t, current.Metadata().ViewUUID(), updated.Metadata().ViewUUID())
	require.Len(t, updated.Schemas(), len(current.Schemas()))
	for id, schema := range current.Schemas() {
		require.True(t, schema.Equals(updated.Schemas()[id]))
	}
	require.Len(t, updated.Versions(), 2)
	log := updated.Metadata().VersionLog()
	require.Len(t, log, 3)
	require.EqualValues(t, 1, log[2].VersionID)
	require.Greater(t, log[2].TimestampMS, current.CurrentVersion().TimestampMS)
	require.EqualValues(t, 2, current.CurrentVersion().VersionID)
}

func TestRollbackViewCurrentAndUnavailableVersions(t *testing.T) {
	current := rollbackFixture(t)
	c := &rollbackTestCatalog{current: current}
	same, err := RollbackView(t.Context(), c, current.Identifier(), current.CurrentVersion().VersionID)
	require.NoError(t, err)
	require.Same(t, current, same)
	require.False(t, c.called)
	for _, version := range []int64{-1, 0, 3, 999} {
		_, err := RollbackView(t.Context(), c, current.Identifier(), version)
		require.ErrorIs(t, err, iceberg.ErrInvalidArgument)
		require.False(t, c.called)
	}
	builder, err := view.MetadataBuilderFromBase(current.Metadata())
	require.NoError(t, err)
	metadata, err := builder.SetProperties(iceberg.Properties{view.VersionHistorySizeKey: "1"}).Build()
	require.NoError(t, err)
	c.current = view.New(current.Identifier(), metadata.Metadata, current.MetadataLocation())
	_, err = RollbackView(t.Context(), c, current.Identifier(), 1)
	require.ErrorIs(t, err, iceberg.ErrInvalidArgument)
	require.False(t, c.called)
}

func TestRollbackViewRejectsRecreatedView(t *testing.T) {
	current := rollbackFixture(t)
	data, err := os.ReadFile("../view/testdata/view-metadata.json")
	require.NoError(t, err)
	data = bytes.ReplaceAll(data, []byte(current.Metadata().ViewUUID().String()), []byte(uuid.NewString()))
	replacement, err := view.ParseMetadataBytes(data)
	require.NoError(t, err)
	c := &rollbackTestCatalog{current: current, commitMetadata: replacement}
	_, err = RollbackView(t.Context(), c, current.Identifier(), 1)
	require.ErrorContains(t, err, "UUID mismatch")
}

func TestRollbackViewErrors(t *testing.T) {
	_, err := RollbackView(t.Context(), nil, table.Identifier{"db", "view"}, 1)
	require.ErrorIs(t, err, iceberg.ErrNotImplemented)
	current := rollbackFixture(t)
	cause := errors.New("catalog unavailable")
	c := &rollbackTestCatalog{current: current, loadError: cause}
	_, err = RollbackView(t.Context(), c, current.Identifier(), 1)
	require.ErrorIs(t, err, cause)
	require.False(t, c.called)
	c.loadError = nil
	c.updateError = cause
	_, err = RollbackView(t.Context(), c, current.Identifier(), 1)
	require.ErrorIs(t, err, cause)
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	_, err = RollbackView(ctx, c, current.Identifier(), 1)
	require.ErrorIs(t, err, context.Canceled)
}

func TestRollbackViewVersionExpiresBeforeCommit(t *testing.T) {
	current := rollbackFixture(t)
	builder, err := view.MetadataBuilderFromBase(current.Metadata())
	require.NoError(t, err)
	expired, err := builder.SetProperties(iceberg.Properties{view.VersionHistorySizeKey: "1"}).Build()
	require.NoError(t, err)
	c := &rollbackTestCatalog{current: current, commitMetadata: expired.Metadata}
	_, err = RollbackView(t.Context(), c, current.Identifier(), 1)
	require.ErrorContains(t, err, "unknown version")
	require.True(t, c.called)
}

func TestRollbackViewPreservesDialectPolicy(t *testing.T) {
	current := rollbackFixture(t)
	next := current.CurrentVersion().Clone()
	next.Representations = append(next.Representations, view.NewRepresentation(view.Representation{SQL: next.Representations[0].SQL, Dialect: "hive"}))
	builder, err := view.MetadataBuilderFromBase(current.Metadata())
	require.NoError(t, err)
	metadata, err := builder.SetCurrentVersion(next, current.CurrentSchema()).Build()
	require.NoError(t, err)
	c := &rollbackTestCatalog{current: view.New(current.Identifier(), metadata.Metadata, "latest.metadata.json")}
	_, err = RollbackView(t.Context(), c, current.Identifier(), 1)
	require.ErrorContains(t, err, "dialect")
	builder, err = view.MetadataBuilderFromBase(c.current.Metadata())
	require.NoError(t, err)
	allowed, err := builder.SetProperties(iceberg.Properties{view.ReplaceDropDialectAllowedKey: "true"}).Build()
	require.NoError(t, err)
	c.current = view.New(current.Identifier(), allowed.Metadata, "latest.metadata.json")
	rolledBack, err := RollbackView(t.Context(), c, current.Identifier(), 1)
	require.NoError(t, err)
	require.EqualValues(t, 1, rolledBack.CurrentVersion().VersionID)
}
