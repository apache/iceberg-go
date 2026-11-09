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

package view

import (
	"bytes"
	"context"
	"net/url"
	"testing"

	"github.com/apache/iceberg-go"
	"github.com/apache/iceberg-go/internal/iomock"
	iceio "github.com/apache/iceberg-go/io"
	"github.com/apache/iceberg-go/table"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
)

type ViewTestSuite struct {
	suite.Suite

	view *View
}

func TestView(t *testing.T) {
	suite.Run(t, new(ViewTestSuite))
}

func (t *ViewTestSuite) SetupSuite() {
	var mockfs iomock.MockFS
	mockfs.Test(t.T())
	mockfs.On("Open", "s3://bucket/test/location/uuid.metadata.json").
		Return(&iomock.MockFile{Contents: bytes.NewReader([]byte(exampleViewJSON))}, nil)
	defer mockfs.AssertExpectations(t.T())

	vw, err := NewFromLocation(
		context.Background(),
		[]string{"foo"},
		"s3://bucket/test/location/uuid.metadata.json",
		func(ctx context.Context) (iceio.IO, error) {
			return &mockfs, nil
		},
	)
	t.Require().NoError(err)
	t.Require().NotNil(vw)

	t.Equal([]string{"foo"}, vw.Identifier())
	t.Equal("s3://bucket/test/location/uuid.metadata.json", vw.MetadataLocation())
	expectedMD, err := ParseMetadataString(exampleViewJSON)
	t.Require().NoError(err)
	t.True(expectedMD.Equals(vw.Metadata()))

	t.view = vw
}

func (t *ViewTestSuite) TestNewViewFromReadFile() {
	var mockfsReadFile iomock.MockFSReadFile
	mockfsReadFile.Test(t.T())
	mockfsReadFile.On("ReadFile", "s3://bucket/test/location/uuid.metadata.json").
		Return([]byte(exampleViewJSON), nil)
	defer mockfsReadFile.AssertExpectations(t.T())

	vw2, err := NewFromLocation(
		t.T().Context(),
		[]string{"foo"},
		"s3://bucket/test/location/uuid.metadata.json",
		func(ctx context.Context) (iceio.IO, error) {
			return &mockfsReadFile, nil
		},
	)
	t.Require().NoError(err)
	t.Require().NotNil(vw2)

	t.True(t.view.Equals(*vw2))
}

func (t *ViewTestSuite) TestCreateViewJoinsTrailingSlashMetadataLocation() {
	createdView, err := CreateView(
		t.T().Context(),
		"test-catalog",
		[]string{"ns", "test_view"},
		newTestSchema(0),
		Representation{SQL: "select 1", Dialect: "spark"},
		[]string{"ns"},
		"mem://view-create-location/test-view/",
		nil,
	)
	t.Require().NoError(err)
	t.Require().NotNil(createdView)

	metadataLocation := createdView.MetadataLocation()
	parsedLocation, err := url.Parse(metadataLocation)
	t.Require().NoError(err)
	t.NotContains(parsedLocation.Path, "//metadata/")
	t.Contains(parsedLocation.Path, "/test-view/metadata/view-")

	fs, err := iceio.LoadFS(t.T().Context(), nil, metadataLocation)
	t.Require().NoError(err)
	metadataFile, err := fs.Open(context.Background(), metadataLocation)
	t.Require().NoError(err)
	t.Require().NoError(metadataFile.Close())
}

func (t *ViewTestSuite) TestLocation() {
	t.Equal("s3://bucket/test/location", t.view.Location())
}

func TestCreateViewReturnsMetadataCloseError(t *testing.T) {
	var mockfs iomock.MockFS
	mockfs.Test(t)
	mockfs.On("Create", mock.Anything).
		Return(&iomock.MockFile{Contents: bytes.NewReader(nil), ErrOnClose: true}, nil)
	defer mockfs.AssertExpectations(t)

	const scheme = "view-close-error"
	iceio.Register(scheme, func(context.Context, *url.URL, map[string]string) (iceio.IO, error) {
		return &mockfs, nil
	})
	defer iceio.Unregister(scheme)

	createdView, err := CreateView(
		t.Context(),
		"test-catalog",
		[]string{"ns", "test_view"},
		newTestSchema(0),
		Representation{SQL: "select 1", Dialect: "spark"},
		[]string{"ns"},
		scheme+"://bucket/test-view",
		nil,
	)
	require.EqualError(t, err, "error on close")
	require.Nil(t, createdView)
}

func TestCreateViewFromVersionKeepsVersion(t *testing.T) {
	version, err := NewVersion(7, 3, []Representation{
		NewRepresentation(Representation{SQL: "SELECT * FROM t", Dialect: "trino"}),
		NewRepresentation(Representation{SQL: "SELECT * FROM t", Dialect: "spark"}),
	}, table.Identifier{"other_ns"},
		WithDefaultViewCatalog("prod"),
		WithVersionSummary(VersionSummary{"engine-version": "467"}),
		WithTimestampMS(1234),
	)
	require.NoError(t, err)

	created, err := CreateViewFromVersion(t.Context(), table.Identifier{"ns", "v"}, newTestSchema(0), version,
		"mem://view-from-version/v", nil, iceberg.Properties{"comment": "kept"})
	require.NoError(t, err)

	fs, err := iceio.LoadFS(t.Context(), nil, created.MetadataLocation())
	require.NoError(t, err)
	f, err := fs.Open(t.Context(), created.MetadataLocation())
	require.NoError(t, err)
	defer f.Close()
	md, err := ParseMetadata(f)
	require.NoError(t, err)

	got := md.CurrentVersion()
	require.Equal(t, int64(InitialVersionID), got.VersionID)
	require.Equal(t, md.CurrentSchemaID(), got.SchemaID)
	require.Equal(t, version.Representations, got.Representations)
	require.Equal(t, table.Identifier{"other_ns"}, got.DefaultNamespace)
	require.Equal(t, "prod", got.DefaultCatalog)
	require.Equal(t, VersionSummary{"engine-name": "iceberg-go", "engine-version": "467"}, got.Summary)
	require.Equal(t, int64(1234), got.TimestampMS)
	require.Equal(t, "kept", md.Properties()["comment"])
	require.Equal(t, int64(7), version.VersionID, "the caller's version is not modified")
}

func TestCreateViewFromVersionFillsUnsetFields(t *testing.T) {
	version := &Version{
		VersionID:       1,
		Representations: []Representation{NewRepresentation(Representation{SQL: "select 1", Dialect: "spark"})},
	}

	created, err := CreateViewFromVersion(t.Context(), table.Identifier{"ns", "v"}, newTestSchema(0), version,
		"mem://view-from-version-defaults/v", nil, nil)
	require.NoError(t, err)

	got := created.CurrentVersion()
	require.Equal(t, table.Identifier{"ns"}, got.DefaultNamespace)
	require.Equal(t, VersionSummary{"engine-name": "iceberg-go"}, got.Summary)
	require.Positive(t, got.TimestampMS)
	require.Empty(t, got.DefaultCatalog)
	require.Nil(t, version.Summary, "the caller's version is not modified")
	require.Nil(t, version.DefaultNamespace)
}

func TestCreateViewFromVersionRejectsNilVersion(t *testing.T) {
	created, err := CreateViewFromVersion(t.Context(), table.Identifier{"ns", "v"}, newTestSchema(0), nil,
		"mem://view-from-version-nil/v", nil, nil)
	require.ErrorIs(t, err, ErrInvalidViewMetadata)
	require.Nil(t, created)
}

func (t *ViewTestSuite) TestIdentifierReturnsDefensiveCopy() {
	identifier := []string{"namespace", "view"}
	vw := New(identifier, nil, "metadata.json")

	identifier[0] = "changed-input"
	t.Equal([]string{"namespace", "view"}, vw.Identifier())

	returned := vw.Identifier()
	returned[1] = "changed-output"
	t.Equal([]string{"namespace", "view"}, vw.Identifier())
}
