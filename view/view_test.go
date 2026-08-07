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

	"github.com/DataDog/iceberg-go/internal"
	iceio "github.com/DataDog/iceberg-go/io"
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
	var mockfs internal.MockFS
	mockfs.Test(t.T())
	mockfs.On("Open", "s3://bucket/test/location/uuid.metadata.json").
		Return(&internal.MockFile{Contents: bytes.NewReader([]byte(exampleViewJSON))}, nil)
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
	var mockfsReadFile internal.MockFSReadFile
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
		"select 1",
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
	metadataFile, err := fs.Open(metadataLocation)
	t.Require().NoError(err)
	t.Require().NoError(metadataFile.Close())
}

func (t *ViewTestSuite) TestLocation() {
	t.Equal("s3://bucket/test/location", t.view.Location())
}

func TestCreateViewReturnsMetadataCloseError(t *testing.T) {
	var mockfs internal.MockFS
	mockfs.Test(t)
	mockfs.On("Create", mock.Anything).
		Return(&internal.MockFile{Contents: bytes.NewReader(nil), ErrOnClose: true}, nil)
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
		"select 1",
		[]string{"ns"},
		scheme+"://bucket/test-view",
		nil,
	)
	require.EqualError(t, err, "error on close")
	require.Nil(t, createdView)
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
