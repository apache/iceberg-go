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
	"testing"

	"github.com/apache/iceberg-go/internal"
	iceio "github.com/apache/iceberg-go/io"
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

func (t *ViewTestSuite) TestLocation() {
	t.Equal("s3://bucket/test/location", t.view.Location())
}
