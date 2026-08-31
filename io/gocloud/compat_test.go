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

package gocloud_test

import (
	"context"
	"testing"

	"github.com/apache/iceberg-go/internal/schemes"
	"github.com/apache/iceberg-go/io"
	"github.com/apache/iceberg-go/io/gocloud"
	"github.com/apache/iceberg-go/io/gocloud/blobfs"
	"github.com/apache/iceberg-go/io/gocloud/gcs"
	"github.com/apache/iceberg-go/io/gocloud/s3"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// For callers who still have not migrated
var (
	_ *blobfs.FileIO      = (*gocloud.BlobFileIO)(nil)
	_ blobfs.KeyExtractor = gocloud.KeyExtractor(nil)
	_ error               = gocloud.ErrEmptyObjectKey
	_ error               = gocloud.ErrUnsupportedObjectAuthority
)

func TestRegistersAllCloudSchemes(t *testing.T) {
	registered := io.GetRegisteredSchemes()
	for _, list := range [][]string{schemes.S3, schemes.GCS, schemes.Azure} {
		assert.Subset(t, registered, list)
	}
}

func TestDeprecatedParseConfigWrappers(t *testing.T) {
	ctx := context.Background()

	t.Run("ParseGCSConfig", func(t *testing.T) {
		for _, props := range []map[string]string{
			{},
			{io.GCSUseJSONAPI: "true"},
			{io.GCSUseJSONAPI: "false"},
			{io.GCSUseJSONAPI: "not-a-bool"},
			{io.GCSEndpoint: "http://localhost:4443"},
			{io.GCSEndpoint: "http://localhost:4443", io.GCSUseJSONAPI: "true"},
		} {
			want, got := gcs.ParseGCSConfig(props), gocloud.ParseGCSConfig(props)
			assert.Equal(t, want, got, "props: %v", props)
		}
	})

	t.Run("ParseAWSConfig", func(t *testing.T) {
		for _, tt := range []struct {
			props  map[string]string
			static bool
		}{
			{props: map[string]string{}},
			{props: map[string]string{io.S3Region: "us-west-2"}},
			{props: map[string]string{io.S3ClientRegion: "eu-central-1"}},
			{props: map[string]string{io.S3Region: "us-west-2", io.S3ClientRegion: "eu-central-1"}},
			{props: map[string]string{"token": "bearer-token"}},
			{props: map[string]string{io.S3AccessKeyID: "ak", io.S3SecretAccessKey: "sk"}, static: true},
			{props: map[string]string{io.S3AccessKeyID: "ak", io.S3SecretAccessKey: "sk", io.S3SessionToken: "st"}, static: true},
		} {
			want, wantErr := s3.ParseAWSConfig(ctx, tt.props)
			got, gotErr := gocloud.ParseAWSConfig(ctx, tt.props)

			require.NoError(t, wantErr, "props: %v", tt.props)
			require.NoError(t, gotErr, "props: %v", tt.props)
			assert.Equal(t, want.Region, got.Region, "props: %v", tt.props)

			// Retrieving from the default chain would reach the network, so
			// only static credentials are compared by value.
			if !tt.static {
				continue
			}

			wantCreds, err := want.Credentials.Retrieve(ctx)
			require.NoError(t, err)
			gotCreds, err := got.Credentials.Retrieve(ctx)
			require.NoError(t, err)
			assert.Equal(t, wantCreds, gotCreds, "props: %v", tt.props)
		}
	})

	t.Run("error is relayed", func(t *testing.T) {
		props := map[string]string{io.S3RemoteSigningEnabled: "true"}
		_, wantErr := s3.ParseAWSConfig(ctx, props)
		_, gotErr := gocloud.ParseAWSConfig(ctx, props)
		require.Error(t, wantErr)
		require.Error(t, gotErr)
		assert.Equal(t, wantErr.Error(), gotErr.Error())
	})
}
