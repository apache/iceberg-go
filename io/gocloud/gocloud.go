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

// Package gocloud registers every gocloud.dev-backed FileIO implementation and
// therefore links the AWS, Google Cloud and Azure SDKs.
// To link only the clouds an application uses, blank-import io/gocloud/s3, io/gocloud/gcs or
// io/gocloud/azure instead, in any combination.
package gocloud

import (
	"context"

	"github.com/apache/iceberg-go/io/gocloud/blobfs"
	"github.com/aws/aws-sdk-go-v2/aws"
	"gocloud.dev/blob/gcsblob"

	_ "github.com/apache/iceberg-go/io/gocloud/azure"
	"github.com/apache/iceberg-go/io/gocloud/gcs"
	"github.com/apache/iceberg-go/io/gocloud/s3"
)

var (
	// Deprecated: use [blobfs.ErrEmptyObjectKey]
	ErrEmptyObjectKey = blobfs.ErrEmptyObjectKey
	// Deprecated: use [blobfs.ErrUnsupportedObjectAuthority]
	ErrUnsupportedObjectAuthority = blobfs.ErrUnsupportedObjectAuthority
)

type (
	// BlobFileIO is the FileIO implementation backed by a gocloud.dev bucket.
	//
	// Deprecated: use [blobfs.FileIO]
	BlobFileIO = blobfs.FileIO
	// KeyExtractor extracts the object key from an input path.
	//
	// Deprecated: use [blobfs.KeyExtractor]
	KeyExtractor = blobfs.KeyExtractor
)

// ParseAWSConfig parses the S3 properties and returns a configuration.
//
// Deprecated: use [s3.ParseAWSConfig]
func ParseAWSConfig(ctx context.Context, props map[string]string) (*aws.Config, error) {
	return s3.ParseAWSConfig(ctx, props)
}

// ParseGCSConfig parses GCS properties and returns bucket options.
//
// Deprecated: use [gcs.ParseGCSConfig]
func ParseGCSConfig(props map[string]string) *gcsblob.Options {
	return gcs.ParseGCSConfig(props)
}
