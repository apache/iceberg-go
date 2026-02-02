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

package gocloud

import (
	"context"
	"net/url"

	icebergio "github.com/apache/iceberg-go/io"
	"gocloud.dev/blob/memblob"
)

func init() {
	registerS3Schemes()
	registerGCSScheme()
	registerMemScheme()
	registerAzureSchemes()
}

// registerS3Schemes registers S3-compatible storage schemes (s3, s3a, s3n).
func registerS3Schemes() {
	s3Factory := func(ctx context.Context, parsed *url.URL, props map[string]string) (icebergio.IO, error) {
		bucket, err := createS3Bucket(ctx, parsed, props)
		if err != nil {
			return nil, err
		}

		return createBlobFS(ctx, bucket, defaultKeyExtractor(parsed.Host)), nil
	}
	icebergio.Register("s3", s3Factory)
	icebergio.Register("s3a", s3Factory)
	icebergio.Register("s3n", s3Factory)
}

// registerGCSScheme registers the Google Cloud Storage scheme (gs).
func registerGCSScheme() {
	icebergio.Register("gs", func(ctx context.Context, parsed *url.URL, props map[string]string) (icebergio.IO, error) {
		bucket, err := createGCSBucket(ctx, parsed, props)
		if err != nil {
			return nil, err
		}

		return createBlobFS(ctx, bucket, defaultKeyExtractor(parsed.Host)), nil
	})
}

// registerMemScheme registers the in-memory blob storage scheme (mem).
func registerMemScheme() {
	icebergio.Register("mem", func(ctx context.Context, parsed *url.URL, props map[string]string) (icebergio.IO, error) {
		bucket := memblob.OpenBucket(nil)

		return createBlobFS(ctx, bucket, defaultKeyExtractor(parsed.Host)), nil
	})
}

// registerAzureSchemes registers Azure Data Lake Storage schemes (abfs, abfss, wasb, wasbs).
func registerAzureSchemes() {
	azureFactory := func(ctx context.Context, parsed *url.URL, props map[string]string) (icebergio.IO, error) {
		bucket, err := createAzureBucket(ctx, parsed, props)
		if err != nil {
			return nil, err
		}

		return createBlobFS(ctx, bucket, adlsKeyExtractor()), nil
	}
	icebergio.Register("abfs", azureFactory)
	icebergio.Register("abfss", azureFactory)
	icebergio.Register("wasb", azureFactory)
	icebergio.Register("wasbs", azureFactory)
}
