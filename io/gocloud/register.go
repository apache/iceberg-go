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
	"fmt"
	"net/url"
	"strconv"

	icebergio "github.com/apache/iceberg-go/io"
)

func init() {
	registerS3Schemes()
	registerGCSScheme()
	registerAzureSchemes()
}

func keyExtractorOptions(props map[string]string) ([]keyExtractorOption, error) {
	value, ok := props[icebergio.ObjectStoreStrictAuthorityValidation]
	if !ok {
		return nil, nil
	}

	enabled, err := strconv.ParseBool(value)
	if err != nil {
		return nil, fmt.Errorf("invalid %s value %q: %w",
			icebergio.ObjectStoreStrictAuthorityValidation, value, err)
	}

	if enabled {
		return []keyExtractorOption{withStrictAuthorityValidation()}, nil
	}

	return nil, nil
}

// registerS3Schemes registers S3-compatible storage schemes (s3, s3a, s3n).
func registerS3Schemes() {
	s3Factory := func(ctx context.Context, parsed *url.URL, props map[string]string) (icebergio.IO, error) {
		opts, err := keyExtractorOptions(props)
		if err != nil {
			return nil, err
		}

		bucket, err := createS3Bucket(ctx, parsed, props)
		if err != nil {
			return nil, err
		}

		extractor := defaultObjectLocationExtractor(parsed.Host, opts...)

		return createBlobFS(ctx, bucket, extractor), nil
	}
	icebergio.Register("s3", s3Factory)
	icebergio.Register("s3a", s3Factory)
	icebergio.Register("s3n", s3Factory)
	icebergio.Register("oss", s3Factory)
}

// registerGCSScheme registers the Google Cloud Storage scheme (gs).
func registerGCSScheme() {
	icebergio.Register("gs", func(ctx context.Context, parsed *url.URL, props map[string]string) (icebergio.IO, error) {
		opts, err := keyExtractorOptions(props)
		if err != nil {
			return nil, err
		}

		bucket, err := createGCSBucket(ctx, parsed, props)
		if err != nil {
			return nil, err
		}

		extractor := defaultObjectLocationExtractor(parsed.Host, opts...)

		return createBlobFS(ctx, bucket, extractor), nil
	})
}

// registerAzureSchemes registers Azure Data Lake Storage schemes (abfs, abfss, wasb, wasbs).
func registerAzureSchemes() {
	azureFactory := func(ctx context.Context, parsed *url.URL, props map[string]string) (icebergio.IO, error) {
		opts, err := keyExtractorOptions(props)
		if err != nil {
			return nil, err
		}

		bucket, err := createAzureBucket(ctx, parsed, props)
		if err != nil {
			return nil, err
		}

		extractor := adlsObjectLocationExtractor(parsed, opts...)

		return createBlobFS(ctx, bucket, extractor), nil
	}
	icebergio.Register("abfs", azureFactory)
	icebergio.Register("abfss", azureFactory)
	icebergio.Register("wasb", azureFactory)
	icebergio.Register("wasbs", azureFactory)
}
