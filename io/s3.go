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

package io

import (
	"net/url"
	"os"

	"github.com/go-kit/log"
	"github.com/thanos-io/objstore"
	s3 "github.com/thanos-io/objstore/providers/s3"
)

// Constants for S3 configuration options
const (
	S3Region          = "s3.region"
	S3SessionToken    = "s3.session-token"
	S3SecretAccessKey = "s3.secret-access-key"
	S3AccessKeyID     = "s3.access-key-id"
	S3EndpointURL     = "s3.endpoint"
	S3ProxyURI        = "s3.proxy-uri"
)

func createS3FileIO(parsed *url.URL, props map[string]string) (objstore.Bucket, error) {
	config, err := s3ConfigFromProps(parsed, props)
	if err != nil {
		return nil, err
	}

	return s3.NewBucketWithConfig(log.NewNopLogger(), config, "iceberg")
}

func s3ConfigFromProps(parsed *url.URL, props map[string]string) (s3.Config, error) {
	endpoint, ok := props[S3EndpointURL]
	if !ok {
		endpoint = os.Getenv("AWS_S3_ENDPOINT")
	}

	if endpoint == "" {
		endpoint = "s3.us-east-1.amazonaws.com"
	}

	return s3.Config{
		Bucket:       parsed.Host,
		Endpoint:     endpoint,
		Region:       props[S3Region],
		AccessKey:    props[S3AccessKeyID],
		SecretKey:    props[S3SecretAccessKey],
		SessionToken: props[S3SessionToken],
	}, nil
}
