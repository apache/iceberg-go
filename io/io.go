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
	"fmt"
	"net/url"

	"github.com/thanos-io/objstore"
	"github.com/thanos-io/objstore/providers/filesystem"
)

func bucketFromSchema(path string, props map[string]string) (objstore.Bucket, error) {
	parsed, err := url.Parse(path)
	if err != nil {
		return nil, err
	}

	switch parsed.Scheme {
	case "s3", "s3a", "s3n":
		return createS3FileIO(parsed, props)
	case "file", "":
		return filesystem.NewBucket("/")
	default:
		return nil, fmt.Errorf("IO for file '%s' not implemented", path)
	}
}

// LoadFS takes a map of properties and an optional URI location
// and attempts to infer an bucket object from it.
//
// A schema of "file://" or an empty string will result in a LocalFS
// implementation. Otherwise this will return an error if the schema
// does not yet have an implementation here.
//
// Currently only LocalFS and S3 are implemented.
func LoadFS(props map[string]string, location string) (objstore.Bucket, error) {
	if location == "" {
		location = props["warehouse"]
	}

	return bucketFromSchema(location, props)
}
