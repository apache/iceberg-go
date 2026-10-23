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
	_ "github.com/apache/iceberg-go/io/gocloud/azure"
	_ "github.com/apache/iceberg-go/io/gocloud/gcs"
	_ "github.com/apache/iceberg-go/io/gocloud/s3"
)
