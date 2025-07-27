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

package spark

import "errors"

// Error definitions for Spark integration
var (
	// ErrSparkLibraryNotAvailable is returned when the Spark Connect Go library is not available
	ErrSparkLibraryNotAvailable = errors.New("Apache Spark Connect Go library is not available due to versioning issues")

	// ErrSparkConnectionFailed is returned when connection to Spark fails
	ErrSparkConnectionFailed = errors.New("failed to connect to Spark server")

	// ErrSparkQueryFailed is returned when a Spark query fails
	ErrSparkQueryFailed = errors.New("Spark query execution failed")

	// ErrTableNotFound is returned when a table is not found
	ErrTableNotFound = errors.New("table not found")

	// ErrInvalidSchema is returned when a schema is invalid
	ErrInvalidSchema = errors.New("invalid schema")

	// ErrUnsupportedOperation is returned when an operation is not supported
	ErrUnsupportedOperation = errors.New("unsupported operation")
)
