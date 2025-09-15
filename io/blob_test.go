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
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestDefaultKeyExtractor(t *testing.T) {
	tests := []struct {
		name        string
		input       string
		expectedKey string
		shouldError bool
	}{
		{
			name:        "s3 URI with path",
			input:       "s3://my-bucket/path/to/file.parquet",
			expectedKey: "path/to/file.parquet",
		},
		{
			name:        "s3a URI with path",
			input:       "s3a://my-bucket/path/to/file.parquet",
			expectedKey: "path/to/file.parquet",
		},
		{
			name:        "s3n URI with path",
			input:       "s3n://my-bucket/path/to/file.parquet",
			expectedKey: "path/to/file.parquet",
		},
		{
			name:        "gs URI with path",
			input:       "gs://my-bucket/path/to/file.parquet",
			expectedKey: "path/to/file.parquet",
		},
		{
			name:        "URI with query and fragment",
			input:       "s3://my-bucket/path/to/file.parquet?param=value#fragment",
			expectedKey: "path/to/file.parquet?param=value#fragment",
		},
		{
			name:        "URI with empty path",
			input:       "s3://my-bucket/",
			shouldError: true,
		},
	}

	extractor := defaultKeyExtractor("my-bucket")
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			key, err := extractor(test.input)

			if test.shouldError {
				assert.Error(t, err, "Expected error for input: %s", test.input)
			} else {
				assert.NoError(t, err, "Unexpected error for input: %s", test.input)
				assert.Equal(t, test.expectedKey, key, "Key mismatch for input: %s", test.input)
			}
		})
	}
}
