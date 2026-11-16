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

package puffin_test

import (
	"bytes"
	"testing"

	"github.com/apache/iceberg-go/puffin"
	"github.com/stretchr/testify/require"
)

func TestReaderRejectsInvalidPropertyValues(t *testing.T) {
	const blob = `{"type":"apache-datasketches-theta-v1","fields":[],"snapshot-id":0,"sequence-number":0,"offset":4,"length":0,"properties":`

	tests := []struct {
		name    string
		footer  string
		wantErr string
	}{
		{
			name:    "file property is null",
			footer:  `{"blobs":[],"properties":{"created-by":null}}`,
			wantErr: `property "created-by" must be a string`,
		},
		{
			name:    "blob property is null",
			footer:  `{"blobs":[` + blob + `{"ndv":null}}]}`,
			wantErr: `property "ndv" must be a string`,
		},
		{
			name:    "file properties object is null",
			footer:  `{"blobs":[],"properties":null}`,
			wantErr: "properties must be a JSON object",
		},
		{
			name:    "blob properties object is null",
			footer:  `{"blobs":[` + blob + `null}]}`,
			wantErr: "properties must be a JSON object",
		},
		{
			name:    "file property is numeric",
			footer:  `{"blobs":[],"properties":{"created-by":123}}`,
			wantErr: "invalid properties",
		},
		{
			name:    "blob property is boolean",
			footer:  `{"blobs":[` + blob + `{"ndv":true}}]}`,
			wantErr: "invalid properties",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := puffin.NewReader(bytes.NewReader(fileWithFooterPayload([]byte(tt.footer))))
			require.ErrorContains(t, err, tt.wantErr)
		})
	}
}

func TestReaderPreservesEmptyStringProperties(t *testing.T) {
	const footer = `{"blobs":[{"type":"apache-datasketches-theta-v1","fields":[],"snapshot-id":0,"sequence-number":0,"offset":4,"length":0,"properties":{"ndv":""}}],"properties":{"created-by":""}}`

	reader, err := puffin.NewReader(bytes.NewReader(fileWithFooterPayload([]byte(footer))))
	require.NoError(t, err)

	fileProperties := reader.Properties()
	fileValue, ok := fileProperties["created-by"]
	require.True(t, ok)
	require.Empty(t, fileValue)

	blobs := reader.Blobs()
	require.Len(t, blobs, 1)
	blobValue, ok := blobs[0].Properties["ndv"]
	require.True(t, ok)
	require.Empty(t, blobValue)
}
