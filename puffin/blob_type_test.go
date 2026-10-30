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
	"encoding/json"
	"testing"

	"github.com/apache/iceberg-go/puffin"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestBlobTypeIsKnown(t *testing.T) {
	assert.True(t, puffin.BlobTypeDataSketchesTheta.IsKnown())
	assert.True(t, puffin.BlobTypeDeletionVector.IsKnown())
	assert.False(t, puffin.BlobType("vendor-sketch-v1").IsKnown())
	assert.False(t, puffin.BlobType("").IsKnown())
}

func TestBlobTypeUnmarshalErrorPreservesPreviousValue(t *testing.T) {
	for _, input := range []string{`""`, "null", "123", `{}`} {
		t.Run(input, func(t *testing.T) {
			got := puffin.BlobTypeDeletionVector
			err := json.Unmarshal([]byte(input), &got)
			require.Error(t, err)
			assert.Equal(t, puffin.BlobTypeDeletionVector, got)
		})
	}
}

func TestBlobTypeJSON(t *testing.T) {
	for _, tc := range []struct {
		name    string
		input   string
		want    puffin.BlobType
		wantErr bool
	}{
		{name: "theta", input: `"apache-datasketches-theta-v1"`, want: puffin.BlobTypeDataSketchesTheta},
		{name: "deletion vector", input: `"deletion-vector-v1"`, want: puffin.BlobTypeDeletionVector},
		{name: "vendor type", input: `"vendor-sketch-v1"`, want: puffin.BlobType("vendor-sketch-v1")},
		{name: "empty", input: `""`, wantErr: true},
		{name: "null", input: "null", wantErr: true},
		{name: "non-string", input: "123", wantErr: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var got puffin.BlobType
			err := json.Unmarshal([]byte(tc.input), &got)
			if tc.wantErr {
				require.Error(t, err)

				return
			}

			require.NoError(t, err)
			assert.Equal(t, tc.want, got)
			encoded, err := json.Marshal(got)
			require.NoError(t, err)
			assert.JSONEq(t, tc.input, string(encoded))
		})
	}
}
