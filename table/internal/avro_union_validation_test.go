// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied. See the License for the
// specific language governing permissions and limitations
// under the License.

package internal

import (
	"testing"

	"github.com/apache/iceberg-go"
	"github.com/stretchr/testify/require"
	avro "github.com/twmb/avro"
)

func TestAvroReadSchemaUnionAlternatives(t *testing.T) {
	for _, tc := range []struct {
		name     string
		branches []string
		valid    bool
	}{
		{name: "nullable", branches: []string{"null", "long"}, valid: true},
		{name: "nullable-reversed", branches: []string{"long", "null"}, valid: true},
		{name: "without-null", branches: []string{"long", "string"}},
		{name: "duplicate-null", branches: []string{"null", "null"}},
		{name: "multiple-values", branches: []string{"null", "long", "string"}},
		{name: "single-branch", branches: []string{"long"}},
		{name: "empty", branches: nil},
	} {
		t.Run(tc.name, func(t *testing.T) {
			branches := make([]avro.SchemaNode, len(tc.branches))
			for i, kind := range tc.branches {
				branches[i] = avro.SchemaNode{Type: kind}
			}
			err := validateAvroReadNode(avro.SchemaNode{Type: "union", Branches: branches}, false, false)
			if tc.valid {
				require.NoError(t, err)
			} else {
				require.ErrorIs(t, err, iceberg.ErrNotImplemented)
			}
		})
	}
}

func TestAvroRejectsExcessiveRecordNesting(t *testing.T) {
	node := avro.SchemaNode{Type: "long"}
	for range maxAvroReadSchemaDepth {
		node = avro.SchemaNode{
			Type:   "record",
			Fields: []avro.SchemaField{{Name: "value", Type: node}},
		}
	}
	require.ErrorIs(t, validateAvroReadNode(node, false, false), iceberg.ErrInvalidSchema)
}
