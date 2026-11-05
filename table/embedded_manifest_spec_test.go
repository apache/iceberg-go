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

package table

import (
	"encoding/json"
	"testing"

	"github.com/apache/iceberg-go"
	"github.com/stretchr/testify/require"
)

func TestEmbeddedManifestSpecHeaderResolution(t *testing.T) {
	field := iceberg.PartitionField{
		SourceIDs: []int{1}, FieldID: 1000, Name: "id", Transform: iceberg.IdentityTransform{},
	}
	spec := iceberg.NewPartitionSpecID(7, field)
	sameFields := iceberg.NewPartitionSpecID(8, field)
	fields, err := json.Marshal([]iceberg.PartitionField{field})
	require.NoError(t, err)

	header, err := parseEmbeddedManifestSpecHeader(map[string][]byte{"partition-spec": fields})
	require.NoError(t, err)
	id, known, ambiguous := header.resolve(map[int32]iceberg.PartitionSpec{7: spec})
	require.Equal(t, int32(7), id)
	require.True(t, known)
	require.False(t, ambiguous)

	_, known, ambiguous = header.resolve(map[int32]iceberg.PartitionSpec{7: spec, 8: sameFields})
	require.False(t, known)
	require.True(t, ambiguous)

	_, known, ambiguous = header.resolve(map[int32]iceberg.PartitionSpec{})
	require.False(t, known)
	require.False(t, ambiguous)

	// Old v1 headers can omit each field-id, which starts at 1000.
	var rawFields []map[string]json.RawMessage
	require.NoError(t, json.Unmarshal(fields, &rawFields))
	delete(rawFields[0], "field-id")
	fields, err = json.Marshal(rawFields)
	require.NoError(t, err)
	header, err = parseEmbeddedManifestSpecHeader(map[string][]byte{"partition-spec": fields})
	require.NoError(t, err)
	require.Equal(t, 1000, header.fields[0].FieldID)
	id, known, ambiguous = header.resolve(map[int32]iceberg.PartitionSpec{7: spec})
	require.Equal(t, int32(7), id)
	require.True(t, known)
	require.False(t, ambiguous)

	// Explicit IDs are returned even when the table has no such spec, so
	// validation can report the unknown ID rather than silently skipping it.
	header, err = parseEmbeddedManifestSpecHeader(map[string][]byte{
		"partition-spec": fields, "partition-spec-id": []byte("42"),
	})
	require.NoError(t, err)
	id, known, ambiguous = header.resolve(map[int32]iceberg.PartitionSpec{7: spec})
	require.Equal(t, int32(42), id)
	require.True(t, known)
	require.False(t, ambiguous)
}

func TestEmbeddedManifestSpecHeaderRejectsInvalidMetadata(t *testing.T) {
	for _, value := range []string{"", "-1", "abc", "2147483648"} {
		t.Run(value, func(t *testing.T) {
			_, err := parseEmbeddedManifestSpecHeader(map[string][]byte{
				"partition-spec": []byte("[]"), "partition-spec-id": []byte(value),
			})
			require.ErrorContains(t, err, "invalid partition-spec-id")
		})
	}
	_, err := parseEmbeddedManifestSpecHeader(map[string][]byte{"partition-spec": []byte("{bad")})
	require.ErrorContains(t, err, "invalid v1 partition-spec")
}
