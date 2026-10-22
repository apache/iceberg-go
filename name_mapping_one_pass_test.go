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

package iceberg_test

import (
	"fmt"
	"testing"

	"github.com/apache/iceberg-go"
	"github.com/stretchr/testify/require"
)

func TestUpdateNameMappingWideFieldIDsIndependent(t *testing.T) {
	const fieldCount = 256
	mapping := make(iceberg.NameMapping, fieldCount)
	for i := range mapping {
		mapping[i] = iceberg.MappedField{
			FieldID: new(i + 1),
			Names:   []string{fmt.Sprintf("field_%d", i)},
		}
	}

	updated, err := iceberg.UpdateNameMapping(mapping, nil, nil)
	require.NoError(t, err)
	require.Equal(t, mapping, updated)

	*updated[0].FieldID = 9000
	*updated[1].FieldID = 9001
	updated[2].Names[0] = "changed"

	require.Equal(t, 1, *mapping[0].FieldID)
	require.Equal(t, 2, *mapping[1].FieldID)
	require.Equal(t, "field_2", mapping[2].Names[0])
	require.Equal(t, 3, *updated[2].FieldID)
	require.Equal(t, 4, *updated[3].FieldID)
}

func TestUpdateNameMappingEmptyAndAnonymous(t *testing.T) {
	_, err := iceberg.UpdateNameMapping(nil, nil, nil)
	require.ErrorIs(t, err, iceberg.ErrInvalidArgument)

	empty, err := iceberg.UpdateNameMapping(
		iceberg.NameMapping{}, nil, map[int][]iceberg.NestedField{-1: {}},
	)
	require.NoError(t, err)
	require.NotNil(t, empty)
	require.Empty(t, empty)

	mapping := iceberg.NameMapping{
		{Names: []string{"alias"}, Fields: []iceberg.MappedField{
			{FieldID: new(7), Names: []string{"old", "current"}},
		}},
		{Names: []string{}},
		{FieldID: new(8), Names: []string{"other"}},
	}
	updated, err := iceberg.UpdateNameMapping(
		mapping,
		map[int]iceberg.NestedField{7: {ID: 7, Name: "renamed"}},
		nil,
	)
	require.NoError(t, err)
	require.Len(t, updated, 2)
	require.Nil(t, updated[0].FieldID)
	require.Equal(t, []string{"old", "current", "renamed"}, updated[0].Fields[0].Names)
	require.Equal(t, 8, *updated[1].FieldID)
	require.Len(t, mapping, 3)
}

func TestUpdateNameMappingPrunesNamelessMappingToNil(t *testing.T) {
	mapping := iceberg.NameMapping{{Names: []string{}}}
	updated, err := iceberg.UpdateNameMapping(mapping, nil, nil)
	require.NoError(t, err)
	require.Nil(t, updated)
}
