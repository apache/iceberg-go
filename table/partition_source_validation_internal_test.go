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
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package table

import (
	"testing"

	"github.com/apache/iceberg-go"
	"github.com/stretchr/testify/require"
)

func TestCheckPartitionSource(t *testing.T) {
	for _, tc := range []struct {
		name      string
		transform iceberg.Transform
		wantErr   bool
	}{
		{name: "identity source missing", transform: iceberg.IdentityTransform{}, wantErr: true},
		{name: "void source dropped", transform: iceberg.VoidTransform{}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			field := iceberg.PartitionField{
				SourceIDs: []int{7}, FieldID: 1000, Name: "partition", Transform: tc.transform,
			}
			err := checkPartitionSource(field, iceberg.NewSchema(0))
			if tc.wantErr {
				require.ErrorIs(t, err, iceberg.ErrInvalidSchema)
				require.ErrorContains(t, err, "partition source field 7 not found")
			} else {
				require.NoError(t, err)
			}
		})
	}
}
