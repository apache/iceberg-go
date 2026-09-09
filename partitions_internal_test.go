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

package iceberg

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestPartitionSpecCloneCopiesFields(t *testing.T) {
	spec := NewPartitionSpecID(7, PartitionField{
		SourceIDs: []int{1}, FieldID: 1000, Name: "id",
		Transform: &BucketTransform{NumBuckets: 16},
	})

	clone := spec.Clone()
	require.Len(t, clone.FieldsBySourceID(1), 1)

	clone.fields[0].SourceIDs[0] = 2
	clone.fields[0].Name = "mutated"
	clone.fields[0].Transform.(*BucketTransform).NumBuckets = 32

	require.Equal(t, []int{1}, spec.fields[0].SourceIDs)
	require.Equal(t, "id", spec.fields[0].Name)
	require.Equal(t, 16, spec.fields[0].Transform.(*BucketTransform).NumBuckets)
	require.Equal(t, []int{2}, clone.fields[0].SourceIDs)
	require.Equal(t, "mutated", clone.fields[0].Name)
	require.Equal(t, 32, clone.fields[0].Transform.(*BucketTransform).NumBuckets)
}
