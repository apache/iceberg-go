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
	"testing"

	"github.com/apache/iceberg-go"
	iceberginternal "github.com/apache/iceberg-go/internal"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type publicManifestFile struct {
	iceberg.ManifestFile
	partitionCalls int
}

func (m *publicManifestFile) Partitions() []iceberg.FieldSummary {
	m.partitionCalls++

	return m.ManifestFile.Partitions()
}

func TestManifestFilePartitionsUsesBorrowedView(t *testing.T) {
	containsNaN := false
	lower := []byte{1, 2}
	upper := []byte{3, 4}
	manifest := iceberg.NewManifestFile(2, "manifest.avro", 0, 1, 1).Partitions(
		[]iceberg.FieldSummary{{
			ContainsNaN: &containsNaN,
			LowerBound:  &lower,
			UpperBound:  &upper,
		}},
	).Build()

	require.Implements(t, (*iceberginternal.ManifestPartitionBorrower[iceberg.FieldSummary])(nil), manifest)
	partitions := manifestFilePartitions(manifest)
	require.Len(t, partitions, 1)
	assert.Equal(t, []byte{1, 2}, *partitions[0].LowerBound)
	assert.Equal(t, []byte{3, 4}, *partitions[0].UpperBound)

	// This intentionally violates the read-only borrow contract to prove that
	// the returned bound aliases manifest storage. Callers must not mutate it.
	(*partitions[0].LowerBound)[0] = 9
	borrowedAgain := manifestFilePartitions(manifest)
	require.NotNil(t, borrowedAgain[0].LowerBound)
	assert.Equal(t, byte(9), (*borrowedAgain[0].LowerBound)[0])
	(*partitions[0].LowerBound)[0] = 1

	allocs := testing.AllocsPerRun(100, func() {
		partitions = manifestFilePartitions(manifest)
	})
	assert.Zero(t, allocs)
}

func TestManifestFilePartitionsFallsBackToPublicGetter(t *testing.T) {
	manifest := iceberg.NewManifestFile(2, "manifest.avro", 0, 1, 1).
		Partitions([]iceberg.FieldSummary{{}}).Build()
	file := &publicManifestFile{ManifestFile: manifest}

	partitions := manifestFilePartitions(file)
	require.Len(t, partitions, 1)
	assert.Equal(t, 1, file.partitionCalls)
}
