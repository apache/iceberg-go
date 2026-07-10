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

package compaction

import (
	"testing"

	"github.com/apache/iceberg-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestReferencedDataFilePath(t *testing.T) {
	filePathID := filePathFieldID

	t.Run("explicit referenced_data_file", func(t *testing.T) {
		df := newPosDelete(t, "del-a.parquet").ReferencedDataFile("data-a.parquet").Build()
		assert.Equal(t, "data-a.parquet", referencedDataFilePath(df))
	})

	t.Run("empty referenced_data_file falls through to bounds", func(t *testing.T) {
		df := newPosDelete(t, "del-a.parquet").ReferencedDataFile("").Build()
		assert.Equal(t, "", referencedDataFilePath(df))
	})

	t.Run("equal file_path bounds resolve the single target", func(t *testing.T) {
		df := newPosDelete(t, "del-a.parquet").
			LowerBoundValues(map[int][]byte{filePathID: []byte("data-a.parquet")}).
			UpperBoundValues(map[int][]byte{filePathID: []byte("data-a.parquet")}).
			Build()
		assert.Equal(t, "data-a.parquet", referencedDataFilePath(df))
	})

	t.Run("unequal file_path bounds are partition-scoped", func(t *testing.T) {
		df := newPosDelete(t, "del-a.parquet").
			LowerBoundValues(map[int][]byte{filePathID: []byte("data-a.parquet")}).
			UpperBoundValues(map[int][]byte{filePathID: []byte("data-b.parquet")}).
			Build()
		assert.Equal(t, "", referencedDataFilePath(df))
	})

	t.Run("no bounds and no ref is partition-scoped", func(t *testing.T) {
		df := newPosDelete(t, "del-a.parquet").Build()
		assert.Equal(t, "", referencedDataFilePath(df))
	})
}

func TestDecideDeadPositionDeletes(t *testing.T) {
	rewritten := map[string]struct{}{"data-a.parquet": {}}

	fileScopedCovered := positionDeleteCandidate{
		df:               newPosDelete(t, "del-a.parquet").Build(),
		fileScopedTarget: "data-a.parquet",
	}
	fileScopedLive := positionDeleteCandidate{
		df:               newPosDelete(t, "del-b.parquet").Build(),
		fileScopedTarget: "data-b.parquet",
	}
	partitionCovered := positionDeleteCandidate{
		df:           newPosDelete(t, "del-p-covered.parquet").Build(),
		partitionKey: "0:_",
		seq:          5,
	}
	partitionWithOlderSurvivor := positionDeleteCandidate{
		df:           newPosDelete(t, "del-p-live.parquet").Build(),
		partitionKey: "1:x",
		seq:          5,
	}
	partitionWithNewerSurvivor := positionDeleteCandidate{
		df:           newPosDelete(t, "del-p-newer.parquet").Build(),
		partitionKey: "2:y",
		seq:          5,
	}

	// "1:x" has a survivor at seq 3 (<= 5) → delete still applies → retain.
	// "2:y" has a survivor at seq 8 (> 5) → delete predates it → dead.
	// "0:_" has no survivor at all → dead.
	minSurvivorSeq := map[string]int64{"1:x": 3, "2:y": 8}

	dead := decideDeadPositionDeletes(
		[]positionDeleteCandidate{
			fileScopedCovered,
			fileScopedLive,
			partitionCovered,
			partitionWithOlderSurvivor,
			partitionWithNewerSurvivor,
		},
		rewritten,
		minSurvivorSeq,
	)

	got := make(map[string]struct{}, len(dead))
	for _, df := range dead {
		got[df.FilePath()] = struct{}{}
	}
	assert.Equal(t, map[string]struct{}{
		"del-a.parquet":         {},
		"del-p-covered.parquet": {},
		"del-p-newer.parquet":   {},
	}, got, "only fully-covered deletes are dead; a delete with an older-or-equal surviving file is retained")
}

func newPosDelete(t *testing.T, path string) *iceberg.DataFileBuilder {
	t.Helper()

	b, err := iceberg.NewDataFileBuilder(
		*iceberg.UnpartitionedSpec, iceberg.EntryContentPosDeletes,
		path, iceberg.ParquetFile, nil, nil, nil, 1, 128)
	require.NoError(t, err)

	return b
}
