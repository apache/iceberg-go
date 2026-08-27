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
	"github.com/stretchr/testify/require"
)

func TestDecideDeadPositionDeletes(t *testing.T) {
	rewritten := map[string]struct{}{"data-a.parquet": {}}
	newDelete := func(path string) iceberg.DataFile {
		return posDeletePartitionScopedFile(t, *iceberg.UnpartitionedSpec, path, nil)
	}

	fileScopedCovered := positionDeleteCandidate{
		df:               newDelete("del-a.parquet"),
		fileScopedTarget: "data-a.parquet",
	}
	fileScopedLive := positionDeleteCandidate{
		df:               newDelete("del-b.parquet"),
		fileScopedTarget: "data-b.parquet",
	}
	partitionCovered := positionDeleteCandidate{
		df:           newDelete("del-p-covered.parquet"),
		partitionKey: "0:_",
		seq:          5,
	}
	partitionWithOlderSurvivor := positionDeleteCandidate{
		df:           newDelete("del-p-live.parquet"),
		partitionKey: "1:x",
		seq:          5,
	}
	partitionWithNewerSurvivor := positionDeleteCandidate{
		df:           newDelete("del-p-newer.parquet"),
		partitionKey: "2:y",
		seq:          5,
	}

	// "1:x" has a survivor at seq 3 (<= 5), so the delete still applies.
	// "2:y" has a survivor at seq 8 (> 5), so the delete predates it.
	// "0:_" has no survivor.
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
	require.Equal(t, map[string]struct{}{
		"del-a.parquet":         {},
		"del-p-covered.parquet": {},
		"del-p-newer.parquet":   {},
	}, got, "only fully-covered deletes are dead; a delete with an older-or-equal surviving file is retained")
}
