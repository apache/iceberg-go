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
	"context"

	"github.com/apache/iceberg-go"
	iceio "github.com/apache/iceberg-go/io"
	"github.com/apache/iceberg-go/table"
)

// CollectDeadPositionDeletes returns the position-delete files made dead by a
// rewrite. The implementation lives in the table package so table-level
// rewrite actions can use the same whole-rewrite safety predicate without an
// import cycle.
func CollectDeadPositionDeletes(
	ctx context.Context,
	fs iceio.IO,
	snap *table.Snapshot,
	rewrittenPaths map[string]struct{},
) ([]iceberg.DataFile, error) {
	return table.CollectDeadPositionDeletes(ctx, fs, snap, rewrittenPaths)
}

type positionDeleteCandidate struct {
	df               iceberg.DataFile
	fileScopedTarget string
	partitionKey     string
	seq              int64
}

func decideDeadPositionDeletes(candidates []positionDeleteCandidate, rewrittenPaths map[string]struct{}, minSurvivorSeq map[string]int64) []iceberg.DataFile {
	dead := make([]iceberg.DataFile, 0, len(candidates))
	for _, c := range candidates {
		if c.fileScopedTarget != "" {
			if _, rewritten := rewrittenPaths[c.fileScopedTarget]; rewritten {
				dead = append(dead, c.df)
			}
			continue
		}
		if cur, ok := minSurvivorSeq[c.partitionKey]; !ok || cur > c.seq {
			dead = append(dead, c.df)
		}
	}
	return dead
}
