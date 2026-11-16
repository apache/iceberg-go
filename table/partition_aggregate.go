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
	"slices"

	"github.com/apache/iceberg-go"
)

type partitionAggregate struct {
	specID              int32
	partition           map[int]any
	dataRecordCount     int64
	dataFileCount       int32
	dataFileSize        int64
	positionDeleteCount int64
	positionDeleteFiles int32
	equalityDeleteCount int64
	equalityDeleteFiles int32
	lastUpdatedAt       *int64
	lastUpdatedSnapshot *int64
	orderingKey         string
	// Persisted partition statistics distinguish DVs from classic delete files
	// and expose an exact live-row total only when metadata proves it.
	deletionVectorCount int32
	totalRecordCount    *int64
	// Partition statistics store timestamps in milliseconds, while the inspect
	// Arrow output above uses microseconds.
	statisticsUpdatedAt       *int64
	statisticsUpdatedSnapshot *int64
}

type partitionAggregateTree struct {
	children       map[any]*partitionAggregateTree
	binaryChildren map[string]*partitionAggregateTree
	aggregate      *partitionAggregate
}

func newPartitionAggregateTree() *partitionAggregateTree {
	return &partitionAggregateTree{}
}

// child uses a typed map for binary values to avoid interface-backed keys
// during repeated lookups. Other values retain their existing key types.
func (t *partitionAggregateTree) child(value any) (*partitionAggregateTree, bool) {
	if binary, ok := value.([]byte); ok {
		child, found := t.binaryChildren[string(binary)]

		return child, found
	}

	child, found := t.children[comparablePartitionKey(value)]

	return child, found
}

func (t *partitionAggregateTree) setChild(value any, child *partitionAggregateTree) {
	if binary, ok := value.([]byte); ok {
		if t.binaryChildren == nil {
			t.binaryChildren = make(map[string]*partitionAggregateTree)
		}
		// A stored string key owns its bytes even if the input buffer changes.
		t.binaryChildren[string(binary)] = child

		return
	}

	if t.children == nil {
		t.children = make(map[any]*partitionAggregateTree)
	}
	t.children[comparablePartitionKey(value)] = child
}

func (t *partitionAggregateTree) lookup(record partitionRecord) *partitionAggregate {
	node := t
	for _, value := range record {
		child, ok := node.child(value)
		if !ok {
			return nil
		}
		node = child
	}

	return node.aggregate
}

func (t *partitionAggregateTree) lookupPartition(
	partition map[int]any,
	partitionType *iceberg.StructType,
) *partitionAggregate {
	node := t
	for i := range partitionType.NumFields() {
		child, ok := node.child(partition[partitionType.Field(i).ID])
		if !ok {
			return nil
		}
		node = child
	}

	return node.aggregate
}

// insert walks the positional record, while lookupPartition walks partition
// field IDs. Build records with the same partition type so their field order
// stays aligned.
func (t *partitionAggregateTree) insert(record partitionRecord, aggregate *partitionAggregate) {
	node := t
	for _, value := range record {
		child, ok := node.child(value)
		if !ok {
			child = newPartitionAggregateTree()
			node.setChild(value, child)
		}
		node = child
	}
	node.aggregate = aggregate
}

func coercePartition(values map[int]any, partitionType *iceberg.StructType) map[int]any {
	if partitionType == nil || partitionType.NumFields() == 0 {
		return nil
	}

	coerced := make(map[int]any, partitionType.NumFields())
	for i := range partitionType.NumFields() {
		id := partitionType.Field(i).ID
		if value, ok := values[id]; ok {
			coerced[id] = value
		}
	}

	return coerced
}

func clonePartition(partition map[int]any) map[int]any {
	if partition == nil {
		return nil
	}
	returnMap := make(map[int]any, len(partition))
	for id, value := range partition {
		if bytes, ok := value.([]byte); ok {
			value = slices.Clone(bytes)
		}
		returnMap[id] = value
	}

	return returnMap
}
