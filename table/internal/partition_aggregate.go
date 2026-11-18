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

package internal

import (
	"slices"

	"github.com/apache/iceberg-go"
)

type PartitionAggregate struct {
	SpecID              int32
	Partition           map[int]any
	DataRecordCount     int64
	DataFileCount       int32
	DataFileSize        int64
	PositionDeleteCount int64
	PositionDeleteFiles int32
	EqualityDeleteCount int64
	EqualityDeleteFiles int32
	LastUpdatedAt       *int64
	LastUpdatedSnapshot *int64
	OrderingKey         string
	// Persisted partition statistics distinguish DVs from classic delete files
	// and expose an exact live-row total only when metadata proves it.
	DeletionVectorCount int32
	TotalRecordCount    *int64
	// Partition statistics store timestamps in milliseconds, while the inspect
	// Arrow output above uses microseconds.
	StatisticsUpdatedAt       *int64
	StatisticsUpdatedSnapshot *int64
}

type PartitionAggregateTree struct {
	children       map[any]*PartitionAggregateTree
	binaryChildren map[string]*PartitionAggregateTree
	aggregate      *PartitionAggregate
}

func NewPartitionAggregateTree() *PartitionAggregateTree {
	return &PartitionAggregateTree{}
}

// child uses a typed map for binary values to avoid interface-backed keys
// during repeated lookups. Other values retain their existing key types.
func (t *PartitionAggregateTree) child(value any) (*PartitionAggregateTree, bool) {
	if binary, ok := value.([]byte); ok {
		child, found := t.binaryChildren[string(binary)]

		return child, found
	}

	child, found := t.children[ComparablePartitionKey(value)]

	return child, found
}

func (t *PartitionAggregateTree) setChild(value any, child *PartitionAggregateTree) {
	if binary, ok := value.([]byte); ok {
		if t.binaryChildren == nil {
			t.binaryChildren = make(map[string]*PartitionAggregateTree)
		}
		// A stored string key owns its bytes even if the input buffer changes.
		t.binaryChildren[string(binary)] = child

		return
	}

	if t.children == nil {
		t.children = make(map[any]*PartitionAggregateTree)
	}
	t.children[ComparablePartitionKey(value)] = child
}

func (t *PartitionAggregateTree) Lookup(record PartitionRecord) *PartitionAggregate {
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

func (t *PartitionAggregateTree) LookupPartition(
	partition map[int]any,
	partitionType *iceberg.StructType,
) *PartitionAggregate {
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

// Insert walks the positional record, while LookupPartition walks partition
// field IDs. Build records with the same partition type so their field order
// stays aligned.
func (t *PartitionAggregateTree) Insert(record PartitionRecord, aggregate *PartitionAggregate) {
	node := t
	for _, value := range record {
		child, ok := node.child(value)
		if !ok {
			child = NewPartitionAggregateTree()
			node.setChild(value, child)
		}
		node = child
	}
	node.aggregate = aggregate
}

func CoercePartition(values map[int]any, partitionType *iceberg.StructType) map[int]any {
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

func ClonePartition(partition map[int]any) map[int]any {
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
