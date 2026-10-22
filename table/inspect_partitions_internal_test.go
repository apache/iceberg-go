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
	"math"
	"testing"

	"github.com/apache/iceberg-go"
)

func TestInspectPartitionAggregateTreeBinaryKeys(t *testing.T) {
	partitionType := &iceberg.StructType{FieldList: []iceberg.NestedField{
		{ID: 1000, Name: "first", Type: iceberg.PrimitiveTypes.Binary},
		{ID: 1001, Name: "second", Type: iceberg.PrimitiveTypes.Binary},
	}}
	tree := newInspectPartitionAggregateTree()

	check := func(first, second any, want *inspectPartitionAggregate) {
		t.Helper()
		if got := tree.lookup(partitionRecord{first, second}); got != want {
			t.Fatalf("positional lookup (%v, %v) = %p, want %p", first, second, got, want)
		}
		partition := map[int]any{1000: first, 1001: second}
		if got := tree.lookupPartition(partition, partitionType); got != want {
			t.Fatalf("map lookup (%v, %v) = %p, want %p", first, second, got, want)
		}
	}

	check([]byte("missing"), []byte("value"), nil)

	first := []byte{0, 0xff}
	second := []byte("x/y")
	original := &inspectPartitionAggregate{}
	tree.insert(partitionRecord{first, second}, original)
	first[0] = 1
	second[0] = 'z'
	check([]byte{0, 0xff}, []byte("x/y"), original)
	check(first, second, nil)

	nullFirst := &inspectPartitionAggregate{}
	emptyFirst := &inspectPartitionAggregate{}
	tree.insert(partitionRecord{nil, []byte("x")}, nullFirst)
	tree.insert(partitionRecord{[]byte{}, []byte("x")}, emptyFirst)
	check(nil, []byte("x"), nullFirst)
	check([]byte{}, []byte("x"), emptyFirst)
	check([]byte(nil), []byte("x"), emptyFirst)

	binaryFirst := &inspectPartitionAggregate{}
	stringFirst := &inspectPartitionAggregate{}
	tree.insert(partitionRecord{[]byte("same"), []byte("tail")}, binaryFirst)
	tree.insert(partitionRecord{"same", []byte("tail")}, stringFirst)
	check([]byte("same"), []byte("tail"), binaryFirst)
	check("same", []byte("tail"), stringFirst)

	otherNested := &inspectPartitionAggregate{}
	tree.insert(partitionRecord{[]byte("same"), []byte("other")}, otherNested)
	check([]byte("same"), []byte("other"), otherNested)
	check([]byte("same"), []byte("tail"), binaryFirst)
}

func TestInspectPartitionAggregateTreeMixedKeys(t *testing.T) {
	tree := newInspectPartitionAggregateTree()
	negativeZero := &inspectPartitionAggregate{}
	positiveZero := &inspectPartitionAggregate{}
	nan := &inspectPartitionAggregate{}

	tree.insert(partitionRecord{[]byte("scope"), math.Copysign(0, -1)}, negativeZero)
	tree.insert(partitionRecord{[]byte("scope"), float64(0)}, positiveZero)
	tree.insert(partitionRecord{[]byte("scope"), math.Float64frombits(0x7ff8000000000001)}, nan)

	for _, test := range []struct {
		value float64
		want  *inspectPartitionAggregate
	}{
		{math.Copysign(0, -1), negativeZero},
		{0, positiveZero},
		{math.Float64frombits(0x7ff8000000000002), nan},
	} {
		if got := tree.lookup(partitionRecord{[]byte("scope"), test.value}); got != test.want {
			t.Fatalf("lookup (%v) = %p, want %p", test.value, got, test.want)
		}
	}
}
