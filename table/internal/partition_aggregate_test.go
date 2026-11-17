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
	"math"
	"testing"

	"github.com/apache/iceberg-go"
	"github.com/stretchr/testify/require"
)

func TestPartitionAggregateTreeBinaryKeys(t *testing.T) {
	partitionType := iceberg.NewStructType(
		iceberg.NestedField{ID: 1000, Name: "first", Type: iceberg.PrimitiveTypes.Binary},
		iceberg.NestedField{ID: 1001, Name: "second", Type: iceberg.PrimitiveTypes.Binary},
	)
	tree := NewPartitionAggregateTree()

	check := func(first, second any, want *PartitionAggregate) {
		t.Helper()
		if got := tree.Lookup(PartitionRecord{first, second}); got != want {
			t.Fatalf("positional lookup (%v, %v) = %p, want %p", first, second, got, want)
		}
		partition := map[int]any{1000: first, 1001: second}
		if got := tree.LookupPartition(partition, partitionType); got != want {
			t.Fatalf("map lookup (%v, %v) = %p, want %p", first, second, got, want)
		}
	}

	check([]byte("missing"), []byte("value"), nil)

	first := []byte{0, 0xff}
	second := []byte("x/y")
	original := &PartitionAggregate{}
	tree.Insert(PartitionRecord{first, second}, original)
	first[0] = 1
	second[0] = 'z'
	check([]byte{0, 0xff}, []byte("x/y"), original)
	check(first, second, nil)

	nullFirst := &PartitionAggregate{}
	emptyFirst := &PartitionAggregate{}
	tree.Insert(PartitionRecord{nil, []byte("x")}, nullFirst)
	tree.Insert(PartitionRecord{[]byte{}, []byte("x")}, emptyFirst)
	check(nil, []byte("x"), nullFirst)
	check([]byte{}, []byte("x"), emptyFirst)
	check([]byte(nil), []byte("x"), emptyFirst)

	binaryFirst := &PartitionAggregate{}
	stringFirst := &PartitionAggregate{}
	tree.Insert(PartitionRecord{[]byte("same"), []byte("tail")}, binaryFirst)
	tree.Insert(PartitionRecord{"same", []byte("tail")}, stringFirst)
	check([]byte("same"), []byte("tail"), binaryFirst)
	check("same", []byte("tail"), stringFirst)

	otherNested := &PartitionAggregate{}
	tree.Insert(PartitionRecord{[]byte("same"), []byte("other")}, otherNested)
	check([]byte("same"), []byte("other"), otherNested)
	check([]byte("same"), []byte("tail"), binaryFirst)
}

func TestPartitionAggregateTreeMixedKeys(t *testing.T) {
	tree := NewPartitionAggregateTree()
	negativeZero := &PartitionAggregate{}
	positiveZero := &PartitionAggregate{}
	nan := &PartitionAggregate{}

	tree.Insert(PartitionRecord{[]byte("scope"), math.Copysign(0, -1)}, negativeZero)
	tree.Insert(PartitionRecord{[]byte("scope"), float64(0)}, positiveZero)
	tree.Insert(PartitionRecord{[]byte("scope"), math.Float64frombits(0x7ff8000000000001)}, nan)

	for _, test := range []struct {
		value float64
		want  *PartitionAggregate
	}{
		{math.Copysign(0, -1), negativeZero},
		{0, positiveZero},
		{math.Float64frombits(0x7ff8000000000002), nan},
	} {
		if got := tree.Lookup(PartitionRecord{[]byte("scope"), test.value}); got != test.want {
			t.Fatalf("lookup (%v) = %p, want %p", test.value, got, test.want)
		}
	}
}

func TestPartitionAggregateTreeHandlesBinaryAndNaNValues(t *testing.T) {
	tree := NewPartitionAggregateTree()
	aggregate := &PartitionAggregate{SpecID: 1}
	record := PartitionRecord{[]byte{1, 2, 3}, math.NaN()}
	tree.Insert(record, aggregate)

	require.Same(t, aggregate, tree.Lookup(PartitionRecord{[]byte{1, 2, 3}, math.NaN()}))
	require.Nil(t, tree.Lookup(PartitionRecord{[]byte{1, 2, 4}, math.NaN()}))
}

func TestPartitionAggregateTreeDistinguishesFloatWidthsAndSignedZero(t *testing.T) {
	tree := NewPartitionAggregateTree()
	float32Aggregate := &PartitionAggregate{SpecID: 1}
	float32Record := PartitionRecord{math.Float32frombits(0xffc00001)}
	tree.Insert(float32Record, float32Aggregate)

	require.Same(t, float32Aggregate, tree.Lookup(PartitionRecord{math.Float32frombits(0x7fc00002)}))
	require.Nil(t, tree.Lookup(PartitionRecord{math.Float64frombits(0x7ff8000000000001)}))

	negativeZeroAggregate := &PartitionAggregate{SpecID: 2}
	tree.Insert(PartitionRecord{math.Copysign(0, -1)}, negativeZeroAggregate)
	require.Same(t, negativeZeroAggregate, tree.Lookup(PartitionRecord{math.Copysign(0, -1)}))
	require.Nil(t, tree.Lookup(PartitionRecord{float64(0)}))
}

func TestPartitionAggregateTreeLookupPartition(t *testing.T) {
	partitionType := iceberg.NewStructType(
		iceberg.NestedField{ID: 1000, Name: "id", Type: iceberg.PrimitiveTypes.Int32},
		iceberg.NestedField{ID: 1001, Name: "payload", Type: iceberg.PrimitiveTypes.Binary},
		iceberg.NestedField{ID: 1002, Name: "score", Type: iceberg.PrimitiveTypes.Float64},
	)
	partition := map[int]any{
		1000: int32(7),
		1001: []byte{1, 2, 3},
		1002: math.NaN(),
	}
	record := NewPartitionRecord(CoercePartition(partition, partitionType), partitionType)
	aggregate := &PartitionAggregate{SpecID: 1}
	tree := NewPartitionAggregateTree()
	tree.Insert(record, aggregate)

	require.Same(t, aggregate, tree.LookupPartition(partition, partitionType))
	require.Nil(t, tree.LookupPartition(map[int]any{
		1000: int32(7),
		1001: []byte{1, 2, 4},
		1002: math.NaN(),
	}, partitionType))

	// A file written with an older partition spec does not have the field
	// added by a later spec. Both lookup paths must key the missing field as nil.
	evolvedPartition := map[int]any{
		1000: int32(7),
		1002: math.NaN(),
	}
	evolvedRecord := NewPartitionRecord(CoercePartition(evolvedPartition, partitionType), partitionType)
	evolvedAggregate := &PartitionAggregate{SpecID: 2}
	tree.Insert(evolvedRecord, evolvedAggregate)

	require.Same(t, evolvedAggregate, tree.LookupPartition(evolvedPartition, partitionType))
	require.Same(t, evolvedAggregate, tree.Lookup(evolvedRecord))
}

func TestClonePartitionClonesBinaryValues(t *testing.T) {
	value := []byte{1, 2, 3}
	cloned := ClonePartition(map[int]any{1000: value})

	value[0] = 4
	require.Equal(t, []byte{1, 2, 3}, cloned[1000])

	clonedValue := cloned[1000].([]byte)
	clonedValue[1] = 5

	require.Equal(t, []byte{4, 2, 3}, value)
}
